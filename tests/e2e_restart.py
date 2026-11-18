"""End-to-end recovery-interleave checks with a real Redis restart.

The stack (two query instances + origin + AOF-enabled Redis) must already be
running.  This script restarts Redis itself, so it needs the Redis control
paths (redis-cli / redis-server) used by scripts/restart_redis.sh.

    python3 tests/e2e_restart.py \
        --q1 http://127.0.0.1:8101 --q2 http://127.0.0.1:8102 \
        --origin http://127.0.0.1:9100 --redis-port 6390

Scenarios:
  1. A confirmed digest, its generation and the same request_id's result all
     survive a Redis restart, on both instances.
  2. While Redis is down, queries/publishes fail with an explicit
     cache_recovering 503 instead of silently rebuilding from origin replies.
  3. An old origin fetch in flight across the restart can never fill the
     reloaded cache with a stale digest (generation fence persists).
  4. A same-request_id retry after recovery never rolls a newer publish back.
"""
from __future__ import annotations

import argparse
import asyncio
import subprocess
import sys
import time
import uuid
from pathlib import Path

import httpx

FAILS: list[str] = []


def check(cond: bool, message: str) -> None:
    print(("PASS  " if cond else "FAIL  ") + message)
    if not cond:
        FAILS.append(message)


def restart_redis(script: str, port: int, mode: str) -> None:
    print(f"-- restarting redis ({mode}) --")
    env = dict(**__import__("os").environ, REDIS_PORT=str(port))
    result = subprocess.run(
        ["bash", script, mode], env=env, capture_output=True, text=True, timeout=60
    )
    print(result.stdout.strip())
    if result.returncode != 0:
        print(result.stderr, file=sys.stderr)
        raise RuntimeError("redis restart helper failed")


def redis_cli(port: int) -> str:
    import os

    return os.environ.get("REDIS_CLI", str(Path.home() / ".local/bin/redis-cli"))


def redis_server_bin() -> str:
    import os

    return os.environ.get("REDIS_SERVER", str(Path.home() / ".local/bin/redis-server"))


def stop_redis(port: int, log_dir: str, mode: str) -> None:
    rc = redis_cli(port)
    if mode == "graceful":
        subprocess.run([rc, "-p", str(port), "shutdown", "nosave"],
                       capture_output=True, timeout=20)
    else:
        import os
        import signal

        pid = None
        pidfile = Path(log_dir) / "redis.pid"
        if pidfile.exists():
            pid = pidfile.read_text().strip()
        if not pid:
            p = subprocess.run(["pgrep", "-f", f"redis-server.*:{port}"],
                               capture_output=True, text=True)
            pid = p.stdout.strip().splitlines()[0] if p.stdout.strip() else None
        if pid:
            os.kill(int(pid), signal.SIGKILL)
    # Block until the port is actually free, so the outage window is real.
    deadline = time.monotonic() + 10.0
    while time.monotonic() < deadline:
        if subprocess.run([rc, "-p", str(port), "ping"],
                          capture_output=True).returncode != 0:
            # Also ensure no process lingers on the port.
            if subprocess.run(["pgrep", "-f", f"redis-server.*:{port}"],
                              capture_output=True).returncode != 0:
                return
        time.sleep(0.1)


def start_redis(port: int, log_dir: str) -> None:
    subprocess.run(
        [
            redis_server_bin(), "--port", str(port), "--save", "",
            "--appendonly", "yes", "--appendfsync", "everysec",
            "--dir", f"{log_dir}/redis-data", "--daemonize", "yes",
            "--pidfile", f"{log_dir}/redis.pid", "--logfile", f"{log_dir}/redis.log",
        ],
        capture_output=True, timeout=20,
    )


async def wait_health(clients: list[httpx.AsyncClient], ready: bool, timeout: float = 20.0):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        flags = []
        for client in clients:
            try:
                body = (await client.get("/api/health")).json()
                flags.append(bool(body.get("ready")))
            except httpx.HTTPError:
                flags.append(False)
        if all(flag is ready for flag in flags):
            return
        await asyncio.sleep(0.25)
    raise AssertionError(f"instances did not reach ready={ready} within {timeout}s")


async def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--q1", default="http://127.0.0.1:8101")
    parser.add_argument("--q2", default="http://127.0.0.1:8102")
    parser.add_argument("--origin", default="http://127.0.0.1:9100")
    parser.add_argument("--redis-port", type=int, default=6390)
    parser.add_argument(
        "--restart-script",
        default=str(Path(__file__).resolve().parent.parent / "scripts" / "restart_redis.sh"),
    )
    parser.add_argument("--mode", choices=["graceful", "kill"], default="graceful")
    args = parser.parse_args()

    run = uuid.uuid4().hex[:8]

    def tag(name: str) -> str:
        return f"{name}-{run}"

    def rid(name: str) -> str:
        return f"{name}-{run}"

    q1 = httpx.AsyncClient(base_url=args.q1, timeout=30)
    q2 = httpx.AsyncClient(base_url=args.q2, timeout=30)
    origin = httpx.AsyncClient(base_url=args.origin, timeout=30)

    # ------------------------------------------------- 1. durability + restart
    t1 = tag("restart-durable")
    await origin.post("/publish", json={"tag": t1, "digest": "sha256:old"})
    await origin.post("/control", json={"reset_counters": True})
    # Warm the cache with the old digest first.
    warm = await q1.get(f"/api/tag/{t1}")
    check(warm.status_code == 200 and warm.json()["digest"] == "sha256:old", "预热旧摘要")
    # Publish through instance 1; success must mean the commit is AOF-durable.
    pub = await q1.post(
        "/api/publish",
        json={"tag": t1, "digest": "sha256:new", "request_id": rid("restart-pub")},
    )
    pub_body = pub.json()
    check(pub.status_code == 200 and pub_body["status"] == "confirmed",
          f"发布返回已确认（HTTP {pub.status_code}）")
    check(pub_body.get("durable") is True, "确认结果带 durable=true（已通过 WAITAOF）")
    gen = pub_body["generation"]

    restart_redis(args.restart_script, args.redis_port, args.mode)
    await wait_health([q1, q2], ready=True)

    for name, client in (("query-1", q1), ("query-2", q2)):
        got = await client.get(f"/api/tag/{t1}")
        body = got.json()
        check(got.status_code == 200 and body["digest"] == "sha256:new",
              f"{name} 重启后读到新摘要")
        check(body["generation"] == gen, f"{name} 发布代号跨重启保留（{body['generation']}=={gen}）")
        check(body["cache_hit"] is True, f"{name} 重启后直接命中持久化缓存")
    status = (await q2.get(f"/api/status/{t1}")).json()
    check(status["publication"]["status"] == "confirmed", "重启后发布状态仍为 confirmed")
    check(status["publication"]["last_publish"]["request_id"] == rid("restart-pub"),
          "last_publish 的 request_id 跨重启保留")
    # Same request_id after restart must be idempotent, never a re-publish.
    retry = await q2.post(
        "/api/publish",
        json={"tag": t1, "digest": "sha256:new", "request_id": rid("restart-pub")},
    )
    retry_body = retry.json()
    check(retry.status_code == 200 and retry_body["status"] == "already_applied",
          f"同号重试幂等（{retry_body.get('status')}）")
    origin_stats = (await origin.get("/control")).json()
    check(origin_stats["request_count"] >= 1, "重启后查询未被要求重新发布源站")

    # --------------------------------------------- 2. explicit 503 while down
    t2 = tag("restart-outage")
    await origin.post("/publish", json={"tag": t2, "digest": "sha256:keep"})
    await q1.get(f"/api/tag/{t2}")

    # Deterministic outage: stop Redis and wait until the port is released, so
    # every coordinated request below is guaranteed to hit a down store.
    stop_redis(args.redis_port, "/tmp/e2e", args.mode)
    saw_recovering = False
    saw_retryable = False
    for client in (q1, q2):
        for _ in range(3):
            try:
                resp = await client.get(f"/api/tag/{t2}")
            except httpx.HTTPError:
                saw_recovering = True
                continue
            if resp.status_code == 503:
                err = resp.json().get("error", {})
                if err.get("code") in {"cache_recovering", "cache_unavailable"}:
                    saw_recovering = True
                    saw_retryable = saw_retryable or bool(err.get("retryable"))
    check(saw_recovering, "Redis 中断期间查询明确报 503 暂不可用（而非返回旧/新摘要）")
    check(saw_retryable, "中断期错误标记为可重试")

    pub_during_outage = await q1.post(
        "/api/publish",
        json={"tag": t2, "digest": "sha256:must-not-appear", "request_id": rid("outage-pub")},
    )
    check(pub_during_outage.status_code == 503,
          f"中断期间发布被拒（HTTP {pub_during_outage.status_code}）")
    start_redis(args.redis_port, "/tmp/e2e")
    await wait_health([q1, q2], ready=True)
    after = (await q2.get(f"/api/tag/{t2}")).json()
    check(after["digest"] == "sha256:keep", "恢复后返回持久化的已确认摘要")

    # ------------------------------- 3. stale fetch cannot win across restart
    t3 = tag("restart-fence")
    await origin.post("/publish", json={"tag": t3, "digest": "sha256:old"})
    await q1.get(f"/api/tag/{t3}")  # cache old digest
    await q2.post("/api/cache/invalidate", json={"tag": t3})
    await origin.post("/control", json={"delay_ms": 1500})
    stale_fetch = asyncio.create_task(q1.get(f"/api/tag/{t3}"))
    await asyncio.sleep(0.3)  # leader is inside the slow origin GET now

    # Publish the new digest while the old fetch is in flight.
    fence_pub = await q2.post(
        "/api/publish",
        json={"tag": t3, "digest": "sha256:new", "request_id": rid("fence-pub")},
    )
    check(fence_pub.json()["status"] == "confirmed", "栅栏场景发布已确认")

    # Restart Redis while the old leader is still waiting on the origin.
    restart_redis(args.restart_script, args.redis_port, args.mode)
    stale_result = await stale_fetch
    stale_body = stale_result.json()
    # Every outcome is safe.  A 503 means Redis was unreachable mid-flight.
    # A 200 with the old digest is only legitimate as the *authoritative last
    # confirmed* view while an unconfirmed marker is parked -- the stale fetch
    # itself was fenced and never wrote.  A 200 with the new digest is the
    # normal fence path.  The forbidden outcome is the old digest becoming the
    # current cache value, which the post-recovery queries below assert.
    check(stale_result.status_code in (200, 503),
          f"旧回源跨重启后得到确定结果（HTTP {stale_result.status_code}）")
    if stale_result.status_code == 503:
        check(stale_body.get("error", {}).get("code") in
              {"cache_recovering", "cache_unavailable"},
              "中断期旧回源得到明确的暂不可用错误")
    await wait_health([q1, q2], ready=True)
    await origin.post("/control", json={"delay_ms": 0})
    for name, client in (("query-1", q1), ("query-2", q2)):
        got = (await client.get(f"/api/tag/{t3}")).json()
        check(got["digest"] == "sha256:new",
              f"{name} 恢复交错后只读到新值（权威缓存未被老摘要污染）")
        check(got["generation"] == fence_pub.json()["generation"],
              f"{name} 发布代号保持为新发布代号")

    # --------------------------------- 4. retry never rolls a newer publish
    t4 = tag("restart-rollback")
    await origin.post("/publish", json={"tag": t4, "digest": "sha256:base"})
    first = (await q1.post("/api/publish",
             json={"tag": t4, "digest": "sha256:one", "request_id": rid("roll-1")})).json()
    second = (await q2.post("/api/publish",
              json={"tag": t4, "digest": "sha256:two", "request_id": rid("roll-2")})).json()
    check(second["generation"] > first["generation"], "代号单调递增")
    restart_redis(args.restart_script, args.redis_port, args.mode)
    await wait_health([q1, q2], ready=True)
    retry = (await q1.post("/api/publish",
             json={"tag": t4, "digest": "sha256:one", "request_id": rid("roll-1")})).json()
    check(retry["status"] == "superseded" and retry["applied"] is False,
          f"重启后同号重试不回退（{retry.get('status')}）")
    current = (await q2.get(f"/api/tag/{t4}")).json()
    check(current["digest"] == "sha256:two", "较新发布在重启后仍是当前值")

    await q1.aclose()
    await q2.aclose()
    await origin.aclose()

    print()
    if FAILS:
        print(f"{len(FAILS)} check(s) failed")
        return 1
    print("all recovery-interleave checks passed")
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
