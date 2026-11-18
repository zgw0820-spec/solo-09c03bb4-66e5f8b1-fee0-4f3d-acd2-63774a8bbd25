# 制品标签摘要查询（跨实例单飞缓存 · 重启可恢复）

FastAPI + Redis 的制品标签查询服务：两个查询实例共享同一份 Redis 缓存，对同一个标签的一轮并发缓存未命中只会向源站回源一次；发布确认之后开始的查询一定能看到新摘要，晚到的旧回源结果不会覆盖它。Redis 启用 AOF 持久化，已确认发布在 Redis 崩溃/重启后仍然保留，恢复交错期间旧回源无法越过发布栅栏，恢复未完成时请求明确返回 503 暂不可用。

## 组成

| 服务 | 说明 | 端口 |
| --- | --- | --- |
| `redis` | 缓存与协调状态，**AOF 持久化**（命名卷 `redis-aof`） | 6379 |
| `origin` | 可设置响应延迟/失败的本地制品源站，自行统计请求数 | 9000 |
| `query-1` / `query-2` | 两个 FastAPI 查询实例（同一镜像） | 8001 / 8002 |

网页由任一查询实例直接提供：<http://localhost:8001/>（或 <http://localhost:8002/>）。

## 启动与重启演示

```bash
docker compose up --build        # Redis 以 appendonly yes + 每秒 fsync 启动
```

首次启动后源站预置了 `v1.0.0=sha256:old1111`。

演示 Redis 崩溃恢复（网页发布与查询保持不变，重启期间页面健康点变为“缓存恢复中”）：

```bash
# 1) 在网页或 API 上确认一次发布（仅当 AOF fsync 完成才返回 confirmed）
# 2) 单独重启 Redis（两个查询实例与源站不动）
docker compose restart redis
# 3) Redis 重载 AOF 后，两实例查询仍返回重启前确认的摘要与发布代号
#    重启进行中的请求得到 503 cache_recovering，而不是被旧回源重写的缓存
```

本地（非 Docker）栈用 `scripts/dev_stack.sh` 起服务，`scripts/restart_redis.sh [graceful|kill]`
重启 Redis：`graceful` 走 `SHUTDOWN`（停机时 fsync AOF），`kill` 直接 `SIGKILL`
（依赖 `everysec` AOF，模拟崩溃）。

## 网页功能

* **发布标签摘要**：选择经由哪个实例发布，填写标签、新摘要和 `request_id`。发布 = 写源站 + 原子写缓存并递增发布代号；同一 `request_id` 重试是幂等的。
* **查询与并发测试**：「两实例并发查询」会同时向两个实例各发 N 个请求，并汇总请求数、实际回源数、等待者数、缓存命中数与摘要一致性。「仅使缓存失效」只把缓存标记为过期（保留发布代号），用来在不动源站的前提下制造一轮并发未命中——随后并发查询时源站计数应只增加 1。
* **源站模拟**：设置响应延迟、接下来 N 次查询失败。
* **发布状态**：查看已确认摘要、发布代号、源站版本、在途发布。
* **统计**：源站的 `request_count` 由源站自己统计，是“回源次数”的权威依据；网关侧另有各实例的缓存命中/未命中/等待/回源调用计数。

## 一致性设计

所有协调状态在 Redis 中，键前缀 `artifact:{tag}:`：

| 键 | 用途 |
| --- | --- |
| `state` | 已确认摘要（`confirmed_digest`/`confirmed_gen` 权威快照 + 候选 `digest`/`gen`）、`origin_revision`、`fetch_seq`、`result_seq`、`fresh_until` |
| `lock` | 跨实例单飞回源锁（UUID 值 + TTL） |
| `fetch_seq` | 回源写缓存的单调序号 |
| `result_seq` | 状态写入与错误记录共享的结果序号，供等待者判断“本轮是否有结果” |
| `fetch_error` | 最近一次失败回源，含结果序号，等待者据此得到同样的明确错误 |
| `pending` | 在途发布（短 TTL） |
| `pub_unconfirmed` | 结果未知的发布标记（源站可能已写入，网关未确认/未拿到持久化确认），读者会看到“旧摘要 + 明确标记” |
| `pub_seq` / `pub_lock` | 发布会话代号 / 发布串行锁 |
| `last_publish` | 最近一次已确认发布 |
| `publish_request:{tag}:{request_id}` | 发布幂等记录，随提交一起 AOF 持久化，重启后同号重试仍然幂等 |

关键保证：

1. **单飞**：同轮未命中的请求竞争 Redis 锁，只有 1 个成为 leader 回源，其余等待 leader 写入的 `state` 或 `fetch_error`，不会各自回源。leader 拿到锁后会再检查一次缓存，避免“leader 刚释放锁、等待者补位”导致重复回源。
2. **发布栅栏**：leader 在调用源站**之前**记录当前发布代号；写缓存用 Lua 原子校验代号未变，且 `fetch_seq` 单调。因此旧回源晚到只会被拒绝，不会覆盖新发布的摘要。源站版本号只用于展示，不参与栅栏——演示源站重启后版本号会归零，若拿它当栅栏会让缓存永久写不进去。
3. **确认后可见**：发布提交用 Lua 原子完成“校验代号单调 → 写 `state` → 写 `last_publish` → 清在途标记 → 递增 `result_seq`”，发布确认后开始的查询直接命中新摘要。
4. **未确认不冒充新结果**：源站发布超时/网关提交失败时保留 `pub_unconfirmed`；读者仍能拿到最后已确认摘要，但响应带 `stale_due_to_pending: true`、`result_kind: "last_confirmed_while_publishing"`，发布状态显示 `publishing`。
5. **重试不回退**：发布代号单调，且 `request_id` 有幂等记录；对旧发布的重试返回 `superseded` 而不是把较新摘要回滚。
6. **明确错误**：源站超时 → `504 origin_timeout`；源站 5xx → `502 origin_error`；Redis 不可用/协调失败 → `503 cache_unavailable`；Redis 仍在重载 AOF 或实例尚未就绪 → `503 cache_recovering`；持久化确认失败 → `503 durability_unconfirmed`；等待超时 → `504 cache_wait_timeout`；被并发写入拒绝 → `503 cache_write_rejected`。均可重试（非 404 标签不存在），等待者与 leader 得到同一错误，之后可以再查。
7. **跨 Redis 重启保留（AOF）**：Redis 以 `appendonly yes`、`appendfsync everysec` 运行并挂载命名卷。已回复成功的摘要、发布代号（`gen`/`confirmed_gen`）、`last_publish` 与同一 `request_id` 的幂等结果都在 AOF 中，重启重载后两个实例只能读到这些值。
8. **持久化闸门（WAITAOF）**：发布提交 Lua 原子写入候选状态后，网关执行 `WAITAOF 1 0 <timeout>` 等待本机 AOF fsync；只有拿到确认才提升 `confirmed_*`、写幂等记录并返回 `confirmed`（响应带 `durable: true`）。拿不到确认（超时、连接断开、服务端不支持/AOF 关闭）一律返回 `503 durability_unconfirmed`，保留 `pub_unconfirmed`，**不回复发布成功**；源站可能已接收的写入按未确认处理，读者继续拿到上一份权威已确认摘要。
9. **恢复栅栏与就绪门**：实例后台每秒探测 Redis，未就绪（宕机或 AOF `LOADING`）时所有协调请求快速失败为 `503 cache_recovering`，不会借旧回源重建缓存。重启后旧回源携带的仍是重启前捕获的发布代号，而持久化的 `state` 代号已前进，写缓存 Lua 原子拒绝（`-1`），老摘要进不了（曾为空的）缓存；恢复完成后两实例都只读到新值。

## API

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| GET | `/api/tag/{tag}?refresh=1` | 查询摘要（`refresh` 强制绕过新鲜度检查，仍走单飞） |
| POST | `/api/publish` | `{tag, digest, request_id?}`，发布并确认 |
| GET | `/api/status/{tag}` | 发布状态、已确认摘要、代号、在途发布 |
| POST | `/api/cache/invalidate` | `{tag}`，只让已有缓存过期，改源站与发布代号 |
| GET | `/api/stats` | 网关计数 + 源站计数 |
| POST | `/api/origin/control` | 设置源站 `delay_ms` / `fail_next` / `reset_counters` 等 |
| GET | `/api/health`, `/api/config` | 健康检查（含 `ready`/`recovering` 就绪状态）与实例配置 |

## 配置（环境变量）

| 变量 | 默认 | 说明 |
| --- | --- | --- |
| `INSTANCE_NAME` | `query` | 实例名 |
| `REDIS_URL` | `redis://localhost:6379/0` | Redis 地址 |
| `ORIGIN_URL` | `http://localhost:9000` | 源站地址 |
| `PUBLIC_INSTANCES` | `http://localhost:8001,http://localhost:8002` | 网页可直连的实例列表（也用于 CORS） |
| `ORIGIN_TIMEOUT_MS` | `2000` | 源站查询超时 |
| `CACHE_WAIT_TIMEOUT_MS` | `3500` | 等待 leader 结果的上限 |
| `LOCK_TTL_MS` | `12000` | 单飞锁 TTL |
| `PUBLISH_TIMEOUT_MS` | `5000` | 源站发布确认超时 |
| `PENDING_TTL_MS` | `15000` | 在途发布标记 TTL |
| `UNCONFIRMED_TTL_S` | `600` | 结果未知的发布标记 TTL |
| `REVALIDATE_TTL_S` | `300` | 缓存新鲜期 |
| `REDIS_SOCKET_TIMEOUT_MS` | `1500` | Redis 操作超时（超时即报协调失败） |
| `DURABILITY_ACK_ENABLED` | `true` | 是否要求发布提交后等待 AOF 持久化确认（`WAITAOF`）；关闭后发布不再等持久化确认，仅用于无 AOF 的测试环境 |
| `AOF_WAIT_TIMEOUT_MS` | `2000` | 等待本机 AOF fsync 的上限，超时按未确认处理 |

## 测试

```bash
# 1) 单元/交错测试（fakeredis，无需外部服务；DURABILITY_ACK 在其中默认关闭，
#    持久化闸门由专门用例注入 WAITAOF 失败来覆盖）
pip install -r requirements.txt fakeredis pytest pytest-asyncio
python3 -m pytest tests/test_cache.py

# 2a) Docker 栈
docker compose up --build -d
python3 tests/e2e_check.py --q1 http://localhost:8001 --q2 http://localhost:8002 --origin http://localhost:9000 --redis-port 6379
# 重启恢复交错（需要栈内 Redis，可重复执行；graceful=SHUTDOWN / kill=SIGKILL）
python3 tests/e2e_restart.py --q1 http://localhost:8001 --q2 http://localhost:8002 \
  --origin http://localhost:9000 --redis-port 6379 --mode graceful
```

没有 Docker 时可用本地栈（需要本机 `redis-server` 7.2+（支持 `WAITAOF`）、`uvicorn`）：

```bash
scripts/dev_stack.sh     # 起 origin + query-1 + query-2（8101/8102/9100，Redis 6390，AOF 开）
python3 tests/e2e_check.py
scripts/restart_redis.sh graceful        # 或 kill
python3 tests/e2e_restart.py
```

`tests/e2e_check.py` 覆盖：16 并发请求单飞仅回源 1 次、缓存命中不回源、旧回源被发布栅栏拦截、确认后两实例都看到新摘要、重试旧发布不回退、发布失败/超时明确报错且未确认标记生效、源站超时 504 后可恢复、含 `/` 的标签、非法输入 422。

`tests/e2e_restart.py` 覆盖**真实 Redis 重启**恢复交错：已确认摘要/代号/同 `request_id` 结果跨重启保留且两实例一致、中断期查询与发布明确报 503、跨重启在途的旧回源不能把老摘要写入重载后的缓存、重启后同号重试返回 `superseded` 不回退较新发布。

`tests/test_cache.py` 另外用 fakeredis 覆盖单元级交错：等待者优先返回“发布后”的新状态而不是上一轮错误、发布提交失败后保留未确认标记并可重试恢复、`WAITAOF` 失败/超时不回复成功且保留未确认标记、就绪门与 AOF `LOADING` 报 `cache_recovering`、模拟 AOF 重载后旧回源被持久化栅栏拒绝、源站重启导致版本号倒退后仍可写入、request_id 复用校验等。
