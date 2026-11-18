#!/usr/bin/env bash
# Restart the Redis used by the local (non-Docker) demo stack without losing
# its AOF data.  The query instances and origin keep running; their readiness
# monitors flip to "recovering" while Redis is down / replaying the AOF and
# back to ready afterwards.
set -uo pipefail

REDIS_PORT="${REDIS_PORT:-6390}"
LOG_DIR="${LOG_DIR:-/tmp/e2e}"
REDIS_CLI="${REDIS_CLI:-$HOME/.local/bin/redis-cli}"
REDIS_SERVER="${REDIS_SERVER:-$HOME/.local/bin/redis-server}"

mode="${1:-graceful}"
case "$mode" in
  graceful)
    echo "graceful restart: SHUTDOWN (AOF is fsync'd on shutdown)"
    "$REDIS_CLI" -p "$REDIS_PORT" shutdown nosave || true
    ;;
  kill)
    pid="$(cat "$LOG_DIR/redis.pid" 2>/dev/null || true)"
    if [ -z "$pid" ]; then
      pid="$(pgrep -f "redis-server.*:$REDIS_PORT" | head -1 || true)"
    fi
    echo "hard restart: SIGKILL pid=${pid:-unknown} (relies on everysec AOF)"
    [ -n "$pid" ] && kill -9 "$pid" || true
    ;;
  *)
    echo "usage: $0 [graceful|kill]" >&2
    exit 2
    ;;
esac

for _ in $(seq 1 30); do
  if ! "$REDIS_CLI" -p "$REDIS_PORT" ping >/dev/null 2>&1; then sleep 0.2; else break; fi
done

# Wait until the old process has fully released the port AND the AOF files.
# Starting the replacement while a dying server still holds them would make
# the two instances race on the AOF manifest and lose confirmed data.
for _ in $(seq 1 50); do
  if ! pgrep -f "redis-server.*:$REDIS_PORT" >/dev/null 2>&1; then
    break
  fi
  sleep 0.1
done
if pgrep -f "redis-server.*:$REDIS_PORT" >/dev/null 2>&1; then
  echo "old redis still holding port $REDIS_PORT; aborting restart" >&2
  exit 1
fi

"$REDIS_SERVER" --port "$REDIS_PORT" --save '' \
  --appendonly yes --appendfsync everysec \
  --dir "$LOG_DIR/redis-data" \
  --daemonize yes --pidfile "$LOG_DIR/redis.pid" --logfile "$LOG_DIR/redis.log"

for _ in $(seq 1 30); do
  if "$REDIS_CLI" -p "$REDIS_PORT" ping >/dev/null 2>&1; then
    echo "redis is back on port $REDIS_PORT (AOF reloaded)"
    exit 0
  fi
  sleep 0.2
done
echo "redis did not come back; see $LOG_DIR/redis.log" >&2
exit 1
