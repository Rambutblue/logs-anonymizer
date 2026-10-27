#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")"

CHAOS=0
case "${1:-}" in
    "") ;;
    --chaos) CHAOS=1 ;;
    --chaos=*)
        CHAOS=1
        export CHAOS_TARGETS=${1#--chaos=}
        for target in ${CHAOS_TARGETS//,/ }; do
            case $target in
                anonymizer|clickhouse|kafka) ;;
                *) echo "unknown chaos target: $target" >&2; exit 2 ;;
            esac
        done ;;
    *) echo "usage: $0 [--chaos[=anonymizer,clickhouse,kafka]]" >&2; exit 2 ;;
esac
export CHAOS

chaos_pid=""
cleanup() {
    [ -n "$chaos_pid" ] && kill "$chaos_pid" 2> /dev/null || true
    docker compose down -v --remove-orphans > /dev/null 2>&1
}
trap cleanup EXIT

docker compose down -v --remove-orphans > /dev/null 2>&1
docker compose build
docker compose up -d kafka clickhouse anonymizer producer

if [ "$CHAOS" = 1 ]; then
    ./chaos.sh &
    chaos_pid=$!
fi

status=0
docker compose run --rm checker || status=$?

[ -n "$chaos_pid" ] && wait "$chaos_pid" || true

if [ "$status" != 0 ]; then
    echo "--- last anonymizer logs"
    docker compose logs --tail 30 anonymizer
fi
exit "$status"
