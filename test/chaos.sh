#!/usr/bin/env bash
# Randomly breaks the stack while the producer is running.
# CHAOS_TARGETS: comma separated subset of anonymizer,clickhouse,kafka (default: all three).
# CHAOS_EVENTS: how many times to break something at most (default 4), so the producer can still finish.
set -u
cd "$(dirname "$0")"

IFS=, read -ra targets <<< "${CHAOS_TARGETS:-anonymizer,clickhouse,kafka}"

log() { echo "[chaos $(date +%T)] $*"; }

producer_running() { docker compose ps --status running --services | grep -qx producer; }

# SIGKILL, then start again after a few seconds
outage() {
    log "killing $1"
    docker compose kill "$1" > /dev/null
    sleep $((3 + RANDOM % 6))
    log "starting $1"
    docker compose start "$1" > /dev/null
}

for ((event = 0; event < ${CHAOS_EVENTS:-4}; event++)); do
    producer_running || break
    sleep $((5 + RANDOM % 6))

    outage "${targets[RANDOM % ${#targets[@]}]}"
done

# leave everything running so the system can recover and the checker can finish
docker compose start kafka clickhouse anonymizer > /dev/null
log "stopped"
