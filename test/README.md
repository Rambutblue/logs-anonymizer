# End-to-end test

Sends a known, finite set of logs through Kafka and the anonymizer into ClickHouse, then checks what was stored.

```bash
./run.sh                            # normal run
./run.sh --chaos                    # randomly kill anonymizer, ClickHouse and Kafka while logs are flowing
./run.sh --chaos=kafka,anonymizer   # only kill the listed services
```

Optional env vars: `AMOUNT` (default 20000), `RATE` (records/s, default 500), `PARTITIONS` (3),
`LOG_BATCH_SIZE` (100), `LOG_BATCH_MAX_TIME` (seconds, 1), `CHAOS_EVENTS` (max number of kills, 4).

Example: `AMOUNT=6000 RATE=200 ./run.sh --chaos`

The script exits with 0 if the check passed. Requires Docker.
