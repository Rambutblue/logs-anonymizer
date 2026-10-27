"""Waits until ClickHouse holds every record the producer sent, then checks what is actually stored.

Always fails on: lost records, unexpected records, wrong field values, non-anonymized IPs.
Without CHAOS also fails on duplicate rows and on materialized view totals that differ from AMOUNT.
With CHAOS those two are only reported: a kill between insert and commit can legitimately cause them.
"""
import os
import sys
import time

import clickhouse_connect

from common import AMOUNT, expected_row

HOST = os.environ.get("CLICKHOUSE_HOST", "clickhouse")
CHAOS = os.environ.get("CHAOS", "0") == "1"
WAIT_TIMEOUT = int(os.environ.get("WAIT_TIMEOUT", "300"))
GRACE_SECONDS = 5

ROWS_SQL = """
    SELECT resource_id, toUnixTimestamp64Milli(timestamp), bytes_sent, request_time_milli,
           response_status, cache_status, method, remote_addr, url
    FROM http_log FINAL
    ORDER BY resource_id
"""


def query(sql):
    """Runs a query, retrying while ClickHouse is unreachable (the chaos script may have killed it)."""
    while True:
        try:
            return clickhouse_connect.get_client(host=HOST).query(sql).result_rows
        except Exception as error:
            print(f"clickhouse not available ({type(error).__name__}), retrying", flush=True)
            time.sleep(2)


def wait_for_all_records():
    deadline = time.monotonic() + WAIT_TIMEOUT
    while time.monotonic() < deadline:
        stored = query("SELECT uniqExact(resource_id) FROM http_log")[0][0]
        print(f"stored {stored}/{AMOUNT} distinct records", flush=True)
        if stored >= AMOUNT:
            return
        time.sleep(3)
    print(f"timed out after {WAIT_TIMEOUT}s, the missing records are listed below", flush=True)


def main():
    wait_for_all_records()
    time.sleep(GRACE_SECONDS)  # let a late redelivery land so it is not missed

    errors = []
    warnings = []

    rows = query(ROWS_SQL)
    stored = {row[0]: row for row in rows}
    expected_ids = set(range(1, AMOUNT + 1))

    missing = sorted(expected_ids - stored.keys())
    unexpected = sorted(stored.keys() - expected_ids)
    wrong = [i for i in sorted(expected_ids & stored.keys()) if tuple(stored[i]) != expected_row(i)]
    not_anonymized = [row[0] for row in rows if not row[7].endswith(".X")]

    if missing:
        errors.append(f"{len(missing)} records lost, first ids: {missing[:10]}")
    if unexpected:
        errors.append(f"{len(unexpected)} unexpected records, first ids: {unexpected[:10]}")
    if wrong:
        errors.append(f"{len(wrong)} records with wrong values, e.g. id {wrong[0]}: "
                      f"{tuple(stored[wrong[0]])} != {expected_row(wrong[0])}")
    if not_anonymized:
        errors.append(f"{len(not_anonymized)} records with a non-anonymized IP, first ids: {not_anonymized[:10]}")

    raw_rows = query("SELECT count() FROM http_log")[0][0]
    totals = query("SELECT sum(requests) FROM http_log_totals")[0][0]
    report = warnings if CHAOS else errors
    if raw_rows != AMOUNT:
        report.append(f"http_log holds {raw_rows} rows without FINAL, expected {AMOUNT} ({raw_rows - AMOUNT:+d} duplicates)")
    if totals != AMOUNT:
        report.append(f"http_log_totals counts {totals} requests, expected {AMOUNT} ({totals - AMOUNT:+d})")

    print(f"\nsent {AMOUNT}, stored {len(stored)} distinct, {raw_rows} raw rows, totals {totals}")
    for warning in warnings:
        print(f"WARN {warning}")
    for error in errors:
        print(f"FAIL {error}")
    print("FAILED" if errors else "PASSED")
    sys.exit(1 if errors else 0)


main()
