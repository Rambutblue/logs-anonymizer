import os

AMOUNT = int(os.environ.get("AMOUNT", "20000"))
TOPIC = os.environ.get("TOPIC", "http_log")

BASE_TIMESTAMP_MILLI = 1_700_000_000_000
STATUSES = [200, 304, 404, 500]
CACHE_STATUSES = ["HIT", "MISS", "STALE", "EXPIRED"]
METHODS = ["GET", "HEAD", "POST"]


def ip_prefix(i):
    return f"10.{(i >> 8) & 255}.{(i >> 4) & 255}"


def record(i):
    """The record the producer sends for id i (1..AMOUNT). resource_id is the id."""
    return {
        "timestampEpochMilli": BASE_TIMESTAMP_MILLI + i * 10,
        "resourceId": i,
        "bytesSent": i * 7,
        "requestTimeMilli": i % 5000,
        "responseStatus": STATUSES[i % 4],
        "cacheStatus": CACHE_STATUSES[i % 4],
        "method": METHODS[i % 3],
        "remoteAddr": f"{ip_prefix(i)}.{i % 256}",
        "url": f"/test/{i}.json",
    }


def expected_row(i):
    """The row ClickHouse should hold for id i: the record with the last octet of the IP masked."""
    r = record(i)
    return (
        r["resourceId"],
        r["timestampEpochMilli"],
        r["bytesSent"],
        r["requestTimeMilli"],
        r["responseStatus"],
        r["cacheStatus"],
        r["method"],
        f"{ip_prefix(i)}.X",
        r["url"],
    )
