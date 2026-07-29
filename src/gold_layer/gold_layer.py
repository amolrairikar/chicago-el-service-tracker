"""Lambda that curates the CTA "El" **gold layer** ``total_trains`` table.

The silver layer writes one row per deduplicated train *observation* (a repeated
prediction snapshot) as date-partitioned Parquet under
``silver/date=YYYY-MM-DD/<route>.parquet``. A single physical train — identified
by its ``run_number`` (a vehicle+operator pairing, unique per line within a
service day and recycled the next day) — produces many silver rows, so the count
of *trains* is the count of distinct run numbers, not rows.

This function runs once a day and publishes browser-friendly JSON to the
``gold/total_trains/`` prefix (served to the React dashboard through CloudFront):

* ``daily.json``  — distinct ``run_number`` per line per service day. This file
  is both the delivered artifact and the canonical fact the rollups are summed
  from, so history accrues here across runs.
* ``weekly.json`` / ``monthly.json`` / ``yearly.json`` — additive sums of the
  daily counts (run numbers recycle daily, so summing daily counts is the only
  correct rollup). Weeks are Sunday-anchored; months and years are calendar.

A "service day" is ``[03:00, 03:00)`` **America/Chicago** local time, applied
uniformly to every day (a 1am train belongs to the previous day's service day).
The CTA Train Tracker API — and therefore silver's ``request_timestamp`` — is
already in local Chicago time, so the window is compared as naive wall-clock time
with no timezone conversion. Because silver is partitioned by *UTC* ingest day,
one Central service day straddles two adjacent UTC partitions (``date=D`` and
``date=D+1``); both are read and then filtered by ``request_timestamp``.

Re-running a service date recomputes and overwrites just that day's rows in the
daily fact before re-deriving the rollups, so the transform is idempotent.
"""

import io
import json
import logging
import os
from collections import defaultdict
from datetime import date, datetime, time, timedelta, timezone

import boto3
import pyarrow.parquet as pq
from botocore.exceptions import ClientError

logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3_client = boto3.client("s3")
cloudfront_client = boto3.client("cloudfront")

# The service day runs 03:00 -> 03:00 America/Chicago local time. request_timestamp
# (from the CTA API's ``tmst``) is already Chicago-local, so this bound is compared
# as naive wall-clock time.
SERVICE_DAY_START_HOUR = 3

# Only these silver columns are needed to count distinct trains per line/day.
SILVER_COLUMNS = ["route", "run_number", "request_timestamp"]

# gold/ layout. CloudFront's origin_path is ``/gold``, so ``gold/total_trains/x``
# is served at ``/total_trains/x`` and invalidated with that same path.
GOLD_NAMESPACE = "total_trains"
GOLD_PREFIX = f"gold/{GOLD_NAMESPACE}"
DAILY_KEY = f"{GOLD_PREFIX}/daily.json"
WEEKLY_KEY = f"{GOLD_PREFIX}/weekly.json"
MONTHLY_KEY = f"{GOLD_PREFIX}/monthly.json"
YEARLY_KEY = f"{GOLD_PREFIX}/yearly.json"
INVALIDATION_PATH = f"/{GOLD_NAMESPACE}/*"


def resolve_service_date(event):
    """Return the Central service date to process as a ``date``.

    Defaults to two UTC days ago: a service day's later half lands in the *next*
    UTC silver partition, which the silver transform only writes the following
    01:00 UTC, so the newest fully-available service day is ~2 UTC days back. An
    ``event["service_date"]`` override (``YYYY-MM-DD``) lets a backfill target an
    arbitrary day.
    """
    override = event.get("service_date") if event else None
    if override:
        return datetime.strptime(override, "%Y-%m-%d").date()
    return datetime.now(timezone.utc).date() - timedelta(days=2)


def service_day_window(service_date):
    """Return the naive ``[start, end)`` Central wall-clock bounds for a service day."""
    start = datetime.combine(service_date, time(hour=SERVICE_DAY_START_HOUR))
    return start, start + timedelta(days=1)


def silver_partition_dates(service_date):
    """Return the UTC silver partition dates that cover a Central service day.

    Central ``[D 03:00, D+1 03:00)`` maps to UTC ``[D 08:00, D+1 08:00)`` (±1h for
    DST), which always falls within UTC calendar days ``D`` and ``D+1``.
    """
    return [service_date, service_date + timedelta(days=1)]


def list_objects(bucket, prefix):
    """Return every object key under ``prefix`` via a paginated listing."""
    paginator = s3_client.get_paginator("list_objects_v2")
    keys = []
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            keys.append(obj["Key"])
    return keys


def read_silver_rows(bucket, key):
    """Read the needed columns of one silver Parquet object as a list of dicts."""
    response = s3_client.get_object(Bucket=bucket, Key=key)
    table = pq.read_table(io.BytesIO(response["Body"].read()), columns=SILVER_COLUMNS)
    return table.to_pylist()


def parse_request_timestamp(value):
    """Parse silver's ISO-8601 ``request_timestamp`` to a naive local datetime.

    The value is Chicago-local wall-clock time; any offset (defensive) is dropped
    so the comparison against the service-day window stays wall-clock. Unparseable
    or missing values return ``None`` so the row is skipped.
    """
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError:
        return None
    if parsed.tzinfo is not None:
        parsed = parsed.replace(tzinfo=None)
    return parsed


def compute_daily_counts(rows, window_start, window_end):
    """Count distinct non-null ``run_number`` per route within the service window."""
    runs_by_route = defaultdict(set)
    for row in rows:
        timestamp = parse_request_timestamp(row.get("request_timestamp"))
        if timestamp is None or not (window_start <= timestamp < window_end):
            continue
        route = row.get("route")
        run_number = row.get("run_number")
        if route and run_number:
            runs_by_route[route].add(run_number)
    return {route: len(runs) for route, runs in runs_by_route.items()}


def read_daily_records(bucket):
    """Return the persisted daily counts (``counts`` list), or ``[]`` if absent."""
    try:
        response = s3_client.get_object(Bucket=bucket, Key=DAILY_KEY)
    except ClientError as error:
        if error.response["Error"]["Code"] in ("NoSuchKey", "404"):
            return []
        raise
    return json.loads(response["Body"].read()).get("counts", [])


def upsert_daily(existing, service_date_str, daily_counts):
    """Replace ``service_date_str``'s rows in the daily fact with fresh counts."""
    records = [r for r in existing if r["service_date"] != service_date_str]
    for route, total in daily_counts.items():
        records.append({"service_date": service_date_str, "route": route, "total_trains": total})
    records.sort(key=lambda r: (r["service_date"], r["route"]))
    return records


def week_start(day):
    """Return the Sunday that begins the week containing ``day``."""
    return day - timedelta(days=(day.weekday() + 1) % 7)


def rollup(daily_records, bucket_key, field_name):
    """Sum daily ``total_trains`` per ``(bucket_key(service_date), route)``.

    ``bucket_key`` maps a ``date`` to the period key (e.g. the Sunday week start),
    stored under ``field_name`` in each output record.
    """
    totals = defaultdict(int)
    for record in daily_records:
        day = date.fromisoformat(record["service_date"])
        totals[(bucket_key(day), record["route"])] += record["total_trains"]
    rolled = [
        {field_name: bucket, "route": route, "total_trains": total}
        for (bucket, route), total in totals.items()
    ]
    rolled.sort(key=lambda r: (r[field_name], r["route"]))
    return rolled


def _document(generated_at, grain, counts):
    """Wrap a counts list in the delivered JSON envelope."""
    return {"generated_at": generated_at, "grain": grain, "counts": counts}


def write_json(bucket, key, payload):
    """Serialize ``payload`` to compact JSON and put it to the gold prefix."""
    body = json.dumps(payload, separators=(",", ":")).encode("utf-8")
    s3_client.put_object(Bucket=bucket, Key=key, Body=body, ContentType="application/json")


def invalidate_cache(distribution_id):
    """Invalidate the gold namespace on CloudFront so the dashboard sees the refresh."""
    if not distribution_id:
        logger.info("No CloudFront distribution configured; skipping invalidation.")
        return False
    cloudfront_client.create_invalidation(
        DistributionId=distribution_id,
        InvalidationBatch={
            "Paths": {"Quantity": 1, "Items": [INVALIDATION_PATH]},
            "CallerReference": f"gold-total-trains-{datetime.now(timezone.utc).isoformat()}",
        },
    )
    return True


def handler(event, context):
    """Lambda entrypoint: count one service day's trains and republish the rollups."""
    bucket = os.environ["DATA_BUCKET"]
    distribution_id = os.environ.get("CLOUDFRONT_DISTRIBUTION_ID")

    service_date = resolve_service_date(event)
    service_date_str = service_date.isoformat()
    window_start, window_end = service_day_window(service_date)

    rows = []
    for partition_date in silver_partition_dates(service_date):
        prefix = f"silver/date={partition_date.isoformat()}/"
        for key in list_objects(bucket, prefix):
            rows.extend(read_silver_rows(bucket, key))
    logger.info("Read %d silver observation(s) for service day %s", len(rows), service_date_str)

    daily_counts = compute_daily_counts(rows, window_start, window_end)

    daily_records = upsert_daily(read_daily_records(bucket), service_date_str, daily_counts)
    weekly = rollup(daily_records, lambda d: week_start(d).isoformat(), "week_start")
    monthly = rollup(daily_records, lambda d: f"{d.year:04d}-{d.month:02d}", "month")
    yearly = rollup(daily_records, lambda d: d.year, "year")

    generated_at = datetime.now(timezone.utc).isoformat()
    write_json(bucket, DAILY_KEY, _document(generated_at, "day", daily_records))
    write_json(bucket, WEEKLY_KEY, _document(generated_at, "week", weekly))
    write_json(bucket, MONTHLY_KEY, _document(generated_at, "month", monthly))
    write_json(bucket, YEARLY_KEY, _document(generated_at, "year", yearly))

    invalidated = invalidate_cache(distribution_id)

    logger.info(
        "Published total_trains for %s: %d line(s), %d day(s) in the daily fact",
        service_date_str,
        len(daily_counts),
        len(daily_records),
    )

    return {
        "statusCode": 200,
        "body": json.dumps(
            {
                "service_date": service_date_str,
                "observations_read": len(rows),
                "daily_counts": daily_counts,
                "daily_records": len(daily_records),
                "cache_invalidated": invalidated,
            }
        ),
    }
