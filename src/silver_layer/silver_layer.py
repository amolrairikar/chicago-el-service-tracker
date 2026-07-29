"""Lambda that curates the CTA "El" train-position **silver layer**.

The fetch Lambda lands raw Firehose output under the bronze prefix
``raw/YYYY/MM/DD/HH/*.gz`` (append-only, GZIP NDJSON envelopes of the shape
``{route, fetched_at, data:{ctatt:...}}``). Because we occasionally poll faster
than the CTA regenerates predictions, the bronze layer contains a few duplicate
train records — rows whose inner train object is byte-for-byte identical and
only the outer ``tmst``/``fetched_at`` changed.

This function runs once a day, reads the *previous UTC day's* bronze partition,
explodes the nested ``ctatt.route[].train[]`` arrays into one row per train
observation, drops exact duplicates (SHA-256 of the raw train dict plus route),
and writes date-partitioned Parquet to the ``silver/`` prefix as
``silver/date=YYYY-MM-DD/<route>.parquet``. Re-running a date overwrites its
output, so the transform is idempotent and self-partitioning.
"""

import gzip
import io
import json
import logging
import os
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from hashlib import sha256

import boto3
import pyarrow as pa
import pyarrow.parquet as pq

logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3_client = boto3.client("s3")

# Explicit output schema. The types are a contract the future gold layer reads
# against, so they are committed here rather than left as raw strings. Timestamps
# stay as ISO-8601 strings; numeric/boolean fields are typed, with parse failures
# falling back to null.
SCHEMA = pa.schema(
    [
        pa.field("request_timestamp", pa.string()),
        pa.field("route", pa.string()),
        pa.field("run_number", pa.string()),
        pa.field("destination_station", pa.string()),
        pa.field("destination_station_name", pa.string()),
        pa.field("train_direction", pa.string()),
        pa.field("next_station_id", pa.string()),
        pa.field("next_stop_id", pa.string()),
        pa.field("next_station_name", pa.string()),
        pa.field("prediction_time", pa.string()),
        pa.field("predicted_arrival_time", pa.string()),
        pa.field("is_approaching", pa.bool_()),
        pa.field("is_delayed", pa.bool_()),
        pa.field("flags", pa.string()),
        pa.field("latitude", pa.float64()),
        pa.field("longitude", pa.float64()),
        pa.field("heading", pa.int16()),
    ]
)


def resolve_target_date(event):
    """Return the UTC service date to process as a ``date``.

    Defaults to yesterday (UTC); an ``event["date"]`` override (``YYYY-MM-DD``)
    lets a backfill target an arbitrary day.
    """
    override = event.get("date") if event else None
    if override:
        return datetime.strptime(override, "%Y-%m-%d").date()
    return datetime.now(timezone.utc).date() - timedelta(days=1)


def list_raw_objects(bucket, prefix):
    """Return every object key under ``prefix`` via a paginated listing."""
    paginator = s3_client.get_paginator("list_objects_v2")
    keys = []
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            keys.append(obj["Key"])
    return keys


def read_envelopes(bucket, key):
    """Yield each parsed NDJSON envelope from a GZIP raw object.

    Blank lines are ignored and malformed JSON lines are skipped and logged,
    mirroring the defensive skip-and-log style of the fetch Lambda.
    """
    response = s3_client.get_object(Bucket=bucket, Key=key)
    text = gzip.decompress(response["Body"].read()).decode("utf-8")
    for line in text.splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            yield json.loads(line)
        except json.JSONDecodeError:
            logger.warning("Skipping malformed NDJSON line in %s", key)


def _as_list(value):
    """Normalize a value the CTA API may return as a single object or a list."""
    if value is None:
        return []
    if isinstance(value, list):
        return value
    return [value]


def iter_train_observations(envelope):
    """Yield ``(route, request_timestamp, train)`` for every train in an envelope.

    ``route`` is the lowercased envelope route (the documented line code) and
    ``request_timestamp`` is the response-level ``ctatt.tmst``. Envelopes without
    a route are skipped.
    """
    route = envelope.get("route")
    if not route:
        return
    route = route.lower()
    ctatt = envelope.get("data", {}).get("ctatt", {})
    request_timestamp = ctatt.get("tmst")
    for route_obj in _as_list(ctatt.get("route")):
        for train in _as_list(route_obj.get("train")):
            yield route, request_timestamp, train


def _to_str(value):
    """Coerce a string-typed field, preserving null."""
    return None if value is None else str(value)


def _to_bool(value):
    """Map the CTA ``"0"``/``"1"`` flags to booleans; anything else is null."""
    if value == "1":
        return True
    if value == "0":
        return False
    return None


def _to_float(value):
    """Parse a float, returning null on missing or unparseable input."""
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _to_int16(value):
    """Parse an integer heading, returning null on missing or unparseable input."""
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def build_row(route, request_timestamp, train):
    """Project a raw train dict into a typed silver row."""
    return {
        "request_timestamp": _to_str(request_timestamp),
        "route": route,
        "run_number": _to_str(train.get("rn")),
        "destination_station": _to_str(train.get("destSt")),
        "destination_station_name": _to_str(train.get("destNm")),
        "train_direction": _to_str(train.get("trDr")),
        "next_station_id": _to_str(train.get("nextStaId")),
        "next_stop_id": _to_str(train.get("nextStpId")),
        "next_station_name": _to_str(train.get("nextStaNm")),
        "prediction_time": _to_str(train.get("prdt")),
        "predicted_arrival_time": _to_str(train.get("arrT")),
        "is_approaching": _to_bool(train.get("isApp")),
        "is_delayed": _to_bool(train.get("isDly")),
        "flags": _to_str(train.get("flags")),
        "latitude": _to_float(train.get("lat")),
        "longitude": _to_float(train.get("lon")),
        "heading": _to_int16(train.get("heading")),
    }


def write_route_parquet(bucket, date_str, route, rows):
    """Serialize ``rows`` for one route to Parquet and put it to the silver prefix."""
    table = pa.Table.from_pylist(rows, schema=SCHEMA)
    buffer = io.BytesIO()
    pq.write_table(table, buffer)
    key = f"silver/date={date_str}/{route}.parquet"
    s3_client.put_object(Bucket=bucket, Key=key, Body=buffer.getvalue())
    return key


def handler(event, context):
    """Lambda entrypoint: dedupe one UTC day of bronze into per-route silver Parquet."""
    bucket = os.environ["DATA_BUCKET"]
    target_date = resolve_target_date(event)
    date_str = target_date.isoformat()
    prefix = f"raw/{target_date.year:04d}/{target_date.month:02d}/{target_date.day:02d}/"

    keys = list_raw_objects(bucket, prefix)
    logger.info("Found %d raw object(s) under %s", len(keys), prefix)

    seen = set()
    rows_by_route = defaultdict(list)
    rows_in = 0
    for key in keys:
        for envelope in read_envelopes(bucket, key):
            for route, request_timestamp, train in iter_train_observations(envelope):
                rows_in += 1
                digest = sha256(
                    (route + json.dumps(train, sort_keys=True)).encode("utf-8")
                ).hexdigest()
                if digest in seen:
                    continue
                seen.add(digest)
                rows_by_route[route].append(build_row(route, request_timestamp, train))

    rows_out = 0
    routes_written = []
    for route, rows in sorted(rows_by_route.items()):
        write_route_parquet(bucket, date_str, route, rows)
        rows_out += len(rows)
        routes_written.append(route)

    logger.info(
        "Wrote %d row(s) across %d route(s) for %s (from %d observation(s))",
        rows_out,
        len(routes_written),
        date_str,
        rows_in,
    )

    return {
        "statusCode": 200,
        "body": json.dumps(
            {
                "date": date_str,
                "objects_read": len(keys),
                "rows_in": rows_in,
                "rows_out": rows_out,
                "routes_written": routes_written,
            }
        ),
    }
