"""Unit tests for the gold_layer (total_trains) Lambda.

S3 and CloudFront are exercised through mocked module-level clients (no moto).
Silver input is crafted as real Parquet bytes read back by the handler, and the
published JSON is read from the captured ``put_object`` bodies.
"""

import io
import json
from datetime import date, datetime, timedelta, timezone
from unittest import mock

import gold_layer
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from botocore.exceptions import ClientError


@pytest.fixture
def s3(monkeypatch):
    """Replace the module-level S3 client with a mock."""
    client = mock.Mock()
    monkeypatch.setattr(gold_layer, "s3_client", client)
    return client


@pytest.fixture
def cloudfront(monkeypatch):
    """Replace the module-level CloudFront client with a mock."""
    client = mock.Mock()
    monkeypatch.setattr(gold_layer, "cloudfront_client", client)
    return client


@pytest.fixture
def env(monkeypatch):
    """Provide the environment variables the handler reads."""
    monkeypatch.setenv("DATA_BUCKET", "test-bucket")
    monkeypatch.setenv("CLOUDFRONT_DISTRIBUTION_ID", "DIST123")


# --------------------------------------------------------------------------
# Fixture builders
# --------------------------------------------------------------------------
def silver_row(route, run_number, request_timestamp):
    """Build a projected silver row (only the columns the gold writer reads)."""
    return {
        "route": route,
        "run_number": run_number,
        "request_timestamp": request_timestamp,
    }


def parquet_bytes(rows):
    """Serialize silver rows to Parquet the way the silver layer lands them."""
    buffer = io.BytesIO()
    pq.write_table(pa.Table.from_pylist(rows), buffer)
    return buffer.getvalue()


def configure_s3(s3, partitions, parquet_objects, daily_json=None):
    """Wire the mock paginator and ``get_object`` to serve crafted fixtures.

    ``partitions`` maps a listing prefix to the object keys it contains;
    ``parquet_objects`` maps a silver key to its Parquet bytes; ``daily_json`` is
    the existing daily fact document (or ``None`` to simulate a first run).
    """

    def fake_paginate(Bucket, Prefix):
        keys = partitions.get(Prefix, [])
        return [{"Contents": [{"Key": key} for key in keys]}] if keys else [{}]

    paginator = mock.Mock()
    paginator.paginate.side_effect = fake_paginate
    s3.get_paginator.return_value = paginator

    def fake_get_object(Bucket, Key):
        if Key == gold_layer.DAILY_KEY:
            if daily_json is None:
                raise ClientError({"Error": {"Code": "NoSuchKey"}}, "GetObject")
            body = mock.Mock()
            body.read.return_value = json.dumps(daily_json).encode("utf-8")
            return {"Body": body}
        body = mock.Mock()
        body.read.return_value = parquet_objects[Key]
        return {"Body": body}

    s3.get_object.side_effect = fake_get_object
    return paginator


def written_json(s3):
    """Return ``{s3_key: parsed_document}`` from the captured ``put_object`` calls."""
    docs = {}
    for call in s3.put_object.call_args_list:
        docs[call.kwargs["Key"]] = json.loads(call.kwargs["Body"])
    return docs


# --------------------------------------------------------------------------
# resolve_service_date
# --------------------------------------------------------------------------
def test_resolve_service_date_uses_override():
    assert gold_layer.resolve_service_date({"service_date": "2026-07-20"}) == date(2026, 7, 20)


def test_resolve_service_date_defaults_to_two_days_ago():
    expected = datetime.now(timezone.utc).date() - timedelta(days=2)
    assert gold_layer.resolve_service_date({}) == expected


# --------------------------------------------------------------------------
# windowing helpers
# --------------------------------------------------------------------------
def test_service_day_window_is_three_am_to_three_am():
    start, end = gold_layer.service_day_window(date(2026, 7, 20))
    assert start == datetime(2026, 7, 20, 3)
    assert end == datetime(2026, 7, 21, 3)


def test_silver_partition_dates_span_two_utc_days():
    assert gold_layer.silver_partition_dates(date(2026, 7, 20)) == [
        date(2026, 7, 20),
        date(2026, 7, 21),
    ]


def test_parse_request_timestamp_variants():
    assert gold_layer.parse_request_timestamp("2026-07-20T10:00:00") == datetime(
        2026, 7, 20, 10, 0, 0
    )
    # An offset (defensive) is dropped so comparison stays wall-clock.
    assert gold_layer.parse_request_timestamp("2026-07-20T10:00:00+00:00") == datetime(
        2026, 7, 20, 10, 0, 0
    )
    assert gold_layer.parse_request_timestamp(None) is None
    assert gold_layer.parse_request_timestamp("") is None
    assert gold_layer.parse_request_timestamp("not-a-date") is None


def test_week_start_is_sunday_anchored():
    # Sun 2026-07-26 begins the week; Mon..Sat all resolve back to it.
    assert gold_layer.week_start(date(2026, 7, 26)) == date(2026, 7, 26)  # Sunday
    assert gold_layer.week_start(date(2026, 7, 27)) == date(2026, 7, 26)  # Monday
    assert gold_layer.week_start(date(2026, 8, 1)) == date(2026, 7, 26)  # Saturday
    assert gold_layer.week_start(date(2026, 8, 2)) == date(2026, 8, 2)  # next Sunday


# --------------------------------------------------------------------------
# compute_daily_counts
# --------------------------------------------------------------------------
def test_compute_daily_counts_distinct_windowed_and_non_null():
    start, end = gold_layer.service_day_window(date(2026, 7, 20))
    rows = [
        silver_row("red", "101", "2026-07-20T10:00:00"),
        silver_row("red", "101", "2026-07-20T10:05:00"),  # same run -> counted once
        silver_row("red", "102", "2026-07-20T11:00:00"),
        silver_row("red", "103", "2026-07-20T02:00:00"),  # before 03:00 -> excluded
        silver_row("red", "104", "2026-07-21T01:30:00"),  # 1:30am -> still this day
        silver_row("red", "105", "2026-07-21T05:00:00"),  # after next 03:00 -> excluded
        silver_row("red", None, "2026-07-20T12:00:00"),  # null run -> excluded
        silver_row("blue", "201", "2026-07-20T12:00:00"),
    ]
    assert gold_layer.compute_daily_counts(rows, start, end) == {"red": 3, "blue": 1}


# --------------------------------------------------------------------------
# upsert_daily / rollup
# --------------------------------------------------------------------------
def test_upsert_daily_replaces_only_the_target_date():
    existing = [
        {"service_date": "2026-07-19", "route": "red", "total_trains": 5},
        {"service_date": "2026-07-20", "route": "red", "total_trains": 99},  # stale
    ]
    result = gold_layer.upsert_daily(existing, "2026-07-20", {"red": 3, "blue": 2})
    assert result == [
        {"service_date": "2026-07-19", "route": "red", "total_trains": 5},
        {"service_date": "2026-07-20", "route": "blue", "total_trains": 2},
        {"service_date": "2026-07-20", "route": "red", "total_trains": 3},
    ]


def test_rollup_sums_daily_counts_per_bucket():
    daily = [
        {"service_date": "2026-07-19", "route": "red", "total_trains": 5},
        {"service_date": "2026-07-20", "route": "red", "total_trains": 3},
        {"service_date": "2026-07-20", "route": "blue", "total_trains": 2},
    ]
    weekly = gold_layer.rollup(daily, lambda d: gold_layer.week_start(d).isoformat(), "week_start")
    # 2026-07-19 (Sun) and 2026-07-20 (Mon) share the week starting 2026-07-19.
    assert weekly == [
        {"week_start": "2026-07-19", "route": "blue", "total_trains": 2},
        {"week_start": "2026-07-19", "route": "red", "total_trains": 8},
    ]


# --------------------------------------------------------------------------
# handler
# --------------------------------------------------------------------------
def test_handler_publishes_rollups_and_invalidates(s3, cloudfront, env):
    day_key = "silver/date=2026-07-20/data.parquet"
    next_key = "silver/date=2026-07-21/data.parquet"
    partitions = {
        "silver/date=2026-07-20/": [day_key],
        "silver/date=2026-07-21/": [next_key],
    }
    parquet_objects = {
        day_key: parquet_bytes(
            [
                silver_row("red", "101", "2026-07-20T10:00:00"),
                silver_row("red", "101", "2026-07-20T10:05:00"),  # dup run
                silver_row("red", "102", "2026-07-20T11:00:00"),
                silver_row("red", "103", "2026-07-20T02:00:00"),  # before window
                silver_row("red", None, "2026-07-20T12:00:00"),  # null run
                silver_row("blue", "201", "2026-07-20T12:00:00"),
            ]
        ),
        next_key: parquet_bytes(
            [
                silver_row("red", "104", "2026-07-21T01:30:00"),  # 1:30am, this day
                silver_row("red", "105", "2026-07-21T05:00:00"),  # after window
                silver_row("blue", "202", "2026-07-21T02:00:00"),
            ]
        ),
    }
    configure_s3(s3, partitions, parquet_objects, daily_json=None)

    result = gold_layer.handler({"service_date": "2026-07-20"}, None)

    body = json.loads(result["body"])
    assert result["statusCode"] == 200
    assert body["service_date"] == "2026-07-20"
    assert body["daily_counts"] == {"red": 3, "blue": 2}
    assert body["daily_records"] == 2
    assert body["cache_invalidated"] is True

    docs = written_json(s3)
    assert set(docs) == {
        gold_layer.DAILY_KEY,
        gold_layer.WEEKLY_KEY,
        gold_layer.MONTHLY_KEY,
        gold_layer.YEARLY_KEY,
    }
    assert docs[gold_layer.DAILY_KEY]["grain"] == "day"
    assert docs[gold_layer.DAILY_KEY]["counts"] == [
        {"service_date": "2026-07-20", "route": "blue", "total_trains": 2},
        {"service_date": "2026-07-20", "route": "red", "total_trains": 3},
    ]
    assert docs[gold_layer.WEEKLY_KEY]["counts"] == [
        {"week_start": "2026-07-19", "route": "blue", "total_trains": 2},
        {"week_start": "2026-07-19", "route": "red", "total_trains": 3},
    ]
    assert docs[gold_layer.MONTHLY_KEY]["counts"] == [
        {"month": "2026-07", "route": "blue", "total_trains": 2},
        {"month": "2026-07", "route": "red", "total_trains": 3},
    ]
    assert docs[gold_layer.YEARLY_KEY]["counts"] == [
        {"year": 2026, "route": "blue", "total_trains": 2},
        {"year": 2026, "route": "red", "total_trains": 3},
    ]

    cloudfront.create_invalidation.assert_called_once()
    invalidation = cloudfront.create_invalidation.call_args.kwargs
    assert invalidation["DistributionId"] == "DIST123"
    assert invalidation["InvalidationBatch"]["Paths"]["Items"] == [gold_layer.INVALIDATION_PATH]


def test_handler_reruns_idempotently_and_accumulates_history(s3, cloudfront, env):
    day_key = "silver/date=2026-07-20/data.parquet"
    partitions = {"silver/date=2026-07-20/": [day_key]}
    parquet_objects = {day_key: parquet_bytes([silver_row("red", "101", "2026-07-20T10:00:00")])}
    existing_daily = {
        "generated_at": "2026-07-21T07:00:00+00:00",
        "grain": "day",
        "counts": [
            {"service_date": "2026-07-19", "route": "red", "total_trains": 5},
            {"service_date": "2026-07-20", "route": "red", "total_trains": 99},  # stale
        ],
    }
    configure_s3(s3, partitions, parquet_objects, daily_json=existing_daily)

    gold_layer.handler({"service_date": "2026-07-20"}, None)

    docs = written_json(s3)
    # The prior day survives; the reprocessed day overwrites its stale 99 -> 1.
    assert docs[gold_layer.DAILY_KEY]["counts"] == [
        {"service_date": "2026-07-19", "route": "red", "total_trains": 5},
        {"service_date": "2026-07-20", "route": "red", "total_trains": 1},
    ]
    # Both days fall in the week starting Sun 2026-07-19: 5 + 1 = 6.
    assert docs[gold_layer.WEEKLY_KEY]["counts"] == [
        {"week_start": "2026-07-19", "route": "red", "total_trains": 6}
    ]


def test_handler_skips_invalidation_without_distribution(s3, cloudfront, env, monkeypatch):
    monkeypatch.delenv("CLOUDFRONT_DISTRIBUTION_ID", raising=False)
    day_key = "silver/date=2026-07-20/data.parquet"
    configure_s3(
        s3,
        {"silver/date=2026-07-20/": [day_key]},
        {day_key: parquet_bytes([silver_row("red", "101", "2026-07-20T10:00:00")])},
        daily_json=None,
    )

    result = gold_layer.handler({"service_date": "2026-07-20"}, None)

    assert json.loads(result["body"])["cache_invalidated"] is False
    cloudfront.create_invalidation.assert_not_called()
    # The rollups are still published even when there is nothing to invalidate.
    assert len(written_json(s3)) == 4


def test_read_daily_records_reraises_unexpected_errors(s3, env):
    s3.get_object.side_effect = ClientError({"Error": {"Code": "AccessDenied"}}, "GetObject")
    with pytest.raises(ClientError):
        gold_layer.read_daily_records("test-bucket")


def test_handler_empty_service_day_writes_empty_rollups(s3, cloudfront, env):
    configure_s3(s3, {}, {}, daily_json=None)

    result = gold_layer.handler({"service_date": "2026-07-20"}, None)

    body = json.loads(result["body"])
    assert body["observations_read"] == 0
    assert body["daily_counts"] == {}
    assert body["daily_records"] == 0

    docs = written_json(s3)
    assert set(docs) == {
        gold_layer.DAILY_KEY,
        gold_layer.WEEKLY_KEY,
        gold_layer.MONTHLY_KEY,
        gold_layer.YEARLY_KEY,
    }
    assert all(doc["counts"] == [] for doc in docs.values())
