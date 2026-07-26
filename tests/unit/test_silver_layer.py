"""Unit tests for the silver_layer Lambda.

Covers every line and branch of the dedup/transform logic. S3 is exercised
through a mocked module-level client (no moto), and written Parquet is read back
from the captured ``put_object`` bytes with pyarrow to assert schema and rows.
"""

import gzip
import io
import json
from datetime import date, datetime, timedelta, timezone
from unittest import mock

import pyarrow.parquet as pq
import pytest

import silver_layer


@pytest.fixture
def s3(monkeypatch):
    """Replace the module-level S3 client with a mock."""
    client = mock.Mock()
    monkeypatch.setattr(silver_layer, "s3_client", client)
    return client


@pytest.fixture
def env(monkeypatch):
    """Provide the environment variable the handler reads."""
    monkeypatch.setenv("DATA_BUCKET", "test-bucket")


# --------------------------------------------------------------------------
# Fixture builders
# --------------------------------------------------------------------------
def make_train(**overrides):
    """Build a raw CTA train dict, overridable field by field."""
    train = {
        "rn": "801",
        "destSt": "30173",
        "destNm": "Howard",
        "trDr": "1",
        "nextStaId": "40900",
        "nextStpId": "30040",
        "nextStaNm": "Jarvis",
        "prdt": "2026-07-20T10:00:00",
        "arrT": "2026-07-20T10:02:00",
        "isApp": "0",
        "isDly": "0",
        "flags": None,
        "lat": "42.019",
        "lon": "-87.675",
        "heading": "90",
    }
    train.update(overrides)
    return train


def envelope_line(route, tmst, trains):
    """Serialize a single Firehose envelope (one NDJSON line)."""
    return json.dumps(
        {
            "route": route,
            "fetched_at": f"{tmst}Z",
            "data": {
                "ctatt": {
                    "tmst": tmst,
                    "route": [{"@name": route.lower(), "train": trains}],
                }
            },
        }
    )


def gz(text):
    """GZIP-compress NDJSON text the way Firehose lands raw objects."""
    return gzip.compress(text.encode("utf-8"))


def configure_s3(s3, pages, objects):
    """Wire the mock paginator and ``get_object`` to serve crafted fixtures.

    ``pages`` is a list of ``list_objects_v2`` page dicts; ``objects`` maps an S3
    key to its already-GZIP-compressed body bytes.
    """
    paginator = mock.Mock()
    paginator.paginate.return_value = pages
    s3.get_paginator.return_value = paginator

    def fake_get_object(Bucket, Key):
        body = mock.Mock()
        body.read.return_value = objects[Key]
        return {"Body": body}

    s3.get_object.side_effect = fake_get_object
    return paginator


def written_parquet(s3):
    """Return ``{s3_key: pyarrow.Table}`` read back from ``put_object`` calls."""
    tables = {}
    for call in s3.put_object.call_args_list:
        key = call.kwargs["Key"]
        tables[key] = pq.read_table(io.BytesIO(call.kwargs["Body"]))
    return tables


# --------------------------------------------------------------------------
# resolve_target_date
# --------------------------------------------------------------------------
def test_resolve_target_date_uses_override():
    assert silver_layer.resolve_target_date({"date": "2026-07-20"}) == date(2026, 7, 20)


def test_resolve_target_date_defaults_to_yesterday():
    expected = datetime.now(timezone.utc).date() - timedelta(days=1)
    assert silver_layer.resolve_target_date({}) == expected


# --------------------------------------------------------------------------
# small typing helpers
# --------------------------------------------------------------------------
def test_as_list_normalizes_shapes():
    assert silver_layer._as_list(None) == []
    assert silver_layer._as_list([1, 2]) == [1, 2]
    assert silver_layer._as_list({"a": 1}) == [{"a": 1}]


def test_to_str_preserves_null():
    assert silver_layer._to_str(None) is None
    assert silver_layer._to_str(5) == "5"


def test_to_bool_maps_flags():
    assert silver_layer._to_bool("1") is True
    assert silver_layer._to_bool("0") is False
    assert silver_layer._to_bool("9") is None


def test_to_float_parses_or_nulls():
    assert silver_layer._to_float("42.0") == 42.0
    assert silver_layer._to_float(None) is None
    assert silver_layer._to_float("nope") is None


def test_to_int16_parses_or_nulls():
    assert silver_layer._to_int16("90") == 90
    assert silver_layer._to_int16(None) is None
    assert silver_layer._to_int16("nope") is None


# --------------------------------------------------------------------------
# handler
# --------------------------------------------------------------------------
def test_handler_dedupes_and_writes_per_route(s3, env):
    train_a = make_train(rn="801")
    train_a_dupe = make_train(rn="801")  # byte-identical to train_a
    train_b_changed = make_train(rn="801", prdt="2026-07-20T10:00:30")  # a field differs
    blue_train = make_train(rn="123", destNm="95th/Dan Ryan")

    # Two captures a second apart: train_a repeats identically (collapses, keeping
    # the first tmst), train_b_changed appears only in the later capture (kept).
    lines = "\n".join(
        [
            envelope_line("Red", "2026-07-20T10:00:01", [train_a]),
            "",  # blank line is skipped
            "{not valid json",  # malformed line is skipped
            envelope_line("Red", "2026-07-20T10:00:02", [train_a_dupe, train_b_changed]),
            envelope_line("Blue", "2026-07-20T10:00:03", [blue_train]),
        ]
    )
    key = "raw/2026/07/20/10/data.gz"
    pages = [
        {"Contents": [{"Key": key}]},
        {},  # a page with no Contents is tolerated
    ]
    paginator = configure_s3(s3, pages, {key: gz(lines)})

    result = silver_layer.handler({"date": "2026-07-20"}, None)

    # The paginator was pointed at the target day's raw partition.
    paginator.paginate.assert_called_once_with(Bucket="test-bucket", Prefix="raw/2026/07/20/")

    body = json.loads(result["body"])
    assert result["statusCode"] == 200
    assert body["date"] == "2026-07-20"
    assert body["objects_read"] == 1
    assert body["rows_in"] == 4  # 2 red captures of train_a + train_b + 1 blue
    assert body["rows_out"] == 3  # one red duplicate collapsed
    assert body["routes_written"] == ["blue", "red"]

    tables = written_parquet(s3)
    assert set(tables) == {
        "silver/date=2026-07-20/red.parquet",
        "silver/date=2026-07-20/blue.parquet",
    }

    red = tables["silver/date=2026-07-20/red.parquet"]
    assert red.schema.equals(silver_layer.SCHEMA)
    red_rows = red.to_pylist()
    assert len(red_rows) == 2

    # The collapsed duplicate keeps the FIRST occurrence's request_timestamp.
    kept = next(r for r in red_rows if r["prediction_time"] == "2026-07-20T10:00:00")
    assert kept["request_timestamp"] == "2026-07-20T10:00:01"
    # Fields are typed for analytics rather than left as raw strings.
    assert kept["is_approaching"] is False
    assert kept["is_delayed"] is False
    assert kept["latitude"] == 42.019
    assert kept["longitude"] == -87.675
    assert kept["heading"] == 90
    assert kept["route"] == "red"

    changed = next(r for r in red_rows if r["prediction_time"] == "2026-07-20T10:00:30")
    assert changed["request_timestamp"] == "2026-07-20T10:00:02"

    blue = tables["silver/date=2026-07-20/blue.parquet"]
    blue_rows = blue.to_pylist()
    assert len(blue_rows) == 1
    assert blue_rows[0]["route"] == "blue"
    assert blue_rows[0]["destination_station_name"] == "95th/Dan Ryan"


def test_handler_handles_singleton_shapes_and_missing_route(s3, env):
    # CTA can emit ``route``/``train`` as a single object instead of a list, and a
    # malformed envelope may lack a route entirely (skipped).
    singleton = json.dumps(
        {
            "route": "Org",
            "data": {
                "ctatt": {
                    "tmst": "2026-07-20T11:00:00",
                    "route": {"@name": "org", "train": make_train(rn="404")},
                }
            },
        }
    )
    no_route = json.dumps({"data": {"ctatt": {"tmst": "2026-07-20T11:00:01"}}})
    lines = "\n".join([singleton, no_route])
    key = "raw/2026/07/20/11/data.gz"
    configure_s3(s3, [{"Contents": [{"Key": key}]}], {key: gz(lines)})

    result = silver_layer.handler({"date": "2026-07-20"}, None)

    body = json.loads(result["body"])
    assert body["rows_in"] == 1
    assert body["rows_out"] == 1
    assert body["routes_written"] == ["org"]


def test_handler_types_unparseable_values_as_null(s3, env):
    # Missing/garbage numeric and flag fields must land as null, not raise.
    messy = make_train(lat=None, lon="bad", heading="north", isApp="9", flags=None)
    del messy["lat"]  # exercise the missing-key (TypeError) float path
    line = envelope_line("Y", "2026-07-20T12:00:00", [messy])
    key = "raw/2026/07/20/12/data.gz"
    configure_s3(s3, [{"Contents": [{"Key": key}]}], {key: gz(line)})

    silver_layer.handler({"date": "2026-07-20"}, None)

    row = written_parquet(s3)["silver/date=2026-07-20/y.parquet"].to_pylist()[0]
    assert row["latitude"] is None
    assert row["longitude"] is None
    assert row["heading"] is None
    assert row["is_approaching"] is None
    assert row["flags"] is None


def test_handler_empty_partition_writes_nothing(s3, env):
    configure_s3(s3, [{}], {})

    result = silver_layer.handler({"date": "2026-07-20"}, None)

    body = json.loads(result["body"])
    assert body["objects_read"] == 0
    assert body["rows_in"] == 0
    assert body["rows_out"] == 0
    assert body["routes_written"] == []
    s3.get_object.assert_not_called()
    s3.put_object.assert_not_called()
