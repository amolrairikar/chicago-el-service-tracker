"""Unit tests for the train_locations Lambda.

Covers every line and branch of ``fetch_route``, ``deliver_records`` and
``handler``. External effects (HTTP, Firehose, thread pool) are exercised
through mocks so the tests run without AWS access or network calls.
"""

import json
from unittest import mock

import pytest

import train_locations


@pytest.fixture
def firehose(monkeypatch):
    """Replace the module-level Firehose client with a mock."""
    client = mock.Mock()
    monkeypatch.setattr(train_locations, "firehose_client", client)
    return client


@pytest.fixture
def env(monkeypatch):
    """Provide the environment variables the handler reads."""
    monkeypatch.setenv("CTA_API_KEY", "test-key")
    monkeypatch.setenv("FIREHOSE_STREAM", "test-stream")


def make_record(route):
    """Build a delivery record shaped like the ones the handler produces."""
    return {"route": route, "Data": f'{{"route": "{route}"}}\n'.encode()}


# --------------------------------------------------------------------------
# fetch_route
# --------------------------------------------------------------------------
def test_fetch_route_returns_raw_body():
    response = mock.MagicMock()
    response.read.return_value = b'{"ctatt": {}}'
    # urlopen is used as a context manager, so __enter__ yields the response.
    response.__enter__.return_value = response

    with mock.patch.object(
        train_locations.urllib.request, "urlopen", return_value=response
    ) as urlopen:
        body = train_locations.fetch_route("Red", "secret-key")

    assert body == b'{"ctatt": {}}'
    called_url = urlopen.call_args.args[0]
    assert called_url.startswith(train_locations.API_URL)
    assert "rt=Red" in called_url
    assert "key=secret-key" in called_url
    assert "outputType=JSON" in called_url


# --------------------------------------------------------------------------
# deliver_records
# --------------------------------------------------------------------------
def test_deliver_records_all_succeed_first_attempt(firehose):
    firehose.put_record_batch.return_value = {"FailedPutCount": 0}
    records = [make_record("Red"), make_record("Blue")]

    delivered = train_locations.deliver_records("stream", records)

    assert delivered == 2
    firehose.put_record_batch.assert_called_once()
    sent = firehose.put_record_batch.call_args.kwargs
    assert sent["DeliveryStreamName"] == "stream"
    assert sent["Records"] == [{"Data": r["Data"]} for r in records]


def test_deliver_records_retries_only_failed_record(firehose):
    # First attempt: Red succeeds, Blue fails. Second attempt: Blue succeeds.
    firehose.put_record_batch.side_effect = [
        {
            "FailedPutCount": 1,
            "RequestResponses": [{}, {"ErrorCode": "ServiceUnavailable"}],
        },
        {"FailedPutCount": 0, "RequestResponses": [{}]},
    ]
    records = [make_record("Red"), make_record("Blue")]

    delivered = train_locations.deliver_records("stream", records)

    assert delivered == 2
    assert firehose.put_record_batch.call_count == 2
    # The retry must carry only the previously failed record.
    retry_sent = firehose.put_record_batch.call_args_list[1].kwargs["Records"]
    assert retry_sent == [{"Data": records[1]["Data"]}]


def test_deliver_records_gives_up_after_max_attempts(firehose):
    # Every attempt reports the single record as failed with an error code.
    firehose.put_record_batch.return_value = {
        "FailedPutCount": 1,
        "RequestResponses": [{"ErrorCode": "ServiceUnavailable"}],
    }
    records = [make_record("Red")]

    delivered = train_locations.deliver_records("stream", records)

    assert delivered == 0
    assert firehose.put_record_batch.call_count == train_locations.MAX_DELIVERY_ATTEMPTS


def test_deliver_records_stops_when_final_attempt_reports_no_error_codes(
    firehose,
):
    # The last attempt reports a failure count but no per-record error code,
    # so nothing is left to retry and the after-loop cleanup branch sees an
    # empty pending list.
    failure_with_code = {
        "FailedPutCount": 1,
        "RequestResponses": [{"ErrorCode": "ServiceUnavailable"}],
    }
    failure_without_code = {"FailedPutCount": 1, "RequestResponses": [{}]}
    firehose.put_record_batch.side_effect = [
        failure_with_code,
        failure_with_code,
        failure_without_code,
    ]
    records = [make_record("Red")]

    delivered = train_locations.deliver_records("stream", records)

    assert delivered == 0
    assert firehose.put_record_batch.call_count == train_locations.MAX_DELIVERY_ATTEMPTS


# --------------------------------------------------------------------------
# handler
# --------------------------------------------------------------------------
def test_handler_streams_fetched_routes(firehose, env, monkeypatch):
    # Red raises (fetch-failure branch), Blue returns invalid JSON
    # (decode-failure branch), every other route returns valid JSON.
    def fake_fetch(route, api_key):
        assert api_key == "test-key"
        if route == "Red":
            raise RuntimeError("network down")
        if route == "Blue":
            return b"not-json{"
        return json.dumps({"ctatt": {"route": route}}).encode()

    monkeypatch.setattr(train_locations, "fetch_route", fake_fetch)
    firehose.put_record_batch.return_value = {"FailedPutCount": 0}

    result = train_locations.handler({}, None)

    body = json.loads(result["body"])
    assert result["statusCode"] == 200
    assert body["routes_requested"] == len(train_locations.TRAIN_ROUTES)
    # 8 routes, Red failed to fetch -> 7 fetched.
    assert body["routes_fetched"] == len(train_locations.TRAIN_ROUTES) - 1
    # Blue's invalid JSON is skipped -> 6 delivered.
    assert body["records_delivered"] == len(train_locations.TRAIN_ROUTES) - 2
    assert body["delivery_stream"] == "test-stream"

    # Each delivered record is newline-terminated JSON carrying the metadata.
    sent = firehose.put_record_batch.call_args.kwargs["Records"]
    envelope = json.loads(sent[0]["Data"].decode())
    assert envelope["route"] in train_locations.TRAIN_ROUTES
    assert "fetched_at" in envelope
    assert envelope["data"] == {"ctatt": {"route": envelope["route"]}}


def test_handler_delivers_nothing_when_no_valid_records(firehose, env, monkeypatch):
    # No route yields valid JSON, so there are no records to deliver.
    monkeypatch.setattr(
        train_locations,
        "fetch_route",
        lambda route, api_key: b"not-json{",
    )

    result = train_locations.handler({}, None)

    body = json.loads(result["body"])
    assert body["records_delivered"] == 0
    assert body["routes_fetched"] == len(train_locations.TRAIN_ROUTES)
    firehose.put_record_batch.assert_not_called()
