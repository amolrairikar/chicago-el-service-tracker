"""Lambda that fetches live CTA "El" train positions and streams them to a
Kinesis Data Firehose delivery stream.

For each train route the CTA Train Tracker ``ttpositions`` endpoint is queried
in parallel. Each raw JSON response is wrapped with metadata and sent to
Firehose, which buffers records and flushes larger batched files to S3 (rather
than this Lambda writing many tiny objects itself).
"""

import concurrent.futures
import datetime
import json
import logging
import os
import urllib.parse
import urllib.request

import boto3

logger = logging.getLogger()
logger.setLevel(logging.INFO)

API_URL = "https://lapi.transitchicago.com/api/1.0/ttpositions.aspx"

# The eight CTA "El" routes as recognized by the ``rt`` query parameter.
TRAIN_ROUTES = ["Red", "Blue", "Brn", "G", "Org", "P", "Pink", "Y"]

# Seconds to wait before giving up on a single route request.
REQUEST_TIMEOUT = 10

# How many times to resend records that Firehose rejects before giving up.
MAX_DELIVERY_ATTEMPTS = 3

firehose_client = boto3.client("firehose")


def fetch_route(route: str, api_key: str) -> bytes:
    """Fetch raw train position data for a single ``route``.

    Returns the raw response body as bytes so it can be archived verbatim.
    """
    query = urllib.parse.urlencode(
        {"rt": route, "key": api_key, "outputType": "JSON"}
    )
    url = f"{API_URL}?{query}"
    logger.info("Requesting train positions for route %s", route)
    with urllib.request.urlopen(url, timeout=REQUEST_TIMEOUT) as response:
        return response.read()


def deliver_records(stream_name: str, records: list[dict]) -> int:
    """Send ``records`` to Firehose, retrying any the service rejects.

    ``records`` is a list of ``{"route": str, "Data": bytes}`` dicts. Firehose
    ``put_record_batch`` can partially fail, so rejected records are resent up
    to ``MAX_DELIVERY_ATTEMPTS`` times. Returns the count successfully
    delivered.
    """
    pending = list(records)
    delivered = 0

    for attempt in range(1, MAX_DELIVERY_ATTEMPTS + 1):
        response = firehose_client.put_record_batch(
            DeliveryStreamName=stream_name,
            Records=[{"Data": record["Data"]} for record in pending],
        )
        failed_count = response.get("FailedPutCount", 0)
        delivered += len(pending) - failed_count
        if not failed_count:
            return delivered

        # Keep only the records whose individual response carries an error.
        retry = [
            record
            for record, result in zip(pending, response["RequestResponses"])
            if result.get("ErrorCode")
        ]
        logger.warning(
            "Firehose rejected %d/%d records on attempt %d/%d: %s",
            failed_count,
            len(pending),
            attempt,
            MAX_DELIVERY_ATTEMPTS,
            ", ".join(record["route"] for record in retry),
        )
        pending = retry

    if pending:
        logger.error(
            "Failed to deliver %d records after %d attempts: %s",
            len(pending),
            MAX_DELIVERY_ATTEMPTS,
            ", ".join(record["route"] for record in pending),
        )
    return delivered


def handler(event, context):
    """Lambda entrypoint: fetch every route in parallel and stream to Firehose."""
    api_key = os.environ["CTA_API_KEY"]
    stream_name = os.environ["FIREHOSE_STREAM"]

    fetched_at = datetime.datetime.now(datetime.timezone.utc)

    results = {}
    with concurrent.futures.ThreadPoolExecutor(
        max_workers=len(TRAIN_ROUTES)
    ) as executor:
        future_to_route = {
            executor.submit(fetch_route, route, api_key): route
            for route in TRAIN_ROUTES
        }
        for future in concurrent.futures.as_completed(future_to_route):
            route = future_to_route[future]
            try:
                results[route] = future.result()
            except Exception:
                logger.exception("Failed to fetch route %s", route)

    # Wrap each raw response with metadata and encode it as a newline-delimited
    # JSON record, so Firehose's concatenated S3 objects stay parseable
    # line-by-line downstream. The eight routes fit well within Firehose's
    # 500-record / 4 MB batch limits, so a single batch call suffices.
    records = []
    for route, body in results.items():
        try:
            payload = json.loads(body)
        except json.JSONDecodeError:
            logger.exception(
                "Skipping route %s: response was not valid JSON", route
            )
            continue
        envelope = {
            "route": route,
            "fetched_at": fetched_at.isoformat(),
            "data": payload,
        }
        data = (json.dumps(envelope) + "\n").encode("utf-8")
        records.append({"route": route, "Data": data})

    delivered = deliver_records(stream_name, records) if records else 0

    return {
        "statusCode": 200,
        "body": json.dumps(
            {
                "routes_requested": len(TRAIN_ROUTES),
                "routes_fetched": len(results),
                "records_delivered": delivered,
                "delivery_stream": stream_name,
            }
        ),
    }
