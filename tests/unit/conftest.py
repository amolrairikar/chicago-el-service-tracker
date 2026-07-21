"""Shared pytest setup for the Lambda unit tests.

A region must be configured before ``boto3.client`` is called at import time,
and the Lambda source lives outside any importable package (so the packaging
script can zip the directory contents at the archive root), so the module's
directory is placed on ``sys.path`` here.
"""

import os
import sys
from pathlib import Path

os.environ.setdefault("AWS_DEFAULT_REGION", "us-east-1")

LAMBDA_DIR = Path(__file__).resolve().parents[2] / "src" / "lambdas" / "train_locations"
sys.path.insert(0, str(LAMBDA_DIR))
