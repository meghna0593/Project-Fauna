"""
Command-line entrypoint for the Animals ETL pipeline.

- Parses CLI args and config
- Initializes HttpClient and AnimalsAPI
- Orchestrates pipeline steps:
    1. Fetch all animal IDs (with limited page concurrency)
    2. Fetch details concurrently
    3. Transform records
    4. Post transformed batches

Handles validation errors (422) and KeyboardInterrupt cleanly for user experience.
"""
from __future__ import annotations
import asyncio, sys

from http_client import HttpClient, ValidationHTTPError

from .api import AnimalsAPI
from .config import parse_args
from .pipeline import fetch_all_ids, fetch_details_concurrent, transform_records, post_batches

async def run(args):
    async with HttpClient(
        base_url=args.base_url,
        connect_timeout=args.connect_timeout,
        read_timeout=args.read_timeout,
        retries=args.retries,
    ) as http:
        api = AnimalsAPI(http)
        print(f"""
            ====== Animals ETL (async) ======
            Base URL       : {args.base_url}
            Concurrency    : {args.concurrency}
            Batch size     : {args.batch_size}
            Retries        : {args.retries}
            Timeouts (s)   : connect={args.connect_timeout} read={args.read_timeout}
            ===============================
        """)
        ids = await fetch_all_ids(api, min(3, max(1, args.concurrency // 2)))
        details = await fetch_details_concurrent(api, ids, args.concurrency)
        transformed = transform_records(details)
        await post_batches(api, transformed, args.batch_size)

def main() -> None:
    args = parse_args()
    try:
        asyncio.run(run(args))
    except ValidationHTTPError as e:
        print(f"Validation error: {e.detail}", file=sys.stderr)
        sys.exit(2)
    except KeyboardInterrupt:
        print("Aborted.", file=sys.stderr)
