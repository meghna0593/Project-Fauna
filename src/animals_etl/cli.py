from __future__ import annotations
import asyncio, sys
from .config import parse_args
from .pipeline import run_etl

def main() -> None:
    args = parse_args()
    try:
        asyncio.run(
            run_etl(
                base_url=args.base_url,
                concurrency=args.concurrency,
                batch_size=args.batch_size,
                retries=args.retries,
                connect_timeout=args.connect_timeout,
                read_timeout=args.read_timeout,
            )
        )
    except KeyboardInterrupt:
        print("Aborted.", file=sys.stderr)