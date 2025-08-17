from __future__ import annotations
import argparse, os

def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Async Animals ETL")
    p.add_argument("--base-url", default=os.getenv("API_BASE_URL", "http://localhost:3123"))
    p.add_argument("--concurrency", type=int, default=int(os.getenv("CONCURRENCY", "8")))
    p.add_argument("--batch-size", type=int, default=int(os.getenv("BATCH_SIZE", "100")))
    p.add_argument("--retries", type=int, default=int(os.getenv("MAX_RETRIES", "6")))
    p.add_argument("--connect-timeout", type=float, default=float(os.getenv("CONNECT_TIMEOUT", "5")))
    p.add_argument("--read-timeout", type=float, default=float(os.getenv("READ_TIMEOUT", "30")))
    return p

def parse_args() -> argparse.Namespace:
    return build_parser().parse_args()