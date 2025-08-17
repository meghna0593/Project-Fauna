#!/usr/bin/env python3
"""
Minimal working Animals ETL script (single file).

- Pages /animals/v1/animals, GETs each detail
- Transforms:
  * friends: "a, b, c" -> ["a","b","c"]
  * born_at: epoch (s/ms/µs/ns) -> ISO8601 UTC ('...Z'), rejects negative or future timestamps
- POSTs batches (<=100) to /animals/v1/home
- Retries 5xx + network errors with exponential backoff + jitter; 4xx are fatal

Run Challenge API locally:
  docker load -i lp-programming-challenge-1-1625610904.tar.gz
  docker run --rm -p 3123:3123 -ti lp-programming-challenge-1
  open http://localhost:3123/docs

Usage:
  python animals_etl_simple.py [--base-url URL] [--batch-size 100] [--retries 6]

Config via env:
  API_BASE_URL (default http://localhost:3123)
  BATCH_SIZE (default 100)
  MAX_RETRIES (default 6)
  CONNECT_TIMEOUT (default 5)
  READ_TIMEOUT (default 30)
"""

import argparse
import os
import sys
import random
import time
from datetime import datetime, timezone
from typing import List, Dict, Any, Optional

import itertools
import requests

_req_counter = itertools.count(1)

RETRY_STATUSES = {500, 502, 503, 504}


def parse_args():
    p = argparse.ArgumentParser(description="Minimal Animals ETL (single-file)")
    p.add_argument("--base-url", default=os.getenv("API_BASE_URL", "http://localhost:3123"))
    p.add_argument("--batch-size", type=int, default=int(os.getenv("BATCH_SIZE", "100")))
    p.add_argument("--retries", type=int, default=int(os.getenv("MAX_RETRIES", "6")))
    p.add_argument("--connect-timeout", type=float, default=float(os.getenv("CONNECT_TIMEOUT", "5")))
    p.add_argument("--read-timeout", type=float, default=float(os.getenv("READ_TIMEOUT", "30")))
    return p.parse_args()


def split_friends(s: Optional[str]) -> List[str]:
    """Split a comma-delimited string into a trimmed list; tolerates None/empty."""
    if not s:
        return []
    return [p.strip() for p in s.split(",") if p.strip()]

def epoch_to_iso8601_utc(epoch: Optional[int | float]) -> Optional[str]:
    """
    Convert Unix epoch (auto-detect s/ms/µs/ns) to ISO8601 UTC with 'Z'.
    Returns None for invalid, negative, or future timestamps.
    """
    if epoch is None:
        return None

    # Reject negatives outright
    if epoch < 0:
        return None
    
    e = int(epoch)
    now = datetime.now(tz=timezone.utc)
    
    # Detect units by magnitude
    if e >= 10**18:          # nanoseconds
        ts = e / 1_000_000_000.0
    elif e >= 10**15:        # microseconds
        ts = e / 1_000_000.0
    elif e >= 10**12:        # milliseconds
        ts = e / 1_000.0
    else:                    # seconds
        ts = float(e)
    try:
        dt = datetime.fromtimestamp(ts, tz=timezone.utc)
    except (OverflowError, OSError, ValueError):
        return None
    
    # Guardrail: keep only dates until present
    if dt <= now:
        return dt.isoformat().replace("+00:00", "Z")
    return None


def chunked(seq: List[Any], size: int):
    for i in range(0, len(seq), size):
        yield seq[i:i + size]


class RequestsETL:
    """Thin ETL client using requests with timeouts, limited 5xx retries, and 422 logging/fail-fast."""
    def __init__(self, base_url: str, connect_timeout: float, read_timeout: float, retries: int):
        self.base_url = base_url.rstrip("/")
        self.connect_timeout = connect_timeout
        self.read_timeout = read_timeout
        self.retries = retries
        self.session = requests.Session()

    def _request(self, method: str, path: str, **kwargs) -> requests.Response:
        """Make an HTTP call with transient 5xx retries and special 422 handling; raise on non-2xx."""
        url = self.base_url + path
        req_id = next(_req_counter)
        timeout = (self.connect_timeout, self.read_timeout)
        last_exc: Exception | None = None
        for attempt in range(1, self.retries + 1):
            try:
                resp = self.session.request(method, url, timeout=timeout, **kwargs)

                # Retry on transient 5xx
                if resp.status_code in RETRY_STATUSES:
                    raise requests.HTTPError(f"Server error {resp.status_code}", response=resp)
                
                # 422 (validation) handling: log useful details, then fail fast
                if resp.status_code == 422:
                    try:
                        payload = resp.json()
                    except ValueError:
                        payload = {"detail": (resp.text or "Unprocessable Entity")}
                    detail = payload.get("detail", payload)
                    print(f"[req#{req_id}] 422 validation error on {method} {url}: {detail}", file=sys.stderr)
                    resp.raise_for_status()

                # Fail fast, don’t retry
                if 400 <= resp.status_code < 500:
                    resp.raise_for_status()

                status = resp.status_code
                if not (200 <= status < 300):
                    # If it's a 5xx that's not in RETRY_STATUSES, no retry
                    if 500 <= status < 600 and status not in RETRY_STATUSES:
                        print(f"[req#{req_id}] [fatal] {method} {url} returned {status}, not retrying", file=sys.stderr)
                    resp.raise_for_status() 
                    
                if attempt > 1:
                    print(f"[req#{req_id}] succeeded after {attempt} attempt(s)")
                return resp
            except requests.HTTPError as e:
                # HTTPError with a 4xx, do NOT retry
                status = getattr(getattr(e, "response", None), "status_code", None)
                if status is not None and 400 <= status < 500:
                    print(f"[req#{req_id}] [fatal] {method} {url} returned {status}, not retrying", file=sys.stderr)
                    raise
                last_exc = e
            except requests.RequestException as e:
                last_exc = e
            
            if attempt < self.retries:
                sleep = min(8.0, 0.5 * (2 ** (attempt - 1))) + random.uniform(0, 0.5)
                params = kwargs.get("params")
                has_json = kwargs.get("json") is not None
                err_kind = (f"HTTP {getattr(getattr(last_exc, 'response', None), 'status_code', 'ERR')}"
                            if isinstance(last_exc, requests.HTTPError) else "network")
                print(f"[req#{req_id}] [retry {attempt}/{self.retries}] {method} {url} "
                    f"params={params} json={has_json} failed: {err_kind}: {last_exc}. "
                    f"Sleeping {sleep:.2f}s", file=sys.stderr)
                time.sleep(sleep)
            else:
                print(f"[req#{req_id}] [giving up] {method} {url}: {last_exc}", file=sys.stderr)
                raise last_exc or RuntimeError("request failed")
                
        raise last_exc or RuntimeError("request failed")

    # API methods
    def get_animals_page(self, page: int) -> Dict[str, Any]:
        resp = self._request("GET", "/animals/v1/animals", params={"page": page})
        return resp.json()

    def get_animal(self, animal_id: int) -> Dict[str, Any]:
        resp = self._request("GET", f"/animals/v1/animals/{animal_id}")
        return resp.json()

    def post_home(self, animals: List[Dict[str, Any]]) -> Dict[str, Any]:
        resp = self._request("POST", "/animals/v1/home", json=animals)
        try:
            return resp.json() if resp.content else {}
        except Exception:
            return {}
        
    def close(self):
        self.session.close()


def main():
    args = parse_args()
    print(f"""
        ====== Animals ETL (single-file) ======
        Base URL       : {args.base_url}
        Batch size     : {args.batch_size}
        Retries        : {args.retries}
        Timeouts (s)   : connect={args.connect_timeout} read={args.read_timeout}
        =======================================
    """)

    etl = RequestsETL(args.base_url, args.connect_timeout, args.read_timeout, args.retries)
    try:

        # 1) List IDs
        first = etl.get_animals_page(1)
        total_pages = int(first.get("total_pages", 1))
        ids = [int(item["id"]) for item in first.get("items", [])]
        
        for p in range(2, total_pages + 1):
            page = etl.get_animals_page(p)
            ids.extend(int(item["id"]) for item in page.get("items", []))

        print(f"Found {len(ids)} ids across {total_pages} page(s).")

        # 2) Fetch details
        details = []
        for i, _id in enumerate(ids, 1):
            animal = etl.get_animal(_id)
            details.append(animal)
            if i % 25 == 0 or i == len(ids):
                print(f"Fetched {i}/{len(ids)} details…")

        # 3) Transform
        transformed = []
        for a in details:
            born_iso = epoch_to_iso8601_utc(a.get("born_at")) if a.get("born_at") is not None else None
            
            outgoing = {
                "id": int(a["id"]),
                "name": a["name"],
                "friends": split_friends(a.get("friends", "")),
            }
            
            if born_iso is not None:
                outgoing["born_at"] = born_iso
            
            transformed.append(outgoing)
        print(f"Transformed {len(transformed)} animals.")

        # 4) Load in batches
        batch_size = max(1, min(100, args.batch_size))
        batches = list(chunked(transformed, batch_size))
        print(f"Uploading {len(batches)} batch(es)…")
        for i, batch in enumerate(batches, 1):
            etl.post_home(batch)
            print(f"Posted batch {i}/{len(batches)} ({len(batch)} records).")

        print("ETL Completed.")
    
    finally:
        etl.close()

if __name__ == "__main__":
    main()