#!/usr/bin/env python3
"""
Async Animals ETL (single-file)

Features
--------
- Extract:
  * Pages through /animals/v1/animals to list IDs
  * Fetches details concurrently (bounded by --concurrency)

- Transform:
  * friends: "a, b, c" -> ["a","b","c"]
  * born_at: epoch (s/ms/µs/ns) -> ISO8601 UTC ('...Z')
      - Rejects negatives, invalid, or future timestamps

- Load:
  * POSTs batches of up to 100 records to /animals/v1/home

- Reliability:
  * Retries transient 5xx + network errors with exponential backoff + jitter
  * Fails fast on 4xx errors
  * Logs request IDs, retries, and warnings

Run Challenge API locally:
  docker load -i lp-programming-challenge-1-1625610904.tar.gz
  docker run --rm -p 3123:3123 -ti lp-programming-challenge-1
  open http://localhost:3123/docs

Execute ETL:
  python animals_etl.py [--base-url URL] [--batch-size 100] [--retries 6] [--concurrency 8]

Config via env:
  API_BASE_URL (default http://localhost:3123)
  BATCH_SIZE (default 100)
  MAX_RETRIES (default 6)
  CONNECT_TIMEOUT (default 5)
  READ_TIMEOUT (default 30)
"""
from __future__ import annotations
import argparse
import asyncio
import os
import random
import re
import sys
import uuid

from datetime import datetime, timezone
from typing import List, Dict, Any, Optional

import httpx

RETRY_STATUSES = {500, 502, 503, 504}
ISO_UTC_Z_RE = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?Z$")

# ------------------ CLI ------------------

def parse_args():
    # Parse command-line args with env defaults
    p = argparse.ArgumentParser(description="Async Animals ETL (single-file)")
    p.add_argument("--base-url", default=os.getenv("API_BASE_URL", "http://localhost:3123"))
    p.add_argument("--concurrency", type=int, default=int(os.getenv("CONCURRENCY", "8")))
    p.add_argument("--batch-size", type=int, default=int(os.getenv("BATCH_SIZE", "100")))
    p.add_argument("--retries", type=int, default=int(os.getenv("MAX_RETRIES", "6")))
    p.add_argument("--connect-timeout", type=float, default=float(os.getenv("CONNECT_TIMEOUT", "5")))
    p.add_argument("--read-timeout", type=float, default=float(os.getenv("READ_TIMEOUT", "30")))
    return p.parse_args()

# ------------------ Transform helpers ------------------

def split_friends(s: Optional[str]) -> List[str]:
    """Split a comma-delimited string into a trimmed list; tolerates None/empty."""
    if not s:
        return []
    return [p.strip() for p in s.split(",") if p.strip()]

def epoch_to_iso8601_utc(epoch: Optional[int | float]) -> Optional[str]:
    """
    Convert epoch to ISO8601 UTC with 'Z'.
    Auto-detect unit (s/ms/µs/ns) by magnitude.
    Returns None for invalid, negative, or future timestamps.
    """
    # Reject negatives and null
    if epoch is None or epoch < 0:
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

def validate_iso8601_utc(z: Optional[str]) -> bool:
    """True iff string is ISO8601 UTC with 'Z' suffix (None allowed)."""
    if z is None:
        return True
    return bool(ISO_UTC_Z_RE.match(z))

# ------------------ HTTP client (async + retries) ------------------

class AsyncETL:
    """
    Async HTTP client wrapper for the Animals API.
    Handles retries, backoff, logging, and concurrency.
    """
    def __init__(self, base_url: str, connect_timeout: float, read_timeout: float, retries: int, concurrency: int):
        self.base_url = base_url.rstrip("/")
        self.timeout = httpx.Timeout(
            connect=connect_timeout,
            read=read_timeout,
            write=read_timeout,
            pool=read_timeout,
        )
        self.concurrency = max(1, min(32, concurrency))
        self.retries = retries
        self.sem = asyncio.Semaphore(self.concurrency)
        self.client: httpx.AsyncClient | None = None
    
    async def __aenter__(self):
        self.client = httpx.AsyncClient(
            base_url=self.base_url,
            timeout=self.timeout,
            headers={"Accept": "application/json"}
        )
        return self

    async def __aexit__(self, exc_type, exc, tb):
        if self.client is not None:
            await self.client.aclose()

    async def _request(self, method: str, path: str, **kwargs) -> httpx.Response:
        """
        Generic request with retry logic.
        Retries transient 5xx + network errors; fails fast on 4xx. Special 422 handling.
        Each request tagged with X-Request-Id for traceability.
        """
        assert self.client is not None
        last_exc: Exception | None = None
        req_id = kwargs.pop("req_id", str(uuid.uuid4()))
        headers = kwargs.pop("headers", {})
        headers.setdefault("X-Request-Id", req_id)
        kwargs["headers"] = headers
    
        url = self.base_url + path # for logs
        
        for attempt in range(1, self.retries + 1):
            try:
                resp = await self.client.request(method, path, **kwargs)
                status = resp.status_code
                # Retry on transient 5xx
                if status in RETRY_STATUSES:
                    raise httpx.HTTPStatusError(
                        f"server error {status}",
                        request=resp.request, response=resp
                    )
                
                # 422 (validation) handling: log useful details, then fail fast
                if status == 422:
                    try:
                        payload = resp.json()
                    except ValueError:
                        payload = {"detail": (resp.text or "Unprocessable Entity")}
                    detail = payload.get("detail", payload)
                    print(f"[req#{req_id}] 422 validation error on {method} {url}: {detail}", file=sys.stderr)
                    resp.raise_for_status()

                # Fail fast, don’t retry
                if 400 <= status < 500:
                    resp.raise_for_status()

                # Defensive: no other non-2xx should slip through
                if not (200 <= status < 300):
                    if 500 <= status < 600 and status not in RETRY_STATUSES:
                        print(f"[req#{req_id}] [fatal] {method} {url} returned {status}, not retrying", file=sys.stderr)
                    resp.raise_for_status() 
                    
                if attempt > 1:
                    print(f"[req#{req_id}] succeeded after {attempt} attempt(s)", file=sys.stderr)
                return resp
            
            except httpx.HTTPStatusError as e:
                # If it's 4xx, do NOT retry
                status = getattr(e.response, "status_code", None)
                if status is not None and 400 <= status < 500:
                    print(f"[req#{req_id}] [fatal] {method} {url} returned {status}, not retrying", file=sys.stderr)
                    raise
                last_exc = e
            except httpx.HTTPError as e:
                last_exc = e
            
            if attempt < self.retries:
                sleep = min(8.0, 0.5 * (2 ** (attempt - 1))) + random.uniform(0, 0.5)
                params = kwargs.get("params")
                has_json = kwargs.get("json") is not None
                err_kind = f"HTTP {getattr(getattr(last_exc, 'response', None), 'status_code', 'ERR')}" \
                       if isinstance(last_exc, httpx.HTTPStatusError) else "network"
                print(f"[req#{req_id}] [retry {attempt}/{self.retries}] {method} {url} "
                    f"params={params} json={has_json} failed: {err_kind}: {last_exc}. "
                    f"Sleeping {sleep:.2f}s", file=sys.stderr)
                await asyncio.sleep(sleep)
            else:
                print(f"[req#{req_id}] [giving up] {method} {url}: {last_exc}", file=sys.stderr)
                raise last_exc or RuntimeError("request failed")
                
        raise last_exc or RuntimeError("request failed")

    # API wrappers
    async def get_animals_page(self, page: int) -> Dict[str, Any]:
        resp = await self._request("GET", "/animals/v1/animals", params={"page": page})
        try:
            return resp.json()
        except ValueError:
            print(f"[warn] Non-JSON for page {page}", file=sys.stderr)
            return {"items": [], "total_pages": 1}

    async def get_animal(self, animal_id: int) -> Dict[str, Any]:
        resp = await self._request("GET", f"/animals/v1/animals/{animal_id}")
        try:
            return resp.json()
        except ValueError:
            print(f"[warn] non-JSON response for id {animal_id}: {resp.text[:200]}", file=sys.stderr)
            return {}

    async def post_home(self, animals: List[Dict[str, Any]]) -> Dict[str, Any]:
        resp = await self._request("POST", "/animals/v1/home", json=animals)
        try:
            return resp.json()
        except ValueError:
            return {}
        
# ------------------ Pipeline ------------------

def chunked(seq: List[Any], size: int):
    """Yield successive chunks from seq of length <= size."""
    for i in range(0, len(seq), size):
        yield seq[i:i+size]

async def fetch_all_ids(api: AsyncETL) -> List[int]:
    """
    Walk through paginated /animals/v1/animals, return list of IDs.
    Starts with page 1 to get total_pages, then fetches the rest sequentially.
    """
    first = await api.get_animals_page(1)
    total_pages = int(first.get("total_pages", 1))
    ids = [int(item["id"]) for item in first.get("items", [])]
    
    for p in range(2, total_pages + 1):
        page = await api.get_animals_page(p)
        ids.extend(int(item["id"]) for item in page.get("items", []))
    return ids

async def fetch_details_concurrent(api: AsyncETL, ids: List[int]) -> List[Dict[str, Any]]:
    """
    Fetch details concurrently for all IDs, bounded by semaphore.
    Logs progress every 100 records.
    """
    async def worker(_id: int) -> Optional[Dict[str, Any]]:
        async with api.sem:
            try:
                return await api.get_animal(_id)
            except Exception as e:
                print(f"[warn] get_animal({_id}) failed: {e}", file=sys.stderr)
                return None

    tasks = [asyncio.create_task(worker(_id)) for _id in ids]
    results: List[Dict[str, Any]] = []
    done = 0
    for fut in asyncio.as_completed(tasks):
        res = await fut
        if res is not None:
            results.append(res)
        done += 1
        if done % 100 == 0 or done == len(ids):
            print(f"Fetched {done}/{len(ids)} details…")
    return results

def transform_records(details: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Transform raw records:
        • normalize friends → list[str]
        • convert born_at → ISO8601 UTC
    Skips invalid/future timestamps and asserts all outgoing dates are valid.
    """
    transformed: List[Dict[str, Any]] = []
    invalid_born = 0
    for a in details:
        born_iso = epoch_to_iso8601_utc(a.get("born_at")) if a.get("born_at") is not None else None
        
        if born_iso is not None and not validate_iso8601_utc(born_iso):
            born_iso = None
            invalid_born += 1
        
        outgoing = {
            "id": int(a["id"]),
            "name": a["name"],
            "friends": split_friends(a.get("friends", "")),
        }
        
        if born_iso is not None:
            outgoing["born_at"] = born_iso
        transformed.append(outgoing)
    if invalid_born:
        print(f"[warn] Skipped {invalid_born} invalid born_at values after conversion.", file=sys.stderr)
    
    assert all(validate_iso8601_utc(r.get("born_at")) for r in transformed), "Non-UTC ISO8601 detected in outgoing payload"
    return transformed

async def post_batches(api: AsyncETL, transformed: List[Dict[str, Any]], batch_size: int):
    """
    Upload records to /home in batches (≤100).
    Logs batch counts and progress.
    """
    batch_size = max(1, min(100, batch_size))
    batches = list(chunked(transformed, batch_size))
    print(f"Uploading {len(batches)} batch(es)…")
    for i, batch in enumerate(batches, 1):
        await api.post_home(batch)
        print(f"Posted batch {i}/{len(batches)} ({len(batch)} records).")

async def run(args):
    """
    Orchestrate ETL:
        1. Extract IDs
        2. Extract details concurrently
        3. Transform records
        4. Load batches
    """
    print(f"""
        ====== Animals ETL (async) ======
        Base URL       : {args.base_url}
        Concurrency    : {args.concurrency}
        Batch size     : {args.batch_size}
        Retries        : {args.retries}
        Timeouts (s)   : connect={args.connect_timeout} read={args.read_timeout}
        ===============================
    """)
    async with AsyncETL(
        base_url=args.base_url,
        connect_timeout=args.connect_timeout,
        read_timeout=args.read_timeout,
        retries=args.retries,
        concurrency=args.concurrency,
    ) as api:
        print("Listing IDs…")
        ids = await fetch_all_ids(api)
        print(f"Found {len(ids)} ids.")

        print("Fetching details concurrently…")
        details = await fetch_details_concurrent(api, ids)

        print("Transforming…")
        transformed = transform_records(details)

        print("Loading…")
        await post_batches(api, transformed, args.batch_size)

    print("ETL Completed.")

def main():
    args = parse_args()
    try:
        asyncio.run(run(args))
    except KeyboardInterrupt:
        print("Aborted.", file=sys.stderr)

if __name__ == "__main__":
    main()