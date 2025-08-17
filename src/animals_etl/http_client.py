from __future__ import annotations
import uuid, sys, asyncio, random
from typing import Any, Dict, List, Optional
import httpx
from .utils import RETRY_STATUSES

from .models import AnimalRaw, AnimalDetail, AnimalsBatch
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
        self.client: Optional[httpx.AsyncClient] = None

    async def __aenter__(self):
        self.client = httpx.AsyncClient(
            base_url=self.base_url,
            timeout=self.timeout,
            headers={"Accept": "application/json"},
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
                    raise httpx.HTTPStatusError(f"server error {status}", request=resp.request, response=resp)
                
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
    async def list_animals(self, page: int) -> AnimalRaw:
        resp = await self._request("GET", "/animals/v1/animals", params={"page": page})
        try:
            return resp.json()
        except ValueError:
            print(f"[warn] Non-JSON for page {page}", file=sys.stderr)
            return {"items": [], "total_pages": 1}

    async def get_animal(self, animal_id: int) -> AnimalDetail:
        resp = await self._request("GET", f"/animals/v1/animals/{animal_id}")
        try:
            return resp.json()
        except ValueError:
            print(f"[warn] non-JSON response for id {animal_id}: {resp.text[:200]}", file=sys.stderr)
            return {}

    async def post_home(self, animals: AnimalsBatch) -> Dict[str, Any]:
        resp = await self._request("POST", "/animals/v1/home", json=animals)
        try:
            return resp.json()
        except ValueError:
            return {}
