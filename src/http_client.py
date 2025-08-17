# animals_etl/http_client.py
from __future__ import annotations
import sys, asyncio, random, uuid
from typing import Optional
import httpx

class RetryPolicy:
    def __init__(
        self,
        retries: int = 6,
        backoff_base: float = 0.25,
        backoff_cap: float = 4.0,
        retry_statuses: set[int] | None = None,
    ):
        self.retries = max(1, retries)
        self.backoff_base = backoff_base
        self.backoff_cap = backoff_cap
        self.retry_statuses = retry_statuses or {500, 502, 503, 504}

    def sleep_seconds(self, attempt: int) -> float:
        # exponential (0.5, 1, 2, 4, 8...) + jitter [0..0.5]
        return min(self.backoff_cap, self.backoff_base * (2 ** (attempt - 1))) + random.uniform(0, 0.5)

class HttpClient:
    """
    - Reusable async HTTP client with:
      - base_url
      - httpx timeouts
      - retry policy (5xx + network)
      - 4xx fail fast
    """

    def __init__(
        self,
        base_url: str,
        connect_timeout: float,
        read_timeout: float,
        retries: int,
        *,
        retry_statuses: Optional[set[int]] = None,
        default_headers: Optional[dict[str, str]] = None,
    ):
        self.base_url = base_url.rstrip("/")
        self.timeout = httpx.Timeout(
            connect=connect_timeout,
            read=read_timeout,
            write=read_timeout,
            pool=read_timeout,
        )
        self.policy = RetryPolicy(retries=retries, retry_statuses=retry_statuses)
        self.default_headers = {"Accept": "application/json", **(default_headers or {})}
        self._client: Optional[httpx.AsyncClient] = None

    async def __aenter__(self):
        self._client = httpx.AsyncClient(base_url=self.base_url, timeout=self.timeout, headers=self.default_headers)
        return self

    async def __aexit__(self, exc_type, exc, tb):
        if self._client is not None:
            await self._client.aclose()

    async def request(self, method: str, path: str, **kwargs) -> httpx.Response:
        """
        Generic request with retry logic.
        Retries transient 5xx + network errors; fails fast on 4xx. Special 422 handling.
        Each request tagged with X-Request-Id for traceability.
        """
        assert self._client is not None
        last_exc: Exception | None = None

        req_id = kwargs.pop("req_id", str(uuid.uuid4()))
        headers = kwargs.pop("headers", {})
        headers.setdefault("X-Request-Id", req_id)
        kwargs["headers"] = headers

        url = self.base_url + path # for logs

        for attempt in range(1, self.policy.retries + 1):
            try:
                resp = await self._client.request(method, path, **kwargs)
                status = resp.status_code

                # Retry on transient 5xx
                if status in self.policy.retry_statuses:
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
                    if 500 <= status < 600 and status not in self.policy.retry_statuses:
                        print(f"[req#{req_id}] [fatal] {method} {url} returned {status}, not retrying", file=sys.stderr)
                    resp.raise_for_status()

                if attempt > 1:
                    print(f"[req#{req_id}] succeeded after {attempt} attempt(s)", file=sys.stderr)
                return resp

            except httpx.HTTPStatusError as e:
                status = getattr(e.response, "status_code", None)
                if status is not None and 400 <= status < 500:
                    print(f"[req#{req_id}] [fatal] {method} {url} returned {status}, not retrying", file=sys.stderr)
                    raise
                last_exc = e

            except httpx.HTTPError as e:
                last_exc = e

            if attempt < self.policy.retries:
                sleep = self.policy.sleep_seconds(attempt)
                params = kwargs.get("params")
                has_json = kwargs.get("json") is not None
                err_kind = f"HTTP {getattr(getattr(last_exc, 'response', None), 'status_code', 'ERR')}" \
                           if isinstance(last_exc, httpx.HTTPStatusError) else "network"
                print(f"[req#{req_id}] [retry {attempt}/{self.policy.retries}] {method} {url} "
                      f"params={params} json={has_json} failed: {err_kind}: {last_exc}. "
                      f"Sleeping {sleep:.2f}s", file=sys.stderr)
                await asyncio.sleep(sleep)
            else:
                print(f"[req#{req_id}] [giving up] {method} {url}: {last_exc}", file=sys.stderr)
                raise last_exc or RuntimeError("request failed")

        raise last_exc or RuntimeError("request failed")
