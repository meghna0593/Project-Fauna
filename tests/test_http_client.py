import types
import pytest
from http_client import HttpClient, RetryPolicy

class FakeResponse:
    def __init__(self, status_code: int, json_data=None):
        self.status_code = status_code
        self._json = json_data or {}
        self.request = types.SimpleNamespace()

    def json(self):
        return self._json

class FakeAsyncClient:
    """Returns a sequence of responses for each call to request()."""
    def __init__(self, responses):
        self._responses = list(responses)
        self.calls = 0

    async def request(self, method, path, **kwargs):
        self.calls += 1
        if not self._responses:
            raise RuntimeError("No more fake responses")
        resp = self._responses.pop(0)
        return resp

    async def aclose(self):
        pass

@pytest.mark.asyncio
async def test_http_client_retries_then_succeeds(monkeypatch):
    hc = HttpClient(base_url="http://where_the_animals_at", connect_timeout=1, read_timeout=1, retries=3)
    hc.policy = RetryPolicy(retries=3, backoff_base=0.0, backoff_cap=0.0)

    # Force no waiting at all
    monkeypatch.setattr(hc.policy, "sleep_seconds", lambda attempt: 0)

    fake = FakeAsyncClient([
        FakeResponse(500),             # triggers retry
        FakeResponse(200, {"ok": 1}),  # success
    ])

    async with hc:
        hc._client = fake
        resp = await hc.request("GET", "/bow")
        assert resp.status_code == 200
        assert resp.json() == {"ok": 1}