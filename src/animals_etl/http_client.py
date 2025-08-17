from __future__ import annotations

class AsyncETL:
    def __init__(self, base_url: str, connect_timeout: float, read_timeout: float,
                 retries: int, concurrency: int):
        # Skeleton; full implementation in next commit.
        self.base_url = base_url.rstrip("/")
        self.connect_timeout = connect_timeout
        self.read_timeout = read_timeout
        self.retries = retries
        self.concurrency = concurrency