from __future__ import annotations
import sys
from typing import Any, Dict

from http_client import HttpClient

from .models import AnimalRaw, AnimalDetail, AnimalsBatch

class AnimalsAPI:
    def __init__(self, http: HttpClient):
        self.http = http

    async def list_animals(self, page: int) -> AnimalRaw:
        resp = await self.http.request("GET", "/animals/v1/animals", params={"page": page})
        try:
            return resp.json()
        except ValueError:
            print(f"[warn] Non-JSON for page {page}", file=sys.stderr)
            return {"items": [], "total_pages": 1, "page": page}

    async def get_animal(self, animal_id: int) -> AnimalDetail:
        resp = await self.http.request("GET", f"/animals/v1/animals/{animal_id}")
        try:
            return resp.json()
        except ValueError:
            print(f"[warn] non-JSON response for id {animal_id}: {resp.text[:200]}", file=sys.stderr)
            return {}

    async def post_home(self, batch: AnimalsBatch) -> Dict[str, Any]:
        resp = await self.http.request("POST", "/animals/v1/home", json=batch)
        try:
            return resp.json()
        except ValueError:
            return {}
