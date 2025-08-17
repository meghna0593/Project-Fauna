import pytest
from animals_etl.pipeline import fetch_all_ids, fetch_details_concurrent, transform_records, post_batches

class FakeAPI:
    def __init__(self, pages, details):
        self._pages = pages
        self._details = details
        self.posted = []

    async def list_animals(self, page: int):
        items = self._pages.get(page, [])
        return {"page": page, "total_pages": max(self._pages), "items": items}

    async def get_animal(self, animal_id: int):
        return self._details[animal_id]

    async def post_home(self, batch):
        self.posted.append(list(batch))
        return {"ok": True}

@pytest.mark.asyncio
async def test_fetch_ids_and_details_and_transform_and_post_batches():
    # Two pages: page1 has ids 1,2; page2 has 3
    pages = {
        1: [{"id": 1, "name": "Dog"}, {"id": 2, "name": "Cat"}],
        2: [{"id": 3, "name": "Mouse"}],
    }
    details = {
        1: {"id": 1, "name": "Dog", "friends": "Kangaroo, Sea Lions", "born_at": None},
        2: {"id": 2, "name": "Cat", "friends": "", "born_at": 1348692957651},
        3: {"id": 3, "name": "Mouse", "friends": "Dog", "born_at": None},
    }
    api = FakeAPI(pages, details)

    ids = await fetch_all_ids(api, page_concurrency=2)
    assert sorted(ids) == [1, 2, 3]

    # Fetch details concurrently
    dd = await fetch_details_concurrent(api, ids, concurrency=4)
    assert {d["id"] for d in dd} == {1, 2, 3}

    # Transform
    transformed = transform_records(dd)
    assert isinstance(transformed, list) and all(isinstance(r, dict) for r in transformed)
    assert len(transformed) == 3
    assert transformed[0]["friends"] in (["Kangaroo","Sea Lions"], [])
    # born_at only present when valid
    assert "born_at" not in transformed[0]
    assert "born_at" in transformed[1]  

    # Post in batches of 2
    await post_batches(api, transformed, batch_size=2)
    assert len(api.posted) == 2           # 2 batches (2 + 1)
    assert api.posted[0][0]["id"] == 1
    assert sum(len(b) for b in api.posted) == 3
