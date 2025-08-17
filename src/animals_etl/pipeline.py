from __future__ import annotations
import sys, asyncio
from typing import List, Optional
from .api import AnimalsAPI
from .models import AnimalRaw, AnimalDetail, AnimalsBatch
from .utils import chunked, split_friends, epoch_to_iso8601_utc, validate_iso8601_utc

async def fetch_all_ids(api: AnimalsAPI, page_concurrency: int = 6) -> List[int]:
    """
    Walk through paginated /animals/v1/animals, return list of IDs.
    Starts with page 1 to get total_pages, then fetches concurrently.
    """
    first: AnimalRaw = await api.list_animals(1)
    total_pages = int(first.get("total_pages", 1))
    ids = [int(item["id"]) for item in first.get("items", [])]
    if total_pages <= 1:
        return ids

    sem = asyncio.Semaphore(max(1, page_concurrency))

    async def fetch_page(p: int) -> List[int]:
        async with sem:
            page = await api.list_animals(p)
            return [int(item["id"]) for item in page.get("items", [])]

    tasks = [asyncio.create_task(fetch_page(p)) for p in range(2, total_pages + 1)]
    for fut in asyncio.as_completed(tasks):
        ids.extend(await fut)
    return ids

async def fetch_details_concurrent(api: AnimalsAPI, ids: List[int], concurrency: int) -> List[AnimalDetail]:
    """
    Fetch details concurrently for all IDs, bounded by semaphore.
    Logs progress every 100 records.
    """
    sem = asyncio.Semaphore(max(1, concurrency))
    async def worker(_id: int) -> Optional[AnimalDetail]:
        async with sem:
            try:
                return await api.get_animal(_id)
            except Exception as e:
                print(f"[warn] get_animal({_id}) failed: {e}", file=sys.stderr)
                return None

    tasks = [asyncio.create_task(worker(_id)) for _id in ids]
    results: List[AnimalDetail] = []
    done = 0
    for fut in asyncio.as_completed(tasks):
        res = await fut
        if res is not None:
            results.append(res)
        done += 1
        if done % 100 == 0 or done == len(ids):
            print(f"Fetched {done}/{len(ids)} details…")
    return results

def transform_records(details: List[AnimalDetail]) -> AnimalsBatch:
    """
    Transform raw records:
        • normalize friends → list[str]
        • convert born_at → ISO8601 UTC
    Skips invalid/future timestamps and asserts all outgoing dates are valid.
    """
    transformed: AnimalsBatch = []
    invalid_born = 0
    for a in details:
        born_iso = epoch_to_iso8601_utc(a.get("born_at")) if a.get("born_at") is not None else None
        if born_iso is not None and not validate_iso8601_utc(born_iso):
            born_iso = None
            invalid_born += 1
        
        rec = {
            "id": int(a["id"]),
            "name": a["name"],
            "friends": split_friends(a.get("friends", "")),
        }
        
        if born_iso is not None:
            rec["born_at"] = born_iso
        transformed.append(rec)
    
    if invalid_born:
        print(f"[warn] Skipped {invalid_born} invalid born_at values after conversion.", file=sys.stderr)
    
    assert all(validate_iso8601_utc(r.get("born_at")) for r in transformed), "Non-UTC ISO8601 detected in outgoing payload"
    return transformed

async def post_batches(api: AnimalsAPI, transformed: AnimalsBatch, batch_size: int):
    """
    Upload records to /home in batches (≤100).
    Logs batch counts and progress.
    """
    batch_size = max(1, min(100, batch_size))
    n_batches = (len(transformed) + batch_size - 1) // batch_size  # ceil division

    print(f"Uploading {n_batches} batch(es)…")
    for i, batch in enumerate(chunked(transformed, batch_size), start=1):
        await api.post_home(batch)
        print(f"Posted batch {i}/{n_batches} ({len(batch)} records).")
