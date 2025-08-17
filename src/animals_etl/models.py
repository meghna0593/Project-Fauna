"""
TypedDict models for requests and responses to/from the Animals API.

Includes:
- BaseAnimal: minimal fields from /animals/v1/animals
- AnimalRaw: paginated response
- AnimalDetail: detailed record from /animals/v1/animals/{id}
- AnimalTransformed: ETL output (friends split, born_at ISO-8601)
- AnimalsBatch: list of transformed animals ready for POST

"""

from __future__ import annotations
from typing import TypedDict, List, Optional

# GET /animals/v1/animals (items)
class BaseAnimal(TypedDict, total=False):
    id: int
    name: str
    born_at: Optional[int]   # epoch

# GET /animals/v1/animals (page)
class AnimalRaw(TypedDict):
    page: int
    total_pages: int
    items: List[BaseAnimal]

# GET /animals/v1/animals/{id}
class AnimalDetail(TypedDict, total=False):
    id: int
    name: str
    friends: str             # comma string
    born_at: Optional[int]   # epoch

# POST /animals/v1/home
class AnimalTransformed(TypedDict, total=False):
    id: int
    name: str
    friends: List[str]       # transformed
    born_at: Optional[str]   # ISO 8601 Z

AnimalsBatch = List[AnimalTransformed]
