# app/schemas.py
from __future__ import annotations
from typing import List, Optional
from pydantic import BaseModel, Field

# уже были:
class ChunkItem(BaseModel):
    uuid: Optional[str] = Field(None)
    vector: List[float]
    properties: dict

class BatchIndexRequest(BaseModel):
    items: List[ChunkItem]

class BatchIndexResponse(BaseModel):
    ids: List[Optional[str]]

class NearVectorRequest(BaseModel):
    vector: List[float]
    limit: int = 10
    case_id: Optional[int] = None
    document_id: Optional[int] = None
    lang: Optional[str] = None

class NearVectorResponseItem(BaseModel):
    id: str
    score: float
    distance: float
    properties: dict

class NearVectorResponse(BaseModel):
    hits: List[NearVectorResponseItem]

# 🔹 сидер
class SeedRequest(BaseModel):
    count: int = 200
    dim: int = 4
    case_id: int = 1

class SeedResponse(BaseModel):
    inserted: int
    ids: List[str]

# 🔹 BM25
class BM25Request(BaseModel):
    query: str
    limit: int = 10
    case_id: Optional[int] = None
    document_id: Optional[int] = None
    lang: Optional[str] = None
    doc_types: Optional[List[str]] = None

class HitsResponse(BaseModel):
    hits: List[NearVectorResponseItem]

# 🔹 Hybrid (RRF: dense + bm25)
class HybridRequest(BaseModel):
    vector: List[float]
    query: str
    k_dense: int = 100
    k_bm25: int = 200
    limit: int = 10
    case_id: Optional[int] = None
    document_id: Optional[int] = None
    lang: Optional[str] = None
    doc_types: Optional[List[str]] = None

# 🔹 MMR
class MMRRequest(BaseModel):
    vector: List[float]
    k_candidates: int = 100
    top_n: int = 20
    lambda_mult: float = 0.5
    case_id: Optional[int] = None
    document_id: Optional[int] = None
    lang: Optional[str] = None

# 🔹 near-object (по вектору существующего объекта)
class NearObjectRequest(BaseModel):
    object_id: str
    limit: int = 10
    case_id: Optional[int] = None
    document_id: Optional[int] = None
    lang: Optional[str] = None
