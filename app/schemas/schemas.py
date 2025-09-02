# app/schemas.py
from __future__ import annotations
from typing import List, Optional
from pydantic import BaseModel, Field

from typing import List, Optional, Union, Literal
from pydantic import BaseModel

class ListChunksRequest(BaseModel):
    limit: int = 50
    offset: int = 0
    case_id: Optional[int] = None
    document_id: Optional[int] = None
    lang: Optional[str] = None
    doc_types: Optional[List[str]] = None
    include_vector: bool = False

class ChunkRecord(BaseModel):
    id: str
    properties: dict
    vector: Optional[List[float]] = None

class ListChunksResponse(BaseModel):
    items: List[ChunkRecord]
    next_offset: Optional[int] = None


class ChunkItem(BaseModel):
    uuid: Optional[str] = Field(None)
    vector: Optional[List[float]] = None          # ← теперь опционально
    properties: dict                               # text может быть внутри properties

class BatchIndexRequest(BaseModel):
    items: List[ChunkItem]

class BatchIndexResponse(BaseModel):
    ids: List[Optional[str]]

class NearVectorRequest(BaseModel):
    text: Optional[str] = None                     # ← НОВОЕ
    vector: Optional[List[float]] = None           # ← теперь опционально
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

# сидер
class SeedRequest(BaseModel):
    count: int = 200
    dim: int = 4
    case_id: int = 1

class SeedResponse(BaseModel):
    inserted: int
    ids: List[str]

# BM25
class BM25Request(BaseModel):
    query: str
    limit: int = 10
    case_id: Optional[int] = None
    document_id: Optional[int] = None
    lang: Optional[str] = None
    doc_types: Optional[List[str]] = None

class HitsResponse(BaseModel):
    hits: List[NearVectorResponseItem]

# Hybrid
class HybridRequest(BaseModel):
    text: Optional[str] = None                     # ← НОВОЕ (альтернатива vector)
    vector: Optional[List[float]] = None
    query: str
    k_dense: int = 100
    k_bm25: int = 200
    limit: int = 10
    case_id: Optional[int] = None
    document_id: Optional[int] = None
    lang: Optional[str] = None
    doc_types: Optional[List[str]] = None

# MMR
class MMRRequest(BaseModel):
    text: Optional[str] = None                     # ← НОВОЕ
    vector: Optional[List[float]] = None
    k_candidates: int = 100
    top_n: int = 20
    lambda_mult: float = 0.5
    case_id: Optional[int] = None
    document_id: Optional[int] = None
    lang: Optional[str] = None

# Near-object — без изменений
class NearObjectRequest(BaseModel):
    object_id: str
    limit: int = 10
    case_id: Optional[int] = None
    document_id: Optional[int] = None
    lang: Optional[str] = None

# Индексация по тексту (автоэмбеддинг)
class TextChunkItem(BaseModel):
    uuid: Optional[str] = None
    properties: dict  # ОБЯЗАТЕЛЬНО должен содержать "text"

class BatchIndexTextRequest(BaseModel):
    items: List[TextChunkItem]

class SeedTextRequest(BaseModel):
    count: int = 200
    case_id: int = 1
    lang: str = "ru"

# --- Поиск по пострадавшим ---
class VictimSearchRequest(BaseModel):
    text: Optional[str] = None
    vector: Optional[List[float]] = None
    limit: int = 10
    case_id: Optional[int] = None
    iin: Optional[str] = None
    full_name: Optional[str] = None
    lang: Optional[str] = None
class VictimSearchResponse(HitsResponse): pass

# --- Поиск по экспертным ответам ---
class ExpertSearchRequest(BaseModel):
    text: Optional[str] = None
    vector: Optional[List[float]] = None
    limit: int = 10
    case_id: Optional[int] = None
    specialty: Optional[str] = None
    lang: Optional[str] = None
    mode: str = "hybrid"  # "dense" | "bm25" | "hybrid"
class ExpertSearchResponse(HitsResponse): pass

# --- Поиск по ответам/позициям прокурора ---
class ProsecutorSearchRequest(BaseModel):
    text: Optional[str] = None
    vector: Optional[List[float]] = None
    query: Optional[str] = None
    limit: int = 10
    case_id: Optional[int] = None
    filing_type: Optional[str] = None
    lang: Optional[str] = None
    mode: str = "hybrid"
class ProsecutorSearchResponse(HitsResponse): pass

class VictimSearchRequest(BaseModel):
    text: Optional[str] = None
    vector: Optional[List[float]] = None
    limit: int = 10
    case_id: Optional[Union[int, List[int]]] = None
    iin: Optional[str] = None
    full_name: Optional[Union[str, List[str]]] = None
    lang: Optional[Union[str, List[str]]] = None
class VictimSearchResponse(HitsResponse): pass

class ExpertSearchRequest(BaseModel):
    text: Optional[str] = None
    vector: Optional[List[float]] = None
    limit: int = 10
    case_id: Optional[Union[int, List[int]]] = None
    specialty: Optional[Union[str, List[str]]] = None
    lang: Optional[Union[str, List[str]]] = None
    mode: Literal["dense", "bm25", "hybrid"] = "hybrid"
class ExpertSearchResponse(HitsResponse): pass

class ProsecutorSearchRequest(BaseModel):
    text: Optional[str] = None
    vector: Optional[List[float]] = None
    query: Optional[str] = None
    limit: int = 10
    case_id: Optional[Union[int, List[int]]] = None
    filing_type: Optional[Union[str, List[str]]] = None
    lang: Optional[Union[str, List[str]]] = None
    mode: Literal["dense", "bm25", "hybrid"] = "hybrid"
class ProsecutorSearchResponse(HitsResponse): pass