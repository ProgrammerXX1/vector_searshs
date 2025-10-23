# app/schemas/schemas.py
from __future__ import annotations
from typing import List, Optional
from pydantic import BaseModel, Field

# ----- вставка/индексация -----
class ReportItem(BaseModel):
    # все поля опциональные, чтобы можно было заполнять постепенно
    type_document: Optional[str] = None
    view_document: Optional[str] = None

    post_main: Optional[str] = None
    post_main_fn: Optional[str] = None
    city_fix: Optional[str] = None
    date_doc: Optional[str] = Field(None, description="ISO date, e.g. 2025-04-17")

    report_begin: Optional[str] = None
    report_next: Optional[str] = None
    report_end: Optional[str] = None

    post_new: Optional[str] = None
    post_new_fn: Optional[str] = None

    document_title: Optional[str] = None
    position_title: Optional[str] = None
    doc_kind: Optional[str] = None


class IndexReportsRequest(BaseModel):
    items: List[ReportItem]


class IndexReportsResponse(BaseModel):
    ids: List[Optional[str]]


# ----- BM25 поиск -----
class BM25Request(BaseModel):
    query: str
    limit: int = 10
    # по умолчанию ищем по всем текстовым полям из схемы
    query_props: List[str] = Field(
        default_factory=lambda: [
            "type_document", "view_document",
            "post_main", "post_main_fn",
            "city_fix", "report_begin", "report_next", "report_end",
            "post_new", "post_new_fn",
            "document_title", "position_title", "doc_kind"
        ]
    )
    # необязательные точные фильтры (например, по типу, городу, виду дока)
    filters: dict = Field(default_factory=dict)


class Hit(BaseModel):
    id: str
    score: float
    properties: dict


class HitsResponse(BaseModel):
    hits: List[Hit]

class UploadReportResponse(BaseModel):
    report_id: str
    chunk_ids: List[str]
    chunks_count: int

class ChunkBM25Request(BaseModel):
    query: str
    limit: int = 10
    query_props: List[str] = ["text", "section"]
    filters: dict = {}
class UploadReportResponse(BaseModel):
    report_id: Optional[str]
    chunk_ids: List[str]
    chunks_count: int

class UploadReportsResponse(BaseModel):
    total: int
    success: int
    report_ids: List[str]
    failed: List[str]