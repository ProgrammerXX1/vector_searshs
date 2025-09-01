# app/main.py
from __future__ import annotations

from fastapi import FastAPI
from fastapi.responses import JSONResponse
from dotenv import load_dotenv

from app.schemas.schemas import (
    BatchIndexRequest, BatchIndexResponse,
    NearVectorRequest, NearVectorResponse, NearVectorResponseItem,
    BM25Request, HitsResponse,
    HybridRequest, MMRRequest, NearObjectRequest,
    SeedRequest, SeedResponse,
)
from app.core.weaviate_client import (
    connect, drop_collection, ensure_schema, get_client, insert_many, near_vector_search,
    bm25_search, hybrid_rrf_search, mmr_search, near_object_search,
    seed_data,
)

load_dotenv()

app = FastAPI(title="Mini Vector API", version="0.1.0")


@app.get("/health")
def health():
    try:
        connect()
        ok = get_client().is_connected()
        return {"weaviate_connected": ok}
    except Exception as e:
        return JSONResponse({"weaviate_connected": False, "error": str(e)}, status_code=500)


@app.post("/schema/init")
def schema_init():
    ensure_schema()
    return {"ok": True}


@app.post("/schema/drop")
def schema_drop():
    ok = drop_collection()
    return {"ok": ok}


@app.post("/schema/reset")
def schema_reset():
    drop_collection()
    ensure_schema()
    return {"ok": True}


@app.post("/chunks/index", response_model=BatchIndexResponse)
def chunks_index(req: BatchIndexRequest):
    """
    ⚠️ Первая вставка фиксирует размерность векторного индекса.
    Все следующие векторы должны быть той же длины.
    """
    ids = insert_many([it.model_dump() for it in req.items])
    return BatchIndexResponse(ids=ids)


@app.post("/data/seed", response_model=SeedResponse)
def data_seed(req: SeedRequest):
    ids = seed_data(count=req.count, dim=req.dim, case_id=req.case_id)
    return SeedResponse(inserted=len(ids), ids=ids)


@app.post("/search/near", response_model=NearVectorResponse)
def search_near(req: NearVectorRequest):
    hits_raw = near_vector_search(
        vector=req.vector,
        limit=req.limit,
        case_id=req.case_id,
        document_id=req.document_id,
        lang=req.lang,
    )
    hits = [NearVectorResponseItem(**h) for h in hits_raw]
    return NearVectorResponse(hits=hits)


@app.post("/search/bm25", response_model=HitsResponse)
def search_bm25(req: BM25Request):
    hits_raw = bm25_search(
        query_text=req.query,
        limit=req.limit,
        case_id=req.case_id,
        document_id=req.document_id,
        lang=req.lang,
        doc_types=req.doc_types,
    )
    hits = [NearVectorResponseItem(**h) for h in hits_raw]
    return HitsResponse(hits=hits)


@app.post("/search/hybrid", response_model=HitsResponse)
def search_hybrid(req: HybridRequest):
    hits_raw = hybrid_rrf_search(
        vector=req.vector,
        query_text=req.query,
        k_dense=req.k_dense,
        k_bm25=req.k_bm25,
        limit=req.limit,
        case_id=req.case_id,
        document_id=req.document_id,
        lang=req.lang,
        doc_types=req.doc_types,
    )
    hits = [NearVectorResponseItem(**h) for h in hits_raw]
    return HitsResponse(hits=hits)


@app.post("/search/mmr", response_model=HitsResponse)
def search_mmr(req: MMRRequest):
    hits_raw = mmr_search(
        vector=req.vector,
        k_candidates=req.k_candidates,
        top_n=req.top_n,
        lambda_mult=req.lambda_mult,
        case_id=req.case_id,
        document_id=req.document_id,
        lang=req.lang,
    )
    hits = [NearVectorResponseItem(**h) for h in hits_raw]
    return HitsResponse(hits=hits)


@app.post("/search/near-object", response_model=HitsResponse)
def search_near_object(req: NearObjectRequest):
    hits_raw = near_object_search(
        object_id=req.object_id,
        limit=req.limit,
        case_id=req.case_id,
        document_id=req.document_id,
        lang=req.lang,
    )
    hits = [NearVectorResponseItem(**h) for h in hits_raw]
    return HitsResponse(hits=hits)
