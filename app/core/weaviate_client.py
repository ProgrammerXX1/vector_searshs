# app/core/weaviate_client.py
from __future__ import annotations

import os
import atexit
import logging
from typing import Optional, List, Dict, Any

from weaviate import WeaviateClient
from weaviate.connect import ConnectionParams
from weaviate.classes.init import AdditionalConfig, Timeout
from weaviate.classes.config import Configure, Property, DataType, VectorDistances
from weaviate.classes.query import Filter
from weaviate.classes.data import DataObject

import uuid as _uuid
import numpy as np

logger = logging.getLogger(__name__)

# -------------------------
# 8 коллекций (схемы)
# -------------------------
CASE = "Case"
VICTIM = "VictimProfile"
EXPERT = "ExpertAnswer"
PROSECUTOR = "ProsecutorAnswer"
DOC_CHUNK = "DocumentChunk"
FIN_TX = "FinancialTransaction"
COMM = "CommunicationRecord"
RULING = "CourtRuling"

ALL_COLLECTIONS = [CASE, VICTIM, EXPERT, PROSECUTOR, DOC_CHUNK, FIN_TX, COMM, RULING]

# -------------------------
# Подключение клиента
# -------------------------
_CLIENT: Optional[WeaviateClient] = None


def _str_to_bool(v: str) -> bool:
    return str(v).lower() in ("1", "true", "yes", "y")


def get_client() -> WeaviateClient:
    global _CLIENT
    if _CLIENT is None:
        _CLIENT = WeaviateClient(
            connection_params=ConnectionParams.from_params(
                http_host=os.getenv("WEAVIATE_HTTP_HOST", "localhost"),
                http_port=int(os.getenv("WEAVIATE_HTTP_PORT", 8080)),
                http_secure=_str_to_bool(os.getenv("WEAVIATE_HTTP_SECURE", "false")),
                grpc_host=os.getenv("WEAVIATE_GRPC_HOST", "localhost"),
                grpc_port=int(os.getenv("WEAVIATE_GRPC_PORT", 50051)),
                grpc_secure=_str_to_bool(os.getenv("WEAVIATE_GRPC_SECURE", "false")),
            ),
            additional_config=AdditionalConfig(grpc=True, timeout=Timeout(init=10)),
            skip_init_checks=True,
        )
    return _CLIENT


def connect() -> None:
    c = get_client()
    if not c.is_connected():
        c.connect()


def is_connected() -> bool:
    try:
        return get_client().is_connected()
    except Exception:
        return False


def close_client() -> None:
    global _CLIENT
    if _CLIENT is None:
        return
    try:
        if _CLIENT.is_connected():
            _CLIENT.close()
    except Exception:
        pass


atexit.register(close_client)

# -------------------------
# Схема (создаём 8 коллекций)
# -------------------------
def ensure_schema() -> None:
    """
    Создаёт/додобавляет 8 коллекций юр-ИИ.
    Векторизатор отключён (vectorizer=none) — вектора подаём сами.
    Индекс: HNSW + cosine.
    """
    connect()
    client = get_client()

    def _exists(name: str) -> bool:
        return client.collections.exists(name)

    def _create(name: str, props: list[Property]):
        client.collections.create(
            name=name,
            properties=props,
            vectorizer_config=Configure.Vectorizer.none(),
            vector_index_config=Configure.VectorIndex.hnsw(distance_metric=VectorDistances.COSINE),
        )

    schemas: dict[str, list[Property]] = {
        CASE: [
            Property(name="case_id", data_type=DataType.INT),
            Property(name="title", data_type=DataType.TEXT),
            Property(name="status", data_type=DataType.TEXT),
            Property(name="opened_at", data_type=DataType.DATE),
            Property(name="lang", data_type=DataType.TEXT),
            Property(name="notes", data_type=DataType.TEXT),
        ],
        VICTIM: [
            Property(name="person_id", data_type=DataType.INT),
            Property(name="case_id", data_type=DataType.INT),
            Property(name="full_name", data_type=DataType.TEXT),
            Property(name="iin", data_type=DataType.TEXT),
            Property(name="birthdate", data_type=DataType.DATE),
            Property(name="contacts", data_type=DataType.TEXT),
            Property(name="notes", data_type=DataType.TEXT),
            Property(name="lang", data_type=DataType.TEXT),
        ],
        EXPERT: [
            Property(name="expert_id", data_type=DataType.INT),
            Property(name="case_id", data_type=DataType.INT),
            Property(name="specialty", data_type=DataType.TEXT),
            Property(name="question", data_type=DataType.TEXT),
            Property(name="answer", data_type=DataType.TEXT),
            Property(name="answered_at", data_type=DataType.DATE),
            Property(name="lang", data_type=DataType.TEXT),
        ],
        PROSECUTOR: [
            Property(name="prosecutor_id", data_type=DataType.INT),
            Property(name="case_id", data_type=DataType.INT),
            Property(name="filing_type", data_type=DataType.TEXT),
            Property(name="text", data_type=DataType.TEXT),
            Property(name="filed_at", data_type=DataType.DATE),
            Property(name="lang", data_type=DataType.TEXT),
        ],
        DOC_CHUNK: [
            Property(name="case_id", data_type=DataType.INT),
            Property(name="document_id", data_type=DataType.INT),
            Property(name="chunk_idx", data_type=DataType.INT),
            Property(name="source_page", data_type=DataType.INT),
            Property(name="doc_type", data_type=DataType.TEXT),
            Property(name="text", data_type=DataType.TEXT),
            Property(name="created_at", data_type=DataType.DATE),
            Property(name="lang", data_type=DataType.TEXT),
        ],
        FIN_TX: [
            Property(name="case_id", data_type=DataType.INT),
            Property(name="iban", data_type=DataType.TEXT),
            Property(name="account", data_type=DataType.TEXT),
            Property(name="amount", data_type=DataType.NUMBER),
            Property(name="currency", data_type=DataType.TEXT),
            Property(name="counterparty", data_type=DataType.TEXT),
            Property(name="timestamp", data_type=DataType.DATE),
            Property(name="note", data_type=DataType.TEXT),
            Property(name="lang", data_type=DataType.TEXT),
        ],
        COMM: [
            Property(name="case_id", data_type=DataType.INT),
            Property(name="channel", data_type=DataType.TEXT),
            Property(name="sender", data_type=DataType.TEXT),
            Property(name="receiver", data_type=DataType.TEXT),
            Property(name="timestamp", data_type=DataType.DATE),
            Property(name="content", data_type=DataType.TEXT),
            Property(name="lang", data_type=DataType.TEXT),
        ],
        RULING: [
            Property(name="case_id", data_type=DataType.INT),
            Property(name="court", data_type=DataType.TEXT),
            Property(name="ruling_type", data_type=DataType.TEXT),
            Property(name="session_date", data_type=DataType.DATE),
            Property(name="text", data_type=DataType.TEXT),
            Property(name="lang", data_type=DataType.TEXT),
        ],
    }

    for name, props in schemas.items():
        if not _exists(name):
            _create(name, props)
            logger.info("✅ created collection %s", name)
        else:
            col = client.collections.get(name)
            existing = {p.name for p in col.config.get().properties}
            for p in props:
                if p.name not in existing:
                    col.config.add_property(p)
            logger.info("ℹ️ ensured props for %s", name)

# -------------------------
# Хелперы и универсальные операции
# -------------------------
def _normalize_vector(vec) -> list[float] | None:
    """Привести вектор к list[float], поддержка dict-мультивекторов."""
    if vec is None:
        return None
    if isinstance(vec, (list, tuple)):
        try:
            return [float(x) for x in vec]
        except Exception:
            return None
    if isinstance(vec, np.ndarray):
        return [float(x) for x in vec.tolist()]
    if isinstance(vec, dict):
        for key in ("default", "vector", "vectors", "data", "values", "value", "embedding"):
            v = vec.get(key)
            if isinstance(v, np.ndarray):
                return [float(x) for x in v.tolist()]
            if isinstance(v, (list, tuple)):
                try:
                    return [float(x) for x in v]
                except Exception:
                    pass
        for _, v in vec.items():
            if isinstance(v, np.ndarray):
                return [float(x) for x in v.tolist()]
            if isinstance(v, (list, tuple)) and (len(v) == 0 or isinstance(v[0], (int, float))):
                try:
                    return [float(x) for x in v]
                except Exception:
                    pass
        try:
            keys = sorted(vec.keys(), key=lambda k: int(k) if str(k).isdigit() else str(k))
            return [float(vec[k]) for k in keys]
        except Exception:
            return None
    return None


def insert_many_into(collection: str, items: list[dict]) -> list[str | None]:
    """
    items: [{"uuid": str|None, "vector": list[float], "properties": {...}}]
    """
    connect()
    col = get_client().collections.get(collection)
    objs: list[DataObject] = []
    ids: list[str] = []
    for it in items:
        uid = it.get("uuid") or str(_uuid.uuid4())
        ids.append(uid)
        objs.append(DataObject(uuid=uid, properties=it["properties"], vector=it["vector"]))
    try:
        col.data.insert_many(objs)
        return ids
    except Exception as e:
        logger.error("insert_many_into[%s] failed: %s → fallback per-item", collection, e)
        out: list[str | None] = []
        for o in objs:
            try:
                col.data.insert(uuid=o.uuid, properties=o.properties, vector=o.vector)
                out.append(o.uuid)
            except Exception as ee:
                logger.error("insert_one[%s] failed: %s", collection, ee)
                out.append(None)
        return out


def _flt_eq(name: str, val):
    return Filter.by_property(name).equal(val)


def build_filters(eqs: dict[str, object] | None = None):
    if not eqs: return None
    w = None
    for k, v in eqs.items():
        if v is None: continue
        if isinstance(v, list):
            ors = None
            for val in v:
                f = Filter.by_property(k).equal(val)
                ors = f if ors is None else (ors | f)
            w = ors if w is None else (w & ors)
        else:
            f = Filter.by_property(k).equal(v)
            w = f if w is None else (w & f)
    return w


def near_vector_search_into(collection: str, vector: list[float], limit: int = 10,
                            filters: dict[str, object] | None = None):
    connect()
    col = get_client().collections.get(collection)
    w = build_filters(filters)
    res = col.query.near_vector(
        near_vector=vector,
        limit=limit,
        filters=w,
        return_metadata=["distance"],
        include_vector=False,
    )
    hits = []
    for o in res.objects:
        dist = float(o.metadata.distance) if o.metadata.distance is not None else float("nan")
        hits.append({
            "id": str(o.uuid),
            "score": (1.0 - dist) if dist == dist else 0.0,
            "distance": 0.0 if dist != dist else dist,
            "properties": o.properties or {},
        })
    return hits


def bm25_search_into(collection: str, query: str, query_props: list[str], limit: int = 10,
                     filters: dict[str, object] | None = None):
    connect()
    col = get_client().collections.get(collection)
    w = build_filters(filters)
    res = col.query.bm25(
        query=query,
        query_properties=query_props,
        limit=limit,
        filters=w,
        return_metadata=["score"],
        include_vector=False,
    )
    hits = []
    for o in res.objects:
        hits.append({
            "id": str(o.uuid),
            "score": float(o.metadata.score or 0.0),
            "distance": 0.0,
            "properties": o.properties or {},
        })
    return hits


def _rrf_merge(dense: list[dict], sparse: list[dict], limit: int, c: int = 60):
    # Reciprocal Rank Fusion
    ranks: dict[str, list[int]] = {}
    store: dict[str, dict] = {}
    for lst in (dense, sparse):
        for i, it in enumerate(lst):
            _id = it["id"]
            ranks.setdefault(_id, []).append(i + 1)
            store[_id] = it
    fused = []
    for _id, rks in ranks.items():
        score = sum(1.0 / (c + r) for r in rks)
        fused.append((score, store[_id]))
    fused.sort(key=lambda x: x[0], reverse=True)
    return [it for _, it in fused[:limit]]


def list_objects(collection: str, limit: int = 50, offset: int = 0,
                 filters: dict[str, object] | None = None,
                 include_vector: bool = False):
    connect()
    col = get_client().collections.get(collection)
    w = build_filters(filters)
    res = col.query.fetch_objects(limit=limit, offset=offset, filters=w, include_vector=include_vector)
    items = []
    for o in res.objects:
        rec = {"id": str(o.uuid), "properties": o.properties or {}}
        if include_vector:
            vec = _normalize_vector(getattr(o, "vector", None))
            rec["vector"] = vec
        items.append(rec)
    next_offset = offset + len(items) if len(items) == limit else None
    return items, next_offset


__all__ = [
    "connect", "is_connected", "close_client", "get_client",
    "ensure_schema",
    "CASE", "VICTIM", "EXPERT", "PROSECUTOR", "DOC_CHUNK", "FIN_TX", "COMM", "RULING", "ALL_COLLECTIONS",
    "insert_many_into", "near_vector_search_into", "bm25_search_into", "_rrf_merge",
    "build_filters", "list_objects",
]
