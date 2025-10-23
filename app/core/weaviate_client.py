# app/core/weaviate_client.py
from __future__ import annotations
import os, atexit, logging
from typing import Optional, Dict, Any, List

from weaviate import WeaviateClient
from weaviate.connect import ConnectionParams
from weaviate.classes.init import AdditionalConfig, Timeout
from weaviate.classes.config import Configure, Property, DataType, VectorDistances
from weaviate.classes.query import Filter
from weaviate.exceptions import WeaviateClosedClientError

logger = logging.getLogger(__name__)

REPORT_FIELDS = [
    "type_document",
    "view_document",
    "post_main",
    "post_main_fn",
    "city_fix",
    "date_doc",
    "report_begin",
    "report_next",
    "report_end",
    "post_new",
    "post_new_fn",
]

# -------------------------
# Единственная коллекция
# -------------------------
REPORT = "ReportKUI"

_CLIENT: Optional[WeaviateClient] = None

# -------------------------
# Клиент
# -------------------------
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

def ensure_connected() -> WeaviateClient:
    c = get_client()
    try:
        if not c.is_connected(): c.connect()
        return c
    except WeaviateClosedClientError:
        reset_client(); c = get_client(); c.connect(); return c
    except Exception:
        reset_client(); c = get_client(); c.connect(); return c

def connect() -> None:
    c = get_client()
    if not c.is_connected():
        c.connect()

def is_connected() -> bool:
    try:
        return get_client().is_connected()
    except Exception:
        return False

def reset_client() -> None:
    global _CLIENT
    try:
        if _CLIENT and _CLIENT.is_connected():
            _CLIENT.close()
    except Exception:
        pass
    _CLIENT = None

def drop_collection(name: str) -> None:
    c = ensure_connected()
    try:
        if c.collections.exists(name):
            c.collections.delete(name)
    except WeaviateClosedClientError:
        reset_client(); c = ensure_connected()
    ensure_connected()

def close_client() -> None:
    global _CLIENT
    if _CLIENT is None: return
    try:
        if _CLIENT.is_connected(): _CLIENT.close()
    except Exception:
        pass

atexit.register(close_client)

# -------------------------
# Схема (ровно 11 полей)
# -------------------------
def ensure_schema() -> None:
    client = ensure_connected()
    if client.collections.exists(REPORT):
        return
    client.collections.create(
        name=REPORT,
        properties=[
            Property(name="type_document",  data_type=DataType.TEXT),
            Property(name="view_document",  data_type=DataType.TEXT),
            Property(name="post_main",      data_type=DataType.TEXT),
            Property(name="post_main_fn",   data_type=DataType.TEXT),
            Property(name="city_fix",       data_type=DataType.TEXT),
            Property(name="date_doc",       data_type=DataType.TEXT),
            Property(name="report_begin",   data_type=DataType.TEXT),
            Property(name="report_next",    data_type=DataType.TEXT),
            Property(name="report_end",     data_type=DataType.TEXT),
            Property(name="post_new",       data_type=DataType.TEXT),
            Property(name="post_new_fn",    data_type=DataType.TEXT),
        ],
        vectorizer_config=Configure.Vectorizer.none(),
        vector_index_config=Configure.VectorIndex.hnsw(distance_metric=VectorDistances.COSINE),
    )

# -------------------------
# CRUD + BM25
# -------------------------
def insert_reports(objs: list[Dict[str, Any]]) -> list[str | None]:
    connect()
    col = get_client().collections.get(REPORT)
    ids: list[str | None] = []
    for props in objs:
        try:
            uid = col.data.insert(properties=props)
            ids.append(str(uid))
        except Exception as e:
            logger.error("insert report failed: %s", e)
            ids.append(None)
    return ids

def build_filters(eqs: Dict[str, Any] | None = None):
    if not eqs: return None
    w = None
    for k, v in eqs.items():
        if v is None: continue
        f = Filter.by_property(k).equal(v)
        w = f if w is None else (w & f)
    return w

def bm25_search(query: str, query_props: list[str], limit: int = 10,
                filters: Dict[str, Any] | None = None):
    connect()
    col = get_client().collections.get(REPORT)
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
        props = o.properties or {}
        # упорядочиваем по REPORT_FIELDS
        ordered = {field: props.get(field) for field in REPORT_FIELDS}
        hits.append({
            "id": str(o.uuid),
            "score": float(o.metadata.score or 0.0),
            "properties": ordered,
        })
    return hits

# -------------------------
# Exports
# -------------------------
__all__ = [
    "connect","is_connected","close_client","get_client",
    "ensure_schema","insert_reports","bm25_search",
    "REPORT","ensure_connected","reset_client","drop_collection"
]
