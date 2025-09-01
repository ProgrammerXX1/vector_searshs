# app/core/weaviate_client.py
from __future__ import annotations

import os
import atexit
import logging
from typing import Optional

import numpy as np
import uuid as _uuid
from datetime import datetime, timedelta, timezone
from random import Random

from weaviate import WeaviateClient
from weaviate.connect import ConnectionParams
from weaviate.classes.init import AdditionalConfig, Timeout
from weaviate.classes.config import Configure, Property, DataType, VectorDistances
from weaviate.classes.query import Filter
from weaviate.classes.data import DataObject

logger = logging.getLogger(__name__)

DOC_COLLECTION = "Document"
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


def ensure_schema() -> None:
    """Создать (или дополнить) коллекцию для векторного поиска (cosine/HNSW)."""
    connect()
    client = get_client()

    exists = client.collections.exists(DOC_COLLECTION)

    props = [
        Property(name="text", data_type=DataType.TEXT, description="Текст чанка"),
        Property(name="case_id", data_type=DataType.INT),
        Property(name="document_id", data_type=DataType.INT),
        Property(name="chunk_idx", data_type=DataType.INT),
        Property(name="source_page", data_type=DataType.INT),
        Property(name="created_at", data_type=DataType.DATE),
        Property(name="lang", data_type=DataType.TEXT),
        Property(name="doc_type", data_type=DataType.TEXT),
    ]

    if not exists:
        client.collections.create(
            name=DOC_COLLECTION,
            properties=props,
            vectorizer_config=Configure.Vectorizer.none(),
            vector_index_config=Configure.VectorIndex.hnsw(
                distance_metric=VectorDistances.COSINE
            ),
        )
        logger.info("✅ Created collection %s", DOC_COLLECTION)
    else:
        col = client.collections.get(DOC_COLLECTION)
        existing = {p.name for p in col.config.get().properties}
        for p in props:
            if p.name not in existing:
                col.config.add_property(p)
        logger.info("ℹ️ Collection %s exists; properties verified", DOC_COLLECTION)


def drop_collection() -> bool:
    connect()
    client = get_client()
    try:
        if client.collections.exists(DOC_COLLECTION):
            client.collections.delete(DOC_COLLECTION)
            logger.info("🗑️ Dropped collection %s", DOC_COLLECTION)
        return True
    except Exception as e:
        logger.error("drop_collection error: %s", e)
        return False


def insert_many(items: list[dict]) -> list[str | None]:
    """
    items: [{"uuid": str|None, "vector": list[float], "properties": {...}}]
    Возвращает список UUID (строки), которые мы сами генерируем/пробрасываем.
    """
    connect()
    col = get_client().collections.get(DOC_COLLECTION)

    objs: list[DataObject] = []
    uuids: list[str] = []

    for it in items:
        uid = it.get("uuid") or str(_uuid.uuid4())
        uuids.append(uid)
        props = it["properties"]
        vec = it["vector"]
        objs.append(DataObject(uuid=uid, properties=props, vector=vec))

    try:
        col.data.insert_many(objs)
        return uuids
    except Exception as e:
        logger.error("insert_many failed: %s. Fallback to per-item.", e)
        out: list[str | None] = []
        for o in objs:
            try:
                col.data.insert(uuid=o.uuid, properties=o.properties, vector=o.vector)
                out.append(o.uuid)
            except Exception as ee:
                logger.error("insert failed for %s: %s", o.uuid, ee)
                out.append(None)
        return out

def _normalize_vector(vec) -> list[float] | None:
    """Привести вектор к list[float], поддержка мультивекторов Weaviate {'default': [...]} и др."""
    if vec is None:
        return None

    # Уже массив/список
    if isinstance(vec, (list, tuple)):
        try:
            return [float(x) for x in vec]
        except Exception:
            return None

    if isinstance(vec, np.ndarray):
        return [float(x) for x in vec.tolist()]

    if isinstance(vec, dict):
        # Популярные ключи у Weaviate/multivectors
        preferred_keys = ("default", "vector", "vectors", "data", "values", "value", "embedding")
        for key in preferred_keys:
            v = vec.get(key)
            if isinstance(v, np.ndarray):
                return [float(x) for x in v.tolist()]
            if isinstance(v, (list, tuple)):
                try:
                    return [float(x) for x in v]
                except Exception:
                    pass

        # Если это мапа name -> list[float], берём первый список
        for _, v in vec.items():
            if isinstance(v, np.ndarray):
                return [float(x) for x in v.tolist()]
            if isinstance(v, (list, tuple)) and (len(v) == 0 or isinstance(v[0], (int, float))):
                try:
                    return [float(x) for x in v]
                except Exception:
                    pass

        # Если это индекс->значение (редко)
        try:
            keys = sorted(vec.keys(), key=lambda k: int(k) if str(k).isdigit() else str(k))
            return [float(vec[k]) for k in keys]
        except Exception:
            return None

    return None


def _build_filters(
    *, case_id: int | None, document_id: int | None, lang: str | None, doc_types: list[str] | None = None
):
    w = None
    if case_id is not None:
        w = Filter.by_property("case_id").equal(case_id)
    if document_id is not None:
        f2 = Filter.by_property("document_id").equal(document_id)
        w = f2 if w is None else (w & f2)
    if lang is not None:
        f3 = Filter.by_property("lang").equal(lang)
        w = f3 if w is None else (w & f3)
    if doc_types:
        ors = None
        for t in doc_types:
            f = Filter.by_property("doc_type").equal(t)
            ors = f if ors is None else (ors | f)
        w = ors if w is None else (w & ors)
    return w


def near_vector_search(
    vector: list[float],
    limit: int = 10,
    case_id: int | None = None,
    document_id: int | None = None,
    lang: str | None = None,
):
    """Простой dense-поиск (ANN) с фильтрами. Возвращает [{id, score, distance, properties}]."""
    connect()
    col = get_client().collections.get(DOC_COLLECTION)

    w = _build_filters(case_id=case_id, document_id=document_id, lang=lang)

    result = col.query.near_vector(
        near_vector=vector,
        limit=limit,
        filters=w,
        return_metadata=["distance"],
        include_vector=False,
    )

    hits = []
    for o in result.objects:
        uid = str(o.uuid)
        dist = float(o.metadata.distance) if o.metadata.distance is not None else float("nan")
        score = (1.0 - dist) if dist == dist else 0.0
        hits.append(
            {
                "id": uid,
                "score": score,
                "distance": 0.0 if dist != dist else dist,
                "properties": o.properties or {},
            }
        )
    return hits


def bm25_search(
    query_text: str,
    limit: int = 10,
    *,
    case_id: int | None = None,
    document_id: int | None = None,
    lang: str | None = None,
    doc_types: list[str] | None = None,
):
    """BM25 по полю text (инвертированный индекс Weaviate)."""
    connect()
    col = get_client().collections.get(DOC_COLLECTION)
    w = _build_filters(case_id=case_id, document_id=document_id, lang=lang, doc_types=doc_types)

    res = col.query.bm25(
        query=query_text,
        query_properties=["text"],
        limit=limit,
        filters=w,
        return_metadata=["score"],
        include_vector=False,
    )

    hits = []
    for o in res.objects:
        uid = str(o.uuid)
        score = float(o.metadata.score or 0.0)
        hits.append(
            {
                "id": uid,
                "score": score,  # BM25 score (больше = лучше)
                "distance": 0.0,
                "properties": o.properties or {},
            }
        )
    return hits


def near_vector_raw(
    vector: list[float],
    limit: int = 100,
    *,
    case_id: int | None = None,
    document_id: int | None = None,
    lang: str | None = None,
    include_vec: bool = True,
):
    """Dense-поиск с возможностью вернуть сами векторы (для MMR)."""
    connect()
    col = get_client().collections.get(DOC_COLLECTION)
    w = _build_filters(case_id=case_id, document_id=document_id, lang=lang)

    res = col.query.near_vector(
        near_vector=vector,
        limit=limit,
        filters=w,
        return_metadata=["distance"],
        include_vector=include_vec,
    )

    items = []
    for o in res.objects:
        uid = str(o.uuid)
        dist = float(o.metadata.distance) if o.metadata.distance is not None else float("nan")
        sim = (1.0 - dist) if dist == dist else 0.0
        vec = getattr(o, "vector", None)
        vec = _normalize_vector(vec)   # ✅ привели к list[float] или None
        items.append(
            {
                "id": uid,
                "score": sim,
                "distance": 0.0 if dist != dist else dist,
                "vector": vec,
                "properties": o.properties or {},
            }
        )
    return items


def _rrf_merge(dense: list[dict], sparse: list[dict], limit: int, c: int = 60):
    """Reciprocal Rank Fusion: объединение рангов dense и sparse."""
    ranks: dict[str, list[int]] = {}
    all_items: dict[str, dict] = {}

    for lst in (dense, sparse):
        for i, it in enumerate(lst):
            _id = it["id"]
            ranks.setdefault(_id, []).append(i + 1)
            all_items[_id] = it

    fused = []
    for _id, rks in ranks.items():
        score = sum(1.0 / (c + r) for r in rks)  # больше = лучше
        fused.append((score, all_items[_id]))
    fused.sort(key=lambda x: x[0], reverse=True)
    return [it for _, it in fused[:limit]]


def hybrid_rrf_search(
    *,
    vector: list[float],
    query_text: str,
    k_dense: int = 100,
    k_bm25: int = 200,
    limit: int = 10,
    case_id: int | None = None,
    document_id: int | None = None,
    lang: str | None = None,
    doc_types: list[str] | None = None,
):
    """Hybrid: RRF(dense, bm25)."""
    dense = near_vector_raw(
        vector, limit=k_dense, case_id=case_id, document_id=document_id, lang=lang, include_vec=False
    )
    sparse = bm25_search(
        query_text, limit=k_bm25, case_id=case_id, document_id=document_id, lang=lang, doc_types=doc_types
    )
    merged = _rrf_merge(dense, sparse, limit=limit, c=60)
    hits = []
    for it in merged:
        hits.append(
            {
                "id": it["id"],
                "score": it.get("score", 0.0),
                "distance": it.get("distance", 0.0),
                "properties": it.get("properties", {}),
            }
        )
    return hits


def mmr_search(
    *,
    vector: list[float],
    k_candidates: int = 100,
    top_n: int = 20,
    lambda_mult: float = 0.5,
    case_id: int | None = None,
    document_id: int | None = None,
    lang: str | None = None,
):
    """MMR-диверсификация поверх dense-кандидатов (возвращает разнообразные документы)."""
    # кандидаты (пытаемся получить векторы)
    cands = near_vector_raw(
        vector, limit=k_candidates, case_id=case_id, document_id=document_id, lang=lang, include_vec=True
    )

    # нормализуем и фильтруем по размерности
    target_dim = len(vector)
    valid = []
    for it in cands:
        v = _normalize_vector(it.get("vector"))
        if v is None or len(v) != target_dim:
            continue
        valid.append({"item": it, "vec": np.array(v, dtype=np.float32)})

    if not valid:
        basic = near_vector_search(vector=vector, limit=top_n, case_id=case_id, document_id=document_id, lang=lang)
        return []

    # матрица кандидатов и нормализация
    V = np.stack([x["vec"] for x in valid], axis=0)  # (N, d)
    V = V / (np.linalg.norm(V, axis=1, keepdims=True) + 1e-12)
    q = np.array(vector, dtype=np.float32)
    q = q / (np.linalg.norm(q) + 1e-12)

    sim_to_q = V @ q  # (N,)
    selected_idx = []

    # старт — лучший по запросу
    first = int(np.argmax(sim_to_q))
    selected_idx.append(first)

    cand_idx = list(range(len(valid)))
    cand_idx.remove(first)

    while len(selected_idx) < min(top_n, len(valid)):
        rel = sim_to_q[cand_idx]  # близость к запросу
        # штраф за схожесть с уже выбранными
        if selected_idx:
            div = (V[cand_idx] @ V[selected_idx].T).max(axis=1)
        else:
            div = 0.0
        score = lambda_mult * rel - (1.0 - lambda_mult) * div
        j = int(np.argmax(score))
        picked = cand_idx[j]
        selected_idx.append(picked)
        cand_idx.remove(picked)

    # собрать выдачу из valid (а не из исходных cands!)
    out = []
    for idx in selected_idx:
        it = valid[idx]["item"]
        sim = float(sim_to_q[idx])
        dist = 1.0 - sim
        out.append({
            "id": it["id"],
            "score": sim,
            "distance": dist,
            "properties": it["properties"]
        })
    return out



def near_object_search(
    *,
    object_id: str,
    limit: int = 10,
    case_id: int | None = None,
    document_id: int | None = None,
    lang: str | None = None,
):
    """Поиск похожих на конкретный объект (nearObject)."""
    connect()
    col = get_client().collections.get(DOC_COLLECTION)
    w = _build_filters(case_id=case_id, document_id=document_id, lang=lang)

    res = col.query.near_object(
        near_object=object_id,
        limit=limit,
        filters=w,
        return_metadata=["distance"],
        include_vector=False,
    )

    hits = []
    for o in res.objects:
        uid = str(o.uuid)
        dist = float(o.metadata.distance) if o.metadata.distance is not None else float("nan")
        sim = (1.0 - dist) if dist == dist else 0.0
        hits.append({"id": uid, "score": sim, "distance": 0.0 if dist != dist else dist, "properties": o.properties or {}})
    return hits


def seed_data(count: int = 200, dim: int = 4, case_id: int = 1) -> list[str]:
    """
    Сидер: 200 объектов вокруг 4 центров (A,B,C,D) — удобно тестировать разные поиски.
    A TAKORP (interrogation/statement)
    B Banking/IBAN/USDT (bank_record/transfer)
    C Contract/Legal (contract/order)
    D Web/Promo (webpage/notice)
    """
    assert dim == 4, "Для простоты сидера dim=4"
    connect()
    col = get_client().collections.get(DOC_COLLECTION)
    rnd = Random(42)

    centers = {
        "A": np.array([0.62, 0.58, 0.12, 0.09], dtype=np.float32),
        "B": np.array([0.10, 0.12, 0.70, 0.60], dtype=np.float32),
        "C": np.array([0.60, 0.12, 0.60, 0.12], dtype=np.float32),
        "D": np.array([0.12, 0.60, 0.12, 0.60], dtype=np.float32),
    }
    texts = {
        "A": [
            "Я вложил деньги в TAKORP",
            "Потерпевший сообщил о вложениях в пирамиду TAKORP",
            "TAKORP обещала 15% доходности еженедельно",
            "Реклама проекта TAKORP в Telegram-канале",
            "Договор инвестиций с TAKORP",
        ],
        "B": [
            "IBAN KZ123 указан в платёжном поручении",
            "Платёж на сумму свыше 1 млн тг",
            "USDT перевод на криптокошелёк",
            "Операции по карте ****4321 за май 2023",
            "Банковская выписка: входящие переводы",
        ],
        "C": [
            "Контракт на поставку оборудования",
            "Договор займа между сторонами",
            "Приказ о проведении проверки",
            "Распоряжение о назначении экспертизы",
            "Соглашение о конфиденциальности",
        ],
        "D": [
            "Лендинг с обещанием высокой доходности",
            "Реклама в соцсетях и мессенджерах",
            "Публичное уведомление на сайте",
            "Описание бонусной программы",
            "Отзыв на форуме о проекте",
        ],
    }
    dtypes = {"A": ["interrogation", "statement"], "B": ["bank_record", "transfer"], "C": ["contract", "order"], "D": ["webpage", "notice"]}

    batch_objs: list[DataObject] = []
    ids: list[str] = []
    now = datetime.now(timezone.utc)

    for i in range(count):
        bucket = "ABCD"[i % 4]
        center = centers[bucket]
        vec = center + np.random.normal(0.0, 0.02, size=(dim,)).astype(np.float32)

        doc_id = 1000 + i
        text = texts[bucket][i % len(texts[bucket])]
        doc_type = dtypes[bucket][i % len(dtypes[bucket])]
        created_at = (now - timedelta(days=rnd.randint(0, 365))).isoformat()

        props = {
            "text": text,
            "case_id": case_id,
            "document_id": doc_id,
            "chunk_idx": i % 5,
            "source_page": (i % 7) + 1,
            "created_at": created_at,
            "lang": "ru",
            "doc_type": doc_type,
        }
        uid = str(_uuid.uuid4())
        ids.append(uid)
        batch_objs.append(DataObject(uuid=uid, properties=props, vector=vec.tolist()))

    for s in range(0, len(batch_objs), 128):
        col.data.insert_many(batch_objs[s : s + 128])

    return ids


__all__ = [
    "get_client",
    "connect",
    "close_client",
    "ensure_schema",
    "drop_collection",
    "insert_many",
    "near_vector_search",
    "bm25_search",
    "near_vector_raw",
    "hybrid_rrf_search",
    "mmr_search",
    "near_object_search",
    "seed_data",
]
