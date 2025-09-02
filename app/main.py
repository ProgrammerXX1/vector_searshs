# app/main.py
from __future__ import annotations

from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse
from dotenv import load_dotenv
from typing import List

from app.core.embeddings import get_embedder
from app.core.weaviate_client import (
    connect, is_connected, ensure_schema,
    insert_many_into, near_vector_search_into, bm25_search_into, _rrf_merge,
    CASE, VICTIM, EXPERT, PROSECUTOR, DOC_CHUNK, FIN_TX, COMM, RULING,
    list_objects,
)

# ВСЕ схемы — из твоего файла app/schemas.py
from app.schemas.schemas import (
    # индексация/списки чанков документов
    BatchIndexRequest, BatchIndexResponse,
    BatchIndexTextRequest, TextChunkItem,
    ListChunksRequest, ListChunksResponse, ChunkRecord,

    # поиск базовый/общий
    NearVectorRequest, NearVectorResponse, NearVectorResponseItem,
    BM25Request, HitsResponse,
    HybridRequest, MMRRequest, NearObjectRequest,  # пригодятся позже/при желании

    # сидеры
    SeedTextRequest, SeedResponse,

    # специализированные поиски по коллекциям
    VictimSearchRequest, VictimSearchResponse,
    ExpertSearchRequest, ExpertSearchResponse,
    ProsecutorSearchRequest, ProsecutorSearchResponse,
)

load_dotenv()
app = FastAPI(title="Legal Vector API", version="1.1.0")


@app.on_event("startup")
def _warmup_embedder():
    # прогрев локального эмбеддера Ollama
    try:
        get_embedder().embed_query("warmup")
    except Exception:
        pass


def _resolve_vec(text: str | None, vector: List[float] | None) -> List[float]:
    if vector is not None:
        return vector
    if not text or not text.strip():
        raise HTTPException(status_code=422, detail="Provide 'text' or 'vector'")
    return get_embedder().embed_query(text)


# ---------------------------
# Health & schema
# ---------------------------
@app.get("/health")
def health():
    try:
        connect()
        return {"weaviate_connected": is_connected()}
    except Exception as e:
        return JSONResponse({"weaviate_connected": False, "error": str(e)}, status_code=500)


@app.post("/schema/init")
def schema_init():
    ensure_schema()
    return {"ok": True}


@app.post("/schema/reset")
def schema_reset():
    # тут без drop — просто ensure; если нужен полный reset с удалением, добавим отдельный роут
    ensure_schema()
    return {"ok": True}


# ---------------------------
# Индексация чанков документов (DocumentChunk)
# ---------------------------
@app.post("/chunks/index", response_model=BatchIndexResponse)
def chunks_index(req: BatchIndexRequest):
    """
    Если vector отсутствует, но есть properties.text — эмбеддим и подставляем.
    ⚠️ Первая вставка в пустую коллекцию фиксирует размерность индекса.
    """
    emb = get_embedder()
    items = []
    for it in req.items:
        d = it.model_dump()
        vec = d.get("vector")
        props = d["properties"]
        if vec is None:
            txt = (props.get("text") or "").strip()
            if not txt:
                raise HTTPException(status_code=422, detail="Item has no 'vector' and properties.text is empty")
            vec = emb.embed_passages([txt])[0]
            d["vector"] = vec
        items.append(d)

    ids = insert_many_into(DOC_CHUNK, items)
    return BatchIndexResponse(ids=ids)


@app.post("/chunks/index-text", response_model=BatchIndexResponse)
def chunks_index_text(req: BatchIndexTextRequest):
    """
    Упрощённая индексация: только properties.text → эмбеддер сам построит вектор.
    """
    emb = get_embedder()
    texts = []
    for it in req.items:
        txt = (it.properties.get("text") or "").strip()
        if not txt:
            raise HTTPException(status_code=422, detail="Each item.properties must include non-empty 'text'.")
        texts.append(txt)

    vectors = emb.embed_passages(texts)
    items = [{"uuid": it.uuid, "properties": it.properties, "vector": vec}
             for it, vec in zip(req.items, vectors)]
    ids = insert_many_into(DOC_CHUNK, items)
    return BatchIndexResponse(ids=ids)


# ---------------------------
# Листинг чанков документов (DocumentChunk)
# ---------------------------
@app.post("/data/chunks", response_model=ListChunksResponse)
def data_chunks(req: ListChunksRequest):
    filters = {
        "case_id": req.case_id,
        "document_id": req.document_id,
        "lang": req.lang,
        # doc_types: list[str] → будет OR внутри build_filters()
        "doc_type": req.doc_types,
    }
    items, next_off = list_objects(
        collection=DOC_CHUNK,
        limit=req.limit,
        offset=req.offset,
        filters=filters,
        include_vector=req.include_vector,
    )
    out = [ChunkRecord(**it) for it in items]
    return ListChunksResponse(items=out, next_offset=next_off)


# ---------------------------
# Специализированные поиски
# ---------------------------
@app.post("/search/victims", response_model=VictimSearchResponse)
def search_victims(req: VictimSearchRequest):
    filters = {"case_id": req.case_id, "lang": req.lang, "iin": req.iin, "full_name": req.full_name}
    if req.text or req.vector:
        vec = _resolve_vec(req.text, req.vector)
        hits = near_vector_search_into(VICTIM, vec, limit=req.limit, filters=filters)
    else:
        hits = bm25_search_into(
            VICTIM,
            query=req.full_name or req.iin or "",
            query_props=["notes", "full_name", "contacts"],
            limit=req.limit,
            filters=filters,
        )
    return VictimSearchResponse(hits=[NearVectorResponseItem(**h) for h in hits])


@app.post("/search/experts", response_model=ExpertSearchResponse)
def search_experts(req: ExpertSearchRequest):
    filters = {"case_id": req.case_id, "lang": req.lang, "specialty": req.specialty}
    mode = (req.mode or "hybrid").lower()
    if mode == "bm25":
        hits = bm25_search_into(EXPERT, query=req.text or "", query_props=["question", "answer"],
                                limit=req.limit, filters=filters)
    elif mode == "dense":
        vec = _resolve_vec(req.text, req.vector)
        hits = near_vector_search_into(EXPERT, vec, limit=req.limit, filters=filters)
    else:
        vec = _resolve_vec(req.text, req.vector)
        dense = near_vector_search_into(EXPERT, vec, limit=100, filters=filters)
        sparse = bm25_search_into(EXPERT, query=req.text or "", query_props=["question", "answer"],
                                  limit=200, filters=filters)
        hits = _rrf_merge(dense, sparse, limit=req.limit, c=60)
    return ExpertSearchResponse(hits=[NearVectorResponseItem(**h) for h in hits])


@app.post("/search/prosecutors", response_model=ProsecutorSearchResponse)
def search_prosecutors(req: ProsecutorSearchRequest):
    filters = {"case_id": req.case_id, "lang": req.lang, "filing_type": req.filing_type}
    mode = (req.mode or "hybrid").lower()
    if mode == "bm25":
        hits = bm25_search_into(PROSECUTOR, query=req.query or req.text or "", query_props=["text"],
                                limit=req.limit, filters=filters)
    elif mode == "dense":
        vec = _resolve_vec(req.text, req.vector)
        hits = near_vector_search_into(PROSECUTOR, vec, limit=req.limit, filters=filters)
    else:
        vec = _resolve_vec(req.text, req.vector)
        dense = near_vector_search_into(PROSECUTOR, vec, limit=100, filters=filters)
        sparse = bm25_search_into(PROSECUTOR, query=req.query or req.text or "", query_props=["text"],
                                  limit=200, filters=filters)
        hits = _rrf_merge(dense, sparse, limit=req.limit, c=60)
    return ProsecutorSearchResponse(hits=[NearVectorResponseItem(**h) for h in hits])


# ---------------------------
# (опционально) сидер на 200 текстовых чанков DocumentChunk
# ---------------------------
def _gen_corpus_texts(count: int, case_id: int, lang: str = "ru"):
    import random
    random.seed(42)

    A = [
        "Я вложил деньги в TAKORP",
        "Потерпевший сообщил о вложениях в пирамиду TAKORP",
        "TAKORP обещала 15% доходности еженедельно",
        "Реклама проекта TAKORP в Telegram-канале",
        "Договор инвестиций с TAKORP",
    ]
    B = [
        "IBAN KZ{iban} указан в платёжном поручении",
        "Платёж на сумму {amt} тг отправлен со счёта {acc}",
        "USDT перевод на криптокошелёк {wal}",
        "Операции по карте ****{last4} за {mon} {year}",
        "Банковская выписка: входящие переводы в {mon} {year}",
    ]
    C = [
        "Контракт на поставку оборудования №{num}",
        "Договор займа между сторонами на сумму {amt} тг",
        "Приказ о проведении проверки от {date}",
        "Распоряжение о назначении экспертизы по делу №{num}",
        "Соглашение о конфиденциальности сторон",
    ]
    D = [
        "Лендинг с обещанием высокой доходности",
        "Реклама в соцсетях и мессенджерах",
        "Публичное уведомление на сайте проекта",
        "Описание бонусной программы рефералов",
        "Отзыв на форуме о проекте",
    ]
    months = ["январь","февраль","март","апрель","май","июнь","июль","август","сентябрь","октябрь","ноябрь","декабрь"]

    out = []
    for i in range(count):
        bucket = "ABCD"[i % 4]
        if bucket == "A":
            text = A[i % len(A)]
            doc_type = "interrogation" if i % 2 == 0 else "statement"
        elif bucket == "B":
            text = B[i % len(B)].format(
                iban=random.randint(100000, 999999),
                amt=random.randint(50_000, 5_000_000),
                acc=random.randint(1000, 9999),
                wal=f"0x{random.randint(10**15,10**16-1):x}",
                last4=random.randint(1000, 9999),
                mon=random.choice(months),
                year=random.randint(2022, 2025),
            )
            doc_type = "bank_record" if i % 2 == 0 else "transfer"
        elif bucket == "C":
            text = C[i % len(C)].format(
                num=random.randint(100, 9999),
                amt=random.randint(100_000, 10_000_000),
                date=f"{random.randint(1,28):02d}.{random.randint(1,12):02d}.{random.randint(2021,2025)}",
            )
            doc_type = "contract" if i % 2 == 0 else "order"
        else:
            text = D[i % len(D)]
            doc_type = "webpage" if i % 2 == 0 else "notice"

        props = {
            "text": text,
            "case_id": case_id,
            "document_id": 10_000 + i,
            "chunk_idx": i % 5,
            "source_page": (i % 7) + 1,
            "created_at": None,
            "lang": lang,
            "doc_type": doc_type,
        }
        out.append({"text": text, "properties": props})
    return out


@app.post("/data/seed-text", response_model=SeedResponse)
def data_seed_text(req: SeedTextRequest):
    """
    Сидер на 200+ текстов в коллекцию DocumentChunk с эмбеддингами через локальный Ollama.
    Сделай /schema/init перед первым вызовом.
    """
    emb = get_embedder()
    corpus = _gen_corpus_texts(req.count, case_id=req.case_id, lang=req.lang)
    texts = [c["text"] for c in corpus]
    vectors = emb.embed_passages(texts)

    items = [{"uuid": None, "properties": c["properties"], "vector": v}
             for c, v in zip(corpus, vectors)]
    ids = insert_many_into(DOC_CHUNK, items)
    return SeedResponse(inserted=sum(1 for x in ids if x), ids=[x for x in ids if x])
