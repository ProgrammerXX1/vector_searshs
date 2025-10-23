# app/main.py
from __future__ import annotations
from typing import List, Optional
from fastapi import FastAPI, UploadFile, File, HTTPException, Query
from fastapi.responses import JSONResponse
from dotenv import load_dotenv
from pathlib import Path
import tempfile
from app.services.parser_dispatch import parse_document

from app.services.type_files._1_6_intro._1_rep_kui import read_any
from app.core.weaviate_client import (
    connect, is_connected, ensure_schema,
    insert_reports, bm25_search, drop_collection, reset_client,
    ensure_connected, get_client, REPORT, REPORT_FIELDS, build_filters
)

from app.schemas.schemas import (
    IndexReportsRequest, IndexReportsResponse,
    BM25Request, HitsResponse, Hit,
    UploadReportResponse, UploadReportsResponse
)

load_dotenv()
app = FastAPI(title="Coder XX1 (DocX)", version="1.1.2")

# -------- Health & Schema --------
@app.get("/health")
def health():
    try:
        connect()
        return {"weaviate_connected": is_connected(), "collection": REPORT}
    except Exception as e:
        return JSONResponse({"weaviate_connected": False, "error": str(e)}, status_code=500)

@app.post("/schema/init")
def schema_init():
    ensure_schema()
    return {"ok": True, "collection": REPORT}

@app.post("/schema/drop-report")
def schema_drop_report():
    drop_collection(REPORT)
    reset_client()
    return {"ok": True}

# -------- Индексация и Поиск --------
@app.post("/reports/index", response_model=IndexReportsResponse)
def reports_index(req: IndexReportsRequest):
    ids = insert_reports([it.model_dump(exclude_none=True) for it in req.items])
    return IndexReportsResponse(ids=ids)

@app.post("/search/bm25", response_model=HitsResponse)
def search_bm25_route(req: BM25Request):
    hits = bm25_search(
        query=req.query,
        query_props=req.query_props,
        limit=req.limit,
        filters=req.filters or None
    )
    return HitsResponse(hits=[Hit(**h) for h in hits])

# -------- Загрузка файла -> Парсинг -> Индексация (без второй схемы) --------
@app.post("/upload/reports", response_model=UploadReportsResponse)
async def upload_reports(files: List[UploadFile] = File(...)):
    all_fields = []
    temp_paths = []

    # === 1. сохранить все файлы временно ===
    for file in files:
        suffix = Path(file.filename).suffix or ".pdf"
        with tempfile.NamedTemporaryFile(delete=False, suffix=suffix) as tmp:
            content = await file.read()
            tmp.write(content)
            tmp_path = Path(tmp.name)
            temp_paths.append(tmp_path)

        # === 2. читать и парсить ===
        full, _ = read_any(tmp_path)
        fields = parse_document(full, filename=file.filename)
        all_fields.append(fields)

    # === 3. индексация всех документов в ReportKUI ===
    ids = insert_reports(all_fields)
    if not ids:
        raise HTTPException(status_code=500, detail="Failed to insert reports")

    # === 4. очистка временных файлов ===
    for p in temp_paths:
        try:
            p.unlink(missing_ok=True)
        except Exception:
            pass

    # === 5. ответ (список успешно загруженных документов) ===
    return UploadReportsResponse(
        total=len(ids),
        success=sum(1 for i in ids if i),
        report_ids=[i for i in ids if i],
        failed=[f.filename for i, f in zip(ids, files) if not i]
    )

@app.get("/reports/chunks")
def list_report_chunks(
    limit: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0),
    include_vector: bool = Query(False),

    # фильтры по 11 полям схемы
    type_document: Optional[str] = None,
    view_document: Optional[str] = None,
    post_main: Optional[str] = None,
    post_main_fn: Optional[str] = None,
    city_fix: Optional[str] = None,
    date_doc: Optional[str] = None,
    report_begin: Optional[str] = None,
    report_next: Optional[str] = None,
    report_end: Optional[str] = None,
    post_new: Optional[str] = None,
    post_new_fn: Optional[str] = None,
):
    """
    Возвращает объекты коллекции REPORT (чанки) с пагинацией.
    - include_vector=true — попытаться вернуть векторы (Weaviate v4: include_vector)
    """
    try:
        ensure_connected()
        col = get_client().collections.get(REPORT)

        eqs = {
            "type_document": type_document,
            "view_document": view_document,
            "post_main": post_main,
            "post_main_fn": post_main_fn,
            "city_fix": city_fix,
            "date_doc": date_doc,
            "report_begin": report_begin,
            "report_next": report_next,
            "report_end": report_end,
            "post_new": post_new,
            "post_new_fn": post_new_fn,
        }
        where = build_filters(eqs)

        try:
            res = col.query.fetch_objects(
                limit=limit,
                offset=offset,
                filters=where,
                return_properties=REPORT_FIELDS,
                include_vector=include_vector,  # <-- правильно для v4
            )
        except TypeError:
            # если вдруг версия SDK не поддерживает include_vector — повторим без него
            res = col.query.fetch_objects(
                limit=limit,
                offset=offset,
                filters=where,
                return_properties=REPORT_FIELDS,
            )

        items = []
        for obj in res.objects:
            item = {
                "uuid": str(obj.uuid),
                "score": None,  # fetch_objects без score
                "properties": {k: (obj.properties or {}).get(k) for k in REPORT_FIELDS},
            }
            # если SDK вернул вектор — добавим (может быть None, т.к. vectorizer=none)
            vec = getattr(obj, "vector", None)
            if include_vector and vec is not None:
                item["vector"] = vec
            items.append(item)

        return {
            "count": len(items),
            "limit": limit,
            "offset": offset,
            "filters_applied": {k: v for k, v in eqs.items() if v is not None},
            "items": items,
        }

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))