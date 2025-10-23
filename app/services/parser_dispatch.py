# app/services/parser_dispatch.py
from __future__ import annotations
from typing import Optional, Dict
import re
import logging

# ============================================================
#                       Л О Г И Р О В А Н И Е
# ============================================================
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
if not logger.handlers:
    ch = logging.StreamHandler()
    ch.setLevel(logging.DEBUG)
    formatter = logging.Formatter("[%(asctime)s] %(levelname)s %(name)s: %(message)s")
    ch.setFormatter(formatter)
    logger.addHandler(ch)

# ============================================================
#                     И М П О Р Т   П А Р С Е Р О В
# 1) Рапорт КУИ                              -> _1_rep_kui.parse_report_fields
# 2) Рапорт ЕРДР                             -> _2_rep_erdr.parse_report_erdr_fields
# 3) Уведомление о начале ДР                 -> _3_uved_o_nazhale.parse_uved_start
# 4) Постановление о принятии материалов     -> _4_postanov.parse_postanovlenie_accept
# 5) Постановление о ведении УП по ДР (эл.)  -> _5_post_vedenie.parse_postanovlenie_vedenie
# 6) Постановление о ведении П по ДР след.   -> _6_post_porushenie.parse_postanovlenie_porushenie
# 7) Постановление о признании лица потерпевшим         -> _7_13_dates/_1_pos_prin_poter.parse_postanovlenie_porushenie
# 8) Заявление потерпевшего о языке судопроизводства    -> _7_13_dates/_2_zayab_lang.parse_zayavlenie_yazyk
# 9) Исковое заявление -> _7_13_dates/_3_isk_zayab.parse_iskovoe_zayavlenie
# 10) Постановление о признании лица гражданским истцом -> _7_13_dates/_4_pos_prin_graj.parse_priznanie_poter_graj
# 11) Заявление об отказе от ознакомления               -> _7_13_dates/_6_ob_otkaze.parse_zayavlenie_otkaz
# 12) Протокол допроса потерпевшего                     -> _7_13_dates/_5_prot_dopros.parse_prot_doprosa
# ============================================================
from .type_files._1_6_intro._1_rep_kui import parse_report_fields as parse_kui_fields
from .type_files._1_6_intro._2_rep_erdr import parse_report_erdr_fields
from .type_files._1_6_intro._3_uved_o_nazhale import parse_uved_start
from .type_files._1_6_intro._4_postanov import parse_postanovlenie_accept
from .type_files._1_6_intro._5_post_vedenie import parse_postanovlenie_vedenie
from .type_files._1_6_intro._6_post_porushenie import parse_postanovlenie_porushenie
from .type_files._7_13_dates._1_pos_prin_poter import parse_priznanie_poter
from .type_files._7_13_dates._2_zayab_lang import parse_zayavlenie_yazyk
from .type_files._7_13_dates._3_isk_zayab import parse_iskovoe_zayavlenie
from .type_files._7_13_dates._4_pos_prin_graj import parse_priznanie_poter_graj
from .type_files._7_13_dates._5_prot_dopros import parse_prot_doprosa
from .type_files._7_13_dates._6_ob_otkaze import parse_zayavlenie_otkaz

# ============================================================
#                           У Т И Л И Т Ы
# ============================================================
def _normalize_name(s: str) -> str:
    """
    Нормализуем имя файла/путь:
    - нижний регистр
    - /, \, _, -, . -> пробел
    - схлопываем пробелы
    - ё -> е
    """
    logger.debug(f"_normalize_name: raw={s!r}")
    s = (s or "").lower()
    s = re.sub(r"[\\/]", " ", s)
    s = re.sub(r"[_\-.]+", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    s = s.replace("ё", "е")
    logger.debug(f"_normalize_name: normalized={s!r}")
    return s

def _prep_text(t: str) -> str:
    """Подготовка текста к эвристикам детектора: lower + ё->е."""
    return (t or "").lower().replace("ё", "е")


# ============================================================
#                  Д Е Т Е К Т О Р Ы   Т И П О В
# Каждый детектор возвращает:
#   - {"type_document": ..., "view_document": "Постановление/Рапорт/Уведомление"}
#   - либо None, если не уверен
# Порядок вызова важен: сначала более специфичные типы.
# ============================================================

# [6] ПОСТАНОВЛЕНИЕ О ВЕДЕНИИ ПРОИЗВОДСТВА ПО ДР СЛЕДОВАТЕЛЮ
def detect_post_porushenie(norm: str, head: str) -> Optional[Dict[str, str]]:
    # norm уже нормализован _normalize_name (нижний регистр, разделители -> пробел)
    # head уже в нижнем регистре (_prep_text)
    hay = f"{norm} {head}"
    # подстрахуемся ещё раз против необычных разделителей
    hay = re.sub(r"[_\-\./\\]+", " ", hay)
    hay = re.sub(r"\s+", " ", hay).strip()

    ok = (
        "постановлен" in hay
        and "о поручении" in hay
        and "производства" in hay
        and "досудебного" in hay
        and "расследования" in hay
        and "следователю" in hay
    )

    if ok:
        return {
            "type_document": "Постановление о поручении производства ДР следователю",
            "view_document": "Постановление",
        }
    return None


# ----------------------------------------------------------------
# [5] ПОСТАНОВЛЕНИЕ О ВЕДЕНИИ УП ПО ДР (В ЭЛЕКТРОННОМ ФОРМАТЕ)
# Пример имени: "6. Постановление_о_ведении_уголовного_производства_по_досудебному_расследованию_в_электронном_формате.pdf"
# Ключи: "постановлен", "о ведени", "уголовн", "производств/судопроизводств",
#        "досудебн", "расследован", обязательно "электронн" и "формат"
# Анти-ложки: не должно содержать "о принятии материалов", "в собственное производство", "продлени".
# ----------------------------------------------------------------
def detect_post_vedenie(norm: str, head: str) -> Optional[Dict[str, str]]:
    hay = f"{norm} {head}"
    ok = (
        ("постановлен" in hay)
        and ("о ведени" in hay)
        and ("уголовн" in hay or "уголовn" in hay)   # подстраховка от OCR
        and ("производств" in hay or "судопроизводств" in hay)
        and ("досудебн" in hay and "расследован" in hay)
        and ("электронн" in hay and "формат" in hay)
        and ("принятии материал" not in hay)
        and ("собственное производство" not in hay)
        and ("продлени" not in hay)
    )
    if ok:
        return {
            "type_document": "Постановление о ведении УП по ДР (электронно)",
            "view_document": "Постановление",
        }
    return None

# ----------------------------------------------------------------
# [4] ПОСТАНОВЛЕНИЕ О ПРИНЯТИИ МАТЕРИАЛОВ УГОЛОВНОГО ДЕЛА
# Пример имени: "4. Постановление о принятии материалов уголовного дела в собственное производство.pdf"
# Эвристика: ищем "постановлен", "о принят", "материал", "собственн", "производств".
# Исключаем пересечение с "о ведени".
# ----------------------------------------------------------------
def detect_post_accept(norm: str, head: str) -> Optional[Dict[str, str]]:
    name_hit = (
        ("постановлен" in norm)
        and ("о принят" in norm)
        and ("материал" in norm)
        and ("собственн" in norm)
        and ("производств" in norm)
        and ("ведени" not in norm)
    )
    text_hit = (
        ("постановлен" in head)
        and ("о принят" in head)
        and ("материал" in head)
        and ("собственн" in head)
        and ("производств" in head)
        and ("ведени" not in head)
    )
    if name_hit or text_hit:
        return {
            "type_document": "Постановление о принятии материалов",
            "view_document": "Постановление",
        }
    return None

# ----------------------------------------------------------------
# [3] УВЕДОМЛЕНИЕ О НАЧАЛЕ ДОСУДЕБНОГО РАССЛЕДОВАНИЯ
# Пример имени: "3. Уведомление о начале досудебного расследования_ЕРДР__255500121000018.pdf"
# ----------------------------------------------------------------
def detect_uved(norm: str, head: str) -> Optional[Dict[str, str]]:
    is_uved = (
        (("уведомлени" in norm) and ("о начале" in norm or "досудебн" in norm))
        or (
            re.search(r"(?m)^\s*уведомлени(?:е|я)\s*$", head)
            and ("досудебн" in head)
        )
    )
    if is_uved:
        return {"type_document": "Уведомление о начале ДР", "view_document": "Уведомление"}
    return None

# ----------------------------------------------------------------
# [2] РАПОРТ ЕРДР
# Пример имени: "2. Рапорт_ЕРДР_255500121000018.pdf"
# ----------------------------------------------------------------
def detect_raport_erdr(norm: str) -> Optional[Dict[str, str]]:
    if re.search(r"\b(erdr|ердр)\b", norm):
        return {"type_document": "Рапорт ЕРДР", "view_document": "Рапорт"}
    return None

# ----------------------------------------------------------------
# [1] РАПОРТ КУИ
# Пример имени: "1. Рапорт_КУИ_255500120000201.pdf"
# ----------------------------------------------------------------
def detect_raport_kui(norm: str) -> Optional[Dict[str, str]]:
    if re.search(r"\b(raport|rapor|рапорт)\b\s*\b(kui|куи)\b", norm):
        return {"type_document": "Рапорт КУИ", "view_document": "Рапорт"}
    return None

# ----------------------------------------------------------------
# [7] ПОСТАНОВЛЕНИЕ О ПРИЗНАНИИ ЛИЦА ПОТЕРПЕВШИМ
# Пример имени: "5. Постановление_о_признании_лица_потерпевшим.pdf"
# ----------------------------------------------------------------
def detect_post_priznanie_poter(norm: str, head: str) -> Optional[Dict[str, str]]:
    hay = f"{norm} {head}"
    # подстрахуемся ещё раз против необычных разделителей
    hay = re.sub(r"[_\-\./\\]+", " ", hay)
    hay = re.sub(r"\s+", " ", hay).strip()

    ok = (
        "постановлен" in hay
        and ("о признани" in hay or "признани" in hay)
        and ("лица" in hay or "лицо" in hay)
        and ("потерпевш" in hay)
    )

    if ok:
        return {
            "type_document": "Постановление о признании лица потерпевшим",
            "view_document": "Постановление",
        }
    return None

# ----------------------------------------------------------------
# [8] ЗАЯВЛЕНИЕ ПОТЕРПЕВШЕГО О ЯЗЫКЕ СУДОПРОИЗВОДСТВА
# Пример имени: "8. Заявление_потерпевшего_о_языке_судопроизводства.pdf"
# ----------------------------------------------------------------
def detect_zayavlenie_yazyk(norm: str, head: str) -> Optional[Dict[str, str]]:
    hay = f"{norm} {head}"
    # подстрахуемся ещё раз против необычных разделителей
    hay = re.sub(r"[_\-\./\\]+", " ", hay)
    hay = re.sub(r"\s+", " ", hay).strip()

    ok = (
        ("заявлени" in hay)
        and ("потерпевш" in hay)
        and ("язык" in hay or "языке" in hay)
        and ("судопроизводств" in hay)
    )

    if ok:
        return {
            "type_document": "Заявление потерпевшего о языке судопроизводства",
            "view_document": "Заявление",
        }
    return None

# ----------------------------------------------------------------
# [9] ИСКОВОЕ ЗАЯВЛЕНИЕ
# Пример имени: "Исковое_заявление.pdf"
# ----------------------------------------------------------------
def detect_iskovoe_zayavlenie(norm: str, head: str) -> Optional[Dict[str, str]]:
    hay = f"{norm} {head}"
    # подстрахуемся ещё раз против необычных разделителей
    hay = re.sub(r"[_\-\./\\]+", " ", hay)
    hay = re.sub(r"\s+", " ", hay).strip()

    ok = (
        ("исков" in hay)
        and ("заявлени" in hay)
    )

    if ok:
        return {
            "type_document": "Исковое заявление",
            "view_document": "Заявление",
        }
    return None

# ----------------------------------------------------------------
# [10] ПОСТАНОВЛЕНИЕ О ПРИЗНАНИИ ЛИЦА ГРАЖДАНСКИМ ИСТЦОМ
# ----------------------------------------------------------------
def detect_post_priznanie_graj(norm: str, head: str) -> Optional[Dict[str, str]]:
    hay = f"{norm} {head}"
    # подстрахуемся ещё раз против необычных разделителей
    hay = re.sub(r"[_\-\./\\]+", " ", hay)
    hay = re.sub(r"\s+", " ", hay).strip()

    ok = (
        "постановлен" in hay
        and ("о признани" in hay or "признани" in hay)
        and ("лица" in hay or "лицо" in hay)
        and ("гражданским" in hay)
        and ("истцом" in hay)
    )

    if ok:
        return {
            "type_document": "Постановление о признании лица гражданским истцом",
            "view_document": "Постановление",
        }
    return None

# ----------------------------------------------------------------
# [11] ЗАЯВЛЕНИЕ ОБ ОТКАЗЕ ОТ ОЗНАКОМЛЕНИЯ
# ----------------------------------------------------------------
def detect_zayavlenie_otkaz(norm: str, head: str) -> Optional[Dict[str, str]]:
    hay = f"{norm} {head}"
    # подстрахуемся ещё раз против необычных разделителей
    hay = re.sub(r"[_\-\./\\]+", " ", hay)
    hay = re.sub(r"\s+", " ", hay).strip()

    ok = (
        ("заявлени" in hay)
        and ("отказ" in hay)
        and ("ознакомлени" in hay or "назначени" in hay or "исследовани" in hay or "экспертиз" in hay)
    )

    if ok:
        return {
            "type_document": "Заявление об отказе от ознакомления",
            "view_document": "Заявление",
        }
    return None


# ----------------------------------------------------------------
# [12] ПРОТОКОЛ ДОПРОСА ПОТЕРПЕВШЕГО
# ----------------------------------------------------------------
def detect_protokol_dopros(norm: str, head: str) -> Optional[Dict[str, str]]:
    hay = f"{norm} {head}"
    ok = (
        ("протокол" in hay)
        and ("допрос" in hay)
        and ("потерпевш" in hay)
    )
    
    if ok:
        return {
            "type_document": "протокол допроса потерпевшего",
            "view_document": "протокол",
        }
    return None


# ============================================================
#                 Г Л А В Н Ы Й   Д Е Т Е К Т О Р
# Вызывает частные детекторы по порядку.
# ============================================================
def detect_type_and_view(filename: Optional[str], text: str) -> Dict[str, str]:
    logger.info(f"detect_type_and_view: filename={filename!r}")

    norm = _normalize_name(filename or "")
    head = _prep_text(text)[:4000]  # проверяем только начало текста

    # Порядок важен: 12 → 11 → 10 → 9 → 8 → 7 → 6 → 5 → 4 → 3 → 2 → 1
    detectors = (
        detect_protokol_dopros,      # [12]
        detect_zayavlenie_otkaz,     # [11]
        detect_post_priznanie_graj,  # [10]
        detect_iskovoe_zayavlenie,   # [9]
        detect_zayavlenie_yazyk,     # [8]
        detect_post_priznanie_poter, # [7]
        detect_post_porushenie,      # [6]
        detect_post_vedenie,         # [5]
        detect_post_accept,          # [4]
        detect_uved,                 # [3]
        detect_raport_erdr,          # [2]
        detect_raport_kui,           # [1]
    )

    for detector in detectors:
        try:
            # детекторы с двумя аргументами (norm, head)
            res = detector(norm, head)  # type: ignore[misc]
        except TypeError:
            # детекторы с одним аргументом (norm)
            res = detector(norm)        # type: ignore[misc]
        if res:
            logger.debug(f"{detector.__name__} -> {res}")
            return res

    logger.debug("No detector matched → Unknown")
    return {"type_document": "Неизвестно", "view_document": "Неизвестно"}


# ============================================================
#                    Д И С П Е Т Ч Е Р   П А Р С И Н Г А
# Возвращает РОВНО 11 полей, как требуют парсеры.
# ============================================================
def parse_document(text: str, filename: Optional[str] = None) -> Dict[str, Optional[str]]:
    """
    Выбрать нужный парсер по типу документа и вернуть ровно 11 полей.
    """
    logger.info(f"parse_document: filename={filename!r}, text_len={len(text or '')}")
    meta = detect_type_and_view(filename, text or "")
    td = (meta["type_document"] or "").lower()
    vd = (meta["view_document"] or "").lower()
    logger.debug(f"meta={meta}, td={td}, vd={vd}")

    # ---------- [1] Рапорт КУИ ----------
    if td == "рапорт куи" and vd == "рапорт":
        logger.info("Dispatch → parse_kui_fields")
        fields = parse_kui_fields(text, filename=filename)
        fields["type_document"] = "Рапорт КУИ"
        fields["view_document"] = "Рапорт"
        return fields

    # ---------- [2] Рапорт ЕРДР ----------
    if td == "рапорт ердр" and vd == "рапорт":
        logger.info("Dispatch → parse_report_erdr_fields")
        fields = parse_report_erdr_fields(text, filename=filename)
        fields["type_document"] = "Рапорт ЕРДР"
        fields["view_document"] = "Рапорт"
        return fields

    # ---------- [3] Уведомление о начале ДР ----------
    if td == "уведомление о начале др" and vd == "уведомление":
        logger.info("Dispatch → parse_uved_start")
        fields = parse_uved_start(text, filename=filename)
        return fields

    # ---------- [4] Постановление о принятии материалов ----------
    if td == "постановление о принятии материалов" and vd == "постановление":
        logger.info("Dispatch → parse_postanovlenie_accept")
        fields = parse_postanovlenie_accept(text, filename=filename)
        return fields

    # ---------- [5] Постановление о ведении УП по ДР (электронно) ----------
    if td == "постановление о ведении уп по др (электронно)" and vd == "постановление":
        logger.info("Dispatch → parse_postanovlenie_vedenie")
        fields = parse_postanovlenie_vedenie(text, filename=filename)
        return fields

    # ---------- [6] Постановление о поручении производства ДР следователю ----------
    if td == "постановление о поручении производства др следователю" and vd == "постановление":
        logger.info("Dispatch → parse_postanovlenie_porushenie")
        fields = parse_postanovlenie_porushenie(text, filename=filename)
        return fields

    # ---------- [7] Постановление о признании лица потерпевшим ----------
    if td == "постановление о признании лица потерпевшим" and vd == "постановление":
        logger.info("Dispatch → parse_priznanie_poter")
        fields = parse_priznanie_poter(text, filename=filename)
        return fields

    # ---------- [8] Заявление потерпевшего о языке судопроизводства ----------
    if td == "заявление потерпевшего о языке судопроизводства" and vd == "заявление":
        logger.info("Dispatch → parse_zayavlenie_yazyk")
        fields = parse_zayavlenie_yazyk(text, filename=filename)
        return fields

    # ---------- [9] Исковое заявление ----------
    if td == "исковое заявление" and vd == "заявление":
        logger.info("Dispatch → parse_iskovoe_zayavlenie")
        fields = parse_iskovoe_zayavlenie(text, filename=filename)
        return fields

    # ---------- [10] Постановление о признании лица гражданским истцом ----------
    if td == "постановление о признании лица гражданским истцом" and vd == "постановление":
        logger.info("Dispatch → parse_priznanie_poter_graj")
        fields = parse_priznanie_poter_graj(text, filename=filename)
        return fields

    # ---------- [11] Заявление об отказе от ознакомления ----------
    if td == "заявление об отказе от ознакомления" and vd == "заявление":
        logger.info("Dispatch → parse_zayavlenie_otkaz")
        fields = parse_zayavlenie_otkaz(text, filename=filename)
        return fields

    # ---------- [12] Протокол допроса потерпевшего ----------
    if td == "протокол допроса потерпевшего" and vd == "протокол":
        logger.info("Dispatch → parse_prot_doprosa")
        fields = parse_prot_doprosa(text, filename=filename)
        return fields

    # ---------- Заглушка (ровно 11 полей) ----------
    logger.warning("Unknown document type → returning stub")
    return {
        "type_document": meta.get("type_document") or "",
        "view_document": meta.get("view_document") or "",
        "post_main": None,
        "post_main_fn": None,
        "city_fix": None,
        "date_doc": None,
        "report_begin": None,
        "report_next": None,
        "report_end": None,
        "post_new": None,
        "post_new_fn": None,
    }
