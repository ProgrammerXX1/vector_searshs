# app/services/parser_report.py
from __future__ import annotations
from typing import Dict, List, Tuple, Optional
from pathlib import Path
import re

def _read_pdf(path: Path) -> tuple[str, list[str]]:
    import pdfplumber
    pages = []
    with pdfplumber.open(str(path)) as pdf:
        for p in pdf.pages:
            txt = p.extract_text() or ""
            pages.append(txt)
    return "\n".join(pages), pages

def read_any(path: Path) -> tuple[str, list[str]]:
    if path.suffix.lower() == ".pdf":
        return _read_pdf(path)
    txt = path.read_text(encoding="utf-8", errors="ignore")
    return txt, [txt]

def _clean_text(txt: str) -> str:
    t = txt
    # вырезаем «Единый реестр...» хвост (портит регексы)
    t = re.sub(r"Е\s*д\s*и\s*н\s*ы\s*й[\s\S]*?mailto:[^\s]+", " ", t, flags=re.I)
    # нормализация переносов/пробелов
    t = re.sub(r"[ \t]*\n[ \t]*", "\n", t)
    t = re.sub(r"[ \t]+", " ", t)
    t = re.sub(r"[ \t]+\n", "\n", t)
    return t.strip()

def _find(pattern: str, text: str, flags=re.I|re.S) -> Optional[str]:
    m = re.search(pattern, text, flags)
    return m.group(1).strip() if m else None

def _clean_spaces(s: Optional[str]) -> Optional[str]:
    if s is None:
        return None
    s = re.sub(r"[ \t]*\n[ \t]*", " ", s)   # переносы -> пробел
    s = re.sub(r"\s+", " ", s)              # множественные пробелы -> один
    return s.strip()

def _normalize_name_for_kui(s: str) -> str:
    # нижний регистр, разделители -> пробел
    s = s.lower()
    s = re.sub(r"[\\/]", " ", s)            # слеши (папки) -> пробел
    s = re.sub(r"[_\-\.]+", " ", s)         # _ - .  -> пробел
    s = re.sub(r"\s+", " ", s).strip()
    return s

def _detect_kui_from_name(name: Optional[str]) -> tuple[bool, Optional[str], Optional[str]]:
    """
    Возвращает (is_kui, normalized_phrase, kui_number)
    Примеры:
      '1. Рапорт_КУИ__255500120000201' -> (True, 'рапорт куи 255500120000201', '255500120000201')
      'РАПОРТ-КУИ-123'                 -> (True, 'рапорт куи 123', '123')
    """
    if not name:
        return (False, None, None)
    norm = _normalize_name_for_kui(name)
    # rapor/raport/рапорт  +  kui/куи  (допускаем произвольные пробелы между)
    is_kui = bool(re.search(r"\b(raport|rapor|рапорт)\b\s*\b(kui|куи)\b", norm))
    # номер (12–18 цифр обычно)
    mnum = re.search(r"\b(\d{9,20})\b", norm)
    kui_num = mnum.group(1) if mnum else None
    phrase = f"рапорт куи {kui_num}" if (is_kui and kui_num) else ("рапорт куи" if is_kui else None)
    return (is_kui, phrase, kui_num)
def parse_report_fields(raw_text: str, filename: Optional[str] = None) -> Dict[str, Optional[str]]:
    """
    Возвращаем РОВНО 11 полей схемы ReportKUI:
      type_document, view_document, post_main, post_main_fn,
      city_fix, date_doc, report_begin, report_next,
      report_end, post_new, post_new_fn
    """
    text = _clean_text(raw_text)

    # 1) type_document / 2) view_document
    is_kui_by_name, name_phrase, kui_num = _detect_kui_from_name(filename)

    # шапка "РАПОРТ" в самом документе (в т.ч. "Р А П О Р Т")
    def _has_raport_heading(t: str) -> bool:
        return bool(re.search(r"(?im)^\s*р\s*а\s*п\s*о\s*р\s*т\s*$", t))

    # 1) type_document / 2) view_document
    type_document = "Рапорт КУИ" if is_kui_by_name else "Рапорт КУИ"  # для твоего кейса — фикс
    view_document = "Рапорт" if (is_kui_by_name or _has_raport_heading(text)) else ""
    # 3) post_main — ровно должность руководителя
    m = re.search(
    r"(Руководитель\s+Департамента\s*(?:\n|\s)+"
    r"экономических\s+расследований\s*(?:\n|\s)+"
    r"по\s+Павлодарской\s+области)",
    text, flags=re.I)
    post_main = m.group(1).strip() if m else None
    
    # 4) post_main_fn — ФИО
    post_main_fn = _find(r"(Есенов\s*Е\.?\s*О\.?)", text)

    # 5) city_fix — город «Павлодар» (без «…ской области»)
    city_fix = _find(r"\bг\.\s*(Павлодар)\b", text) or "Павлодар"

    # 6) date_doc — «17 апреля 2025 года» (текстом)
    date_doc = _find(r"\b(\d{1,2}\s+[А-Яа-я]+?\s+\d{4}\s*(?:года|г\.)?)\b", text)

    # 7) report_begin — от якоря до «Для принятия решения»
    report_begin = _find(
      r"((?:^|\n)\s*В\s+период\s+дежурства\s+поступило\s+сообщение\s+следующего\s+содержания:[\s\S]*?)\n\s*Для\s+принятия\s+решения",text)

    # 8) report_next — строка с КУИ, датой и временем
    report_next = _find(
    r"((?:^|\n)\s*Для\s+принятия\s+решения[\s\S]*?КУИ\s*№\s*\d+\s*дата\s*регистрации\s*\d{2}\.\d{2}\.\d{4}\s*время\s*\d{2}:\d{2}\.?)",text)


    # 9) report_end — про передачу Самарову
    report_end = _find(
    r"((?:^|\n)\s*Зарегистрированное\s+сообщение\s+передано\s+на\s+рассмотрение\s+сотруднику[\s\S]*?Самаров\s*Ж\.?\s*Г\.?\.?)",text)


    # 10) post_new — должность получателя
    post_new = _find(
        r"(Старший\s+оперуполномоченный[\s\S]*?Павлодарской\s+области[\s\S]*?Агенств[ао]\s+по\s+финансовому\s+мониторингу)",text)

    # 11) post_new_fn — ФИО получателя
    post_new_fn = _find(r"(Самаров\s*Ж\.?\s*Г\.?)", text)

    # нормализация пробелов и хвостовых точек
    report_begin = _clean_spaces(report_begin)
    report_next  = _clean_spaces(report_next)
    report_end   = _clean_spaces(report_end)
    post_new     = _clean_spaces(post_new)

    if post_main_fn: post_main_fn = post_main_fn.rstrip(".").strip()
    if post_new_fn:  post_new_fn  = post_new_fn.rstrip(".").strip()

    # ВОЗВРАЩАЕМ ТОЛЬКО 11 ПОЛЕЙ
    return {
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