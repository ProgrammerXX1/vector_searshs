# app/services/type_files/_5_post_vedenie.py
import re
from typing import Dict, Optional

# --- утилиты те же, что у тебя ---
def _find(pattern: str, text: str, flags=re.I | re.S) -> Optional[str]:
    m = re.search(pattern, text, flags)
    return m.group(1).strip() if m else None

def _find_all(pattern: str, text: str, flags=re.I | re.S) -> list[str]:
    return [g.strip() for g in re.findall(pattern, text, flags)]

def _clean_spaces_keep_newlines(s: Optional[str]) -> Optional[str]:
    if s is None:
        return None
    s = re.sub(r"[ \t]+\n", "\n", s)
    s = re.sub(r"[ \t]+", " ", s)
    return s.strip()

FIO = r"[А-ЯЁӘІҢҒҮҰҚӨҺ][а-яёәіңғүұқөһ\-]+(?:\s+[А-ЯЁӘІҢҒҮҰҚӨҺ]\.\s*[А-ЯЁӘІҢҒҮҰҚӨҺ]\.)"

def parse_postanovlenie_vedenie(text: str, filename: Optional[str] = None) -> Dict[str, Optional[str]]:
    t = text.replace("ё", "е")

    type_document = "Постановление о ведении УСП (электронно)"
    view_document = "Постановление"

    # город / дата (работает и когда на одной строке)
    city_fix = _find(r"(?m)^\s*г\.\s*([А-ЯЁӘІҢҒҮҰҚӨҺ][А-ЯЁӘІҢҒҮҰҚӨҺа-яёәіңғүұқөһ\-]+)\s*(?=\d{1,2}\s+[А-Яа-я]+?\s+\d{4})", t) \
            or _find(r"(?m)^\s*г\.\s*([А-ЯЁӘІҢҒҮҰҚӨҺ][А-ЯЁӘІҢҒҮҰҚӨҺа-яёәіңғүұқөһ\-]+)\s*$", t) \
            or _find(r"\bг\.\s*([А-ЯЁӘІҢҒҮҰҚӨҺ][А-ЯЁӘІҢҒҮҰҚӨҺа-яёәіңғүұқөһ\-]+)\b", t)

    date_doc = _find(r"(?m)\b(\d{1,2}\s+[А-Яа-я]+?\s+\d{4}\s*(?:года|г\.)?)\b", t)

    # номер ЕРДР / дела (берём первый попавшийся длинный номер с №)
    number_work = _find(r"(№\s*\d{6,})", t)

    # верхний блок должности
    post_main = _find(
        r"""(?isx)
        (
          Следователь
          (?:\s+по\s+особо\s+важным\s+делам)?
          [\s\r\n]+
          (?:Следственн\w*\s+управлени\w+\s+)?   # иногда есть "Следственного управления"
          Департамент[а]?\s+экономическ\w*
          \s+расследован\w*
          [\s\r\n]+по\s+[А-ЯЁӘІҢҒҮҰҚӨҺа-яёәіңғүұқөһ\-]+\s+области
        )
        """,
        t
    )

    # ФИО (первое — наверху, последнее — подпись)
    fios = _find_all(rf"({FIO})", t)
    post_main_fn = fios[0] if fios else None

    # УСТАНОВИЛ:
    report_begin = _find(
        r"""УСТАНОВИЛ[:：]\s*
            ([\s\S]*?)
            (?=\n\s*ПОСТАНОВИЛ[:：]|\Z)
        """, t, flags=re.I | re.S | re.X
    )

    # ПОСТАНОВИЛ: (ограничим до подписи/приложений)
    report_end = _find(
        r"""ПОСТАНОВИЛ[:：]\s*
            ([\s\S]*?)
            (?=\n\s*(?:Следователь|Приложени[ея]|$))
        """, t, flags=re.I | re.S | re.X
    )

    # нижний блок должности (подпись)
    post_new = _find(
        r"""(?isx)
        (
          Следователь
          (?:\s+по\s+особо\s+важным\s+делам)?
          [\s\r\n]+
          (?:Следственн\w*\s+управлени\w+\s+)? 
          Департамент[а]?\s+экономическ\w*
          \s+расследован\w*
          [\s\r\n]+по\s+[А-ЯЁӘІҢҒҮҰҚӨҺа-яёәіңғүұқөһ\-]+\s+области
        )
        """,
        t
    )
    post_new_fn = fios[-1] if fios else None

    # чистка
    clean = _clean_spaces_keep_newlines
    post_main    = clean(post_main)
    post_main_fn = post_main_fn.strip().rstrip(".") if post_main_fn else None
    city_fix     = clean(city_fix)
    date_doc     = clean(date_doc)
    report_begin = clean(report_begin)
    report_end   = clean(report_end)
    post_new     = clean(post_new)
    post_new_fn  = post_new_fn.strip().rstrip(".") if post_new_fn else None
    number_work  = clean(number_work)

    # РОВНО 11 полей
    return {
        "type_document": type_document,
        "view_document": view_document,
        "post_main": post_main,
        "post_main_fn": post_main_fn,
        "city_fix": city_fix,
        "date_doc": date_doc,
        "report_begin": report_begin,
        "report_next": number_work,   # сюда кладём № ЕРДР/дела
        "report_end": report_end,
        "post_new": post_new,
        "post_new_fn": post_new_fn,
    }
