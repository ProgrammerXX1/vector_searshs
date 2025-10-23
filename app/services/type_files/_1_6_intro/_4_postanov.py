# ---------- утилиты ----------
import re
from typing import Dict, Optional


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

# ФИО: "Фамилия И.О." (с поддержкой казахских букв)
FIO = r"[А-ЯЁӘІҢҒҮҰҚӨҺ][а-яёәіңғүұқөһ\-]+(?:\s+[А-ЯЁӘІҢҒҮҰҚӨҺ]\.\s*[А-ЯЁӘІҢҒҮҰҚӨҺ]\.)"

# ---------- парсер ПОСТАНОВЛЕНИЯ ----------
def parse_postanovlenie_accept(text: str, filename: Optional[str] = None) -> Dict[str, Optional[str]]:
    t = text

    type_document = "Постановление о принятии материалов"
    view_document = "Постановление"

    # city / date
    city_fix = _find(r"(?m)^\s*г\.\s*([А-ЯЁӘІҢҒҮҰҚӨҺ][А-ЯЁӘІҢҒҮҰҚӨҺа-яёәіңғүұқөһ\-]+)\s*$", t) \
            or _find(r"\bг\.\s*([А-ЯЁӘІҢҒҮҰҚӨҺ][А-ЯЁӘІҢҒҮҰҚӨҺа-яёәіңғүұқөһ\-]+)\b", t)
    date_doc = _find(r"(?m)^\s*(\d{1,2}\s+[А-Яа-я]+?\s+\d{4}\s*(?:года|г\.)?)\s*$", t) \
            or _find(r"\b(\d{1,2}\s+[А-Яа-я]+?\s+\d{4}\s*(?:года|г\.)?)\b", t)
    if not city_fix:
        city_fix = _find(r"г\.\s*([А-ЯЁӘІҢҒҮҰҚӨҺ][А-ЯЁӘІҢҒҮҰҚӨҺа-яёәіңғүұқөһ\-]+)\s+\d{1,2}\s+[А-Яа-я]+?\s+\d{4}", t)

    # № дела
    number_work = _find(r"(№\s*\d{6,}\,?)", t)

    # верхний блок должности (post_main)
    post_main = _find(
    r"""(?isx)
    (Следователь
       (?:\s+по\s+особо\s+важным\s+делам)?   # иногда есть, иногда нет
       [\s\r\n]+
       Департамент[а]?\s+экономическ\w*\s+расследован\w*
       [\s\r\n]+по\s+[А-ЯЁӘІҢҒҮҰҚӨҺа-яёәіңғүұқөһ\-]+\s+области
        )
        """,
        t
    )


    # ищем все ФИО; первое — вверху, последнее — подпись
    fios = _find_all(rf"({FIO})", t)
    post_main_fn = fios[0] if fios else None

    # УСТАНОВИЛ:
    report_begin = _find(
        r"""УСТАНОВИЛ[:：]\s*
            ([\s\S]*?)
            (?=\n\s*ПОСТАНОВИЛ[:：]|\Z)
        """, t, flags=re.I | re.S | re.X
    )

    # ПОСТАНОВИЛ:
    report_end = _find(
        r"""ПОСТАНОВИЛ[:：]\s*
            ([\s\S]*?)
            (?=\n\s*(?:Следователь|Приложени[ея]|$))
        """, t, flags=re.I | re.S | re.X
    )

    # нижняя подпись
    post_new = _find(
        r"""(?isx)
    (                                   # вернуть весь блок
      Следователь
      (?:\s+по\s+особо\s+важным\s+делам)?   # опционально
      \s+                                  # может быть перенос(ы)

      Департамент[а]?\s+экономическ\w*     # экономических/экономического...
      \s+расследован\w*                    # расследований/расследования

      \s+по\s+[А-ЯЁӘІҢҒҮҰҚӨҺа-яёәіңғүұқөһ\-]+\s+области
    )
        """,
        t
    )

    post_new_fn = fios[-1] if fios else None

    # чистим
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

    # РОВНО 11 полей (номер дела кладём в report_next)
    return {
        "type_document": type_document,
        "view_document": view_document,
        "post_main": post_main,
        "post_main_fn": post_main_fn,
        "city_fix": city_fix,
        "date_doc": date_doc,
        "report_begin": report_begin,
        "report_next": number_work,     # ← №255500121000018,
        "report_end": report_end,
        "post_new": post_new,
        "post_new_fn": post_new_fn,
    }
