# app/services/type_files/_6_post_porushenie.py
import re
from typing import Dict, Optional

# ---------- утилиты ----------
def _find(pattern: str, text: str, flags=re.I | re.S) -> Optional[str]:
    """
    Возвращает:
      - первый НЕ-None захваченный группой фрагмент (group(1..n)) если есть
      - иначе весь матч (group(0))
      - None, если нет матча или все группы None
    """
    m = re.search(pattern, text, flags)
    if not m:
        return None

    # Если есть захваченные группы — вернём первую непустую
    if m.lastindex:
        for i in range(1, m.lastindex + 1):
            g = m.group(i)
            if g:
                return g.strip()
        return None  # все группы оказались None

    # Иначе — весь матч
    g0 = m.group(0)
    return g0.strip() if g0 else None


def _find_all(pattern: str, text: str, flags=re.I | re.S) -> list[str]:
    """
    Собирает список: на каждом матче — первый НЕ-None group(1..n), иначе group(0).
    Пропускает элементы, где всё None.
    """
    out: list[str] = []
    for m in re.finditer(pattern, text, flags):
        val = None
        if m.lastindex:
            for i in range(1, m.lastindex + 1):
                gi = m.group(i)
                if gi:
                    val = gi
                    break
        else:
            val = m.group(0)

        if val:
            out.append(val.strip())
    return out

def _clean_spaces_keep_newlines(s: Optional[str]) -> Optional[str]:
    if s is None:
        return None
    s = re.sub(r"[ \t]+\n", "\n", s)
    s = re.sub(r"[ \t]+", " ", s)
    return s.strip()

# ФИО: "Фамилия И.О." (с поддержкой казахских букв)
FIO = r"[А-ЯЁӘІҢҒҮҰҚӨҺ][а-яёәіңғүұқөһ\-]+(?:\s+[А-ЯЁӘІҢҒҮҰҚӨҺ]\.\s*[А-ЯЁӘІҢҒҮҰҚӨҺ]\.)"

# ---------- парсер ПОСТАНОВЛЕНИЯ (2 варианта заголовка) ----------
def parse_postanovlenie_porushenie(text: str, filename: Optional[str] = None) -> Dict[str, Optional[str]]:
    """
    Универсальный парсер двух родственных видов:
      A) "ПОСТАНОВЛЕНИЕ о ведении уголовного (судо)производства ... (в электронном формате)"
      B) "ПОСТАНОВЛЕНИЕ о поручении производства досудебного расследования"
    Возвращает РОВНО 11 полей.
    """
    t = (text or "").replace("ё", "е")

    # --- определить точный тип по шапке ---
    heading = (t[:1200]).lower()

    is_vedenie = bool(re.search(
        r"постановлени\w*[\s\r\n]+о\s+ведени[еия][\s\S]{0,120}?"
        r"уголовн\w+[\s\S]{0,40}?(?:судо)?производств\w+"
        r"(?:[\s\S]{0,120}?электронн\w+[\s\S]{0,40}?формат\w+)?",
        heading, re.I | re.S
    ))

    is_poruchenie = bool(re.search(
        r"постановлени\w*[\s\r\n]+о\s+поручени[еия]\s+производств[ао]\s+до[сc]удебн\w+\s+расследован\w+",
        heading, re.I | re.S
    ))

    # дефолт — оставим старое именование, но если поймали поручение — переопределим
    type_document = "Постановление о ведении УП по ДР (электронно)" if is_vedenie else \
                    ("Постановление о поручении производства ДР" if is_poruchenie else
                     "Постановление (неопределённый вид)")
    view_document = "Постановление"

    # --- город / дата ---
    city_fix = _find(
        r"(?m)^\s*г\.\s*([А-ЯЁӘІҢҒҮҰҚӨҺ][А-ЯЁӘІҢҒҮҰҚӨҺа-яёәіңғүұқөһ\-]+)\s*(?=\d{1,2}\s+[А-Яа-я]+?\s+\d{4})",
        t
    ) or _find(
        r"(?m)^\s*г\.\s*([А-ЯЁӘІҢҒҮҰҚӨҺ][А-ЯЁӘІҢҒҮҰҚӨҺа-яёәіңғүұқөһ\-]+)\s*$", t
    ) or _find(
        r"\bг\.\s*([А-ЯЁӘІҢҒҮҰҚӨҺ][А-ЯЁӘІҢҒҮҰҚӨҺа-яёәіңғүұқөһ\-]+)\b", t
    )

    date_doc = _find(
        r"(?m)\b(\d{1,2}\s+[А-Яа-я]+?\s+\d{4}\s*(?:года|г\.)?)\b", t
    )

    # --- № ЕРДР / дела ---
    number_work = _find(r"(№\s*\d{6,})", t)

    # --- блок должности наверху (2 формы: Следователь ИЛИ Руководитель СУ ДЭР) ---
    post_main = _find(
        r"""(?isx)
        (                                   # ВАРИАНТ 1: Следователь ...
          Следователь
          (?:\s+по\s+особо\s+важным\s+делам)?
          [\s\r\n]+
          (?:Следственн\w*\s+управлени\w+\s+)?   # иногда есть "Следственного управления"
          Департамент[а]?\s+экономическ\w*
          \s+расследован\w*
          [\s\r\n]+по\s+[А-ЯЁӘІҢҒҮҰҚӨҺа-яёәіңғүұқөһ\-]+\s+области
        )
        |
        (                                   # ВАРИАНТ 2: Руководитель СУ ДЭР ...
          Руководител[ьяе]?
          (?:\s+СУ|\s+Следственн\w*\s+управлени\w+)   # "СУ" или "Следственного управления"
          \s+ДЭР
          [^\S\r\n]*по[^\S\r\n]+[А-ЯЁӘІҢҒҮҰҚӨҺа-яёәіңғүұқөһ\-]+
          [^\S\r\n]+области
        )
        """,
        t
    )

    # --- ФИО (первое сверху, последнее в подписи) ---
    fios = _find_all(rf"({FIO})", t)
    post_main_fn = fios[0] if fios else None

    # --- УСТАНОВИЛ: ---
    report_begin = _find(
        r"""УСТАНОВИЛ[:：]\s*
            ([\s\S]*?)
            (?=\n\s*ПОСТАНОВИЛ[:：]|\Z)
        """, t, flags=re.I | re.S | re.X
    )

    # --- ПОСТАНОВИЛ: ---
    report_end = _find(
        r"""ПОСТАНОВИЛ[:：]\s*
            ([\s\S]*?)
            (?=\n\s*(?:Следователь|Руководител[ьяе]?|Приложени[ея]|$))
        """, t, flags=re.I | re.S | re.X
    )

    # --- нижняя подпись (те же 2 формы должности) ---
    post_new = _find(
        r"""(?isx)
        (
          (?:Следователь
             (?:\s+по\s+особо\s+важным\s+делам)?[\s\r\n]+
             (?:Следственн\w*\s+управлени\w+\s+)? 
             Департамент[а]?\s+экономическ\w*\s+расследован\w*
             [\s\r\n]+по\s+[А-ЯЁӘІҢҒҮҰҚӨҺа-яёәіңғүұқөһ\-]+\s+области)
          |
          (?:Руководител[ьяе]?
             (?:\s+СУ|\s+Следственн\w*\s+управлени\w+)\s+ДЭР
             [^\S\r\n]*по[^\S\r\n]+[А-ЯЁӘІҢҒҮҰҚӨҺа-яёәіңғүұқөһ\-]+
             [^\S\r\n]+области)
        )
        """,
        t
    )
    post_new_fn = fios[-1] if fios else None

    # --- чистка ---
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

    # --- РОВНО 11 полей ---
    return {
        "type_document": type_document,
        "view_document": view_document,
        "post_main": post_main,
        "post_main_fn": post_main_fn,
        "city_fix": city_fix,
        "date_doc": date_doc,
        "report_begin": report_begin,
        "report_next": number_work,   # № ЕРДР/дела
        "report_end": report_end,
        "post_new": post_new,
        "post_new_fn": post_new_fn,
    }
