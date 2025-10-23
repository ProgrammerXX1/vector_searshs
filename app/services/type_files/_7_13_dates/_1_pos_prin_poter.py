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

    if m.lastindex:
        for i in range(1, m.lastindex + 1):
            g = m.group(i)
            if g:
                return g.strip()
        return None  # все группы оказались None

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


# ФИО с инициалами: "Фамилия И.О." (с поддержкой казахских букв)
FIO_INITS = r"[А-ЯЁӘІҢҒҮҰҚӨҺ][а-яёәіңғүұқөһ\-]+(?:\s+[А-ЯЁӘІҢҒҮҰҚӨҺ]\.\s*[А-ЯЁӘІҢҒҮҰҚӨҺ]\.)"
# ФИО полное: "Фамилия Имя Отчество" (для строки «Признать ... потерпевшим»)
FIO_FULL = r"[А-ЯЁӘІҢҒҮҰҚӨҺ][а-яёәіңғүұқөһ\-]+(?:\s+[А-ЯЁӘІҢҒҮҰҚӨҺ][а-яёәіңғүұқөһ\-]+){1,2}"

# ---------- парсер: ПОСТАНОВЛЕНИЕ о признании лица потерпевшим ----------
def parse_priznanie_poter(text: str, filename: Optional[str] = None) -> Dict[str, Optional[str]]:
    """
    Парсер под вид:
       «ПОСТАНОВЛЕНИЕ о признании лица потерпевшим»
    Возвращает РОВНО 11 полей (тот же контракт, что и у остальных).
    """
    t = (text or "").replace("ё", "е")

    # --- определить тип по шапке ---
    heading = (t[:1500]).lower()
    is_priznanie = bool(re.search(
        r"постановлени\w*[\s\r\n]+о\s+признани[ея]\s+лица\s+потерпевш\w+",
        heading, re.I | re.S
    ))

    type_document = "Постановление о признании лица потерпевшим" if is_priznanie else "Постановление"
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

    date_doc = _find(r"(?m)\b(\d{1,2}\s+[А-Яа-я]+?\s+\d{4}\s*(?:года|г\.)?)\b", t)

    # --- № ЕРДР / дела ---
    number_work = _find(r"(№\s*\d{6,})", t)

    # --- верхний блок должности (Следователь … ДЭР … по <области>) ---
    post_main = _find(
        r"""(?isx)
        (
          Следователь
          (?:\s+по\s+особо\s+важным\s+делам)?        # бывает и без этого
          [\s\r\n]+
          (?:Следственн\w*\s+управлени\w+\s+)?       # опц. "Следственного управления"
          Департамент[а]?\s+экономическ\w*
          \s+расследован\w*
          [\s\r\n]+по\s+[А-ЯЁӘІҢҒҮҰҚӨҺа-яёәіңғүұқөһ\-]+\s+области
        )
        """,
        t
    )

    # --- ФИО исполнителя (в шапке и подписи) ---
    fios = _find_all(rf"({FIO_INITS})", t)
    post_main_fn = fios[0] if fios else None

    # --- УСТАНОВИЛ: (до блока ПОСТАНОВИЛ) ---
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
            (?=\n\s*(?:Следователь|Приложени[ея]|$))
        """, t, flags=re.I | re.S | re.X
    )

    # --- вытащим из ПОСТАНОВИЛ: ФИО потерпевшего и ДР (если есть) ---
    victim_fio = _find(rf"Признать\s+({FIO_FULL})\s+(?:[, ]*\s*(\d{{2}}\.\d{{2}}\.\d{{4}}))?\s*(?:г\.?\s*р\.?)?[, ]*\s*потерпевш\w*", report_end or "", flags=re.I)
    # дату рождения отдельно (если вдруг имя/дата идут в иной последовательности)
    victim_dob = _find(r"(\d{2}\.\d{2}\.\d{4})\s*г\.?\s*р\.?", report_end or "", flags=re.I) or None

    # --- нижняя подпись (тот же блок должности) ---
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

    # ПРИМЕЧАНИЕ: victim_fio/victim_dob можно сохранить в report_next, если нужно куда-то отдать
    # но чтобы не ломать контракт — положим туда № дела; а при необходимости меняй маппинг здесь.
    report_next = number_work

    # --- РОВНО 11 полей ---
    return {
        "type_document": type_document,
        "view_document": view_document,
        "post_main": post_main,
        "post_main_fn": post_main_fn,
        "city_fix": city_fix,
        "date_doc": date_doc,
        "report_begin": report_begin,
        "report_next": report_next,  # № ЕРДР/дела (см. примечание выше)
        "report_end": report_end,
        "post_new": post_new,
        "post_new_fn": post_new_fn,
    }
