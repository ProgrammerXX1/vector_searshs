# app/services/parser_uved_start.py
from __future__ import annotations
from typing import Optional, Dict
import re

# ---------- утилиты, как у тебя ----------
def _find(pattern: str, text: str, flags=re.I | re.S) -> Optional[str]:
    m = re.search(pattern, text, flags)
    return m.group(1).strip() if m else None

def _clean_spaces_keep_newlines(s: Optional[str]) -> Optional[str]:
    if s is None:
        return None
    s = re.sub(r"[ \t]+\n", "\n", s)
    s = re.sub(r"[ \t]+", " ", s)
    return s.strip()

def _fio_any_order(line: str) -> Optional[str]:
    # "Есенов Е.О."  → Есенов Е.О.
    m1 = re.search(r"(?m)^\s*([А-ЯЁ][а-яё\-]+)\s+([А-ЯЁ]\.)\s*([А-ЯЁ]\.)\s*$", line)
    if m1:
        return f"{m1.group(1)} {m1.group(2)}{m1.group(3)}".rstrip(".")
    # "Е.О. Есенов" → Есенов Е.О.
    m2 = re.search(r"(?m)^\s*([А-ЯЁ]\.)\s*([А-ЯЁ]\.)\s*([А-ЯЁ][а-яё\-]+)\s*$", line)
    if m2:
        return f"{m2.group(3)} {m2.group(1)}{m2.group(2)}".rstrip(".")
    # В одной строке без начала/конца
    m3 = re.search(r"\b([А-ЯЁ][а-яё\-]+)\s+([А-ЯЁ]\.)\s*([А-ЯЁ]\.)\b", line)
    if m3:
        return f"{m3.group(1)} {m3.group(2)}{m3.group(3)}".rstrip(".")
    return None

# ---------- парсер уведомления ----------
def parse_uved_start(text: str, filename: Optional[str] = None) -> Dict[str, Optional[str]]:
    """
    Парсит 'Уведомление о начале досудебного расследования'
    и отдаёт 11 полей под единую схему.
    """
    # 1–2) тип/вид
    type_document = "Уведомление о начале ДР"
    view_document = "Уведомление"

    # Нормализованные варианты текста
    t = text

    # 3–4) адресат в шапке (должность + ФИО)
    # пример строки: "Прокурор Павлодарской области ЖУЙРИКТАЕВ Б.К."
    pat = re.compile(
    r"""(?mx)
    ^\s*
    (                                                   # 1) post_main
      Прокурор
      (?:\s+[А-ЯЁӘІҢҒҮҰҚӨҺа-яёәіңғүұқөһ\-]+){0,5}      # уточнения до ключевого слова
      \s+(?:области|города|района)
      (?:\s+[А-ЯЁӘІҢҒҮҰҚӨҺа-яёәіңғүұқөһ\-]+){0,3}       # возможный хвост локации
    )
    \s*
    (?:\n+|\s{2,})                                      # переход (новая строка или 2+ пробела)
    \s*
    (                                                   # 2) post_main_fn — ФИО
      [А-ЯЁӘІҢҒҮҰҚӨҺ][А-ЯЁӘІҢҒҮҰҚӨҺа-яёәіңғүұқөһ\-]+    # Фамилия (любой регистр)
      (?:\s+
         (?:                                            # И.О. или Имя (+Отчество)
            [А-ЯЁӘІҢҒҮҰҚӨҺ]\s*\.\s*[А-ЯЁӘІҢҒҮҰҚӨҺ]\s*\. # Б.К. / И.И.
           |[А-ЯЁӘІҢҒҮҰҚӨҺ][А-ЯЁӘІҢҒҮҰҚӨҺа-яёәіңғүұқөһ\-]+
            (?:\s+[А-ЯЁӘІҢҒҮҰҚӨҺ][А-ЯЁӘІҢҒҮҰҚӨҺа-яёәіңғүұқөһ\-]+)?
         )
      )
    )
    \s*$
    """,
)

    m = pat.search(t)
    post_main    = "Уведомление"
    post_main_fn = m.group(2) if m else None


    # 5–6) город и дата в шапке справа
    city_fix = _find(r"(?m)^\s*г\.\s*([А-ЯЁ][а-яё\-]+)\s*$", t) or _find(r"\bг\.\s*([А-ЯЁ][а-яё\-]+)\b", t)
    date_doc = _find(r"(?m)^\s*(\d{1,2}\s+[А-Яа-я]+?\s+\d{4}\s*(?:года|г\.)?)\s*$", t) \
               or _find(r"\b(\d{1,2}\s+[А-Яа-я]+?\s+\d{4}\s*(?:года|г\.)?)\b", t)
    if not city_fix:
        city_fix = _find(r"г\.\s*([А-ЯЁ][а-яё\-]+)\s+\d{1,2}\s+[А-Яа-я]+?\s+\d{4}", t)

    # 7) первый абзац (со ссылкой на ст.179 УПК)
    report_begin = _find(
        r"""(
            (?:^|\n)\s*В\s+соответствии\s+с\s+частью\s+1\s+статьи\s+179[\s\S]{0,1000}?
            начаты\s+неотложные\s+следственные\s+действия\.
        )""",
        t, flags=re.I | re.X
    )
    # fallbacks на случай синонимов/переносов
    if not report_begin:
        report_begin = _find(
            r"((?:^|\n)\s*В\s+соответствии[\s\S]{0,1200}?следственные\s+действия\.)", t
        )

    # 8) абзац про регистрацию в ЕРДР (№ и дата)
    report_next = _find(
        r"""(
            (?:^|\n)[^\n]*?(?:Един\w*\s+реестр\w*|ЕРДР)[\s\S]{0,400}?
            под\W*№\s*\d+\s*(?:от\s*\d{1,2}\s+[А-Яа-я]+?\s+\d{4}\s*г(?:ода|\.)?)?
            \.
        )""",
        t, flags=re.I | re.X
    )

    # 9) завершающий абзац («При отсутствии обстоятельств…»)
    report_end = _find(
        r"""(
            (?:^|\n)\s*При\s+отсутствии\s+обстоятельств[\s\S]{0,600}?\.
        )""",
        t, flags=re.I | re.X
    )

    # 10) блок должности подписанта (Старший следователь … РК …)
    post_new = _find(
        r"""(
            Старший\s+следователь[\s\S]{0,300}?
            (?:Агентств[ао]\s+Республик[ии]\s+Казахстан[\s\S]{0,200}?)?
            Департамент[^\n]*Павлодарской\s+области
        )""",
        t, flags=re.I | re.X
    )
    if not post_new:
        post_new = _find(r"(Старший\s+следователь[\s\S]{0,300}?(?:области|города)[^\n]*)", t)

    # 11) ФИО подписанта
    post_new_fn = _find(r"\b([А-ЯЁ][а-яё\-]+(?:\s+[А-ЯЁ]\.\s*[А-ЯЁ]\.)?)\s*$", t)  # последняя строка ФИО
    # если в подписи фигурирует конкретное ФИО (пример: Закиев Е.Б.)
    m_fio_end = re.search(r"(Закиев\s*Е\.?\s*Б\.?)", t, flags=re.I)
    if m_fio_end:
        post_new_fn = m_fio_end.group(1)

    # косметика
    post_main    = _clean_spaces_keep_newlines(post_main)
    post_main_fn = _clean_spaces_keep_newlines(_fio_any_order(post_main_fn or "") or post_main_fn)
    report_begin = _clean_spaces_keep_newlines(report_begin)
    report_next  = _clean_spaces_keep_newlines(report_next)
    report_end   = _clean_spaces_keep_newlines(report_end)
    post_new     = _clean_spaces_keep_newlines(post_new)
    if post_new_fn:
        post_new_fn = post_new_fn.strip().rstrip(".")

    return {
        "type_document": type_document,
        "view_document": view_document,
        "post_main": post_main,         # Прокурор Павлодарской области
        "post_main_fn": post_main_fn,   # Жуйриктаев Б.К.
        "city_fix": city_fix,           # г.Павлодар
        "date_doc": date_doc,           # 17 апреля 2025 года
        "report_begin": report_begin,   # первый абзац (ст.179 УПК)
        "report_next": report_next,     # абзац с № ЕРДР
        "report_end": report_end,       # завершающий абзац
        "post_new": post_new,           # Старший следователь ... ДЭР по Павлодарской области
        "post_new_fn": post_new_fn,     # Закиев Е.Б.
    }
