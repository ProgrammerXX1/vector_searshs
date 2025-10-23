import re
from typing import Dict, Optional

# ---------- утилиты ----------
def _find(pattern: str, text: str, flags=re.I | re.S) -> Optional[str]:
    m = re.search(pattern, text, flags)
    if not m:
        return None
    if m.lastindex:
        for i in range(1, m.lastindex + 1):
            g = m.group(i)
            if g:
                return g.strip()
        return None
    return m.group(0).strip()

def _find_all(pattern: str, text: str, flags=re.I | re.S) -> list[str]:
    return [g.strip() for g in re.findall(pattern, text, flags)]

def _clean_spaces_keep_newlines(s: Optional[str]) -> Optional[str]:
    if s is None:
        return None
    s = re.sub(r"[ \t]+\n", "\n", s)
    s = re.sub(r"[ \t]+", " ", s)
    return s.strip()

RU_UP   = "А-ЯЁӘІҢҒҮҰҚӨҺ"
RU_LOW  = "а-яёәіңғүұқөһ"
LETTERS = f"{RU_UP}{RU_LOW}"

FIO_INITS = rf"[{RU_UP}][{RU_LOW}\-]+(?:\s+[{RU_UP}]\.\s*[{RU_UP}]\.)"
FIO_FULL  = rf"[{RU_UP}][{RU_LOW}\-]+(?:\s+[{RU_UP}][{RU_LOW}\-]+){{1,2}}"

# ---------- парсер: ИСКОВОЕ ЗАЯВЛЕНИЕ ----------
def parse_iskovoe_zayavlenie(text: str, filename: Optional[str] = None) -> Dict[str, Optional[str]]:
    t = (text or "").replace("ё", "е")

    type_document = "Исковое заявление"
    view_document = "Заявление"

    # --- город/дата ---
    city_fix = _find(rf"(?m)^\s*г\.\s*([{RU_UP}][{LETTERS}\-]+)\s*$", t) \
            or _find(rf"\bг\.\s*([{RU_UP}][{LETTERS}\-]+)\b", t)
    # возьмём первую дату формата дд.мм.гггг (внизу возле подписи она есть)
    date_doc = _find(r"\b(\d{2}\.\d{2}\.\d{4})\s*г?\.?", t)

    # --- адресат (правый верх) ---
    # берём блок от "Председателю суда" до "Гражданский истец"/"ИСКОВОЕ ЗАЯВЛЕНИЕ"
    post_main = _find(
        r"""(?isx)
        (Председател[юа]\s+суда[\s\S]{0,400}?
          (?=Гражданск\w+\s+истец:|ИСКОВОЕ\s+ЗАЯВЛЕНИЕ)
        )
        """, t
    )
    post_main_fn = None  # обычно ФИО адресата не указано

    # --- блоки сторон ---
    plaint_blk = _find(r"(Гражданск\w+\s+истец:\s*[\s\S]{0,250}?)(?=Ответчик:|ИСКОВОЕ\s+ЗАЯВЛЕНИЕ)", t)
    def_blk    = _find(r"(Ответчик:\s*[\s\S]{0,200}?)(?=ИСКОВОЕ\s+ЗАЯВЛЕНИЕ)", t)

    plaint_fio = _find(rf"Гражданск\w+\s+истец:\s*({FIO_FULL})", plaint_blk or "")
    plaint_dob = _find(r"(\d{2}\.\d{2}\.\d{4})\s*г\.?\s*р\.?", plaint_blk or "")
    plaint_addr= _find(r"(?:проживающ\w*\s+по\s+адрес[ую]:\s*)([^\n]+)", plaint_blk or "")

    def_fio    = _find(rf"Ответчик:\s*(?:Представител\w*\s+проекта\s+«?[^»\n]+»?,?\s*)?({FIO_FULL})", def_blk or "")
    def_dob    = _find(r"(\d{2}\.\d{2}\.\d{4})\s*г\.?\s*р\.?", def_blk or "")

    # сумма иска (первая встреченная)
    amount = _find(r"(\d[\d\s]{1,14})\s*тенг", t)
    if amount:
        amount = re.sub(r"\s+", " ", amount).strip()

    # --- тело: от "ИСКОВОЕ ЗАЯВЛЕНИЕ" до "ПРОШУ:" ---
    report_begin = _find(
        r"""ИСКОВОЕ\s+ЗАЯВЛЕНИЕ\s*
            ([\s\S]*?)
            (?=\n\s*ПРОШУ[:：]|\Z)
        """, t, flags=re.I | re.S | re.X
    )

    # --- ПРОШУ: до даты/подписи/служебного блока ---
    report_end = _find(
        r"""ПРОШУ[:：]\s*
            ([\s\S]*?)
            (?=\n\s*\d{2}\.\d{2}\.\d{4}|\n\s*Заявлени[ея]\s+принял|Следовател\w*\s+по\s+ОВД|$)
        """, t, flags=re.I | re.S | re.X
    )

    # --- нижний служебный блок + подпись справа (как в заявлении о языке) ---
    post_new = None
    post_new_fn = None

    m = re.search(
        rf"""
        (                                         # блок слева
          Следовател\w*\s+по\s+ОВД
          (?:[\s\r\n]+Следственн\w*\s+управлени\w+)?
          [\s\r\n]+(?:Департамент[а]?\s+экономическ\w*\s+расследован\w*|ДЭР)
          [\s\r\n]+по\s+[{LETTERS}\.\- ]+?\s+области
        )
        [\s\r\n,;:–—-]*
        (?:\S[^\n]*?)?
        [\s\r\n]*
        ({FIO_INITS})                             # Фамилия И.О. справа (например, Закиев Е.Б.)
        """, t, flags=re.I | re.S | re.X
    )
    if m:
        post_new = _clean_spaces_keep_newlines(m.group(1))
        post_new_fn = m.group(2).strip().rstrip(".")
    else:
        post_new = _find(
            rf"""(?isx)
            (
              Следовател\w*\s+по\s+ОВД
              (?:[\s\r\n]+Следственн\w*\s+управлени\w+)?
              [\s\r\n]+(?:Департамент[а]?\s+экономическ\w*\s+расследован\w*|ДЭР)
              [\s\r\n]+по\s+[{LETTERS}\.\- ]+?\s+области
            )
            """, t
        )
        # подпись — возьмём самое правдоподобное последнее ФИО-инициалы в документе
        init_list = _find_all(rf"\b({FIO_INITS})\b", t)
        post_new_fn = init_list[-1] if init_list else None

    # --- сводка по сторонам/сумме в report_next (контракт 11 полей не меняем) ---
    chunks = []
    if plaint_fio: chunks.append(f"Гражданский истец: {plaint_fio}")
    if plaint_dob: chunks.append(f"ДР истца: {plaint_dob}")
    if plaint_addr: chunks.append(f"адрес истца: {plaint_addr}")
    if def_fio: chunks.append(f"ответчик: {def_fio}")
    if def_dob: chunks.append(f"ДР ответчика: {def_dob}")
    if amount: chunks.append(f"сумма: {amount} тг")
    report_next = ", ".join(chunks) or None

    # --- чистка ---
    clean = _clean_spaces_keep_newlines
    post_main    = clean(post_main)
    city_fix     = clean(city_fix)
    date_doc     = clean(date_doc)
    report_begin = clean(report_begin)
    report_end   = clean(report_end)
    post_new     = clean(post_new)
    post_new_fn  = post_new_fn.strip().rstrip(".") if post_new_fn else None

    # Если post_main_fn равно null, заменяем его на post_new
    if post_main_fn is None:
        post_main_fn = post_new

    return {
        "type_document": type_document,
        "view_document": view_document,
        "post_main": post_main,       # адресат (суд)
        "post_main_fn": post_main_fn, # если null, то = post_new
        "city_fix": city_fix,
        "date_doc": date_doc,
        "report_begin": report_begin, # обоснование иска
        "report_next": report_next,   # сводка: стороны/даты/сумма
        "report_end": report_end,     # раздел «ПРОШУ:»
        "post_new": post_new,         # нижний служебный блок
        "post_new_fn": post_new_fn,   # подпись принимающего (Закиев Е.Б.)
    }
