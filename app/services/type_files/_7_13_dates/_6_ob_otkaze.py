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

# ---------- ПАРСЕР: Заявление об отказе от ознакомления ----------
def parse_zayavlenie_otkaz(text: str, filename: Optional[str] = None) -> Dict[str, Optional[str]]:
    """
    Обрабатывает оба варианта:
      • с блоком «От: <ФИО, ДР, адрес, телефон>»
      • без «От:», когда данные заявителя перечислены строками
    Возвращает РОВНО 11 полей.
    """
    t = (text or "").replace("ё", "е")

    type_document = "Заявление об отказе от ознакомления"
    view_document = "Заявление"

    # --- город / дата (дата обычно у подписи заявителя; берём ПОСЛЕДНЮЮ) ---
    city_fix = _find(rf"(?m)^\s*г\.\s*([{RU_UP}][{LETTERS}\-]+)\s*$", t) \
            or _find(rf"\bг\.\s*([{RU_UP}][{LETTERS}\-]+)\b", t)
    all_dates = _find_all(r"(\d{2}\.\d{2}\.\d{4})\s*г?\.?", t)
    date_doc = all_dates[-1] if all_dates else None

    # --- адресат/должность (верхний правый блок) ---
    post_main = _find(
        rf"""(?isx)
        (
          Следовател\w*\s+по\s+ОВД
          (?:[\s\r\n]+Следственн\w*\s+управлени\w+)?
          [\s\r\n]+(?:ДЭР|Департамент[а]?\s+экономическ\w*\s+расследован\w*)
          [\s\r\n]+по\s+[{LETTERS}\.\- ]+?\s+области
          (?:[\s\r\n]+(?:майор|подполковник|полковник|капитан|лейтенант|старший\s+лейтенант)\s+(?:СЭР|полиции))?
        )
        """, t
    )
    # ФИО адресата рядом
    post_main_fn = _find(
        rf"""Следовател[\s\S]{{0,400}}?
             (?:майор|подполковник|полковник|капитан|лейтенант|старший\s+лейтенант)?\s*
             (?:СЭР|полиции)?\s*
             ({FIO_INITS})
        """, t, flags=re.I | re.S | re.X
    )

    # --- блок заявителя (оба варианта) ---
    # A) «От: ... (до заголовка ЗАЯВЛЕНИЕ)»
    applicant_block = _find(r"(От\s*:[\s\S]{0,500}?)(?=\n\s*ЗАЯВЛЕНИЕ\b)", t)
    # B) если нет «От:», возьмём 400 символов перед заголовком
    if not applicant_block:
        pre = _find(r"([\s\S]{0,400})\n\s*ЗАЯВЛЕНИЕ\b", t)
        applicant_block = pre or ""

    applicant_fio   = _find(rf"(?:От\s*:)?\s*({FIO_FULL})", applicant_block)
    applicant_dob   = _find(r"(\d{2}\.\d{2}\.\d{4})\s*г\.?\s*р\.?", applicant_block)
    applicant_addr  = _find(r"(?:адрес[ау]:|место\s+проживани[яия]\s*:)\s*([^\n]+)", applicant_block) \
                   or _find(r"(г\.[^,\n]+,\s*ул\.[^\n]+)", applicant_block)
    applicant_phone = _find(r"(?:Сотовый|Мобильн\w*|Телефон)[^:]{0,8}:\s*([\d\+\-\(\) ]{7,})", applicant_block)

    # --- основная часть заявления ---
    # от «ЗАЯВЛЕНИЕ» до «Заявление принял:»/подписи/нижнего блока
    report_begin = _find(
        r"""ЗАЯВЛЕНИЕ\s*
            ([\s\S]*?)
            (?=\n\s*Заявлени[ея]\s+принял[:：]|\n\s*(?:Касангужинов|[{РУ_UP}][{РУ_LOW}\-]+\s*[{РУ_UP}]\.[{РУ_UP}]\.)|\n\s*Следовател\w*\s+по\s+ОВД|$)
        """.replace("{РУ_UP}", RU_UP).replace("{РУ_LOW}", RU_LOW),
        t, flags=re.I | re.S | re.X
    )

    # --- маркер «Заявление принял» ---
    report_end = _find(r"(Заявлени[ея]\s+принял[:：])", t)

    # --- нижний служебный блок + подпись справа (следователь) ---
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
        ({FIO_INITS})
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
        inits = _find_all(rf"\b({FIO_INITS})\b", t)
        post_new_fn = inits[-1] if inits else None

    # --- № ЕРДР (если есть) + сводка по заявителю в report_next ---
    erdr = _find(r"(№\s*\d{6,})", report_begin or "") or _find(r"(№\s*\d{6,})", t)
    summary = ", ".join([v for v in [
        applicant_fio,
        f"ДР: {applicant_dob}" if applicant_dob else None,
        f"адрес: {applicant_addr}" if applicant_addr else None,
        f"тел.: {applicant_phone}" if applicant_phone else None,
        erdr
    ] if v]) or None
    report_next = summary

    # --- чистка ---
    clean = _clean_spaces_keep_newlines
    post_main    = clean(post_main)
    post_main_fn = post_main_fn.strip().rstrip(".") if post_main_fn else None
    city_fix     = clean(city_fix)
    date_doc     = clean(date_doc)
    report_begin = clean(report_begin)
    report_next  = clean(report_next)
    report_end   = clean(report_end)
    post_new     = clean(post_new)
    post_new_fn  = post_new_fn.strip().rstrip(".") if post_new_fn else None

    return {
        "type_document": type_document,
        "view_document": view_document,
        "post_main": post_main,
        "post_main_fn": post_main_fn,
        "city_fix": city_fix,
        "date_doc": date_doc,
        "report_begin": report_begin,  # текст заявления
        "report_next": report_next,    # сводка: заявитель + №ЕРДР
        "report_end": report_end,      # маркер «Заявление принял:»
        "post_new": post_new,          # нижний служебный блок
        "post_new_fn": post_new_fn,    # подпись (Фамилия И.О.)
    }
