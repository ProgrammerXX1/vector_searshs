# app/services/type_files/_7_zayavlenie_lang.py
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

# ---- общие классы символов ----
RU_UP   = "А-ЯЁӘІҢҒҮҰҚӨҺ"
RU_LOW  = "а-яёәіңғүұқөһ"
LETTERS = f"{RU_UP}{RU_LOW}"

# ФИО: инициалы (Фамилия И.О.) и полная форма (Фамилия Имя Отчество)
FIO_INITS = rf"[{RU_UP}][{RU_LOW}\-]+(?:\s+[{RU_UP}]\.\s*[{RU_UP}]\.)"
FIO_FULL  = rf"[{RU_UP}][{RU_LOW}\-]+(?:\s+[{RU_UP}][{RU_LOW}\-]+){{1,2}}"

# ---------- ПАРСЕР: Заявление о языке уголовного судопроизводства ----------
def parse_zayavlenie_yazyk(text: str, filename: Optional[str] = None) -> Dict[str, Optional[str]]:
    """
    Универсальный парсер под шаблон «ЗАЯВЛЕНИЕ о языке уголовного судопроизводства».
    Возвращает РОВНО 11 полей.
    """
    t = (text or "").replace("ё", "е")

    type_document = "Заявление о языке уголовного судопроизводства"
    view_document = "Заявление"

    # ---- город (иногда есть, дата обычно отсутствует) ----
    city_fix = _find(rf"(?m)^\s*г\.\s*([{RU_UP}][{LETTERS}\-]+)\s*$", t) \
            or _find(rf"\bг\.\s*([{RU_UP}][{LETTERS}\-]+)\b", t)
    date_doc = None

    # ---- верхний адресат/должность (динамически, без привязки к фамилиям/области) ----
    # Примеры: «Следователю по ОВД», «Следственного управления ДЭР/Департамента экономических расследований по <области>»
    post_main = _find(
        rf"""(?isx)
        (
          Следовател\w*\s+по\s+ОВД
          (?:[\s\r\n]+(?:Следственн\w*\s+управлени\w+))?
          (?:[\s\r\n]+(?:
                ДЭР
              | Департамент[а]?\s+экономическ\w*\s+расследован\w*
          ))?
          [\s\r\n]+по\s+[{LETTERS}\.\- ]+?\s+области
        )
        """, t
    )

    # ФИО адресата (рядом с шапкой; любые звания между блоком и ФИО допустимы)
    post_main_fn = _find(
        rf"""Следовател[\s\S]{{0,400}}?
             (?:майор|подполковник|полковник|капитан|лейтенант|старший\s+лейтенант)?\s*
             (?:СЭР|полиции)?\s*
             ({FIO_INITS})
        """, t, flags=re.I | re.S | re.X
    )
    # fallback: первое встреченное ФИО-инициалы сверху документа
    if not post_main_fn:
        first_inits = _find(rf"^.*?({FIO_INITS})", t, flags=re.I | re.S | re.M)
        post_main_fn = first_inits

    # ---- заголовок ----
    # держим для устойчивости, но само значение не сохраняем
    _ = _find(r"ЗАЯВЛЕНИЕ[\s\r\n]+о\s+язык\w+[\s\r\n]+уголовн\w+\s+судопроизводств\w*", t)

    # ---- блок "От:" (заявитель) ----
    # Собираем данные заявителя в один блок
    applicant_fio = _find(rf"От\s*:\s*({FIO_FULL})", t)
    applicant_dob = _find(r"(\d{2}\.\d{2}\.\d{4})\s*г\.?\s*р\.?", t)
    applicant_addr = _find(r"(?:место\s+проживани[яия]|адрес)\s*:\s*([^\n;]+)", t)
    applicant_phone = _find(r"(?:Сотовый|Мобильн\w*|Телефон)[^:]{0,8}:\s*([\d\+\-\(\) ]{7,})", t)

    # Формируем единый блок данных заявителя
    applicant_parts = []
    if applicant_fio:
        applicant_parts.append(f"От: {applicant_fio}")
    if applicant_dob:
        applicant_parts.append(f"{applicant_dob} г.р.")
    if applicant_addr:
        applicant_parts.append(f"место проживания: {applicant_addr}")
    if applicant_phone:
        applicant_parts.append(f"Сотовый телефон: {applicant_phone}")
    
    report_next = ";\n".join(applicant_parts) if applicant_parts else None

    # ---- основной текст заявления ----
    report_begin = _find(
        r"""(
            Мне\s+разъяснена\s+сущность\s+статьи\s+30\s+УПК\s+РК[\s\S]*?
            с\s+моим\s+материальным\s+положением\.
        )""", t, flags=re.I | re.X)

    # ---- "Заявление принял:" ----
    report_end = _find(r"(Заявлени[ея]\s+принял[:：])", t)

    # ---- нижний служебный блок (кто принял заявление) ----
    post_new = _find(
        rf"""(
            Следовател\w*\s+по\s+ОВД\s*
            (?:\n|\s)*
            Следственн\w*\s+управлени\w+\s*
            (?:\n|\s)*
            Департамент[а]?\s+экономическ\w*\s+расследован\w*\s*
            (?:\n|\s)*
            по\s+[{LETTERS}\.\- ]+?\s+области
        )""", t, flags=re.I | re.X)

    # ФИО принявшего заявление (ищем после "Заявление принял:")
    post_new_fn = _find(rf"Заявлени[ея]\s+принял[:：]\s*.*?({FIO_INITS})", t, flags=re.I | re.S)
    
    # Fallback: последнее ФИО в документе
    if not post_new_fn:
        post_new_fn = _find(rf"({FIO_INITS})\s*$", t[-200:])

    # ---- финальная очистка ----
    clean = _clean_spaces_keep_newlines
    post_main = clean(post_main)
    post_main_fn = post_main_fn.strip().rstrip(".") if post_main_fn else None
    city_fix = clean(city_fix)
    report_begin = clean(report_begin)
    report_next = clean(report_next)
    report_end = clean(report_end)
    post_new = clean(post_new)
    post_new_fn = post_new_fn.strip().rstrip(".") if post_new_fn else None

    return {
        "type_document": type_document,
        "view_document": view_document,
        "post_main": post_main,
        "post_main_fn": post_main_fn,
        "city_fix": city_fix,
        "date_doc": None,  # всегда None для заявлений о языке
        "report_begin": report_begin,
        "report_next": report_next,
        "report_end": report_end,
        "post_new": post_new,
        "post_new_fn": post_new_fn,
    }
