# app/services/parser_report_erdr.py
from __future__ import annotations
from typing import Optional, Dict
import re

# --- утилиты ---

def _find(pattern: str, text: str, flags=re.I|re.S) -> Optional[str]:
    m = re.search(pattern, text, flags)
    return m.group(1).strip() if m else None

def _clean_spaces_keep_newlines(s: Optional[str]) -> Optional[str]:
    if s is None:
        return None
    s = re.sub(r"[ \t]+\n", "\n", s)
    s = re.sub(r"[ \t]+", " ", s)
    return s.strip()

def _fio_any_order(text: str) -> Optional[str]:
    """
    Находит ФИО типа 'Есенов Е.О.' или 'Е.О. Есенов' и нормализует в 'Есенов Е.О.'.
    """
    m1 = re.search(r"(?m)^\s*([А-ЯЁ][а-яё\-]+)\s+([А-ЯЁ]\.)\s*([А-ЯЁ]\.)\s*$", text)  # Есенов Е.О.
    if m1:
        return f"{m1.group(1)} {m1.group(2)}{m1.group(3)}".rstrip(".")  # без завершающей точки
    m2 = re.search(r"(?m)^\s*([А-ЯЁ]\.)\s*([А-ЯЁ]\.)\s*([А-ЯЁ][а-яё\-]+)\s*$", text)  # Е.О. Есенов
    if m2:
        return f"{m2.group(3)} {m2.group(1)}{m2.group(2)}".rstrip(".")
    return None

def _post_main_above_fio(text: str, fio_line: str | None) -> Optional[str]:
    """
    Берём блок строк над ФИО (2–6 строк), начиная с роли
    (Руководитель/Начальник/и.о./вр.и.о.).
    Возвращаем только должность без строки ФИО.
    """
    lines = [ln.rstrip() for ln in text.splitlines()]
    fio_idx = None
    fio_re = re.compile(rf"^\s*{re.escape(fio_line or '')}\s*$") if fio_line else None
    if fio_re:
        for i, ln in enumerate(lines):
            if fio_re.match(ln):
                fio_idx = i
                break
    if fio_idx is None:
        return None

    role_re = re.compile(
        r"^(Руководител[ья]|Начальник|Заместитель\s+руководителя|И\.?\s*о\.?|Вр\.?\s*и\.?\s*о\.?)\b",
        re.IGNORECASE,
    )
    start = None
    for j in range(max(0, fio_idx - 6), fio_idx):
        if role_re.match(lines[j].strip()):
            start = j
            break
    if start is None:
        start = max(0, fio_idx - 3)

    # формируем блок над ФИО
    block_lines = [ln.strip() for ln in lines[start:fio_idx] if ln.strip()]

    # убираем строки, совпадающие с fio_line (на случай если ФИО «прилипло» к должности)
    if fio_line:
        fio_norm = fio_line.strip()
        block_lines = [ln for ln in block_lines if fio_norm not in ln]

    block = "\n".join(block_lines)
    return block or None


# --- основной парсер ЕРДР ---

def parse_report_erdr_fields(text: str, filename: Optional[str] = None) -> Dict[str, Optional[str]]:
    """
    Возвращаем те же 11 полей (для единой схемы), но с наполнением под Рапорт ЕРДР.
    """
    # 1–2) тип/вид
    type_document = "Рапорт ЕРДР"
    view_document = "Рапорт"

    # 3–4) должность руководителя и ФИО в шапке (Е.О. Есенов / Есенов Е.О.)
    post_main_fn = _fio_any_order(text)
    pat_post_main = re.compile(
    r"(Руководител[ья]\s+Департамента[\s\S]{0,200}?"      # Руководитель Департамента ...
    r"экономическ[^\n]*?[\s\S]{0,200}?"                   # экономических (с переносами)
    r"расследовани[^\n]*?\s+по[\s\S]{0,200}?"             # расследований по
    r"[А-ЯЁ][а-яё\-]+ской\s+области)",                    # <регион>ской области
    re.IGNORECASE
)

    m = pat_post_main.search(text)
    if m:
    # аккуратно нормализуем, сохраняя переносы строк
        post_main = "\n".join(ln.strip() for ln in m.group(1).splitlines() if ln.strip())
    else:
    # fallback: берем блок над ФИО, как раньше, и удаляем возможную строку с ФИО
        post_main = _post_main_above_fio(text, post_main_fn)

    # 5–6) город и дата
    city_fix = _find(r"(?m)^\s*г\.\s*([А-ЯЁ][а-яё\-]+)\s*$", text) or _find(r"\bг\.\s*([А-ЯЁ][а-яё\-]+)\b", text)
    date_doc = _find(r"(?m)^\s*(\d{1,2}\s+[А-Яа-я]+?\s+\d{4}\s*(?:года|г\.)?)\s*$", text) \
               or _find(r"\b(\d{1,2}\s+[А-Яа-я]+?\s+\d{4}\s*(?:года|г\.)?)\b", text)

    # 7) начало (описательная часть) — от "Я, старший следователь..." до "Учитывая наличие..."
    report_begin = _find(
        r"((?:^|\n)\s*Я,\s*старший\s+следователь[\s\S]*?)\n\s*Учитывая\s+наличие",
        text
    )
    if report_begin is None:
        # fallback: от "о выявлении сведений ..." до "Учитывая ..."
        report_begin = _find(
            r"((?:^|\n)\s*о\s+выявлении\s+сведений[\s\S]*?)\n\s*Учитывая\s+наличие",
            text
        )

    # 8) регистрация в ЕРДР — абзац с номером
    report_next = _find(
    r"""(
        (?:^|\n)\s*Учитывая\s+наличие      # начало абзаца
        [\s\S]{0,1000}?                    # весь текст с переносами
        (?:Един\w*\s+реестр\w*|ЕРДР)       # 'Едином реестре …' или 'ЕРДР'
        [\s\S]{0,400}?                     # до 'под №'
        под\W*№\W*\d+                      # сам номер
        \.                                 # точка в конце
    )""",
    text,
    flags=re.I | re.X
)


    # 9) подпись/передача — блок "Старший следователь ... Закиев Е.Б."
    report_end = _find(
        r"""(
            (?:^|\n)\s*К\s+рапорту\s+прилагаются   # начало абзаца
            [^\n\.]*                                # первая строка до точки
            (?:\n[^\n\.]*)*                         # возможные переносы строки, но без точек
            \.                                      # завершающая точка этого предложения
        )""",
        text,
        flags=re.I | re.X
    )

    # 10) должность получателя (в ЕРДР это же автор/подписант)
    post_new = _find(
        r"(Старший\s+следователь[\s\S]*?Республики\s+Казахстан)",
        text
    )

    # 11) ФИО подписанта
    post_new_fn = _find(r"(Закиев\s*Е\.?\s*Б\.?)", text)

    # косметика (без ломания переносов в 7–9)
    post_main = _clean_spaces_keep_newlines(post_main)
    report_begin = _clean_spaces_keep_newlines(report_begin)
    report_next  = _clean_spaces_keep_newlines(report_next)
    report_end   = _clean_spaces_keep_newlines(report_end)
    post_new     = _clean_spaces_keep_newlines(post_new)

    if post_main_fn: post_main_fn = post_main_fn.strip().rstrip(".")
    if post_new_fn:  post_new_fn  = post_new_fn.strip().rstrip(".")

    # если город/дата не вытащились: пробуем "г.Павлодар 17 апреля 2025 года" в одной строке
    if not city_fix:
        city_fix = _find(r"г\.\s*([А-ЯЁ][а-яё\-]+)\s+\d{1,2}\s+[А-Яа-я]+?\s+\d{4}", text)

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
