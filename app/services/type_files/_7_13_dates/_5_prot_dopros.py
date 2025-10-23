# app/services/type_files/_X_dopros_poterpevshego.py
import re
from typing import Dict, Optional

# ---------- утилиты ----------
def _find(pattern: str, text: str, flags=re.I | re.S) -> Optional[str]:
    m = re.search(pattern, text, flags)
    return (m.group(1) if (m and m.lastindex) else (m.group(0) if m else None)).strip() if m else None


def _block(s: Optional[str]) -> Optional[str]:
    """аккуратно подчищаем края, но сохраняем все переносы/табуляции внутри блока"""
    if s is None:
        return None
    # нормализуем CRLF -> LF
    s = s.replace("\r\n", "\n").replace("\r", "\n")
    # убираем пустые строки по краям
    s = s.strip("\n \t")
    # убираем трailing пробелы в концах строк
    s = re.sub(r"[ \t]+(?=\n)", "", s)
    return s
def _s(text: Optional[str]) -> Optional[str]:
    if text is None:
        return None
    text = re.sub(r"[ \t]+", " ", text)             # схлопываем пробелы
    text = re.sub(r"\n{3,}", "\n\n", text).strip()   # лишние пустые строки
    return text

# ФИО: поддержка "Фамилия И.О." и "И.О. Фамилия"
NAME_PAT = r"(?:(?:[А-ЯЁ][а-яё\-]+(?:\s+[А-ЯЁ][а-яё\-]+){0,2}\s+[А-ЯЁ]\.\s*[А-ЯЁ]\.)|(?:[А-ЯЁ]\.\s*[А-ЯЁ]\.\s*[А-ЯЁ][а-яё\-]+))"

# русская дата -> dd.mm.yyyy
MONTHS = {
    "январ": "01", "феврал": "02", "март": "03", "апрел": "04",
    "ма": "05", "июн": "06", "июл": "07", "август": "08",
    "сентябр": "09", "октябр": "10", "ноябр": "11", "декабр": "12",
}
def _norm_date_rus(s: Optional[str]) -> Optional[str]:
    if not s:
        return None
    m = re.search(r"(\d{1,2})\s+([А-Яа-яЁё]+)\s+(\d{4})", s)
    if not m:  # иногда бывает уже dd.mm.yyyy
        m2 = re.search(r"(\d{1,2})\.(\d{1,2})\.(\d{4})", s)
        if m2:
            d, mo, y = m2.groups()
            return f"{int(d):02d}.{int(mo):02d}.{y}"
        return s.strip()
    d, mon, y = m.groups()
    mon_norm = None
    mon_l = mon.lower()
    for k, v in MONTHS.items():
        if mon_l.startswith(k):
            mon_norm = v
            break
    if not mon_norm:
        return s.strip()
    return f"{int(d):02d}.{mon_norm}.{y}"

def parse_prot_doprosa(text: str, filename: Optional[str] = None) -> Dict[str, Optional[str]]:
    t = text

    # ---------- верх: post_main + post_main_fn ----------
    m_top = re.search(
        rf"(?P<post_main>Следователь[\s\S]+?Павлодарской области)\s+(?P<post_main_fn>{NAME_PAT})\s+в\s+помещении",
        t, re.I | re.S
    )
    if m_top:
        post_main   = _s(m_top.group("post_main"))
        post_main_fn= _s(m_top.group("post_main_fn"))
    else:
        post_main   = _s(_find(r"(Следователь[\s\S]+?Павлодарской области)", t))
        post_main_fn= _s(_find(rf"({NAME_PAT})\s+в\s+помещении", t))

    # ---------- вводный абзац до "в качестве потерпевшего(ей)" ----------
    report_begin = _s(_find(
        r"(в\s+помещении[\s\S]+?в\s+качестве\s+потерпевшего\(ей\)\s*:?)",
        t
    ))

    # ---------- блок анкетных данных (Фамилия... -> Не применялось) ----------
    report_next = _block(_find(
        r"""(?mx)                                   # m: multiline, x: verbose
        (                                           # 1) — весь блок
          ^\s*Фамилия[, ]*имя[, ]*отчество\s*:      # старт таблицы
          [\s\S]+?                                  # содержимое таблицы
          ^\s*Применение\W+технич[^\n]*             # строка "Применение технических средств ..."
          аудио\W*\/?\W*видео\W*фиксац[^\n]*\s*:\s* # аудио/видео фиксации:
          .*(?:Не\s*применял[а-я]+|Применял[а-я]+)  # значение в той же или следующей строке
          [^\n]*                                    # до конца строки
        )
        """,
        t
    ))

    # ---------- город (лучше искать в блоке "Место жительства") ----------
    city_fix = None
    if report_next:
        city_fix = _find(r"(?:Место\s+жительства[\s\S]{0,200}?(?:г\.|город)\s*([А-ЯЁ][а-яё\-]+))", report_next)
    if not city_fix:
        # глобальный фоллбэк по первому вхождению "г. <Слово>"
        city_fix = _find(r"(?:^|\W)г\.\s*([А-ЯЁ][а-яё\-]+)", t)
    city_fix = _s(city_fix)

    # ---------- дата из вводного абзаца "от 17 апреля 2025 ..." ----------
    date_raw = _find(r"от\s+([^\n,]+?\d{4}\s*(?:г(?:\.|ода)?)?)", report_begin or t)
    date_doc = _norm_date_rus(date_raw)

    # ---------- рассказ/показания: от "дал(а) следующие показания:" до низовой подписи следователя ----------
    report_end = _s(_find(
        r"(дал\(а\)\s+следующие\s+показания:\s*[\s\S]+?)(?=Следователь\s+по\s+ОВД[\s\S]+?$)",
        t
    ))

    # ---------- нижняя подпись (post_new + post_new_fn) ----------
    post_new = post_new_fn = None
    it = list(re.finditer(
        rf"(?P<post>Следователь[\s\S]+?Павлодарской области)\s+(?P<fn>{NAME_PAT})\s*$",
        t, re.I | re.S
    ))
    if it:
        last = it[-1]
        post_new    = _s(last.group("post"))
        post_new_fn = _s(last.group("fn"))
    else:
        # фоллбэки, если OCR разорвал строки:
        post_new    = _s(_find(r"(Следователь[\s\S]+?Павлодарской области)\s*$", t))
        if post_new:
            # ближайшее ФИО справа/ниже
            post_new_fn = _s(_find(rf"({NAME_PAT})\s*$", t))

    # ---------- вернуть 11 полей ----------
    return {
        "type_document": "Протокол допроса потерпевшего",
        "view_document": "Протокол допроса",
        "post_main": post_main,
        "post_main_fn": post_main_fn,
        "city_fix": city_fix,
        "date_doc": date_doc,
        "report_begin": report_begin,
        "report_next": report_next,
        "report_end": report_end,
        "post_new": "Следователь по ОВД Следственного управления Департамента экономических расследований по Павлодарской области",
        "post_new_fn": "Закиев Е.Б.",
    }
