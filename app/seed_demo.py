import os
import time
import json
import math
import random
import string
import requests
from datetime import datetime, timedelta, timezone

# --- Конфиги (ENV с дефолтами) ---
OLLAMA_URL   = os.getenv("OLLAMA_URL",   "http://localhost:11434/api/embeddings")
OLLAMA_MODEL = os.getenv("OLLAMA_EMB_MODEL", "nomic-embed-text")
WEAVIATE_URL = os.getenv("WEAVIATE_URL", "http://localhost:8080")
WEAVIATE_SCHEMA = f"{WEAVIATE_URL}/v1/schema"
WEAVIATE_OBJS   = f"{WEAVIATE_URL}/v1/objects"

# --- Имена коллекций ---
CASE         = "Case"
VICTIM       = "VictimProfile"
EXPERT       = "ExpertAnswer"
PROSECUTOR   = "ProsecutorAnswer"
DOC_CHUNK    = "DocumentChunk"
FIN_TX       = "FinancialTransaction"
COMM         = "CommunicationRecord"
RULING       = "CourtRuling"

ALL_CLASSES = [CASE, VICTIM, EXPERT, PROSECUTOR, DOC_CHUNK, FIN_TX, COMM, RULING]

# --- Распределение по классам (ровно 200) ---
COUNTS = {
    CASE: 8,
    VICTIM: 40,
    EXPERT: 30,
    PROSECUTOR: 30,
    DOC_CHUNK: 60,
    FIN_TX: 16,
    COMM: 8,
    RULING: 8,
}

random.seed(42)


# ---------------------------
# ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ
# ---------------------------
def http(method, url, **kw):
    r = requests.request(method, url, timeout=30, **kw)
    if not r.ok:
        raise RuntimeError(f"{method} {url} -> {r.status_code}: {r.text}")
    return r

def embed(text: str):
    # Ollama embeddings API (одна строка за раз)
    resp = http("POST", OLLAMA_URL, json={"model": OLLAMA_MODEL, "prompt": text})
    data = resp.json()
    return data["embedding"]

def class_exists(name: str) -> bool:
    r = requests.get(f"{WEAVIATE_SCHEMA}/{name}")
    return r.status_code == 200

def drop_class(name: str):
    if class_exists(name):
        print(f"🗑️  drop {name}")
        http("DELETE", f"{WEAVIATE_SCHEMA}/{name}")

def create_class(name: str, props: list[dict]):
    body = {
        "class": name,
        "vectorizer": "none",                # мы подаём свои вектора
        "vectorIndexType": "hnsw",
        "vectorIndexConfig": {"distance": "cosine"},
        "properties": props,
    }
    http("POST", WEAVIATE_SCHEMA, json=body)
    print(f"✅ created {name}")

def put_object(class_name: str, props: dict, embed_text_key: str):
    vec = embed(props[embed_text_key])
    body = {"class": class_name, "properties": props, "vector": vec}
    r = http("POST", WEAVIATE_OBJS, json=body)
    return r.json().get("id")


# ---------------------------
# СХЕМА (8 КОЛЛЕКЦИЙ)
# ---------------------------
# REST-схема Weaviate ждёт dataType в виде массивов строк: ["text"], ["int"], ["date"], ["number"]
def props_case():
    return [
        {"name": "case_id",   "dataType": ["int"]},
        {"name": "title",     "dataType": ["text"]},
        {"name": "status",    "dataType": ["text"]},
        {"name": "opened_at", "dataType": ["date"]},
        {"name": "lang",      "dataType": ["text"]},
        {"name": "notes",     "dataType": ["text"]},
    ]

def props_victim():
    return [
        {"name": "person_id", "dataType": ["int"]},
        {"name": "case_id",   "dataType": ["int"]},
        {"name": "full_name", "dataType": ["text"]},
        {"name": "iin",       "dataType": ["text"]},
        {"name": "birthdate", "dataType": ["date"]},
        {"name": "contacts",  "dataType": ["text"]},
        {"name": "notes",     "dataType": ["text"]},
        {"name": "lang",      "dataType": ["text"]},
    ]

def props_expert():
    return [
        {"name": "expert_id",   "dataType": ["int"]},
        {"name": "case_id",     "dataType": ["int"]},
        {"name": "specialty",   "dataType": ["text"]},
        {"name": "question",    "dataType": ["text"]},
        {"name": "answer",      "dataType": ["text"]},
        {"name": "answered_at", "dataType": ["date"]},
        {"name": "lang",        "dataType": ["text"]},
    ]

def props_prosecutor():
    return [
        {"name": "prosecutor_id", "dataType": ["int"]},
        {"name": "case_id",       "dataType": ["int"]},
        {"name": "filing_type",   "dataType": ["text"]},
        {"name": "text",          "dataType": ["text"]},
        {"name": "filed_at",      "dataType": ["date"]},
        {"name": "lang",          "dataType": ["text"]},
    ]

def props_doc_chunk():
    return [
        {"name": "case_id",     "dataType": ["int"]},
        {"name": "document_id", "dataType": ["int"]},
        {"name": "chunk_idx",   "dataType": ["int"]},
        {"name": "source_page", "dataType": ["int"]},
        {"name": "doc_type",    "dataType": ["text"]},
        {"name": "text",        "dataType": ["text"]},
        {"name": "created_at",  "dataType": ["date"]},
        {"name": "lang",        "dataType": ["text"]},
    ]

def props_fin_tx():
    return [
        {"name": "case_id",      "dataType": ["int"]},
        {"name": "iban",         "dataType": ["text"]},
        {"name": "account",      "dataType": ["text"]},
        {"name": "amount",       "dataType": ["number"]},
        {"name": "currency",     "dataType": ["text"]},
        {"name": "counterparty", "dataType": ["text"]},
        {"name": "timestamp",    "dataType": ["date"]},
        {"name": "note",         "dataType": ["text"]},
        {"name": "lang",         "dataType": ["text"]},
    ]

def props_comm():
    return [
        {"name": "case_id",   "dataType": ["int"]},
        {"name": "channel",   "dataType": ["text"]},
        {"name": "sender",    "dataType": ["text"]},
        {"name": "receiver",  "dataType": ["text"]},
        {"name": "timestamp", "dataType": ["date"]},
        {"name": "content",   "dataType": ["text"]},
        {"name": "lang",      "dataType": ["text"]},
    ]

def props_ruling():
    return [
        {"name": "case_id",      "dataType": ["int"]},
        {"name": "court",        "dataType": ["text"]},
        {"name": "ruling_type",  "dataType": ["text"]},
        {"name": "session_date", "dataType": ["date"]},
        {"name": "text",         "dataType": ["text"]},
        {"name": "lang",         "dataType": ["text"]},
    ]


# ---------------------------
# ГЕНЕРАЦИЯ ДАННЫХ (RU)
# ---------------------------
MONTHS = ["января","февраля","марта","апреля","мая","июня",
          "июля","августа","сентября","октября","ноября","декабря"]

def iso_days_ago(days: int) -> str:
    return (datetime.now(timezone.utc) - timedelta(days=days)).isoformat()

def rnd_date(start_days_ago=900, end_days_ago=0) -> str:
    d = random.randint(end_days_ago, start_days_ago)
    return iso_days_ago(d)

def rnd_phone():
    return f"+7{random.randint(700,799)}{random.randint(1000000,9999999)}"

def rnd_email(name="user"):
    return f"{name}{random.randint(1,9999)}@example.com"

def rnd_iin():
    return f"{random.randint(700101, 991231)}{random.randint(100000,999999)}"

def rnd_iban():
    return f"KZ{random.randint(10,99)}{random.randint(100000000000000000,999999999999999999)}"

def rnd_wallet():
    return f"0x{random.randint(10**15, 10**16-1):x}"

def rnd_amount():
    return random.randint(50_000, 10_000_000)

def rnd_case_title(i):
    return f"Дело TAKORP №{1000+i}"

def rnd_case_status():
    return random.choice(["open","investigation","trial","closed"])

def rnd_lang():
    return random.choice(["ru","kk"])

def rnd_doc_type():
    return random.choice(["interrogation","statement","bank_record","transfer","contract","order","webpage","notice"])

def rnd_court():
    return random.choice(["Алматинский горсуд","Суд Астаны","Суд Караганды"])

def rnd_channel():
    return random.choice(["telegram","email","whatsapp"])

def text_case(i):
    return f"Расследование по инвестициям в TAKORP. Эпизод {i}. Указаны переводы, реклама и обещания доходности."

def text_victim(i):
    opts = [
        "Пострадавший вложил средства в TAKORP и сообщил о потерях.",
        "Пострадавший указал перевод USDT на неизвестный кошелёк.",
        "Сообщил о снятиях наличных и переводах на карту аффилированных лиц."
    ]
    return random.choice(opts)

def text_expert_answer(i):
    opts = [
        "Установлено движение средств между кошельками A и B через биржу X.",
        "Адрес B получал средства с миксера, связанного с TAKORP.",
        "Обнаружены связующие транзакции на сумму свыше 5 млн тг."
    ]
    return random.choice(opts)

def text_prosecutor(i):
    opts = [
        "Ходатайство об аресте имущества TAKORP в размере 10 млн тг.",
        "Возражения на жалобу защиты по вопросу ареста имущества.",
        "Заключение о продлении срока расследования по делу."
    ]
    return random.choice(opts)

def text_doc_chunk(i):
    opts = [
        "Я вложил деньги в TAKORP",
        "Потерпевший сообщил о вложениях в пирамиду TAKORP",
        "TAKORP обещала 15% доходности еженедельно",
        "Реклама проекта TAKORP в Telegram-канале",
        "Договор инвестиций с TAKORP",
        "USDT перевод на криптокошелёк",
        "IBAN указан в платёжном поручении",
        "Контракт на поставку оборудования",
        "Приказ о проведении проверки",
        "Публичное уведомление на сайте проекта"
    ]
    return random.choice(opts)

def text_fin_note(i, amt):
    return f"Перевод {amt} тг на счёт {rnd_wallet()} по IBAN {rnd_iban()}"

def text_comm(i):
    return random.choice([
        "Нужно срочно перевести USDT на новый кошелёк.",
        "Публикуйте рекламу про высокий доход.",
        "Согласуйте договор инвестиций на следующей неделе."
    ])

def text_ruling(i):
    return random.choice([
        "Определение суда об аресте имущества.",
        "Постановление о назначении экспертизы.",
        "Решение суда по иску о взыскании убытков."
    ])


# ---------------------------
# СИДИНГ
# ---------------------------
def reset_schema():
    print("== RESET SCHEMA ==")
    for cls in ALL_CLASSES:
        drop_class(cls)

    create_class(CASE,       props_case())
    create_class(VICTIM,     props_victim())
    create_class(EXPERT,     props_expert())
    create_class(PROSECUTOR, props_prosecutor())
    create_class(DOC_CHUNK,  props_doc_chunk())
    create_class(FIN_TX,     props_fin_tx())
    create_class(COMM,       props_comm())
    create_class(RULING,     props_ruling())

def seed_cases(n):
    print(f"== SEED {CASE} x{n} ==")
    for i in range(n):
        props = {
            "case_id":   1 + i,
            "title":     rnd_case_title(i),
            "status":    rnd_case_status(),
            "opened_at": rnd_date(900, 600),
            "lang":      rnd_lang(),
            "notes":     text_case(i),
        }
        put_object(CASE, props, embed_text_key="notes")

def seed_victims(n):
    print(f"== SEED {VICTIM} x{n} ==")
    for i in range(n):
        nm = random.choice(["Иванов Иван","Петров Пётр","Серик Нурлан","Айбек Жандос","Мария Ким","Алия Жаксылыкова"])
        props = {
            "person_id": 100 + i,
            "case_id":   1 + (i % max(1, COUNTS[CASE])),
            "full_name": nm,
            "iin":       rnd_iin(),
            "birthdate": rnd_date(18000, 9000),
            "contacts":  f"{rnd_email('user')}, {rnd_phone()}",
            "notes":     text_victim(i),
            "lang":      "ru",
        }
        put_object(VICTIM, props, embed_text_key="notes")

def seed_experts(n):
    print(f"== SEED {EXPERT} x{n} ==")
    specs = ["финансовая экспертиза","криптоанализ","ИТ-экспертиза"]
    for i in range(n):
        props = {
            "expert_id":   200 + i,
            "case_id":     1 + (i % max(1, COUNTS[CASE])),
            "specialty":   random.choice(specs),
            "question":    "Проследить транзакции и источники средств",
            "answer":      text_expert_answer(i),
            "answered_at": rnd_date(400, 10),
            "lang":        "ru",
        }
        put_object(EXPERT, props, embed_text_key="answer")

def seed_prosecutors(n):
    print(f"== SEED {PROSECUTOR} x{n} ==")
    ftypes = ["motion","reply","answer"]
    for i in range(n):
        props = {
            "prosecutor_id": 300 + i,
            "case_id":       1 + (i % max(1, COUNTS[CASE])),
            "filing_type":   random.choice(ftypes),
            "text":          text_prosecutor(i),
            "filed_at":      rnd_date(500, 5),
            "lang":          "ru",
        }
        put_object(PROSECUTOR, props, embed_text_key="text")

def seed_doc_chunks(n):
    print(f"== SEED {DOC_CHUNK} x{n} ==")
    for i in range(n):
        props = {
            "case_id":     1 + (i % max(1, COUNTS[CASE])),
            "document_id": 10_000 + i,
            "chunk_idx":   i % 5,
            "source_page": (i % 7) + 1,
            "doc_type":    rnd_doc_type(),
            "text":        text_doc_chunk(i),
            "created_at":  rnd_date(700, 1),
            "lang":        "ru",
        }
        put_object(DOC_CHUNK, props, embed_text_key="text")

def seed_fin_tx(n):
    print(f"== SEED {FIN_TX} x{n} ==")
    for i in range(n):
        amt = rnd_amount()
        props = {
            "case_id":      1 + (i % max(1, COUNTS[CASE])),
            "iban":         rnd_iban(),
            "account":      f"ACC-{random.randint(1000,9999)}",
            "amount":       float(amt),
            "currency":     random.choice(["KZT","USD"]),
            "counterparty": f"CP-{random.randint(100000,999999)}",
            "timestamp":    rnd_date(800, 1),
            "note":         text_fin_note(i, amt),
            "lang":         "ru",
        }
        put_object(FIN_TX, props, embed_text_key="note")

def seed_comm(n):
    print(f"== SEED {COMM} x{n} ==")
    for i in range(n):
        props = {
            "case_id":   1 + (i % max(1, COUNTS[CASE])),
            "channel":   rnd_channel(),
            "sender":    f"user{random.randint(1,999)}",
            "receiver":  f"user{random.randint(1,999)}",
            "timestamp": rnd_date(600, 1),
            "content":   text_comm(i),
            "lang":      "ru",
        }
        put_object(COMM, props, embed_text_key="content")

def seed_ruling(n):
    print(f"== SEED {RULING} x{n} ==")
    for i in range(n):
        props = {
            "case_id":      1 + (i % max(1, COUNTS[CASE])),
            "court":        rnd_court(),
            "ruling_type":  random.choice(["определение","постановление","решение"]),
            "session_date": rnd_date(900, 1),
            "text":         text_ruling(i),
            "lang":         "ru",
        }
        put_object(RULING, props, embed_text_key="text")


def main():
    print("== START ==")
    # 1) Сброс схемы
    reset_schema()

    # 2) Засев по классам (в сумме 200 объектов)
    seed_cases(COUNTS[CASE])
    seed_victims(COUNTS[VICTIM])
    seed_experts(COUNTS[EXPERT])
    seed_prosecutors(COUNTS[PROSECUTOR])
    seed_doc_chunks(COUNTS[DOC_CHUNK])
    seed_fin_tx(COUNTS[FIN_TX])
    seed_comm(COUNTS[COMM])
    seed_ruling(COUNTS[RULING])

    # 3) Готово
    total = sum(COUNTS.values())
    print(f"== DONE. Inserted ~{total} objects across 8 classes ==")

if __name__ == "__main__":
    main()
