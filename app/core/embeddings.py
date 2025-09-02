# app/core/embeddings.py
from __future__ import annotations
import os
from typing import Iterable, List

import numpy as np
import requests


def _l2norm_1d(v: np.ndarray) -> np.ndarray:
    return v / (np.linalg.norm(v) + 1e-12)


class OllamaEmbedder:
    """
    Встраивание через локальный Ollama embeddings API.
    Совместимо с nomic-embed-text и др. эмбеддинг-моделями.
    Поддерживает E5-стиль префиксов (query:/passage:) по флагу EMBED_PREFIX_MODE.
    """
    def __init__(self):
        # URL вида http://host:11434/api/embeddings
        self.url = os.getenv("OLLAMA_URL", "http://localhost:11434/api/embeddings").rstrip("/")
        self.model = os.getenv("OLLAMA_EMB_MODEL", "nomic-embed-text")
        self.timeout = float(os.getenv("OLLAMA_TIMEOUT", "30"))
        mode = os.getenv("EMBED_PREFIX_MODE", "e5").strip().lower()
        self.use_e5_prefix = mode in ("e5", "true", "1", "yes")

    def _embed_one(self, text: str) -> List[float]:
        """Вызов Ollama для одной строки. Возвращает L2-нормированный вектор."""
        payload = {"model": self.model, "prompt": text}
        resp = requests.post(self.url, json=payload, timeout=self.timeout)
        resp.raise_for_status()
        data = resp.json()

        # Форматы ответа: {"embedding":[...]} или {"data":[{"embedding":[...]}]}
        if isinstance(data, dict) and "embedding" in data:
            vec = data["embedding"]
        elif isinstance(data, dict) and "data" in data and data["data"]:
            vec = data["data"][0].get("embedding")
        else:
            raise RuntimeError(f"Unexpected Ollama embeddings response: {str(data)[:200]}")

        if not isinstance(vec, list):
            raise RuntimeError("Ollama returned embedding in unexpected type")

        arr = np.asarray(vec, dtype=np.float32)
        arr = _l2norm_1d(arr)  # важно для cosine
        return [float(x) for x in arr.tolist()]

    def embed_query(self, text: str) -> List[float]:
        t = (text or "").strip()
        if not t:
            raise ValueError("Empty query text")
        if self.use_e5_prefix:
            t = f"query: {t}"
        return self._embed_one(t)

    def embed_passages(self, texts: Iterable[str]) -> List[List[float]]:
        out: List[List[float]] = []
        for t in texts:
            s = (t or "").strip()
            if not s:
                raise ValueError("Empty passage text")
            if self.use_e5_prefix:
                s = f"passage: {s}"
            out.append(self._embed_one(s))
        return out


# Singleton
_EMBEDDER: OllamaEmbedder | None = None

def get_embedder() -> OllamaEmbedder:
    global _EMBEDDER
    if _EMBEDDER is None:
        _EMBEDDER = OllamaEmbedder()
    return _EMBEDDER
