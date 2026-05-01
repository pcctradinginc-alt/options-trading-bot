"""
finbert_sentiment.py — finBERT Sentiment-Analyse

Ersetzt Keyword-Dictionaries durch ein auf Finanznachrichten
trainiertes BERT-Modell (ProsusAI/finbert).

Nur für Top-Cluster mit Confidence > 2.0 aufgerufen
um GitHub Actions Laufzeit zu schonen.

Modell-Output: positive / negative / neutral
Gibt Score [-1.0, +1.0] zurück — kompatibel mit calculate_sentiment().

Verwendung:
    from finbert_sentiment import get_finbert_sentiment, FINBERT_AVAILABLE
    score = get_finbert_sentiment("GOOGL surges 10% after record earnings beat")
"""

import logging

logger = logging.getLogger(__name__)

# Lazy Loading — Modell nur laden wenn tatsächlich gebraucht
_pipeline = None
FINBERT_AVAILABLE = False


def _load_model():
    """Lädt finBERT beim ersten Aufruf. Danach gecacht."""
    global _pipeline, FINBERT_AVAILABLE
    if _pipeline is not None:
        return _pipeline

    try:
        from transformers import pipeline
        logger.info("Lade finBERT (ProsusAI/finbert)...")
        _pipeline = pipeline(
            "text-classification",
            model="ProsusAI/finbert",
            tokenizer="ProsusAI/finbert",
            top_k=None,           # alle 3 Labels zurückgeben
            truncation=True,
            max_length=512,
        )
        FINBERT_AVAILABLE = True
        logger.info("finBERT geladen")
        return _pipeline
    except ImportError:
        logger.warning("transformers nicht installiert — finBERT nicht verfügbar")
        return None
    except Exception as e:
        logger.warning("finBERT konnte nicht geladen werden: %s", e)
        return None


def get_finbert_sentiment(text: str) -> float:
    """
    Berechnet Sentiment-Score via finBERT.

    Returns:
        float: Score von -1.0 (sehr negativ) bis +1.0 (sehr positiv)
               0.0 bei Fehler oder neutral
    """
    if not text or not text.strip():
        return 0.0

    pipe = _load_model()
    if pipe is None:
        return 0.0

    try:
        # Text auf 512 Tokens kürzen (finBERT-Limit)
        text_truncated = text[:1000]

        results = pipe(text_truncated)[0]  # Liste von {label, score}

        # Labels: positive, negative, neutral
        scores = {r["label"].lower(): r["score"] for r in results}

        pos     = scores.get("positive", 0.0)
        neg     = scores.get("negative", 0.0)
        neutral = scores.get("neutral",  0.0)

        # Netto-Score: positiv - negativ, gewichtet
        # Bei dominantem neutral → Score nahe 0
        if neutral > 0.6:
            net = (pos - neg) * 0.3  # stark gedämpft
        else:
            net = pos - neg

        return round(max(-1.0, min(1.0, net)), 3)

    except Exception as e:
        logger.debug("finBERT Inference Fehler: %s", e)
        return 0.0


def get_finbert_sentiment_batch(texts: list) -> list:
    """
    Berechnet Sentiment für eine Liste von Texten (effizienter als Einzelaufrufe).

    Returns:
        list[float]: Scores für jeden Text
    """
    if not texts:
        return []

    pipe = _load_model()
    if pipe is None:
        return [0.0] * len(texts)

    try:
        truncated = [t[:1000] for t in texts if t and t.strip()]
        if not truncated:
            return [0.0] * len(texts)

        all_results = pipe(truncated)
        scores      = []

        for results in all_results:
            label_scores = {r["label"].lower(): r["score"] for r in results}
            pos     = label_scores.get("positive", 0.0)
            neg     = label_scores.get("negative", 0.0)
            neutral = label_scores.get("neutral",  0.0)
            if neutral > 0.6:
                net = (pos - neg) * 0.3
            else:
                net = pos - neg
            scores.append(round(max(-1.0, min(1.0, net)), 3))

        return scores

    except Exception as e:
        logger.debug("finBERT Batch Fehler: %s", e)
        return [0.0] * len(texts)
