import re
import threading

from sentence_transformers import SentenceTransformer, util

from app.services.entity_extractor import extract_entities
from app.services.intent_classifier import classify_intent
from app.services.query_normalizer import normalize_query

MODEL_NAME = "BAAI/bge-small-en-v1.5"
SECTORS = {
    "programming": "Programming languages, software development, coding, databases and frameworks.",
    "ai": "Artificial intelligence, machine learning, deep learning, LLMs and RAG.",
    "technology": "Cloud computing, networking, hardware, infrastructure and DevOps.",
    "education": "Universities, colleges, courses, admissions and examinations.",
    "health": "Health, medicine, diseases, symptoms, diagnosis, treatment and healthcare.",
    "general": "General knowledge, daily life, culture, entertainment and other topics.",
}
RUST_SECTOR = {
    "programming": "technology",
    "ai": "technology",
    "technology": "technology",
    "education": "general",
    "health": "health",
    "general": "general",
}

STOP_WORDS = {
    "a", "an", "and", "are", "at", "based", "best", "between", "find", "for", "from",
    "give", "how", "i", "in", "information", "is", "it", "me", "of", "on", "or",
    "please", "related", "search", "show", "site", "sites", "the", "this", "to", "top",
    "website", "websites", "what", "with", "domain", "domains", "about", "page", "pages",
}
HEALTH_TERMS = {
    "health", "healthcare", "medical", "medicine", "doctor", "symptom", "symptoms", "disease",
    "diseases", "treatment", "diagnosis", "clinic", "hospital", "nutrition", "diabetes", "cancer",
    "heart", "heartattack", "mental", "patient", "therapy", "drug", "vaccine", "wellness",
}

_model = None
_sector_embeddings = None
_model_lock = threading.Lock()


def load_sector_classifier() -> None:
    """Load the embedding model and sector vectors once during API startup."""
    global _model, _sector_embeddings
    if _model is not None:
        return
    with _model_lock:
        if _model is None:
            model = SentenceTransformer(MODEL_NAME)
            embeddings = model.encode(
                list(SECTORS.values()),
                convert_to_tensor=True,
                normalize_embeddings=True,
            )
            _model = model
            _sector_embeddings = embeddings


def classify_sector(query: str) -> tuple[str, float]:
    load_sector_classifier()
    query_embedding = _model.encode(query, convert_to_tensor=True, normalize_embeddings=True)
    scores = util.cos_sim(query_embedding, _sector_embeddings)[0]
    best_index = int(scores.argmax().item())
    return list(SECTORS.keys())[best_index], float(scores[best_index].item())


def embed_texts(texts: list[str], batch_size: int = 64) -> list[list[float]]:
    """Create normalized BGE vectors for the offline indexer and lazy URL cache."""
    load_sector_classifier()
    if not texts:
        return []
    vectors = _model.encode(
        texts,
        batch_size=batch_size,
        convert_to_numpy=True,
        normalize_embeddings=True,
        show_progress_bar=False,
    )
    return vectors.tolist()


def understand_query(
    query: str,
    requested_sector: str | None = None,
    requested_keywords: list[str] | None = None,
) -> dict:
    explicit_keywords = requested_keywords or []
    normalized = normalize_query(query)
    entities = extract_entities(normalized + " " + " ".join(explicit_keywords))
    query_words = re.findall(r"[\w+#.]+", normalized)
    explicit_words = [word for value in explicit_keywords for word in re.findall(r"[\w+#.]+", value.lower())]
    keywords = list(dict.fromkeys(
        word for word in query_words + explicit_words
        if word not in STOP_WORDS and (len(word) > 1 or word in {"c", "c++", "c#"})
    ))[:20]

    load_sector_classifier()
    query_vector = _model.encode(normalized, convert_to_tensor=True, normalize_embeddings=True)
    scores = util.cos_sim(query_vector, _sector_embeddings)[0]
    best_index = int(scores.argmax().item())
    topic = list(SECTORS.keys())[best_index]
    similarity = float(scores[best_index].item())
    sector = requested_sector or RUST_SECTOR[topic]

    return {
        "original_query": query,
        "normalized_query": normalized,
        "intent": classify_intent(normalized),
        "topic": topic,
        "topic_similarity": round(similarity, 4),
        "query_embedding": query_vector.detach().cpu().tolist(),
        "sector": sector,
        "entities": entities,
        "attributes": [],
        "constraints": {"level": None, "date": None, "location": None},
        "keywords": keywords,
        "search_plan": {"strategy": "database_then_common_crawl_text", "source": sector},
        "limit": 5,
    }
