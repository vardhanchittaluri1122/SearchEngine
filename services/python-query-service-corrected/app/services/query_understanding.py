import re
import threading
import json
from pathlib import Path

from app.services.entity_extractor import extract_entities
from app.services.intent_classifier import classify_intent
from app.services.query_normalizer import normalize_query

MODEL_NAME = "BAAI/bge-small-en-v1.5"
SECTOR_FILE = Path(__file__).resolve().parent.parent / "data" / "sectors.json"
with SECTOR_FILE.open("r", encoding="utf-8") as sector_file:
    SECTOR_CATALOG = json.load(sector_file)["sectors"]
SECTORS = {sector["id"]: sector["description"] for sector in SECTOR_CATALOG}
SECTOR_BY_ID = {sector["id"]: sector for sector in SECTOR_CATALOG}
ROUTE_FAMILIES = {
    "technology": {"programming_software", "ai_data", "engineering", "computing_infra"},
    "health": {"health_medicine", "biology"},
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
_sentence_transformer_util = None
_model_lock = threading.Lock()


def load_sector_classifier() -> None:
    """Load the embedding model and sector vectors once during API startup."""
    global _model, _sector_embeddings, _sentence_transformer_util
    if _model is not None:
        return
    with _model_lock:
        if _model is None:
            import torch
            from sentence_transformers import SentenceTransformer, util

            torch.set_num_threads(1)
            torch.set_num_interop_threads(1)
            model = SentenceTransformer(MODEL_NAME, device="cpu")
            model.max_seq_length = 128
            embeddings = model.encode(
                list(SECTORS.values()),
                convert_to_tensor=True,
                normalize_embeddings=True,
            )
            _model = model
            _sector_embeddings = embeddings
            _sentence_transformer_util = util


def classify_sector(query: str) -> tuple[str, float]:
    load_sector_classifier()
    query_embedding = _model.encode(query, convert_to_tensor=True, normalize_embeddings=True)
    scores = _sentence_transformer_util.cos_sim(query_embedding, _sector_embeddings)[0]
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
    scores = _sentence_transformer_util.cos_sim(query_vector, _sector_embeddings)[0]
    eligible_ids = list(SECTORS.keys())
    if requested_sector in ROUTE_FAMILIES:
        eligible_ids = [
            sector["id"] for sector in SECTOR_CATALOG
            if sector["family_id"] in ROUTE_FAMILIES[requested_sector]
        ]
    elif requested_sector not in (None, "", "general"):
        eligible_ids = [requested_sector]
    eligible_indices = [list(SECTORS.keys()).index(sector_id) for sector_id in eligible_ids]
    eligible_scores = scores[eligible_indices]
    best_index = int(eligible_scores.argmax().item())
    sector_id = eligible_ids[best_index]
    similarity = float(eligible_scores[best_index].item())
    selected_sector = SECTOR_BY_ID[sector_id]
    storage_group = "health" if selected_sector["family_id"] in {"health_medicine", "biology"} else "technology"
    top_order = eligible_scores.argsort(descending=True)[: min(3, len(eligible_ids))].tolist()
    top_matches = []
    for rank_index in top_order:
        match_id = eligible_ids[rank_index]
        match = SECTOR_BY_ID[match_id]
        match_group = "health" if match["family_id"] in {"health_medicine", "biology"} else "technology"
        top_matches.append({
            "sector_id": match_id,
            "name": match["name"],
            "category": match["family"],
            "family_id": match["family_id"],
            "database_name": match["database_name"],
            "sector_group": match_group,
            "similarity": round(float(eligible_scores[rank_index].item()), 4),
        })

    return {
        "original_query": query,
        "normalized_query": normalized,
        "intent": classify_intent(normalized),
        "topic": selected_sector["name"],
        "topic_similarity": round(similarity, 4),
        "query_embedding": query_vector.detach().cpu().tolist(),
        "sector": sector_id,
        "sector_group": storage_group,
        "sector_id": sector_id,
        "sector_family": selected_sector["family_id"],
        "category": selected_sector["family"],
        "top_matches": top_matches,
        "database_name": selected_sector["database_name"],
        "entities": entities,
        "attributes": [],
        "constraints": {"level": None, "date": None, "location": None},
        "keywords": keywords,
        "search_plan": {
            "strategy": "sector_database_then_common_crawl",
            "source": storage_group,
            "sector_id": sector_id,
            "database": selected_sector["database_name"],
        },
        "limit": 5,
    }
