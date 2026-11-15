import re
import unicodedata


def normalize_query(query: str) -> str:
    """Trim, normalize Unicode, collapse whitespace, and lowercase a query."""
    normalized = unicodedata.normalize("NFKC", query)
    normalized = re.sub(r"\s+", " ", normalized).strip().lower()
    return normalized
