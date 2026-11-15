import re
from urllib.parse import urlsplit

DOMAIN_PATTERN = re.compile(r"(?i)\b(?:https?://)?(?:www\.)?(?:[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?\.)+[a-z]{2,63}\b")


def extract_domain(text: str) -> str | None:
    match = DOMAIN_PATTERN.search(text)
    return normalize_domain(match.group(0)) if match else None


def normalize_domain(value: str) -> str:
    candidate = value.strip()
    if not candidate:
        raise ValueError("domain cannot be blank")
    parsed = urlsplit(candidate if "://" in candidate else "https://" + candidate)
    if parsed.username or parsed.password or not parsed.hostname:
        raise ValueError("provide a hostname or URL")
    return parsed.hostname.rstrip(".").lower()
