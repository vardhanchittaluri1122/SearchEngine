
def classify_intent(query: str) -> str:
    text = query.lower()
    if any(word in text for word in ("compare", " vs ", "versus", "difference between")):
        return "comparison"
    if any(word in text for word in ("how to", "how do", "steps to", "tutorial", "guide")):
        return "how_to"
    if any(word in text for word in ("what is", "who is", "define", "meaning of")):
        return "definition"
    return "search"
