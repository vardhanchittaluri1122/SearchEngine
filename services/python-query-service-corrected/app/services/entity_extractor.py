import re

TECHNOLOGIES = (
    "javascript", "spring boot", "postgresql", "typescript", "kubernetes",
    "python", "golang", "docker", "java", "rust", "react", "fastapi",
    "mysql", "redis", "node.js", " C# ", " C++ ",
)


def extract_entities(query: str) -> list[dict[str, str]]:
    padded = f" {query.lower()} "
    found = []
    for technology in TECHNOLOGIES:
        name = technology.strip()
        if technology.startswith(" "):
            match = technology.lower() in padded
        else:
            match = re.search(rf"(?<![\w]){re.escape(name)}(?![\w])", query, re.IGNORECASE) is not None
        if match:
            found.append({"name": name, "type": "technology"})
    return found
