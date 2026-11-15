from typing import Optional
from pydantic import BaseModel, Field, field_validator


class QueryRequest(BaseModel):
    query: str = Field(min_length=1, max_length=2000)
    sector: Optional[str] = None
    keywords: list[str] = Field(default_factory=list, max_length=20)

    @field_validator("query")
    @classmethod
    def query_must_not_be_blank(cls, value: str) -> str:
        value = value.strip()
        if not value:
            raise ValueError("query must not be blank")
        return value

    @field_validator("keywords")
    @classmethod
    def clean_keywords(cls, values: list[str]) -> list[str]:
        return list(dict.fromkeys(value.strip()[:100] for value in values if value.strip()))[:20]

    @field_validator("sector")
    @classmethod
    def sector_must_be_supported(cls, value: Optional[str]) -> Optional[str]:
        if value is None or not value.strip():
            return None
        value = value.strip().lower()
        if value not in {"technology", "health", "general"}:
            raise ValueError("sector must be technology, health, or general")
        return value
