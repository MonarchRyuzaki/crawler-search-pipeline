BOILERPLATE = ["cookie", "subscribe", "404", "not found", "javascript"]


def filter_excerpt(excerpt: str | None) -> str | None:
    """
    Return cleaned excerpt or None if it should be discarded.

    Rules:
    - If excerpt is None or empty after stripping → return None.
    - If any boilerplate word appears (case-insensitive) → return None.
    """
    if excerpt is None:
        return None

    cleaned = excerpt.strip()
    if not cleaned:
        return None

    lowered = cleaned.lower()
    for word in BOILERPLATE:
        if word in lowered:
            return None

    return cleaned


def filter_description(description: str | None, max_chars: int = 300) -> str:
    """
    Normalize description text:
    - Replace all whitespace (tabs, newlines, multiple spaces) with a single space.
    - Strip leading/trailing spaces.
    - Truncate to max_chars characters.
    """
    if not description:
        return ""

    # Collapse all whitespace (spaces, tabs, newlines, etc.) to single spaces
    import re

    normalized = re.sub(r"\s+", " ", description)
    normalized = normalized.strip()

    if len(normalized) > max_chars:
        normalized = normalized[:max_chars]

    return normalized


def process_text(title: str | None, excerpt: str | None, description: str | None) -> str:
    """Return a single string made from title, filtered excerpt and filtered description.

    - Title is used as-is (or "" if None).
    - Excerpt is passed through `filter_excerpt`; if it is filtered out, it is omitted.
    - Description is normalized via `filter_description`.
    - Components are joined with a single space, skipping empty/None parts.
    """
    cleaned_excerpt = filter_excerpt(excerpt)
    cleaned_description = filter_description(description, max_chars=300)

    parts: list[str] = []

    if title:
        parts.append(title)

    if cleaned_excerpt:
        parts.append(cleaned_excerpt)

    if cleaned_description:
        parts.append(cleaned_description)

    return " ".join(parts)