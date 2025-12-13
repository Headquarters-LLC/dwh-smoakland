from __future__ import annotations

import json
import logging
import re
from functools import lru_cache
from pathlib import Path
from typing import Dict, Set

ACRONYMS_PATH = Path(__file__).resolve().parent / "acronyms.json"

# Common lower-case “small words” we usually don't capitalize mid-string.
_SMALL_WORDS: Set[str] = {
    "and",
    "or",
    "of",
    "the",
    "a",
    "an",
    "to",
    "in",
    "on",
    "for",
    "by",
    "with",
}


@lru_cache(maxsize=1)
def _acronym_lookup() -> Dict[str, str]:
    """
    Load acronyms mapping lower -> canonical form.
    Falls back to empty mapping if the JSON is missing or malformed.
    """
    try:
        data = json.loads(ACRONYMS_PATH.read_text())
        items = data.get("acronyms", [])
        return {
            str(item).lower(): str(item)
            for item in items
            if isinstance(item, (str, int, float)) and str(item).strip()
        }
    except Exception as exc:  # noqa: BLE001
        logging.warning(
            "acronyms.json missing or invalid (%s); proceeding without acronyms",
            exc,
        )
        return {}


def _is_strong_separator(token: str) -> bool:
    """Separators that reset capitalization context."""
    return token in {":", "/", "-", "&", "(", ")", ";", "|"}


def _title_token(token: str) -> str:
    if not token:
        return token
    if len(token) == 1:
        return token.upper()
    return token[0].upper() + token[1:].lower()


def smart_title_case(text: str | None) -> str | None:
    """
    Title-case text while preserving known acronyms from acronyms.json.

    Rules:
    - Leaves blanks and 'UNKNOWN' untouched.
    - Preserves acronyms exactly as defined.
    - Keeps small words lower-case mid-string.
    - Resets capitalization after strong separators.
    """
    if text is None or not isinstance(text, str):
        return text

    stripped = text.strip()
    if stripped == "" or stripped.upper() == "UNKNOWN":
        return text

    acronyms = _acronym_lookup()

    tokens: list[str] = []
    # Split but keep separators
    parts = re.split(r"(\s+|[:&/,\-()])", text)

    prev_strong = True   # start-of-string behaves like a strong separator
    first_word = True

    for part in parts:
        if not part:
            continue

        if re.fullmatch(r"\s+", part):
            tokens.append(part)
            continue

        if re.fullmatch(r"[:&/,\-()]", part):
            tokens.append(part)
            prev_strong = _is_strong_separator(part)
            continue

        lower = part.lower()

        if lower in acronyms:
            tokens.append(acronyms[lower])
        elif lower in _SMALL_WORDS and not first_word and not prev_strong:
            tokens.append(lower)
        else:
            tokens.append(_title_token(part))

        first_word = False
        prev_strong = False

    return "".join(tokens)