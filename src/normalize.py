"""
String normalization and key generation for metabolite names.

Rules (applied in order):
 1. Unicode NFKC normalisation
 2. Lowercase
 3. Strip "Family – X" / "Class – X" prefixes (MarkerDB pattern)
 4. Remove parenthetical suffixes like "(FAME)" or "(TMS)"
 5. Collapse whitespace
 6. Remove common punctuation noise: hyphens between digits, trailing dots
 7. Strip leading/trailing whitespace
"""

from __future__ import annotations
import re
import unicodedata


# Patterns compiled once at import
# Only strip em-dash / en-dash family prefixes ("Fatty acids – X"), NOT plain hyphens
# (plain hyphens are used in chemical names: "2-Pentanone", "N-acetyl-...")
_RE_FAMILY    = re.compile(r'^[^–—]+\s*[–—]\s*', re.UNICODE)   # "Fatty acids – X"
_RE_PAREN_SFX = re.compile(r'\s*\([^)]+\)\s*$')                   # trailing "(FAME)"
_RE_DASH_DIG  = re.compile(r'(?<=\d)-(?=\d)')                     # 2-3 in numbers → 23 (WRONG: keep)
_RE_SPACES    = re.compile(r'\s+')
_RE_NOISE     = re.compile(r'[^\w\s,+\-.]')                       # keep word chars + basic punctuation


def normalize(name: str) -> str:
    """Return a normalized string suitable for display (canonical_name)."""
    if not name:
        return ""
    name = unicodedata.normalize("NFKC", name)
    # Remove family prefix (e.g. "Phospholipids – Lysophosphatidylcholine")
    name = _RE_FAMILY.sub("", name)
    # Remove trailing derivative suffix: (FAME), (TMS), (methyl ester)…
    name = _RE_PAREN_SFX.sub("", name)
    name = name.strip()
    return name


def make_key(name: str) -> str:
    """
    Generate a stable, lowercase deduplication key from a metabolite name.
    Two names that differ only in case / punctuation noise will share a key.
    """
    key = normalize(name)
    key = key.lower()
    # collapse all whitespace
    key = _RE_SPACES.sub(" ", key)
    # remove remaining noise characters (keep letters, digits, space, comma, +, -, .)
    key = re.sub(r'[^\w\s,+\-.]', '', key)
    # collapse again
    key = _RE_SPACES.sub(" ", key).strip()
    return key


def batch_normalize(names: list[str]) -> list[tuple[str, str]]:
    """Return list of (normalized_name, key) tuples."""
    return [(normalize(n), make_key(n)) for n in names]
