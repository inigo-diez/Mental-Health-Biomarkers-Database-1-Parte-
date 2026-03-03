"""
Robust matrix / biofluid detection from free-text fields.

Returns a MatrixResult with:
  - matrix_type : str  (fecal | urine | plasma | serum | csf | saliva | breath | tissue | other | unknown)
  - fecal_hint  : bool
  - all_matrices: list[str]  (all detected types, in case of mixed samples)

Detects from any combination of text fields (title, description, sample_type,
organism_part, protocol_description, filenames, etc.)
"""

from __future__ import annotations
import re
from dataclasses import dataclass, field

# ── Matrix keyword dictionaries ──────────────────────────────────────────────

MATRIX_PATTERNS: dict[str, list[str]] = {
    "fecal":   [
        "fec", "stool", "faeces", "feces", "fecal", "feces",
        "gut content", "intestinal content", "colon content",
        "rectal", "copro", "cecal", "caecal",
        "bowel", "defec",
    ],
    "urine":   ["urin", "urinary", "pee", "voided"],
    "plasma":  ["plasma"],
    "serum":   ["serum"],
    "csf":     ["csf", "cerebrospinal", "spinal fluid"],
    "saliva":  ["saliv", "oral fluid", "spit"],
    "breath":  ["breath", "exhaled", "expired air", "bvoc"],
    "blood":   ["whole blood", "blood cell", "erythrocyte", "platelet"],
    "tissue":  [
        "tissue", "biopsy", "liver", "brain", "muscle",
        "intestin", "mucos", "colon", "ileum", "jejun",
    ],
    "bile":    ["bile", "biliary"],
    "sweat":   ["sweat", "perspir"],
}

# Pre-compile once
_COMPILED: dict[str, re.Pattern] = {
    mtype: re.compile(
        r"(?<!\w)(" + "|".join(re.escape(p) for p in patterns) + r")(?!\w)",
        re.IGNORECASE,
    )
    for mtype, patterns in MATRIX_PATTERNS.items()
}

# GC/LC method hints
_GC_PATTERNS = re.compile(
    r"\b(GC[-\s]?MS|GC[-\s]?FID|GC[-\s]?x[-\s]?GC|GCMS|HS[-\s]?SPME|"
    r"headspace|thermal desorption|SPME|pyrolysis|VOC|volatile)\b",
    re.IGNORECASE,
)
_LC_PATTERNS = re.compile(
    r"\b(LC[-\s]?MS|UHPLC|HPLC|HILIC|UPLC|LC[-\s]?MS/MS|NMR|LCMS|"
    r"capillary electrophoresis|CE[-\s]?MS)\b",
    re.IGNORECASE,
)


@dataclass
class MatrixResult:
    matrix_type:  str = "unknown"
    fecal_hint:   bool = False
    all_matrices: list[str] = field(default_factory=list)
    method_hint:  str | None = None   # "GC" | "LC" | "NMR" | None


def detect_matrix(
    *text_fields: str | None,
    existing_matrix_hint: str | None = None,
) -> MatrixResult:
    """
    Detect matrix type from any number of text fields.
    Existing hint is merged (non-destructive).
    """
    combined = " ".join(f for f in text_fields if f).lower()

    detected: list[str] = []
    for mtype, pat in _COMPILED.items():
        if pat.search(combined):
            detected.append(mtype)

    # Also parse existing_matrix_hint if it exists
    if existing_matrix_hint:
        for mtype, pat in _COMPILED.items():
            if pat.search(existing_matrix_hint.lower()) and mtype not in detected:
                detected.append(mtype)

    fecal = "fecal" in detected

    # Primary type: fecal > urine > plasma > serum > csf > saliva > blood > tissue > …
    priority = ["fecal", "urine", "plasma", "serum", "csf", "saliva",
                "blood", "breath", "bile", "sweat", "tissue"]
    matrix_type = "unknown"
    for p in priority:
        if p in detected:
            matrix_type = p
            break
    if not detected:
        matrix_type = "unknown"

    # Method hint
    method_hint = None
    if _GC_PATTERNS.search(combined):
        method_hint = "GC"
    elif _LC_PATTERNS.search(combined):
        method_hint = "LC"
    # Check existing
    if method_hint is None and existing_matrix_hint:
        if _GC_PATTERNS.search(existing_matrix_hint):
            method_hint = "GC"
        elif _LC_PATTERNS.search(existing_matrix_hint):
            method_hint = "LC"

    return MatrixResult(
        matrix_type=matrix_type,
        fecal_hint=fecal,
        all_matrices=detected,
        method_hint=method_hint,
    )


def matrix_result_to_hint_str(result: MatrixResult) -> str | None:
    """Convert MatrixResult to a compact string for the sources.matrix_hint column."""
    if not result.all_matrices:
        return None
    return "; ".join(result.all_matrices)
