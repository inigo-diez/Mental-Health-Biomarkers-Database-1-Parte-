"""
Centralized whitelist for mental-health condition detection.

All collectors that need to tag metabolites with specific mental-health
conditions should import from here.  The key design principle is:

    NO generic fallback — if detect_conditions() returns [], the
    study/paper is NOT included in the mh_biomarkers pipeline.
"""

from __future__ import annotations

# ── Whitelist (user-defined) ───────────────────────────────────────────────────

MH_WHITELIST: list[str] = [
    "schizophrenia", "psychosis", "first episode psychosis", "schizoaffective",
    "major depressive disorder", "depression", "mdd",
    "bipolar", "bipolar disorder",
    "anxiety disorder", "gad", "panic disorder",
    "ptsd", "post-traumatic stress",
    "autism spectrum disorder", "asd",
    "adhd", "attention deficit",
]

# Canonical conditions
CONDITIONS: list[str] = [
    "schizophrenia", "depression", "bipolar",
    "anxiety", "ptsd", "autism", "adhd",
]

# ── Condition map (whitelist term → canonical condition) ───────────────────────

_CONDITION_MAP: dict[str, str] = {
    # Schizophrenia spectrum
    "schizophrenia":           "schizophrenia",
    "psychosis":               "schizophrenia",
    "psychotic":               "schizophrenia",
    "first episode psychosis": "schizophrenia",
    "first episode":           "schizophrenia",
    "schizoaffective":         "schizophrenia",
    "antipsychotic":           "schizophrenia",
    # Depression
    "major depressive disorder": "depression",
    "major depressive":          "depression",
    "depression":                "depression",
    "depressive":                "depression",
    "mdd":                       "depression",
    # Bipolar
    "bipolar":                   "bipolar",
    "bipolar disorder":          "bipolar",
    "manic":                     "bipolar",
    "mania":                     "bipolar",
    # Anxiety
    "anxiety disorder":          "anxiety",
    "anxiety":                   "anxiety",
    "anxious":                   "anxiety",
    "gad":                       "anxiety",
    "panic disorder":            "anxiety",
    "panic":                     "anxiety",
    "social anxiety":            "anxiety",
    "phobia":                    "anxiety",
    # PTSD
    "ptsd":                      "ptsd",
    "post-traumatic stress":     "ptsd",
    "post-traumatic":            "ptsd",
    "posttraumatic":             "ptsd",
    # Autism
    "autism spectrum disorder":  "autism",
    "autism spectrum":           "autism",
    "autism":                    "autism",
    "autistic":                  "autism",
    "asd":                       "autism",
    # ADHD
    "adhd":                      "adhd",
    "attention deficit":         "adhd",
    "attention-deficit":         "adhd",
    "hyperactivity":             "adhd",
}

# ── Detection functions ────────────────────────────────────────────────────────

def detect_conditions(text: str) -> list[str]:
    """
    Strict whitelist match.
    Returns sorted list of canonical conditions found in *text*.
    Returns [] if no match — NO fallback to 'mental_health'.
    """
    t = text.lower()
    return sorted({v for k, v in _CONDITION_MAP.items() if k in t})


def is_mental_health(text: str) -> bool:
    """True only if at least one whitelist condition is present."""
    return bool(detect_conditions(text))


def tags_from_conditions(cond: list[str]) -> dict:
    """
    Build the standard tags dict for upsert_metabolite() from a list of
    canonical conditions.  Every per-condition key is a bool so the OR-merge
    logic in db.py works correctly (True is never overwritten by False).
    """
    return {
        "mental_health":       True,
        "schizophrenia":       "schizophrenia" in cond,
        "depression":          "depression"    in cond,
        "bipolar":             "bipolar"       in cond,
        "anxiety":             "anxiety"       in cond,
        "ptsd":                "ptsd"          in cond,
        "autism":              "autism"        in cond,
        "adhd":                "adhd"          in cond,
        "condition_hits":      cond,
        "mh_biomarker":        True,
    }
