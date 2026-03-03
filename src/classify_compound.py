"""
Heuristic compound classification — anti-noise flags + volatility.

All outputs are tri-state strings: 'true' | 'false' | 'unknown'
so they can be stored in SQLite TEXT columns.

Rules are heuristic (name-based). They NEVER remove compounds; they only
add informational flags. Final research filtering is left to the user.
"""

from __future__ import annotations
import re
from dataclasses import dataclass

# ── Keyword lists ─────────────────────────────────────────────────────────────

_INORGANIC_TERMS = re.compile(
    r"\b(chloride|oxide|hydroxide|sulfate|sulphate|nitrate|nitrite|"
    r"phosphate|carbonate|bicarbonate|arsenite|arsenate|chromate|"
    r"cyanide|fluoride|bromide|iodide|sodium|potassium|calcium|"
    r"magnesium|manganese|zinc|copper|iron|lead|mercury|cadmium|"
    r"arsenic|chromium|nickel|cobalt|barium|strontium|aluminum|"
    r"aluminium|silicon|silicate|titanium|vanadium|selenium|telluride|"
    r"ammonia|ammonium|hydrogen peroxide|ozone|water|carbon dioxide|"
    r"carbon monoxide|sulfur dioxide|nitrogen oxide|dinitrogen)\b",
    re.IGNORECASE,
)

_CATEGORY_TERMS = re.compile(
    r"\b(pollutants?|contaminants?|pesticides?|herbicides?|"
    r"insecticides?|metals?|dioxins?|furans?|particles?|"
    r"particulate matter|pm2\.5|pm10|volatile organic|"
    r"air pollutants?|environmental chemicals?|mixtures?|"
    r"compounds? nec|not elsewhere classified|miscellaneous|"
    r"^[A-Z][a-z]+ acids?$|^[A-Z][a-z]+ esters?$)\b",
    re.IGNORECASE,
)

# Category-like names: ends with "s" and has a capital (plural category names)
# or contains conjunctions suggesting a category
_CATEGORY_PATTERNS = [
    re.compile(r"^(Fatty acids?|Amino acids?|Bile acids?|Nucleotides?|"
               r"Organic acids?|Lipids?|Steroids?|Vitamins?|Hormones?|"
               r"Carbohydrates?|Sugars?|Polyphenols?|Flavonoids?|"
               r"Purines?|Pyrimidines?|Porphyrins?|Quinones?|"
               r"Air Pollutants?|Aromatic hydrocarbons?)$", re.IGNORECASE),
    re.compile(r"\band\b.{1,30}\band\b", re.IGNORECASE),   # "A and B and C"
    re.compile(r"^(Total|All|Mixed|Various|Multiple|Several)\s", re.IGNORECASE),
]

_DRUG_TERMS = re.compile(
    r"\b(hydrochloride|mesylate|maleate|fumarate|tartrate|acetate salt|"
    r"sulfate salt|phosphate salt)\b",
    re.IGNORECASE,
)
# Common drug suffixes (rough approximation, not authoritative)
_DRUG_SUFFIX = re.compile(
    r"(mab|nib|vir|olol|pril|artan|statin|zole|cillin|mycin|"
    r"cycline|floxacin|oxacin|azole|prazole|dipine|tidine|setron|"
    r"lukast|tropin|umab|zumab|ximab|kinase inhibitor)$",
    re.IGNORECASE,
)

_ENVIRONMENTAL_TERMS = re.compile(
    r"\b(polychlorinated|polybrominated|dioxin|furan|phthalate|bisphenol|"
    r"perfluor|PFAS|PFOA|PFOS|flame retardant|pesticide|herbicide|"
    r"insecticide|fungicide|organochlorine|organophosphate|"
    r"heavy metal|PAH|polycyclic aromatic|benzopyrene|naphthalene|"
    r"toluene exposure|xylene|DDT|PCB|TCDD|dioxin)\b",
    re.IGNORECASE,
)

# VOC heuristics by name
_VOC_NAME = re.compile(
    r"\b(aldehyde|ketone|alcohol|ester|furan|terpene|indole|skatole|"
    r"dimethyl sulfide|methyl mercaptan|hydrogen sulfide|"
    r"isoprene|monoterpene|sesquiterpene|benzaldehyde|"
    r"acetone|pentanone|hexanal|butanal|nonanal|decanal|"
    r"propionic acid|butyric acid|valeric acid|caproic acid|"
    r"trimethylamine|dimethylamine|putrescine|cadaverine|"
    r"phenol|p-cresol|m-cresol|skatole|indole)\b",
    re.IGNORECASE,
)

# Non-volatile heuristics
_NONVOL_NAME = re.compile(
    r"\b(phosphatidyl|lysophosphatidyl|sphingomyelin|ceramide|"
    r"ganglioside|glycerophospho|bile acid|cholesterol ester|"
    r"triglyceride|diacylglycerol|phospholipid|"
    r"peptide|protein|oligonucleotide|nucleotide|DNA|RNA|"
    r"immunoglobulin|antibody|enzyme|polysaccharide|"
    r"glucuronide|sulfate conjugate|glycine conjugate|"
    r"taurine conjugate)\b",
    re.IGNORECASE,
)


@dataclass
class CompoundFlags:
    is_inorganic:    str = "unknown"   # 'true' | 'false' | 'unknown'
    is_drug:         str = "unknown"
    is_environmental: str = "unknown"
    is_category_like: str = "false"   # bool-like: 'true' | 'false'
    volatility:      str = "Unknown"  # 'VOC' | 'Non-volatile' | 'Unknown'
    gc_compatible:   str = "Unknown"
    lc_compatible:   str = "Unknown"


def classify(
    name: str,
    method_hint: str | None = None,
    source_types: str | None = None,
) -> CompoundFlags:
    """
    Classify a compound by name and optional source hints.
    Never raises — always returns a CompoundFlags with safe defaults.
    """
    flags = CompoundFlags()
    if not name:
        return flags

    # ── is_inorganic ─────────────────────────────────────────────────────────
    if _INORGANIC_TERMS.search(name):
        flags.is_inorganic = "true"
    elif re.search(r"[A-Z][a-z]?(\d|\+|-|\[|\()|(acid|base)", name):
        flags.is_inorganic = "unknown"
    else:
        flags.is_inorganic = "false"

    # ── is_category_like ─────────────────────────────────────────────────────
    if _CATEGORY_TERMS.search(name) or any(p.search(name) for p in _CATEGORY_PATTERNS):
        flags.is_category_like = "true"

    # ── is_drug ──────────────────────────────────────────────────────────────
    if _DRUG_TERMS.search(name) or _DRUG_SUFFIX.search(name.lower()):
        flags.is_drug = "true"

    # ── is_environmental ─────────────────────────────────────────────────────
    if _ENVIRONMENTAL_TERMS.search(name):
        flags.is_environmental = "true"
    elif source_types and "CTD" in source_types:
        # CTD includes many environmental/toxicological chemicals
        flags.is_environmental = "unknown"

    # ── volatility ───────────────────────────────────────────────────────────
    if method_hint and "GC" in method_hint.upper():
        flags.volatility    = "VOC"
        flags.gc_compatible = "Yes"
        if flags.lc_compatible == "Unknown":
            flags.lc_compatible = "Unknown"
    elif method_hint and any(k in method_hint.upper() for k in ("LC", "HPLC", "NMR")):
        flags.volatility    = "Non-volatile"
        flags.lc_compatible = "Yes"

    # Name-based volatility hints (override only if still Unknown)
    if flags.volatility == "Unknown":
        if _VOC_NAME.search(name):
            flags.volatility    = "VOC"
            flags.gc_compatible = "Yes"
        elif _NONVOL_NAME.search(name):
            flags.volatility    = "Non-volatile"
            flags.lc_compatible = "Yes"

    # GC/LC compatibility from volatility
    if flags.gc_compatible == "Unknown" and flags.volatility == "VOC":
        flags.gc_compatible = "Yes"
    if flags.lc_compatible == "Unknown" and flags.volatility == "Non-volatile":
        flags.lc_compatible = "Yes"

    return flags
