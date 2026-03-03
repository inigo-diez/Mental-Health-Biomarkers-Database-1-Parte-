"""
Optional PubChem PUG-REST resolver.

Given a compound name, attempts to fetch:
  - CID
  - InChIKey
  - canonical name
  - synonyms (top-10)

Results are disk-cached. All errors are caught and logged — never
blocks the main pipeline.
"""

from __future__ import annotations
import logging
import time
from dataclasses import dataclass, field
from typing import Optional

from src.cache import DiskCache
from src.utils import HTTPClient

logger = logging.getLogger(__name__)

_BASE = "https://pubchem.ncbi.nlm.nih.gov/rest/pug"


@dataclass
class PubChemResult:
    cid: Optional[int] = None
    inchikey: Optional[str] = None
    canonical_name: Optional[str] = None
    synonyms: list[str] = field(default_factory=list)


class PubChemResolver:
    def __init__(self, http: HTTPClient, enabled: bool = True):
        self.http = http
        self.enabled = enabled
        self._cache = http.cache   # reuse same disk cache

    def resolve(self, name: str) -> PubChemResult:
        if not self.enabled or not name.strip():
            return PubChemResult()

        cache_key = f"pubchem:{name.lower().strip()}"
        cached = self._cache.get(cache_key)
        if cached is not None:
            return PubChemResult(**cached)

        result = PubChemResult()
        try:
            # Step 1: name → CID
            url = f"{_BASE}/compound/name/{requests_quote(name)}/cids/JSON"
            data = self.http.get_json(url, use_cache=False)
            cids = data.get("IdentifierList", {}).get("CID", [])
            if not cids:
                self._cache.set(cache_key, {})
                return result
            result.cid = cids[0]

            # Step 2: CID → properties
            prop_url = (
                f"{_BASE}/compound/cid/{result.cid}/property"
                "/InChIKey,IUPACName/JSON"
            )
            props = self.http.get_json(prop_url, use_cache=False)
            prop = props.get("PropertyTable", {}).get("Properties", [{}])[0]
            result.inchikey = prop.get("InChIKey")
            result.canonical_name = prop.get("IUPACName") or name

            # Step 3: synonyms (first 10)
            syn_url = f"{_BASE}/compound/cid/{result.cid}/synonyms/JSON"
            syn_data = self.http.get_json(syn_url, use_cache=False)
            syns = (
                syn_data.get("InformationList", {})
                .get("Information", [{}])[0]
                .get("Synonym", [])
            )
            result.synonyms = syns[:10]

        except Exception as exc:
            logger.debug("PubChem lookup failed for '%s': %s", name, exc)

        self._cache.set(
            cache_key,
            {
                "cid": result.cid,
                "inchikey": result.inchikey,
                "canonical_name": result.canonical_name,
                "synonyms": result.synonyms,
            },
        )
        return result


def requests_quote(s: str) -> str:
    from urllib.parse import quote
    return quote(s, safe="")
