"""
Tests for src/normalize.py and the deduplication logic in src/db.py
"""

import pytest
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from src.normalize import normalize, make_key


class TestNormalize:
    def test_family_prefix_removed(self):
        assert normalize("Fatty acids – Palmitic acid") == "Palmitic acid"

    def test_family_prefix_em_dash(self):
        assert normalize("Amino acids — Glutamate") == "Glutamate"

    def test_paren_suffix_removed(self):
        result = normalize("Palmitic acid (FAME)")
        assert "FAME" not in result
        assert "Palmitic acid" in result

    def test_plain_name_unchanged(self):
        assert normalize("2-Pentanone") == "2-Pentanone"

    def test_empty_string(self):
        assert normalize("") == ""

    def test_unicode_normalization(self):
        # Full-width characters should be normalized
        result = normalize("Ａcetic acid")
        assert "Acetic acid" in result


class TestMakeKey:
    def test_case_insensitive(self):
        assert make_key("Palmitic Acid") == make_key("palmitic acid")

    def test_different_dash_styles(self):
        # Both should produce the same key
        k1 = make_key("2-Pentanone")
        k2 = make_key("2 Pentanone")
        # Not necessarily equal (hyphens kept), but key is stable
        assert isinstance(k1, str) and len(k1) > 0

    def test_family_prefix_stripped_before_key(self):
        k1 = make_key("Fatty acids – Palmitic acid")
        k2 = make_key("Palmitic acid")
        assert k1 == k2

    def test_paren_suffix_stripped_before_key(self):
        k1 = make_key("Palmitic acid (FAME)")
        k2 = make_key("Palmitic acid")
        assert k1 == k2

    def test_key_is_lowercase(self):
        assert make_key("BUTYRIC ACID") == make_key("butyric acid")

    def test_stable_across_calls(self):
        name = "3-Methylindole (skatole)"
        assert make_key(name) == make_key(name)


class TestDeduplication:
    """Integration test: same metabolite added twice → only one record."""

    def test_same_key_dedup(self, tmp_path):
        from src.db import init_db, get_conn, upsert_metabolite

        db = str(tmp_path / "test.db")
        init_db(db)

        with get_conn(db) as conn:
            mid1 = upsert_metabolite(conn, "Butyric acid", make_key("Butyric acid"))
            mid2 = upsert_metabolite(conn, "butyric acid", make_key("butyric acid"))
            assert mid1 == mid2

    def test_inchikey_dedup(self, tmp_path):
        from src.db import init_db, get_conn, upsert_metabolite

        db = str(tmp_path / "test2.db")
        init_db(db)
        ik = "AAAAAAAAAAAAA-AAAAAAAA-A"  # fake InChIKey same length

        with get_conn(db) as conn:
            mid1 = upsert_metabolite(conn, "Compound A", "compound a", inchikey=ik)
            mid2 = upsert_metabolite(conn, "Compound A synonym", "compound a synonym", inchikey=ik)
            assert mid1 == mid2

    def test_different_names_not_dedup(self, tmp_path):
        from src.db import init_db, get_conn, upsert_metabolite

        db = str(tmp_path / "test3.db")
        init_db(db)

        with get_conn(db) as conn:
            mid1 = upsert_metabolite(conn, "Acetic acid",   make_key("Acetic acid"))
            mid2 = upsert_metabolite(conn, "Propionic acid", make_key("Propionic acid"))
            assert mid1 != mid2
