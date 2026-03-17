"""
Tests for app/scheduler.py
Covers: _get_last_scan_count, _last_sent stamp helpers
"""
import json
import os
import sys
import tempfile
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))


# ─────────────────────────────────────────────
# _get_last_scan_count
# ─────────────────────────────────────────────

class TestGetLastScanCount:

    def test_returns_none_when_no_file(self, monkeypatch):
        import app.scheduler as sch
        monkeypatch.setattr(sch, "RESULTS_FILE", "/nonexistent/results.json")
        assert sch._get_last_scan_count() is None

    def test_returns_count_from_valid_file(self, monkeypatch):
        import app.scheduler as sch
        data = {"plex": {"indexed_tmdb": 1131}}
        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            json.dump(data, f)
            path = f.name
        monkeypatch.setattr(sch, "RESULTS_FILE", path)
        try:
            assert sch._get_last_scan_count() == 1131
        finally:
            os.unlink(path)

    def test_returns_none_on_corrupt_file(self, monkeypatch):
        import app.scheduler as sch
        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            f.write("{ bad json }")
            path = f.name
        monkeypatch.setattr(sch, "RESULTS_FILE", path)
        try:
            assert sch._get_last_scan_count() is None
        finally:
            os.unlink(path)

    def test_returns_none_when_plex_key_missing(self, monkeypatch):
        import app.scheduler as sch
        data = {"scores": {"global_cinema_score": 80}}
        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            json.dump(data, f)
            path = f.name
        monkeypatch.setattr(sch, "RESULTS_FILE", path)
        try:
            assert sch._get_last_scan_count() is None
        finally:
            os.unlink(path)

    def test_returns_zero_count(self, monkeypatch):
        import app.scheduler as sch
        data = {"plex": {"indexed_tmdb": 0}}
        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            json.dump(data, f)
            path = f.name
        monkeypatch.setattr(sch, "RESULTS_FILE", path)
        try:
            assert sch._get_last_scan_count() == 0
        finally:
            os.unlink(path)


# ─────────────────────────────────────────────
# _get_plex_movie_count (error handling only —
# no real Plex calls in tests)
# ─────────────────────────────────────────────

class TestGetPlexMovieCount:

    def test_returns_none_when_config_missing(self, monkeypatch):
        import app.scheduler as sch
        monkeypatch.setattr(
            "app.scheduler.load_config",
            lambda: {"PLEX": {"PLEX_URL": "", "PLEX_TOKEN": "", "LIBRARY_NAME": ""}}
        )
        assert sch._get_plex_movie_count() is None

    def test_returns_none_on_connection_error(self, monkeypatch):
        import app.scheduler as sch
        import requests
        monkeypatch.setattr(
            "app.scheduler.load_config",
            lambda: {"PLEX": {"PLEX_URL": "http://fake", "PLEX_TOKEN": "tok", "LIBRARY_NAME": "Movies"}}
        )
        def raise_error(*a, **k):
            raise requests.exceptions.ConnectionError("unreachable")
        monkeypatch.setattr("requests.get", raise_error)
        assert sch._get_plex_movie_count() is None