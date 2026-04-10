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

    def test_returns_none_when_config_missing(self):
        import app.scheduler as sch
        lib = {"url": "", "token": "", "library_name": ""}
        assert sch._get_plex_movie_count(lib) is None

    def test_returns_none_on_connection_error(self, monkeypatch):
        import app.scheduler as sch
        import requests
        def raise_error(*a, **k):
            raise requests.exceptions.ConnectionError("unreachable")
        monkeypatch.setattr("requests.get", raise_error)
        lib = {"url": "http://fake", "token": "tok", "library_name": "Movies"}
        assert sch._get_plex_movie_count(lib) is None


# ─────────────────────────────────────────────
# _get_emby_movie_count
# ─────────────────────────────────────────────

class TestGetEmbyMovieCount:

    def test_returns_none_when_config_missing(self):
        import app.scheduler as sch
        lib = {"url": "", "api_key": "", "library_name": ""}
        assert sch._get_emby_movie_count(lib) is None

    def test_returns_none_on_connection_error(self, monkeypatch):
        import app.scheduler as sch
        import requests
        def raise_error(*a, **k):
            raise requests.exceptions.ConnectionError("unreachable")
        monkeypatch.setattr("requests.get", raise_error)
        lib = {"url": "http://fake", "api_key": "tok", "library_name": "Movies"}
        assert sch._get_emby_movie_count(lib) is None

    def test_uses_emby_path_prefix(self, monkeypatch):
        """Verify /emby/ prefix appears in MediaFolders request."""
        import app.scheduler as sch

        class _Resp:
            status_code = 200
            def raise_for_status(self): pass
            def json(self): return {"Items": [{"Name": "Movies", "Id": "lib-1"}]}

        class _Resp2:
            status_code = 200
            def raise_for_status(self): pass
            def json(self): return {"TotalRecordCount": 42}

        captured = []
        call_n = {"n": 0}
        def mock_get(url, *a, **k):
            captured.append(url)
            call_n["n"] += 1
            return _Resp() if call_n["n"] == 1 else _Resp2()

        monkeypatch.setattr("requests.get", mock_get)
        lib = {"url": "http://emby:8096", "api_key": "key", "library_name": "Movies"}
        count = sch._get_emby_movie_count(lib)

        assert count == 42
        assert all("/emby/" in u for u in captured), \
            f"Expected /emby/ in all URLs, got: {captured}"

    def test_returns_none_when_library_not_found(self, monkeypatch):
        import app.scheduler as sch

        class _Resp:
            status_code = 200
            def raise_for_status(self): pass
            def json(self): return {"Items": [{"Name": "TV Shows", "Id": "lib-tv"}]}

        monkeypatch.setattr("requests.get", lambda *a, **k: _Resp())
        lib = {"url": "http://emby:8096", "api_key": "key", "library_name": "Movies"}
        assert sch._get_emby_movie_count(lib) is None