"""
Tests for app/telegram.py
Covers: min interval logic, missing credentials, message building
"""
import os
import sys
import time
import tempfile
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

# We test the pure logic functions without making real HTTP calls


def _make_results(lib=100, score=75.0, franchises=5, directors=10,
                  classics=3, suggestions=50, no_guid=2, no_match=1):
    """Build a minimal results dict matching scanner output."""
    return {
        "plex":   {"indexed_tmdb": lib},
        "scores": {"global_cinema_score": score},
        "franchises":    [{"missing": ["x"] * franchises}],
        "directors":     [{"missing": ["x"] * directors}],
        "classics":      ["x"] * classics,
        "suggestions":   ["x"] * suggestions,
        "no_tmdb_guid":  ["x"] * no_guid,
        "tmdb_not_found":["x"] * no_match,
    }


# ─────────────────────────────────────────────
# Min interval logic (pure, no HTTP)
# ─────────────────────────────────────────────

class TestMinInterval:

    def test_never_sent_returns_zero(self):
        """_last_sent should return 0.0 when stamp file does not exist."""
        import app.telegram as tg
        # Point stamp file to a non-existent path
        original = tg.STAMP_FILE
        tg.STAMP_FILE = "/nonexistent/path/last_telegram.txt"
        try:
            assert tg._last_sent() == 0.0
        finally:
            tg.STAMP_FILE = original

    def test_save_and_read_stamp(self):
        """_save_sent + _last_sent round-trip."""
        import app.telegram as tg
        with tempfile.NamedTemporaryFile(delete=False, suffix=".txt") as f:
            path = f.name
        original = tg.STAMP_FILE
        tg.STAMP_FILE = path
        try:
            before = time.time()
            tg._save_sent()   # writes to path (already set above)
            after  = time.time()
            stamp  = tg._last_sent()
            assert before <= stamp <= after, f"stamp {stamp} not between {before} and {after}"
        finally:
            tg.STAMP_FILE = original
            try:
                os.unlink(path)
            except Exception:
                pass

    def test_interval_respected(self, monkeypatch):
        """send_scan_summary should skip if called within min interval."""
        import app.telegram as tg

        sent = []

        def fake_post(*args, **kwargs):
            sent.append(True)
            class R:
                status_code = 200
            return R()

        monkeypatch.setattr("requests.post", fake_post)

        cfg = {
            "TELEGRAM": {
                "TELEGRAM_ENABLED":      True,
                "TELEGRAM_BOT_TOKEN":    "fake_token",
                "TELEGRAM_CHAT_ID":      "12345",
                "TELEGRAM_MIN_INTERVAL": 60,   # 60 min
            }
        }
        monkeypatch.setattr("app.telegram.load_config", lambda: cfg)

        with tempfile.NamedTemporaryFile(delete=False, suffix=".txt") as f:
            path = f.name
        original = tg.STAMP_FILE
        tg.STAMP_FILE = path

        try:
            # Write a stamp from 1 second ago — within 60 min interval
            with open(path, "w") as f:
                f.write(str(time.time() - 1))

            tg.send_scan_summary(_make_results())
            assert len(sent) == 0   # should be skipped
        finally:
            tg.STAMP_FILE = original
            os.unlink(path)

    def test_sends_when_interval_elapsed(self, monkeypatch):
        """send_scan_summary should send if enough time has passed."""
        import app.telegram as tg

        sent = []

        def fake_post(*args, **kwargs):
            sent.append(True)
            class R:
                status_code = 200
            return R()

        monkeypatch.setattr("requests.post", fake_post)

        cfg = {
            "TELEGRAM": {
                "TELEGRAM_ENABLED":      True,
                "TELEGRAM_BOT_TOKEN":    "fake_token",
                "TELEGRAM_CHAT_ID":      "12345",
                "TELEGRAM_MIN_INTERVAL": 1,   # 1 min
            }
        }
        monkeypatch.setattr("app.telegram.load_config", lambda: cfg)

        with tempfile.NamedTemporaryFile(delete=False, suffix=".txt") as f:
            path = f.name
        original = tg.STAMP_FILE
        tg.STAMP_FILE = path

        try:
            # Write a stamp from 2 minutes ago — beyond 1 min interval
            with open(path, "w") as f:
                f.write(str(time.time() - 120))

            tg.send_scan_summary(_make_results())
            assert len(sent) == 1
        finally:
            tg.STAMP_FILE = original
            os.unlink(path)


# ─────────────────────────────────────────────
# Missing credentials
# ─────────────────────────────────────────────

class TestMissingCredentials:

    def test_disabled_does_not_send(self, monkeypatch):
        import app.telegram as tg
        sent = []
        monkeypatch.setattr("requests.post", lambda *a, **k: sent.append(True))
        cfg = {"TELEGRAM": {"TELEGRAM_ENABLED": False,
                            "TELEGRAM_BOT_TOKEN": "tok",
                            "TELEGRAM_CHAT_ID": "123",
                            "TELEGRAM_MIN_INTERVAL": 0}}
        monkeypatch.setattr("app.telegram.load_config", lambda: cfg)
        tg.send_scan_summary(_make_results())
        assert len(sent) == 0

    def test_missing_token_does_not_send(self, monkeypatch):
        import app.telegram as tg
        sent = []
        monkeypatch.setattr("requests.post", lambda *a, **k: sent.append(True))
        cfg = {"TELEGRAM": {"TELEGRAM_ENABLED": True,
                            "TELEGRAM_BOT_TOKEN": "",
                            "TELEGRAM_CHAT_ID": "123",
                            "TELEGRAM_MIN_INTERVAL": 0}}
        monkeypatch.setattr("app.telegram.load_config", lambda: cfg)
        tg.send_scan_summary(_make_results())
        assert len(sent) == 0

    def test_missing_chat_id_does_not_send(self, monkeypatch):
        import app.telegram as tg
        sent = []
        monkeypatch.setattr("requests.post", lambda *a, **k: sent.append(True))
        cfg = {"TELEGRAM": {"TELEGRAM_ENABLED": True,
                            "TELEGRAM_BOT_TOKEN": "tok",
                            "TELEGRAM_CHAT_ID": "",
                            "TELEGRAM_MIN_INTERVAL": 0}}
        monkeypatch.setattr("app.telegram.load_config", lambda: cfg)
        tg.send_scan_summary(_make_results())
        assert len(sent) == 0