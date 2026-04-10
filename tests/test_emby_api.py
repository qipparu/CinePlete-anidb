"""
Tests for app/emby_api.py
Covers: _library_id, scan_movies (mocked HTTP), /emby/ path prefix
"""
import os
import sys
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

BASE_CONFIG = {
    "EMBY": {
        "EMBY_URL":          "http://emby:8096",
        "EMBY_API_KEY":      "testtoken",
        "EMBY_LIBRARY_NAME": "Movies",
    },
    "PLEX": {
        "SHORT_MOVIE_LIMIT": 60,
        "PLEX_PAGE_SIZE":    500,
    },
}


# ─────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────

class _MockResponse:
    def __init__(self, data, status=200):
        self._data       = data
        self.status_code = status

    def raise_for_status(self):
        if self.status_code >= 400:
            import requests
            raise requests.exceptions.HTTPError(response=self)

    def json(self):
        return self._data


def _media_folders_response(names=("Movies",)):
    return _MockResponse({
        "Items": [{"Name": n, "Id": f"lib-{n.lower()}"} for n in names]
    })


def _items_response(items, total=None):
    return _MockResponse({
        "Items": items,
        "TotalRecordCount": total if total is not None else len(items),
    })


_LONG_MOVIE_TICKS  = 37_200_000_000   # 62 min
_SHORT_MOVIE_TICKS = 9_000_000_000    # 15 min


def _make_movie(tmdb_id, title="Test Film", duration_ticks=_LONG_MOVIE_TICKS,
                directors=("Director A",), actors=("Actor 1", "Actor 2")):
    people = [{"Name": n, "Type": "Director"} for n in directors]
    people += [{"Name": n, "Type": "Actor"}   for n in actors]
    return {
        "Name":           title,
        "ProductionYear": 2020,
        "RunTimeTicks":   duration_ticks,
        "ProviderIds":    {"Tmdb": str(tmdb_id)},
        "People":         people,
    }


# ─────────────────────────────────────────────
# Path prefix verification
# ─────────────────────────────────────────────

class TestEmbyPathPrefix:
    """Verify that every HTTP call includes the /emby/ prefix."""

    def test_library_id_uses_emby_prefix(self, monkeypatch):
        import app.emby_api as emby
        monkeypatch.setattr("app.emby_api.load_config", lambda: BASE_CONFIG)

        captured = {}
        def mock_get(url, *a, **k):
            captured["url"] = url
            return _media_folders_response(["Movies"])

        monkeypatch.setattr("requests.get", mock_get)
        emby._library_id("Movies")
        assert "/emby/" in captured["url"], f"Expected /emby/ in URL, got: {captured['url']}"

    def test_scan_movies_items_call_uses_emby_prefix(self, monkeypatch):
        import app.emby_api as emby
        monkeypatch.setattr("app.emby_api.load_config", lambda: BASE_CONFIG)

        captured_urls = []
        def mock_get(url, *a, **k):
            captured_urls.append(url)
            if "/Library/MediaFolders" in url:
                return _media_folders_response(["Movies"])
            return _items_response([_make_movie(1)], 1)

        monkeypatch.setattr("requests.get", mock_get)
        emby.scan_movies()
        assert all("/emby/" in u for u in captured_urls), \
            f"Not all URLs use /emby/ prefix: {captured_urls}"


# ─────────────────────────────────────────────
# _library_id
# ─────────────────────────────────────────────

class TestLibraryId:

    def test_finds_library_case_insensitive(self, monkeypatch):
        import app.emby_api as emby
        monkeypatch.setattr("app.emby_api.load_config", lambda: BASE_CONFIG)
        monkeypatch.setattr("requests.get", lambda *a, **k: _media_folders_response(["Movies"]))

        assert emby._library_id("movies") == "lib-movies"

    def test_raises_when_library_not_found(self, monkeypatch):
        import app.emby_api as emby
        monkeypatch.setattr("app.emby_api.load_config", lambda: BASE_CONFIG)
        monkeypatch.setattr("requests.get", lambda *a, **k: _media_folders_response(["TV Shows"]))

        with pytest.raises(RuntimeError, match="not found"):
            emby._library_id("Movies")

    def test_raises_when_library_list_empty(self, monkeypatch):
        import app.emby_api as emby
        monkeypatch.setattr("app.emby_api.load_config", lambda: BASE_CONFIG)
        monkeypatch.setattr("requests.get", lambda *a, **k: _MockResponse({"Items": []}))

        with pytest.raises(RuntimeError):
            emby._library_id("Movies")


# ─────────────────────────────────────────────
# scan_movies — stats & basic indexing
# ─────────────────────────────────────────────

class TestScanMovies:

    def _patch(self, monkeypatch, pages):
        """pages: list of (items, total) tuples returned in sequence."""
        import app.emby_api as emby
        monkeypatch.setattr("app.emby_api.load_config", lambda: BASE_CONFIG)

        call_count = {"n": 0}
        def mock_get(url, *a, **k):
            if "/Library/MediaFolders" in url:
                return _media_folders_response(["Movies"])
            idx = call_count["n"]
            call_count["n"] += 1
            if idx < len(pages):
                items, total = pages[idx]
                return _items_response(items, total)
            return _items_response([], 0)
        monkeypatch.setattr("requests.get", mock_get)

    def test_returns_correct_tuple_structure(self, monkeypatch):
        import app.emby_api as emby
        self._patch(monkeypatch, [([_make_movie(1)], 1)])
        media_ids, directors, actors, stats, no_tmdb = emby.scan_movies()

        assert isinstance(media_ids,  dict)
        assert isinstance(directors,  dict)
        assert isinstance(actors,     dict)
        assert isinstance(stats,      dict)
        assert isinstance(no_tmdb,    list)

    def test_indexes_movie_with_valid_tmdb(self, monkeypatch):
        import app.emby_api as emby
        self._patch(monkeypatch, [([_make_movie(42, title="My Movie")], 1)])
        media_ids, *_ = emby.scan_movies()

        assert 42 in media_ids
        assert media_ids[42] == "My Movie"

    def test_skips_short_movies(self, monkeypatch):
        import app.emby_api as emby
        self._patch(monkeypatch, [([_make_movie(99, duration_ticks=_SHORT_MOVIE_TICKS)], 1)])
        media_ids, _, _, stats, _ = emby.scan_movies()

        assert 99 not in media_ids
        assert stats["skipped_short"] == 1

    def test_missing_tmdb_goes_to_no_tmdb_guid(self, monkeypatch):
        import app.emby_api as emby
        movie = _make_movie(0)
        movie["ProviderIds"] = {}
        self._patch(monkeypatch, [([movie], 1)])
        media_ids, _, _, _, no_tmdb = emby.scan_movies()

        assert len(media_ids) == 0
        assert len(no_tmdb)   == 1

    def test_lowercase_tmdb_key_accepted(self, monkeypatch):
        """ProviderIds.tmdb (lowercase) should also be accepted as fallback."""
        import app.emby_api as emby
        movie = _make_movie(0)
        movie["ProviderIds"] = {"tmdb": "55"}   # lowercase key
        self._patch(monkeypatch, [([movie], 1)])
        media_ids, *_ = emby.scan_movies()

        assert 55 in media_ids

    def test_invalid_tmdb_string_goes_to_no_tmdb_guid(self, monkeypatch):
        import app.emby_api as emby
        movie = _make_movie(0)
        movie["ProviderIds"] = {"Tmdb": "not-a-number"}
        self._patch(monkeypatch, [([movie], 1)])
        _, _, _, _, no_tmdb = emby.scan_movies()

        assert len(no_tmdb) == 1

    def test_runtimeticks_as_string_does_not_crash(self, monkeypatch):
        import app.emby_api as emby
        movie = _make_movie(7)
        movie["RunTimeTicks"] = str(_LONG_MOVIE_TICKS)
        self._patch(monkeypatch, [([movie], 1)])
        media_ids, *_ = emby.scan_movies()

        assert 7 in media_ids

    def test_runtimeticks_none_not_skipped_as_short(self, monkeypatch):
        """None RunTimeTicks → duration 0 → falsy → NOT skipped."""
        import app.emby_api as emby
        movie = _make_movie(8)
        movie["RunTimeTicks"] = None
        self._patch(monkeypatch, [([movie], 1)])
        _, _, _, stats, _ = emby.scan_movies()

        assert stats["skipped_short"] == 0

    def test_directors_kept_only_in_2_plus_films(self, monkeypatch):
        import app.emby_api as emby
        m1 = _make_movie(1, directors=("Kubrick",),    actors=())
        m2 = _make_movie(2, directors=("Kubrick",),    actors=())
        m3 = _make_movie(3, directors=("Spielberg",),  actors=())
        self._patch(monkeypatch, [([m1, m2, m3], 3)])
        _, directors, _, _, _ = emby.scan_movies()

        assert "Kubrick"   in directors
        assert "Spielberg" not in directors

    def test_actors_kept_only_in_2_plus_films(self, monkeypatch):
        import app.emby_api as emby
        m1 = _make_movie(1, directors=(), actors=("De Niro",))
        m2 = _make_movie(2, directors=(), actors=("De Niro", "Pacino"))
        self._patch(monkeypatch, [([m1, m2], 2)])
        _, _, actors, _, _ = emby.scan_movies()

        assert "De Niro" in actors
        assert "Pacino"  not in actors

    def test_actors_limited_to_5_per_film(self, monkeypatch):
        import app.emby_api as emby
        many_actors = [f"Actor {i}" for i in range(10)]
        m1 = _make_movie(1, directors=(), actors=many_actors)
        m2 = _make_movie(2, directors=(), actors=many_actors)
        self._patch(monkeypatch, [([m1, m2], 2)])
        _, _, actors, _, _ = emby.scan_movies()

        assert len(actors) <= 5

    def test_stats_fields_present(self, monkeypatch):
        import app.emby_api as emby
        self._patch(monkeypatch, [([_make_movie(1)], 1)])
        _, _, _, stats, _ = emby.scan_movies()

        for key in ("scanned_items", "indexed_tmdb", "skipped_short",
                    "directors_kept", "actors_kept", "no_tmdb_guid"):
            assert key in stats, f"Missing stats key: {key}"

    def test_pagination_fetches_all_pages(self, monkeypatch):
        import app.emby_api as emby
        page1 = [_make_movie(i) for i in range(1, 4)]
        page2 = [_make_movie(i) for i in range(4, 6)]
        self._patch(monkeypatch, [(page1, 5), (page2, 5)])
        media_ids, *_ = emby.scan_movies()

        assert len(media_ids) == 5
        for i in range(1, 6):
            assert i in media_ids

    def test_empty_library_returns_empty_results(self, monkeypatch):
        import app.emby_api as emby
        self._patch(monkeypatch, [([], 0)])
        media_ids, directors, actors, stats, no_tmdb = emby.scan_movies()

        assert media_ids  == {}
        assert directors  == {}
        assert actors     == {}
        assert no_tmdb    == []
        assert stats["scanned_items"] == 0
