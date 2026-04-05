"""
app/anidb_mapping.py — AniDB ↔ TVDB ↔ TMDB mapping

Downloads anime-list-master.xml from Anime-Lists/anime-lists (same source as the
HAMA Plex plugin in plextvdb/AnimeLists.py) and builds bi-directional lookup dicts:

    forward:  anidb_id  → MappingEntry
    reverse:  tvdb_id   → [MappingEntry, ...]   # all seasons for that TVDB show

Usage:
    from app.anidb_mapping import get_mapper
    mapper = get_mapper()
    entry  = mapper.lookup(7729)    # → MappingEntry for Steins;Gate
    seasons = mapper.tvdb_seasons(236622)   # → all AniDB entries for TVDB 236622
"""

import os
import time
import threading
import requests
import defusedxml.ElementTree as ET
from dataclasses import dataclass, field
from typing import Optional

from app.logger import get_logger

log = get_logger(__name__)

# ──────────────────────────────────────────────────────────────────────────────
# Constants
# ──────────────────────────────────────────────────────────────────────────────

ANIMELISTS_URL = (
    "https://raw.githubusercontent.com/Anime-Lists/anime-lists/"
    "master/anime-list-master.xml"
)
DATA_DIR   = os.getenv("DATA_DIR", "/data")
CACHE_FILE = os.path.join(DATA_DIR, "animelists_cache.xml")

_DEFAULT_TTL_DAYS = 7


# ──────────────────────────────────────────────────────────────────────────────
# Data model
# ──────────────────────────────────────────────────────────────────────────────

@dataclass
class MappingEntry:
    anidb_id:       int
    tvdb_id:        Optional[int]   = None
    tmdb_id:        Optional[int]   = None
    imdb_id:        Optional[str]   = None
    default_season: str             = "1"   # "1", "2", "a" (absolute), etc.
    episode_offset: str             = "0"
    title:          str             = ""

    def as_dict(self) -> dict:
        return {
            "anidb_id":       self.anidb_id,
            "tvdb_id":        self.tvdb_id,
            "tmdb_id":        self.tmdb_id,
            "imdb_id":        self.imdb_id,
            "default_season": self.default_season,
            "episode_offset": self.episode_offset,
            "title":          self.title,
        }


# ──────────────────────────────────────────────────────────────────────────────
# Mapper class
# ──────────────────────────────────────────────────────────────────────────────

class AniDBMapper:
    """
    Thread-safe bi-directional AniDB ↔ TVDB ↔ TMDB mapper.

    Loaded once at startup; can be refreshed based on TTL.
    """

    def __init__(self, ttl_days: int = _DEFAULT_TTL_DAYS):
        self._ttl_seconds = ttl_days * 86_400
        self._lock        = threading.Lock()
        self._forward:  dict[int, MappingEntry]       = {}   # anidb_id → entry
        self._reverse:  dict[int, list[MappingEntry]] = {}   # tvdb_id  → [entry, ...]
        self._loaded_at: float = 0.0
        self._ready    : bool  = False

    # ── Internal helpers ──────────────────────────────────────────────────────

    def _download_xml(self) -> Optional[str]:
        """Fetch the master XML from GitHub. Returns raw XML text or None."""
        try:
            log.info(f"Downloading anime-list-master.xml from {ANIMELISTS_URL}")
            r = requests.get(ANIMELISTS_URL, timeout=60,
                             headers={"User-Agent": "CinePlete/1.0 AniDB-mapper"})
            r.raise_for_status()
            return r.text
        except Exception as e:
            log.warning(f"AniDB mapping: download failed — {e}")
            return None

    def _load_from_cache(self) -> Optional[str]:
        """Load XML text from the on-disk cache if it exists."""
        try:
            with open(CACHE_FILE, "r", encoding="utf-8") as f:
                return f.read()
        except OSError:
            return None

    def _save_cache(self, xml_text: str) -> None:
        os.makedirs(DATA_DIR, exist_ok=True)
        tmp = CACHE_FILE + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            f.write(xml_text)
        os.replace(tmp, CACHE_FILE)
        log.debug("AniDB mapping: cache saved")

    def _cache_age_seconds(self) -> float:
        """Return age of on-disk cache in seconds, or infinity if absent."""
        try:
            return time.time() - os.path.getmtime(CACHE_FILE)
        except OSError:
            return float("inf")

    def _parse(self, xml_text: str) -> dict:
        """Parse the XML and return forward / reverse dicts."""
        forward: dict[int, MappingEntry]       = {}
        reverse: dict[int, list[MappingEntry]] = {}

        try:
            root = ET.fromstring(xml_text.encode("utf-8"))
        except Exception as e:
            log.error(f"AniDB mapping: XML parse error — {e}")
            return {"forward": forward, "reverse": reverse}

        for anime in root.iter("anime"):
            anidb_raw = anime.get("anidbid", "")
            if not anidb_raw or not anidb_raw.isdigit():
                continue
            anidb_id = int(anidb_raw)

            tvdb_raw = anime.get("tvdbid", "")
            tvdb_id: Optional[int] = None
            if tvdb_raw and tvdb_raw.isdigit():
                tvdb_id = int(tvdb_raw)

            tmdb_raw = anime.get("tmdbid", "")
            tmdb_id: Optional[int] = None
            if tmdb_raw and tmdb_raw.isdigit():
                tmdb_id = int(tmdb_raw)

            imdb_id = anime.get("imdbid", "") or None
            default_season = anime.get("defaulttvdbseason", "1") or "1"
            episode_offset = anime.get("episodeoffset", "0") or "0"

            # Best-effort title from <name> child element
            name_el = anime.find("name")
            title   = (name_el.text or "") if name_el is not None else ""

            entry = MappingEntry(
                anidb_id       = anidb_id,
                tvdb_id        = tvdb_id,
                tmdb_id        = tmdb_id,
                imdb_id        = imdb_id,
                default_season = default_season,
                episode_offset = episode_offset,
                title          = title,
            )

            forward[anidb_id] = entry

            if tvdb_id is not None:
                reverse.setdefault(tvdb_id, []).append(entry)

        # Sort each reverse list by (default_season numerically, anidb_id)
        for tvdb_id, entries in reverse.items():
            entries.sort(key=lambda e: (
                int(e.default_season) if e.default_season.isdigit() else 9999,
                e.anidb_id,
            ))

        log.info(f"AniDB mapping: parsed {len(forward)} entries, "
                 f"{len(reverse)} unique TVDB IDs")
        return {"forward": forward, "reverse": reverse}

    # ── Public interface ──────────────────────────────────────────────────────

    def load(self) -> None:
        """
        Load the mapping (download → cache → parse).
        Called once on startup; idempotent if TTL has not expired.
        """
        with self._lock:
            cache_age = self._cache_age_seconds()
            xml_text  = None

            if cache_age < self._ttl_seconds:
                # Cache is fresh — use it
                xml_text = self._load_from_cache()
                if xml_text:
                    log.info(f"AniDB mapping: using cached file "
                             f"(age {cache_age/3600:.1f}h < TTL {self._ttl_seconds/86400}d)")

            if xml_text is None:
                # Cache missing or stale — download
                xml_text = self._download_xml()
                if xml_text:
                    self._save_cache(xml_text)
                else:
                    # Fallback to stale cache
                    xml_text = self._load_from_cache()
                    if xml_text:
                        log.warning("AniDB mapping: download failed — using stale cache")
                    else:
                        log.error("AniDB mapping: no cache and download failed — "
                                  "AniDB resolution disabled")
                        return

            parsed = self._parse(xml_text)
            self._forward   = parsed["forward"]
            self._reverse   = parsed["reverse"]
            self._loaded_at = time.time()
            self._ready     = True

    def refresh(self) -> None:
        """Force a fresh download, ignoring TTL."""
        with self._lock:
            xml_text = self._download_xml()
            if xml_text:
                self._save_cache(xml_text)
                parsed = self._parse(xml_text)
                self._forward   = parsed["forward"]
                self._reverse   = parsed["reverse"]
                self._loaded_at = time.time()
                self._ready     = True
            else:
                log.warning("AniDB mapping: refresh download failed — keeping current data")

    @property
    def ready(self) -> bool:
        return self._ready

    @property
    def entry_count(self) -> int:
        return len(self._forward)

    def lookup(self, anidb_id: int) -> Optional[MappingEntry]:
        """
        Forward lookup: AniDB ID → MappingEntry (or None if unknown).

        Example:
            entry = mapper.lookup(7729)
            # MappingEntry(anidb_id=7729, tvdb_id=236622, tmdb_id=42942, ...)
        """
        return self._forward.get(anidb_id)

    def tvdb_seasons(self, tvdb_id: int) -> list[MappingEntry]:
        """
        Reverse lookup: TVDB series ID → all AniDB mapping entries (all seasons).
        Returns a list sorted by (default_season, anidb_id).

        Example:
            seasons = mapper.tvdb_seasons(236622)
            # [MappingEntry(season="1", title="Steins;Gate"),
            #  MappingEntry(season="2", title="Steins;Gate 0")]
        """
        return list(self._reverse.get(tvdb_id, []))

    def tmdb_for_anidb(self, anidb_id: int) -> Optional[int]:
        """Shortcut: return just the TMDB ID for an AniDB ID, or None."""
        entry = self.lookup(anidb_id)
        return entry.tmdb_id if entry else None

    def tvdb_for_anidb(self, anidb_id: int) -> Optional[int]:
        """Shortcut: return just the TVDB ID for an AniDB ID, or None."""
        entry = self.lookup(anidb_id)
        return entry.tvdb_id if entry else None


# ──────────────────────────────────────────────────────────────────────────────
# Module-level singleton
# ──────────────────────────────────────────────────────────────────────────────

_mapper_instance: Optional[AniDBMapper] = None
_mapper_lock = threading.Lock()


def get_mapper(ttl_days: int = _DEFAULT_TTL_DAYS) -> AniDBMapper:
    """
    Return the module-level AniDB mapper singleton.
    Lazy-loads on first call (downloads XML, parses, caches).
    Thread-safe.
    """
    global _mapper_instance
    if _mapper_instance is None:
        with _mapper_lock:
            if _mapper_instance is None:
                m = AniDBMapper(ttl_days=ttl_days)
                m.load()
                _mapper_instance = m
    return _mapper_instance


def reset_mapper() -> None:
    """Reset the singleton (used in tests)."""
    global _mapper_instance
    with _mapper_lock:
        _mapper_instance = None
