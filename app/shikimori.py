import os
import json
import time
import threading
import requests
from typing import Optional, List, Dict, Any
from dataclasses import dataclass

from app.logger import get_logger
from app.config import load_config

log = get_logger(__name__)

DATA_DIR = os.getenv("DATA_DIR", "/data")
MAPPING_CACHE_FILE = os.path.join(DATA_DIR, "shikimori_mappings_cache.json")

@dataclass
class ShikimoriMappingEntry:
    internal_id: str
    anidb_id: Optional[int] = None
    mal_ids: List[int] = None
    tvdb_id: Optional[int] = None
    tmdb_show_id: Optional[int] = None
    tmdb_movie_id: Optional[int] = None
    imdb_ids: List[str] = None

class ShikimoriMapper:
    """
    Handles downloading and searching the PlexAniBridge-Mappings v2 JSON.
    Maps MAL IDs (Shikimori target_id) to AniDB, TVDB, and TMDB.
    """
    def __init__(self, mapping_url: str, ttl_days: int = 7):
        self.mapping_url = mapping_url
        self.ttl_seconds = ttl_days * 86_400
        self._lock = threading.Lock()
        self._mal_to_entry: Dict[int, ShikimoriMappingEntry] = {}
        self._ready = False

    def load(self, force: bool = False):
        with self._lock:
            cache_age = self._get_cache_age()
            data = None

            if not force and cache_age < self.ttl_seconds:
                data = self._load_from_cache()
                if data:
                    log.info(f"Shikimori Mapping: using cached file (age {cache_age/3600:.1f}h)")

            if data is None:
                data = self._download_mapping()
                if data:
                    self._save_to_cache(data)
                else:
                    data = self._load_from_cache()
                    if not data:
                        log.error("Shikimori Mapping: No cache and download failed.")
                        return

            self._parse(data)
            self._ready = True

    def _get_cache_age(self) -> float:
        try:
            return time.time() - os.path.getmtime(MAPPING_CACHE_FILE)
        except OSError:
            return float("inf")

    def _load_from_cache(self) -> Optional[dict]:
        try:
            with open(MAPPING_CACHE_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            return None

    def _save_to_cache(self, data: dict):
        os.makedirs(DATA_DIR, exist_ok=True)
        with open(MAPPING_CACHE_FILE, "w", encoding="utf-8") as f:
            json.dump(data, f)

    def _download_mapping(self) -> Optional[dict]:
        try:
            log.info(f"Downloading Shikimori mappings from {self.mapping_url}")
            r = requests.get(self.mapping_url, timeout=60)
            r.raise_for_status()
            return r.json()
        except Exception as e:
            log.warning(f"Shikimori Mapping: download failed — {e}")
            return None

    def _parse(self, data: dict):
        mal_map = {}
        for internal_id, entry in data.items():
            mal_ids_raw = entry.get("mal_id", [])
            if isinstance(mal_ids_raw, int):
                mal_ids = [mal_ids_raw]
            else:
                mal_ids = mal_ids_raw

            mapping = ShikimoriMappingEntry(
                internal_id = internal_id,
                anidb_id    = entry.get("anidb_id"),
                mal_ids     = mal_ids,
                tvdb_id     = entry.get("tvdb_id"),
                tmdb_show_id = entry.get("tmdb_show_id"),
                tmdb_movie_id = entry.get("tmdb_movie_id"),
                imdb_ids    = entry.get("imdb_id", []) if isinstance(entry.get("imdb_id"), list) else ([entry.get("imdb_id")] if entry.get("imdb_id") else [])
            )
            for mid in mal_ids:
                mal_map[mid] = mapping
        self._mal_to_entry = mal_map
        log.info(f"Shikimori Mapping: parsed {len(mal_map)} MAL IDs from {len(data)} entries")

    def lookup_mal(self, mal_id: int) -> Optional[ShikimoriMappingEntry]:
        return self._mal_to_entry.get(mal_id)

_mapper_instance: Optional[ShikimoriMapper] = None
_mapper_lock = threading.Lock()

def get_shikimori_mapper() -> ShikimoriMapper:
    global _mapper_instance
    if _mapper_instance is None:
        with _mapper_lock:
            if _mapper_instance is None:
                cfg = load_config().get("SHIKIMORI", {})
                url = cfg.get("SHIKIMORI_MAPPING_URL", "https://raw.githubusercontent.com/eliasbenb/PlexAniBridge-Mappings/refs/heads/v2/mappings.json")
                ttl = cfg.get("SHIKIMORI_CACHE_TTL_DAYS", 7)
                m = ShikimoriMapper(url, ttl)
                m.load()
                _mapper_instance = m
    return _mapper_instance

def load_shikimori_export(path_or_url: str) -> List[Dict[str, Any]]:
    """Loads the Shikimori JSON export from file or URL."""
    if path_or_url.startswith(("http://", "https://")):
        try:
            r = requests.get(path_or_url, timeout=30)
            r.raise_for_status()
            return r.json()
        except Exception as e:
            log.error(f"Failed to fetch Shikimori export from URL: {e}")
            return []
    else:
        # Resolve path relative to app root or absolute
        if not os.path.isabs(path_or_url):
            # Assume relative to workspace root if not found
            base = os.getcwd()
            full_path = os.path.join(base, path_or_url)
        else:
            full_path = path_or_url

        if not os.path.exists(full_path):
            log.error(f"Shikimori export file not found: {full_path}")
            return []
        try:
            with open(full_path, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception as e:
            log.error(f"Failed to read Shikimori export file: {e}")
            return []

class ShikimoriAnalyzer:
    def __init__(self, tmdb_manager, library_snapshot: dict):
        """
        :param tmdb_manager: An instance of the TMDB utility class
        :param library_snapshot: Results from the latest scan (from results.json)
        """
        self.tmdb = tmdb_manager
        self.library = library_snapshot
        self.mapper = get_shikimori_mapper()

        # Build sets of what we have on the server for fast lookup
        # plex_ids contains TMDB IDs for movies/shows
        self.owned_tmdb = set(self.library.get("plex_ids", {}).keys())
        
        # also check anidb_items for anime specifically
        self.owned_anidb = set()
        for item in self.library.get("media_server", {}).get("anidb_items", []):
            if item.get("anidb_id"):
                self.owned_anidb.add(int(item["anidb_id"]))

    def analyze(self, export_items: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Compare Shikimori list with library and return grouped results.
        """
        groups = {
            "watching": [],
            "planned":  [],
            "completed": [],
            "on_hold":  [],
            "dropped":  []
        }
        
        stats = {
            "total": len(export_items),
            "owned": 0,
            "missing": 0
        }

        for item in export_items:
            mal_id = item.get("target_id")
            status = item.get("status", "planned")
            if not mal_id:
                continue

            mapping = self.mapper.lookup_mal(mal_id)
            is_owned = False
            
            if mapping:
                # Check ownership across all possible IDs
                if mapping.anidb_id and mapping.anidb_id in self.owned_anidb:
                    is_owned = True
                elif mapping.tmdb_show_id and str(mapping.tmdb_show_id) in self.owned_tmdb:
                    is_owned = True
                elif mapping.tmdb_movie_id and str(mapping.tmdb_movie_id) in self.owned_tmdb:
                    is_owned = True
                # fallback: some systems use ints in plex_ids
                elif mapping.tmdb_show_id and mapping.tmdb_show_id in self.owned_tmdb:
                    is_owned = True
                elif mapping.tmdb_movie_id and mapping.tmdb_movie_id in self.owned_tmdb:
                    is_owned = True

            if is_owned:
                stats["owned"] += 1
            else:
                stats["missing"] += 1

            # Fetch basic info for display
            display_item = {
                "mal_id": mal_id,
                "title": item.get("target_title"),
                "title_ru": item.get("target_title_ru"),
                "status": status,
                "score": item.get("score"),
                "episodes": item.get("episodes"),
                "is_owned": is_owned,
                "tmdb_id": mapping.tmdb_show_id or mapping.tmdb_movie_id if mapping else None,
                "anidb_id": mapping.anidb_id if mapping else None,
                "poster": None
            }

            # Try to get a poster if it's missing and we have a TMDB ID
            if not is_owned and display_item["tmdb_id"]:
                # We don't want to spam TMDB here, but we can use the cached URL format
                # Most posters are already cached or available via /api/movie/{id}
                pass

            if status in groups:
                groups[status].append(display_item)
            else:
                groups.setdefault("other", []).append(display_item)

        # Sort each group by score (desc) then title
        for key in groups:
            groups[key].sort(key=lambda x: (-(x["score"] or 0), x["title"] or ""))

        return {
            "stats": stats,
            "groups": groups
        }
