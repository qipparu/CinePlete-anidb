"""
Shikimori / MyAnimeList integration.
Handles ID mapping, export parsing (JSON/XML), and collection analysis.
"""
import os
import json
import time
import threading
import requests
import yaml
import xml.etree.ElementTree as ET
from typing import Optional, List, Dict, Any
from dataclasses import dataclass
from fastapi import APIRouter, Body

from app.logger import get_logger
from app.config import load_config, save_config
from app.tmdb import TMDB
from app.routers._shared import log, read_results

router = APIRouter()

DATA_DIR = os.getenv("DATA_DIR", "/data")
MAPPING_CACHE_FILE = os.path.join(DATA_DIR, "shikimori_mappings_cache.json")
EDITS_CACHE_FILE   = os.path.join(DATA_DIR, "shikimori_edits_cache.yaml")

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
    def __init__(self, mapping_url: str, edits_url: str, ttl_days: int = 7):
        self.mapping_url = mapping_url
        self.edits_url   = edits_url
        self.ttl_seconds = ttl_days * 86_400
        self._lock = threading.Lock()
        self._mal_to_entry: Dict[int, ShikimoriMappingEntry] = {}
        self._ready = False

    def load(self, force: bool = False):
        with self._lock:
            # Paths for local overrides in the workspace
            workspace_root = os.getcwd()
            local_json = os.path.join(workspace_root, "shikimori", "mappings.json")
            local_yaml = os.path.join(workspace_root, "shikimori", "mappings.edits.yaml")
            
            data = None
            edits = None
            
            cache_age = self._get_cache_age()
            is_stale = force or cache_age >= self.ttl_seconds

            # --- 1. Load/Download Base Mappings (JSON) ---
            if os.path.exists(local_json):
                try:
                    with open(local_json, "r", encoding="utf-8") as f:
                        data = json.load(f)
                        log.info(f"Shikimori Mapping: loaded local JSON {local_json}")
                except Exception as e:
                    log.warning(f"Shikimori Mapping: failed local JSON — {e}")

            if data is None:
                if not is_stale:
                    data = self._load_json_from_cache()
                
                if data is None:
                    data = self._download_file(self.mapping_url, "JSON")
                    if data:
                        self._save_json_to_cache(data)
                    else:
                        data = self._load_json_from_cache()
            
            if data is None:
                log.error("Shikimori Mapping: No local file, no cache, and download failed.")
                return

            # --- 2. Load/Download Edits (YAML) ---
            if os.path.exists(local_yaml):
                try:
                    with open(local_yaml, "r", encoding="utf-8") as f:
                        edits = yaml.safe_load(f)
                        log.info(f"Shikimori Mapping: loaded local YAML edits {local_yaml}")
                except Exception as e:
                    log.warning(f"Shikimori Mapping: failed local YAML — {e}")

            if edits is None:
                if not is_stale:
                    edits = self._load_yaml_from_cache()
                
                if edits is None and self.edits_url:
                    edits_raw = self._download_file(self.edits_url, "YAML", is_json=False)
                    if edits_raw:
                        try:
                            edits = yaml.safe_load(edits_raw)
                            self._save_yaml_to_cache(edits_raw)
                        except Exception as e:
                            log.warning(f"Shikimori Edits: failed to parse downloaded YAML — {e}")
                    else:
                        edits = self._load_yaml_from_cache()

            # --- 3. Merge Edits ---
            if edits:
                log.info(f"Shikimori Mapping: applying {len(edits)} overrides")
                for internal_id, edit_values in edits.items():
                    str_id = str(internal_id)
                    if str_id in data:
                        if isinstance(edit_values, dict) and isinstance(data[str_id], dict):
                            data[str_id].update(edit_values)
                        else:
                            data[str_id] = edit_values
                    else:
                        data[str_id] = edit_values

            self._parse(data)
            self._ready = True

    def _get_cache_age(self) -> float:
        try:
            return time.time() - os.path.getmtime(MAPPING_CACHE_FILE)
        except OSError:
            return float("inf")

    def _load_json_from_cache(self) -> Optional[dict]:
        try:
            with open(MAPPING_CACHE_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            return None

    def _save_json_to_cache(self, data: dict):
        os.makedirs(DATA_DIR, exist_ok=True)
        with open(MAPPING_CACHE_FILE, "w", encoding="utf-8") as f:
            json.dump(data, f)

    def _load_yaml_from_cache(self) -> Optional[dict]:
        try:
            with open(EDITS_CACHE_FILE, "r", encoding="utf-8") as f:
                return yaml.safe_load(f)
        except Exception:
            return None

    def _save_yaml_to_cache(self, content_str: str):
        os.makedirs(DATA_DIR, exist_ok=True)
        with open(EDITS_CACHE_FILE, "w", encoding="utf-8") as f:
            f.write(content_str)

    def _download_file(self, url: str, label: str, is_json: bool = True) -> Any:
        try:
            log.info(f"Downloading Shikimori {label} from {url}")
            r = requests.get(url, timeout=60)
            r.raise_for_status()
            return r.json() if is_json else r.text
        except Exception as e:
            log.warning(f"Shikimori {label}: download failed — {e}")
            return None

    def _parse(self, data: dict):
        mal_map = {}
        def _as_list(val):
            if val is None: return []
            if isinstance(val, (int, str)): return [val]
            return val

        for internal_id, entry in data.items():
            mal_ids = _as_list(entry.get("mal_id"))
            mapping = ShikimoriMappingEntry(
                internal_id = internal_id,
                anidb_id    = entry.get("anidb_id"), # usually single int
                mal_ids     = mal_ids,
                tvdb_id     = _as_list(entry.get("tvdb_id")),
                tmdb_show_id = _as_list(entry.get("tmdb_show_id")),
                tmdb_movie_id = _as_list(entry.get("tmdb_movie_id")),
                imdb_ids    = _as_list(entry.get("imdb_id"))
            )
            for mid in mal_ids:
                mal_map[int(mid)] = mapping
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
                edits_url = cfg.get("SHIKIMORI_EDITS_URL", "https://raw.githubusercontent.com/eliasbenb/PlexAniBridge-Mappings/refs/heads/v2/mappings.edits.yaml")
                ttl = cfg.get("SHIKIMORI_CACHE_TTL_DAYS", 7)
                m = ShikimoriMapper(url, edits_url, ttl)
                m.load()
                _mapper_instance = m
    return _mapper_instance

def load_shikimori_export(path_or_url: str) -> List[Dict[str, Any]]:
    """Loads the Shikimori/MAL export (JSON or XML) from file or URL."""
    content = None
    if path_or_url.startswith(("http://", "https://")):
        try:
            r = requests.get(path_or_url, timeout=30)
            r.raise_for_status()
            content = r.text
        except Exception as e:
            log.error(f"Failed to fetch Shikimori export from URL: {e}")
            return []
    else:
        if not os.path.isabs(path_or_url):
            base = os.getcwd()
            full_path = os.path.join(base, path_or_url)
        else:
            full_path = path_or_url

        if not os.path.exists(full_path):
            log.error(f"Shikimori export file not found: {full_path}")
            return []
        try:
            with open(full_path, "r", encoding="utf-8") as f:
                content = f.read()
        except Exception as e:
            log.error(f"Failed to read Shikimori export file: {e}")
            return []

    if not content:
        return []

    stripped = content.strip()
    if stripped.startswith("["):
        try:
            return json.loads(content)
        except Exception as e:
            log.error(f"Failed to parse Shikimori JSON: {e}")
            return []
    elif stripped.startswith("<"):
        return _parse_mal_xml(content)
    else:
        log.error("Unknown Shikimori export format (not JSON or XML)")
        return []

def _parse_mal_xml(xml_content: str) -> List[Dict[str, Any]]:
    """Parses MAL-style XML export into internal list format."""
    try:
        root = ET.fromstring(xml_content)
        items = []
        for anime in root.findall(".//anime"):
            def _get(tag):
                node = anime.find(tag)
                return node.text if node is not None else None

            mal_id = _get("series_animedb_id")
            if not mal_id: continue
            
            raw_status = (_get("my_status") or "plan to watch").lower().replace(" ", "_")
            if raw_status == "plan_to_watch": raw_status = "planned"

            items.append({
                "target_id": int(mal_id),
                "target_title": _get("series_title"),
                "status": raw_status,
                "score": int(_get("my_score") or 0),
                "episodes": int(_get("my_watched_episodes") or 0)
            })
        log.info(f"Shikimori: parsed {len(items)} items from XML")
        return items
    except Exception as e:
        log.error(f"Failed to parse MAL XML: {e}")
        return []

class ShikimoriAnalyzer:
    def __init__(self, tmdb_manager, library_snapshot: dict):
        self.tmdb = tmdb_manager
        self.library = library_snapshot
        self.mapper = get_shikimori_mapper()
        self.owned_tmdb  = set(self.library.get("plex_ids", {}).keys())
        self.owned_anidb = set()
        self.owned_tvdb  = set()
        for item in self.library.get("media_server", {}).get("anidb_items", []):
            if item.get("anidb_id"):
                self.owned_anidb.add(int(item["anidb_id"]))
            if item.get("tvdb_id"):
                self.owned_tvdb.add(int(item["tvdb_id"]))

    def analyze(self, export_items: List[Dict[str, Any]]) -> Dict[str, Any]:
        groups = {"watching": [], "planned": [], "completed": [], "on_hold": [], "dropped": []}
        stats = {"total": len(export_items), "owned": 0, "missing": 0}

        for item in export_items:
            mal_id = item.get("target_id")
            status = item.get("status", "planned")
            if not mal_id: continue

            mapping = self.mapper.lookup_mal(mal_id)
            is_owned = False
            def _is_owned(id_val, container):
                if not id_val: return False
                if isinstance(id_val, list):
                    return any(str(i) in container or i in container for i in id_val)
                return str(id_val) in container or id_val in container

            if mapping:
                if _is_owned(mapping.anidb_id, self.owned_anidb):
                    is_owned = True
                elif _is_owned(mapping.tvdb_id, self.owned_tvdb):
                    is_owned = True
                elif _is_owned(mapping.tmdb_show_id, self.owned_tmdb):
                    is_owned = True
                elif _is_owned(mapping.tmdb_movie_id, self.owned_tmdb):
                    is_owned = True

            if is_owned: stats["owned"] += 1
            else: stats["missing"] += 1

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
            }
            if status in groups: groups[status].append(display_item)
            else: groups.setdefault("other", []).append(display_item)

        for key in groups:
            groups[key].sort(key=lambda x: (-(x["score"] or 0), x["title"] or ""))
        return {"stats": stats, "groups": groups}

# --------------------------------------------------
# API Routes
# --------------------------------------------------

@router.get("/api/shikimori/config")
def api_shikimori_get_config():
    cfg = load_config()
    return {"ok": True, "shikimori": cfg.get("SHIKIMORI", {})}

@router.post("/api/shikimori/config")
def api_shikimori_save_config(payload: dict = Body(...)):
    cfg = load_config()
    cfg["SHIKIMORI"] = payload
    save_config(cfg)
    return {"ok": True}

@router.get("/api/shikimori/collection")
def api_shikimori_get_collection():
    cfg = load_config()
    shiki_cfg = cfg.get("SHIKIMORI", {})
    if not shiki_cfg.get("SHIKIMORI_ENABLED"):
        return {"ok": False, "error": "Shikimori integration is disabled"}
    export_path = shiki_cfg.get("SHIKIMORI_EXPORT_URL")
    if not export_path:
        return {"ok": False, "error": "Shikimori Export URL/Path is not configured in Settings."}
    export_items = load_shikimori_export(export_path)
    if not export_items:
        return {"ok": False, "error": f"Could not load Shikimori export from {export_path}"}
    mapper = get_shikimori_mapper()
    if not mapper._ready:
        mapper.load()
    results = read_results()
    if not results:
        return {"ok": False, "error": "No scan results found. Please run a full scan first."}
    tmdb_api_key = cfg.get("TMDB", {}).get("TMDB_API_KEY")
    tmdb = TMDB(tmdb_api_key) if tmdb_api_key else None
    analyzer = ShikimoriAnalyzer(tmdb, results)
    analysis = analyzer.analyze(export_items)
    return {
        "ok": True,
        "collection": analysis["groups"],
        "stats": analysis["stats"],
        "fetched_at": results.get("generated_at")
    }

@router.post("/api/shikimori/refresh")
def api_shikimori_refresh():
    mapper = get_shikimori_mapper()
    mapper.load(force=True)
    return {"ok": True}
