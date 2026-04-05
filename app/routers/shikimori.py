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
from fastapi import APIRouter, Body, BackgroundTasks

from app.logger import get_logger
from app.config import load_config, save_config
from app.tmdb import TMDB
from app.routers._shared import log, read_results

router = APIRouter()

DATA_DIR = os.getenv("DATA_DIR", "/data")
MAPPING_CACHE_FILE  = os.path.join(DATA_DIR, "shikimori_mappings_cache.json")
EDITS_CACHE_FILE    = os.path.join(DATA_DIR, "shikimori_edits_cache.yaml")
POSTERS_CACHE_FILE  = os.path.join(DATA_DIR, "shikimori_posters.json")

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
        self._anidb_to_entry: Dict[int, ShikimoriMappingEntry] = {}
        self._tvdb_to_entry: Dict[int, ShikimoriMappingEntry] = {}
        self._tmdb_to_entry: Dict[int, ShikimoriMappingEntry] = {}
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
        """
        Parses V3 dictionary-based mappings.
        Data structure: { "provider:id[:scope]": { "target_provider:id[:scope]": { "range": "range" } } }
        """
        mal_map:   Dict[int, ShikimoriMappingEntry] = {}
        anidb_map: Dict[int, ShikimoriMappingEntry] = {}
        tvdb_map:  Dict[int, ShikimoriMappingEntry] = {}
        tmdb_map:  Dict[int, ShikimoriMappingEntry] = {}

        def _split_descriptor(desc: str):
            parts = desc.split(":")
            provider = parts[0]
            id_val = parts[1] if len(parts) > 1 else None
            return provider, id_val

        # We treat each source -> targets block as a cluster of related IDs.
        # Note: In V3, an entry can be both a source and a target.
        clusters: List[Dict[str, set]] = []

        for source_desc, targets in data.items():
            if source_desc.startswith("$"): continue # Skip metadata
            
            cluster = {"mal": set(), "anidb": set(), "tvdb": set(), "tmdb_show": set(), "tmdb_movie": set(), "internal": source_desc}
            
            # Add source to cluster
            src_prov, src_id = _split_descriptor(source_desc)
            if src_prov == "mal": cluster["mal"].add(int(src_id))
            elif src_prov == "anidb": cluster["anidb"].add(int(src_id))
            elif src_prov == "tvdb_show": cluster["tvdb"].add(int(src_id))
            elif src_prov == "tmdb_show": cluster["tmdb_show"].add(int(src_id))
            elif src_prov == "tmdb_movie": cluster["tmdb_movie"].add(int(src_id))

            # Add targets to cluster
            for target_desc in targets.keys():
                trg_prov, trg_id = _split_descriptor(target_desc)
                if trg_prov == "mal": cluster["mal"].add(int(trg_id))
                elif trg_prov == "anidb": cluster["anidb"].add(int(trg_id))
                elif trg_prov == "tvdb_show": cluster["tvdb"].add(int(trg_id))
                elif trg_prov == "tmdb_show": cluster["tmdb_show"].add(int(trg_id))
                elif trg_prov == "tmdb_movie": cluster["tmdb_movie"].add(int(trg_id))
            
            clusters.append(cluster)

        # Reconstruct ShikimoriMappingEntry from clusters.
        # If multiple clusters share a MAL ID, we could merge them, but for the analyzer,
        # having the first match is usually sufficient for identity.
        for clus in clusters:
            mapping = ShikimoriMappingEntry(
                internal_id = clus["internal"],
                anidb_id    = list(clus["anidb"])[0] if clus["anidb"] else None,
                mal_ids     = list(clus["mal"]),
                tvdb_id     = list(clus["tvdb"]),
                tmdb_show_id = list(clus["tmdb_show"]),
                tmdb_movie_id = list(clus["tmdb_movie"]),
                imdb_ids    = []
            )
            
            for mid in mapping.mal_ids:
                if mid not in mal_map: mal_map[mid] = mapping
            if mapping.anidb_id and mapping.anidb_id not in anidb_map:
                anidb_map[mapping.anidb_id] = mapping
            for tid in mapping.tvdb_id:
                if tid not in tvdb_map: tvdb_map[tid] = mapping
            for tid in mapping.tmdb_show_id:
                if tid not in tmdb_map: tmdb_map[tid] = mapping
            for tid in mapping.tmdb_movie_id:
                if tid not in tmdb_map: tmdb_map[tid] = mapping

        self._mal_to_entry   = mal_map
        self._anidb_to_entry = anidb_map
        self._tvdb_to_entry  = tvdb_map
        self._tmdb_to_entry  = tmdb_map
        log.info(f"Shikimori Mapping V3: parsed {len(mal_map)} MAL IDs from {len(data)} mapping blocks")

    def lookup_mal(self, mal_id: int) -> Optional[ShikimoriMappingEntry]:
        try: return self._mal_to_entry.get(int(mal_id))
        except (ValueError, TypeError): return None

    def lookup_anidb(self, anidb_id: int) -> Optional[ShikimoriMappingEntry]:
        try: return self._anidb_to_entry.get(int(anidb_id))
        except (ValueError, TypeError): return None

    def lookup_tvdb(self, tvdb_id: int) -> Optional[ShikimoriMappingEntry]:
        try: return self._tvdb_to_entry.get(int(tvdb_id))
        except (ValueError, TypeError): return None

    def lookup_tmdb(self, tmdb_id: int) -> Optional[ShikimoriMappingEntry]:
        try: return self._tmdb_to_entry.get(int(tmdb_id))
        except (ValueError, TypeError): return None

_mapper_instance: Optional[ShikimoriMapper] = None
_mapper_lock = threading.Lock()

def get_shikimori_mapper() -> ShikimoriMapper:
    global _mapper_instance
    if _mapper_instance is None:
        with _mapper_lock:
            if _mapper_instance is None:
                cfg = load_config().get("SHIKIMORI", {})
                # NEW V3 URLs
                default_url = "https://github.com/anibridge/anibridge-mappings/releases/download/v3/mappings.json"
                default_edits = "https://raw.githubusercontent.com/anibridge/anibridge-mappings/refs/heads/main/mappings.edits.yaml"
                
                url = cfg.get("SHIKIMORI_MAPPING_URL") or default_url
                edits_url = cfg.get("SHIKIMORI_EDITS_URL") or default_edits
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
        self.owned_anidb = set()
        self.owned_tvdb  = set()
        
        # Build poster lookup map
        self.poster_lookup = {} # TMDB_ID -> URL
        
        # 1. From anidb_items (posters already extracted from media server)
        for li in self.library.get("media_server", {}).get("anidb_items", []):
            if li.get("anidb_id"):
                self.owned_anidb.add(int(li["anidb_id"]))
            if li.get("tvdb_id"):
                self.owned_tvdb.add(int(li["tvdb_id"]))
            
            p = li.get("poster")
            for tid in [li.get("tmdb_id")]:
                if tid and p: self.poster_lookup[int(tid)] = p
        
        # 2. From suggestions (TMDB ID -> full poster path)
        for s in self.library.get("suggestions", []):
            tid = s.get("tmdb")
            if tid and s.get("poster"):
                self.poster_lookup[int(tid)] = s["poster"]
                
        # 3. From classics
        for c in self.library.get("classics", []):
            tid = c.get("tmdb")
            if tid and c.get("poster"):
                self.poster_lookup[int(tid)] = c["poster"]
        
        # 4. From persistent posters cache
        try:
            if os.path.exists(POSTERS_CACHE_FILE):
                with open(POSTERS_CACHE_FILE, "r", encoding="utf-8") as f:
                    p_cache = json.load(f)
                    for tid, url in p_cache.items():
                        if url: self.poster_lookup[int(tid)] = url
        except Exception as e:
            log.debug(f"Failed to load posters cache: {e}")

    def analyze(self, export_items: List[Dict[str, Any]]) -> Dict[str, Any]:
        groups = {"watching": [], "planned": [], "completed": [], "on_hold": [], "dropped": []}
        stats = {"total": len(export_items), "owned": 0, "missing": 0}
        
        mal_ids_in_export = set()
        for item in export_items:
            if item.get("target_id"):
                mal_ids_in_export.add(int(item["target_id"]))

        for item in export_items:
            mal_id = item.get("target_id")
            status = item.get("status", "planned")
            if not mal_id: continue

            mapping = self.mapper.lookup_mal(mal_id)
            is_owned = False
            
            # Find library item if owned (to get its poster)
            lib_item = None
            if mapping:
                # Try to find a matching library item in anidb_items
                for li in self.library.get("media_server", {}).get("anidb_items", []):
                    # Match by AniDB
                    if mapping.anidb_id and li.get("anidb_id") == int(mapping.anidb_id):
                        lib_item = li; break
                    # Match by TVDB
                    if li.get("tvdb_id") and int(li["tvdb_id"]) in [int(tid) for tid in mapping.tvdb_id]:
                        lib_item = li; break
                    # Match by TMDB
                    if li.get("tmdb_id") and (int(li["tmdb_id"]) in [int(tid) for tid in mapping.tmdb_show_id] or 
                                              int(li["tmdb_id"]) in [int(tid) for tid in mapping.tmdb_movie_id]):
                        lib_item = li; break
            
            if lib_item: is_owned = True

            if is_owned: stats["owned"] += 1
            else: stats["missing"] += 1

            # Poster resolution: Cache -> Library -> suggestions
            poster = None
            tmdb_id = (mapping.tmdb_show_id[0] if mapping and mapping.tmdb_show_id else 
                       (mapping.tmdb_movie_id[0] if mapping and mapping.tmdb_movie_id else None))
            
            if lib_item and lib_item.get("poster"):
                poster = lib_item["poster"]
            elif tmdb_id and tmdb_id in self.poster_lookup:
                poster = self.poster_lookup[int(tmdb_id)]
            elif tmdb_id and self.tmdb:
                # DONT make sequential network calls. 
                # Instead just use placeholder and we'd eventually populate cache.
                pass

            display_item = {
                "mal_id": mal_id,
                "title": item.get("target_title"),
                "status": status,
                "score": item.get("score"),
                "episodes": item.get("episodes"),
                "is_owned": is_owned,
                "poster": poster,
                "tmdb_id": (mapping.tmdb_show_id[0] if (mapping and mapping.tmdb_show_id) else 
                            (mapping.tmdb_movie_id[0] if (mapping and mapping.tmdb_movie_id) else None)),
                "anidb_id": mapping.anidb_id if mapping else None,
            }
            if status in groups: groups[status].append(display_item)
            else: groups.setdefault("other", []).append(display_item)

        # --- "Missing on MAL" Analysis ---
        missing_on_mal_groups = {} # { franchise_name: [ items ] }
        
        from app.anidb_mapping import get_mapper as get_anidb_mapper
        anidb_mapper = get_anidb_mapper()

        seen_mal_ids = set()
        for li in self.library.get("media_server", {}).get("anidb_items", []):
            mapped = None
            if li.get("anidb_id"): mapped = self.mapper.lookup_anidb(li["anidb_id"])
            if not mapped and li.get("tvdb_id"): mapped = self.mapper.lookup_tvdb(li["tvdb_id"])
            if not mapped and li.get("tmdb_id"): mapped = self.mapper.lookup_tmdb(li["tmdb_id"])
            
            if mapped:
                on_mal = False
                for mid in mapped.mal_ids:
                    if int(mid) in mal_ids_in_export:
                        on_mal = True; break
                
                if not on_mal:
                    # Use the first MAL ID as representative
                    mid = int(mapped.mal_ids[0]) if mapped.mal_ids else None
                    if mid and mid not in seen_mal_ids:
                        seen_mal_ids.add(mid)
                        
                        # Resolve franchise name
                        cname = "General"
                        if li.get("anidb_id"):
                            cname = anidb_mapper.collection_for_anidb(int(li["anidb_id"])) or "Other"
                        
                        missing_on_mal_groups.setdefault(cname, []).append({
                            "title": li.get("title"),
                            "poster": li.get("poster"),
                            "mal_id": mid,
                            "tmdb_id": li.get("tmdb_id"),
                            "anidb_id": li.get("anidb_id"),
                            "tvdb_id": li.get("tvdb_id")
                        })
        
        # Convert to list of groups
        missing_on_mal = []
        for cname, items in missing_on_mal_groups.items():
            missing_on_mal.append({
                "name": cname,
                "items": items
            })
        missing_on_mal.sort(key=lambda x: x["name"].lower())

        for key in groups:
            groups[key].sort(key=lambda x: (-(x["score"] or 0), (x["title"] or "")))
        
        return {"stats": stats, "groups": groups, "missing_on_mal": missing_on_mal}

    def fetch_missing_posters(self, export_items: List[Dict[str, Any]]):
        """Background task to populate missing posters from TMDB API."""
        if not self.tmdb: return
        
        updated = False
        # Limit to watching/planned/on_hold to focus on relevant items first (prevents huge spam on first run)
        relevant_statuses = ["watching", "planned", "on_hold"]
        
        for item in export_items:
            if item.get("status") not in relevant_statuses: continue
            
            mal_id = item.get("target_id")
            if not mal_id: continue
            
            mapping = self.mapper.lookup_mal(mal_id)
            if not mapping: continue
            
            tmdb_id = (mapping.tmdb_show_id[0] if mapping.tmdb_show_id else 
                       (mapping.tmdb_movie_id[0] if mapping.tmdb_movie_id else None))
            
            if not tmdb_id or int(tmdb_id) in self.poster_lookup: continue
            
            # Resolve it from TMDB API
            is_tv = bool(mapping.tmdb_show_id)
            try:
                res = self.tmdb.tv_show(int(tmdb_id)) if is_tv else self.tmdb.movie(int(tmdb_id))
                if res and res.get("poster_path"):
                    url = f"https://image.tmdb.org/t/p/w342{res['poster_path']}"
                    self.poster_lookup[int(tmdb_id)] = url
                    updated = True
                    log.debug(f"Resolved new poster for TMDB:{tmdb_id} -> {url}")
            except Exception as e:
                log.warning(f"Background fetch failed for TMDB:{tmdb_id} - {e}")
        
        if updated:
            # Save back to persistent cache
            try:
                # Merge with existing file to be safe
                existing = {}
                if os.path.exists(POSTERS_CACHE_FILE):
                    with open(POSTERS_CACHE_FILE, "r", encoding="utf-8") as f:
                        existing = json.load(f)
                
                # Convert keys to string for JSON
                to_save = {str(k): v for k, v in self.poster_lookup.items()}
                existing.update(to_save)
                
                with open(POSTERS_CACHE_FILE, "w", encoding="utf-8") as f:
                    json.dump(existing, f)
                log.info(f"Updated persistent poster cache with resolved items.")
            except Exception as e:
                log.error(f"Failed to save posters cache: {e}")



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
def api_shikimori_get_collection(background_tasks: BackgroundTasks):
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
    
    # Trigger background poster resolution to populate cache for future visits
    if tmdb:
        background_tasks.add_task(analyzer.fetch_missing_posters, export_items)

    return {
        "ok": True,
        "collection": analysis["groups"],
        "missing_on_mal": analysis["missing_on_mal"],
        "stats": analysis["stats"],
        "fetched_at": results.get("generated_at")
    }


@router.post("/api/shikimori/refresh")
def api_shikimori_refresh():
    mapper = get_shikimori_mapper()
    mapper.load(force=True)
    return {"ok": True}
