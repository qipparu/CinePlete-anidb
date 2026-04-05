"""
Cineplete — Jellyfin scanner
-----------------------------
Drop-in replacement for plex_xml.py.
Returns the exact same tuple: (plex_ids, directors, actors, stats, no_tmdb_guid)
so scanner.py needs no logic changes.
"""

import requests
from collections import defaultdict

from app.config import load_config
from app.logger import get_logger

log = get_logger(__name__)


def _build_lib_cfg(lib_cfg):
    """Resolve lib_cfg — if None, fall back to legacy JELLYFIN config section."""
    if lib_cfg is not None:
        return lib_cfg
    cfg = load_config()
    jf = cfg["JELLYFIN"]
    return {
        "url": jf["JELLYFIN_URL"],
        "api_key": jf["JELLYFIN_API_KEY"],
        "library_name": jf.get("JELLYFIN_LIBRARY_NAME", "Movies"),
        "page_size": int(jf.get("JELLYFIN_PAGE_SIZE", 500)),
        "short_movie_limit": int(jf.get("SHORT_MOVIE_LIMIT", 60)),
    }


def _jf_get(path: str, lib_cfg=None, params: dict = None, timeout: int = 120) -> dict:
    """Make an authenticated GET request to Jellyfin and return JSON."""
    lc = _build_lib_cfg(lib_cfg)

    headers = {"X-Emby-Token": lc["api_key"]}
    url     = lc["url"].rstrip("/") + path

    try:
        r = requests.get(url, headers=headers, params=params or {}, timeout=timeout)
        r.raise_for_status()
        return r.json()
    except requests.exceptions.ConnectionError as exc:
        raise RuntimeError(
            f"Cannot connect to Jellyfin at {lc['url']} — "
            "check url in config and that Jellyfin is reachable"
        ) from exc


def _library_id(library_name: str, lib_cfg=None) -> str:
    """Resolve a library name to its Jellyfin item ID."""
    data = _jf_get("/Library/MediaFolders", lib_cfg)
    for item in data.get("Items", []):
        if item.get("Name", "").lower() == library_name.lower():
            return item["Id"]
    raise RuntimeError(f"Jellyfin library '{library_name}' not found")


def scan_movies(lib_cfg=None):
    """
    Scan the configured Jellyfin movie library.

    Falls back to AniDB→TMDB mapping when anidb:// provider ID is present
    but no Tmdb ID is found.

    Returns:
        plex_ids      dict[int, str]   — {tmdb_id: title}
        directors     dict[str, set]   — {director_name: {tmdb_id, ...}}
        actors        dict[str, set]   — {actor_name: {tmdb_id, ...}}
        stats         dict             — scan statistics (includes anidb_items=[])
        no_tmdb_guid  list[dict]       — films without a TMDB provider ID
    """
    lc = _build_lib_cfg(lib_cfg)

    short_movie_limit = int(lc.get("short_movie_limit", 60))
    page_size         = int(lc.get("page_size", 500))
    library_name      = lc.get("library_name", "Movies")

    library_id = _library_id(library_name, lc)
    log.info(f"Jellyfin library '{library_name}' → ID {library_id}")

    _mapper = None
    def get_mapper():
        nonlocal _mapper
        if _mapper is None:
            from app.anidb_mapping import get_mapper as _gm
            _mapper = _gm()
        return _mapper

    media_ids    = {}          # {tmdb_id: title}
    tmdb_id_dupes = {}
    directors    = defaultdict(set)
    actors       = defaultdict(set)
    no_tmdb_guid = []

    start     = 0
    scanned   = 0
    skipped_short = 0
    anidb_resolved   = 0
    anidb_not_mapped = 0

    while True:
        data = _jf_get("/Items", lc, {
            "ParentId":        library_id,
            "IncludeItemTypes":"Movie",
            "Recursive":       "true",
            "Fields":          "ProviderIds,People,RunTimeTicks",
            "StartIndex":      start,
            "Limit":           page_size,
        })

        items = data.get("Items", [])
        if not items:
            break

        for item in items:
            scanned += 1

            title = item.get("Name", "")
            year  = str(item.get("ProductionYear", "")) or None

            ticks        = int(item.get("RunTimeTicks") or 0)
            duration_min = ticks / 600_000_000

            if duration_min and duration_min < short_movie_limit:
                skipped_short += 1
                continue

            provider_ids = item.get("ProviderIds", {})
            tmdb_raw     = provider_ids.get("Tmdb") or provider_ids.get("tmdb")
            anidb_raw    = provider_ids.get("AniDB") or provider_ids.get("Anidb") or provider_ids.get("anidb")

            tmdb_id = None
            if tmdb_raw:
                try:
                    tmdb_id = int(tmdb_raw)
                except (ValueError, TypeError):
                    pass

            # AniDB fallback
            if not tmdb_id and anidb_raw:
                try:
                    entry = get_mapper().lookup(int(anidb_raw))
                except (ValueError, TypeError):
                    entry = None
                if entry and entry.tmdb_id:
                    tmdb_id = entry.tmdb_id
                    anidb_resolved += 1
                    log.debug(f"AniDB→TMDB: anidb/{anidb_raw} → tmdb/{tmdb_id} ({title})")
                elif anidb_raw:
                    anidb_not_mapped += 1
                    no_tmdb_guid.append({"title": title, "year": year, "source": "anidb"})
                    continue

            if not tmdb_id:
                no_tmdb_guid.append({"title": title, "year": year})
                continue

            if tmdb_id in media_ids:
                if tmdb_id not in tmdb_id_dupes:
                    tmdb_id_dupes[tmdb_id] = [{"title": media_ids[tmdb_id], "edition": ""}]
                tmdb_id_dupes[tmdb_id].append({"title": title, "edition": ""})
            else:
                media_ids[tmdb_id] = title

            actor_count = 0
            for person in item.get("People", []):
                name      = person.get("Name", "").strip()
                role_type = person.get("Type", "")
                if not name:
                    continue
                if role_type == "Director":
                    directors[name].add(tmdb_id)
                elif role_type == "Actor" and actor_count < 5:
                    actors[name].add(tmdb_id)
                    actor_count += 1

        total = data.get("TotalRecordCount", 0)
        start += len(items)
        if start >= total:
            break

    directors = {k: v for k, v in directors.items() if len(v) > 1}
    actors    = {k: v for k, v in actors.items()    if len(v) > 1}

    if anidb_resolved:
        log.info(f"AniDB→TMDB resolved: {anidb_resolved} movies, "
                 f"not mapped: {anidb_not_mapped}")

    stats = {
        "scanned_items":  scanned,
        "indexed_tmdb":   len(media_ids),
        "skipped_short":  skipped_short,
        "directors_kept": len(directors),
        "actors_kept":    len(actors),
        "no_tmdb_guid":   len(no_tmdb_guid),
        "anidb_resolved": anidb_resolved,
        "anidb_not_mapped": anidb_not_mapped,
        "anidb_items":    [],   # empty for movie libraries
        "duplicates": [
            {"tmdb": tmdb_id, "titles": titles}
            for tmdb_id, titles in tmdb_id_dupes.items()
        ],
    }

    return media_ids, directors, actors, stats, no_tmdb_guid


def scan_shows(lib_cfg=None):
    """
    Scan a Jellyfin TV show library — intended for HAMA-tagged anime libraries.

    Collects show-level provider IDs (AniDB, Tvdb, Tmdb) and resolves them
    via the AniDB mapper. stats["anidb_items"] contains full mapping dicts
    for every AniDB-tagged show, consumed by scanner._analyze_anime_seasons().

    Returns: (plex_ids, directors={}, actors={}, stats, no_tmdb_guid)
    """
    lc = _build_lib_cfg(lib_cfg)

    page_size    = int(lc.get("page_size", 500))
    library_name = lc.get("library_name", "Anime")

    library_id = _library_id(library_name, lc)
    log.info(f"Jellyfin anime library '{library_name}' → ID {library_id}")

    from app.anidb_mapping import get_mapper
    mapper = get_mapper()

    plex_ids:    dict[int, str] = {}
    no_tmdb_guid: list[dict]    = []
    anidb_items: list[dict]     = []
    start     = 0
    scanned   = 0
    anidb_resolved   = 0
    anidb_not_mapped = 0

    while True:
        data = _jf_get("/Items", lc, {
            "ParentId":        library_id,
            "IncludeItemTypes":"Series",
            "Recursive":       "true",
            "Fields":          "ProviderIds",
            "StartIndex":      start,
            "Limit":           page_size,
        })

        items = data.get("Items", [])
        if not items:
            break

        for item in items:
            scanned += 1
            title = item.get("Name", "")
            year  = str(item.get("ProductionYear", "")) or None

            provider_ids = item.get("ProviderIds", {})
            tmdb_raw  = provider_ids.get("Tmdb")  or provider_ids.get("tmdb")
            anidb_raw = provider_ids.get("AniDB") or provider_ids.get("Anidb") or provider_ids.get("anidb")
            tvdb_raw  = provider_ids.get("Tvdb")  or provider_ids.get("tvdb")

            tmdb_id = None
            if tmdb_raw:
                try:
                    tmdb_id = int(tmdb_raw)
                except (ValueError, TypeError):
                    pass

            entry = None
            if anidb_raw:
                try:
                    entry = mapper.lookup(int(anidb_raw))
                except (ValueError, TypeError):
                    entry = None

                if entry:
                    if not tmdb_id and entry.tmdb_id:
                        tmdb_id = entry.tmdb_id
                        anidb_resolved += 1
                    anidb_items.append(entry.as_dict())
                else:
                    anidb_not_mapped += 1

            # Fallback: if no AniDB or not in mapper, but we have TVDB or TMDB,
            # still track it for Shikimori matching and season tracking.
            if not entry and (tvdb_raw or tmdb_id):
                anidb_items.append({
                    "title":    title,
                    "anidb_id": int(anidb_raw) if anidb_raw else None,
                    "tvdb_id":  int(tvdb_raw)  if tvdb_raw else None,
                    "tmdb_id":  tmdb_id
                })

            if tmdb_id:
                if tmdb_id not in plex_ids:
                    plex_ids[tmdb_id] = title
            else:
                # If it's an anime that mapped/detected successfully, don't flag as missing GUID
                is_detected = anidb_raw or tvdb_raw
                if not is_detected:
                    no_tmdb_guid.append({
                        "title": title, 
                        "year": year, 
                        "source": "unknown"
                    })


        total = data.get("TotalRecordCount", 0)
        start += len(items)
        if start >= total:
            break

    log.info(f"Anime show scan (Jellyfin): {scanned} shows, "
             f"{len(anidb_items)} in AniDB mapper, "
             f"{anidb_resolved} resolved, {anidb_not_mapped} not mapped")

    stats = {
        "scanned_items":  scanned,
        "indexed_tmdb":   len(plex_ids),
        "skipped_short":  0,
        "directors_kept": 0,
        "actors_kept":    0,
        "no_tmdb_guid":   len(no_tmdb_guid),
        "anidb_resolved": anidb_resolved,
        "anidb_not_mapped": anidb_not_mapped,
        "anidb_items":    anidb_items,
        "duplicates":     [],
    }
    return plex_ids, {}, {}, stats, no_tmdb_guid


