import requests
import defusedxml.ElementTree as ET
from collections import defaultdict

from app.config import load_config
from app.logger import get_logger

log = get_logger(__name__)


def _build_lib_cfg(lib_cfg):
    """Resolve lib_cfg — if None, fall back to legacy PLEX config section."""
    if lib_cfg is not None:
        return lib_cfg
    cfg = load_config()
    plex = cfg["PLEX"]
    return {
        "url": plex["PLEX_URL"],
        "token": plex["PLEX_TOKEN"],
        "library_name": plex["LIBRARY_NAME"],
        "page_size": int(plex.get("PLEX_PAGE_SIZE", 500)),
        "short_movie_limit": int(plex.get("SHORT_MOVIE_LIMIT", 60)),
    }


def plex_get(path, lib_cfg=None, params=None):
    lc = _build_lib_cfg(lib_cfg)
    if params is None:
        params = {}
    params["X-Plex-Token"] = lc["token"]
    r = requests.get(lc["url"].rstrip("/") + path, params=params, timeout=30)
    r.raise_for_status()
    return r.text


def library_key(lib_cfg=None):
    lc = _build_lib_cfg(lib_cfg)
    xml = plex_get("/library/sections", lc)
    root = ET.fromstring(xml)
    for d in root.findall("Directory"):
        if d.attrib.get("title") == lc["library_name"]:
            return d.attrib.get("key")
    raise RuntimeError(f"Plex library '{lc['library_name']}' not found on {lc['url']}")


def _extract_guids(element) -> dict:
    """
    Extract tmdb_id, anidb_id, tvdb_id from a Plex XML element.

    Supports TWO formats produced by different Plex agents:

    1. Modern (new Plex TV/Movie agent) — child <Guid> elements:
           <Guid id="tmdb://12345"/>
           <Guid id="anidb://11370"/>
           <Guid id="tvdb://78914"/>

    2. Legacy HAMA agent — guid attribute on the element itself:
           guid="com.plexapp.agents.hama://anidb-11370?lang=en"
       HAMA also supports tvdb: scheme inside the value.

    Returns a dict with nullable keys: tmdb_id (int|None), anidb_id (int|None), tvdb_id (int|None).
    """
    tmdb_id: int | None = None
    anidb_id: int | None = None
    tvdb_id:  int | None = None

    # ── Modern: child <Guid id="..."> elements ──────────────────────────────
    for g in element.findall("Guid"):
        gid = g.attrib.get("id", "")
        if gid.startswith("tmdb://"):
            try:
                tmdb_id = int(gid[7:])
            except ValueError:
                pass
        elif gid.startswith("anidb://"):
            try:
                anidb_id = int(gid[8:])
            except ValueError:
                pass
        elif gid.startswith("tvdb://"):
            try:
                tvdb_id = int(gid[7:])
            except ValueError:
                pass

    # ── Legacy HAMA agent: guid attribute ───────────────────────────────────
    # Format examples:
    #   com.plexapp.agents.hama://anidb-11370?lang=en
    #   com.plexapp.agents.hama://tvdb-78914/1/1?lang=en
    raw_guid = element.attrib.get("guid", "")
    if not any([tmdb_id, anidb_id, tvdb_id]) and "hama://" in raw_guid:
        # Strip query string and scheme
        # Format: com.plexapp.agents.hama://anidb-11370?lang=en
        #         com.plexapp.agents.hama://tvdb-78914/1/1?lang=en
        #         com.plexapp.agents.hama://tvdb2-78914/1/1?lang=en  (absolute numbering)
        #         com.plexapp.agents.hama://tvdb6-78914?lang=en      (specials mapping)
        hama_part = raw_guid.split("hama://")[1].split("?")[0]   # "anidb-11370" / "tvdb2-78914/1/1"
        # Split on first "-" to get source type and the rest
        hama_type, _, hama_rest = hama_part.partition("-")
        hama_id_str = hama_rest.split("/")[0]   # take only series-level ID
        if hama_id_str.isdigit():
            # anidb, anidb2, anidb3… all map to anidb_id
            if hama_type.startswith("anidb"):
                anidb_id = int(hama_id_str)
            # tvdb, tvdb2, tvdb3, tvdb4, tvdb6… all map to tvdb_id
            elif hama_type.startswith("tvdb"):
                tvdb_id = int(hama_id_str)
            elif hama_type == "tmdb":
                tmdb_id = int(hama_id_str)

    return {"tmdb_id": tmdb_id, "anidb_id": anidb_id, "tvdb_id": tvdb_id}



def scan_movies(lib_cfg=None):
    """
    Scan a Plex movie library (type=1).
    Falls back to AniDB→TMDB mapping when anidb:// GUID is present
    but no tmdb:// GUID is found.

    Returns: (plex_ids, directors, actors, stats, no_tmdb_guid)
    stats["anidb_items"] is an empty list here (movies don't have show-level season data).
    """
    lc = _build_lib_cfg(lib_cfg)
    short_movie_limit = int(lc.get("short_movie_limit", 60))
    page_size = int(lc.get("page_size", 500))
    key = library_key(lc)

    # Lazy-load the mapper only when we actually encounter an anidb:// GUID
    _mapper = None
    def get_mapper():
        nonlocal _mapper
        if _mapper is None:
            from app.anidb_mapping import get_mapper as _gm
            _mapper = _gm()
        return _mapper

    plex_ids = {}
    plex_editions = {}
    tmdb_id_dupes = {}
    directors = defaultdict(set)
    actors = defaultdict(set)
    no_tmdb_guid = []
    anidb_items = []   # kept as empty for movie libraries (no season tracking)
    start = 0
    scanned = 0
    skipped_short = 0
    anidb_resolved = 0
    anidb_not_mapped = 0

    while True:
        xml = plex_get(
            f"/library/sections/{key}/all",
            lc,
            {
                "type": "1",
                "includeGuids": "1",
                "X-Plex-Container-Start": start,
                "X-Plex-Container-Size": page_size,
            },
        )
        root = ET.fromstring(xml)
        videos = root.findall("Video")
        if not videos:
            break
        for v in videos:
            scanned += 1
            title = v.attrib.get("title")
            year = v.attrib.get("year")
            duration_ms = v.attrib.get("duration")
            duration_min = int(duration_ms) / 60000 if duration_ms else None
            if duration_min is not None and duration_min < short_movie_limit:
                skipped_short += 1
                continue

            guids     = _extract_guids(v)
            tmdb_id   = guids["tmdb_id"]
            anidb_raw = str(guids["anidb_id"]) if guids["anidb_id"] else None

            # AniDB fallback for movie libraries
            if not tmdb_id and anidb_raw and anidb_raw.isdigit():
                entry = get_mapper().lookup(int(anidb_raw))
                if entry and entry.tmdb_id:
                    tmdb_id = entry.tmdb_id
                    anidb_resolved += 1
                    log.debug(f"AniDB→TMDB: anidb/{anidb_raw} → tmdb/{tmdb_id} ({title})")
                else:
                    anidb_not_mapped += 1
                    no_tmdb_guid.append({"title": title, "year": year, "source": "anidb"})
                    continue

            if not tmdb_id:
                no_tmdb_guid.append({"title": title, "year": year})
                continue

            edition = v.attrib.get("editionTitle", "")
            if tmdb_id in plex_ids:
                if tmdb_id not in tmdb_id_dupes:
                    tmdb_id_dupes[tmdb_id] = [{"title": plex_ids[tmdb_id], "edition": plex_editions.get(tmdb_id, "")}]
                tmdb_id_dupes[tmdb_id].append({"title": title, "edition": edition})
            else:
                plex_ids[tmdb_id] = title
                plex_editions[tmdb_id] = edition
            for d in v.findall("Director"):
                tag = d.attrib.get("tag")
                if tag:
                    directors[tag].add(tmdb_id)
            for r in v.findall("Role"):
                tag = r.attrib.get("tag")
                if tag:
                    actors[tag].add(tmdb_id)
        start += len(videos)

    directors = {k: v for k, v in directors.items() if len(v) > 1}
    actors = {k: v for k, v in actors.items() if len(v) > 1}

    if anidb_resolved:
        log.info(f"AniDB→TMDB resolved: {anidb_resolved} movies, "
                 f"not mapped: {anidb_not_mapped}")

    stats = {
        "scanned_items": scanned,
        "indexed_tmdb": len(plex_ids),
        "skipped_short": skipped_short,
        "directors_kept": len(directors),
        "actors_kept": len(actors),
        "no_tmdb_guid": len(no_tmdb_guid),
        "anidb_resolved": anidb_resolved,
        "anidb_not_mapped": anidb_not_mapped,
        "anidb_items": anidb_items,   # empty for movie libraries
        "duplicates": [
            {"tmdb": tmdb_id, "titles": titles}
            for tmdb_id, titles in tmdb_id_dupes.items()
        ],
    }
    return plex_ids, directors, actors, stats, no_tmdb_guid


def scan_shows(lib_cfg=None):
    """
    Scan a Plex TV show library (type=2) — intended for HAMA-tagged anime libraries.

    Collects show-level GUIDs (anidb://, tvdb://, tmdb://) and resolves them via
    the AniDB mapper. Items with a resolved TMDB ID join plex_ids so they participate
    in the existing suggestions/director/actor pipeline where applicable.

    stats["anidb_items"] contains full MappingEntry dicts for ALL anidb-tagged shows —
    this is the data consumed by scanner._analyze_anime_seasons().

    Returns: (plex_ids, directors={}, actors={}, stats, no_tmdb_guid)
    """
    lc = _build_lib_cfg(lib_cfg)
    page_size = int(lc.get("page_size", 500))
    key = library_key(lc)

    from app.anidb_mapping import get_mapper
    mapper = get_mapper()

    plex_ids: dict[int, str] = {}         # {tmdb_id: title}
    no_tmdb_guid: list[dict] = []
    anidb_items: list[dict]  = []         # all resolvable anidb entries
    start     = 0
    scanned   = 0
    anidb_resolved   = 0
    anidb_not_mapped = 0

    while True:
        xml = plex_get(
            f"/library/sections/{key}/all",
            lc,
            {
                "type": "2",           # 2 = Show
                "includeGuids": "1",
                "X-Plex-Container-Start": start,
                "X-Plex-Container-Size": page_size,
            },
        )
        root = ET.fromstring(xml)
        shows = root.findall("Directory")
        if not shows:
            break

        for show in shows:
            scanned += 1
            title = show.attrib.get("title", "")
            year  = show.attrib.get("year", "")

            guids     = _extract_guids(show)
            tmdb_id   = guids["tmdb_id"]
            anidb_raw = str(guids["anidb_id"]) if guids["anidb_id"] else None
            tvdb_raw  = str(guids["tvdb_id"])  if guids["tvdb_id"]  else None

            # Resolve AniDB → tmdb & tvdb via mapper
            if anidb_raw and anidb_raw.isdigit():
                entry = mapper.lookup(int(anidb_raw))
                if entry:
                    # Prefer explicit tmdb:// GUID from Plex, fall back to mapping
                    if not tmdb_id and entry.tmdb_id:
                        tmdb_id = entry.tmdb_id
                        anidb_resolved += 1
                        log.debug(f"AniDB→TMDB: anidb/{anidb_raw} → tmdb/{tmdb_id} ({title})")
                    # Record this show for season-tracking regardless
                    anidb_items.append(entry.as_dict())
                else:
                    anidb_not_mapped += 1
                    log.debug(f"AniDB not mapped: anidb/{anidb_raw} ({title})")

            if tmdb_id:
                if tmdb_id not in plex_ids:
                    plex_ids[tmdb_id] = title
            else:
                # If it's an anime that mapped to anidb/tvdb successfully, don't flag as missing
                if not (anidb_raw and anidb_raw.isdigit() and entry):
                    no_tmdb_guid.append({"title": title, "year": year, "source": "anidb" if anidb_raw else "unknown"})


        start += len(shows)

    if anidb_resolved or anidb_not_mapped:
        log.info(f"Anime show scan: {scanned} shows, "
                 f"{len(anidb_items)} in AniDB mapper, "
                 f"{anidb_resolved} resolved to TMDB, "
                 f"{anidb_not_mapped} not mapped")

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
    # directors/actors empty — TV show libraries don't have useful director metadata
    return plex_ids, {}, {}, stats, no_tmdb_guid
