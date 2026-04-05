"""
Router for Shikimori / MyAnimeList integration.
"""
import os
from fastapi import APIRouter, Body
from app.config import load_config, save_config
from app.tmdb import TMDB
from app.routers._shared import log, read_results
from app.shikimori import (
    get_shikimori_mapper,
    load_shikimori_export,
    ShikimoriAnalyzer
)

router = APIRouter()

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

    # Use default export path if not specified
    export_path = shiki_cfg.get("SHIKIMORI_EXPORT_URL") or "shikimori/qipparu_animes.json"
    
    # Load export data
    export_items = load_shikimori_export(export_path)
    if not export_items:
        return {"ok": False, "error": f"Could not load Shikimori export from {export_path}"}

    # Load mapping
    mapper = get_shikimori_mapper()
    if not mapper._ready:
        mapper.load()

    # Get library snapshot
    results = read_results()
    if not results:
        return {"ok": False, "error": "No scan results found. Please run a full scan first."}

    # Analyze
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
