"""
TMDB cache management routes.
  GET  /api/cache/info
  POST /api/cache/backup
  POST /api/cache/restore
  GET  /api/cache/backup/info
  POST /api/cache/clear
"""
import os
from datetime import datetime

from fastapi import APIRouter

from app.routers._shared import log, DATA_DIR

router = APIRouter()


@router.get("/api/cache/info")
def api_cache_info():
    """Return TMDB cache file age and size."""
    cache_file = f"{DATA_DIR}/tmdb_cache.json"
    try:
        stat    = os.stat(cache_file)
        age_s   = int(datetime.utcnow().timestamp() - stat.st_mtime)
        size_mb = round(stat.st_size / 1024 / 1024, 1)
        return {"exists": True, "age_seconds": age_s, "size_mb": size_mb}
    except FileNotFoundError:
        return {"exists": False, "age_seconds": None, "size_mb": 0}


@router.post("/api/cache/backup")
def api_cache_backup():
    """Copy tmdb_cache.json → tmdb_cache.backup.json"""
    import shutil
    cache_file  = f"{DATA_DIR}/tmdb_cache.json"
    backup_file = f"{DATA_DIR}/tmdb_cache.backup.json"
    try:
        if not os.path.exists(cache_file):
            return {"ok": False, "error": "No cache file to back up"}
        shutil.copy2(cache_file, backup_file)
        stat    = os.stat(backup_file)
        size_mb = round(stat.st_size / 1024 / 1024, 1)
        log.info(f"TMDB cache backed up ({size_mb} MB)")
        return {"ok": True, "size_mb": size_mb}
    except Exception as e:
        log.error(f"Cache backup failed: {e}")
        return {"ok": False, "error": str(e)}


@router.post("/api/cache/restore")
def api_cache_restore():
    """Copy tmdb_cache.backup.json → tmdb_cache.json"""
    import shutil
    cache_file  = f"{DATA_DIR}/tmdb_cache.json"
    backup_file = f"{DATA_DIR}/tmdb_cache.backup.json"
    try:
        if not os.path.exists(backup_file):
            return {"ok": False, "error": "No backup file found"}
        shutil.copy2(backup_file, cache_file)
        stat    = os.stat(cache_file)
        size_mb = round(stat.st_size / 1024 / 1024, 1)
        log.info(f"TMDB cache restored from backup ({size_mb} MB)")
        return {"ok": True, "size_mb": size_mb}
    except Exception as e:
        log.error(f"Cache restore failed: {e}")
        return {"ok": False, "error": str(e)}


@router.get("/api/cache/backup/info")
def api_cache_backup_info():
    """Return backup file age and size if it exists."""
    backup_file = f"{DATA_DIR}/tmdb_cache.backup.json"
    try:
        stat    = os.stat(backup_file)
        age_s   = int(datetime.utcnow().timestamp() - stat.st_mtime)
        size_mb = round(stat.st_size / 1024 / 1024, 1)
        return {"exists": True, "age_seconds": age_s, "size_mb": size_mb}
    except FileNotFoundError:
        return {"exists": False}


@router.post("/api/cache/clear")
def api_cache_clear():
    """Delete the TMDB cache file."""
    cache_file = f"{DATA_DIR}/tmdb_cache.json"
    try:
        os.remove(cache_file)
        log.info("TMDB cache cleared by user")
        return {"ok": True}
    except FileNotFoundError:
        return {"ok": True, "message": "Cache was already empty"}
    except Exception as e:
        log.error(f"Could not clear cache: {e}")
        return {"ok": False, "error": str(e)}
