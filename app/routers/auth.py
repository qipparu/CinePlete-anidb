"""
Auth + version routes.
  GET  /
  GET  /login
  GET  /api/auth/status
  POST /api/auth/login
  POST /api/auth/logout
  GET  /api/version
"""
import os
import time

import requests
from fastapi import APIRouter, Request, Response
from fastapi.responses import HTMLResponse, RedirectResponse

from app.config import load_config
from app.auth import (
    COOKIE_NAME, get_client_ip, is_local_address,
    hash_password, verify_password,
    create_token, verify_token, generate_secret_key,
)
from app.routers._shared import log

APP_VERSION = os.getenv("APP_VERSION", "dev")
STATIC_DIR  = os.getenv("STATIC_DIR",  "/app/static")
GITHUB_REPO = "sdblepas/CinePlete"

# Simple in-memory cache for GitHub release check (avoid hammering the API)
_release_cache: dict = {"checked_at": 0, "latest": None, "url": None}

router = APIRouter()


# --------------------------------------------------
# Static pages
# --------------------------------------------------

@router.get("/", response_class=HTMLResponse)
def index():
    with open(f"{STATIC_DIR}/index.html", "r", encoding="utf-8") as f:
        html = f.read()
    # Inject version into script URLs for automatic browser cache-busting
    # on every new deployment (browsers re-fetch JS when ?v= changes)
    return html.replace("__VERSION__", APP_VERSION)


@router.get("/login", response_class=HTMLResponse)
def login_page():
    with open(f"{STATIC_DIR}/login.html", "r", encoding="utf-8") as f:
        return f.read()


# --------------------------------------------------
# Auth API
# --------------------------------------------------

@router.get("/api/auth/status")
def api_auth_status(request: Request):
    """Returns current auth mode and whether the current request is authenticated."""
    cfg      = load_config()
    auth_cfg = cfg.get("AUTH", {})
    method   = auth_cfg.get("AUTH_METHOD", "None")
    has_user = bool(auth_cfg.get("AUTH_USERNAME") and auth_cfg.get("AUTH_PASSWORD_HASH"))

    authed = False
    if method == "None":
        authed = True
    elif method == "DisabledForLocalAddresses" and is_local_address(get_client_ip(request)):
        authed = True
    else:
        token  = request.cookies.get(COOKIE_NAME, "")
        secret = auth_cfg.get("AUTH_SECRET_KEY", "")
        authed = bool(token and secret and verify_token(token, secret))

    return {"method": method, "authenticated": authed, "has_user": has_user}


@router.post("/api/auth/login")
async def api_auth_login(request: Request, response: Response):
    body        = await request.json()
    username    = str(body.get("username", "")).strip()
    password    = str(body.get("password", ""))
    remember_me = bool(body.get("remember_me", False))

    cfg      = load_config()
    auth_cfg = cfg.get("AUTH", {})

    stored_user = auth_cfg.get("AUTH_USERNAME", "")
    stored_hash = auth_cfg.get("AUTH_PASSWORD_HASH", "")
    stored_salt = auth_cfg.get("AUTH_PASSWORD_SALT", "")
    secret      = auth_cfg.get("AUTH_SECRET_KEY", "")

    if not stored_user or not stored_hash:
        return {"ok": False, "error": "No user configured — set credentials in Config first"}

    if not secret:
        return {"ok": False, "error": "Auth not fully configured (missing secret key)"}

    if username != stored_user or not verify_password(password, stored_hash, stored_salt):
        log.warning(f"Auth failed for '{username}' from {get_client_ip(request)}")
        return {"ok": False, "error": "Invalid username or password"}

    token   = create_token(username, remember_me, secret)
    max_age = 30 * 86_400 if remember_me else 86_400
    response.set_cookie(
        COOKIE_NAME, token,
        max_age=max_age, httponly=True, samesite="lax", path="/",
    )
    log.info(f"Auth success for '{username}' from {get_client_ip(request)}")
    return {"ok": True}


@router.post("/api/auth/logout")
def api_auth_logout(response: Response):
    response.delete_cookie(COOKIE_NAME, path="/")
    return {"ok": True}


# --------------------------------------------------
# Version
# --------------------------------------------------

def _get_latest_release() -> dict:
    """Check GitHub for the latest release, cached for 1 hour."""
    now = time.time()
    if now - _release_cache["checked_at"] < 3600:
        return _release_cache
    try:
        r = requests.get(
            f"https://api.github.com/repos/{GITHUB_REPO}/releases/latest",
            headers={"Accept": "application/vnd.github+json"},
            timeout=5,
        )
        if r.status_code == 200:
            data = r.json()
            _release_cache["latest"] = data.get("tag_name", "").lstrip("v")
            _release_cache["url"]    = data.get("html_url", "")
    except Exception:
        pass
    _release_cache["checked_at"] = now
    return _release_cache


def _parse_ver(v: str):
    """Parse a semver string into a comparable tuple, e.g. '2.4.0' → (2, 4, 0)."""
    try:
        return tuple(int(x) for x in v.split("."))
    except Exception:
        return (0, 0, 0)


@router.get("/api/version")
def api_version():
    cache   = _get_latest_release()
    current = APP_VERSION.lstrip("v")
    latest  = cache.get("latest")
    # Only show update banner when latest is strictly NEWER than current.
    has_update = bool(
        latest
        and current not in ("dev", "e2e")
        and _parse_ver(latest) > _parse_ver(current)
    )
    return {
        "version":     APP_VERSION,
        "latest":      latest,
        "has_update":  has_update,
        "release_url": cache.get("url") if has_update else None,
    }
