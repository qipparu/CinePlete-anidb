"""
Shared constants and utilities used by all routers.
"""
import ipaddress
import json
import os
from urllib.parse import urlparse

from app.logger import get_logger

DATA_DIR              = os.getenv("DATA_DIR", "/data")
RESULTS_FILE          = f"{DATA_DIR}/results.json"
OVERRIDES_FILE        = f"{DATA_DIR}/overrides.json"
LOG_FILE              = f"{DATA_DIR}/cineplete.log"
LETTERBOXD_CACHE_FILE = f"{DATA_DIR}/letterboxd_cache.json"

log = get_logger(__name__)


def read_results() -> dict | None:
    try:
        with open(RESULTS_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return None


def _parse_tmdb_id(value) -> int | None:
    """Return int TMDB ID or None if value is missing / non-numeric."""
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _validate_url_for_fetch(url: str) -> str | None:
    """
    Return an error string if url should not be fetched (SSRF guard).
    Returns None when the URL is safe.
    Allows only http/https to public IP addresses.
    """
    try:
        parsed = urlparse(url)
        if parsed.scheme not in ("http", "https"):
            return "Only http/https URLs are allowed"
        hostname = parsed.hostname or ""
        if not hostname:
            return "Missing hostname"
        try:
            addr = ipaddress.ip_address(hostname)
            if addr.is_private or addr.is_loopback or addr.is_link_local or addr.is_reserved:
                return "Private/internal IP addresses are not allowed"
        except ValueError:
            # hostname is a domain name — check for localhost explicitly
            if hostname.lower() in ("localhost", "localhost.localdomain"):
                return "localhost is not allowed"
        return None
    except Exception:
        return "Invalid URL"
