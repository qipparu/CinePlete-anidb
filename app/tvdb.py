import time
import requests
from app.logger import get_logger

log = get_logger(__name__)

class TVDB:
    def __init__(self, api_key: str):
        self.api_key = api_key
        self.base_url = "https://api4.thetvdb.com/v4"
        self.session = requests.Session()
        self.session.headers.update({"Accept": "application/json"})
        self._token = None
        self._token_expires = 0

    def _auth(self):
        if not self.api_key:
            return False
            
        if self._token and time.time() < self._token_expires:
            return True

        try:
            r = self.session.post(f"{self.base_url}/login", json={"apikey": self.api_key}, timeout=10)
            r.raise_for_status()
            data = r.json()
            if "data" in data and "token" in data["data"]:
                self._token = data["data"]["token"]
                self.session.headers.update({"Authorization": f"Bearer {self._token}"})
                self._token_expires = time.time() + 24 * 3600  # Refresh every 24h
                log.info("TVDB API authenticated successfully")
                return True
        except Exception as e:
            log.warning(f"Failed to authenticate with TVDB: {e}")
        return False

    def get(self, endpoint: str, params: dict = None) -> dict:
        if not self._auth():
            return {}
        try:
            r = self.session.get(f"{self.base_url}{endpoint}", params=params, timeout=10)
            if r.status_code == 404:
                return {}
            r.raise_for_status()
            return r.json().get("data", {})
        except requests.exceptions.RequestException as e:
            log.warning(f"TVDB API error on {endpoint}: {e}")
            return {}

    def series_extended(self, tvdb_id: int) -> dict:
        """
        Fetch extended series data which includes 'artworks' (all series & season images).
        """
        return self.get(f"/series/{tvdb_id}/extended", params={"meta": "artworks"})
