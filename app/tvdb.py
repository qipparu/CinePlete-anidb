import time
import requests
from app.logger import get_logger

log = get_logger(__name__)

class TVDB:
    def __init__(self, api_key: str):
        self.api_key = api_key
        self.base_url = "https://api.thetvdb.com"
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
            if "token" in data:
                self._token = data["token"]
                self.session.headers.update({"Authorization": f"Bearer {self._token}"})
                self._token_expires = time.time() + 24 * 3600  # Refresh every 24h
                log.info("TVDB v2 API authenticated successfully")
                return True
        except Exception as e:
            log.warning(f"Failed to authenticate with TVDB v2: {e}")
        return False

    def get(self, endpoint: str, lang: str = "en") -> dict:
        if not self._auth():
            return {}
        try:
            headers = {"Accept-Language": lang}
            r = self.session.get(f"{self.base_url}{endpoint}", headers=headers, timeout=10)
            if r.status_code == 404:
                return {}
            r.raise_for_status()
            return r.json().get("data", [])
        except requests.exceptions.RequestException as e:
            log.warning(f"TVDB API error on {endpoint}: {e}")
            return {}

    def season_images(self, tvdb_id: int, lang: str = "en") -> list:
        """
        Fetch all season artworks for the given TVDB ID natively resolving via the v2 API, 
        passing the correct language headers so TVDB returns localized graphics.
        """
        return self.get(f"/series/{tvdb_id}/images/query?keyType=season", lang=lang)
