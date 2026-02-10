"""
ScaleInsights Scraper
Handles login and Excel file download from ScaleInsights web portal.
"""

import os
import time
import logging
import requests
from bs4 import BeautifulSoup
from typing import Optional

logger = logging.getLogger(__name__)

BASE_URL = "https://portal.scaleinsights.com"


class ScaleInsightsScraper:
    """
    Downloads keyword ranking Excel files from ScaleInsights.

    Usage:
        scraper = ScaleInsightsScraper(email, password)
        scraper.login()
        excel_bytes = scraper.download_rankings("US", "2026-01-01", "2026-02-01")
    """

    def __init__(self, email: str, password: str, base_url: str = BASE_URL):
        self.email = email
        self.password = password
        self.base_url = base_url
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": (
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/120.0.0.0 Safari/537.36"
            )
        })
        self._logged_in = False

    def login(self) -> bool:
        """
        Login to ScaleInsights web portal.

        Fetches login page, extracts CSRF/hidden fields, submits credentials.

        Returns:
            True if login successful

        Raises:
            Exception on login failure
        """
        login_url = f"{self.base_url}/Identity/Account/Login"
        logger.info(f"Logging into ScaleInsights as {self.email}...")

        # GET login page
        resp = self.session.get(login_url, timeout=30)
        resp.raise_for_status()

        # Parse hidden form fields (CSRF token etc.)
        soup = BeautifulSoup(resp.text, "html.parser")
        form = soup.find("form")
        if not form:
            raise Exception("Could not find login form on page")

        form_data = {}
        for hidden_input in form.find_all("input", {"type": "hidden"}):
            name = hidden_input.get("name")
            value = hidden_input.get("value", "")
            if name:
                form_data[name] = value

        # Add credentials
        form_data["Input.UserName"] = self.email
        form_data["Input.Password"] = self.password

        # Submit login
        post_resp = self.session.post(
            login_url,
            data=form_data,
            allow_redirects=True,
            timeout=30,
        )

        # Verify login succeeded — check we're not still on the login page
        if "/Identity/Account/Login" in post_resp.url and post_resp.status_code == 200:
            # Could still be on login page if credentials are wrong
            if "Invalid login attempt" in post_resp.text:
                raise Exception("ScaleInsights login failed: Invalid credentials")

        # Verify we can access a protected page
        check_resp = self.session.get(f"{self.base_url}/KeywordRanking", timeout=30, allow_redirects=False)
        if check_resp.status_code in (301, 302):
            redirect_location = check_resp.headers.get("Location", "")
            if "Login" in redirect_location:
                raise Exception("ScaleInsights login failed: Redirected back to login")

        self._logged_in = True
        logger.info("ScaleInsights login successful")
        return True

    def _ensure_logged_in(self):
        """Re-login if session expired."""
        if not self._logged_in:
            self.login()

    def download_rankings(
        self,
        country_code: str,
        from_date: str,
        to_date: str,
        max_retries: int = 3,
    ) -> bytes:
        """
        Download keyword ranking Excel file for a country.

        Args:
            country_code: ScaleInsights country code (US, CA, GB, DE, FR, AE, AU)
            from_date: Start date YYYY-MM-DD
            to_date: End date YYYY-MM-DD
            max_retries: Number of retry attempts

        Returns:
            Raw Excel file bytes

        Raises:
            Exception on download failure after all retries
        """
        self._ensure_logged_in()

        url = (
            f"{self.base_url}/KeywordRanking"
            f"?countrycode={country_code}&from={from_date}&to={to_date}&handler=Excel"
        )

        for attempt in range(1, max_retries + 1):
            try:
                logger.info(f"Downloading {country_code} rankings (attempt {attempt})...")
                resp = self.session.get(url, timeout=120, stream=True)

                # Check if we got redirected to login (session expired)
                if "Login" in resp.url or resp.status_code in (301, 302):
                    logger.warning("Session expired, re-logging in...")
                    self._logged_in = False
                    self.login()
                    resp = self.session.get(url, timeout=120, stream=True)

                resp.raise_for_status()

                # Verify we got an Excel file, not an HTML error page
                content_type = resp.headers.get("Content-Type", "")
                if "html" in content_type.lower():
                    raise Exception(
                        f"Got HTML response instead of Excel — "
                        f"likely login expired or invalid country code '{country_code}'"
                    )

                file_bytes = resp.content
                logger.info(f"Downloaded {country_code}: {len(file_bytes):,} bytes")
                return file_bytes

            except Exception as e:
                logger.warning(f"Download attempt {attempt} failed for {country_code}: {e}")
                if attempt < max_retries:
                    wait = 5 * attempt
                    logger.info(f"Retrying in {wait}s...")
                    time.sleep(wait)
                else:
                    raise Exception(
                        f"Failed to download {country_code} after {max_retries} attempts: {e}"
                    )
