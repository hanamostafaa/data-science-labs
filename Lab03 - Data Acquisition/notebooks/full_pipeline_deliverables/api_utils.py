import time
import requests
from datetime import datetime
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


# =========================================================
# RATE LIMIT CHECK
# =========================================================

def check_rate_limit_logged(response, logger):
    if "X-RateLimit-Limit" not in response.headers:
        return

    limit = int(response.headers["X-RateLimit-Limit"])
    remaining = int(response.headers["X-RateLimit-Remaining"])
    reset_timestamp = int(response.headers["X-RateLimit-Reset"])
    reset_time = datetime.fromtimestamp(reset_timestamp)

    logger.info(f"Rate Limit: {remaining}/{limit}, resets at {reset_time}")

    if remaining == 0:
        wait_seconds = max(reset_timestamp - int(time.time()), 0)
        logger.warning(f"Rate limit reached. Sleeping {wait_seconds}s...")
        time.sleep(wait_seconds + 1)

def check_rate_limit(response):
    """
    Check GitHub rate limit and sleep if necessary.
    """

    if "X-RateLimit-Limit" not in response.headers:
        return

    limit = int(response.headers["X-RateLimit-Limit"])
    remaining = int(response.headers["X-RateLimit-Remaining"])
    reset_timestamp = int(response.headers["X-RateLimit-Reset"])

    now = int(time.time())
    reset_time = datetime.fromtimestamp(reset_timestamp)

    print(f"Rate Limit: {remaining}/{limit}")
    print(f"Reset Time: {reset_time}")

    if remaining == 0:
        wait_seconds = max(reset_timestamp - now, 0)

        print(f"Rate limit reached. Waiting {wait_seconds} seconds...")

        time.sleep(wait_seconds + 1)


# =========================================================
# RATE LIMITER
# =========================================================

class RateLimiter:
    """
    Sliding window rate limiter.
    """

    def __init__(self, max_requests=60, time_window=3600):
        self.max_requests = max_requests
        self.time_window = time_window
        self.requests = []

    def wait_if_needed(self):
        now = time.time()

        # remove old requests
        self.requests = [
            r for r in self.requests
            if now - r < self.time_window
        ]

        if len(self.requests) >= self.max_requests:
            oldest = self.requests[0]
            sleep_time = self.time_window - (now - oldest)

            if sleep_time > 0:
                print(f"Sleeping {sleep_time:.1f}s due to rate limit")
                time.sleep(sleep_time)

            self.requests = []

        self.requests.append(now)

# =========================================================
# ERROR HANDLING 
# =========================================================

def fetch_with_error_handling(session, url, params=None, logger=None, max_retries=3):
    """
    Robust GET request with retries and error handling.
    Logs messages if a logger is provided.
    """
    for attempt in range(max_retries):
        try:
            response = session.get(url, params=params, timeout=10)

            # Rate limit check
            if logger:
                check_rate_limit_logged(response, logger)
            else:
                check_rate_limit(response)

            # Success
            if response.status_code == 200:
                return response.json()

            elif response.status_code == 404:
                if logger:
                    logger.error(f"Not Found: {url}")
                return None

            elif response.status_code == 429:
                retry_after = int(response.headers.get("Retry-After", 60))
                msg = f"Rate limited. Waiting {retry_after}s..."
                if logger: logger.warning(msg)
                else: print(msg)
                time.sleep(retry_after)
                continue

            elif response.status_code >= 500:
                msg = f"Server error {response.status_code}, retrying..."
                if logger: logger.warning(msg)
                else: print(msg)
                time.sleep(2**attempt)
                continue

            else:
                msg = f"HTTP {response.status_code}: {url}"
                if logger: logger.error(msg)
                else: print(msg)
                response.raise_for_status()

        except requests.exceptions.Timeout:
            msg = f"Timeout on attempt {attempt + 1}, retrying..."
            if logger: logger.warning(msg)
            else: print(msg)
            time.sleep(2**attempt)

        except requests.exceptions.ConnectionError as e:
            msg = f"Connection error: {e}"
            if logger: logger.error(msg)
            else: print(msg)
            time.sleep(2**attempt)

        except Exception as e:
            msg = f"Unexpected error: {e}"
            if logger: logger.error(msg)
            else: print(msg)
            return None

    if logger:
        logger.error(f"Failed after {max_retries} attempts: {url}")
    return None

# =========================================================
# RETRY SESSION
# =========================================================

def create_retry_session():

    retry_strategy = Retry(
        total=5,
        backoff_factor=1,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"]
    )

    adapter = HTTPAdapter(max_retries=retry_strategy)

    session = requests.Session()
    session.mount("https://", adapter)
    session.mount("http://", adapter)

    return session


# =========================================================
# API CLIENT
# =========================================================

class GitHubAPIClient:
    """
    Reusable GitHub API client with:
    - rate limiting
    - retries
    - logging friendly design
    """

    def __init__(self,logger=None, token=None):

        self.base_url = "https://api.github.com"

        self.session = create_retry_session()
        self.logger = logger

        self.rate_limiter = RateLimiter(
            max_requests=60,
            time_window=3600
        )

        self.session.headers.update({
            "Accept": "application/vnd.github+json",
            "User-Agent": "Market-Intelligence-Pipeline/1.0"
        })

        if token:
            self.session.headers.update({
                "Authorization": f"Bearer {token}"
            })

    def get(self, endpoint, params=None):

        self.rate_limiter.wait_if_needed()
    
        url = f"{self.base_url}/{endpoint.lstrip('/')}"
    
        return fetch_with_error_handling(self.session, url, params=params,logger=self.logger)
    
    def get_paginated(self, endpoint, params=None, max_pages=5):
        """
        Fetch paginated results from GitHub API.

        Args:
            endpoint (str): API endpoint
            params (dict): query parameters
            max_pages (int): maximum pages to fetch

        Returns:
            list: combined results from all pages
        """

        all_results = []
        page = 1

        params = params or {}

        while page <= max_pages:

            print(f"Fetching page {page}...")

            params["page"] = page
            params["per_page"] = 5

            data = self.get(endpoint, params=params)

            if not data:
                print("No more results.")
                break

            if isinstance(data, dict) and "items" in data:
                results = data["items"]
            else:
                results = data

            if not results:
                break

            all_results.extend(results)

            page += 1

        return all_results