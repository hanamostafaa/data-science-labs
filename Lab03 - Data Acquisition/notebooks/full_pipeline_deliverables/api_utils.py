import time
import requests
from datetime import datetime
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


# =========================================================
# RATE LIMIT CHECK
# =========================================================

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

        print(f"⏰ Rate limit reached. Waiting {wait_seconds} seconds...")

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
                print(f"⏰ Sleeping {sleep_time:.1f}s due to rate limit")
                time.sleep(sleep_time)

            self.requests = []

        self.requests.append(now)


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

    def __init__(self, token=None):

        self.base_url = "https://api.github.com"

        self.session = create_retry_session()

        self.rate_limiter = RateLimiter(
            max_requests=60,
            time_window=3600
        )

        self.session.headers.update({
            "Accept": "application/vnd.github+json",
            "User-Agent": "MarketIntelligencePipeline"
        })

        if token:
            self.session.headers.update({
                "Authorization": f"Bearer {token}"
            })

    def get(self, endpoint, params=None):

        self.rate_limiter.wait_if_needed()
    
        url = f"{self.base_url}/{endpoint.lstrip('/')}"
    
        response = self.session.get(url, params=params, timeout=10)
    
        check_rate_limit(response)
    
        response.raise_for_status()
    
        return response.json()