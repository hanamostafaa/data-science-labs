import pandas as pd
import requests
import time
import matplotlib.pyplot as plt
from datetime import datetime, timedelta
import logging
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

###### Helpers 
def check_rate_limit(response):
    """
    Check rate limit info from response headers.
    GitHub includes rate limit details in every response.
    """
    if 'X-RateLimit-Limit' in response.headers:
        limit = int(response.headers['X-RateLimit-Limit'])
        remaining = int(response.headers['X-RateLimit-Remaining'])
        reset_timestamp = int(response.headers['X-RateLimit-Reset'])
        reset_time = datetime.fromtimestamp(reset_timestamp)

        print(f"Rate Limit: {remaining}/{limit}")
        print(f"Resets at: {reset_time}")

        # Warn when running low on available requests
        if remaining < 10:
            print("⚠️ Warning: Low on API requests!")

        return remaining
    return None

class RateLimiter:
    """
    Smart rate limiter that tracks API usage.
    Uses a sliding time window to count recent requests.
    """

    def __init__(self, max_requests=60, time_window=3600):
        """
        Args:
            max_requests: Maximum requests allowed in the time window
            time_window: Time window in seconds (3600 = 1 hour)
        """
        self.max_requests = max_requests
        self.time_window = time_window
        self.requests = []  # List of timestamps of past requests

    def wait_if_needed(self):
        """Wait if we've hit the rate limit before making a new request."""
        now = time.time()

        # Remove old timestamps outside the sliding time window
        self.requests = [
            req_time for req_time in self.requests if now - req_time < self.time_window
        ]

        # If we've used up our quota, sleep until the oldest request expires
        if len(self.requests) >= self.max_requests:
            oldest_request = self.requests[0]
            sleep_time = self.time_window - (now - oldest_request)
            if sleep_time > 0:
                print(
                    f"⏰ Rate limit reached. Sleeping for {sleep_time:.1f} seconds..."
                )
                time.sleep(sleep_time)
            self.requests = []  # Clear after sleeping

        # Record the timestamp of this new request
        self.requests.append(now)

# -----------------------------
# Setup Logging
# -----------------------------
logging.basicConfig(
    filename="api_requests.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

logger = logging.getLogger("GitHubAnalysis")

# -----------------------------
# Task 1: Repository Information
# -----------------------------
def task1_fetch_repos():
    """
    Fetch repository information for major ML frameworks.
    Returns a DataFrame with key metrics.
    """
    repos = ['tensorflow/tensorflow', 'pytorch/pytorch', 'scikit-learn/scikit-learn']
    all_repos = []
    for repo in repos:
        url = f'https://api.github.com/repos/{repo}'
        try:
            response = requests.get(url)
            if response.status_code == 200:
                data = response.json()
                dic = {
                    'name': data['name'],
                    'stars': data['stargazers_count'],
                    'forks': data['forks_count'],
                    'language': data['language'],
                    'open_issues': data['open_issues_count'],
                    'created_date': data['created_at'],
                }
                all_repos.append(dic)
                logger.info(f"Fetched repo: {repo}")
            else:
                logger.error(f"Failed to fetch {repo}: {response.status_code}")
        except Exception as e:
            logger.error(f"Exception fetching {repo}: {e}")
    df = pd.DataFrame(all_repos)
    df.to_csv('task1_github.csv', index=False)
    return df

def task1_calculate_metrics():
    """Calculate age, stars/day, and issues per star for repos"""
    df = pd.read_csv('task1_github.csv')
    df['created_date'] = pd.to_datetime(df['created_date'])
    today = pd.Timestamp.now(tz='UTC')
    df['age_days'] = (today - df['created_date']).dt.days
    df['stars_per_day'] = df['stars'] / df['age_days']
    df['issues_per_star'] = df['open_issues'] / df['stars']
    df.to_csv('task1_metrics.csv', index=False)
    logger.info("Calculated repo metrics and saved task1_metrics.csv")
    return df

def task1_visualization():
    """Visualize stars and forks for the repositories"""
    df = pd.read_csv('task1_metrics.csv')
    plt.figure(figsize=(10,6))
    x = df['name']
    plt.bar(x, df['stars'], alpha=0.6, label='Stars')
    plt.bar(x, df['forks'], alpha=0.6, label='Forks')
    plt.title("GitHub Repository Comparison")
    plt.xlabel("Repository")
    plt.ylabel("Metric Value")
    plt.legend()
    plt.tight_layout()
    plt.savefig("task1_comparison.png")
    plt.show()
    logger.info("task1_comparison.png saved successfully ✓")

# -----------------------------
# Task 2: User Repository Analysis
# -----------------------------
def fetch_user_repos_paginated(username):
    """
    Fetch all repositories for a user with pagination.

    Args:
        username: GitHub username

    Returns:
        list: All repositories
    """
    all_repos = []
    page = 1
    while True:
        logger.info(f"Fetching page {page} for user {username}...")
        params = {'page': page, 'per_page': 100}
        url = f'https://api.github.com/users/{username}/repos'
        try:
            response = requests.get(url, params=params)
            if response.status_code != 200:
                logger.error(f"Error fetching page {page}: {response.status_code}")
                break
            data = response.json()
            if not data or len(data) == 0:
                logger.info("No more results!")
                break
            for repo in data:
                all_repos.append({
                    "name": repo["name"],
                    "stars": repo["stargazers_count"],
                    "forks": repo["forks_count"],
                    "language": repo["language"],
                    "created_at": repo["created_at"],
                    "open_issues": repo["open_issues_count"]
                })
            page += 1
            time.sleep(1)
        except Exception as e:
            logger.error(f"Exception occurred: {e}")
            break
    df = pd.DataFrame(all_repos)
    df.to_csv("task2_all_repos.csv", index=False)
    logger.info(f"Fetched all repos for user {username}, saved to task2_all_repos.csv")
    return df

def analyze_user_repos():
    """Analyze user repositories and generate summary report"""
    df = pd.read_csv("task2_all_repos.csv")
    if df.empty:
        logger.warning("No data available for analysis.")
        return
    df["created_at"] = pd.to_datetime(df["created_at"])
    if "updated_at" in df.columns:
        df["updated_at"] = pd.to_datetime(df["updated_at"])
    else:
        df["updated_at"] = df["created_at"]
    most_used_language = df["language"].value_counts().idxmax()
    avg_stars = df["stars"].mean()
    total_forks = df["forks"].sum()
    most_recent_repo = df.loc[df["updated_at"].idxmax(), "name"]
    oldest_repo = df.loc[df["created_at"].idxmin(), "name"]
    report = f"""
GitHub Repository Analysis Report
==================================

Total Repositories: {len(df)}

Most Used Programming Language: {most_used_language}
Average Stars per Repository: {avg_stars:.2f}
Total Forks Across All Repositories: {total_forks}

Most Recently Updated Repository: {most_recent_repo}
Oldest Repository: {oldest_repo}
"""
    with open("task2_analysis.txt", "w", encoding="utf-8") as f:
        f.write(report)
    logger.info("Analysis complete. Summary saved to task2_analysis.txt")

# -----------------------------
# Task 3: GitHubAnalyzer Class
# -----------------------------
class GitHubAnalyzer:
    """
    Complete GitHub API client with analysis capabilities.
    Build on top of the GitHubAPI class concepts from section 2.7.
    """
    def __init__(self, token=None):
        self.logger = logger
        self.base_url = 'https://api.github.com'
        self.session = self._create_session()
        self.rate_limiter = RateLimiter(
            max_requests=5000, time_window=3600
        ) 

        if token:
            self.session.headers.update({'Authorization': f'Bearer {token}'})

        self.session.headers.update({
            'Accept': 'application/vnd.github.v3+json',
            'User-Agent': 'Library-Tutorial/1.0',
        })

    def _create_session(self):
        """Create session with retry logic (private method)."""
        session = requests.Session()
        retry_strategy = Retry(
            total=5, backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET"]
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        return session

    def get(self, endpoint, params=None):
        """Make GET request with rate limiting."""
        self.rate_limiter.wait_if_needed()  # Respect rate limits
        url = f"{self.base_url}/{endpoint.lstrip('/')}"

        try:
            response = self.session.get(url, params=params, timeout=10)
            response.raise_for_status()  # Raises exception for 4xx/5xx
            self.logger.info(f"GET {endpoint} - Status: {response.status_code}")

            # Peek at remaining rate limit with each response
            remaining = check_rate_limit(response)
            return response.json()

        except Exception as e:
            self.logger.error(f"Error fetching {endpoint}: {e}")
            raise

    def search_repos(self, query, language=None, min_stars=0):
        """
        Search repositories with filters.

        Returns:
            DataFrame with results
        """
        q_parts = [query]
        if language: q_parts.append(f"language:{language}")
        if min_stars: q_parts.append(f"stars:>={min_stars}")
        q = ' '.join(q_parts)
        results = self.get('/search/repositories', params={"q": q, "sort": "stars", "order": "desc", "per_page": 30})
        items = results.get("items", [])
        data = [{
            "name": repo["full_name"],
            "stars": repo["stargazers_count"],
            "forks": repo["forks_count"],
            "language": repo["language"],
            "created_at": repo["created_at"],
            "updated_at": repo["updated_at"]
        } for repo in items]
        self.logger.info(f"Search completed: {query}")
        return pd.DataFrame(data)

    def get_trending(self, language="Python", since=7):
        """
        Get trending repositories.
        Trending = most starred repos created in the last `since` days.

        Args:
            language: Programming language
            since: Number of days back

        Returns:
        """
        date_since = (datetime.utcnow() - timedelta(days=since)).strftime("%Y-%m-%d")
        query = f"language:{language} created:>={date_since}"
        results = self.get("/search/repositories", params={"q": query, "sort": "stars", "order": "desc", "per_page": 30})
        items = results.get("items", [])

        data = []
        for repo in items:
            data.append({
                "name": repo["full_name"],
                "stars": repo["stargazers_count"],
                "forks": repo["forks_count"],
                "language": repo["language"],
                "created_at": repo["created_at"]
            })

        return pd.DataFrame(data)

    def compare_repos(self, repo_list):
        """
        Compare multiple repositories.

        Args:
            repo_list: List of "owner/repo" strings

        Returns:
            DataFrame with comparison
        """
        comparison_data = []
        for repo in repo_list:
            try:
                data = self.get(f"/repos/{repo}")
                comparison_data.append({
                    "name": data["full_name"],
                    "stars": data["stargazers_count"],
                    "forks": data["forks_count"],
                    "open_issues": data["open_issues_count"],
                    "watchers": data["watchers_count"],
                    "language": data["language"],
                    "created_at": data["created_at"],
                    "updated_at": data["updated_at"]
                })
                self.logger.info(f"Compared repo: {repo}")
            except Exception as e:
                self.logger.error(f"Failed to fetch {repo}: {e}")
                continue
        return pd.DataFrame(comparison_data)

    def export_to_excel(self, df, filename):
        """
        Export DataFrame to Excel with formatting.
        - Bold headers
        - Auto-adjust column widths
        - Add creation timestamp
        """
        with pd.ExcelWriter(filename, engine="xlsxwriter") as writer:
            df.to_excel(writer, sheet_name="Results", index=False)
            workbook = writer.book
            worksheet = writer.sheets["Results"]

            header_format = workbook.add_format({
                "bold": True,
                "text_wrap": True,
                "valign": "middle"
            })

            for col_num, value in enumerate(df.columns.values):
                worksheet.write(0, col_num, value, header_format)
                column_len = max(df[value].astype(str).map(len).max(), len(value)) + 2
                worksheet.set_column(col_num, col_num, column_len)
            worksheet.write(len(df)+2, 0, f"Exported at: {datetime.now()}")
        self.logger.info(f"{filename} exported successfully")

# -----------------------------
# Main Execution
# -----------------------------
if __name__ == "__main__":
    task1_fetch_repos()
    task1_calculate_metrics()
    task1_visualization()
    fetch_user_repos_paginated("torvalds")
    analyze_user_repos()
    analyzer = GitHubAnalyzer()
    search_df = analyzer.search_repos("data science", language="Python", min_stars=500)
    trending_df = analyzer.get_trending(language="Python", since=7)
    repos_to_compare = [
        "tensorflow/tensorflow",
        "pytorch/pytorch",
        "scikit-learn/scikit-learn",
        "keras-team/keras",
        "apache/spark"
    ]
    compare_df = analyzer.compare_repos(repos_to_compare)
    analyzer.export_to_excel(compare_df, "task3_results.xlsx")