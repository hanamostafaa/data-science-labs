# ─── Core standard library imports ───────────────────────────────────────────
import sqlite3
import logging
from datetime import datetime
import time
import json
import os
from book_scraper import CategoryScraper

# ─── Third-party imports (install via pip if needed) ─────────────────────────
import requests
from bs4 import BeautifulSoup
import pandas as pd

class DataCollectionPipeline:
    """
    Unified data collection from multiple sources.
    Integrates: SQLite databases, REST APIs, and Web Scraping.
    """

    # =========================================================================
    # INITIALIZATION
    # =========================================================================

    def __init__(self, db_path='collected_data.db'):
        """
        Initialize pipeline with database and logging.

        Args:
            db_path (str): Path to the SQLite database file.
                           Will be created if it doesn't exist.
        """
        # ── Setup logging ─────────────────────────────────────────────────────
        # Logs go to BOTH a file ('pipeline.log') and the console (StreamHandler)
        logging.basicConfig(
            level=logging.INFO,  # Log INFO and above (INFO, WARNING, ERROR, CRITICAL)
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler('pipeline.log'),  # Persistent log file
                logging.StreamHandler(),  # Real-time console output
            ],
        )
        self.logger = logging.getLogger(__name__)  # Logger named after this module

        # ── Setup database ────────────────────────────────────────────────────
        self.db_path = db_path
        self.conn = sqlite3.connect(db_path)  # Creates file if it doesn't exist
        self._create_tables()  # Ensure all tables exist

        # ── Setup HTTP session ────────────────────────────────────────────────
        # Using a Session object reuses TCP connections (faster than fresh requests)
        self.session = requests.Session()
        self.session.headers.update(
        {
            "User-Agent": "BookMarketIntelligenceProject",
            "Accept": "application/vnd.github+json"
        })

        self.logger.info("Pipeline initialized")
        self.scraper = CategoryScraper()  

    def _create_tables(self):
        """
        Create all required tables if they don't already exist.
        Uses CREATE TABLE IF NOT EXISTS so it's safe to call multiple times.
        """
        cursor = self.conn.cursor()

        # ── Table for API data ────────────────────────────────────────────────
        # Stores the full JSON response as TEXT (json.dumps)
        cursor.execute(
            '''
            CREATE TABLE IF NOT EXISTS api_data (
                id           INTEGER PRIMARY KEY AUTOINCREMENT,
                source       TEXT NOT NULL,       -- URL of the API endpoint
                data_type    TEXT NOT NULL,       -- e.g. 'json'
                content      TEXT NOT NULL,       -- JSON-serialized response
                collected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        '''
        )

        # ── Table for scraped data ────────────────────────────────────────────
        # Stores the URL, page title, and JSON-encoded field dict
        cursor.execute(
            '''
            CREATE TABLE IF NOT EXISTS scraped_data (
                id         INTEGER PRIMARY KEY AUTOINCREMENT,
                url        TEXT NOT NULL,
                title      TEXT,
                content    TEXT,          -- JSON-encoded dict of scraped fields
                scraped_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        '''
        )
        # create books table
        cursor.execute(
            '''
            CREATE TABLE IF NOT EXISTS books (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                title TEXT NOT NULL,
                rating REAL,
                price REAL,
                availability TEXT
            )
        '''
        )

        # ── Table for pipeline operation logs ─────────────────────────────────
        # Audit trail: which source, how many records, success/error, and why
        cursor.execute(
            '''
            CREATE TABLE IF NOT EXISTS pipeline_logs (
                id               INTEGER PRIMARY KEY AUTOINCREMENT,
                source_type      TEXT NOT NULL,       -- 'database', 'api', or 'web'
                records_collected INTEGER,
                status           TEXT,               -- 'success' or 'error'
                error_message    TEXT,               -- NULL on success
                timestamp        TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        '''
        )

        self.conn.commit()  # Persist table creation

    # =========================================================================
    # DATABASE METHODS
    # =========================================================================

    def collect_from_database(self, query, source_db_path):
        """
        Collect data from another SQLite database by running a SQL query.

        Args:
            query (str):           SQL SELECT query to run on the source DB.
            source_db_path (str):  Path to the source .db file.

        Returns:
            pd.DataFrame: Query results. Empty DataFrame on error.
        """
        self.logger.info(f"Collecting from database: {source_db_path}")
        try:
            # Open a separate connection to the source database
            source_conn = sqlite3.connect(source_db_path)

            # pandas read_sql_query runs the query and returns a DataFrame directly
            df = pd.read_sql_query(query, source_conn)
            source_conn.close()  # Always close the connection when done

            self.logger.info(f"Collected {len(df)} records from database")
            self._log_collection('database', len(df), 'success')  # Audit log
            return df

        except Exception as e:
            # Catch-all: file not found, SQL syntax error, permission issues, etc.
            self.logger.error(f"Database error: {e}")
            self._log_collection('database', 0, 'error', str(e))
            return pd.DataFrame()  # Return empty DF so callers don't need to check None

    # =========================================================================
    # API METHODS
    # =========================================================================

    def collect_from_api(self, url, params=None):
        """
        Collect JSON data from a REST API endpoint via GET request.
        The full response is serialized and stored in the 'api_data' table.

        Args:
            url (str):    Full API endpoint URL.
            params (dict): Optional query parameters (appended to URL).

        Returns:
            dict | list | None: Parsed JSON response, or None on error.
        """
        self.logger.info(f"Collecting from API: {url}")
        try:
            # timeout=10 prevents the pipeline from hanging indefinitely
            response = self.session.get(url, params=params, timeout=10)
            response.raise_for_status()  # Raises HTTPError for 4xx/5xx status codes

            data = response.json()  # Parse JSON response body

            # Persist raw JSON to the database for later analysis
            cursor = self.conn.cursor()
            cursor.execute(
                '''
                INSERT INTO api_data (source, data_type, content)
                VALUES (?, ?, ?)
            ''',
                (url, 'json', json.dumps(data)),
            )  # json.dumps converts dict → string
            self.conn.commit()

            self.logger.info(f"Collected API data from {url}")
            self._log_collection('api', 1, 'success')
            return data

        except Exception as e:
            # Covers: network errors, timeouts, bad JSON, HTTP errors
            self.logger.error(f"API error: {e}")
            self._log_collection('api', 0, 'error', str(e))
            return None

    # =========================================================================
    # WEB SCRAPING METHODS
    # =========================================================================

    def collect_from_web(self, categories, max_pages_per_category=None):
        """
        uses category scraper
        """

        self.logger.info(f"Scraping categories: {categories}")
        books = []
        for cat in categories:
            try:
                url, cat_books = self.scraper.scrape_category(cat, max_pages=max_pages_per_category)
                books.extend(cat_books)
            except Exception as e:
                self.logger.error(f"Error scraping category '{cat}': {e}")

        for book in books:
            cursor = self.conn.cursor()
            cursor.execute(
                '''
                INSERT INTO books (title, rating, price, availability)
                VALUES (?, ?, ?, ?)
            ''',
                (book['title'], book['rating'], book['price'], book['availability']),
            )
            cursor.execute(
                '''
                INSERT INTO scraped_data (url, title, content)
                VALUES (?, ?, ?)
            ''',
                (url, book.get('title'), json.dumps(book)),
            )
            self._log_collection('web', 1, 'success')  # Log each successful book scrape
            
        self.conn.commit()
        self.logger.info(f"Scraped {len(books)} books from categories: {categories}")
        return books

    # =========================================================================
    # UTILITY METHODS
    # =========================================================================

    def _log_collection(self, source_type, records, status, error_msg=None):
        """
        Internal method: Write a collection event to the pipeline_logs table.
        Called automatically after every collect_* method (success or failure).

        Args:
            source_type (str): 'database', 'api', or 'web'
            records (int):     Number of records collected (0 on error)
            status (str):      'success' or 'error'
            error_msg (str):   Exception message if status=='error', else None
        """
        cursor = self.conn.cursor()
        cursor.execute(
            '''
            INSERT INTO pipeline_logs (source_type, records_collected, status, error_message)
            VALUES (?, ?, ?, ?)
        ''',
            (source_type, records, status, error_msg),
        )
        self.conn.commit()

    def get_collection_stats(self):
        """
        Query the database to summarize all data collection activity.

        Returns:
            dict with keys:
                'api_records'     (int): total rows in api_data table
                'scraped_records' (int): total rows in scraped_data table
                'logs'            (DataFrame): per-source success counts
        """
        stats = {}
        cursor = self.conn.cursor()

        # Count total API records collected
        cursor.execute("SELECT COUNT(*) FROM api_data")
        stats['api_records'] = cursor.fetchone()[0]

        # Count total scraped records collected
        cursor.execute("SELECT COUNT(*) FROM scraped_data")
        stats['scraped_records'] = cursor.fetchone()[0]

        # Summarize logs: per source_type — total attempts and how many succeeded
        logs_df = pd.read_sql_query(
            '''
            SELECT
                source_type,
                COUNT(*) AS count,
                SUM(CASE WHEN status='success' THEN 1 ELSE 0 END) AS successful
            FROM pipeline_logs
            GROUP BY source_type
        ''',
            self.conn,
        )
        stats['logs'] = logs_df

        return stats

    def export_all_data(self, output_dir='exports'):
        """
        Export all collected data tables to CSV files.
        Creates the output directory if it doesn't exist.

        Files created:
            {output_dir}/api_data.csv
            {output_dir}/scraped_data.csv
            {output_dir}/pipeline_logs.csv
        """
        os.makedirs(output_dir, exist_ok=True)  # Create dir (no error if exists)

        # Export API data
        api_df = pd.read_sql_query("SELECT * FROM api_data", self.conn)
        api_df.to_csv(f'{output_dir}/api_data.csv', index=False)

        # Export scraped data
        scraped_df = pd.read_sql_query("SELECT * FROM scraped_data", self.conn)
        scraped_df.to_csv(f'{output_dir}/scraped_data.csv', index=False)

        # Export operation logs (useful for debugging and auditing)
        logs_df = pd.read_sql_query("SELECT * FROM pipeline_logs", self.conn)
        logs_df.to_csv(f'{output_dir}/pipeline_logs.csv', index=False)

        self.logger.info(f"Data exported to {output_dir}/")

    def close(self):
        """
        Close the SQLite database connection.
        Always call this when finished to avoid database locking issues.
        """
        self.conn.close()
        self.logger.info("Pipeline closed")


print("✅ DataCollectionPipeline class defined successfully!")

# ─── Initialize the pipeline ──────────────────────────────────────────────────
# Creates 'collected_data.db' and sets up logging
pipeline = DataCollectionPipeline("market_intelligence.db")

# ─── Step 1: Collect from a local SQLite database ─────────────────────────────
# Total fines oer membership type 
library_data = pipeline.collect_from_database(
    """
WITH unique_members AS (SELECT member_id, SUM(fine_amount) as fines_per_member from borrowings group by member_id HAVING SUM(fine_amount) > 0)

SELECT membership_type, COUNT(*) as total_members, SUM(fines_per_member) as total_fines , AVG(fines_per_member) as avg_fine_per_member
FROM members m INNER JOIN unique_members u ON m.member_id = u.member_id
GROUP BY membership_type
""",  # SQL query
    'library.db',  # Path to source database
)

# ─── Step 2: Collect from the GitHub REST API ─────────────────────────────────
# Fetches metadata (stars, forks, description, etc.) for the pandas repository
github_data = pipeline.collect_from_api(
    "https://api.github.com/search/repositories",
    params={
        "q": "book python",
        "sort": "stars",
        "order": "desc",
        "per_page": 5
    }
)

# ─── Step 3: Scrape structured data from a webpage ───────────────────────────
# CSS selectors map field names to HTML elements on the target page
book_data = pipeline.collect_from_web(
    categories=['Travel', 'Mystery'], 
    max_pages_per_category=2,  
)

# ─── Step 4: Print collection statistics ─────────────────────────────────────
stats = pipeline.get_collection_stats()
print("\n=== Collection Statistics ===")
print(f"API Records:     {stats['api_records']}")
print(f"Scraped Records: {stats['scraped_records']}")
print("\nLogs by Source Type:")
print(stats['logs'])

# ─── Step 5: Export all collected data to CSV ─────────────────────────────────
# Creates: exports/api_data.csv, exports/scraped_data.csv, exports/pipeline_logs.csv
pipeline.export_all_data()
# checking books were stored fine
df = pd.read_sql_query("SELECT * FROM books LIMIT 10", pipeline.conn)
print(df)

# ─── Step 6: Close the pipeline ───────────────────────────────────────────────
# Always close to release the database file lock
pipeline.close()