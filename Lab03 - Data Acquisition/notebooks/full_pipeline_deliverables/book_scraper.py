import logging
from datetime import datetime
import json
from collections import deque
import os
import time
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse
from urllib.robotparser import RobotFileParser
import pandas as pd
import matplotlib.pyplot as plt

# task 1 
def scrape_travel_books():
    """
    Scrape all travel books.

    Requirements:
    - Handle pagination
    - Add 1 second delay between pages
    - Extract all required fields

    Returns:
        DataFrame with book data
    """
    # Your code here
    url = 'http://books.toscrape.com/catalogue/category/books/travel_2/index.html'
    books = []
    session = requests.Session()
    session.headers.update(
        {'User-Agent': 'Mozilla/5.0 (Educational Purpose) BookScraper/1.0'}
        )
    while url:
        response = session.get(url, timeout=10)
        response.raise_for_status()  
        soup = BeautifulSoup(response.text, 'lxml') 
        for article in soup.select('article.product_pod'):
            book = {}
            title_element = article.select_one('h3 a')
            book['title'] = title_element.get('title')
            price_text = article.select_one('.price_color').text
            book['price'] = float(price_text.replace('Â£', ''))
            availability = article.select_one('.availability').text.strip()
            book['availability'] = 'In stock' in availability
            rating_element = article.select_one('.star-rating')
            rating_text = rating_element.get('class')[1]  
            rating_map = {'One': 1, 'Two': 2, 'Three': 3, 'Four': 4, 'Five': 5}
            book['rating'] = rating_map.get(rating_text, 0)
            books.append(book)
        next_link = soup.select_one('.next a')
        url = urljoin(url, next_link['href']) if next_link else None
        if url:
            time.sleep(1)   
    df = pd.DataFrame(books)
    # save to csv
    df.to_csv('task1_travel_books.csv', index=False)
    return df
# task 2
class CategoryScraper:
    """
    Scrape multiple categories and compare.
    """

    def __init__(self):
        self.base_url = 'http://books.toscrape.com'
        # Your setup (session, headers, etc.)
        self.session = requests.Session()
        self.session.headers.update(
            {'User-Agent': 'Mozilla/5.0 (Educational Purpose) CategoryScraper/1.0'}
        )
    def _get_category_url(self, category_name):

        url = self.base_url + "/index.html"
        response = self.session.get(url, timeout=10)
        soup = BeautifulSoup(response.text, "lxml")

        categories = soup.select(".side_categories a")

        for cat in categories:
            name = cat.text.strip().lower()
            if name == category_name.lower():
                return self.base_url + "/" + cat["href"]

        return None
    def scrape_category(self, category_name, max_pages=2):
        """
        Scrape books from a category.

        Args:
            category_name: Category to scrape
            max_pages:     Maximum pages to scrape

        Returns:
            list: Book dictionaries
        """
        # Your code
        category_url = self._get_category_url(category_name)
        if not category_url:
            print(f"Category '{category_name}' not found.")
            return []
        print(f"Scraping category '{category_name}' from {category_url}...")
        books = []
        url = category_url
        page = 1
        while url and (max_pages is None or page <= max_pages):
            response = self.session.get(url, timeout=10)
            response.raise_for_status()
            soup = BeautifulSoup(response.text, 'lxml')
            for article in soup.select('article.product_pod'):
                book = {}
                title_element = article.select_one('h3 a')
                book['title'] = title_element.get('title')
                price_text = article.select_one('.price_color').text
                book['price'] = float(price_text.replace('Â£', ''))
                rating_element = article.select_one('.star-rating')
                rating_text = rating_element.get('class')[1]
                rating_map = {'One': 1, 'Two': 2, 'Three': 3, 'Four': 4, 'Five': 5}
                book['rating'] = rating_map.get(rating_text, 0)
                book['category'] = category_name
                availability = article.select_one('.availability').text.strip()
                book['availability'] = 'In stock' in availability
                books.append(book)
            next_link = soup.select_one('.next a')
            url = urljoin(url, next_link['href']) if next_link else None
            if url:
                time.sleep(1)
            page += 1
        return category_url, books

    def scrape_multiple_categories(self, categories):
        """
        Scrape multiple categories.

        Returns:
            DataFrame with all books
        """
        df_all = pd.DataFrame()
        for cat in categories:
            print(f"Scraping category: {cat}")
            _,cat_list = self.scrape_category(cat)
            df_cat = pd.DataFrame(cat_list)
            df_all = pd.concat([df_all, df_cat], ignore_index=True)
        return df_all
            

    def compare_categories(self, df):
        """
        Create comparison analysis.

        Should include:
        - Price comparison
        - Rating comparison
        - Stock availability

        Returns:
            dict: Comparison statistics
        """
        grouped = df.groupby('category').agg(
                avg_rating=('rating', 'mean'),
                avg_price=('price', 'mean'),
                in_stock_percent=('availability', 'mean')
            )

        highest_avg_rating = grouped['avg_rating'].idxmax()
        highest_avg_price = grouped['avg_price'].idxmax()

        return {
            'table': grouped,
            'highest_avg_rating': highest_avg_rating,
            'highest_avg_price': highest_avg_price,
            'stock_percentage': (grouped['in_stock_percent'] * 100).to_dict()
        }
# task 3
import logging
from datetime import datetime
import json
from collections import deque
import os
import time
from urllib3 import Retry
from requests.adapters import HTTPAdapter


class AdvancedBookScraper:
    """
    Production-ready book scraper with logging, rate limiting, and error recovery.
    """

    def __init__(self, output_dir='scraped_data'):
        """
        Initialize scraper with logging and rate limiting.
        """
        # Setup file-based logging — all events will be written to scraper.log
        logging.basicConfig(
            filename='scraper.log',
            filemode='w',  # overwrite log each run
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
        )
        self.logger = logging.getLogger(__name__)

        # TODO: Add the following:
        # - Rate limiter (track request times to enforce max 10/min)
        # - requests.Session with proper headers
        # - Progress tracker (dict mapping page -> status)
        self.session = requests.Session()
        self.session.headers.update(
            {'User-Agent': 'Mozilla/5.0 (Educational Purpose) AdvancedBookScraper/1.0'}
        )
        retry_strategy = Retry(
            total=3,  # Total retry attempts
            backoff_factor=1,  # Exponential backoff: 1s → 2s → 4s
            status_forcelist=[429, 500, 502, 503, 504],  # Retry on these HTTP status codes
            allowed_methods=["HEAD", "GET", "OPTIONS", "POST"]
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        self.session.mount("http://", adapter)
        self.session.mount("https://", adapter)
        self.rate_limit = 10  # max requests per minute
        self.time_window = 60  # seconds
        self.request_times = deque()
        self.progress = {}
        # self.category_scraper = CategoryScraper()  
        # self.category_scraper.session = self.session
        self.output_dir = output_dir
        os.makedirs(self.output_dir, exist_ok=True)  # create output directory if it doesn't exist
        self.base_url = 'http://books.toscrape.com'
    


    def check_robots_txt(self, url):
        """
        Check if scraping is allowed for the given URL.
        """
        try:
            # Build the robots.txt URL from the domain
            parsed = urlparse(url)
            robots_url = f"{parsed.scheme}://{parsed.netloc}/robots.txt"

            # Parse the robots.txt file
            rp = RobotFileParser()
            rp.set_url(robots_url)
            rp.read()

            # Check if our user_agent is allowed to access this URL
            allowed = rp.can_fetch(self.session.headers.get('User-Agent', ''), url)
            self.logger.info(f"Checked robots.txt for {url}: {'Allowed' if allowed else 'Blocked'}")
            return allowed

        except Exception as e:
            self.logger.warning(f"Could not read robots.txt for {url}: {e}")
            return False # should i proceed anyway ?
    def _get_category_url(self, category_name):

        url = self.base_url + "/index.html"
        self.enforce_rate_limit()  
        response = self.session.get(url)
        soup = BeautifulSoup(response.text, "lxml")

        categories = soup.select(".side_categories a")

        for cat in categories:
            name = cat.text.strip().lower()
            if name == category_name.lower():
                return self.base_url + "/" + cat["href"]

        return None
    def scrape_category(self, category_name, max_pages=2):
        """
        Scrape books from a category.

        Args:
            category_name: Category to scrape
            max_pages:     Maximum pages to scrape

        Returns:
            list: Book dictionaries
        """
        # Your code
        category_url = self._get_category_url(category_name)
        if not category_url:
            self.logger.warning(f"Category '{category_name}' not found.")
            return []
        self.logger.info(f"Scraping category '{category_name}' from {category_url}...")
        books = []
        url = category_url
        page = 1
        while url and (max_pages is None or page <= max_pages):
            try:
                self.enforce_rate_limit()
                response = self.session.get(url, timeout=10)
                response.raise_for_status()
                response_text = response.text
            except Exception as e:
                self.logger.warning(f"Failed to scrape category '{category_name}' page {page}: {e}")
                break
            soup = BeautifulSoup(response_text, 'lxml')
            for article in soup.select('article.product_pod'):
                book = {}
                title_element = article.select_one('h3 a')
                book['title'] = title_element.get('title')
                price_text = article.select_one('.price_color').text
                book['price'] = float(price_text.replace('Â£', ''))
                rating_element = article.select_one('.star-rating')
                rating_text = rating_element.get('class')[1]
                rating_map = {'One': 1, 'Two': 2, 'Three': 3, 'Four': 4, 'Five': 5}
                book['rating'] = rating_map.get(rating_text, 0)
                book['category'] = category_name
                availability = article.select_one('.availability').text.strip()
                book['availability'] = 'In stock' in availability
                books.append(book)
            next_link = soup.select_one('.next a')
            url = urljoin(url, next_link['href']) if next_link else None
            if url:
                time.sleep(1)
            page += 1
        return books

        
    def enforce_rate_limit(self):

        now = time.time()
        # Remove timestamps older than the rate period
        while self.request_times and now - self.request_times[0] > self.time_window:
            self.request_times.popleft() # remove requests older than the time window (60s)

        if len(self.request_times) >= self.rate_limit: # wait until next request is allowed
            wait_time = self.time_window - (now - self.request_times[0]) + 1  # wait till oldest request is time window old +1s to be safe
            self.logger.info(f"Rate limit reached. Waiting for {wait_time:.2f} seconds...")
            time.sleep(wait_time)

        # record the current request time
        self.request_times.append(time.time())
    
    def validate_book_data(self, book):
        """
        Validate a book record before adding to results.

        Checks:
        - Price is a valid positive number
        - Rating is between 1 and 5
        - Title is not empty

        Returns:
            bool: True if valid
        """
        return (
            isinstance(book.get('price'), (int, float)) and book['price'] > 0 and
            isinstance(book.get('rating'), (int,float)) and 1 <= book['rating'] <= 5 and
            isinstance(book.get('title'), str) and book['title'].strip() != ''
        )
                                                              

    def save_progress(self, books, filename='progress.json'):
        """
        Save current scraping progress to disk (for resumability).
        """
        try:
            filename = os.path.join(self.output_dir, filename)
            with open(filename, 'w') as f:
                json.dump(books, f, indent=2)
            self.logger.info(f"Progress saved to {filename}")
        except Exception as e:
            self.logger.error(f"Failed to save progress: {e}")

    def load_progress(self, filename='progress.json'):
        """
        Load previous progress from disk to resume interrupted scraping.
        """
        try:
            filename = os.path.join(self.output_dir, filename)
            with open(filename, 'r') as f:
                books = json.load(f)
            self.logger.info(f"Progress loaded from {filename}")
            return books
        except Exception as e:
            self.logger.warning(f"No progress file found or failed to load: {e}")
            return []

    def export_data(self, books, base_filename='books'):
        """
        Export collected data to multiple file formats.

        Creates:
        - books.csv   (UTF-8 encoded)
        - books.xlsx  (with formatted headers)
        - books.json  (properly structured)
        """
        try:
            filename = os.path.join(self.output_dir, base_filename)
            df = pd.DataFrame(books)
            df.to_csv(f'{filename}.csv', index=False, encoding='utf-8')
            df.to_excel(f'{filename}.xlsx', index=False)
            with open(f'{filename}.json', 'w', encoding='utf-8') as f:
                json.dump(books, f, indent=2, ensure_ascii=False)
            self.logger.info(f"Data exported to {base_filename}.csv, .xlsx, and .json")
        except Exception as e:
            self.logger.error(f"Failed to export data: {e}")

    def run_full_pipeline(self, categories, max_pages_per_category=5):
        """
        Complete end-to-end scraping pipeline.

        Steps:
        1. Check robots.txt
        2. Load previous progress (if exists)
        3. Scrape each category
        4. Validate data
        5. Save progress after each category
        6. Export final results
        7. Generate summary report
        """
        all_books = self.load_progress()

        for category in categories:
            category_url = self._get_category_url(category)
            if not category_url:
                self.logger.warning(f"Category '{category}' not found. Skipping.")
                continue

            if not self.check_robots_txt(category_url):
                self.logger.warning(f"Scraping blocked by robots.txt for {category_url}. Skipping.")
                continue

            self.logger.info(f"Starting scrape for category: {category}")
            books = self.scrape_category(category, max_pages=max_pages_per_category)

            # validate books
            valid_books = [b for b in books if self.validate_book_data(b)]
            all_books.extend(valid_books)
            self.logger.info(f"Added {len(valid_books)} valid books from category '{category}'")
            # ensure no duplicates (based on title + category)
            unique_books = {f"{b['title']}|{b['category']}": b for b in all_books}
            all_books = list(unique_books.values())

            # save progress after each category
            self.save_progress(all_books)

        # export final results
        base_filename = f'task3_books'
        self.export_data(all_books, base_filename=base_filename)

        # Generate summary report
        self.logger.info(f"SUMMARY REPORT. Total valid books collected: {len(all_books)}")
        summary_report = {
            'total_books': len(all_books),
            'average_price': sum(b['price'] for b in all_books) / len(all_books) if all_books else 0,
            'average_rating': sum(b['rating'] for b in all_books) / len(all_books) if all_books else 0,
        }
        # highest rated book
        if all_books:
            highest_rated = max(all_books, key=lambda b: b['rating'])
            self.logger.info(f"Highest rated book: '{highest_rated['title']}' with rating {highest_rated['rating']}")
            # most expensive book
            most_expensive = max(all_books, key=lambda b: b['price'])
            self.logger.info(f"Most expensive book: '{most_expensive['title']}' at £{most_expensive['price']:.2f}")
            # lowest rated category
            category_ratings = {}
            for book in all_books:
                category = book['category']
                if category not in category_ratings:
                    category_ratings[category] = []
                category_ratings[category].append(book['rating'])
            lowest_rated_category = min(category_ratings, key=lambda c: sum(category_ratings[c]) / len(category_ratings[c]))
            self.logger.info(f"Lowest rated category: {lowest_rated_category}")
            # highest rated caregory
            highest_rated_category = max(category_ratings, key=lambda c: sum(category_ratings[c]) / len(category_ratings[c]))
            self.logger.info(f"Highest rated category: {highest_rated_category}")
        # log summary report
        self.logger.info(f"Summary Report: {summary_report}")
        return summary_report


if __name__ == "__main__":
    # task 1
    travel_books_df = scrape_travel_books()
    print(travel_books_df.head())
    # analysis questions

    avg_price = travel_books_df['price'].mean()


    five_star_count = (travel_books_df['rating'] == 5).sum()


    in_stock_count = travel_books_df['availability'].sum()
    total_books = len(travel_books_df)
    in_stock_percentage = (in_stock_count / total_books) * 100


    with open("task1_analysis.txt", "w") as f:
        f.write(f"Average price of travel books: {avg_price:.2f}\n")
        f.write(f"Number of 5-star travel books: {five_star_count}\n")
        f.write(f"Percentage of books in stock: {in_stock_percentage:.2f}%\n")

    print("Average price:", avg_price)
    print("5-star books:", five_star_count)
    print("In-stock percentage:", in_stock_percentage)
    
    # task 2
    categories = ['Fiction', 'Mystery', 'Historical Fiction', 'Science Fiction']
    scraper = CategoryScraper()
    df = scraper.scrape_multiple_categories(categories)
    # save to csv
    df.to_csv('task2_categories.csv', index=False)
    comparison = scraper.compare_categories(df)
    print("Comparison results:")
    print(comparison)


    comparison_df = comparison["table"]

    fig, axes = plt.subplots(2, 1, figsize=(8, 10))

    # -avg rating
    comparison_df['avg_rating'].plot(
        kind='bar',
        ax=axes[0],
        title="Average Rating by Category"
    )
    axes[0].set_ylabel("Rating")

    # avg price
    comparison_df['avg_price'].plot(
        kind='bar',
        ax=axes[1],
        title="Average Price by Category"
    )
    axes[1].set_ylabel("Price")

    plt.tight_layout()
    plt.savefig("task2_comparison.png")
    plt.close()

    # Task 3
    scraper = AdvancedBookScraper()
    scraper.run_full_pipeline(
        categories=['Mystery', 'Science Fiction', 'Fantasy'], max_pages_per_category=3
    )