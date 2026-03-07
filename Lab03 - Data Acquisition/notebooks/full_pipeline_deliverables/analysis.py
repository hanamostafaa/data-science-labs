import sqlite3
import pandas as pd
import matplotlib.pyplot as plt
import json
import re

# connect to project database
conn = sqlite3.connect("library.db")

# -----------------------------
# GRAPH 1 – Popular Genres
# -----------------------------
genre_df = pd.read_sql_query("""
SELECT genre, COUNT(*) as total_books
FROM books
GROUP BY genre
""", conn)

genre_df.plot(kind='bar', x='genre', y='total_books')
plt.title("Popular Genres (Number of Books)")
plt.ylabel("Books")
plt.xlabel("Genre")
plt.tight_layout()
plt.savefig("graph1_popular_genres.png")
plt.clf()


# -----------------------------
# GRAPH 2 – Top Borrowed Books
# -----------------------------
top_books = pd.read_sql_query("""
SELECT b.title, COUNT(*) as borrow_count
FROM borrowings br
JOIN books b ON br.book_id = b.book_id
GROUP BY br.book_id
ORDER BY borrow_count DESC
LIMIT 10
""", conn)

top_books.plot(kind="bar", x="title", y="borrow_count")
plt.title("Top 10 Most Borrowed Books")
plt.ylabel("Borrow Count")
plt.xlabel("Book Title")
plt.xticks(rotation=45)
plt.tight_layout()
plt.savefig("graph2_top_borrowed_books.png")
plt.clf()

# -----------------------------
# GRAPH 6 – Fine Patterns (Extra Insight)
# -----------------------------
fine_df = pd.read_sql_query("""
SELECT m.membership_type, AVG(br.fine_amount) as avg_fine
FROM borrowings br
JOIN members m ON br.member_id = m.member_id
GROUP BY m.membership_type
""", conn)

fine_df.plot(kind="bar", x="membership_type", y="avg_fine")
plt.title("Average Fine by Membership Type")
plt.ylabel("Average Fine")
plt.xlabel("Membership Type")
plt.tight_layout()
plt.savefig("graph6_fine_patterns.png")
plt.clf()

conn = sqlite3.connect("market_intelligence.db")


# -----------------------------
# GRAPH 4 – Price Trends (Scraped Data)
# -----------------------------
scraped_df = pd.read_sql_query("SELECT content FROM scraped_data", conn)

titles = []
prices = []

for row in scraped_df["content"]:
    data = json.loads(row)

    title = data.get("title")
    price = data.get("price")

    if price:
        price = re.sub(r"[^\d.]", "", price)
        prices.append(float(price))
        titles.append(title)

price_df = pd.DataFrame({"title": titles, "price": prices})

price_df.plot(kind="bar", x="title", y="price")
plt.title("Book Prices from Scraped Data")
plt.ylabel("Price (£)")
plt.xlabel("Book")
plt.xticks(rotation=45)
plt.tight_layout()
plt.savefig("graph4_price_trends.png")
plt.clf()

# -----------------------------
# GRAPH 5 – Technology Trends (GitHub Stars)
# -----------------------------
api_df = pd.read_sql_query("SELECT content FROM api_data", conn)

repo_names = []
stars = []

for row in api_df["content"]:
    data = json.loads(row)

    if "items" in data:
        for repo in data["items"]:
            repo_names.append(repo["name"])
            stars.append(repo["stargazers_count"])

stars_df = pd.DataFrame({
    "repository": repo_names,
    "stars": stars
})

stars_df.plot(kind="bar", x="repository", y="stars")
plt.title("Top GitHub Book-related Repositories by Stars")
plt.ylabel("Stars")
plt.xlabel("Repository")
plt.xticks(rotation=45)
plt.tight_layout()
plt.savefig("graph5_technology_trends.png")
plt.clf()

conn.close()

print("Graphs generated successfully")