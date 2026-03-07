import sqlite3
import pandas as pd
import matplotlib.pyplot as plt
import json
import re

# connect to database
conn = sqlite3.connect("library.db")

# -----------------------------
# Analysis for data in original Database
# -----------------------------

# -----------------------------
# GRAPH 1 – Popular Genres
# -----------------------------
genre_df = pd.read_sql_query("""
SELECT genre, COUNT(*) as total_books
FROM books
GROUP BY genre
""", conn)

plt.figure()

plt.pie(
    genre_df["total_books"],
    labels=genre_df["genre"],
    autopct="%1.1f%%",
    startangle=140
)

plt.title("Popular Genres (Distribution of Books)")

plt.tight_layout()
plt.savefig("graph1_popular_genres.png")
plt.clf()


# -----------------------------
# GRAPH 2 – Top borrowed books
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
# GRAPH 3 – Fine Patterns by membership type 
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
plt.savefig("graph3_fine_patterns.png")
plt.clf()

conn.close()

# connect to full pipeline database
conn = sqlite3.connect("market_intelligence.db")

# -----------------------------
# Analysis for data from github API 
# -----------------------------

api_df = pd.read_sql_query("SELECT content FROM api_data", conn)

repos = []
stars = []
languages = []

for row in api_df["content"]:
    data = json.loads(row)

    for repo in data["items"]:
        repos.append(repo["name"])
        stars.append(repo["stargazers_count"])
        lang = repo["language"]

        if lang:  # ignore None values
            languages.append(lang)


# -----------------------------
# GRAPH 4 – Top book-related repos 
# -----------------------------

repo_df = pd.DataFrame({
    "repo": repos,
    "stars": stars
})

repo_df.plot(kind="bar", x="repo", y="stars")

plt.title("Top Book-related GitHub Repositories by Stars")
plt.xlabel("Repository")
plt.ylabel("Stars")
plt.xticks(rotation=45)
plt.tight_layout()
plt.savefig("graph4_github_popularity.png")
plt.clf()

# -----------------------------
# GRAPH 5 – Top book-related Programming Languages 
# -----------------------------

lang_df = pd.DataFrame(languages, columns=["language"])

lang_counts = lang_df["language"].value_counts()

lang_counts.plot(kind="bar")

plt.title("Programming Languages Used in Book-related GitHub Projects")
plt.xlabel("Programming Language")
plt.ylabel("Number of Repositories")
plt.xticks(rotation=45)
plt.tight_layout()

plt.savefig("graph5_github_languages.png")
plt.clf()

# -----------------------------
# Analysis for data from scraping
# -----------------------------

# -----------------------------
# GRAPH 6 – Prices
# -----------------------------
scraped_df = pd.read_sql_query("SELECT content FROM scraped_data", conn)

prices = []
ratings = []

for row in scraped_df["content"]:
    data = json.loads(row)

    price = data.get("price")
    rating = data.get("rating")

    if price and rating:
        prices.append(float(price))
        ratings.append(rating)

price_df = pd.DataFrame({
    "price": prices,
    "rating": ratings
})

avg_price_rating = price_df.groupby("rating")["price"].mean().reset_index()

avg_price_rating.plot(kind="bar", x="rating", y="price")

plt.title("Average Book Price by Rating")
plt.xlabel("Book Rating")
plt.ylabel("Average Price (£)")
plt.xticks(rotation=0)
plt.tight_layout()

plt.savefig("graph6_price_by_rating.png")
plt.clf()
conn.close()

print("Graphs generated successfully")