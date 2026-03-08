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

top_books = top_books.sort_values("borrow_count")

plt.figure()

plt.barh(top_books["title"], top_books["borrow_count"])

plt.title("Top 10 Most Borrowed Books")
plt.xlabel("Borrow Count")
plt.ylabel("Book Title")

plt.tight_layout()
plt.savefig("graph2_top_borrowed_books.png")
plt.clf()

# -----------------------------
# GRAPH 3 – avg fine by membership type 
# -----------------------------
fine_df = pd.read_sql_query("""
WITH unique_members AS (
    SELECT member_id, SUM(fine_amount) AS fines_per_member
    FROM borrowings
    GROUP BY member_id
    HAVING SUM(fine_amount) > 0
)
SELECT membership_type, COUNT(*) AS total_members, SUM(fines_per_member) AS total_fines, AVG(fines_per_member) AS avg_fine_per_member
FROM members m
INNER JOIN unique_members u ON m.member_id = u.member_id
GROUP BY membership_type
""", conn)

fine_df.plot(kind="bar", x="membership_type", y="avg_fine_per_member")
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
    repo = json.loads(row)

    repos.append(repo["name"])
    stars.append(repo["stars"])
    languages.append(repo["language"])
            

# -----------------------------
# GRAPH 4 – Top book-related repos 
# -----------------------------

repo_df = pd.DataFrame({
    "repo": repos,
    "stars": stars
})

repo_df = repo_df.sort_values("stars")[:5]

plt.figure()

plt.barh(repo_df["repo"], repo_df["stars"])

plt.title("Top 5 Book-related GitHub Repositories by Stars")
plt.xlabel("Stars")
plt.ylabel("Repository")

plt.tight_layout()
plt.savefig("graph4_github_popularity.png")
plt.clf()

# -----------------------------
# GRAPH 5 – Top book-related Programming Languages 
# -----------------------------

lang_df = pd.DataFrame(languages, columns=["language"])

lang_counts = lang_df["language"].value_counts()

plt.figure()

plt.pie(
    lang_counts,
    labels=lang_counts.index,
    autopct="%1.1f%%",
    startangle=140
)

plt.title("Programming Languages Used in Book-related GitHub Projects")

plt.tight_layout()
plt.savefig("graph5_github_languages.png")
plt.clf()
# -----------------------------
# Analysis for data from scraping
# -----------------------------

scraped_df = pd.read_sql_query("SELECT content FROM scraped_data", conn)

prices = []
ratings = []
books = []
categories = []

for row in scraped_df["content"]:
    data = json.loads(row)

    price = data.get("price")
    rating = data.get("rating")
    category = data.get("category")
    books.append({
        "title": data.get("title"),
        "rating": data.get("rating"),
        "price": float(data.get("price")),
        "category": category
    })

    if price and rating:
        prices.append(float(price))
        ratings.append(rating)
        categories.append(category)


# -----------------------------
# GRAPH 6 – Prices
# -----------------------------

price_df = pd.DataFrame({
    "price": prices,
    "rating": ratings
})

avg_price_rating = price_df.groupby("rating")["price"].mean().reset_index()

avg_price_rating = avg_price_rating.sort_values("rating")

plt.figure()

plt.plot(
    avg_price_rating["rating"],
    avg_price_rating["price"],
    marker="o"
)

plt.title("Average Book Price by Rating")
plt.xlabel("Book Rating")
plt.ylabel("Average Price (£)")

plt.tight_layout()
plt.savefig("graph6_price_by_rating.png")
plt.clf()

# -----------------------------
# GRAPH 7 – Genres 
# -----------------------------

genre_df = pd.DataFrame({
    "category": categories,
    "rating": ratings
})

avg_genre_rating = genre_df.groupby("category")["rating"].mean().reset_index()

avg_genre_rating = avg_genre_rating.sort_values("rating")

plt.figure()

plt.bar(
    avg_genre_rating["category"],
    avg_genre_rating["rating"]
)

plt.title("Average Book Rating by Category")
plt.xlabel("Catgeory")
plt.ylabel("Average Rating")

plt.tight_layout()
plt.savefig("graph7_rating_by_category.png")
plt.clf()

# =====================================================
# GRAPH 8: Genre distribution
# =====================================================

genre_counts = genre_df["category"].value_counts()

plt.figure()

plt.pie(
    genre_counts.values,
    labels=genre_counts.index,
    autopct="%1.1f%%"
)

plt.title("Book Distribution by Genre")

plt.tight_layout()
plt.savefig("graph8_genre_distribution.png")
plt.clf()

conn.close()

print("Graphs generated successfully")