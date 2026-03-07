import pandas as pd
import sqlite3  

# 1.1 Books published after 1960 in Fiction genre
query1_1 = """
SELECT title, name AS author_name, publication_year
FROM books b
INNER JOIN authors a ON a.author_id = b.author_id
WHERE publication_year > 1960 AND genre = 'Fiction'
"""

# 1.2 Student members with total borrowings
query1_2 = """
SELECT name AS member_name, email, COUNT(*) AS total_borrowings
FROM members m
INNER JOIN borrowings b ON m.member_id = b.member_id
WHERE membership_type = 'student'
GROUP BY m.member_id 
ORDER BY total_borrowings DESC
"""

# 1.3 Total and average fines per membership type
query1_3 = """
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
"""

##### TASK 2: Book Borrowing Analysis

# 2.1 Most borrowed books (show 'Never Borrowed' if count = 0)
query2_1 = """
SELECT title, name AS author_name,
CASE WHEN COUNT(due_date) = 0 THEN 'Never Borrowed' ELSE CAST(COUNT(due_date) AS TEXT) END AS times_borrowed,
COUNT(due_date) - COUNT(return_date) < copies_available AS currently_available
FROM books b
JOIN authors a ON b.author_id = a.author_id
LEFT JOIN borrowings br ON b.book_id = br.book_id
GROUP BY b.book_id
ORDER BY COUNT(br.borrow_id) DESC
LIMIT 3
"""


# 2.2 Overdue books with estimated fines
query2_2 = """
SELECT title AS book_title, name AS borrower_name, borrow_date, due_date,
JULIANDAY(CURRENT_DATE) - JULIANDAY(due_date) AS days_overdue,
(JULIANDAY(CURRENT_DATE) - JULIANDAY(due_date))*2 AS estimated_fine
FROM borrowings br
INNER JOIN books b ON br.book_id = b.book_id
INNER JOIN members m ON br.member_id = m.member_id 
WHERE return_date IS NULL AND due_date < CURRENT_DATE 
ORDER BY days_overdue DESC
"""

##### TASK 3: Member Borrowing Performance

query3_1 = """
WITH member_borrowing_stats AS (
    SELECT member_id, 
           COUNT(*) AS total_borrowings, 
           COUNT(return_date) AS books_returned, 
           SUM(fine_amount) AS total_fines_paid
    FROM borrowings
    GROUP BY member_id
),
return_performance AS (
    SELECT member_id, 
           SUM(CASE WHEN return_date IS NOT NULL AND return_date <= due_date THEN 1 ELSE 0 END)
           / NULLIF(COUNT(return_date),0) AS on_time_return_rate
    FROM borrowings
    GROUP BY member_id
)
SELECT m.name AS member_name,
       m.membership_type,
       br.total_borrowings,
       br.books_returned,
       br.total_borrowings - br.books_returned AS books_still_borrowed,
       br.total_fines_paid,
       ROUND(100 * r.on_time_return_rate, 2) AS on_time_return_rate,
       CASE 
           WHEN br.total_borrowings IS NULL OR br.total_borrowings = 0 THEN 'Inactive'
           WHEN br.total_borrowings BETWEEN 1 AND 5 THEN 'Active'
           ELSE 'Very Active'
       END AS member_category
FROM members m
LEFT JOIN member_borrowing_stats br ON br.member_id = m.member_id
LEFT JOIN return_performance r ON r.member_id = m.member_id;
"""
