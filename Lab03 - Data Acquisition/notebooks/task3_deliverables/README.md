# Book Scraping 


# How to Run the Code

## 1. Install Dependencies

```bash
pip install requests beautifulsoup4 lxml pandas matplotlib openpyxl
```


## 2. Run the Script

```bash
python book_scraper.py
```

Running the script will automatically execute:

* Task 1 scraping and analysis
* Task 2 category comparison
* Task 3 advanced scraping pipeline

---

# Dependencies

The project requires the following Python libraries:

| Library        | Purpose                                |
| -------------- | -------------------------------------- |
| requests       | Send HTTP requests                     |
| beautifulsoup4 | Parse HTML pages                       |
| lxml           | Faster HTML parser                     |
| pandas         | Data processing and analysis           |
| matplotlib     | Data visualization                     |
| openpyxl       | Excel file export                      |
| urllib         | URL handling and robots.txt parsing    |
| logging        | logging for the advanced scraper |


---

# Project Outputs

Running the project generates:

### Task 1

* `task1_travel_books.csv`
* `task1_analysis.txt`

### Task 2

* `task2_categories.csv`
* `task2_comparison.png`

### Task 3

Inside `scraped_data/`:

* `task3_books.csv`
* `task3_books.xlsx`
* `task3_books.json`
* `progress.json`
* `scraper.log`

---

# Issues Encountered


### 1. Pagination Handling

Some categories contain multiple pages.
Pagination was handled using the **Next button** and constructing the next URL using:

```python
urljoin(current_url, next_link)
```

---
### 2. Getting category url
each url has a different endpoint and to solve that i implemented a function that takes category name, navigates to homepage, and finds the href of the intended category

### 3. Rate Limiting

The rate limiting logic was not working since it was misplaced, but after noticing the low delay it was suspicious and the problem was detected & solved

---

# Key Findings
### task 1
- all travel books are in stock (100%)
- only one book has 5-star rating
- average price of travel books ~ 39 pounds
### task 2
- the comparison plot shows that sci fiction has the lowest average rating while historical fiction has the highest
- Mystery has the lowest average price while Fiction has the highest

