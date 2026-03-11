# Book Market Intelligence System

## Installation
pip install pandas requests beautifulsoup4 lxml

## How to Run
```
cd full_pipeline_deliverables
```
```
python full_pipeline.py
```
```
python analysis.py
```

## Architecture
- SQLite database
- REST API (GitHub)
- Web scraping (BooksToScrape)

## Data Schema
Tables:
- api_data
- scraped_data
- pipeline_logs
- books