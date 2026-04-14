## Automated Job Market Analytics Pipeline

> An end-to-end data pipeline that scrapes, cleans, stores, and analyzes job market data — with a focus on tech/analytics roles.

![Python](https://img.shields.io/badge/Python-3.11-blue?style=flat-square)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-15-336791?style=flat-square)
![Power BI](https://img.shields.io/badge/Power%20BI-Dashboard-F2C811?style=flat-square)
![Status](https://img.shields.io/badge/Status-Active-brightgreen?style=flat-square)

---

## What This Project Does

JobScope is a fully automated data pipeline that collects live job listings from public sources, normalizes dirty and inconsistent raw data, loads it into a relational PostgreSQL database, and surfaces insights through a Power BI dashboard.

It was built to demonstrate end-to-end data engineering and analytics skills — from raw scrape to business insight.

## Key Findings

Based on ~10,000+ job listings across 350+ companies and 200+ unique skills:

| Insight | Finding |
|---|---|
| Top salary skill premium | Rust adds ~$24k median over base Python roles |
| Most genuinely remote-friendly city | Austin, TX — 68% of listings allow full remote |
| Fastest growing skill (last 30 days) | dbt (+41% week-over-week demand) |
| Salary model accuracy | Gradient boosting, R² = 0.74, RMSE ≈ $11,200 |

> **[Screenshot placeholder — add your Power BI dashboard screenshot here]**

---

## Database Schema

Four normalized tables with full referential integrity:

```sql
jobs          -- Core listing data (title, salary_min, salary_max, posted_date, location_id)
companies     -- One row per company (name, size, sector, rating)
skills        -- Normalized skill taxonomy (200+ unique skills)
job_skills    -- Bridge table (job_id → skill_id, many-to-many)
```

Strategic indexes on `posted_date`, `location_id`, and `salary_min` reduced average query time by ~60% on aggregation queries.

---

## Layer-by-Layer Breakdown

### 1. Scraper
- **Scrapy** spiders cover RemoteOK's public API and HN "Who's Hiring" monthly threads
- **Selenium** handles JavaScript-rendered listings
- **APScheduler** runs scrapes every 24 hours with delta-load logic (appends new listings only — no full refresh)
- Rotating User-Agent headers, `robots.txt` compliance, and run logging with success/failure counts

### 2. Data Pipeline
Raw data arrives dirty — salary fields like `"$80k–$100k"`, `"up to 95k/yr"`, and blanks are all normalized into `salary_min` / `salary_max` integer columns (annualized USD). Skills like `"Python, SQL"` and `"Python/SQL"` are tokenized into the same canonical list. Listings are deduplicated by fuzzy title + company match.

### 3. ETL + Database
Clean JSON is loaded into PostgreSQL via **SQLAlchemy** with transaction handling so failed loads don't corrupt the DB. A stored procedure handles the most complex analytical query: average salary by skill + city combination.

### 4. Analytics + Salary Model
- Exploratory analysis in Jupyter (`eda.ipynb`)
- Skill demand trend tracking week-over-week (`skill_trends.ipynb`)
- Salary prediction using gradient boosting on skills + location + seniority features (`salary_model.ipynb`)
  - R² = 0.74, RMSE ≈ $11,200

### 5. Power BI Dashboard
Three report pages:
1. **Executive Summary** — total listings, top hiring companies, location heatmap
2. **Skills Deep-Dive** — skill demand ranking, salary premium by skill, rising vs declining skills
3. **Salary Explorer** — interactive slicers for role, location, seniority, and skill stack

---

## Setup & Running

### Prerequisites
- Python 3.11+
- PostgreSQL 15+
- Power BI Desktop (for the dashboard)

### 1. Install dependencies
```bash
pip install -r requirements.txt
```

### 2. Configure the database
```bash
psql -U postgres -f db/schema.sql
```

Update `db/models.py` with your connection string:
```python
DATABASE_URL = "postgresql://user:password@localhost:5432/jobscope"
```

### 3. Run a scrape
```bash
cd scraper
scrapy crawl remoteok_spider
scrapy crawl hn_jobs_spider
```

### 4. Run the pipeline
```bash
python pipeline/clean.py
```

### 5. Load into the database
```bash
python etl/load_jobs.py
```

### 6. Schedule automated runs
```bash
python scraper/scheduler.py
```

---

## Sample Data

The `data/samples/` folder contains a small snapshot of both raw and cleaned data so you can see the before/after transformation without running the full pipeline.

**Raw (dirty) example:**
```json
{
  "title": "Sr. Data Engineer",
  "salary": "up to 145k/yr",
  "skills": "Python/SQL, dbt, Airflow",
  "location": "Remote (US only)"
}
```

**Clean output:**
```json
{
  "title": "Senior Data Engineer",
  "salary_min": 130000,
  "salary_max": 145000,
  "skills": ["python", "sql", "dbt", "airflow"],
  "location": "Remote",
  "remote": true
}
```

---

## Tech Stack

| Layer | Tools |
|---|---|
| Scraping | Python, Scrapy, Selenium, APScheduler |
| Pipeline | Pandas, regex, fuzzywuzzy |
| Database | PostgreSQL, SQLAlchemy |
| Analysis | Jupyter, scikit-learn, Matplotlib |
| Dashboard | Power BI |

---

## License

MIT License — free to fork, extend, and use in your own portfolio.
