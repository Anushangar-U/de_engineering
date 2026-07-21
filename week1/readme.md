# Week 1 — SQL Foundations, PostgreSQL, and First ETL Pipeline

Foundational week of an 8-week self-directed data engineering learning path, focused on getting PostgreSQL running locally, connecting it to Python, and building a first working ETL pipeline end to end.

## What This Week Covers

- PostgreSQL installation and local database setup
- Connecting Python to PostgreSQL using `psycopg2`
- SQL window functions: `ROW_NUMBER`, `RANK`, `SUM() OVER`, `LAG`, `AVG() OVER`
- Common Table Expressions (CTEs), including chained CTEs
- LeetCode SQL practice (Easy and Medium difficulty)
- A first complete ETL pipeline: Extract → Transform → Load, with logging and error handling

## Key Concepts Practiced

**Window functions** — `PARTITION BY` splits data into independent groups before applying ranking or aggregate functions; used to answer questions like "top performer per group" without collapsing the underlying rows.

**CTEs** — `WITH` clauses used to break complex queries into named, readable steps, including chaining multiple CTEs together where one references the output of another.

**ETL fundamentals** — a working pipeline (`etl_pipeline.py`) that extracts sample data, transforms it (adding calculated columns, filtering invalid rows), and loads it into PostgreSQL using `execute_values` for efficient bulk inserts.

**Error handling discipline** — `try/except/finally` used specifically around the Load step, since it's the only step that writes to something permanent; Extract and Transform only manipulate in-memory data and don't require the same defensive handling.

## Files

```
week1/
├── connect.py          # First Python-to-PostgreSQL connection test
├── etl_pipeline.py      # First complete ETL pipeline (Extract, Transform, Load)
└── README.md
```

## Tech Stack

Python · PostgreSQL · psycopg2 · pandas

## Notes

This week also involved genuine environment setup debugging — a PostgreSQL installation directory conflict, and later a persistent SSL certificate environment variable (`REQUESTS_CA_BUNDLE`) pointing to an invalid path, which caused `pip install` failures. That certificate issue resurfaced in later weeks (affecting `dbt --version`) before being permanently resolved by removing the stale environment variable at the system level.