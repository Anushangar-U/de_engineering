# Week 4 — Star Schema Data Warehouse

This week migrated the pipeline's storage from a single flat table into a proper dimensional (star schema) data warehouse, and introduced TaskGroups to organize the growing Airflow DAG.

## What This Week Covers

- Star schema designed and built in PostgreSQL: one fact table, two dimension tables
- Slowly Changing Dimension (SCD) strategy evaluated and deliberately chosen (Type 1, not Type 2)
- Airflow TaskGroups added to organize the DAG visually as task count grew
- `CryptoLoader` rewritten to upsert dimensions and insert facts with correct foreign key relationships
- A real, non-trivial bug encountered, investigated, and resolved

## Schema Design

```
FactCryptoPrices
├── coin_key    → DimCoin.coin_key
├── date_key    → DimDate.date_key
├── current_price, market_cap, market_cap_rank, total_volume,
│   high_24h, low_24h, price_change_24h, price_change_percentage_24h

DimCoin
├── coin_key (PK), coin_id, symbol, name

DimDate
├── date_key (PK), full_date, year, month, day, hour, day_of_week, is_weekend
```

**Why a star schema instead of one flat table:** separating descriptive attributes (coin identity, date breakdowns) from quantifiable measurements (price, market cap) means date-based and coin-based analysis becomes a simple join rather than repeated parsing logic inside every query. `DimDate` in particular is pre-populated with derived attributes (day of week, is_weekend) so that queries like "average price by day of week" require no date logic at all — just a join and a `GROUP BY`.

**Why SCD Type 1, not Type 2, for `DimCoin`:** coin renames are rare, and historical price snapshots are already preserved through `date_key` in the fact table. Preserving name-change history in the dimension itself was judged unnecessary complexity for this project's scope — a deliberate choice, not a default.

## Engineering Challenge: A Silent Data Bug

After migrating to the star schema, the DAG ran with no errors, but `DimCoin` and `FactCryptoPrices` stayed at a single row each instead of the expected ten. The root cause took several layers to uncover:

1. A Python `int` vs `datetime` comparison error appeared first, traced to `pandas.to_datetime()` failing to correctly restore a timestamp column after a JSON round-trip between Airflow tasks.
2. After fixing that, the error disappeared, but the pipeline began reporting "no new records to load" on every run despite fresh data being available.
3. Inspecting the raw intermediate JSON file directly revealed the actual cause: the timestamp had been serialized as milliseconds, but `pandas.to_datetime()` defaults to interpreting raw integers as nanoseconds — producing a technically valid but wildly incorrect date, decades removed from reality.
4. Explicitly specifying `unit='ms'` resolved it completely.

This was a genuinely instructive case of a bug that stopped crashing before it was actually fixed — a reminder that "no errors" is not the same as "correct results."

## TaskGroups

As the pipeline's task count grew, tasks were organized into two logical groups — `ingestion` (extract, transform) and `processing` (validate, load) — to keep the Airflow Graph view readable. TaskGroups are purely visual/organizational; they do not change execution order or behavior, which is still governed entirely by explicit `>>` dependencies.

## Files

```
week4/
├── crypto_pipeline_dag.py       # Updated DAG with TaskGroups and star schema loading logic
├── create_star_schema.sql       # Table creation script (DimCoin, DimDate, FactCryptoPrices)
├── populate_dimdate.sql          # One-time script populating DimDate with hourly date slots
└── README.md
```

## Tech Stack

Python · PostgreSQL · Apache Airflow · Docker

## Sample Query

```sql
SELECT c.name, d.full_date, d.hour, f.current_price,
  LAG(f.current_price) OVER (
    PARTITION BY c.coin_key ORDER BY f.date_key
  ) AS previous_price
FROM FactCryptoPrices f
JOIN DimCoin c ON f.coin_key = c.coin_key
JOIN DimDate d ON f.date_key = d.date_key
WHERE c.coin_id = 'bitcoin'
ORDER BY f.date_key;
```