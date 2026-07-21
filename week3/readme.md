# Week 3 — Apache Airflow Orchestration

This week moved the crypto ETL pipeline from a manually-run Python script into a fully orchestrated, scheduled pipeline using Apache Airflow, running in Docker.

## What This Week Covers

- Apache Airflow installed and running via Docker Compose (7 containers: scheduler, worker, triggerer, dag-processor, apiserver, internal PostgreSQL, Redis)
- First DAG built and successfully triggered
- The full Week 2 crypto pipeline (`CryptoExtractor`, `CryptoTransformer`, `DataValidator`, `CryptoLoader`) wrapped into Airflow tasks
- Retry logic configured and deliberately tested by forcing a failure
- Backfill and `catchup` behavior understood and correctly configured for a live-data pipeline

## Key Concepts

**DAG** — a Python file that defines a pipeline's tasks and their order; Airflow reads and schedules it, it is not run directly like a normal script.

**Task isolation** — each Airflow task can run as a separate process, so data can't be passed directly in memory between tasks like in a single continuous script. Intermediate results are written to temporary JSON files (`/tmp/crypto_raw.json`, etc.) and read back in by the next task.

**Docker networking** — `localhost` inside a Docker container refers to the container itself, not the host machine. Since PostgreSQL runs on the host (Windows) rather than inside Docker, the pipeline's database host was changed to `host.docker.internal`, Docker's built-in address for reaching host-machine services from inside a container.

**Retries** — Airflow does not retry failed tasks by default; `retries` and `retry_delay` were explicitly configured per task, and verified by deliberately breaking the API endpoint and confirming Airflow retried the correct number of times (1 original attempt + 3 retries = 4 total) before marking the task failed.

**Catchup and backfill** — `catchup=True` only makes sense when a data source can accurately return historical data for a past point in time (e.g. a database of completed transactions, or daily log files). Since this pipeline queries a live-only API with no historical lookup capability, `catchup=False` is the correct configuration — backfilling would otherwise produce misleading, duplicate-looking "historical" data that is really just repeated live snapshots.

## Files

```
week3/
├── crypto_pipeline_dag.py   # Airflow DAG wrapping the crypto ETL pipeline
└── README.md
```

*Note: this file is a snapshot copy for documentation purposes. The live, running version of this DAG is located in the Airflow project's `dags/` folder, where Airflow's dag-processor actively reads it.*

## Tech Stack

Python · Apache Airflow · Docker · PostgreSQL

## Notes

Getting Airflow running required installing a separate Python 3.11 environment path initially considered, before switching to Docker as the cleaner solution — since Airflow does not support the Python 3.14 version already installed on the host machine, and Docker sidesteps that conflict entirely by providing an isolated, pre-configured environment.