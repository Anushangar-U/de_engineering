# Week 7 — AWS: S3 Integration & Cloud Infrastructure Fundamentals

This week introduced AWS into the project, focused on securely setting up cloud infrastructure and integrating S3 as a permanent, cloud-based archive for the crypto pipeline's raw API data.

## What This Week Covers

- AWS account setup on the Free Plan, with billing safety configured before touching any service
- IAM user created with least-privilege access (S3-only, no console login) rather than using root credentials
- First S3 bucket created and integrated directly into the live Airflow pipeline
- RDS (managed PostgreSQL) evaluated and deliberately deferred, with documented reasoning

## AWS Account & Security Setup

- **Free Plan** selected specifically (not Paid Plan) — this plan cannot incur charges beyond the provided credit; the account automatically closes if credits are exhausted or after 6 months, rather than silently converting to real billing
- **MFA enabled** on the root account
- **Budget alerts configured** via AWS Budgets — a "Cost budget" set to $100 (matching the actual starting credit, not the aspirational $200 maximum), with alert thresholds at 1% (catches any spending at all) and 85% (early warning)
- **AWS Cost Anomaly Detection** — automatically enabled by AWS as a secondary, free safety layer, using machine learning to flag unusual spend patterns independent of the fixed budget thresholds
- **IAM user (`crypto-pipeline-s3-user`)** created specifically for programmatic S3 access, scoped to `AmazonS3FullAccess` only, with console login disabled — following least-privilege practice rather than using root account credentials for pipeline code

## S3 Integration

An S3 bucket (`anushangar-crypto-pipeline-raw`) was created and integrated directly into the `CryptoExtractor` class, archiving the raw CoinGecko API response to S3 immediately after each successful extraction — before any transformation or validation occurs.

**Why this matters:** previously, raw extraction data only existed transiently in `/tmp/` files used for Airflow's inter-task handoff, with no permanent record. S3 now provides a genuine, permanent historical archive of every raw API pull, independent of the pipeline's processed/transformed output in PostgreSQL.

**Design decision — S3 failure is non-fatal:** the `archive_to_s3` method wraps its S3 call in a try/except that logs a warning on failure rather than raising an exception. This is deliberate: S3 archiving is a secondary, backup concern, while successfully loading current data into the star schema warehouse is the pipeline's primary purpose. A failed backup write should not discard a successful primary data load.

```python
def archive_to_s3(self, raw_json_text):
    timestamp = datetime.now().strftime('%Y-%m-%d_%H-%M-%S')
    key = f'raw/{timestamp}_crypto_data.json'
    try:
        self.s3_client.put_object(Bucket=self.s3_bucket, Key=key, Body=raw_json_text)
        logger.info(f"Archived raw data to S3: {key}")
    except Exception as e:
        logger.warning(f"S3 archive failed (non-critical): {e}")
```

**Credentials handling:** AWS Access Keys are loaded from a `.env` file via `python-dotenv`, following the same pattern already used for the PostgreSQL password in `profiles.yml` — real credentials never appear directly in code, and `.env` is excluded from version control at both the repository root and within individual week folders.

**Docker integration:** since Airflow tasks run inside a Docker container, the `.env` file was copied into the same directory already mounted for DAG files (`airflow-docker/dags/`), with `load_dotenv()` pointed explicitly at that path — the same volume-mounting pattern already established for making the dbt project and other configuration visible inside the container.

## RDS — Evaluated, Deliberately Deferred

Migrating the project's PostgreSQL database from local Windows hosting to AWS RDS (removing the dependency on a personal machine staying powered on) was researched and scoped, but intentionally not implemented in this phase.

**Reasoning:** AWS Cost Explorer and Budget alerts have a documented refresh delay of up to 24 hours between actual spend and visible billing data. Additionally, there are multiple documented cases — including a confirmed AWS-side metering bug — of free-tier RDS instances being billed unexpectedly even when correctly configured within stated free-tier limits. Given this combination of delayed cost feedback and documented billing edge cases, S3 integration was prioritized as this phase's cloud deliverable, since its cost profile (storage of a few KB per run) carries negligible risk by comparison.

RDS migration remains a planned next step, ideally implemented alongside AWS Budget Actions configured to automatically stop instances at a defined spending threshold — providing faster, automated protection than manual monitoring of a delayed dashboard alone.

## Files

```
week7/
├── crypto_pipeline_dag.py     # DAG with CryptoExtractor now archiving raw data to S3
├── test_s3_connection.py      # Initial connectivity test (lists buckets)
├── test_s3_upload.py          # Initial upload/read test
└── README.md
```

*Note: as with previous weeks, `crypto_pipeline_dag.py` here is a documentation snapshot. The live, running version resides in the Airflow project's `dags/` folder.*

## Tech Stack

Python · boto3 (AWS SDK) · python-dotenv · Amazon S3 · IAM · AWS Budgets

## What I Learned

Setting up billing safety net *before* touching any billable service — rather than after — turned out to matter more than any specific technical step this week. Understanding the actual mechanics of AWS's Free Plan (credit-based, auto-closing rather than auto-charging) removed most of the uncertainty around account creation. Researching RDS's real-world billing risk before creating anything, rather than after encountering a problem, was a deliberate application of the same "verify actual behavior, don't assume" principle that came out of debugging the Week 4 timestamp bug — in this case applied to infrastructure risk rather than a data bug.