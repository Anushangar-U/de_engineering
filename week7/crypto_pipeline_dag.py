from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime
import requests
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
import logging
import os
import urllib3
import pytz
from datetime import datetime, timedelta
from airflow.utils.task_group import TaskGroup
from airflow.operators.bash import BashOperator
import boto3
from dotenv import load_dotenv

load_dotenv()

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Logging setup
os.makedirs('/opt/airflow/logs/crypto_pipeline', exist_ok=True)
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
file_handler = logging.FileHandler('/opt/airflow/logs/crypto_pipeline/api_etl.log')
file_handler.setLevel(logging.DEBUG)
file_handler.setFormatter(formatter)
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)
console_handler.setFormatter(formatter)
logger.addHandler(file_handler)
logger.addHandler(console_handler)


class CryptoExtractor:
    def __init__(self):
        self.url = "https://api.coingecko.com/api/v3/coins/markets"
        self.params = {
            'vs_currency': 'usd',
            'order': 'market_cap_desc',
            'per_page': 10,
            'page': 1
        }
        self.s3_bucket = 'anushangar-crypto-pipeline-raw'
        self.s3_client = boto3.client(
            's3',
            aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
            aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
            region_name='eu-north-1'
        )
        
    def archive_to_s3(self, raw_json_text):
        timestamp = datetime.now().strftime('%Y-%m-%d_%H-%M-%S')
        key = f'raw/{timestamp}_crypto_data.json'
        try:
            self.s3_client.put_object(
                Bucket=self.s3_bucket,
                Key=key,
                Body=raw_json_text
            )
            logger.info(f"Archived raw data to S3: {key}")
        except Exception as e:
            logger.warning(f"S3 archive failed (non-critical): {e}")

    def extract(self):
        logger.info("Starting crypto API extraction...")
        try:
            response = requests.get(self.url, params=self.params, timeout=10, verify=False)
            response.raise_for_status()
            self.archive_to_s3(response.text)
            data = response.json()
            df = pd.DataFrame(data)
            logger.info(f"Extracted {len(df)} records from CoinGecko")
            return df
        except requests.exceptions.ConnectionError as e:
            logger.error(f"API connection failed: {e}")
            return None
        except requests.exceptions.Timeout as e:
            logger.error(f"API timed out: {e}")
            return None
        except requests.exceptions.HTTPError as e:
            logger.error(f"HTTP error: {e}")
            return None


class CryptoTransformer:
    def transform(self, df):
        logger.info("Starting transformation...")
        df = df[[
            'id', 'symbol', 'name', 'current_price', 'market_cap',
            'market_cap_rank', 'total_volume', 'high_24h', 'low_24h',
            'price_change_24h', 'price_change_percentage_24h', 'last_updated'
        ]].copy()
        df['last_updated'] = pd.to_datetime(df['last_updated'])
        df['last_updated'] = df['last_updated'].dt.tz_localize(None)
        df['extracted_at'] = datetime.now()
        before = len(df)
        df = df.dropna(subset=['current_price'])
        after = len(df)
        if before - after > 0:
            logger.warning(f"Dropped {before - after} rows with null price")
        logger.info(f"Transformed {len(df)} records")
        return df


class DataValidator:
    def validate(self, df):
        logger.info("Starting data validation...")
        original_count = len(df)
        df = self._check_nulls(df)
        df = self._check_duplicates(df)
        df = self._check_price_range(df)
        df = self._check_high_low_logic(df)
        final_count = len(df)
        if original_count - final_count > 0:
            logger.warning(f"Validation removed {original_count - final_count} records total")
        logger.info(f"Validation complete - {final_count} clean records remain")
        return df

    def _check_nulls(self, df):
        before = len(df)
        df = df.dropna(subset=['id', 'symbol', 'current_price'])
        after = len(df)
        if before - after > 0:
            logger.warning(f"Null check: removed {before - after} records")
        return df

    def _check_duplicates(self, df):
        before = len(df)
        df = df.drop_duplicates(subset=['id'])
        after = len(df)
        if before - after > 0:
            logger.warning(f"Duplicate check: removed {before - after} records")
        return df

    def _check_price_range(self, df):
        before = len(df)
        df = df[df['current_price'] > 0]
        after = len(df)
        if before - after > 0:
            logger.warning(f"Price range check: removed {before - after} records")
        return df

    def _check_high_low_logic(self, df):
        before = len(df)
        df = df[df['high_24h'] >= df['low_24h']]
        after = len(df)
        if before - after > 0:
            logger.warning(f"High/low logic check: removed {before - after} records")
        return df


class CryptoLoader:
    def __init__(self):
        self.host = "host.docker.internal"
        self.database = "Practise"
        self.user = "postgres"
        self.password = "Ugash"

    def get_watermark(self, conn):
        cursor = conn.cursor()
        cursor.execute("""
            SELECT last_run FROM pipeline_watermark
            WHERE pipeline_name = 'crypto_pipeline'
        """)
        result = cursor.fetchone()
        cursor.close()
        watermark = result[0]
        watermark = pytz.utc.localize(watermark) if watermark.tzinfo is None else watermark
        logger.info(f"Last run watermark: {watermark}")
        return watermark

    def update_watermark(self, conn):
        cursor = conn.cursor()
        cursor.execute("""
            UPDATE pipeline_watermark
            SET last_run = NOW()
            WHERE pipeline_name = 'crypto_pipeline'
        """)
        cursor.close()
        logger.info("Watermark updated")

    def upsert_coin(self, conn, coin_id, symbol, name):
        cursor = conn.cursor()
        cursor.execute("""
            INSERT INTO DimCoin (coin_id, symbol, name)
            VALUES (%s, %s, %s)
            ON CONFLICT (coin_id)
            DO UPDATE SET symbol = EXCLUDED.symbol, name = EXCLUDED.name
            RETURNING coin_key
        """, (coin_id, symbol, name))
        coin_key = cursor.fetchone()[0]
        cursor.close()
        return coin_key

    def get_or_create_date_key(self, conn, timestamp):
        cursor = conn.cursor()
        date_key = int(timestamp.strftime('%Y%m%d%H'))
        cursor.execute("""
            SELECT date_key FROM DimDate WHERE date_key = %s
        """, (date_key,))
        result = cursor.fetchone()

        if result is None:
            cursor.execute("""
                INSERT INTO DimDate (date_key, full_date, year, month, day, hour, day_of_week, is_weekend)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (date_key) DO NOTHING
            """, (
                date_key,
                timestamp.date(),
                timestamp.year,
                timestamp.month,
                timestamp.day,
                timestamp.hour,
                timestamp.strftime('%A'),
                timestamp.isoweekday() in (6, 7)
            ))
            logger.debug(f"Created new DimDate entry for {date_key}")

        cursor.close()
        return date_key

    def load(self, df):
        logger.info("Load started")
        conn = None
        try:
            conn = psycopg2.connect(
                host=self.host, database=self.database,
                user=self.user, password=self.password
            )

            watermark = self.get_watermark(conn)
            df['last_updated'] = pd.to_datetime(df['last_updated'])
            new_records = df[df['last_updated'] > watermark.replace(tzinfo=None)]

            if len(new_records) == 0:
                logger.info("No new records to load")
                return

            logger.info(f"Loading {len(new_records)} new records")

            for _, row in new_records.iterrows():
                coin_key = self.upsert_coin(conn, row['id'], row['symbol'], row['name'])
                date_key = self.get_or_create_date_key(conn, row['last_updated'])

                cursor = conn.cursor()
                cursor.execute("""
                    INSERT INTO FactCryptoPrices (
                        coin_key, date_key, current_price, market_cap,
                        market_cap_rank, total_volume, high_24h, low_24h,
                        price_change_24h, price_change_percentage_24h
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    coin_key, date_key, row['current_price'], row['market_cap'],
                    row['market_cap_rank'], row['total_volume'], row['high_24h'],
                    row['low_24h'], row['price_change_24h'], row['price_change_percentage_24h']
                ))
                cursor.close()

            self.update_watermark(conn)
            conn.commit()
            logger.info(f"Successfully loaded {len(new_records)} records into star schema")

        except psycopg2.OperationalError as e:
            logger.error(f"Database connection failed: {e}")
            if conn is not None:
                conn.rollback()
        except psycopg2.Error as e:
            logger.error(f"Database error: {e}")
            if conn is not None:
                conn.rollback()
        except Exception as e:
            logger.error(f"Unexpected error: {e}")
            if conn is not None:
                conn.rollback()
        finally:
            if conn is not None:
                conn.close()


# Airflow task functions
def extract_task_func():
    extractor = CryptoExtractor()
    df = extractor.extract()
    if df is None:
        raise ValueError("Extraction failed - stopping pipeline")
    df.to_json('/tmp/crypto_raw.json')

def transform_task_func():
    df = pd.read_json('/tmp/crypto_raw.json')
    transformer = CryptoTransformer()
    clean = transformer.transform(df)
    clean.to_json('/tmp/crypto_clean.json')

def validate_task_func():
    df = pd.read_json('/tmp/crypto_clean.json')
    validator = DataValidator()
    validated = validator.validate(df)
    validated.to_json('/tmp/crypto_validated.json')

def load_task_func():
    df = pd.read_json('/tmp/crypto_validated.json')
    df['last_updated'] = pd.to_datetime(df['last_updated'], unit='ms')
    loader = CryptoLoader()
    loader.load(df)


with DAG(
    dag_id='crypto_etl_pipeline',
    start_date=datetime(2026, 1, 1),
    schedule='@hourly',
    catchup=False
) as dag:

    with TaskGroup("ingestion") as ingestion_group:
        extract_task = PythonOperator(
            task_id='extract',
            python_callable=extract_task_func,
            retries=3,
            retry_delay=timedelta(minutes=1)
        )

        transform_task = PythonOperator(
            task_id='transform',
            python_callable=transform_task_func,
            retries=3,
            retry_delay=timedelta(minutes=1)
        )

        extract_task >> transform_task

    with TaskGroup("processing") as processing_group:
        validate_task = PythonOperator(
            task_id='validate',
            python_callable=validate_task_func,
            retries=3,
            retry_delay=timedelta(minutes=1)
        )

        load_task = PythonOperator(
            task_id='load',
            python_callable=load_task_func,
            retries=3,
            retry_delay=timedelta(minutes=1)
        )

        validate_task >> load_task

ingestion_group >> processing_group

dbt_run_task = BashOperator(
    task_id='dbt_run',
    bash_command='cd /opt/airflow/crypto_dbt && dbt run --profiles-dir .',
    retries=3,
    retry_delay=timedelta(minutes=1)
)

processing_group >> dbt_run_task