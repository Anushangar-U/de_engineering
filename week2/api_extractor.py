import requests
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
import logging
import os
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
from datetime import datetime

# Logging setup
os.makedirs('week2/logs', exist_ok=True)
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
file_handler = logging.FileHandler('week2/logs/api_etl.log')
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
    
    def extract(self):
        logger.info("Starting crypto API extraction...")
        try:
            logger.debug(f"Calling API: {self.url}")
            response = requests.get(
                self.url, 
                params=self.params,
                timeout=10,
                verify = False
            )
            response.raise_for_status()
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
        
        # Keep only useful columns
        df = df[[
            'id',
            'symbol',
            'name',
            'current_price',
            'market_cap',
            'market_cap_rank',
            'total_volume',
            'high_24h',
            'low_24h',
            'price_change_24h',
            'price_change_percentage_24h',
            'last_updated'
        ]].copy()
        
        # Convert last_updated to proper timestamp
        df['last_updated'] = pd.to_datetime(df['last_updated'])
        
        # Add extraction timestamp
        df['extracted_at'] = datetime.now()
        
        # Remove any rows with null current_price
        before = len(df)
        df = df.dropna(subset=['current_price'])
        after = len(df)
        
        if before - after > 0:
            logger.warning(f"Dropped {before - after} rows with null price")
        
        logger.info(f"Transformed {len(df)} records")
        return df


class CryptoLoader:
    def __init__(self):
        self.host = "localhost"
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
        
        # Make watermark timezone-aware (UTC)
        import pytz
        watermark = pytz.utc.localize(watermark)
        
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
        
    def load(self, df):
        logger.info("Load started")
        conn = None
        cursor = None
        try:
            logger.debug("Attempting database connection...")
            conn = psycopg2.connect(
                host=self.host,
                database=self.database,
                user=self.user,
                password=self.password
            )
            logger.debug("Database connection successful")
            cursor = conn.cursor()

            # Create table if not exists
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS crypto_prices (
                    id SERIAL PRIMARY KEY,
                    coin_id VARCHAR(50),
                    symbol VARCHAR(20),
                    name VARCHAR(100),
                    current_price DECIMAL(20,8),
                    market_cap BIGINT,
                    market_cap_rank INTEGER,
                    total_volume BIGINT,
                    high_24h DECIMAL(20,8),
                    low_24h DECIMAL(20,8),
                    price_change_24h DECIMAL(20,8),
                    price_change_percentage_24h DECIMAL(10,4),
                    last_updated TIMESTAMP,
                    extracted_at TIMESTAMP
                )
            """)

            # Get watermark
            watermark = self.get_watermark(conn)

            # Filter only new records
            df['extracted_at'] = pd.to_datetime(df['extracted_at'])
            new_records = df[df['last_updated'] > watermark]

            if len(new_records) == 0:
                logger.info("No new records to load")
                return

            logger.info(f"Loading {len(new_records)} new records")
            records = new_records.values.tolist()
            execute_values(cursor, """
                INSERT INTO crypto_prices
                (coin_id, symbol, name, current_price, market_cap,
                market_cap_rank, total_volume, high_24h, low_24h,
                price_change_24h, price_change_percentage_24h,
                last_updated, extracted_at)
                VALUES %s
            """, records)

            # Update watermark after successful load
            self.update_watermark(conn)
            conn.commit()
            logger.info(f"Successfully loaded {len(new_records)} records")

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
            if cursor is not None:
                cursor.close()
            if conn is not None:
                conn.close()
            logger.debug("Database connection closed")

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
        critical_columns = ['id', 'symbol', 'current_price']
        df = df.dropna(subset=critical_columns)
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
            logger.warning(f"Price range check: removed {before - after} records with invalid price")
        return df
    
    def _check_high_low_logic(self, df):
        before = len(df)
        df = df[df['high_24h'] >= df['low_24h']]
        after = len(df)
        if before - after > 0:
            logger.warning(f"High/low logic check: removed {before - after} records")
        return df


if __name__ == "__main__":
    extractor = CryptoExtractor()
    transformer = CryptoTransformer()
    validator = DataValidator()
    loader = CryptoLoader()
    
    raw = extractor.extract()
    if raw is not None:
        clean = transformer.transform(raw)
        validated = validator.validate(clean)
        loader.load(validated)
    else:
        logger.error("Extraction failed - pipeline stopped")