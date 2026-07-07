import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
import logging
import os

os.makedirs("logs",exist_ok=True)

#creating logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

#formating
formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")

file_handler = logging.FileHandler("logs/etl_pipeline.log")
file_handler.setLevel(logging.DEBUG)
file_handler.setFormatter(formatter)

console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)
console_handler.setFormatter(formatter)

logger.addHandler(file_handler)
logger.addHandler(console_handler)

#extract
class Extractor:
    def extract(self):
        logger.info("starting the extraction...")
        data = {'product': ['Rice', 'Sugar', 'Flour', 'Salt', 'Oil'],
                'price_lkr': [180, 220, 150, 80, 350],
                'quantity': [100, 85, 200, 300, 60],
                'category': ['grain', 'sweetener', 'grain', 'seasoning', 'fat']
        }
        df = pd.DataFrame(data)
        logger.info(f"Extracted {len(df)} records")
        return df

#transform
class Transformer:
    def transform(self,df):
        logger.info("Starting transformation...")
        df['Total_value'] = df['price_lkr'] * df["quantity"]
        df["Price_category"] = df["price_lkr"].apply(lambda x: "expensive" if x>200 else "affordable")
        df = df[df["quantity"]>0]
        logger.info(f"Transformed {len(df)} records")
        return df

#load
class Loader:
    def __init__(self):
        self.host = "localhost"
        self.database = "Practise"
        self.user = "postgres"
        self.password = "Ugash"
    
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
            logger.debug("Database connection successfull")
            cursor = conn.cursor()
            
            logger.debug("Creating table if not exists...")
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS products_v2 (
                    id SERIAL PRIMARY KEY,
                    product VARCHAR(100),
                    price_lkr DECIMAL(10,2),
                    quantity INTEGER,
                    category VARCHAR(50),
                    total_value DECIMAL(10,2),
                    price_category VARCHAR(20),
                    loaded_at TIMESTAMP DEFAULT NOW()
                )
            """)

            records = df.values.tolist()
            execute_values(cursor, """
                INSERT INTO products_v2
                (product, price_lkr, quantity, category, total_value, price_category)
                VALUES %s
            """, records)

            conn.commit()
            logger.info(f"Loaded {len(df)} records to PostgreSQL")
            
        except psycopg2.OperationalError as e:
            logger.error(f"database connection failed:{e}")
            if conn is not None:
                conn.rollback()
        
        except psycopg2.Error as e:
            logger.error(f"database error during insert:{e}")
            if conn is not None:
                conn.rollback()

        except Exception as e:
            logger.error(f"Load failed: {e}")
            if conn is not None:
                conn.rollback()
        
        finally:
            if cursor is not None:
                cursor.close()
            if conn is not None:
                conn.close()
            logger.debug("Database connection closed")

if __name__ == "__main__":
    logger.info("ETL pipeline started...")
    extractor = Extractor()
    transformer = Transformer()
    loader = Loader()
    raw = extractor.extract()
    clean = transformer.transform(raw)
    loader.load(clean)
    logger.info("ETL pipeline completed")
    
        
        
         
