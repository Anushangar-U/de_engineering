import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
import logging

#logging configuration
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s',
                    handlers=[logging.FileHandler('etl.log'),logging.StreamHandler()]
)

#extract
class Extractor:
    def extract(self):
        logging.info("starting the extraction...")
        data = {'product': ['Rice', 'Sugar', 'Flour', 'Salt', 'Oil'],
                'price_lkr': [180, 220, 150, 80, 350],
                'quantity': [100, 85, 200, 300, 60],
                'category': ['grain', 'sweetener', 'grain', 'seasoning', 'fat']
        }
        df = pd.DataFrame(data)
        logging.info(f"Extracted {len(df)} records")
        return df

#transform
class Transformer:
    def transform(self,df):
        logging.info("Starting transformation...")
        df['Total_value'] = df['price_lkr'] * df["quantity"]
        df["Price_category"] = df["price_lkr"].apply(lambda x: "expensive" if x>200 else "affordable")
        df = df[df["quantity"]>0]
        logging.info(f"Transformed {len(df)} records")
        return df

#load
class Loader:
    def __init__(self):
        self.host = "localhost"
        self.database = "Practise"
        self.user = "postgres"
        self.password = "Ugash"
    
    def load(self, df):
        logging.info("Load started")
        conn = None
        cursor = None
        try:
            conn = psycopg2.connect(
                host=self.host,
                database=self.database,
                user=self.user,
                password=self.password
            )
            cursor = conn.cursor()

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
            logging.info(f"Loaded {len(df)} records to PostgreSQL")

        except Exception as e:
            logging.error(f"Load failed: {e}")
            if conn is not None:
                conn.rollback()
        finally:
            if cursor is not None:
                cursor.close()
            if conn is not None:
                conn.close()

if __name__ == "__main__":
    logging.info("ETL pipeline started...")
    extractor = Extractor()
    transformer = Transformer()
    loader = Loader()
    raw = extractor.extract()
    clean = transformer.transform(raw)
    loader.load(clean)
    logging.info("ETL pipeline completed")
    
        
        
         
