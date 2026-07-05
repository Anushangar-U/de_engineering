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
def extract():
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
def transform(df):
    logging.info("Starting transformation...")
    df['Total_value'] = df['price_lkr'] * df["quantity"]
    df["Price_category"] = df["price_lkr"].apply(lambda x: "expensive" if x>200 else "affordable")
    df = df[df["quantity"]>0]
    logging.info(f"Transformed {len(df)} records")
    return df

#load
def load(df):
    logging.info("Starting load...")
    try:
        conn = psycopg2.connect(host="localhost",
                                database="Practise",
                                user="postgres",
                                password="Ugash"
        )
        cursor = conn.cursor()
        
        cursor.execute("""
                       create table if not exists products(
                           id serial primary key,
                           product varchar(100),
                           price_lkr decimal(10,2),
                           quantity integer,
                           category varchar(50),
                           total_value DECIMAL(10,2),
                           price_category VARCHAR(20),
                           loaded_at TIMESTAMP DEFAULT NOW()
                       )
         """)
        
        records = df.values.tolist()
        execute_values(cursor,"""
                       insert into products
                       (product, price_lkr, quantity, category, total_value, price_category)
                       values %s
                       """,records)
        
        conn.commit()
        logging.info(f"loaded {len(df)} records to postgreSQL")
        
    except Exception as e:
        logging.error(f"load failed: {e}")
        conn.rollback()
        
    finally:
        cursor.close()
        conn.close()
        
if __name__ == "__main__":
    logging.info("ETL pipeline started...")
    raw_data = extract()
    clean_data = transform(raw_data)
    load(clean_data)
    logging.info("ETL pipeline completed")
    
        
        
         
