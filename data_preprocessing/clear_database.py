import psycopg2
from config import Config
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

TABLES_TO_CLEAR = [
    'content_chunks',
    'document_content',
    'file_metadata',
    'scan_sessions',
    'file_duplicates'
]

def get_connection():
    return psycopg2.connect(
        host=Config.DB_HOST,
        port=Config.DB_PORT,
        database=Config.DB_NAME,
        user=Config.DB_USER,
        password=Config.DB_PASSWORD
    )

def clear_tables():
    conn = get_connection()
    cur = conn.cursor()
    try:
        for table in TABLES_TO_CLEAR:
            logging.info(f"Clearing table: {table}")
            cur.execute(f"TRUNCATE TABLE {table} RESTART IDENTITY CASCADE;")
        conn.commit()
        logging.info("All specified tables have been cleared.")
    except Exception as e:
        logging.error(f"Error clearing tables: {e}")
        conn.rollback()
    finally:
        cur.close()
        conn.close()

def main():
    confirm = input("Are you sure you want to clear ALL data from the database? This cannot be undone! (yes/no): ")
    if confirm.lower() == 'yes':
        clear_tables()
    else:
        print("Aborted. No data was deleted.")

if __name__ == "__main__":
    main()