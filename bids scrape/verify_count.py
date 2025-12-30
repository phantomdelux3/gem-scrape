import os
import psycopg2
from dotenv import load_dotenv

load_dotenv()

DB_HOST = os.getenv("DB_HOST", "127.0.0.1")
DB_PORT = os.getenv("DB_PORT", "5433")
DB_NAME = os.getenv("DB_NAME", "localtoastd") 
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASSWORD = os.getenv("DB_PASSWORD", "kali")

print(f"Connecting to {DB_NAME} on {DB_HOST}:{DB_PORT} as {DB_USER}")

try:
    conn = psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD
    )
    cur = conn.cursor()
    cur.execute("SELECT count(*) FROM bids")
    count = cur.fetchone()[0]
    print(f"Total records in 'bids' table: {count}")
    cur.close()
    conn.close()
except Exception as e:
    print(f"Error: {e}")
