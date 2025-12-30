import os
import psycopg2
from dotenv import load_dotenv

load_dotenv()

DB_HOST = os.getenv("DB_HOST", "127.0.0.1")
DB_PORT = os.getenv("DB_PORT", "5433")
DB_NAME = os.getenv("DB_NAME", "localtoastd") 
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASSWORD = os.getenv("DB_PASSWORD", "kali")

bids_to_check = [
    # Page 2
    "GEM/2025/B/6945003",
    "GEM/2025/B/6977543",
    "GEM/2025/B/6977659",
    "GEM/2025/B/6977915",
    "GEM/2025/B/6978038",
    "GEM/2025/B/6977967",
    "GEM/2025/B/6977945",
    "GEM/2025/B/6972606",
    "GEM/2025/B/6967598",
    "GEM/2025/B/6967461",
    # Page 3 (Unique ones)
    "GEM/2025/B/6908277",
    "GEM/2025/B/6967435",
    "GEM/2025/B/6788197",
    "GEM/2025/B/6978183",
    # Page 4
    "GEM/2025/B/6978111",
    "GEM/2025/B/6978093",
    "GEM/2025/B/6978050",
    "GEM/2025/B/6977984",
    "GEM/2025/B/6970917",
    "GEM/2025/B/6968877",
    "GEM/2025/B/6959002",
    "GEM/2025/B/6958141",
    "GEM/2025/B/6978064",
    "GEM/2025/B/6951989"
]

try:
    conn = psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD
    )
    cur = conn.cursor()
    
    print(f"Checking {len(bids_to_check)} specific bids...")
    
    found_count = 0
    missing_bids = []
    
    for bid_num in bids_to_check:
        cur.execute("SELECT 1 FROM bids WHERE b_bid_number = %s", (bid_num,))
        if cur.fetchone():
            found_count += 1
        else:
            missing_bids.append(bid_num)
            
    print(f"\nFound: {found_count}/{len(bids_to_check)}")
    
    if missing_bids:
        print("\nMissing Bids:")
        for b in missing_bids:
            print(f" - {b}")
    else:
        print("\nAll User Verification Bids Found!")
        
    cur.close()
    conn.close()
except Exception as e:
    print(f"Error: {e}")
