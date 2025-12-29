import requests
import json
import psycopg2
from psycopg2.extras import Json
import os
import subprocess
import sys
from dotenv import load_dotenv
from datetime import datetime
import time
import concurrent.futures
import queue
import threading

# Load env variables
load_dotenv()

# Database Configuration
DB_HOST = os.getenv("DB_HOST", "127.0.0.1")
DB_PORT = os.getenv("DB_PORT", "5433")
DB_NAME = os.getenv("DB_NAME", "localtoastd") 
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASSWORD = os.getenv("DB_PASSWORD", "kali")

# Global Queue for Producer-Consumer
data_queue = queue.Queue()
# Event to signal "done fetching"
done_event = threading.Event()

def create_database_if_not_exists():
    """Creates the database if it doesn't exist."""
    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            dbname="postgres",
            user=DB_USER,
            password=DB_PASSWORD
        )
        conn.autocommit = True
        cur = conn.cursor()
        
        cur.execute("SELECT 1 FROM pg_database WHERE datname = %s", (DB_NAME,))
        exists = cur.fetchone()
        
        if not exists:
            print(f"[INFO] Database '{DB_NAME}' does not exist. Creating it...")
            cur.execute(f'CREATE DATABASE "{DB_NAME}"')
            print(f"[INFO] Database '{DB_NAME}' created successfully.")
        
        cur.close()
        conn.close()
    except Exception as e:
        print(f"[WARNING] Could not check/create database '{DB_NAME}': {e}")

def get_db_connection():
    """Establishes connection to PostgreSQL database."""
    # Ensure DB exists first
    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD
        )
        return conn
    except Exception as e:
        print(f"Error connecting to database '{DB_NAME}': {e}")
        # Only exit in main thread context if critical, but here helper might be called in thread
        raise e 

def init_db(conn, reset=False):
    """Creates the bids table if it doesn't exist, updating schema if needed."""
    cur = conn.cursor()
    
    if reset:
        print("[INFO] Reset requested. Dropping table 'bids'...")
        cur.execute("DROP TABLE IF EXISTS bids")
        conn.commit()
    else:
        # Check for old schema
        check_query = """
        SELECT column_name FROM information_schema.columns 
        WHERE table_name='bids' AND column_name='raw_data';
        """
        cur.execute(check_query)
        if cur.fetchone():
            print("[INFO] Old schema detected. Recreating table 'bids'...")
            cur.execute("DROP TABLE bids")
            conn.commit()

    create_table_query = """
    CREATE TABLE IF NOT EXISTS bids (
        b_id BIGINT PRIMARY KEY,
        b_bid_number TEXT,
        b_category_name TEXT,
        b_cat_id TEXT,
        b_total_quantity INT,
        b_status INT,
        b_type INT,
        final_start_date_sort TIMESTAMP,
        final_end_date_sort TIMESTAMP,
        bd_category_name TEXT,
        b_eval_type INT,
        ministry_name TEXT,
        department_name TEXT,
        search_vector TSVECTOR,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    
    CREATE INDEX IF NOT EXISTS bids_search_idx ON bids USING GIN (search_vector);
    """
    cur.execute(create_table_query)
    conn.commit()
    cur.close()

def fetch_bids_page(search_bid, from_date, to_date, start_row, retries=3):
    """Fetches a single page of bid data. Runs in a worker thread."""
    url = "https://bidplus.gem.gov.in/all-bids-data"
    headers = {
        "accept": "application/json, text/javascript, */*; q=0.01",
        "accept-language": "en-US,en;q=0.9",
        "content-type": "application/x-www-form-urlencoded; charset=UTF-8",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "X-Requested-With": "XMLHttpRequest",
        "origin": "https://bidplus.gem.gov.in",
        "referer": "https://bidplus.gem.gov.in/all-bids",
        "cookie": "_ga=GA1.3.484596475.1761793171; _gid=GA1.3.2012138776.1767016991; csrf_gem_cookie=8cc1b7dd8d2b7a3bf24c8d202e66efba; GeM=1474969956.20480.0000; TS01dc9e29=01e393167d41a87ef9148e2474363d258331045994ac93a6149a797dd7aa039861810c0ebf548e69a073d949244cf077fa4b3bd6b21cdb2943eca9160a7e1984637852f2a6; ci_session=f0448a5079d2e59679c5e00b8a2abe6c1c7133fa; TS0174a79d=01e393167d8212e6a7b01d2891a0115d589e25d300db9603187096b91ddc9ec081fc215ab39cfe607265e2129cdfaac1c1ee68435f1f814c26e65eae311061db80d197765115707d9b0f50a07d2bab97cb4c441ae5137a2977bc257408d15b5e198c708eba"
    }

    payload_dict = {
        "param": {
            "searchBid": search_bid, 
            "searchType": "fullText",
            "start": start_row
        },
        "filter": {
            "bidStatusType": "bidrastatus",
            "byType": "all",
            "highBidValue": "",
            "byEndDate": {"from": from_date, "to": to_date},
            "sort": "Bid-End-Date-Latest",
            "byStatus": ""
        }
    }
    
    data = {"payload": json.dumps(payload_dict)}
    data["csrf_bd_gem_nk"] = "8cc1b7dd8d2b7a3bf24c8d202e66efba" 

    for attempt in range(retries):
        try:
            response = requests.post(url, headers=headers, data=data, timeout=30)
            response.raise_for_status()
            
            json_data = response.json()
            if json_data and "response" in json_data and "response" in json_data["response"] and "docs" in json_data["response"]["response"]:
                docs = json_data["response"]["response"]["docs"]
                # Put results into generic queue
                data_queue.put(docs)
                return True
            else:
                if attempt == retries - 1:
                    print(f"[WARN] Empty or valid but empty response at {start_row}")
                return False
        except Exception as e:
            if attempt < retries - 1:
                time.sleep(1 + attempt) # Backoff
            else:
                print(f"[Error] Failed fetching start={start_row}: {e}")
                return False

def db_worker():
    """Consumer thread: reads from queue and saves to DB."""
    try:
        conn = get_db_connection()
    except Exception as e:
        print(f"[FATAL] DB Worker failed to connect: {e}")
        return

    cur = conn.cursor()
    global total_fetched
    total_fetched = 0

    while True:
        try:
            # Block for 2 seconds waiting for item, then check done_event
            docs = data_queue.get(timeout=2) 
        except queue.Empty:
            if done_event.is_set():
                break
            continue
        
        count = 0
        for doc in docs:
            try:
                def get_val(key, default=None):
                    val = doc.get(key)
                    if isinstance(val, list):
                        return val[0] if val else default
                    return val if val is not None else default

                b_id = get_val("b_id", 0)
                if not b_id:
                    b_id = int(doc.get("id"))
                
                b_bid_num = get_val("b_bid_number", "")
                b_cat = get_val("b_category_name", "")
                bd_cat = get_val("bd_category_name", "")
                b_cat_id = get_val("b_cat_id", "")
                
                qty = get_val("b_total_quantity", 0)
                status = get_val("b_status", 0)
                b_type = get_val("b_type", 0)
                eval_type = get_val("b_eval_type", 0)
                
                start_date = get_val("final_start_date_sort")
                end_date = get_val("final_end_date_sort")
                
                min_name = get_val("ba_official_details_minName", "")
                dept_name = get_val("ba_official_details_deptName", "")

                sv_text = f"{b_cat or ''} {bd_cat or ''} {b_cat_id or ''} {min_name or ''} {dept_name or ''} {b_bid_num or ''}"
                
                query = """
                INSERT INTO bids (
                    b_id, b_bid_number, b_category_name, b_cat_id, b_total_quantity, b_status, b_type,
                    final_start_date_sort, final_end_date_sort, bd_category_name, b_eval_type,
                    ministry_name, department_name, search_vector
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, to_tsvector('english', %s))
                ON CONFLICT (b_id) DO UPDATE SET
                    b_status = EXCLUDED.b_status,
                    search_vector = EXCLUDED.search_vector,
                    final_end_date_sort = EXCLUDED.final_end_date_sort;
                """
                cur.execute(query, (
                    b_id, b_bid_num, b_cat, b_cat_id, qty, status, b_type,
                    start_date, end_date, bd_cat, eval_type,
                    min_name, dept_name, sv_text
                ))
                count += 1
            except Exception as e:
                # print(f"Skipping doc ID {doc.get('id', 'unknown')}: {e}")
                pass
                
        conn.commit()
        total_fetched += count
        data_queue.task_done()
    
    cur.close()
    conn.close()

def create_backup():
    """Creates a database backup using pg_dump."""
    print("\nCreating database backup...")
    filename = f"Bids_backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}.sql"
    env = os.environ.copy()
    env["PGPASSWORD"] = DB_PASSWORD
    
    cmd = [
        "pg_dump",
        "-h", DB_HOST,
        "-p", str(DB_PORT),
        "-U", DB_USER,
        "-f", filename,
        DB_NAME
    ]
    
    try:
        subprocess.run(cmd, env=env, check=True)
        print(f"Backup created successfully: {filename}")
    except Exception as e:
        print(f"Backup error: {e}")

def main():
    print("====================================")
    print("       Gem Bids Scraper v3.0        ")
    print("       (Parallel Execution)         ")
    print("====================================")
    
    search_bid = input("Enter Search Bid Keyword (optional): ").strip()
    from_date = input("Enter From Date (dd-mm-yyyy, optional): ").strip()
    to_date = input("Enter To Date (dd-mm-yyyy, optional): ").strip()
    reset_input = input("Delete existing data and start fresh? (y/n): ").strip().lower()
    reset_db = reset_input == 'y'
    
    workers_input = input("Enter number of worker threads (default 5): ").strip()
    max_workers = int(workers_input) if workers_input.isdigit() and int(workers_input) > 0 else 5
    
    print("[INFO] Checking Database...")
    create_database_if_not_exists()
    
    try:
        conn = get_db_connection()
        init_db(conn, reset=reset_db)
        conn.close()
    except Exception as e:
        print(f"[FATAL] Could not connect to DB: {e}")
        return

    print("\n[INFO] Starting Parallel Scrape...")
    
    # Start DB Worker Thread
    consumer_thread = threading.Thread(target=db_worker, daemon=True)
    consumer_thread.start()
    
    # 1. Fetch first page to get Total Count
    first_page_url = "https://bidplus.gem.gov.in/all-bids-data"
    # Reusing payload construction locally to get total count safely
    # ideally we should modify fetch_bids_page to return metadata, but due to thread pool nature
    # it is easier to do one synchronous call first.
    
    # Quick headers/payload for first call
    headers = {
        "accept": "application/json, text/javascript, */*; q=0.01",
        "content-type": "application/x-www-form-urlencoded; charset=UTF-8",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "X-Requested-With": "XMLHttpRequest",
        "origin": "https://bidplus.gem.gov.in",
        "referer": "https://bidplus.gem.gov.in/all-bids",
        "cookie": "_ga=GA1.3.484596475.1761793171; _gid=GA1.3.2012138776.1767016991; csrf_gem_cookie=8cc1b7dd8d2b7a3bf24c8d202e66efba; GeM=1474969956.20480.0000; TS01dc9e29=01e393167d41a87ef9148e2474363d258331045994ac93a6149a797dd7aa039861810c0ebf548e69a073d949244cf077fa4b3bd6b21cdb2943eca9160a7e1984637852f2a6; ci_session=f0448a5079d2e59679c5e00b8a2abe6c1c7133fa; TS0174a79d=01e393167d8212e6a7b01d2891a0115d589e25d300db9603187096b91ddc9ec081fc215ab39cfe607265e2129cdfaac1c1ee68435f1f814c26e65eae311061db80d197765115707d9b0f50a07d2bab97cb4c441ae5137a2977bc257408d15b5e198c708eba"
    }
    payload_dict = {
        "param": {"searchBid": search_bid, "searchType": "fullText", "start": 0},
        "filter": {
            "bidStatusType": "bidrastatus", "byType": "all", "highBidValue": "",
            "byEndDate": {"from": from_date, "to": to_date},
            "sort": "Bid-End-Date-Latest", "byStatus": ""
        }
    }
    data = {"payload": json.dumps(payload_dict), "csrf_bd_gem_nk": "8cc1b7dd8d2b7a3bf24c8d202e66efba"}
    
    total_available = 0
    try:
        resp = requests.post(first_page_url, headers=headers, data=data) 
        j = resp.json()
        total_available = j['response']['response'].get('numFound', 0)
        print(f"[INFO] Total records found: {total_available}")
        
        # Manually put the first batch
        if "docs" in j["response"]["response"]:
            data_queue.put(j["response"]["response"]["docs"])
            
    except Exception as e:
        print(f"[FATAL] Failed initial fetch: {e}")
        return

    # 2. Spawn Workers
    # We start from row 10 since we fetched 0 already
    start_time = time.time()
    page_size = 10
    
    # Generate list of start offsets
    # Limit max scraping to avoid infinite loop on huge datasets during demo/testing if needed
    # But user wants all.
    offsets = range(10, total_available, page_size)
    
    print(f"[INFO] Spawning {max_workers} workers for {len(offsets)} pages...")
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(fetch_bids_page, search_bid, from_date, to_date, o): o for o in offsets}
        
        # Monitoring loop
        completed = 0
        while not done_event.is_set():
            # Check futures status
            # Actually, futures will finish on their own. We just need to monitor Progress.
            # But the 'total_fetched' is updating in the consumer thread.
            
            elapsed_time = time.time() - start_time
            if total_fetched > 0 and elapsed_time > 0:
                rate = total_fetched / elapsed_time
                remaining = total_available - total_fetched
                eta_s = remaining / rate if rate > 0 else 0
                eta_str = str(time.strftime('%H:%M:%S', time.gmtime(eta_s)))
                
                # Overwrite line
                sys.stdout.write(f"\r[PROGRESS] Saved: {total_fetched}/{total_available} | Rate: {rate:.1f}/s | ETA: {eta_str}   ")
                sys.stdout.flush()
            
            # Check if all futures are done
            # A simple way is to check data_queue size or use shared counter for 'processed pages'
            # But 'futures' dict holds them.
            
            pending = [f for f in futures if not f.done()]
            if not pending and data_queue.empty():
                done_event.set()
                print("\n[INFO] All tasks completed.")
                break
            
            time.sleep(1)

    # Wait for consumer to finish writing everything
    data_queue.join()
    
    # Final check
    elapsed_time = time.time() - start_time
    print(f"\n[SUCCESS] Completed in {elapsed_time:.2f}s. Total Saved: {total_fetched}")
    
    if total_fetched > 0:
        create_backup()

if __name__ == "__main__":
    main()
