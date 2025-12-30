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
import argparse

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
DEBUG_MODE = False

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

def fetch_bids_page(search_bid, from_date, to_date, page_num, retries=3):
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
        "cookie": "csrf_gem_cookie=e331d254a1625325352a675d7eff471e; ci_session=e72373b6e945b9800026970b0837c53951942c57; TS0174a79d=01e393167d4a963be55d4c4af088a2d434fbd183521b8287cee50260d2dd642cd6618da0b8c76ab225e315b68566bd19fe820be4947b8f7ac675ef7912a451446b2133190d32eafe14ead8175e1d7dec7eeeff542e9388afc22660f2eec96b049cd2932333; GeM=1474969956.20480.0000; _ga=GA1.3.484596475.1761793171; _gid=GA1.3.2012138776.1767016991"
    }

    payload_dict = {
        "page": page_num,
        "param": {
            "searchBid": search_bid, 
            "searchType": "fullText"
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
    data["csrf_bd_gem_nk"] = "e331d254a1625325352a675d7eff471e" 

    # print(f"[DEBUG] Fetching Page {page_num}...") 
    if DEBUG_MODE:
        print(f"[DEBUG] Fetching Page {page_num}...")

    for attempt in range(retries):
        try:
            response = requests.post(url, headers=headers, data=data, timeout=30)
            response.raise_for_status()
            
            json_data = response.json()
            if json_data and "response" in json_data and "response" in json_data["response"] and "docs" in json_data["response"]["response"]:
                docs = json_data["response"]["response"]["docs"]
                # Put results into generic queue
                data_queue.put(docs)
                print(f"[INFO] Page {page_num} fetched ({len(docs)} items)")
                return True
            else:
                if attempt == retries - 1:
                    print(f"[WARN] Empty/invalid response for Page {page_num}")
                return False
        except Exception as e:
            if attempt < retries - 1:
                time.sleep(1 + attempt) # Backoff
            else:
                print(f"[Error] Failed Page {page_num}: {e}")
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
    
    parser = argparse.ArgumentParser(description="Gem Bids Scraper")
    parser.add_argument("-t", "--test", action="store_true", help="Run in test mode (limit to 10 pages)")
    parser.add_argument("--reset", action="store_true", help="Reset the database (drop table)")
    parser.add_argument("-w", "--workers", type=int, default=20, help="Number of worker threads (default: 20)")
    parser.add_argument("--search", type=str, default="", help="Search keyword")
    parser.add_argument("--from_date", type=str, default="", help="From Date (dd-mm-yyyy)")
    parser.add_argument("--to_date", type=str, default="", help="To Date (dd-mm-yyyy)")
    parser.add_argument("--pages", type=int, default=None, help="Specific page limit (overrides --test)")
    parser.add_argument("-d", "--debug", action="store_true", help="Enable debug logging")
    parser.add_argument("--interactive", action="store_true", help="Force interactive mode")
    
    # Default to interactive if no args provided
    if len(sys.argv) == 1:
        sys.argv.append('--interactive')
        
    args = parser.parse_args()

    global DEBUG_MODE
    DEBUG_MODE = args.debug
    
    # Determine mode
    if args.interactive:
        search_bid = input("Enter Search Bid Keyword (optional): ").strip()
        from_date = input("Enter From Date (dd-mm-yyyy, optional): ").strip()
        to_date = input("Enter To Date (dd-mm-yyyy, optional): ").strip()
        reset_input = input("Delete existing data and start fresh? (y/n): ").strip().lower()
        reset_db = reset_input == 'y'
        
        workers_input = input("Enter number of worker threads (default 20): ").strip()
        max_workers = int(workers_input) if workers_input.isdigit() and int(workers_input) > 0 else 20
        
        limit_pages_input = input("Limit number of pages to scrape (optional, press Enter for all): ").strip()
        max_pages = int(limit_pages_input) if limit_pages_input.isdigit() and int(limit_pages_input) > 0 else None
    else:
        # CLI Mode
        search_bid = args.search
        from_date = args.from_date
        to_date = args.to_date
        reset_db = args.reset
        max_workers = args.workers
        
        if args.pages:
            max_pages = args.pages
        elif args.test:
             print("[INFO] Test mode enabled: Limiting to 10 pages.")
             max_pages = 10
        else:
            max_pages = None
    
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
    
    # Quick headers/payload for first call
    headers = {
        "accept": "application/json, text/javascript, */*; q=0.01",
        "accept-language": "en-US,en;q=0.9",
        "content-type": "application/x-www-form-urlencoded; charset=UTF-8",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "X-Requested-With": "XMLHttpRequest",
        "origin": "https://bidplus.gem.gov.in",
        "referer": "https://bidplus.gem.gov.in/all-bids",
        "cookie": "csrf_gem_cookie=e331d254a1625325352a675d7eff471e; ci_session=e72373b6e945b9800026970b0837c53951942c57; TS0174a79d=01e393167d4a963be55d4c4af088a2d434fbd183521b8287cee50260d2dd642cd6618da0b8c76ab225e315b68566bd19fe820be4947b8f7ac675ef7912a451446b2133190d32eafe14ead8175e1d7dec7eeeff542e9388afc22660f2eec96b049cd2932333; GeM=1474969956.20480.0000; _ga=GA1.3.484596475.1761793171; _gid=GA1.3.2012138776.1767016991"
    }
    
    payload_dict = {
        "param": {"searchBid": search_bid, "searchType": "fullText", "start": 0},
        "filter": {
            "bidStatusType": "bidrastatus", "byType": "all", "highBidValue": "",
            "byEndDate": {"from": from_date, "to": to_date},
            "sort": "Bid-End-Date-Latest", "byStatus": ""
        }
    }
    data = {"payload": json.dumps(payload_dict), "csrf_bd_gem_nk": "e331d254a1625325352a675d7eff471e"}
    
    total_available = 0
    max_init_retries = 3
    
    for attempt in range(max_init_retries):
        try:
            print(f"[INFO] Fetching initial metadata (Attempt {attempt+1}/{max_init_retries})...")
            resp = requests.post(first_page_url, headers=headers, data=data, timeout=30)
            resp.raise_for_status()
            
            j = resp.json()
            # Validate response structure
            if "response" not in j or "response" not in j["response"]:
                raise ValueError("Invalid JSON structure: Missing 'response' key")
                
            total_available = j['response']['response'].get('numFound', 0)
            print(f"[INFO] Total records found: {total_available}")
            
            if total_available == 0:
                 print("[WARN] Total records is 0. Please check your search filter or date range.")
            
            # Manually put the first batch
            if "docs" in j["response"]["response"]:
                docs = j["response"]["response"]["docs"]
                print(f"[INFO] First batch fetched: {len(docs)} items.")
                data_queue.put(docs)
            else:
                print("[WARN] No 'docs' in first batch.")
                
            break # Success
            
        except Exception as e:
            print(f"[WARN] Initial fetch failed: {e}")
            if attempt < max_init_retries - 1:
                time.sleep(2)
            else:
                print("[FATAL] Could not fetch initial data after retries.")
                return

    # 2. Spawn Workers
    # We fetched page 1 (which is index 1 for them?) manually? Wait, the initial fetch used start=0.
    # The new logic uses "page": N.
    # Let's assume the initial fetch we did in step 1 was effectively Page 1.
    
    # 2. Spawn Workers
    start_time = time.time()
    
    # Calculate total pages
    per_page = 10 
    total_pages = (total_available // per_page) + 1
    
    # Create Page Queue
    # Each item: (page_num, retry_count)
    page_queue = queue.Queue()
    
    # Generate list of pages to fetch. Start from page 2.
    pages_to_fetch = list(range(2, total_pages + 1))
    
    if max_pages:
        remaining_pages = max_pages - 1
        if remaining_pages > 0:
             pages_to_fetch = pages_to_fetch[:remaining_pages]
        else:
             pages_to_fetch = []
        print(f"[INFO] Limiting scrape to next {len(pages_to_fetch)} pages.")

    print(f"[INFO] Spawning {max_workers} workers for ~{len(pages_to_fetch)} pages...")

    for p in pages_to_fetch:
        page_queue.put((p, 0)) # page, attempts

    def worker_task():
        while True:
            try:
                # Non-blocking get not suitable if we want them to stay alive until queue is truly done?
                # Actually, standard pattern is: get, process, task_done.
                # If queue empty, break? 
                # But other threads might push back items (retries).
                # So getting with timeout is safer, or using a sentinel. 
                # For simplicity here: queue.get(timeout=3) -> if empty after 3s and no active work?
                # Let's use a "not empty" check or simply rely on the fact that we populate all first.
                
                try:
                    item = page_queue.get(timeout=3)
                except queue.Empty:
                    return # Exit worker if queue is empty for 3s
                
                page_num, attempt = item
                
                # Try fetching
                # We reduce internal retries to 1 since we have queue retry
                success = fetch_bids_page(search_bid, from_date, to_date, page_num, retries=1)
                
                if success:
                    page_queue.task_done()
                else:
                    if attempt < 3:
                        print(f"[RETRY] Re-queuing Page {page_num} (Attempt {attempt+1})")
                        time.sleep(1) # Slight pause
                        page_queue.put((page_num, attempt + 1))
                        # Note: We must call task_done for the FAILED item, 
                        # because we put a NEW item.
                        page_queue.task_done() 
                    else:
                        print(f"[FAIL] Dropping Page {page_num} after {attempt} attempts.")
                        page_queue.task_done()
                        
            except Exception as e:
                print(f"[FATAL WORKER ERROR] {e}")

    # Start Worker Threads
    threads = []
    for _ in range(max_workers):
        t = threading.Thread(target=worker_task, daemon=True)
        t.start()
        threads.append(t)
        
    # Monitoring Loop
    while True:
        elapsed_time = time.time() - start_time
        unfinished = page_queue.unfinished_tasks
        
        if total_fetched > 0 and elapsed_time > 0:
            rate = total_fetched / elapsed_time
            # remaining items = incomplete tasks in queue
            eta_s = unfinished / (rate/10) if rate > 0 else 0 # rate is records/s, items are pages (10 recs)
            eta_str = str(time.strftime('%H:%M:%S', time.gmtime(eta_s)))
            
            sys.stdout.write(f"\r[PROGRESS] Saved: {total_fetched} | Queue Pending: {unfinished} | Rate: {rate:.1f}/s | ETA: {eta_str}   ")
            sys.stdout.flush()
        
        if page_queue.empty() and unfinished == 0:
             break
             
        # Also check if all threads died?
        if not any(t.is_alive() for t in threads):
            print("\n[WARN] All worker threads died unexpectedly.")
            break
            
        time.sleep(1)

    # Wait for queue logic to flush (should be done due to loop break)
    page_queue.join()
    
    # Signal DB worker to stop
    done_event.set()

    # Wait for consumer to finish writing everything
    data_queue.join()
    
    # Final check
    elapsed_time = time.time() - start_time
    print(f"\n[SUCCESS] Completed in {elapsed_time:.2f}s. Total Saved: {total_fetched}")
    
    if total_fetched > 0:
        create_backup()

if __name__ == "__main__":
    main()
