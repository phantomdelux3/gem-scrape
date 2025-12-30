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
import random

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

# Error handling globals
ERRORS_FILE = "errors.txt"
error_file_lock = threading.Lock()

# Skip list / Redundancy globals
DUPLICATE_PAGES_FILE = "duplicate_pages.txt"
duplicate_file_lock = threading.Lock()

# Progress tracking globals
PROGRESS_FILE = "progress.txt"
progress_file_lock = threading.Lock()
pages_fetched_counter = 0
pages_fetched_lock = threading.Lock()

COOKIE_POOL = [
    {
        "csrf_bd_gem_nk": "e331d254a1625325352a675d7eff471e",
        "cookie": "csrf_gem_cookie=e331d254a1625325352a675d7eff471e; ci_session=e72373b6e945b9800026970b0837c53951942c57; TS0174a79d=01e393167d4a963be55d4c4af088a2d434fbd183521b8287cee50260d2dd642cd6618da0b8c76ab225e315b68566bd19fe820be4947b8f7ac675ef7912a451446b2133190d32eafe14ead8175e1d7dec7eeeff542e9388afc22660f2eec96b049cd2932333; GeM=1474969956.20480.0000; _ga=GA1.3.484596475.1761793171; _gid=GA1.3.2012138776.1767016991"
    },
    {
        "csrf_bd_gem_nk": "b5fb60626ae44c5480c44aa0f9bc1539",
        "cookie": "_ga=GA1.3.484596475.1761793171; GeM=1474969956.20480.0000; ci_session=7c12fd6ff99b24e04d19daa46bc70b661e051b38; csrf_gem_cookie=b5fb60626ae44c5480c44aa0f9bc1539; TS0174a79d=01e393167d51c511867685cd9dce75231aca5f16074697fe94dcde0d7670cabd630be6d35ed6aa606ca5609346bfc4b71b29d7bbcc4e9d7760d006b73dc3ccdf4d9c637a5e31449a0a8098dab6536125c10037ec5c1e0fb7a800225f5d431005680a5a0c4f; TS01dc9e29=01e393167d076ba5fe9dbfce61d77a95f9374cc616c10ab9bd69afd9275b863fa10aa3c9db82b7244b7ca49810586252c2b99c8db35eef5f534770513073ea9884adef93da"
    },
    {
        "csrf_bd_gem_nk": "85a27261b6ffab1d79b4496fb381537d",
        "cookie": "csrf_gem_cookie=85a27261b6ffab1d79b4496fb381537d; ci_session=c0ca4f4959c84baf8abdc0057f12d82da337571e; GeM=1458192740.20480.0000; TS0174a79d=01e393167d43cb61c2ed60b65102d9a49dc82311bc3bf41e6e99e8c66f9fcd5f8c5d787526ea7dbd6736e6142c3a0ee13838b6f8fa2e370d06e0ddea380cd9e20509e8242b33d966c24a4558016d3697b93a1f28d2ebdabde39eda7ae29850d187b0499ffc; _ga=GA1.3.733646782.1767108018; _gid=GA1.3.24216270.1767108018; _gat=1; TS01dc9e29=01e393167db5bcea8df0a600284b06ed8d421d8dfc621b50d9c9297b0eee764f944ab6591d274c20217e9fec466771cd71e5ff330a; _ga_MMQ7TYBESB=GS2.3.s1767108018$o1$g0$t1767108018$j60$l0$h0"
    },
    {
        "csrf_bd_gem_nk": "a57b3165f8fc5c80276b5994d1e52fb9",
        "cookie": "csrf_gem_cookie=a57b3165f8fc5c80276b5994d1e52fb9; ci_session=802fd01457ce6423504f9b04e295f63bbf68d044; GeM=1474969956.20480.0000; TS0174a79d=01e393167d426cda702d7d8fb5f242df02072baadb01983b7d2f053dc6656494dc08de4ac70de52987df0a2f7d14f75cc648f056384ad85329928cc4fcb1acc362c38ebd442caa1b20c74d5a281b7d0639687476930e51574a128f8969f4ac7f324b43511b; TS01dc9e29=01e393167def79d4ca9d4e66622c063920823ce5def2855d955e262a0708ce57bea0997104d86c0a7497d873944a02fee7e889f7abf6f6cb9b5bd82b6ef5c7052b69f893d3"
    },
    {
        "csrf_bd_gem_nk": "845e50cb42c7c91513d294fd4982bc79",
        "cookie": "csrf_gem_cookie=845e50cb42c7c91513d294fd4982bc79; ci_session=4b1c3716024369bc1e3dcd46c3ecfb7a20e29ad4; GeM=1542078820.20480.0000; TS0174a79d=01e393167d75a3a6c4eb5fc873247513bb321db3088470c813514ee61ec44544e33db17ea96b054026dea37faf01cb46f9c87e1a6832dd8db8e65460cce78551962dc2449b394cde96307f7da287053f6af38ea5c0204ed8df1421f00713bd8c093bb195e7"
    },
    {
        "csrf_bd_gem_nk": "165d08640356ca26d5f9d85683e1b935",
        "cookie": "csrf_gem_cookie=165d08640356ca26d5f9d85683e1b935; ci_session=7218099c261672a436b28e64568f37e544262d04; GeM=1474969956.20480.0000; TS0174a79d=01e393167d31d203f98ce25b0a3d0f547a3b6d2f5823de73fd8d0caa72d0f7f357e7bb179489f159b8e0e3020110253ef023fd56711cb7239b341ce6122037e2bf6fdc9c26614111727f5402b8ef0a3907fac703e2ff596dda8e5ef2dfd35a54e37574fd90; TS01dc9e29=01e393167d7dc10aa21cdbce3e8c69ac3ca2c2e2398dfbec9bead2f8096208114a056a9e1e9ad9b401c320eca5a5b7eae735dbff73480daab5b022ee8e7f833cfc1c7cc249"
    }
]

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

def fetch_bids_page(search_bid, from_date, to_date, page_num, cookie_set, retries=3):
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
        "cookie": cookie_set["cookie"]
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
    data["csrf_bd_gem_nk"] = cookie_set["csrf_bd_gem_nk"] 

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
                # Put results into generic queue as (page_num, docs) tuple
                data_queue.put((page_num, docs))
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
            item = data_queue.get(timeout=2)
            # Support both new format and potentially legacy if mixed usage (unlikely here but good practice)
            if isinstance(item, tuple) and len(item) == 2 and isinstance(item[1], list):
                page_num, docs = item
            elif isinstance(item, list):
                page_num = -1 # Unknown
                docs = item
            else:
                print(f"[WARN] DB Worker received invalid Item: {type(item)}")
                data_queue.task_done()
                continue
                
        except queue.Empty:
            if done_event.is_set():
                break
            continue
        
        count = 0
        new_records_count = 0 
        
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
                    final_end_date_sort = EXCLUDED.final_end_date_sort
                RETURNING (xmax = 0) AS is_insert;
                """
                cur.execute(query, (
                    b_id, b_bid_num, b_cat, b_cat_id, qty, status, b_type,
                    start_date, end_date, bd_cat, eval_type,
                    min_name, dept_name, sv_text
                ))
                
                is_insert = cur.fetchone()[0]
                if is_insert:
                    new_records_count += 1
                
                count += 1
            except Exception as e:
                # print(f"Skipping doc ID {doc.get('id', 'unknown')}: {e}")
                pass
                
        conn.commit()
        total_fetched += count
        
        # Redundancy check logic
        if page_num != -1 and len(docs) > 0 and new_records_count == 0:
             if DEBUG_MODE:
                 print(f"[DEBUG] Page {page_num} is redundant (all records updated).")
             with duplicate_file_lock:
                 try:
                     with open(DUPLICATE_PAGES_FILE, "a") as f:
                         f.write(f"{page_num}\n")
                 except Exception as e:
                      print(f"[ERROR] Could not write to {DUPLICATE_PAGES_FILE}: {e}")

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
    parser.add_argument("--resume", action="store_true", help="Resume from progress.txt")
    
    # Default to interactive if no args provided
    if len(sys.argv) == 1:
        sys.argv.append('--interactive')
        
    args = parser.parse_args()

    global DEBUG_MODE
    DEBUG_MODE = args.debug
    
    # Determine mode
    rescrape_errors = False
    resume_from_progress = args.resume
    
    if args.interactive:
        # Check for error file first
        if os.path.exists(ERRORS_FILE) and os.path.getsize(ERRORS_FILE) > 0:
            with open(ERRORS_FILE, 'r') as f:
                lines = f.readlines()
                error_count = len(lines)
            
            if error_count > 0:
                print(f"\n[INFO] Found {error_count} failed pages in '{ERRORS_FILE}'.")
                user_choice = input("Do you want to rescrape these failed pages? (y/n): ").strip().lower()
                if user_choice == 'y':
                    rescrape_errors = True
        
        # Check for progress file
        if not rescrape_errors and os.path.exists(PROGRESS_FILE) and os.path.getsize(PROGRESS_FILE) > 0:
            with open(PROGRESS_FILE, 'r') as f:
                progress_count = sum(1 for line in f if line.strip())
            
            if progress_count > 0:
                print(f"\n[INFO] Found {progress_count} pages already scraped in '{PROGRESS_FILE}'.")
                resume_choice = input("Do you want to resume? (y/n): ").strip().lower()
                if resume_choice == 'y':
                    resume_from_progress = True
        
        if not rescrape_errors:
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
            # Defaults for rescrape mode
            search_bid = ""
            from_date = ""
            to_date = ""
            reset_db = False
            max_workers = 20 # Can interactively ask if needed, but keeping it simple
            max_pages = None
            print("[INFO] Rescraping errors... (using default workers=20)")

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
    
    # 1. Page Calculation
    pages_to_fetch = []
    
    if rescrape_errors:
         try:
            with open(ERRORS_FILE, 'r') as f:
                pages_to_fetch = [int(line.strip()) for line in f if line.strip().isdigit()]
            print(f"[INFO] Loaded {len(pages_to_fetch)} pages from {ERRORS_FILE}.")
            
            # Clear the file so we can fill it with NEW errors from this run
            with open(ERRORS_FILE, 'w') as f:
                f.truncate(0)
                
         except Exception as e:
             print(f"[ERROR] Failed to read errors file: {e}")
             return
    else:
        # Standard Fetch
        # 1. Fetch first page to get Total Count
        first_page_url = "https://bidplus.gem.gov.in/all-bids-data"
        
        # Use first cookie for initial fetch
        init_cookie = COOKIE_POOL[0]
        
        headers = {
            "accept": "application/json, text/javascript, */*; q=0.01",
            "accept-language": "en-US,en;q=0.9",
            "content-type": "application/x-www-form-urlencoded; charset=UTF-8",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "X-Requested-With": "XMLHttpRequest",
            "origin": "https://bidplus.gem.gov.in",
            "referer": "https://bidplus.gem.gov.in/all-bids",
            "cookie": init_cookie["cookie"]
        }
        
        payload_dict = {
            "param": {"searchBid": search_bid, "searchType": "fullText", "start": 0},
            "filter": {
                "bidStatusType": "bidrastatus", "byType": "all", "highBidValue": "",
                "byEndDate": {"from": from_date, "to": to_date},
                "sort": "Bid-End-Date-Latest", "byStatus": ""
            }
        }
        data = {"payload": json.dumps(payload_dict), "csrf_bd_gem_nk": init_cookie["csrf_bd_gem_nk"] }
        
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
                
                # Manually put the first batch (Page 1)
                if "docs" in j["response"]["response"]:
                    docs = j["response"]["response"]["docs"]
                    print(f"[INFO] First batch fetched: {len(docs)} items.")
                    data_queue.put((1, docs)) # Page 1
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
        
        # Calculate total pages
        if total_available > 0:
            per_page = 10 
            total_pages = (total_available // per_page) + 1
            
            # Generate list of pages to fetch. Start from page 2.
            pages_to_fetch = list(range(2, total_pages + 1))
            
            if max_pages:
                remaining_pages = max_pages - 1
                if remaining_pages > 0:
                     pages_to_fetch = pages_to_fetch[:remaining_pages]
                else:
                     pages_to_fetch = []
                print(f"[INFO] Limiting scrape to next {len(pages_to_fetch)} pages.")
        else:
             pages_to_fetch = []

        if resume_from_progress and not rescrape_errors:
             try:
                 if os.path.exists(PROGRESS_FILE):
                     with open(PROGRESS_FILE, 'r') as f:
                         done_pages = set(int(line.strip()) for line in f if line.strip().isdigit())
                     
                     initial_count = len(pages_to_fetch)
                     pages_to_fetch = [p for p in pages_to_fetch if p not in done_pages]
                     print(f"[INFO] Resume: Skipped {initial_count - len(pages_to_fetch)} pages. {len(pages_to_fetch)} remaining.")
             except Exception as e:
                 print(f"[ERROR] Failed to read progress file: {e}")

    # 2. Spawn Workers
    start_time = time.time()
    
    # Create Page Queue
    # Each item: (page_num, retry_count)
    page_queue = queue.Queue()
    
    print(f"[INFO] Spawning {max_workers} workers for ~{len(pages_to_fetch)} pages...")

    for p in pages_to_fetch:
        page_queue.put((p, 0)) # page, attempts

    def worker_task(worker_index):
        # Assign cookie set based on worker index (Round Robin)
        cookie_set = COOKIE_POOL[worker_index % len(COOKIE_POOL)]
        
        while True:
            try:
                try:
                    item = page_queue.get(timeout=3)
                except queue.Empty:
                    return # Exit worker if queue is empty for 3s
                
                page_num, attempt = item
                
                # Try fetching
                # We reduce internal retries to 1 since we have queue retry
                success = fetch_bids_page(search_bid, from_date, to_date, page_num, cookie_set, retries=1)
                
                if success:
                    # Log progress
                    with progress_file_lock:
                        try:
                            with open(PROGRESS_FILE, "a") as f:
                                f.write(f"{page_num}\n")
                        except Exception as e:
                            print(f"[ERROR] Could not write to progress.txt: {e}")
                    
                    with pages_fetched_lock:
                         global pages_fetched_counter
                         pages_fetched_counter += 1

                    page_queue.task_done()
                else:
                    if attempt < 3:
                        print(f"[RETRY] Re-queuing Page {page_num} (Attempt {attempt+1})")
                        time.sleep(1) # Slight pause
                        page_queue.put((page_num, attempt + 1))
                        page_queue.task_done() 
                    else:
                        print(f"[FAIL] Dropping Page {page_num} after {attempt} attempts. Saving to errors.txt.")
                        
                        # Save to errors.txt
                        with error_file_lock:
                            try:
                                with open(ERRORS_FILE, "a") as f:
                                    f.write(f"{page_num}\n")
                            except Exception as e:
                                print(f"[ERROR] Could not write to errors.txt: {e}")
                        
                        page_queue.task_done()
                        
            except Exception as e:
                print(f"[FATAL WORKER ERROR] {e}")

    # Start Worker Threads
    threads = []
    for i in range(max_workers):
        t = threading.Thread(target=worker_task, args=(i,), daemon=True)
        t.start()
        threads.append(t)
        
    # Monitoring Loop
    
    # Moving Average Logic
    history = [] # List of (timestamp, count)
    WINDOW_SIZE = 10 # seconds

    while True:
        elapsed_time = time.time() - start_time
        unfinished = page_queue.unfinished_tasks
        
        # Update history for rate calculation
        now = time.time()
        with pages_fetched_lock:
            current_count = pages_fetched_counter
        
        history.append((now, current_count))
        
        # Prune old history
        while history and history[0][0] < now - WINDOW_SIZE:
            history.pop(0)
            
        # Calculate instantaneous rate
        if len(history) > 1:
            delta_count = history[-1][1] - history[0][1]
            delta_time = history[-1][0] - history[0][0]
            if delta_time > 0:
                rate_pages = delta_count / delta_time
            else:
                rate_pages = 0
        else:
             # Fallback to global average if not enough history
             if elapsed_time > 0:
                 rate_pages = current_count / elapsed_time
             else:
                 rate_pages = 0

        if total_fetched >= 0: # Always show
            # ETA based on pages
            if rate_pages > 0:
                eta_s = unfinished / rate_pages
            else:
                eta_s = 0
            
            eta_str = str(time.strftime('%H:%M:%S', time.gmtime(eta_s)))
            
            # Display: Rate in Pages/s (or Records/s if preferred, let's do Pages/s as it's cleaner, or both)
            rec_rate = rate_pages * 10
            
            sys.stdout.write(f"\r[PROGRESS] Saved: {total_fetched} | Queue Pending: {unfinished} | Rate: {rec_rate:.1f}/s | ETA: {eta_str}   ")
            sys.stdout.flush()
        
        if page_queue.empty() and unfinished == 0:
             break
             
        # Also check if all threads died?
        if not any(t.is_alive() for t in threads):
            print("\n[WARN] All worker threads died unexpectedly.")
            break
            
        time.sleep(1)

    # Wait for queue logic to flush
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
