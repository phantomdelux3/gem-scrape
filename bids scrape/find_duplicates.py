import requests
import json
import collections
import concurrent.futures
import threading
import time

# Configuration
MAX_PAGES = 50 
THREADS = 10

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

# Tracking: ID -> List of Pages
bid_tracker = collections.defaultdict(list)
lock = threading.Lock()

def fetch_page(page_num):
    payload_dict = {
        "page": page_num,
        "param": {"searchBid": "", "searchType": "fullText"},
        "filter": {
            "bidStatusType": "bidrastatus", "byType": "all", "highBidValue": "",
            "byEndDate": {"from": "", "to": ""},
            "sort": "Bid-End-Date-Latest", "byStatus": ""
        }
    }
    data = {"payload": json.dumps(payload_dict), "csrf_bd_gem_nk": "e331d254a1625325352a675d7eff471e"}
    
    try:
        resp = requests.post(url, headers=headers, data=data, timeout=30)
        j = resp.json()
        if "response" in j and "response" in j["response"] and "docs" in j["response"]["response"]:
            docs = j["response"]["response"]["docs"]
            with lock:
                for doc in docs:
                    b_id = doc.get("b_bid_number")
                    if isinstance(b_id, list):
                        b_id = b_id[0]
                    if b_id:
                        bid_tracker[b_id].append((page_num, doc))
            return True
    except Exception as e:
        print(f"Failed Page {page_num}: {e}")
        return False

print(f"Scanning first {MAX_PAGES} pages for duplicates...")
with concurrent.futures.ThreadPoolExecutor(max_workers=THREADS) as executor:
    futures = [executor.submit(fetch_page, p) for p in range(1, MAX_PAGES + 1)]
    concurrent.futures.wait(futures)

print("\n=== DUPLICATE ANALYSIS ===")
total_records = 0
duplicates_found = 0

# Sort by ID for cleaner output
sorted_ids = sorted(bid_tracker.keys())

with open("duplicates_debug.txt", "w", encoding="utf-8") as f:
    f.write(f"Duplicate Analysis Report - {time.ctime()}\n")
    f.write("="*50 + "\n\n")

    for b_id in sorted_ids:
        occurrences = bid_tracker[b_id]
        # Sort occurrences by page number
        occurrences.sort(key=lambda x: x[0])
        
        pages = [x[0] for x in occurrences]
        
        if len(pages) > 1:
            duplicates_found += 1
            msg = f"Bid {b_id} found on pages: {pages}"
            print(msg)
            f.write(msg + "\n")
            
            for i, (pg, doc) in enumerate(occurrences):
                f.write(f"  --- Occurrence {i+1} (Page {pg}) ---\n")
                f.write(json.dumps(doc, indent=2) + "\n")
            
            f.write("-" * 30 + "\n")
            
        total_records += 1

print(f"\nTotal Unique IDs: {total_records}")
print(f"Total IDs with Duplicates: {duplicates_found}")
print(f"Duplicate Ratio: {duplicates_found/total_records*100:.1f}%")
print("\nFull details written to 'duplicates_debug.txt'")
