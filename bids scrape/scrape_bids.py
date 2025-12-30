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
        "csrf_bd_gem_nk": "4755cdae7f8a2023804caf3b55da6e26",
        "cookie": "csrf_gem_cookie=4755cdae7f8a2023804caf3b55da6e26; ci_session=c1d438e224354eb06202b3313baf1271011dae56; GeM=1542078820.20480.0000; TS0174a79d=01e393167d235ea4d385feee234225838828ce3fab326579e3c1b91cf1806fd67b36239ff9eb4d050ee27cfa77d195f72fbef40a1e1c5cbd2a839bbdb72211732d92abc0391763572e81f03b4ddcc18c0b6a26583167fedf69ad4dd5ecbb0b43bc18f38d18"
    },
    {
        "csrf_bd_gem_nk": "ab521cae44622abda1c4b79c6ba2b502",
        "cookie": "csrf_gem_cookie=ab521cae44622abda1c4b79c6ba2b502; ci_session=a46c5c6942968c27f4f5056bbc373fecff5b0423; GeM=1474969956.20480.0000; TS0174a79d=01e393167d8a684d202f5ae3196cc37e50ed7d0ce1fa0589127bce2591295fac58e49bf7a088556b7faaa965160df77aecdc9226dfdd63b01e050471deb0f35b9dd31becb468ae8e33240b620baf90a75adfe422021c1a12395238c0204b895ea7ff61ddb3; TS01dc9e29=01e393167d328e223770fc5e890fd6cab13c32ff08a74697771d9c0983073a3a3526d8c0f32adbc3c7e9bf03ab6bccfe48932d07fb"
    },
    {
        "csrf_bd_gem_nk": "ca0b9a33b6149231d2b90e6e08d88be7",
        "cookie": "csrf_gem_cookie=ca0b9a33b6149231d2b90e6e08d88be7; ci_session=128f393a856d5ba082e1e016e4dcd4c74cff7706; GeM=1474969956.20480.0000; TS0174a79d=01e393167d46de19c773fd6ef636bd7fb0dfc3f3b02157de9482637cd2c28b4bd2e22b0df380211a02791361bd1b15189f83e030abe0beff444771e404c567e90161d828625c777a90cd9c0e1633c0da320a5eacb19f36730934f213402f8f65043dab0d94; TS01dc9e29=01e393167d17af83087d181dc376710c1a339a0bcb2e62cf17387cc8007a64f272406854359608ceafd0a55350f6385cb98f281e2c7a521daddccedb1a4b2f21f29b4fa1a8"
    },
    {
        "csrf_bd_gem_nk": "f65f295627e88fd4549c1c67c335e3da",
        "cookie": "csrf_gem_cookie=f65f295627e88fd4549c1c67c335e3da; ci_session=f02d62ef4dc483fbaaf524d3dde53b89b54ad008; TS0174a79d=01e393167d061ff15bce29efe8230ce9e5db72245454d020ce0e4ef996e36120a7ecc9e6321d2fcd0496224cef0bff62def22c3824b412cef4c13b77a1a4fe80955cd3acdf6a7427471278081819159e51d6ed0520"
    },
    {
        "csrf_bd_gem_nk": "e11adfd90909e34931b7cf1f229c66be",
        "cookie": "csrf_gem_cookie=e11adfd90909e34931b7cf1f229c66be; ci_session=7d384afb28985b33adcdf308b4d0504923d03548; TS0174a79d=01e393167d0df66305e735171067638fc70e99d1afe9a5c72da9748d08ca89e258a4cd7e418e8bc6bc8b3e30b3a02720e88b9b21979461bbf830682563ee04ea7b21d50a164ecb375b85fa7fd3fb6646f4a711a149; TS01dc9e29=01e393167d55803dc06aacb7a0d2a941e86805b777b4c76c25757172050ed389c44d4cc28489205bc8b66bb82abde36ad1fe0cae25"
    },
    {
        "csrf_bd_gem_nk": "9e065a942b54ebb71d7d66e14b6b74cf",
        "cookie": "csrf_gem_cookie=9e065a942b54ebb71d7d66e14b6b74cf; ci_session=a3e09b8f1db0161cc031a7ade138088af9506f54; TS0174a79d=01e393167d5399a87aa6d9e1c13ace3e02a81c85bafd99626b422160825714fb011d59d17e820d034f01153883f2496a243f44ff44fc71a609b94c4af220e283a7d187475f1cd3554c70ccf64de4d2134f49074f61; TS01dc9e29=01e393167d6c59d1d7668ae842a08ba4f5deb30490a58c3121bb2fb33a794242da6f2f00e3f4680faf4b28735a6be58ad658071189"
    },
    {
        "csrf_bd_gem_nk": "5bf3cd92de5585ffcf20a4e26cf6aa15",
        "cookie": "csrf_gem_cookie=5bf3cd92de5585ffcf20a4e26cf6aa15; ci_session=8dd504a404ba39f729c1f69ffbe8dc31fea924ee; TS0174a79d=01e393167d3b4ccf1e5ec9c8d42939bcc45475a19018da618a39edd631f726a8da05a668835c277ff392f81910c6e95063632ec81adb269881952a017061f90bed7d9d3a53be90ebdc1c5758d94c8128504222e2e9; TS01dc9e29=01e393167d7ebd8ef5021d19623c7919f852075b9689fbe705da31162391b0668e4d81d5c30b4ff4384bb7e1e3096c08e547856517"
    },
    {
        "csrf_bd_gem_nk": "62960e161e12c004e3eeef01c8affbad",
        "cookie": "csrf_gem_cookie=62960e161e12c004e3eeef01c8affbad; ci_session=5a081440456b27132db70e1d0d5eb5a8ea6ffd1a; TS0174a79d=01e393167d877203bc06326712fa0724e4690afdd654a86cdbb72f28a61ba65aad8466144a8d1cb5f547be845e44c4bd2cf262c9a5d8ae9bddee64b807ce23e39d35742c76cbfcc17752c5aeb2f33480d104be9a6b"
    },
    {
        "csrf_bd_gem_nk": "1bfe823fef3692a8432b5043ee338f90",
        "cookie": "csrf_gem_cookie=1bfe823fef3692a8432b5043ee338f90; ci_session=1e01e63f421858a82061cf1f70b01e6b27b92562; TS0174a79d=01e393167ddf1cbf87b3302a4ab58809d6db9011a24d7cb8b8f993d71e8b16c736725625310488b5a26596fcb9d7222b95afcd2a9bb3b593a1a23d6c11ada2ff0afb85e0f79f93b362d7fdb667609703a66486da7b; TS01dc9e29=01e393167d21345bbf63bc97735da135ee03b47fcaff708321246dfbce4d96987a29e58d79243b8be8c6e733fd2fa2e3f9e3288785"
    },
    {
        "csrf_bd_gem_nk": "1c58fd36160a6e58110590260453fa99",
        "cookie": "csrf_gem_cookie=1c58fd36160a6e58110590260453fa99; ci_session=acddddde40b7b07e6422c445cf54144c75af379d; TS0174a79d=01e393167dd47343fcd31cd4fda7e48e62ed4c5459061a335d4c6c4fa2bb249c0e433f3c16769f1d0e2bcdb2daacea9a9a2ab61ef9527bca0ac988b50f53fda40f9a93bdcbe121ecd1dd9d306e74b8f598588918d3; TS01dc9e29=01e393167db6c5938dedc79ffff9589a00799005e93a2978c5058b642432b5a13c365f1c075ca3abdd942a924d5e938ae28f4ee69a"
    },
    {
        "csrf_bd_gem_nk": "2643e2afc0775e8ad74f5afd2451e598",
        "cookie": "csrf_gem_cookie=2643e2afc0775e8ad74f5afd2451e598; ci_session=0c689e129b75e9f65e1f4ee741a30955eb4cb4b5; TS0174a79d=01e393167d14a9475d519938db768c842722f1ffaf30658ee59fd5cab2afbb68851954cfa839b1180b483c0239902e75223fa65b4d5dab10af3acd7bdf036a36ec94faa915a73f93d9272bf96a080889f52a637f07; TS01dc9e29=01e393167d3ec3f86e067bc5ea1602e182fb9b639b83f9f6ba7ac2516cac4231212ae732a6a0e870dab15127e3b1ff6d024422b9bf"
    },
    {
        "csrf_bd_gem_nk": "db0fa8b6201284b8c1a9856276449766",
        "cookie": "csrf_gem_cookie=db0fa8b6201284b8c1a9856276449766; ci_session=f8c20614d2e623d2d7761d5d4ce8dbaeda57f150; TS0174a79d=01e393167dd2562330f5a8788c8325c76865021538d5dae258f7485ac15a9a4f2aa78d0ffb64d889390fa2b05c74996cc32fc55bd4a1aa991a3f3dc6604541a3dca97d342df159c9ea19ad6af21fbb776874d8593f; TS01dc9e29=01e393167d8b0628d436ec774a2087328c0b4413753b9da14331bb98eb72a3c7a2353c76e9915e8a189ef759e733a968ea2fba2e69"
    },
    {
        "csrf_bd_gem_nk": "b07919db9d828944a0aaaa20fbca093d",
        "cookie": "csrf_gem_cookie=b07919db9d828944a0aaaa20fbca093d; ci_session=e53d6a082289d6ba795f962a4ec334294a557d49; TS0174a79d=01e393167d5cfd0308fd5e295fdfca56cf62ec2ef1976e13f7822fbab8a0c27c71fd0937cc552af4736f82dbdc121141e9c7fbce839b49d350a4371c904916c8b427cf06b7e6d623d5a76ed940f158491b5c2f8706; TS01dc9e29=01e393167dbc0ab3465414b9400a0264737e9189cbf14c4a4c480f5b14053a7dec6b8bc618dd59f68838278958aa46b95a2a98b5fe"
    },
    {
        "csrf_bd_gem_nk": "cea650e94f624a23e4205c7a7a484857",
        "cookie": "csrf_gem_cookie=cea650e94f624a23e4205c7a7a484857; ci_session=88a854d93e29ac3afc0ae9a4ddfc2ccb5740d359; TS0174a79d=01e393167dff4b3a5f35e68e856ef1080c11b3906059eb71a52c4d99c397ad1ba4b9c34a00d6dbb935ff5c9bac11a8b9d97122373c019871de39d2d5938c010df531c17ffeae7c8a4dfeb48c0d3f42a7dc9f3110a7; TS01dc9e29=01e393167d5db08f3088cbda6e2871e1dd1c2bde6e505488d56233676d4f940b015a7d4c69804ea4f20e867cc29e0e011ac9ea332f"
    },
    {
        "csrf_bd_gem_nk": "6f9d53d51928fa92b2b09cfe41349050",
        "cookie": "csrf_gem_cookie=6f9d53d51928fa92b2b09cfe41349050; ci_session=6cdaff5b55961e70bc1ad5e8fa619ee1a887798b; TS0174a79d=01e393167d6f7e5b431703ff8df9768359c30d6e6c187c2cc436324fc0849c2e9e0ac2a28ef946ea36ed0c6d4f9c2600bb5e6bccebfeca08cb688c6c34080c8e5b3afca4a711b71e98b8cf2e2a20d15e7623f1a135; TS01dc9e29=01e393167d10bda6588a1b6d1f957705463ddd4a981392c7e76fe889f95c7d75d0c4f5c7812ba7d932b19a03561832566312be1504"
    },
    {
        "csrf_bd_gem_nk": "b289efcbf29c6c85001b77823fe44db8",
        "cookie": "csrf_gem_cookie=b289efcbf29c6c85001b77823fe44db8; ci_session=2b0bb3f34b3a48d94b6db0bac2360e1d470d6580; TS0174a79d=01e393167da3d0b43e12e594d10a3951174800f7af69b55b8951453d93e5420e08e6c444f180516c8fff678d2216a16468e1774ba4023829f9973be5c044077bc49173d578414d2ccf2e34e41aaec4f304a736587d; TS01dc9e29=01e393167d5b729b871bab158fd82d6afb4c004aaa7ace35289ab0990cefc9c648b640eb7985027465db1b253a9be5ddb94c32772f"
    },
    {
        "csrf_bd_gem_nk": "908072322d5929814db3f60b9760ad6b",
        "cookie": "csrf_gem_cookie=908072322d5929814db3f60b9760ad6b; ci_session=6e8dab1ad289840b77526d02af3561d46edea2d3; TS0174a79d=01e393167dcd3993c67f04bf6d882df54d6d3f9f5dbd6aa629cfaf7e0d4dac5532d369da91f1b21bee06f847716ee75b58bc493eb339f5ba1227828f3bcdfffd1e2a8142b5aeaba0d129c8472cc44e3e42d3d70b1e; TS01dc9e29=01e393167d4a1b45141420305059aea5751831610e095c900310c60297339f93b6dac15f89fa2f411f651f682c2466232fd0ff67d1"
    },
    {
        "csrf_bd_gem_nk": "c18cce76fd67f517b96ed27c2e0f51a0",
        "cookie": "csrf_gem_cookie=c18cce76fd67f517b96ed27c2e0f51a0; ci_session=08b2af91d51d1de3606c28921e68cca927a80e3c; TS0174a79d=01e393167dcd50a92fe867ddb19d5973673f8b184b96d7f2c3aad28e55c42515e8d8646e7915138ecd472634827cee008d0dc9b5a43415b96e7953159827ffd5ddf0e0375fdd64da372f8311ef2710be4c0a4ebf83; TS01dc9e29=01e393167dc86c5d0caea3c84f1049d1a66dd06606482934db520750995ae3377ec72ec84cc31d393fe87f7f877bcb036ebfc6c35c"
    },
    {
        "csrf_bd_gem_nk": "5e088ac71d47518248f14af6be92606a",
        "cookie": "csrf_gem_cookie=5e088ac71d47518248f14af6be92606a; ci_session=4612e093bfca1f9c697ffcd5f542c3e75fc25f2a; TS0174a79d=01e393167d2a8b17a979d474998576274eadeb38349594999e0506413eb22c842c1bd33d2a849be13d7c371438dcca1c2e0d5a98f83fc1ca1f3a49a723614052706515b0eeb453b6f0562af88699bf4ef0b1ec743d; TS01dc9e29=01e393167dcedaa2fa9e50ca2c4794a7a71ce655309879dba2d5c155641dc5327155de1055cbecdc6c421378cc8e2d7052e467f6f1"
    },
    {
        "csrf_bd_gem_nk": "27625ffa9ad9321aca27844cb98edfb9",
        "cookie": "csrf_gem_cookie=27625ffa9ad9321aca27844cb98edfb9; ci_session=42c69ed24669c016af9563b44ad3ed2480041155; TS0174a79d=01e393167d72bf622321dddde7d445c1bc47f9697ac3e72c91bab41ee774a1e31e0b1eee8128509346581a8795a568864ae3c262a58598e54eb3be61e7555ab1ded077e373895f4ba938889da2f1feac7fdcfe4922; TS01dc9e29=01e393167d0bd99b26d217f888f2078272e8659fa41c9a1fa7ba736fdcdeaf52e196fab6ecbadae34000607aede502bc57338757fd"
    },
    {
        "csrf_bd_gem_nk": "cd0e6af51694ff37e8615502ed9a9ceb",
        "cookie": "csrf_gem_cookie=785d718c986231bf31d2cfd4312ddda6; ci_session=24ba913b5a0d0adb32f26e78571bd29b14cf9915; GeM=1458192740.20480.0000; TS0174a79d=01e393167da927f7d7152d97f1bd96233143ed47f5a267784929b95c95284d1eb118d144a3d615bd8a7fcfe204ce48dcb401512c49e24705f4212d301c23be0b604ad4858651a76471dd2420ac04ddf7f7f1653e22d951edc45ce53c9c555c81d1050fe78d; TS01dc9e29=01e393167d7f47f330b550d3b2b71b9d1e11a6a0a0ca75113512b58e3184e559e21a184bc6a3503a1bcb7309ae02492f5a594479e305532412468bfb850708fb2b80d45647"
    },
    {
        "csrf_bd_gem_nk": "faafe38cece83687776be6c014763557",
        "cookie": "csrf_gem_cookie=faafe38cece83687776be6c014763557; ci_session=49bf69bdba43a4e93823afece6933584225bdb95; GeM=1474969956.20480.0000; TS0174a79d=01e393167d4db99dc422e4e6d1d7566638d5fdc50b1983f25faa02367919736132637ba950a5c821be345340ba296fd77aa4df3ad426f367b791b399a4cc3f8dbd3b1610c27529c0c79d4d04c06a61974bfc23cef38dde4d9588f11569f78f339436cc6690; TS01dc9e29=01e393167d172b16321ff36edf6c9cd3afa89b5516acf6d15426abcfeb21c7d6f4ca00eefb54550e3187151dce057a408ffcad392b"
    },
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
