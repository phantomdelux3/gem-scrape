#!/usr/bin/env python3
"""
gem_extract.py

Usage:
    python gem_extract.py -t 5

Scans pdfs/ recursively, extracts structured contract + item info,
and writes gem_contracts.xlsx with two sheets: Contracts, Items.
"""

from pathlib import Path
import re
import argparse
from collections import defaultdict
import difflib
import pdfplumber
import pandas as pd

# ---------- CONFIG ----------
PDF_DIR = Path("pdfs")
DATA_EXPORTS_DIR = Path("data_exports")
OUTPUT_XLSX = DATA_EXPORTS_DIR / "gem_contracts.xlsx"

# Toggle specific extraction blocks
INCLUDE_ORG_DETAILS = False
INCLUDE_PAYING_DETAILS = False

# Heuristics for sanitizing numbers
MAX_REASONABLE_LEN = 10   # prefer numeric candidates with <= this many digits
TAIL_DIGITS = 5           # if digits are too long, keep last TAIL_DIGITS digits
MIN_USE_LAST_SEGMENT_LEN = 2  # if last comma-segment length >= this, prefer it

# ---------- canonical label->column maps ----------
ORG_FIELDS = {
    "type": "Org_Type",
    "ministry": "Org_Ministry",
    "department": "Org_Department",
    "organisation name": "Org_OrganisationName",
    "office zone": "Org_OfficeZone",
}


BUYER_FIELDS = {
    "designation": "Buyer_Designation",
    "contact no": "Buyer_ContactNo",
    "contact no.": "Buyer_ContactNo",
    "contact": "Buyer_ContactNo",
    "email id": "Buyer_EmailID",
    "email": "Buyer_EmailID",
    "gstin": "Buyer_GSTIN",
    "address": "Buyer_Address",
}

PAYING_FIELDS = {
    "role": "Paying_Role",
    "payment mode": "Paying_PaymentMode",
    "designation": "Paying_Designation",
    "email id": "Paying_EmailID",
    "gstin": "Paying_GSTIN",
    "address": "Paying_Address",
}


SELLER_FIELDS = {
    "gem seller id": "Seller_GeMSellerID",
    "company name": "Seller_CompanyName",
    "contact no": "Seller_ContactNo",
    "contact": "Seller_ContactNo",
    "seller contact": "Seller_ContactNo",
    "email id": "Seller_EmailID",
    "address": "Seller_Address",
    "msme registration number": "Seller_MSMERegistrationNumber",
    "mse social category": "Seller_MSESocialCategory",
    "mse gender": "Seller_MSEGender",
    "gstin": "Seller_GSTIN",
}

PRODUCT_FIELDS = {
    "product name": "ProductName",
    "brand": "Brand",
    "brand type": "BrandType",
    "catalogue status": "CatalogueStatus",
    "selling as": "SellingAs",
    "category name & quadrant": "CategoryNameQuadrant",
    "category name and quadrant": "CategoryNameQuadrant",
    "model": "Model",
    "hsn code": "HSNCode",
}

# Build global mapping (fill order for duplicate labels)
GLOBAL_LABEL_COLUMNS = defaultdict(list)
for mapping in (ORG_FIELDS, BUYER_FIELDS, PAYING_FIELDS, SELLER_FIELDS):
    for lbl, col in mapping.items():
        GLOBAL_LABEL_COLUMNS[lbl].append(col)
CANONICAL_LABELS = list(GLOBAL_LABEL_COLUMNS.keys())

# ---------- regex constants ----------
CID_RE = re.compile(r"\(cid:\d+\)")
NON_PRINTABLE_RE = re.compile(r"[\x00-\x08\x0b-\x0c\x0e-\x1f]")
MULTICOMMA_RE = re.compile(r",\s*,+")
WHITESPACE_RE = re.compile(r"\s+")
NONALPHANUM = re.compile(r"[^a-z0-9]")
DUP_RUN_RE = re.compile(r"(.)\1+")

# Reduced label length from 160/120 to 80 to avoid capturing too much preceding text
PAIR_WITH_PIPE_RE = re.compile(r'(?P<label>[^:\n|]{1,80}\|[^:]{1,80})\s*:\s*(?P<value>.*?)(?=\s[^:\n|]+\|[^:]+:|$)')
PAIR_GENERIC_RE = re.compile(r'(?P<label>[^:\n]{1,80}?)\s*:\s*(?P<value>.*?)(?=\s[^:\n]{1,80}?:|$)')

# ---------- cleaning & normalization helpers ----------

def collapse_duplicate_runs_if_artifact(token: str, min_dup_runs: int = 2, min_dup_ratio: float = 0.30) -> str:
    """Collapse duplicate-letter runs in token only when token looks corrupted."""
    if not token or len(token) < 3:
        return token
    dup_runs = list(DUP_RUN_RE.finditer(token))
    if len(dup_runs) < min_dup_runs:
        return token
    total_dup_chars = sum(m.end() - m.start() for m in dup_runs)
    if (total_dup_chars / len(token)) < min_dup_ratio:
        return token
    return DUP_RUN_RE.sub(r"\1", token)

def preprocess_text(raw: str) -> str:
    """Remove (cid), nonprintables, multiple commas, collapse duplicates conservatively."""
    if not raw:
        return ""
    t = CID_RE.sub(" ", raw)
    t = NON_PRINTABLE_RE.sub(" ", t)
    t = MULTICOMMA_RE.sub(",", t)
    t = WHITESPACE_RE.sub(" ", t)
    lines = [ln.strip() for ln in t.splitlines() if ln.strip()]
    cleaned_lines = []
    for line in lines:
        parts = re.split(r"(\W+)", line)  # keep punctuation tokens separate
        parts = [p if not p.isalpha() else collapse_duplicate_runs_if_artifact(p) for p in parts]
        cleaned_lines.append("".join(parts).strip())
    return "\n".join([ln for ln in cleaned_lines if ln])

def preprocess_text_preserve_layout(raw: str) -> str:
    """Same as preprocess_text but separates lines with \\n instead of flattening."""
    if not raw:
        return ""
    # Don't replace whitespace generally, just handle CIDs and non-printables
    t = CID_RE.sub(" ", raw)
    t = NON_PRINTABLE_RE.sub(" ", t)
    # Don't flatten newlines
    lines = [ln.strip() for ln in t.splitlines() if ln.strip()]
    cleaned_lines = []
    for line in lines:
        # collapse runs?
        parts = re.split(r"(\W+)", line)
        parts = [p if not p.isalpha() else collapse_duplicate_runs_if_artifact(p) for p in parts]
        cleaned_lines.append("".join(parts).strip())
    return "\n".join(cleaned_lines)

def remove_devanagari_and_noise(s: str) -> str:
    """Strip Devanagari/Hindi characters and cid tokens from values."""
    if s is None:
        return s
    s = re.sub(r'[\u0900-\u097F]+', ' ', s)
    s = CID_RE.sub(' ', s)
    s = WHITESPACE_RE.sub(' ', s).strip()
    return s

def normalize_label_for_match(s: str) -> str:
    """Create normalized label string for fuzzy matching."""
    if not s:
        return ""
    s = s.lower()
    s = s.replace("।", " ")
    s = CID_RE.sub(" ", s)
    s = WHITESPACE_RE.sub(" ", s).strip()
    s = NONALPHANUM.sub('', s)
    dup_runs = list(DUP_RUN_RE.finditer(s))
    if len(dup_runs) >= 2:
        s = DUP_RUN_RE.sub(r'\1', s)
    return s

# precompute normalized canonical labels
NORM_CANON = {lbl: normalize_label_for_match(lbl) for lbl in CANONICAL_LABELS}

def best_label_match(extracted_label: str, threshold: float = 0.75):
    nl = normalize_label_for_match(extracted_label)
    if not nl:
        return None
    # exact match
    for k, nk in NORM_CANON.items():
        if nl == nk:
            return k
    # fuzzy
    best = None
    best_ratio = 0.0
    for k, nk in NORM_CANON.items():
        ratio = difflib.SequenceMatcher(None, nl, nk).ratio()
        if ratio > best_ratio:
            best_ratio = ratio
            best = k
    return best if best_ratio >= threshold else None

# ---------- numeric & phone sanitization ----------

def extract_digit_segments(orig: str):
    """Split orig on commas/dots/spaces and return digit-only segments."""
    if not orig:
        return []
    parts = re.split(r"[,\.\s]+", orig)
    digits = [re.sub(r"[^\d]", "", p) for p in parts if re.sub(r"[^\d]", "", p)]
    return digits

def sanitize_number_string(orig: str, max_reasonable_len: int = MAX_REASONABLE_LEN, tail_digits: int = TAIL_DIGITS):
    """Heuristic to sanitize corrupted numbers and return cleaned digit string (no formatting)."""
    if not orig or not isinstance(orig, str):
        return None
    orig = orig.strip()
    orig = CID_RE.sub(' ', orig)
    segs = extract_digit_segments(orig)
    if not segs:
        m = re.findall(r'\d+', orig)
        if not m:
            return None
        segs = m
    # prefer last segment if reasonably short
    if segs:
        last_seg = segs[-1]
        if MIN_USE_LAST_SEGMENT_LEN <= len(last_seg) <= max_reasonable_len:
            return last_seg.lstrip('0') or "0"
        short_cands = [s for s in segs if len(s) <= max_reasonable_len]
        if short_cands:
            return short_cands[-1].lstrip('0') or "0"
        # fallback: take longest, then last tail_digits
        longest = max(segs, key=len)
        chosen = longest[-tail_digits:] if len(longest) > tail_digits else longest
        return chosen.lstrip('0') or "0"
    return None

def sanitize_double_digits(s: str) -> str:
    """
    Collapses doubled digits pattern often found in bold PDF text.
    e.g. '33662200' -> '3620'
    Condition: Even length, and s[2i] == s[2i+1] for all i.
    """
    if not s or len(s) < 2 or len(s) % 2 != 0:
        return s
    # Check if only digits (or standard floats) - this function expects raw digit string usually
    # But let's be strict: if it looks like a "double artifact", collapse it.
    
    is_artifact = True
    for i in range(0, len(s), 2):
        if s[i] != s[i+1]:
            is_artifact = False
            break
            
    if is_artifact:
        return s[::2]
    return s

def sanitize_phone_string(orig: str):
    """Keep digits plus leading +, hyphen and spaces; remove Devanagari and noise."""
    if not orig or not isinstance(orig, str):
        return None
    s = orig.strip()
    s = CID_RE.sub(' ', s)
    s = re.sub(r'[\u0900-\u097F]', ' ', s)
    # keep plus if at start
    plus = ''
    if s.startswith('+'):
        plus = '+'
    digits = re.sub(r'[^0-9\+\-\s,]', '', s).strip()
    if plus and not digits.startswith('+'):
        digits = '+' + digits
    
    # Strip trailing separators sometimes captured
    digits = digits.strip('-, ')
    return digits

# ---------- pdf text extraction ----------

def extract_text_from_pdf(pdf_path: Path) -> str:
    parts = []
    with pdfplumber.open(pdf_path) as pdf:
        for page in pdf.pages:
            txt = page.extract_text() or ""
            parts.append(txt)
    return "\n".join(parts)

def normalize_text_preserve_lines(raw_text: str) -> str:
    lines = []
    for line in raw_text.splitlines():
        ln = re.sub(r'\s+', ' ', line).strip()
        if ln:
            lines.append(ln)
    return "\n".join(lines)

# ---------- robust kv extraction (multiple pairs per line) ----------

def parse_global_key_values(text: str) -> dict:
    """
    Extract multiple label:value pairs per line; prefer '|EnglishLabel : value' patterns;
    fuzzy match labels to canonical labels and fill first available column for duplicates.
    """
    data = {}
    lines = [ln for ln in text.splitlines() if ln.strip()]
    for i, line in enumerate(lines):
        # first pipe-based pairs
        for m in PAIR_WITH_PIPE_RE.finditer(line):
            raw_label = m.group('label').strip()
            eng_label = raw_label.split('|')[-1].strip() if '|' in raw_label else raw_label
            val = m.group('value').strip()
            val = remove_devanagari_and_noise(val)
            matched = best_label_match(eng_label)
            if matched:
                candidate_cols = GLOBAL_LABEL_COLUMNS.get(matched, [])
                target_col = next((c for c in candidate_cols if c not in data), None)
                if target_col:
                    # gather continuation lines for multiline values
                    extras = []
                    j = i + 1
                    while j < len(lines) and ':' not in lines[j]:
                        extras.append(remove_devanagari_and_noise(lines[j].strip()))
                        j += 1
                    if extras:
                        val = (val + " " + " ".join(extras)).strip()
                    # phone/number sanitization by column name
                    if 'ContactNo' in target_col or 'Contact' in target_col:
                        val = sanitize_phone_string(val)
                    elif target_col in ('TotalOrderValueINR',):
                        num = sanitize_number_string(val)
                        val = num if num is not None else val
                    else:
                        val = val.strip()
                    data[target_col] = val

        # then generic pairs
        for m in PAIR_GENERIC_RE.finditer(line):
            raw_label = m.group('label').strip()
            if '|' in raw_label:
                continue
            val = m.group('value').strip()
            val = remove_devanagari_and_noise(val)
            matched = best_label_match(raw_label)
            if matched:
                candidate_cols = GLOBAL_LABEL_COLUMNS.get(matched, [])
                target_col = next((c for c in candidate_cols if c not in data), None)
                if target_col:
                    extras = []
                    j = i + 1
                    while j < len(lines) and ':' not in lines[j]:
                        extras.append(remove_devanagari_and_noise(lines[j].strip()))
                        j += 1
                    if extras:
                        val = (val + " " + " ".join(extras)).strip()
                    if 'ContactNo' in target_col or 'Contact' in target_col:
                        val = sanitize_phone_string(val)
                    elif target_col in ('TotalOrderValueINR',):
                        num = sanitize_number_string(val)
                        val = num if num is not None else val
                    else:
                        val = val.strip()
                    data[target_col] = val
    return data

# ---------- product parsing ----------

def parse_items(text: str, contract_no: str):
    items = []
    lines = [ln for ln in text.splitlines() if ln.strip()]
    item = None
    item_no = 0
    i = 0
    while i < len(lines):
        line = lines[i]
        # detect start of product block by "Product Name"
        if 'Product Name' in line and ':' in line:
            if item:
                items.append(item)
            item_no += 1
            item = {'ContractNo': contract_no, 'ItemNo': item_no}
            _, val = line.split(':', 1)
            item['ProductName'] = remove_devanagari_and_noise(val.strip())
        elif item is not None and ':' in line:
            raw_label, val = line.split(':', 1)
            label = raw_label.split('|')[-1].strip()
            # match against product fields
            for k, outcol in PRODUCT_FIELDS.items():
                if normalize_label_for_match(k) == normalize_label_for_match(label):
                    item[outcol] = remove_devanagari_and_noise(val.strip())
                    break
        elif item is not None:
            # match numeric order row (e.g., "4 pieces 1,810 NA 7,240")
            m = re.match(r'^(\d+)\s+([A-Za-z]+)\s+([\d,.,]+)\s+(\S+)\s+([\d,.,]+)', line)
            if m:
                try:
                    item['OrderedQuantity'] = str(int(m.group(1)))
                except:
                    item['OrderedQuantity'] = sanitize_number_string(m.group(1)) or m.group(1)
                item['Unit'] = m.group(2)
                item['UnitPriceINR'] = sanitize_number_string(m.group(3)) or re.sub(r'[^\d]', '', m.group(3))
                item['TaxBifurcationINR'] = m.group(4)
                item['PriceInclusiveINR'] = sanitize_number_string(m.group(5)) or re.sub(r'[^\d]', '', m.group(5))
        i += 1
    if item:
        items.append(item)
    return items

def parse_total_order_value(text: str):
    """
    Find line containing 'Total Order Value' and pick best numeric candidate using heuristics.
    Prioritizes numbers immediately following the label.
    """
    for line in text.splitlines():
        if 'Total Order Value' in line:
            # Try specific partial match first: Label followed by optional space/colon/chars then number
            # The text often looks like: "Total Order Value (in INR) 33,662200"
            m = re.search(r'Total Order Value[^\d]*?([\d][\d,\.]*)', line, re.IGNORECASE)
            if m:
                val_str = m.group(1)
                digits = re.sub(r'[^\d]', '', val_str)
                # Sanity check: if it's too short (like '1') or too long, maybe irrelevant.
                # But usually the immediate next number is the correct one.
                if len(digits) > 3: 
                    return digits.lstrip('0') or "0"

            # Fallback to old logic if regex miss
            candidates = re.findall(r'[\d][\d,\.]{0,}', line)
            clean_nums = []
            for c in candidates:
                digits = re.sub(r'[^\d]', '', c)
                if digits:
                    clean_nums.append(digits)
            if not clean_nums:
                continue
            # prefer candidates with commas and reasonable length
            with_commas = [c for c in candidates if ',' in c and len(re.sub(r'[^\d]', '', c)) <= 12]
            if with_commas:
                chosen = re.sub(r'[^\d]', '', with_commas[-1])
                return chosen.lstrip('0') or "0"
            short = [c for c in clean_nums if len(c) <= MAX_REASONABLE_LEN]
            if short:
                return short[-1].lstrip('0') or "0"
            # fallback: take last upto TAIL_DIGITS digits
            last = clean_nums[-1]
            if len(last) > TAIL_DIGITS:
                last = last[-TAIL_DIGITS:]
            return last.lstrip('0') or "0"
    return None

def extract_contract_date(text: str):
    m = re.search(r'\b(\d{1,2}-[A-Za-z]{3}-\d{4})\b', text)
    return m.group(1) if m else None

def extract_seller_fields_by_position(text: str) -> dict:
    """
    Locate 'Seller' keyword and extract the nearest Contact and Email
    appearing *after* it in the text stream using strict positional lookahead.
    """
    data = {}
    # Find "Seller" - usually near the end or distinct section
    # Use case-insensitive search
    m_seller = re.search(r'\bSeller\b', text, re.IGNORECASE)
    if not m_seller:
        return data

    start_idx = m_seller.end()
    relevant_text = text[start_idx:]

    # 1. Look for Contact
    # Find 'Contact' or 'Contact No' followed by digits
    # Use non-greedy match to find the *first* one
    m_cont = re.search(r'(?:Contact|Contact No)[\s\w|]*?[:]\s*([+\d\-\s]+)', relevant_text, re.IGNORECASE)
    if m_cont:
        val = m_cont.group(1).strip()
        val = sanitize_phone_string(val)
        if val and len(val) > 4: # Sanity check for minimum length
            data['Seller_ContactNo'] = val

    # 2. Look for Email
    # Find 'Email' or 'Email ID' followed by email-like string
    if m_email:
        data['Seller_EmailID'] = m_email.group(1).strip()
        
    return data

# regex for finding keys only
KEY_PIPE_RE = re.compile(r'(?P<label>[^:\n|]{1,80}\|[^:]{1,80})\s*:')
KEY_GENERIC_RE = re.compile(r'(?P<label>[^:\n]{1,80}?)\s*:')

# Helper to parse a text block with a specific field mapping
def parse_kv_block(text: str, field_mapping: dict) -> dict:
    local_data = {}
    
    # Pre-calculate normalized keys for this specific block
    local_norm_keys = {}
    for k, col in field_mapping.items():
        local_norm_keys[normalize_label_for_match(k)] = col

    # Strategy: Find all candidate KEYS, filter by validity, then extract values between them.
    matches = []
    
    # 1. Find matches
    # Use finditer for both patterns
    raw_matches = []
    for m in KEY_PIPE_RE.finditer(text):
        raw_matches.append({'m': m, 'type': 'pipe'})
    for m in KEY_GENERIC_RE.finditer(text):
        raw_matches.append({'m': m, 'type': 'generic'})
        
    # Sort matches by position
    raw_matches.sort(key=lambda x: x['m'].start())
    
    # Filter matches: Resolve overlaps and check validity
    valid_matches = []
    last_end = 0
    
    for rm in raw_matches:
        m = rm['m']
        start, end = m.start(), m.end()
        if start < last_end: continue # Skip overlaps
        
        raw_label = m.group('label')
        # Identify Key
        if '|' in raw_label:
            eng_label = raw_label.split('|')[-1].strip()
        else:
            eng_label = raw_label.strip()
        
        nl = normalize_label_for_match(eng_label)
        
        # Check validity against mapping
        matched_col = None
        if nl in local_norm_keys:
            matched_col = local_norm_keys[nl]
        else:
            # Fuzzy check
            best_ratio = 0.0
            best_col = None
            for k_norm, col in local_norm_keys.items():
                 ratio = difflib.SequenceMatcher(None, nl, k_norm).ratio()
                 if ratio > best_ratio:
                     best_ratio = ratio
                     best_col = col
            if best_col and best_ratio >= 0.75:
                matched_col = best_col
        
        if matched_col:
            valid_matches.append({
                'start': start,
                'end': end, # End of "Label :"
                'matched_col': matched_col,
                'raw_label': raw_label,
                'eng_label': eng_label
            })
            last_end = end
            
    # Iterate valid keys and extract values
    carry_over_text = None
    
    for i, match in enumerate(valid_matches):
        col = match['matched_col']
        
        # Value is from match['end'] to next_match['start']
        val_start = match['end']
        val_end = valid_matches[i+1]['start'] if i+1 < len(valid_matches) else len(text)
        
        raw_val = text[val_start:val_end]
        
        
        # Post-process
        val = remove_devanagari_and_noise(raw_val)
        
        # Special handling for carry-over (Address leakage into Email)
        if 'EmailID' in col:
             raw_lines = [ln.strip() for ln in raw_val.splitlines() if ln.strip()]
             if len(raw_lines) > 1:
                 if '@' in raw_lines[0]:
                     val = raw_lines[0] 
                     carry_over_text = " ".join(raw_lines[1:])
                 else:
                     val = " ".join(raw_lines)
             else:
                 val = " ".join(raw_lines)

        elif 'Address' in col:
            if carry_over_text:
                raw_lines = [ln.strip() for ln in raw_val.splitlines() if ln.strip()]
                current_addr = " ".join(raw_lines)
                val = f"{carry_over_text}, {current_addr}"
                carry_over_text = None 
            else:
                 val = remove_devanagari_and_noise(raw_val)
                
        elif 'ContactNo' in col:
             val = remove_devanagari_and_noise(raw_val)
             val = sanitize_phone_string(val)
        elif 'GSTIN' in col:
             val = remove_devanagari_and_noise(raw_val)
             val = re.sub(r'[^\w]', '', val)
        else:
             val = remove_devanagari_and_noise(raw_val)
        
        local_data[col] = val

    return local_data

    return local_data


def extract_dict_from_tables(pdf_path: Path) -> dict:
    """
    Extracts structured data from any detected tables in the PDF.
    Returns a dict with 'Seller_ContactNo', 'TotalOrderValueINR' etc if found.
    """
    data = {}
    try:
        with pdfplumber.open(pdf_path) as pdf:
            for page in pdf.pages:
                tables = page.extract_tables()
                for table in tables:
                    for row in table:
                        # We need to preserve newlines for block parsing
                        # row_text_list = [str(c) if c else '' for c in row] 
                        
                        # Check each cell for blocks
                        for cell_raw in row:
                            if not cell_raw: continue
                            cell_text = str(cell_raw)
                            
                            # Preprocess to normalize text BUT preserve structure
                            # preprocess_text returns lines joined by \n
                            norm_text = preprocess_text_preserve_layout(cell_text) 
                            norm_lower = norm_text.lower()
                            
                            # --- SELLER DETAILS BLOCK ---
                            # 'Seller' might become 'Seler'
                            if 'details' in norm_lower and ('seller' in norm_lower or 'seler' in norm_lower):
                                # 1. Parse all standard KV fields using SELLER_FIELDS
                                block_data = parse_kv_block(norm_text, SELLER_FIELDS)
                                data.update(block_data)

                                # 2. ROBUST Contact Logic (User Request) overrides regex parse
                                # Look for "Contact" in this cell specifically for Mobile extraction
                                # Use original cell_text for regex to avoid over-cleaning artifacts?
                                # Actually norm_text is better as it cleans spaces/cids.
                                m_cont = re.search(r'(?:Contact|Contact No)[\s\w|.]*?[:]\s*([+\d\-\s,]+)', norm_text, re.IGNORECASE)
                                if m_cont:
                                    raw_nums = m_cont.group(1)
                                    val_sanitized = sanitize_double_digits(raw_nums)
                                    digits_only = re.sub(r'[^\d]', ' ', val_sanitized)
                                    nums = re.findall(r'\b\d{10,}\b', digits_only)
                                    
                                    if nums:
                                        final_contacts = []
                                        seen = set()
                                        for n in nums:
                                            if n not in seen:
                                                final_contacts.append(n)
                                                seen.add(n)
                                        data['Seller_ContactNo'] = ", ".join(final_contacts)

                            # --- BUYER DETAILS BLOCK ---
                            elif 'details' in norm_lower and 'buyer' in norm_lower:
                                block_data = parse_kv_block(norm_text, BUYER_FIELDS)
                                data.update(block_data)

                            # --- PAYING DETAILS BLOCK ---
                            elif 'paying' in norm_lower and ('authority' in norm_lower or 'detail' in norm_lower):
                                if INCLUDE_PAYING_DETAILS:
                                    block_data = parse_kv_block(norm_text, PAYING_FIELDS)
                                    data.update(block_data)

                            # --- ORG DETAILS BLOCK ---
                            elif 'organisation' in norm_lower and 'details' in norm_lower:
                                if INCLUDE_ORG_DETAILS:
                                    block_data = parse_kv_block(norm_text, ORG_FIELDS)
                                    data.update(block_data)
                        # Check row for Total Order Value
                        # For TOV, we can flatten to single line for search
                        row_flat = [str(c).replace('\n', ' ').strip() if c else '' for c in row]
                        found_tov = False
                        for cell_text in row_flat:
                            if 'total order value' in cell_text.lower():
                                found_tov = True
                                break
                        
                        if found_tov:
                            # Look for the number in the SAME ROW (other cells)
                            candidates = []
                            for other in row_flat:
                                if 'total order value' in other.lower(): continue
                                if not other.strip(): continue
                                
                                s = sanitize_double_digits(other)
                                d = re.sub(r'[^\d]', '', s)
                                if len(d) > 2: 
                                    candidates.append(s)
                            
                            if candidates:
                                val = candidates[-1]
                                val = re.sub(r'[^\d]', '', val)
                                val = val.lstrip('0') or "0"
                                data['TotalOrderValueINR'] = val
                            
    except Exception as e:
        print(f"  [WARN] Table extraction failed: {e}")
        
    return data

# ---------- per-pdf processing ----------

def process_pdf(pdf_path: Path, debug_first: bool = False):
    print(f'\n=== Processing {pdf_path.name} ===')
    raw_text = extract_text_from_pdf(pdf_path)
    if not raw_text.strip():
        print('  WARNING: no text extracted by pdfplumber')
    text = normalize_text_preserve_lines(raw_text)
    text = preprocess_text(text)
    if debug_first:
        dbg = pdf_path.with_suffix('.normalized.txt')
        dbg.write_text(text, encoding='utf-8')
        print('  [DEBUG] normalized text written to', dbg)
    contract_no = pdf_path.stem
    contract_date = extract_contract_date(text)
    row = {'ContractNo': contract_no, 'GeneratedDate': contract_date}
    kv = parse_global_key_values(text)
    # ensure kv keys are strings
    for k, v in kv.items():
        row[k] = v
    
    # --- Positional Override for Seller/Buyer Details ---
    # 1. Table extraction (Highest Priority)
    table_data = extract_dict_from_tables(pdf_path)
    # Merge all table extracted data (overrides global parse)
    row.update(table_data)
    
    # 2. Textual Fallbacks if missing
    if not row.get('Seller_ContactNo') or not row.get('Seller_EmailID'):
        seller_data_text = extract_seller_fields_by_position(text)
        if not row.get('Seller_ContactNo') and seller_data_text.get('Seller_ContactNo'):
            row['Seller_ContactNo'] = seller_data_text['Seller_ContactNo']
        if not row.get('Seller_EmailID') and seller_data_text.get('Seller_EmailID'):
            row['Seller_EmailID'] = seller_data_text['Seller_EmailID']
    # ----------------------------------------------

    total = parse_total_order_value(text)
    if total is not None:
        # Apply double-digit artifact fix
        total = sanitize_double_digits(total)
        row['TotalOrderValueINR'] = total
    items = parse_items(text, contract_no)
    print(f"  Fields extracted (Org+Buyer+Paying+Seller): {len(kv)}, Items: {len(items)}")
    return row, items

# ---------- Main entry + Excel writing ----------

def main():
    parser = argparse.ArgumentParser(description='Extract GeM contract PDFs into Excel.')
    parser.add_argument('-t', '--test', type=int, help='process first N PDFs (test mode).')
    args = parser.parse_args()

    all_pdfs = sorted(PDF_DIR.rglob('*.pdf'))
    if not all_pdfs:
        print('No PDFs found under', PDF_DIR.resolve())
        return
    pdf_files = all_pdfs[:args.test] if args.test else all_pdfs
    print(f'Found {len(all_pdfs)} PDFs, processing {len(pdf_files)} now.')

    contracts = []
    items = []
    for idx, pdf in enumerate(pdf_files):
        try:
            row, item_rows = process_pdf(pdf, debug_first=(idx == 0))
            contracts.append(row)
            items.extend(item_rows)
        except Exception as e:
            print('ERROR processing', pdf, ':', e)

    df_contracts = pd.DataFrame(contracts)
    df_items = pd.DataFrame(items)

    # Ensure required columns exist and in correct order
    required_contract_cols = [
        'ContractNo','GeneratedDate',
        'Buyer_Designation','Buyer_ContactNo','Buyer_EmailID','Buyer_GSTIN','Buyer_Address',
        'Seller_GeMSellerID','Seller_CompanyName','Seller_ContactNo','Seller_EmailID','Seller_Address',
        'Seller_MSMERegistrationNumber','Seller_MSESocialCategory','Seller_MSEGender','Seller_GSTIN',
        'TotalOrderValueINR'
    ]

    if INCLUDE_ORG_DETAILS:
        required_contract_cols.extend(['Org_Type','Org_Ministry','Org_Department','Org_OrganisationName','Org_OfficeZone'])

    if INCLUDE_PAYING_DETAILS:
        required_contract_cols.extend(['Paying_Role','Paying_PaymentMode','Paying_Designation','Paying_EmailID','Paying_GSTIN','Paying_Address'])
    for c in required_contract_cols:
        if c not in df_contracts.columns:
            df_contracts[c] = pd.NA
    df_contracts = df_contracts[required_contract_cols]

    required_item_cols = ['ContractNo','ItemNo','ProductName','Brand','BrandType','CatalogueStatus','SellingAs',
                          'CategoryNameQuadrant','Model','HSNCode','OrderedQuantity','Unit','UnitPriceINR',
                          'TaxBifurcationINR','PriceInclusiveINR']
    for c in required_item_cols:
        if c not in df_items.columns:
            df_items[c] = pd.NA
    df_items = df_items[required_item_cols]

    # Convert numeric/special columns to strings so Excel won't use scientific notation
    df_contracts['TotalOrderValueINR'] = df_contracts['TotalOrderValueINR'].astype(str).fillna('')
    for col in ['Buyer_ContactNo','Paying_ContactNo','Seller_ContactNo']:
        if col in df_contracts.columns:
            df_contracts[col] = df_contracts[col].astype(str).fillna('')

    for col in ['UnitPriceINR','PriceInclusiveINR','OrderedQuantity']:
        if col in df_items.columns:
            df_items[col] = df_items[col].astype(str).fillna('')

    # Write Excel with text formatting for those columns
    print(f"DONE. Excel at {OUTPUT_XLSX}")
    # Ensure dir
    OUTPUT_XLSX.parent.mkdir(parents=True, exist_ok=True)
    with pd.ExcelWriter(OUTPUT_XLSX) as writer:
        df_contracts.to_excel(writer, sheet_name='Contracts', index=False)
        df_items.to_excel(writer, sheet_name='Items', index=False)
        workbook = writer.book
        text_fmt = workbook.add_format({'num_format': '@'})  # text format

        # Set Contracts TotalOrderValueINR as text column
        ws_contracts = writer.sheets['Contracts']
        col_idx = df_contracts.columns.get_loc('TotalOrderValueINR')
        ws_contracts.set_column(col_idx, col_idx, 20, text_fmt)

        # Make phone columns text (if present)
        for col in ['Buyer_ContactNo','Paying_ContactNo','Seller_ContactNo']:
            if col in df_contracts.columns:
                idx = df_contracts.columns.get_loc(col)
                ws_contracts.set_column(idx, idx, 18, text_fmt)

        # Items: set string columns for numeric fields
        ws_items = writer.sheets['Items']
        for col in ['UnitPriceINR','PriceInclusiveINR','OrderedQuantity']:
            if col in df_items.columns:
                idx = df_items.columns.get_loc(col)
                ws_items.set_column(idx, idx, 18, text_fmt)

    print('DONE. Excel at', OUTPUT_XLSX.resolve())
    print(f'Contracts rows: {len(df_contracts)}, Items rows: {len(df_items)}')

if __name__ == '__main__':
    main()
