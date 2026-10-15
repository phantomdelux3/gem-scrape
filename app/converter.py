"""
Convert downloaded GeM contract PDFs (plus saved search results) into Excel / CSV / JSON.

- Parses PDFs in parallel worker processes and caches each result, so re-running only
  parses new or changed PDFs.
- The user chooses which sheets and which columns to export (everything by default).
- Pure Python + XlsxWriter (constant-memory mode), no pandas: small, fast, low RAM.
"""

from concurrent.futures import ProcessPoolExecutor
from concurrent.futures.process import BrokenProcessPool
from datetime import datetime
from pathlib import Path
import csv
import json
import os
import re
import time

from .pdf_parser import PARSER_VERSION, parse_date, safe_process, split_location

# Any Indic script (Hindi etc.), PDF glyph codes and XML-illegal control characters
NON_ENGLISH_RE = re.compile("[ऀ-෿‌‍]+")
CID_RE = re.compile(r"\(cid:\d+\)")
ILLEGAL_XML_RE = re.compile(r"[\x00-\x08\x0b\x0c\x0e-\x1f]")

EXCEL_MAX_ROWS = 1_048_000

# ---------------------------------------------------------------------------
# Schema: sheet -> [(key, column title, kind)]   kind: text | date | money | number
# ---------------------------------------------------------------------------
SCHEMA = {
    "Contracts": [
        ("ContractNo", "Contract No", "text"), ("GeneratedDate", "Contract Date", "date"),
        ("Status", "Contract Status", "text"), ("BuyingMode", "Buying Mode", "text"),
        ("ProcurementMode", "Procurement Mode", "text"), ("BidNo", "Bid/RA No", "text"),
        ("BidScheduleName", "Bid Schedule", "text"),
        ("TotalOrderValue", "Total Order Value (INR)", "money"), ("ItemCount", "Item Count", "number"),
        ("TotalQuantity", "Total Quantity", "number"), ("Products", "Products", "text"),
        ("Categories", "Categories", "text"), ("Brands", "Brands", "text"), ("HSNCodes", "HSN Codes", "text"),
        ("Org_Type", "Org Type", "text"), ("Org_Ministry", "Ministry", "text"),
        ("Org_Department", "Department", "text"), ("Org_Name", "Organisation Name", "text"),
        ("Org_OfficeZone", "Office Zone", "text"),
        ("Buyer_Name", "Buyer Name", "text"), ("Buyer_Designation", "Buyer Designation", "text"),
        ("Buyer_Contact", "Buyer Contact", "text"), ("Buyer_Email", "Buyer Email", "text"),
        ("Buyer_GSTIN", "Buyer GSTIN", "text"), ("Buyer_Address", "Buyer Address", "text"),
        ("Buyer_District", "Buyer District", "text"), ("Buyer_State", "Buyer State", "text"),
        ("Buyer_Pincode", "Buyer Pincode", "text"),
        ("IFD_Concurrence", "IFD Concurrence", "text"),
        ("AdminApproval_Designation", "Administrative Approval (Designation)", "text"),
        ("FinancialApproval_Designation", "Financial Approval (Designation)", "text"),
        ("Paying_Name", "Paying Authority Name", "text"), ("Paying_Role", "Paying Authority Role", "text"),
        ("Paying_PaymentMode", "Payment Mode", "text"), ("Paying_Designation", "Paying Authority Designation", "text"),
        ("Paying_Email", "Paying Authority Email", "text"), ("Paying_Contact", "Paying Authority Contact", "text"),
        ("Paying_GSTIN", "Paying Authority GSTIN", "text"), ("Paying_Address", "Paying Authority Address", "text"),
        ("Paying_Timelines", "Payment Timelines", "text"),
        ("Seller_GeMID", "Seller GeM ID", "text"), ("Seller_Company", "Seller Company", "text"),
        ("Seller_Contact", "Seller Contact", "text"), ("Seller_Email", "Seller Email", "text"),
        ("Seller_Address", "Seller Address", "text"), ("Seller_District", "Seller District", "text"),
        ("Seller_State", "Seller State", "text"), ("Seller_Pincode", "Seller Pincode", "text"),
        ("Seller_GSTIN", "Seller GSTIN", "text"), ("Seller_AllGSTINs", "Seller All GSTINs", "text"),
        ("Seller_MSME_RegNo", "Seller MSME Reg No", "text"), ("Seller_MSME_Verified", "Seller MSME Verified", "text"),
        ("Seller_MSME_StatusVerifiedByBuyer", "MSME Status (Verified by Buyer)", "text"),
        ("Seller_MSE_SocialCategory", "Seller MSE Social Category", "text"),
        ("Seller_MSE_Gender", "Seller MSE Gender", "text"),
        ("GSTInvoiceInNameOf", "GST Invoice In Name Of", "text"),
        ("DeliveryInstructions", "Delivery Instructions", "text"),
        ("ConsigneeCount", "Consignee Count", "number"), ("ConsigneeStates", "Consignee States", "text"),
        ("DeliveryStart", "Delivery Start After", "date"), ("DeliveryEnd", "Delivery Completed By", "date"),
        ("DeliveryDays", "Delivery Days", "number"),
        ("Service_StartDate", "Service Start Date", "date"), ("Service_EndDate", "Service End Date", "date"),
        ("Service_BillingCycle", "Billing Cycle", "text"),
        ("ePBG_Detail", "ePBG Detail", "text"), ("ePBG_AdvisoryBank", "ePBG Advisory Bank", "text"),
        ("ePBG_Percentage", "ePBG %", "text"),
        ("AdditionalRequiredDocs", "Additional Required Documents", "text"),
        ("SourceFile", "Source File", "text"),
    ],
    "Items": [
        ("ContractNo", "Contract No", "text"), ("GeneratedDate", "Contract Date", "date"),
        ("ItemNo", "Item No", "number"), ("ProductName", "Product / Service Name", "text"),
        ("ServiceDetails", "Service Details", "text"), ("Brand", "Brand", "text"),
        ("BrandType", "Brand Type", "text"), ("Model", "Model", "text"), ("Category", "Category", "text"),
        ("Quadrant", "Quadrant", "text"), ("CatalogueStatus", "Catalogue Status", "text"),
        ("SellingAs", "Selling As", "text"), ("HSNCode", "HSN Code", "text"),
        ("CountryOfOrigin", "Country Of Origin", "text"), ("Quantity", "Quantity", "number"),
        ("Unit", "Unit", "text"), ("UnitPrice", "Unit Price (INR)", "money"), ("GST_Rate", "GST %", "number"),
        ("GST_Amount", "GST Amount (INR)", "money"), ("TaxBifurcation", "Tax Bifurcation", "text"),
        ("TotalPrice", "Total Price (INR)", "money"),
        ("ProcurementMode", "Procurement Mode", "text"), ("BidNo", "Bid/RA No", "text"),
        ("Org_Department", "Department", "text"), ("Org_Name", "Organisation Name", "text"),
        ("Buyer_Designation", "Buyer Designation", "text"), ("Buyer_Email", "Buyer Email", "text"),
        ("Buyer_State", "Buyer State", "text"), ("Seller_Company", "Seller Company", "text"),
        ("Seller_Contact", "Seller Contact", "text"), ("Seller_Email", "Seller Email", "text"),
        ("Seller_State", "Seller State", "text"),
    ],
    "Consignees": [
        ("ContractNo", "Contract No", "text"), ("GeneratedDate", "Contract Date", "date"), ("SNo", "S.No", "number"),
        ("Name", "Consignee Name", "text"), ("Designation", "Consignee Designation", "text"),
        ("Email", "Consignee Email", "text"), ("Contact", "Consignee Contact", "text"),
        ("GSTIN", "Consignee GSTIN", "text"), ("Address", "Consignee Address", "text"),
        ("District", "District", "text"), ("State", "State", "text"), ("Pincode", "Pincode", "text"),
        ("Item", "Item", "text"), ("LotNo", "Lot No", "text"), ("Quantity", "Quantity", "number"),
        ("DeliveryStart", "Delivery Start After", "date"), ("DeliveryEnd", "Delivery Completed By", "date"),
    ],
    "Specifications": [
        ("ContractNo", "Contract No", "text"), ("Product", "Product", "text"),
        ("Group", "Specification Group", "text"), ("Specification", "Specification", "text"),
        ("Value", "Value", "text"),
    ],
    "Seller Summary": [
        ("Seller_Company", "Seller Company", "text"), ("Seller_GeMID", "Seller GeM ID", "text"),
        ("Seller_Contact", "Seller Contact", "text"), ("Seller_Email", "Seller Email", "text"),
        ("Seller_State", "Seller State", "text"), ("Seller_GSTIN", "Seller GSTIN", "text"),
        ("Seller_MSME_RegNo", "Seller MSME Reg No", "text"), ("Categories", "Categories", "text"),
        ("Departments", "Buyers (Departments)", "text"), ("Contracts", "Contracts", "number"),
        ("TotalValue", "Total Value (INR)", "money"), ("FirstContract", "First Contract", "date"),
        ("LastContract", "Last Contract", "date"),
    ],
    "Buyer Summary": [
        ("Buyer_Email", "Buyer Email", "text"), ("Buyer_Designation", "Buyer Designation", "text"),
        ("Buyer_Contact", "Buyer Contact", "text"), ("Org_Department", "Department", "text"),
        ("Org_Name", "Organisation Name", "text"), ("Org_Ministry", "Ministry", "text"),
        ("Buyer_State", "Buyer State", "text"), ("Categories", "Categories", "text"),
        ("Sellers", "Sellers", "text"), ("Contracts", "Contracts", "number"),
        ("TotalValue", "Total Value (INR)", "money"), ("FirstContract", "First Contract", "date"),
        ("LastContract", "Last Contract", "date"),
    ],
    "Search Results": [
        ("ContractNo", "Contract No", "text"), ("ContractDate", "Contract Date", "date"),
        ("Status", "Contract Status", "text"), ("BuyingMode", "Buying Mode", "text"),
        ("BidNo", "Bid Number", "text"), ("Total", "Total (INR)", "money"),
        ("OrgType", "Organization Type", "text"), ("Ministry", "Ministry", "text"),
        ("Department", "Department", "text"), ("OrgName", "Organization Name", "text"),
        ("OfficeZone", "Office Zone", "text"), ("BuyerDesignation", "Buyer Designation", "text"),
        ("Products", "Products", "text"), ("Brands", "Brands", "text"), ("Models", "Models", "text"),
        ("Quantity", "Ordered Quantity", "number"), ("SearchCategory", "Searched Category", "text"),
    ],
}
SHEET_ORDER = list(SCHEMA)


def default_selection():
    """Every sheet and every column selected."""
    return {sheet: [title for _, title, _ in cols] for sheet, cols in SCHEMA.items()}


def english(v):
    """Final safety net: exported text is English only (no Hindi / glyph codes / control chars)."""
    if isinstance(v, str):
        v = CID_RE.sub(" ", v)
        v = NON_ENGLISH_RE.sub(" ", v)
        v = ILLEGAL_XML_RE.sub(" ", v)
        v = re.sub(r"[ \t]{2,}", " ", v).strip(" |")
        return v or None
    return v


# ---------------------------------------------------------------------------
# Parse PDFs (parallel + cached)
# ---------------------------------------------------------------------------
def file_sig(path):
    st = Path(path).stat()
    return f"{st.st_size}:{int(st.st_mtime)}"


def default_workers():
    return max(1, min(4, (os.cpu_count() or 2) - 1))


class ParsePool:
    """Reads PDFs in worker processes and stores the results in the database.

    Used by the converter and, during scraping, to read each PDF right after it downloads.
    Falls back to reading in this process if a worker dies (e.g. low memory)."""

    def __init__(self, db, workers=None):
        self.db = db
        self.workers = workers or default_workers()
        self._ex = ProcessPoolExecutor(max_workers=self.workers) if self.workers > 1 else None
        self._pending = {}   # future -> (path, sig)
        self.done = 0
        self.errors = 0

    @property
    def backlog(self):
        return len(self._pending)

    def submit(self, path):
        path = str(path)
        sig = file_sig(path)
        if self._ex is not None:
            try:
                self._pending[self._ex.submit(safe_process, path)] = (path, sig)
                return
            except (BrokenProcessPool, RuntimeError):
                self._ex = None
        self._store([(path, sig, *safe_process(path)[1:])])

    def collect(self, wait=False, cancel=None):
        """Save finished results. With wait=True, block until everything submitted is done."""
        while self._pending:
            if cancel is not None and cancel.is_set():
                for f in self._pending:
                    f.cancel()
                self._pending.clear()
                return
            finished = [f for f in self._pending if f.done()]
            rows = []
            for f in finished:
                path, sig = self._pending.pop(f)
                try:
                    _, data, err = f.result()
                except (BrokenProcessPool, Exception) as e:  # noqa: BLE001 - retry in this process
                    if isinstance(e, BrokenProcessPool):
                        self._ex = None
                    _, data, err = safe_process(path)
                rows.append((path, sig, data, err))
            if rows:
                self._store(rows)
            if not wait:
                return
            if self._pending and not finished:
                time.sleep(0.05)

    def _store(self, rows):
        self.db.save_parsed(rows, PARSER_VERSION)
        for path, _, _, err in rows:
            self.done += 1
            if err:
                self.errors += 1
                self.db.add_failure("READ", path, err)

    def close(self):
        if self._ex is not None:
            self._ex.shutdown(wait=False, cancel_futures=True)
            self._ex = None


def parse_pdfs(pdf_files, db, workers=None, progress=None, cancel=None, use_cache=True):
    """Make sure every PDF is parsed into the database (re-using earlier results).

    progress(done, total, parsed, cached, errors) is called from this thread."""
    known = db.parsed_sigs(PARSER_VERSION) if use_cache else {}
    todo = [p for p in pdf_files if known.get(str(p)) != file_sig(p)]
    total, cached = len(pdf_files), len(pdf_files) - len(todo)
    if progress:
        progress(cached, total, 0, cached, 0)
    pool = ParsePool(db, workers if len(todo) > 3 else 1)
    try:
        it = iter(todo)
        for p in it:
            if cancel is not None and cancel.is_set():
                break
            pool.submit(p)
            # keep a bounded number of jobs in flight (low memory, quick cancel)
            while pool.backlog >= pool.workers * 3:
                pool.collect(cancel=cancel)
                if pool.backlog >= pool.workers * 3:
                    time.sleep(0.05)
                if progress:
                    progress(cached + pool.done, total, pool.done - pool.errors, cached, pool.errors)
            if progress:
                progress(cached + pool.done, total, pool.done - pool.errors, cached, pool.errors)
        pool.collect(wait=True, cancel=cancel)
    finally:
        pool.close()
    if progress:
        progress(cached + pool.done, total, pool.done - pool.errors, cached, pool.errors)
    return {"total": total, "cached": cached, "parsed": pool.done - pool.errors, "errors": pool.errors}


# ---------------------------------------------------------------------------
# Build sheet rows
# ---------------------------------------------------------------------------
def _uniq(vals, limit=2000):
    s = " | ".join(dict.fromkeys(str(v) for v in vals if v not in (None, "")))
    return s[:limit] or None


def build_rows(parsed, listing=None):
    """parsed: iterable of per-PDF data dicts; listing: {contract no: search-result card}."""
    listing = listing or {}
    sheets = {name: [] for name in SHEET_ORDER}
    seen = set()
    for data in parsed:
        c = dict(data["contract"])
        if c["ContractNo"] in seen:
            continue
        seen.add(c["ContractNo"])
        card = listing.get(c["ContractNo"], {})
        c["Status"] = card.get("Status")
        c["BuyingMode"] = card.get("BuyingMode")

        for prefix in ("Buyer", "Seller"):
            d, s, pin = split_location(c.get(f"{prefix}_Address"))
            c[f"{prefix}_District"], c[f"{prefix}_State"], c[f"{prefix}_Pincode"] = d, s, pin

        its, cons = data["items"], data["consignees"]
        c["ItemCount"] = len(its)
        qtys = [i["Quantity"] for i in its if isinstance(i.get("Quantity"), (int, float))]
        c["TotalQuantity"] = sum(qtys) if qtys else None
        c["Products"] = _uniq(i.get("ProductName") for i in its)
        c["Categories"] = _uniq(i.get("Category") for i in its)
        c["Brands"] = _uniq(i.get("Brand") for i in its)
        c["HSNCodes"] = _uniq(i.get("HSNCode") for i in its
                              if i.get("HSNCode") and "not specified" not in str(i["HSNCode"]).lower())
        for row in cons:
            row["District"], row["State"], row["Pincode"] = split_location(row.get("Address"))
        c["ConsigneeCount"] = len({(r.get("SNo"), r.get("Email"), r.get("Address")) for r in cons}) or None
        c["ConsigneeStates"] = _uniq(r.get("State") for r in cons)
        starts = [d for d in (parse_date(r.get("DeliveryStart")) for r in cons) if d]
        ends = [d for d in (parse_date(r.get("DeliveryEnd")) for r in cons) if d]
        c["DeliveryStart"] = min(starts).strftime("%d-%b-%Y") if starts else None
        c["DeliveryEnd"] = max(ends).strftime("%d-%b-%Y") if ends else None
        gen = parse_date(c.get("GeneratedDate"))
        c["DeliveryDays"] = (max(ends) - gen).days if ends and gen else None
        sheets["Contracts"].append(c)

        ctx = {k: c.get(k) for k in ("ContractNo", "GeneratedDate", "ProcurementMode", "BidNo", "Org_Department",
                                     "Org_Name", "Buyer_Designation", "Buyer_Email", "Buyer_State",
                                     "Seller_Company", "Seller_Contact", "Seller_Email", "Seller_State")}
        sheets["Items"].extend({**ctx, **i} for i in its)
        sheets["Consignees"].extend({"ContractNo": c["ContractNo"], "GeneratedDate": c.get("GeneratedDate"), **r}
                                    for r in cons)
        sheets["Specifications"].extend({"ContractNo": c["ContractNo"], **s} for s in data["specs"])

    sheets["Seller Summary"] = _summary(sheets["Contracts"], "Seller_Company", {
        "Seller_GeMID": "Seller_GeMID", "Seller_Contact": "Seller_Contact", "Seller_Email": "Seller_Email",
        "Seller_State": "Seller_State", "Seller_GSTIN": "Seller_GSTIN", "Seller_MSME_RegNo": "Seller_MSME_RegNo",
        "Categories": "Categories", "Departments": "Org_Department"})
    sheets["Buyer Summary"] = _summary(sheets["Contracts"], "Buyer_Email", {
        "Buyer_Designation": "Buyer_Designation", "Buyer_Contact": "Buyer_Contact",
        "Org_Department": "Org_Department", "Org_Name": "Org_Name", "Org_Ministry": "Org_Ministry",
        "Buyer_State": "Buyer_State", "Categories": "Categories", "Sellers": "Seller_Company"})
    sheets["Search Results"] = sorted(listing.values(), key=lambda r: (r.get("ContractDate") or "", r["ContractNo"]))
    return sheets


def _summary(contracts, key, fields):
    groups = {}
    for c in contracts:
        groups.setdefault(c.get(key), []).append(c)
    out = []
    for k, rows in groups.items():
        if k in (None, ""):
            continue
        r = {key: k}
        for out_key, src in fields.items():
            r[out_key] = _uniq((x.get(src) for x in rows), 1000)
        dates = [d for d in (parse_date(x.get("GeneratedDate")) for x in rows) if d]
        r["Contracts"] = len(rows)
        r["TotalValue"] = sum(x["TotalOrderValue"] for x in rows if isinstance(x.get("TotalOrderValue"), (int, float)))
        r["FirstContract"] = min(dates).strftime("%d-%b-%Y") if dates else None
        r["LastContract"] = max(dates).strftime("%d-%b-%Y") if dates else None
        out.append(r)
    out.sort(key=lambda r: r["TotalValue"] or 0, reverse=True)
    return out


# ---------------------------------------------------------------------------
# Export
# ---------------------------------------------------------------------------
def _as_date(v):
    if isinstance(v, datetime):
        return v
    if not v:
        return None
    s = str(v).strip()
    for fmt in ("%d-%b-%Y", "%d-%m-%Y", "%d/%m/%Y %H:%M", "%d/%m/%Y"):
        try:
            return datetime.strptime(s, fmt)
        except ValueError:
            pass
    return None


def _as_number(v):
    if isinstance(v, (int, float)):
        return v
    if v in (None, ""):
        return None
    try:
        f = float(str(v).replace(",", ""))
        return int(f) if f.is_integer() else f
    except ValueError:
        return None


def _columns_for(sheet, selection):
    wanted = set(selection.get(sheet, []))
    return [c for c in SCHEMA[sheet] if c[1] in wanted]


def export(sheets, selection, out_dir, base_name, formats, errors=None, log=None):
    """Write the selected sheets/columns. formats: subset of {'xlsx', 'csv', 'json'}.
    Returns list of written file paths."""
    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    written = []
    active = [s for s in SHEET_ORDER if selection.get(s)]

    if "xlsx" in formats:
        written.append(_write_xlsx(out_dir / f"{base_name}.xlsx", sheets, selection, active, errors))
    if "csv" in formats:
        for sheet in active:
            cols = _columns_for(sheet, selection)
            path = out_dir / f"{base_name} - {sheet}.csv"
            with open(path, "w", newline="", encoding="utf-8-sig") as f:
                w = csv.writer(f)
                w.writerow([t for _, t, _ in cols])
                for row in sheets.get(sheet, []):
                    w.writerow([_text_value(row.get(k), kind) for k, _, kind in cols])
            written.append(path)
    if "json" in formats:
        path = out_dir / f"{base_name}.json"
        payload = {}
        for sheet in active:
            cols = _columns_for(sheet, selection)
            payload[sheet] = [{t: _json_value(row.get(k), kind) for k, t, kind in cols} for row in sheets.get(sheet, [])]
        with open(path, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=1)
        written.append(path)
    return written


def _text_value(v, kind):
    v = english(v)
    if v is None:
        return ""
    if kind == "date":
        d = _as_date(v)
        return d.strftime("%d-%m-%Y") if d else v
    return v


def _json_value(v, kind):
    v = english(v)
    if kind in ("money", "number"):
        n = _as_number(v)
        return n if n is not None else v
    if kind == "date":
        d = _as_date(v)
        return d.strftime("%Y-%m-%d") if d else v
    return v


def _write_xlsx(path, sheets, selection, active, errors):
    tables = [(sheet, _columns_for(sheet, selection), sheets.get(sheet, [])) for sheet in active]
    if errors:
        tables.append(("Errors", [("File", "File", "text"), ("Error", "Error", "text")], errors))
    return write_tables_xlsx(path, tables)


def write_tables_xlsx(path, tables):
    """tables: [(sheet name, [(key, title, kind)], [row dicts])] -> one formatted .xlsx file."""
    import xlsxwriter

    path = Path(path)
    try:
        with open(path, "ab"):
            pass
    except PermissionError:
        raise PermissionError(f"{path.name} is open in Excel - close it and try again.")
    wb = xlsxwriter.Workbook(str(path), {"constant_memory": True, "strings_to_numbers": False,
                                         "strings_to_urls": False, "strings_to_formulas": False})
    header = wb.add_format({"bold": True, "bg_color": "#DDEBF7", "border": 1, "text_wrap": True, "valign": "top"})
    fmt = {"date": wb.add_format({"num_format": "dd-mmm-yyyy"}),
           "money": wb.add_format({"num_format": "#,##0.00"}),
           "number": wb.add_format({"num_format": "#,##0.##"}),
           "text": wb.add_format({"num_format": "@"})}

    def write(sheet_name, cols, rows):
        chunks = [rows[i:i + EXCEL_MAX_ROWS] for i in range(0, len(rows), EXCEL_MAX_ROWS)] or [[]]
        for n, chunk in enumerate(chunks):
            ws = wb.add_worksheet(sheet_name if n == 0 else f"{sheet_name[:26]} ({n + 1})")
            for ci, (key, title, kind) in enumerate(cols):
                sample = [len(str(r.get(key))) for r in chunk[:300] if r.get(key) not in (None, "")]
                width = min(max([len(title)] + sample) + 2, 60)
                if kind == "date":
                    width = 14
                elif kind == "money":
                    width = max(width, 16)
                ws.set_column(ci, ci, max(width, 9), fmt[kind])
                ws.write_string(0, ci, title, header)
            ws.freeze_panes(1, 1)
            for ri, row in enumerate(chunk, start=1):
                for ci, (key, _, kind) in enumerate(cols):
                    v = english(row.get(key))
                    if v is None:
                        continue
                    if kind == "date":
                        d = _as_date(v)
                        if d:
                            ws.write_datetime(ri, ci, d, fmt["date"])
                        else:
                            ws.write_string(ri, ci, str(v)[:32000])
                    elif kind in ("money", "number"):
                        num = _as_number(v)
                        if num is not None:
                            ws.write_number(ri, ci, num, fmt[kind])
                        else:
                            ws.write_string(ri, ci, str(v)[:32000])
                    else:
                        ws.write_string(ri, ci, str(v)[:32000])
            if cols:
                ws.autofilter(0, 0, max(len(chunk), 1), len(cols) - 1)

    for sheet_name, cols, rows in tables:
        write(sheet_name, cols, rows)
    wb.close()
    return path


# ---------------------------------------------------------------------------
# One-call conversion used by the GUI and the CLI
# ---------------------------------------------------------------------------
def find_pdfs(pdf_dir):
    pdf_dir = Path(pdf_dir)
    if not pdf_dir.exists():
        return []
    out = []
    stack = [str(pdf_dir)]
    while stack:  # os.scandir is much faster than Path.rglob on big folders
        d = stack.pop()
        try:
            with os.scandir(d) as it:
                for e in it:
                    if e.is_dir(follow_symlinks=False):
                        stack.append(e.path)
                    elif e.name.lower().endswith(".pdf"):
                        out.append(Path(e.path))
        except OSError:
            continue
    out.sort()
    return out


def convert(db, pdf_dir, out_dir, base_name="gem_contracts", formats=("xlsx",), selection=None, workers=None,
            progress=None, cancel=None, use_cache=True, limit=None, log=None):
    """Parse (new) PDFs into the database, then export every parsed contract + search results."""
    selection = selection or default_selection()
    log = log or (lambda msg: None)
    t0 = time.time()
    pdfs = find_pdfs(pdf_dir)
    if limit:
        pdfs = pdfs[:limit]
    log(f"Found {len(pdfs):,} PDF(s) in {pdf_dir}")
    stats = parse_pdfs(pdfs, db, workers, progress, cancel, use_cache)
    if cancel is not None and cancel.is_set():
        log("Conversion cancelled.")
        return None
    listing = db.listing()
    if listing:
        log(f"{len(listing):,} search-result row(s) in the database")
    errors = []
    parsed = []
    for path, data, err in db.iter_parsed([str(p) for p in pdfs], PARSER_VERSION):
        if err:
            errors.append({"File": path, "Error": err})
        elif data:
            parsed.append(data)
    parsed.sort(key=lambda d: d["contract"].get("ContractNo") or "")
    sheets = build_rows(parsed, listing)
    del parsed
    log("Writing files ...")
    files = export(sheets, selection, out_dir, base_name, formats, errors, log)
    stats.update({"seconds": time.time() - t0, "files": [str(f) for f in files],
                  "rows": {k: len(v) for k, v in sheets.items()}, "errors": len(errors)})
    return stats


__all__ = ["SCHEMA", "SHEET_ORDER", "ParsePool", "default_selection", "default_workers", "convert", "find_pdfs",
           "export", "build_rows", "parse_pdfs", "english", "file_sig", "write_tables_xlsx"]
