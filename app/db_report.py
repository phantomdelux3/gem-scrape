"""
What is in the database, per category: used by the Database and History tabs.

Every contract is counted once by its contract number. A contract that was found under
two categories is listed under both categories, but only once in the overall totals.
"""

from collections import Counter
from datetime import datetime
import time

STATUS_READ = "Read into database"
STATUS_PDF = "PDF downloaded, not read yet"
STATUS_UNREADABLE = "PDF could not be read"
STATUS_FAILED = "PDF download failed"
STATUS_NO_PDF = "No PDF yet"
STATUSES = [STATUS_READ, STATUS_PDF, STATUS_UNREADABLE, STATUS_FAILED, STATUS_NO_PDF]

# (key, column title, kind) - kinds as in converter.SCHEMA, so the same Excel writer is used
SEARCH_COLUMNS = [
    ("no", "Contract No", "text"), ("date", "Contract Date", "date"), ("categories", "Categories", "text"),
    ("status", "Database Status", "text"), ("contract_status", "Contract Status", "text"),
    ("bid", "Bid/RA No", "text"), ("ministry", "Ministry", "text"), ("department", "Department", "text"),
    ("org", "Organisation", "text"), ("buyer", "Buyer Designation", "text"), ("buyer_email", "Buyer Email", "text"),
    ("products", "Products", "text"), ("brands", "Brands", "text"), ("value", "Total Value (INR)", "money"),
    ("seller", "Seller Company", "text"), ("seller_email", "Seller Email", "text"),
    ("seller_contact", "Seller Contact", "text"), ("added", "Added To Database", "text"),
    ("pdf", "PDF File", "text"),
]

PROGRESS_COLUMNS = [
    ("category", "Category / Search", "text"), ("found", "Contracts Found", "number"),
    ("downloaded", "PDFs Downloaded", "number"), ("read", "Read Into Database", "number"),
    ("pending", "Downloaded, Not Read Yet", "number"), ("unreadable", "Unreadable PDFs", "number"),
    ("failed", "Download Failed", "number"), ("missing", "No PDF Yet", "number"),
    ("percent", "Complete %", "number"), ("shared", "Also In Other Categories", "number"),
    ("last_added", "Last Added", "text"),
]


def _date(s):
    if not s:
        return None
    s = str(s).strip().split()[0]
    for f in ("%d/%m/%Y", "%d-%b-%Y", "%d-%m-%Y", "%Y-%m-%d"):
        try:
            return datetime.strptime(s, f).date()
        except ValueError:
            pass
    return None


def _fmt_time(ts):
    return time.strftime("%d-%m-%Y %H:%M", time.localtime(ts)) if ts else ""


def prepare(rows):
    """Add display fields, status and a lowercase search text to each contract row (in place)."""
    for r in rows:
        card, info = r["card"], r["info"] or {}
        if info:
            r["status"] = STATUS_READ
        elif r["read_error"]:
            r["status"] = STATUS_UNREADABLE
        elif r["pdf"]:
            r["status"] = STATUS_PDF
        elif r["download_failed"]:
            r["status"] = STATUS_FAILED
        else:
            r["status"] = STATUS_NO_PDF
        d = _date(card.get("ContractDate")) or _date(info.get("GeneratedDate"))
        r["date_obj"] = d
        r["date"] = d.strftime("%d-%m-%Y") if d else ""
        r["categories"] = " | ".join(r["sources"])
        r["contract_status"] = card.get("Status") or ""
        r["bid"] = card.get("BidNo") or info.get("BidNo") or ""
        r["ministry"] = card.get("Ministry") or info.get("Org_Ministry") or ""
        r["department"] = card.get("Department") or info.get("Org_Department") or ""
        r["org"] = card.get("OrgName") or info.get("Org_Name") or ""
        r["buyer"] = card.get("BuyerDesignation") or info.get("Buyer_Designation") or ""
        r["buyer_email"] = info.get("Buyer_Email") or ""
        r["products"] = card.get("Products") or info.get("Products") or ""
        r["brands"] = card.get("Brands") or ""
        value = card.get("Total") if card.get("Total") not in (None, "") else info.get("TotalOrderValue")
        r["value"] = value if isinstance(value, (int, float)) else (value or "")
        r["seller"] = info.get("Seller_Company") or ""
        r["seller_email"] = info.get("Seller_Email") or ""
        r["seller_contact"] = info.get("Seller_Contact") or ""
        r["added"] = _fmt_time(r["first_seen"])
        r["pdf"] = r["pdf"] or ""
        r["hay"] = " ".join(str(r[k]) for k, _, _ in SEARCH_COLUMNS
                            if k not in ("date", "added", "pdf")).lower() + " " + info.get("Categories", "").lower()
    return rows


def category_progress(rows):
    """(per-category rows, overall totals) - see PROGRESS_COLUMNS."""
    groups = {}
    for r in rows:
        shared = len(r["sources"]) > 1
        for src in r["sources"]:
            g = groups.setdefault(src, Counter())
            g["found"] += 1
            g[r["status"]] += 1
            g["shared"] += shared
            g["last"] = max(g["last"], r["first_seen"] or 0)
    out = [_progress_row(src, g) for src, g in groups.items()]
    out.sort(key=lambda x: (-x["found"], x["category"].lower()))

    total = Counter(r["status"] for r in rows)
    total["found"] = len(rows)
    total["shared"] = sum(1 for r in rows if len(r["sources"]) > 1)
    total["last"] = max((r["first_seen"] or 0 for r in rows), default=0)
    totals = _progress_row("ALL CATEGORIES (each contract counted once)", total)
    totals["category_sum"] = sum(x["found"] for x in out)
    return out, totals


def _progress_row(name, g):
    found = g["found"]
    read = g[STATUS_READ]
    return {
        "category": name, "found": found,
        "downloaded": read + g[STATUS_PDF] + g[STATUS_UNREADABLE], "read": read,
        "pending": g[STATUS_PDF], "unreadable": g[STATUS_UNREADABLE], "failed": g[STATUS_FAILED],
        "missing": g[STATUS_NO_PDF],  # downloaded + failed + missing = found
        "percent": round(read / found * 100, 1) if found else 0.0,
        "shared": g["shared"], "last_added": _fmt_time(g["last"]),
    }


def bar(percent, downloaded_percent=0.0, width=12):
    """Text progress bar: █ read into the database, ▒ downloaded but not read yet, ░ still to do."""
    read = int(round(percent / 100 * width))
    got = max(read, int(round(downloaded_percent / 100 * width)))
    return "█" * read + "▒" * (got - read) + "░" * (width - got) + f" {percent:5.1f}%"


def search(rows, text="", category="", status="", date_from=None, date_to=None, only=None):
    """Contracts matching every given filter. text: all words must appear somewhere in the row."""
    words = text.lower().split()
    out = []
    for r in rows:
        if only is not None and r["no"] not in only:
            continue
        if category and category not in r["sources"]:
            continue
        if status and r["status"] != status:
            continue
        if date_from and (not r["date_obj"] or r["date_obj"] < date_from):
            continue
        if date_to and (not r["date_obj"] or r["date_obj"] > date_to):
            continue
        if words and not all(w in r["hay"] for w in words):
            continue
        out.append(r)
    out.sort(key=lambda r: (r["date_obj"] or datetime.min.date(), r["no"]), reverse=True)
    return out
