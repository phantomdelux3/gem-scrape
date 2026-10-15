"""
Excel / CSV downloads from the Database and History tabs.

Contracts come in as db_report rows, which are already one row per contract number, so a
contract that several runs or categories found is written once per table - never twice.
"""

from pathlib import Path
import csv
import re

from . import converter, db_report
from .pdf_parser import PARSER_VERSION

INVALID_CHARS_RE = re.compile(r'[\\/:*?"<>|\[\]]+')


def safe_name(name, limit=80):
    """Text usable as a file or sheet name."""
    return INVALID_CHARS_RE.sub(" ", str(name)).strip(" .")[:limit].strip() or "data"


def _unique(names):
    """Make names unique, ignoring case (Excel sheet names and Windows file names)."""
    seen = set()
    out = []
    for name in names:
        cand, k = name, 2
        while cand.lower() in seen:
            cand = f"{name[:25]} ({k})"
            k += 1
        seen.add(cand.lower())
        out.append(cand)
    return out


def write_tables(base, tables, fmt):
    """Write tables as one .xlsx (one sheet each) or as CSV (one file each; several files go in a folder
    named like the base). base: path without extension. Returns the written files."""
    base = Path(base)
    names = [safe_name(t[0], 31 if fmt == "xlsx" else 80) for t in tables]
    if fmt == "xlsx":  # Excel reserves the sheet name "History"
        names = [f"{n} sheet" if n.lower() == "history" else n for n in names]
    names = _unique(names)
    tables = [(n, cols, rows) for n, (_, cols, rows) in zip(names, tables)]
    if fmt == "xlsx":
        return [converter.write_tables_xlsx(base.parent / f"{base.name}.xlsx", tables)]
    if len(tables) == 1:
        targets = [(base.parent / f"{base.name}.csv", tables[0])]
    else:
        base.mkdir(parents=True, exist_ok=True)
        targets = [(base / f"{name}.csv", t) for name, t in zip(names, tables)]
    for path, (_, cols, rows) in targets:
        try:
            with open(path, "w", newline="", encoding="utf-8-sig") as f:
                w = csv.writer(f)
                w.writerow([title for _, title, _ in cols])
                for row in rows:
                    w.writerow([converter._text_value(row.get(k), kind) for k, _, kind in cols])
        except PermissionError:
            raise PermissionError(f"{path.name} is open in another program - close it and try again.")
    return [p for p, _ in targets]


def filter_categories(rows, categories):
    """Contracts found under at least one of the categories (None = all)."""
    if not categories:
        return rows
    wanted = set(categories)
    return [r for r in rows if wanted.intersection(r["sources"])]


def download_contracts(db, rows, base, fmt="xlsx", full=False, split=False, categories=None, selection=None):
    """Write contracts to Excel/CSV.

    full:  also every detail sheet (Contracts, Items, Consignees ...) using `selection` (Convert tab choices)
    split: per category - list mode adds one sheet/file per category; full mode writes one file per category
    categories: only these categories (and only these groups when split)."""
    rows = filter_categories(rows, categories)
    base = Path(base)
    groups = []
    if split:
        names = categories or sorted({s for r in rows for s in r["sources"]}, key=str.lower)
        groups = [(c, [r for r in rows if c in r["sources"]]) for c in names]
        groups = [(c, g) for c, g in groups if g]

    if not full:
        tables = [("All contracts" if split else "Contracts", db_report.SEARCH_COLUMNS, rows)]
        if split:
            cats, totals = db_report.category_progress(rows)
            cats = [c for c in cats if c["category"] in {g for g, _ in groups}]
            tables.insert(0, ("Category Summary", db_report.PROGRESS_COLUMNS, [totals] + cats))
            tables += [(c, db_report.SEARCH_COLUMNS, g) for c, g in groups]
        return write_tables(base, tables, fmt)

    selection = {k: v for k, v in (selection or converter.default_selection()).items() if v}
    wanted = {r["no"] for r in rows}
    listing = {no: card for no, card in db.listing().items() if no in wanted}
    parsed = {}
    for no, data in db.iter_parsed_contracts(wanted, PARSER_VERSION):
        # same contract number as the search results, so status / buying mode line up
        data["contract"]["ContractNo"] = no
        parsed[no] = data

    def detail(group_rows, first_sheet):
        nos = {r["no"] for r in group_rows}
        sheets = converter.build_rows(sorted((parsed[n] for n in nos if n in parsed),
                                             key=lambda d: d["contract"].get("ContractNo") or ""),
                                      {n: c for n, c in listing.items() if n in nos})
        return [(first_sheet, db_report.SEARCH_COLUMNS, group_rows)] + [
            (s, converter._columns_for(s, selection), sheets.get(s, []))
            for s in converter.SHEET_ORDER if selection.get(s)]

    if not split:
        return write_tables(base, detail(rows, "Contract List"), fmt)
    # one file (Excel) or folder of CSVs per category, plus one for everything, inside a folder
    base.mkdir(parents=True, exist_ok=True)
    files = []
    group_names = _unique(["All contracts"] + [safe_name(c) for c, _ in groups])
    for name, (_, g) in zip(group_names, [("All contracts", rows)] + groups):
        files += write_tables(base / name, detail(g, "Contract List"), fmt)
    return files


HISTORY_COLUMNS = [("id", "#", "number"), ("started", "Started", "text"), ("kind", "Type", "text"),
                   ("title", "Search / Details", "text"), ("status", "Status", "text"),
                   ("duration", "Duration", "text"), ("found", "Found", "number"), ("new", "New", "number"),
                   ("downloaded", "PDFs Downloaded", "number"), ("skipped", "Already Had", "number"),
                   ("failed", "Failed", "number"), ("read", "Read", "number"),
                   ("contracts", "Unique Contracts", "number"), ("categories", "Categories", "text"),
                   ("date_range", "Contract Dates Searched", "text"), ("file", "Saved File(s)", "text")]
