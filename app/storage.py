"""
SQLite storage for everything the app keeps: search results, downloaded PDFs, parsed PDF
data (the parse cache), failures, settings and the website's search options.

One connection, shared by all threads behind a lock (WAL mode, so readers never block the
writer). Only the main process touches the database - PDF-reading worker processes just
return data.
"""

from pathlib import Path
import json
import sqlite3
import sys
import threading
import time
import zlib

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS listing (
    contract_no     TEXT PRIMARY KEY,
    contract_date   TEXT,
    search_category TEXT,
    data            TEXT NOT NULL,
    first_seen      REAL NOT NULL
);
CREATE TABLE IF NOT EXISTS pdfs (
    contract_no   TEXT PRIMARY KEY,
    path          TEXT NOT NULL,
    size          INTEGER,
    downloaded_at REAL
);
CREATE TABLE IF NOT EXISTS parsed (
    path      TEXT PRIMARY KEY,
    sig       TEXT NOT NULL,
    version   INTEGER NOT NULL,
    data      BLOB,
    error     TEXT,
    parsed_at REAL NOT NULL
);
CREATE TABLE IF NOT EXISTS failures (
    id     INTEGER PRIMARY KEY AUTOINCREMENT,
    at     REAL NOT NULL,
    kind   TEXT NOT NULL,
    ref    TEXT,
    detail TEXT
);
CREATE TABLE IF NOT EXISTS kv (
    key   TEXT PRIMARY KEY,
    value TEXT,
    at    REAL
);
-- which category / search found each contract: one row per (contract, category), so a contract
-- found under two categories shows in both but is still one contract in the totals
CREATE TABLE IF NOT EXISTS contract_sources (
    contract_no TEXT NOT NULL,
    source      TEXT NOT NULL,
    first_seen  REAL NOT NULL,
    PRIMARY KEY (contract_no, source)
) WITHOUT ROWID;
CREATE INDEX IF NOT EXISTS contract_sources_source ON contract_sources(source);
-- history: every scrape / convert / Excel download
CREATE TABLE IF NOT EXISTS runs (
    id       INTEGER PRIMARY KEY AUTOINCREMENT,
    kind     TEXT NOT NULL,
    started  REAL NOT NULL,
    finished REAL,
    status   TEXT,
    title    TEXT,
    config   TEXT,
    stats    TEXT
);
CREATE TABLE IF NOT EXISTS run_contracts (
    run_id      INTEGER NOT NULL,
    contract_no TEXT NOT NULL,
    PRIMARY KEY (run_id, contract_no)
) WITHOUT ROWID;
"""

# category label for search results saved before categories were tracked per contract
OTHER_SOURCE = "Other searches (no category)"
# category label for PDFs that are in the database but not in any saved search result
PDF_ONLY_SOURCE = "PDFs added without a search"
# bump when _parsed_info() changes how contract numbers are matched, to re-index once
PARSED_INDEX_VERSION = 2
# small searchable summary kept next to each parsed PDF (the full data is compressed)
INFO_KEYS = ("GeneratedDate", "BidNo", "TotalOrderValue", "Org_Ministry", "Org_Department", "Org_Name",
             "Buyer_Designation", "Buyer_Email", "Seller_Company", "Seller_Email", "Seller_Contact")


def app_home():
    """Folder of the installed app (next to the .exe), or the project folder when run from source."""
    if getattr(sys, "frozen", False):
        return Path(sys.executable).resolve().parent
    return Path(__file__).resolve().parent.parent


def data_dir():
    """<install folder>/GeM Data - falls back to %LOCALAPPDATA% only if the install folder is read-only."""
    preferred = app_home() / "GeM Data"
    try:
        preferred.mkdir(parents=True, exist_ok=True)
        probe = preferred / ".write_test"
        probe.write_text("ok")
        probe.unlink()
        return preferred
    except OSError:
        import os
        fallback = Path(os.environ.get("LOCALAPPDATA", Path.home())) / "GeM Scraper" / "GeM Data"
        fallback.mkdir(parents=True, exist_ok=True)
        return fallback


def _pack(obj):
    return zlib.compress(json.dumps(obj, ensure_ascii=False, default=str).encode("utf-8"), 6)


def _unpack(blob):
    return json.loads(zlib.decompress(blob).decode("utf-8"))


def _parsed_info(path, data):
    """(contract no, searchable summary JSON) for one parsed PDF.

    Downloaded PDFs are named after the website's contract ID (GEMC-...), the same ID the search results
    and the pdfs table use, so that name wins - the number printed inside a PDF may be written differently."""
    stem = Path(path).stem
    if not data:
        return stem, None
    c = data.get("contract") or {}
    items = data.get("items") or []
    info = {k: c.get(k) for k in INFO_KEYS}
    info["Products"] = " | ".join(dict.fromkeys(str(i["ProductName"]) for i in items if i.get("ProductName")))[:1000]
    info["Categories"] = " | ".join(dict.fromkeys(str(i["Category"]) for i in items if i.get("Category")))[:1000]
    contract_no = stem if stem.upper().startswith("GEMC-") else (c.get("ContractNo") or stem)
    return contract_no, json.dumps(info, ensure_ascii=False, default=str)


class Storage:
    def __init__(self, path):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.RLock()
        self._conn = sqlite3.connect(str(self.path), check_same_thread=False, timeout=30)
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA synchronous=NORMAL")
        self._conn.execute("PRAGMA temp_store=MEMORY")
        self._conn.executescript(SCHEMA_SQL)
        self._migrate()
        self._conn.commit()

    def _migrate(self):
        cols = {r[1] for r in self._conn.execute("PRAGMA table_info(parsed)")}
        if "contract_no" not in cols:
            self._conn.execute("ALTER TABLE parsed ADD COLUMN contract_no TEXT")
        if "info" not in cols:
            self._conn.execute("ALTER TABLE parsed ADD COLUMN info TEXT")
        self._conn.execute("CREATE INDEX IF NOT EXISTS parsed_contract ON parsed(contract_no)")
        # index built with older matching rules: clear it, backfill_parsed_index() rebuilds it once
        row = self._conn.execute("SELECT value FROM kv WHERE key='parsed_index_version'").fetchone()
        if row is None or json.loads(row[0]) < PARSED_INDEX_VERSION:
            self._conn.execute("UPDATE parsed SET contract_no=NULL")
            self._conn.execute("INSERT OR REPLACE INTO kv(key, value, at) VALUES ('parsed_index_version', ?, ?)",
                               (json.dumps(PARSED_INDEX_VERSION), time.time()))
        # search results from older versions: use the one category that was saved with them
        self._conn.execute(
            "INSERT OR IGNORE INTO contract_sources(contract_no, source, first_seen) "
            "SELECT contract_no, COALESCE(NULLIF(search_category, ''), ?), first_seen FROM listing "
            "WHERE contract_no NOT IN (SELECT contract_no FROM contract_sources)", (OTHER_SOURCE,))

    def close(self):
        with self._lock:
            self._conn.close()

    def _exec(self, sql, params=(), many=False):
        with self._lock:
            cur = self._conn.executemany(sql, params) if many else self._conn.execute(sql, params)
            self._conn.commit()
            return cur

    def _query(self, sql, params=()):
        with self._lock:
            return self._conn.execute(sql, params).fetchall()

    # ----- search results ---------------------------------------------------
    def add_listing(self, cards, category=None):
        """Insert new result cards; returns how many were new."""
        if not cards:
            return 0
        now = time.time()
        rows = [(c["ContractNo"], c.get("ContractDate"), category or None,
                 json.dumps(c, ensure_ascii=False), now) for c in cards]
        ids = [r[0] for r in rows]
        with self._lock:
            known = {r[0] for r in self._conn.execute(
                f"SELECT contract_no FROM listing WHERE contract_no IN ({','.join('?' * len(ids))})", ids)}
            # existing rows get the latest card (e.g. a changed contract status), new rows are inserted
            self._conn.executemany(
                "INSERT INTO listing(contract_no, contract_date, search_category, data, first_seen) "
                "VALUES (?,?,?,?,?) ON CONFLICT(contract_no) DO UPDATE SET data=excluded.data", rows)
            self._conn.commit()
            return len(set(ids) - known)

    def listing(self):
        out = {}
        for no, cat, data in self._query("SELECT contract_no, search_category, data FROM listing"):
            card = json.loads(data)
            card["SearchCategory"] = cat
            out[no] = card
        return out

    def listing_count(self):
        return self._query("SELECT COUNT(*) FROM listing")[0][0]

    def add_sources(self, contract_nos, source, run_id=None):
        """Remember which category/search found these contracts (and in which run)."""
        now = time.time()
        with self._lock:
            self._conn.executemany("INSERT OR IGNORE INTO contract_sources(contract_no, source, first_seen) "
                                   "VALUES (?,?,?)", [(no, source, now) for no in contract_nos])
            if run_id:
                self._conn.executemany("INSERT OR IGNORE INTO run_contracts(run_id, contract_no) VALUES (?,?)",
                                       [(run_id, no) for no in contract_nos])
            self._conn.commit()

    # ----- downloaded PDFs --------------------------------------------------
    def add_pdf(self, contract_no, path, size):
        self._exec("INSERT OR REPLACE INTO pdfs(contract_no, path, size, downloaded_at) VALUES (?,?,?,?)",
                   (contract_no, str(path), size, time.time()))

    def add_pdfs(self, rows):
        self._exec("INSERT OR IGNORE INTO pdfs(contract_no, path, size, downloaded_at) VALUES (?,?,?,?)",
                   [(no, str(p), size, time.time()) for no, p, size in rows], many=True)

    def downloaded_ids(self):
        return {r[0] for r in self._query("SELECT contract_no FROM pdfs")}

    def pdf_count(self):
        return self._query("SELECT COUNT(*) FROM pdfs")[0][0]

    # ----- parsed PDF data (parse cache) ------------------------------------
    def parsed_sigs(self, version):
        return {p: s for p, s in self._query("SELECT path, sig FROM parsed WHERE version=?", (version,))}

    def save_parsed(self, rows, version):
        """rows: [(path, sig, data or None, error or None)]"""
        now = time.time()
        self._exec("INSERT OR REPLACE INTO parsed(path, sig, version, data, error, parsed_at, contract_no, info) "
                   "VALUES (?,?,?,?,?,?,?,?)",
                   [(p, s, version, _pack(d) if d is not None else None, e, now, *_parsed_info(p, d))
                    for p, s, d, e in rows], many=True)

    def backfill_parsed_index(self, batch=300):
        """Fill contract_no/info for PDFs parsed by older versions of the app (runs once)."""
        while True:
            rows = self._query("SELECT path, data FROM parsed WHERE contract_no IS NULL LIMIT ?", (batch,))
            if not rows:
                return
            updates = []
            for path, blob in rows:
                try:
                    data = _unpack(blob) if blob is not None else None
                except (zlib.error, ValueError):
                    data = None
                updates.append((*_parsed_info(path, data), path))
            self._exec("UPDATE parsed SET contract_no=?, info=? WHERE path=?", updates, many=True)

    def iter_parsed_contracts(self, contract_nos, version):
        """Yield (contract no, parsed data) once per wanted contract, even if its PDF was read from two paths."""
        wanted = set(contract_nos)
        # pick the newest parse of each wanted contract first, then load only those blobs, a batch at a time
        latest = {}
        for no, path in self._query("SELECT contract_no, path FROM parsed WHERE version=? AND data IS NOT NULL "
                                    "ORDER BY parsed_at", (version,)):
            if no in wanted:
                latest[no] = path
        by_path = {path: no for no, path in latest.items()}
        paths = list(by_path)
        for i in range(0, len(paths), 500):
            chunk = paths[i:i + 500]
            rows = self._query(f"SELECT path, data FROM parsed WHERE path IN ({','.join('?' * len(chunk))})", chunk)
            for path, blob in rows:
                yield by_path[path], _unpack(blob)

    # ----- everything known about each contract (Database tab) ---------------
    def contract_rows(self, version):
        """One dict per unique contract number: search card, categories, PDF and read status."""
        with self._lock:
            listing = self._conn.execute("SELECT contract_no, data, first_seen FROM listing").fetchall()
            sources = self._conn.execute("SELECT contract_no, source FROM contract_sources").fetchall()
            pdfs = self._conn.execute("SELECT contract_no, path FROM pdfs").fetchall()
            parsed = self._conn.execute("SELECT contract_no, info, error FROM parsed "
                                        "WHERE version=? AND contract_no IS NOT NULL", (version,)).fetchall()
            failed = {r[0] for r in self._conn.execute("SELECT DISTINCT ref FROM failures WHERE kind='PDF'")}
        out = {}

        def row(no):
            r = out.get(no)
            if r is None:
                r = out[no] = {"no": no, "card": {}, "first_seen": None, "sources": [], "pdf": None,
                               "info": None, "read_error": None}
            return r
        for no, data, seen in listing:
            r = row(no)
            r["card"] = json.loads(data)
            r["first_seen"] = seen
        for no, source in sources:
            row(no)["sources"].append(source)
        for no, path in pdfs:
            row(no)["pdf"] = path
        for no, info, err in parsed:
            r = row(no)
            if info:
                r["info"] = json.loads(info)
            elif err and not r["info"]:
                r["read_error"] = err
        for r in out.values():
            r["download_failed"] = r["no"] in failed and not r["pdf"]
            if not r["sources"]:
                r["sources"] = [PDF_ONLY_SOURCE]
        return list(out.values())

    def iter_parsed(self, paths, version):
        """Yield (path, data, error) for the given PDF paths, one row at a time (low memory)."""
        wanted = set(paths)
        found = [p for (p,) in self._query("SELECT path FROM parsed WHERE version=?", (version,)) if p in wanted]
        for i in range(0, len(found), 500):  # a batch at a time, not every blob in memory at once
            chunk = found[i:i + 500]
            rows = self._query(f"SELECT path, data, error FROM parsed WHERE path IN ({','.join('?' * len(chunk))})",
                               chunk)
            for path, blob, err in rows:
                yield path, (_unpack(blob) if blob is not None else None), err

    def parsed_count(self, version):
        return self._query("SELECT COUNT(*) FROM parsed WHERE version=? AND data IS NOT NULL", (version,))[0][0]

    # ----- failures ---------------------------------------------------------
    def add_failure(self, kind, ref, detail):
        self._exec("INSERT INTO failures(at, kind, ref, detail) VALUES (?,?,?,?)",
                   (time.time(), kind, ref, str(detail)[:2000]))

    def failures(self, limit=2000):
        return self._query("SELECT at, kind, ref, detail FROM failures ORDER BY id DESC LIMIT ?", (limit,))

    def clear_failures(self):
        self._exec("DELETE FROM failures")

    # ----- history -----------------------------------------------------------
    def start_run(self, kind, title, config=None):
        cur = self._exec("INSERT INTO runs(kind, started, status, title, config) VALUES (?,?,?,?,?)",
                         (kind, time.time(), "Running", title, json.dumps(config or {}, default=str)))
        return cur.lastrowid

    def finish_run(self, run_id, status, stats=None):
        self._exec("UPDATE runs SET finished=?, status=?, stats=? WHERE id=?",
                   (time.time(), status, json.dumps(stats or {}, default=str), run_id))

    def add_run(self, kind, title, status, stats=None, config=None, started=None):
        """Record an already finished job (convert, Excel download)."""
        now = time.time()
        self._exec("INSERT INTO runs(kind, started, finished, status, title, config, stats) VALUES (?,?,?,?,?,?,?)",
                   (kind, started or now, now, status, title, json.dumps(config or {}, default=str),
                    json.dumps(stats or {}, default=str)))

    def close_stale_runs(self):
        """Runs still marked 'Running' from an app session that was closed or crashed."""
        self._exec("UPDATE runs SET status='Interrupted', finished=COALESCE(finished, started) "
                   "WHERE status='Running'")

    def runs(self, limit=1000):
        rows = self._query(
            "SELECT r.id, r.kind, r.started, r.finished, r.status, r.title, r.config, r.stats, "
            "(SELECT COUNT(*) FROM run_contracts rc WHERE rc.run_id = r.id) "
            "FROM runs r ORDER BY r.id DESC LIMIT ?", (limit,))
        return [{"id": i, "kind": k, "started": s, "finished": f, "status": st, "title": t,
                 "config": json.loads(c or "{}"), "stats": json.loads(x or "{}"), "contracts": n}
                for i, k, s, f, st, t, c, x, n in rows]

    def run_contract_ids(self, run_id):
        return {r[0] for r in self._query("SELECT contract_no FROM run_contracts WHERE run_id=?", (run_id,))}

    def delete_runs(self, run_ids):
        with self._lock:
            for rid in run_ids:
                self._conn.execute("DELETE FROM run_contracts WHERE run_id=?", (rid,))
                self._conn.execute("DELETE FROM runs WHERE id=?", (rid,))
            self._conn.commit()

    # ----- key/value (settings, search options) -----------------------------
    def get(self, key, default=None):
        rows = self._query("SELECT value, at FROM kv WHERE key=?", (key,))
        return (json.loads(rows[0][0]), rows[0][1]) if rows else (default, None)

    def put(self, key, value):
        self._exec("INSERT OR REPLACE INTO kv(key, value, at) VALUES (?,?,?)",
                   (key, json.dumps(value, ensure_ascii=False), time.time()))
