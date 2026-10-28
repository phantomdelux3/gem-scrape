"""
Scrape engine: runs a search on gem.gov.in/view_contracts (same options as the website),
walks every result page, stores each result card in the database and downloads every PDF.

Optional "read PDFs while scraping": each downloaded PDF is parsed straight into the database
by worker processes, so the Excel export at the end is instant. When the readers fall behind,
downloading waits for them - which also spaces out requests and keeps GeM's rate limit calm.

Files live in the app's data folder:  <data>/pdfs/<YYYY-MM>/<contract no>.pdf  +  gem_data.db
Re-running the same search skips PDFs that are already downloaded (resume / retry failed).
"""

from concurrent.futures import ThreadPoolExecutor, wait
from dataclasses import asdict, dataclass, field
from datetime import date, datetime, timedelta
from pathlib import Path
import json
import os
import threading
import time

from .converter import ParsePool
from .gem_client import GemClient, RequestMonitor, StopRequested

MAX_PAGES_PER_QUERY = 20000


@dataclass
class ScrapeConfig:
    mode: str = "category"                 # category | ministry | number
    number: str = ""                       # Bid number or Contract number (mode=number)
    categories: list = field(default_factory=list)   # [(value, label)], empty = all categories
    buyer_entity: str = ""
    ministry: str = ""
    state: str = ""
    department: str = ""
    organization: str = ""
    from_date: date = None
    to_date: date = None
    window_days: int = 0                   # 0 = automatic
    delay_ms: int = 1000
    downloads: int = 2                     # parallel PDF downloads
    download_pdfs: bool = True
    read_while_scraping: bool = True       # parse PDFs into the database as they arrive
    read_workers: int = 2


ALL_CATEGORIES = "All categories (no category filter)"
NUMBER_SOURCE = "Bid / Contract No. search"


def fmt(d):
    return d.strftime("%d-%m-%Y")


def _ministry_label(cfg):
    return " / ".join(x for x in (cfg.buyer_entity, cfg.ministry, cfg.state, cfg.department, cfg.organization) if x)


def describe(cfg):
    """One-line description of a search, used in the log and the History tab."""
    if cfg.mode == "number":
        return f"Bid / Contract No. {cfg.number}"
    if cfg.mode == "category":
        labels = [label for _, label in cfg.categories]
        if not labels:
            what = "All categories"
        elif len(labels) == 1:
            what = labels[0]
        else:
            what = f"{len(labels)} categories: " + ", ".join(labels[:3]) + (" ..." if len(labels) > 3 else "")
    else:
        what = "Ministry: " + _ministry_label(cfg)
    return f"{what} | {fmt(cfg.from_date)} to {fmt(cfg.to_date)}"


def source_label(cfg, cat):
    """Category name a contract is filed under in the Database tab."""
    if cfg.mode == "category":
        return cat or ALL_CATEGORIES
    if cfg.mode == "ministry":
        return "Ministry: " + _ministry_label(cfg)
    return NUMBER_SOURCE


def config_to_dict(cfg):
    d = asdict(cfg)
    d["from_date"] = cfg.from_date.isoformat() if cfg.from_date else None
    d["to_date"] = cfg.to_date.isoformat() if cfg.to_date else None
    return d


def auto_window_days(cfg):
    if cfg.mode == "category":
        return 30 if cfg.categories else 1
    if cfg.mode == "ministry":
        return 30  # ministry/state searches are split per department, which keeps each search small
    return 1


def build_queries(cfg, departments=None):
    """List of (label, params, category label) to run, oldest window first.

    departments: for a Ministry/State-only search, run one search per department instead -
    GeM answers ministry-wide searches very slowly (minutes, often timing out) but
    department searches in seconds."""
    if cfg.mode == "number":
        num = cfg.number.strip()
        return [(num, {"fromDate": "", "toDate": "", "department": "", "bno": num, "buyer_category": ""}, "")]

    days = cfg.window_days or auto_window_days(cfg)
    windows = []
    start = cfg.from_date
    while start <= cfg.to_date:
        end = min(start + timedelta(days=days - 1), cfg.to_date)
        windows.append((start, end))
        start = end + timedelta(days=1)

    queries = []
    for s, e in windows:
        span = fmt(s) if s == e else f"{fmt(s)} to {fmt(e)}"
        if cfg.mode == "category":
            for value, label in (cfg.categories or [("", "All categories")]):
                queries.append((f"{span} | {label}", {
                    "fromDate": fmt(s), "toDate": fmt(e), "department": "", "bno": "", "buyer_category": value,
                }, label if value else ""))
        else:
            for dept in (departments or [cfg.department]):
                queries.append((f"{span} | {dept}" if departments else span, {
                    "buyer_entity": cfg.buyer_entity, "buyer_ministry": cfg.ministry, "buyer_state": cfg.state,
                    "fromDate": fmt(s), "toDate": fmt(e), "department": dept,
                    "organization": cfg.organization,
                }, ""))
    return queries


class ScrapeStats:
    FIELDS = ("queries_total", "queries_done", "pages", "found", "new_results", "downloaded", "skipped",
              "failed", "failed_queries", "bytes", "download_seconds", "read", "read_errors", "read_backlog",
              "query_pages", "query_found")   # query_*: progress inside the search that is running now
    LIVE_ONLY = ("read_backlog", "query_pages", "query_found")   # not kept in History

    def __init__(self):
        self.lock = threading.Lock()
        self.started = time.time()
        self.finished = None
        self.status = "Starting"
        self.current = ""
        self.paused_total = 0.0   # seconds spent paused - not counted in elapsed time / ETA
        self.paused_at = None
        for f in self.FIELDS:
            setattr(self, f, 0)

    def add(self, **kw):
        with self.lock:
            for k, v in kw.items():
                setattr(self, k, getattr(self, k) + v)

    def set(self, **kw):
        with self.lock:
            for k, v in kw.items():
                setattr(self, k, v)

    def snapshot(self):
        with self.lock:
            d = {k: v for k, v in self.__dict__.items() if k != "lock"}
        end = d["finished"] or time.time()
        paused = d["paused_total"] + (end - d["paused_at"] if d["paused_at"] else 0)
        d["elapsed"] = max(0.0, end - d["started"] - paused)
        return d

    def pause(self, on):
        with self.lock:
            if on and self.paused_at is None:
                self.paused_at = time.time()
            elif not on and self.paused_at is not None:
                self.paused_total += time.time() - self.paused_at
                self.paused_at = None


class ScrapeEngine:
    def __init__(self, cfg, db, data_dir, log=None, on_done=None):
        self.cfg = cfg
        self.db = db
        self.log = log or print
        self.on_done = on_done
        self.stop_event = threading.Event()
        self.pause_event = threading.Event()
        self.stats = ScrapeStats()
        self.monitor = RequestMonitor()
        self.client = GemClient(cfg.delay_ms, log=self.log, stop_event=self.stop_event,
                                pause_event=self.pause_event, pool_size=max(4, cfg.downloads + 2),
                                monitor=self.monitor)
        self.pdf_dir = Path(data_dir) / "pdfs"
        self.thread = None
        self.reader = None
        self.run_id = None
        self._reader_lock = threading.Lock()

    # ----- control ----------------------------------------------------------
    def start(self):
        self.thread = threading.Thread(target=self._run, daemon=True, name="scrape")
        self.thread.start()

    def is_running(self):
        # finished is set just before on_done is called, while the thread is still winding down - without this
        # check, the "export when finished" step could see the scrape as still running and refuse to start
        return self.thread is not None and self.thread.is_alive() and self.stats.finished is None

    def stop(self):
        self.stop_event.set()
        self.pause_event.clear()
        self.stats.pause(False)
        if self.is_running():
            self.stats.status = "Stopping"

    def toggle_pause(self):
        if self.stop_event.is_set() or not self.is_running():
            return self.pause_event.is_set()
        if self.pause_event.is_set():
            self.pause_event.clear()
            self.stats.pause(False)
            self.stats.status = "Running"
        else:
            self.pause_event.set()
            self.stats.pause(True)
            self.stats.status = "Paused"
        return self.pause_event.is_set()

    # ----- run --------------------------------------------------------------
    def _run(self):
        cfg = self.cfg
        try:
            self.run_id = self.db.start_run("Scrape", describe(cfg), config_to_dict(cfg))
            self.pdf_dir.mkdir(parents=True, exist_ok=True)
            self.have = self._sync_existing_pdfs()
            if cfg.download_pdfs and cfg.read_while_scraping:
                self.reader = ParsePool(self.db, cfg.read_workers)
            # keep "Paused" / "Stopping" if the user pressed a button during the PDF check
            if not (self.pause_event.is_set() or self.stop_event.is_set()):
                self.stats.status = "Running"
            self.log(f"{len(self.have):,} PDF(s) already downloaded - they will be skipped.")

            departments = None
            if cfg.mode == "ministry" and not (cfg.department or cfg.buyer_entity or cfg.organization):
                self.stats.current = "Loading department list"
                try:
                    departments = self.client.get_departments(cfg.ministry, cfg.state) or None
                except StopRequested:
                    raise
                except Exception as e:  # noqa: BLE001 - still searchable, just slower
                    self.log(f"Could not load the department list ({e}) - searching the whole ministry/state "
                             "in one go instead.")
                if departments:
                    self.log(f"{len(departments)} department(s) found - searching each one "
                             f"(much faster than one ministry-wide search).")
            queries = build_queries(cfg, departments)
            self.stats.set(queries_total=len(queries))
            if cfg.mode != "number":
                self.log(f"{len(queries):,} search(es) to run ({cfg.window_days or auto_window_days(cfg)}-day windows).")

            self.client.ensure_session()
            with ThreadPoolExecutor(max_workers=max(1, cfg.downloads), thread_name_prefix="dl") as pool:
                self.pool = pool
                for label, params, cat in queries:
                    if self.stop_event.is_set():
                        break
                    self.stats.current = label
                    self.stats.set(query_pages=0, query_found=0)
                    try:
                        self._run_query(label, params, cat)
                    except StopRequested:
                        raise
                    except Exception as e:  # noqa: BLE001 - record and continue with the next window
                        self.stats.add(failed_queries=1)
                        self.log(f"x Search '{label}' failed: {e}")
                        self.db.add_failure("SEARCH", label, f"{e} | {json.dumps(params)}")
                    self.stats.add(queries_done=1)
            self._finish_reading()
            self.stats.status = "Stopped" if self.stop_event.is_set() else "Finished"
        except StopRequested:
            self.stats.status = "Stopped"
            self._finish_reading()
        except Exception as e:  # noqa: BLE001
            self.stats.status = "Error"
            self.log(f"ERROR: {type(e).__name__}: {e}")
        finally:
            if self.reader:
                self.reader.close()
            self.stats.pause(False)
            self.stats.finished = time.time()
            if self.run_id:
                s = self.stats.snapshot()
                try:
                    self.db.finish_run(self.run_id, self.stats.status,
                                       {k: s[k] for k in ScrapeStats.FIELDS + ("elapsed",)
                                        if k not in ScrapeStats.LIVE_ONLY})
                except Exception as e:  # noqa: BLE001 - history is optional, never fail the scrape on it
                    self.log(f"Could not save this run to History: {e}")
            self.log(f"Scrape {self.stats.status.lower()}.")
            if self.on_done:
                self.on_done(self)

    def _run_query(self, label, params, cat):
        page = 0
        previous = None
        while page < MAX_PAGES_PER_QUERY:
            self.client.wait_if_paused()
            cards = self.client.search_page(params, page, f"{label} p{page}")
            if not cards:
                # GeM sometimes returns an empty or short page mid-stream: re-check this page and the next
                self.client.sleep(3)
                cards = self.client.search_page(params, page, f"{label} p{page}")
                if not cards:
                    nxt = self.client.search_page(params, page + 1, f"{label} p{page + 1}")
                    if not nxt:
                        if page == 0:
                            self.log(f"{label}: no contracts found")
                        break
                    page += 1
                    cards = nxt
            ids = [c["ContractNo"] for c in cards]
            if ids == previous:
                self.log(f"  ! {label}: page {page} repeated the previous page - moving on")
                break
            previous = ids
            new_results = self.db.add_listing(cards, cat)
            self.db.add_sources(ids, source_label(self.cfg, cat), self.run_id)
            found = len(set(ids))
            self.stats.add(pages=1, found=found, new_results=new_results, query_pages=1, query_found=found)

            # a contract listed twice on one page must be downloaded (and counted) once
            unique = list({c["ContractNo"]: c for c in cards}.values())
            new = [c for c in unique if c["ContractNo"] not in self.have]
            self.stats.add(skipped=len(unique) - len(new))
            if self.cfg.download_pdfs and new:
                futures = [self.pool.submit(self._download, c) for c in new]
                wait(futures)
                for f in futures:
                    if isinstance(f.exception(), StopRequested):
                        raise StopRequested()
            self._collect_reads()
            self.log(f"{label} | page {page + 1}: {len(cards)} contracts, {len(new)} new")
            # a bid can have more contracts than fit on one page, so number searches page on too;
            # the empty-page and repeated-page checks above end the loop
            page += 1

    def _download(self, card):
        cid = card["ContractNo"]
        self._wait_for_readers()
        t0 = time.time()
        try:
            data = self.client.download_pdf(cid)
        except StopRequested:
            raise
        except Exception as e:  # noqa: BLE001
            self.stats.add(failed=1)
            self.log(f"  x {cid}: {e}")
            self.db.add_failure("PDF", cid, e)
            return
        folder = self.pdf_dir / self._month_folder(card)
        path = folder / f"{cid}.pdf"
        tmp = folder / f"{cid}.pdf.part"
        try:
            folder.mkdir(parents=True, exist_ok=True)
            tmp.write_bytes(data)
            os.replace(tmp, path)
        except OSError as e:  # disk full, folder not writable, file locked by antivirus ...
            self.stats.add(failed=1)
            self.log(f"  x {cid}: could not save the PDF ({e})")
            self.db.add_failure("PDF", cid, f"could not save: {e}")
            return
        self.db.add_pdf(cid, path, len(data))
        self.have.add(cid)
        self.stats.add(downloaded=1, bytes=len(data), download_seconds=time.time() - t0)
        if self.reader:
            with self._reader_lock:
                self.reader.submit(path)

    # ----- reading PDFs while scraping --------------------------------------
    def _wait_for_readers(self):
        """Back-pressure: if PDF reading falls behind, hold downloads (also eases rate limiting)."""
        if not self.reader:
            return
        while True:
            with self._reader_lock:
                self.reader.collect()
                backlog = self.reader.backlog
            self.stats.set(read=self.reader.done - self.reader.errors, read_errors=self.reader.errors,
                           read_backlog=backlog)
            if backlog < self.reader.workers * 3:
                return
            self.client.sleep(0.2)

    def _collect_reads(self):
        if self.reader:
            with self._reader_lock:
                self.reader.collect()
                self.stats.set(read=self.reader.done - self.reader.errors, read_errors=self.reader.errors,
                               read_backlog=self.reader.backlog)

    def _finish_reading(self):
        if self.reader:
            self.stats.current = "Reading the last PDFs into the database"
            with self._reader_lock:
                self.reader.collect(wait=True)
            self._collect_reads()

    # ----- files ----------------------------------------------------------
    @staticmethod
    def _month_folder(card):
        try:
            return datetime.strptime(card.get("ContractDate", "").split()[0], "%d/%m/%Y").strftime("%Y-%m")
        except (ValueError, IndexError):
            return "unknown-date"

    def _sync_existing_pdfs(self):
        """IDs already downloaded: PDFs the database knows that are still on disk, plus any PDFs in the
        folder it doesn't know yet. PDFs deleted by hand are forgotten, so they are downloaded again."""
        known = self.db.pdf_paths()
        on_disk = {}
        stack = [str(self.pdf_dir)]
        while stack:
            d = stack.pop()
            try:
                with os.scandir(d) as it:
                    for e in it:
                        if e.is_dir(follow_symlinks=False):
                            stack.append(e.path)
                        elif e.name.lower().endswith(".pdf"):
                            size = e.stat().st_size
                            if size > 1000:
                                on_disk[e.name[:-4]] = (e.path, size)
            except OSError:
                pass
        have = set()
        gone, moved = [], []
        for cid, path in known.items():
            if os.path.exists(path):
                have.add(cid)
            elif cid in on_disk:  # file moved inside the PDF folder: remember the new place
                moved.append((cid, *on_disk[cid]))
            else:
                gone.append(cid)
        found = [(cid, path, size) for cid, (path, size) in on_disk.items() if cid not in known]
        if moved:
            self.db.add_pdfs(moved, replace=True)
            have.update(cid for cid, _, _ in moved)
        if gone:
            self.db.remove_pdfs(gone)
            self.log(f"{len(gone):,} PDF(s) in the database are no longer on disk - they will be downloaded again.")
        if found:
            self.db.add_pdfs(found)
            have.update(cid for cid, _, _ in found)
        return have
