"""
GeM Contract Scraper - desktop app (Tkinter).

Tab 1 "Scrape":  same search options as https://gem.gov.in/view_contracts
                 (Bid/Category, Ministry/Organization, Bid or Contract number), live
                 category/ministry/state lists, automatic cookies, PDF download with live stats.
Tab 2 "Convert": PDFs -> Excel / CSV / JSON, pick sheets and columns (all selected by default).
Tab 3 "Database": progress per category (found / downloaded / read into the database), search
                 everything in the database and download the matches to Excel.
Tab 4 "History": every scrape, convert and Excel download, with "run this search again".
"""

from datetime import date, datetime, timedelta
from pathlib import Path
import os
import queue
import subprocess
import sys
import threading
import time
import tkinter as tk
from tkinter import filedialog, font as tkfont, messagebox, ttk
from tkinter.scrolledtext import ScrolledText

from . import converter, db_report, downloads
from .developer_banner import DeveloperBanner
from .gem_client import GemClient
from .pdf_parser import PARSER_VERSION
from .scrape_engine import ALL_CATEGORIES, NUMBER_SOURCE, ScrapeConfig, ScrapeEngine, describe, source_label
from .storage import Storage, data_dir

APP_NAME = "GeM Contract Scraper"
APP_VERSION = "3.1"
DEVELOPER_NAME = "deadpool1555"
SUPPORT_EMAIL = "phantomdeluxe.0555@gmail.com"
OPTIONS_MAX_AGE = 24 * 3600
LOG_MAX_LINES = 3000
DB_SHOW_MAX = 3000          # search results shown in the Database tab (Excel gets all of them)
DB_LIVE_REFRESH = 15        # seconds between Database tab refreshes while scraping
ANY_CATEGORY = "(any category)"
ANY_STATUS = "(any status)"
HISTORY_ANY = "(any)"

# Windows display scaling (1.0 = 100 %, 1.5 = 150 % ...), set when the window opens. Fonts scale by
# themselves; fixed pixel sizes (window, table columns, charts) go through px().
UI_SCALE = 1.0


def px(n):
    return int(round(n * UI_SCALE))

# Help tab: (heading, items). "Term: text" shows the term in bold; "NOTE text" is a grey note.
HELP_SECTIONS = [
    ("Quick start", [
        "Open 1. Scrape contracts and pick the categories you want (or a Ministry, or one Bid / Contract No.).",
        "Choose the contract dates, or click a quick range such as Last 7 days.",
        "Press Start scraping. PDFs download and are read into the database as they arrive.",
        "Watch progress per category in 3. Database & search.",
        "Download what you need to Excel from 3. Database & search, or export everything from 2. Convert PDFs.",
    ]),
    ("1. Scrape contracts", [
        "Bid / Category: pick one or more categories (type to find them). No category selected = every contract.",
        "Ministry / Organization: choose a Ministry and/or State; Department and Organization load by themselves. "
        "Without a Department, each department is searched one by one (much faster on GeM).",
        "Bid or Contract No.: fetch one bid's contracts or a single contract. Dates are not used.",
        "Scrape + add to Excel: reads each PDF into the database while scraping, so the Excel export is instant. "
        "It also spaces out requests, which keeps GeM calm.",
        "Resume: run the same search again - PDFs you already have are skipped, failed ones are retried.",
        "Throttling bar: one bar per request. Red = HTTP 500/429 (GeM is throttling), purple = timeout, "
        "amber = slow. The pace slows down after errors and speeds back up by itself.",
        "Pause / Stop: Stop finishes the current request first, nothing is lost.",
    ]),
    ("2. Convert PDFs", [
        "Reads every PDF (current and older layouts, Bid/RA, services, multi-page tables) and writes Excel, "
        "CSV and/or JSON. All text is English only.",
        "Sheets and columns: everything is selected by default. Click a sheet name to choose its columns. "
        "Your choice is remembered.",
        "Cache: PDFs already read are not read again, so converting after new downloads is quick. Tick "
        "'Re-read all PDFs' only after an app update that improves reading.",
        "CPU workers: lower it on slower PCs to keep the computer responsive.",
        "Sheets: Contracts, Items, Consignees, Specifications, Seller Summary, Buyer Summary and Search Results "
        "(filled from the search pages, even without PDFs).",
    ]),
    ("3. Database & search", [
        "Totals: unique contracts, PDFs downloaded, read into the database, waiting to be read, unreadable, "
        "failed and not downloaded yet.",
        "Progress by category: found / downloaded / read for each category with a progress bar. It updates live "
        "while scraping. Click a column heading to sort, double-click a category to list its contracts.",
        "No double counting: each contract is stored once by its contract number. A contract found under two "
        "categories is listed under both, but counted once in the totals and in every Excel file.",
        "Search: any words (contract or bid no., ministry, organisation, product, seller, buyer email), plus "
        "category, status and contract date. Double-click a result to open its PDF.",
        "Download (Excel / CSV): the current search results or the whole database - see Downloads below.",
    ]),
    ("4. History", [
        "Every scrape, convert and download is listed with its date, duration and results.",
        "Filters: type, status, category, run date (with Today / 7 days / 30 days / This month), only runs that "
        "added new contracts, and free-text search.",
        "Select several: Ctrl-click or Shift-click entries, 'Select all shown' or Ctrl+A.",
        "Download contracts: from the selected runs or from all runs shown. A contract found by several runs is "
        "written once.",
        "Download history list: the selected entries, the entries shown, or the whole history.",
        "Show / download its contracts: lists one run's contracts in 3. Database & search.",
        "Run this search again: loads the search back into 1. Scrape contracts to repeat or resume it.",
        "Deleting a history entry never deletes contracts, PDFs or downloaded files.",
    ]),
    ("Downloads (Excel / CSV)", [
        "One row per contract: contract no., date, categories, database status, buyer, seller, value, PDF file.",
        "Full details: every sheet (Contracts, Items, Consignees, Specifications, Summaries) using the sheets "
        "and columns chosen in 2. Convert PDFs.",
        "Categories: download all categories or tick only the ones you want (type to find them).",
        "Split by category: an Excel list gets a Category Summary sheet, an All contracts sheet and one sheet per "
        "category. Full details or CSV get one file per category, all in one folder.",
        "Format: Excel (.xlsx) or CSV (.csv, opens in Excel; one file per table).",
        "No double counting: every table has each contract once. With split, a contract in two categories "
        "appears in both category sheets - and once in All contracts.",
        "Every download is recorded in 4. History, where 'Open file' opens it again.",
    ]),
    ("Where your data is saved", [
        "Everything is kept next to the app in the 'GeM Data' folder (or in %LOCALAPPDATA%\\GeM Scraper if the "
        "app folder is read-only).",
        "pdfs\\<year-month>\\: the downloaded contract PDFs.",
        "gem_data.db: the database (search results, read PDF data, history, settings).",
        "exports\\: Excel / CSV / JSON files.",
        "NOTE Back up the whole 'GeM Data' folder to keep everything. Do not open gem_data.db while the app runs.",
    ]),
    ("Troubleshooting", [
        "Lots of red bars: GeM is throttling. Let the app slow down by itself, or raise the base delay and lower "
        "parallel downloads.",
        "'is open in Excel': close the file in Excel and save again.",
        "Categories list is empty: check your internet connection and press 'Refresh categories / ministries'.",
        "Some PDFs failed: open 'Failures ...' to see why, then run the same search again to retry them.",
        "PDF could not be read: the file may be damaged. Delete it from the pdfs folder and scrape again.",
        "Still stuck? Email support (see the right) with a screenshot of the error.",
    ]),
]
BAR_COLORS = {"ok": "#2e9d4f", "slow": "#e0a100", "throttled": "#d93025", "error": "#8e44ad"}


def fmt_duration(sec):
    if sec is None:
        return "-"
    sec = int(max(0, sec))
    h, rem = divmod(sec, 3600)
    m, s = divmod(rem, 60)
    return f"{h}h {m:02d}m {s:02d}s" if h else (f"{m}m {s:02d}s" if m else f"{s}s")


def parse_dmy(s):
    try:
        return datetime.strptime(s.strip(), "%d-%m-%Y").date()
    except ValueError:
        return None


def _sort_key(v):
    """Sort table cells as numbers, then dates, then text."""
    s = v.replace(",", "").replace("%", "").replace("█", "").replace("▒", "").replace("░", "").strip()
    try:
        return (0, float(s), "")
    except ValueError:
        pass
    for f in ("%d-%m-%Y %H:%M", "%d-%m-%Y"):
        try:
            return (0, datetime.strptime(v, f).timestamp(), "")
        except ValueError:
            pass
    return (1, 0, v.lower())


def open_path(path):
    try:
        os.startfile(str(path))  # noqa: S606 - Windows only, opens folder/file in Explorer
    except AttributeError:
        subprocess.Popen(["xdg-open", str(path)])
    except OSError as e:
        messagebox.showerror(APP_NAME, f"Cannot open {path}:\n{e}")


def keep_split(panes, fraction):
    """Keep a two-pane Panedwindow's divider at `fraction` of its size whenever the window is resized
    (the panes are laid out before the window has its final size, which can leave one pane squashed),
    until the user drags the divider themselves."""
    state = {"user": False}

    def place(event=None):
        if state["user"]:
            return
        size = panes.winfo_height() if str(panes.cget("orient")) == "vertical" else panes.winfo_width()
        if size > 1:
            panes.sashpos(0, int(size * fraction))

    panes.bind("<Configure>", place, add="+")
    panes.bind("<ButtonRelease-1>", lambda e: state.update(user=True), add="+")


class TabScroller(ttk.Frame):
    """A tab page whose content fills the tab like a normal frame, but gets scrollbars when the window is
    smaller than the content (small screens, high Windows scaling). Build the page inside .inner."""

    def __init__(self, parent, padding=6, min_width=880):
        super().__init__(parent)
        self.min_width = min_width
        self.canvas = tk.Canvas(self, highlightthickness=0, borderwidth=0)
        self.vbar = ttk.Scrollbar(self, orient="vertical", command=self.canvas.yview)
        self.hbar = ttk.Scrollbar(self, orient="horizontal", command=self.canvas.xview)
        self.canvas.configure(yscrollcommand=self.vbar.set, xscrollcommand=self.hbar.set)
        self.inner = ttk.Frame(self.canvas, padding=padding)
        self._win = self.canvas.create_window((0, 0), window=self.inner, anchor="nw")
        self.canvas.grid(row=0, column=0, sticky="nsew")
        self.rowconfigure(0, weight=1)
        self.columnconfigure(0, weight=1)
        self._last = None
        self.canvas.bind("<Configure>", lambda e: self.relayout())

    def relayout(self):
        """Stretch the content to the visible area, or past it (with scrollbars) when the window is smaller
        than the page needs. Cheap when nothing changed, so it can be called often."""
        cw, ch = self.canvas.winfo_width(), self.canvas.winfo_height()
        # wide tables shrink (they scroll sideways themselves) down to min_width (at 100 % scaling);
        # None = the page always gets its full width. Heights are never squashed: the page scrolls.
        rw = self.inner.winfo_reqwidth()
        if self.min_width is not None:
            rw = min(rw, px(self.min_width))
        rh = self.inner.winfo_reqheight()
        if cw <= 1 or (cw, ch, rw, rh) == self._last:
            return
        self._last = (cw, ch, rw, rh)
        w, h = max(cw, rw), max(ch, rh)
        self.canvas.itemconfigure(self._win, width=w, height=h)
        self.canvas.configure(scrollregion=(0, 0, w, h))
        for bar, needed, grid in ((self.vbar, rh > ch, {"row": 0, "column": 1, "sticky": "ns"}),
                                  (self.hbar, rw > cw, {"row": 1, "column": 0, "sticky": "ew"})):
            if needed and not bar.winfo_ismapped():
                bar.grid(**grid)
            elif not needed and bar.winfo_ismapped():
                bar.grid_remove()
        if rh <= ch:
            self.canvas.yview_moveto(0)
        if rw <= cw:
            self.canvas.xview_moveto(0)

    def can_scroll(self):
        return self.vbar.winfo_ismapped()


class ScrollFrame(ttk.Frame):
    """A vertically scrollable frame (for the column checkboxes)."""

    def __init__(self, parent, **kw):
        super().__init__(parent, **kw)
        self.canvas = tk.Canvas(self, highlightthickness=0, borderwidth=0)
        bar = ttk.Scrollbar(self, orient="vertical", command=self.canvas.yview)
        self.inner = ttk.Frame(self.canvas)
        self.inner.bind("<Configure>", lambda e: self.canvas.configure(scrollregion=self.canvas.bbox("all")))
        self._win = self.canvas.create_window((0, 0), window=self.inner, anchor="nw")
        self.canvas.bind("<Configure>", lambda e: self.canvas.itemconfigure(self._win, width=e.width))
        self.canvas.configure(yscrollcommand=bar.set)
        self.canvas.pack(side="left", fill="both", expand=True)
        bar.pack(side="right", fill="y")

    def can_scroll(self):
        return self.inner.winfo_reqheight() > self.canvas.winfo_height()


class DownloadDialog(tk.Toplevel):
    """Asks what to download. After it closes, .result is None (cancelled) or
    {"scope", "full", "categories" (list or None = all), "split", "fmt" ("xlsx" | "csv")}.

    scopes: [(key, label, enabled)]   categories: [(name, contract count)] or None"""

    def __init__(self, parent, title, scopes, categories=None, detail=True, note=""):
        super().__init__(parent)
        self.title(title)
        self.transient(parent)
        self.resizable(False, False)
        self.result = None
        self.categories = categories or []
        self.chosen = set()
        body = ttk.Frame(self, padding=12)
        body.pack(fill="both", expand=True)

        sec = ttk.LabelFrame(body, text="What to download", padding=8)
        sec.pack(fill="x")
        self.scope = tk.StringVar(value=next((k for k, _, ok in scopes if ok), scopes[0][0]))
        for key, label, enabled in scopes:
            ttk.Radiobutton(sec, text=label, value=key, variable=self.scope,
                            state="normal" if enabled else "disabled").pack(anchor="w", pady=1)
        if note:
            ttk.Label(sec, text=note, style="Hint.TLabel").pack(anchor="w", pady=(2, 0))

        self.full = tk.BooleanVar(value=False)
        if detail:
            sec = ttk.LabelFrame(body, text="Detail", padding=8)
            sec.pack(fill="x", pady=(8, 0))
            ttk.Radiobutton(sec, text="One row per contract (contract no., date, categories, status, buyer, "
                                      "seller, value ...)", value=False, variable=self.full).pack(anchor="w")
            ttk.Radiobutton(sec, text="Full details - every sheet (Contracts, Items, Consignees, Specifications, "
                                      "Summaries),\nusing the sheets and columns chosen in 2. Convert PDFs",
                            value=True, variable=self.full).pack(anchor="w", pady=(2, 0))

        self.cat_mode = tk.StringVar(value="all")
        self.split = tk.BooleanVar(value=False)
        if self.categories:
            sec = ttk.LabelFrame(body, text="Categories", padding=8)
            sec.pack(fill="x", pady=(8, 0))
            ttk.Radiobutton(sec, text=f"All categories ({len(self.categories):,})", value="all",
                            variable=self.cat_mode, command=self._toggle_cats).pack(anchor="w")
            ttk.Radiobutton(sec, text="Only the categories selected below", value="some", variable=self.cat_mode,
                            command=self._toggle_cats).pack(anchor="w")
            find = ttk.Frame(sec)
            find.pack(fill="x", pady=(4, 2))
            ttk.Label(find, text="Find").pack(side="left")
            self.cat_find = tk.StringVar()
            self.cat_find.trace_add("write", lambda *a: self._fill_cats())
            self.find_entry = ttk.Entry(find, textvariable=self.cat_find, width=30)
            self.find_entry.pack(side="left", padx=4, fill="x", expand=True)
            ttk.Button(find, text="All shown", width=9, command=lambda: self._pick_all(True)).pack(side="left")
            ttk.Button(find, text="None", width=6, command=lambda: self._pick_all(False)).pack(side="left", padx=2)
            lf = ttk.Frame(sec)
            lf.pack(fill="x")
            self.cat_list = tk.Listbox(lf, selectmode="multiple", height=8, width=70, activestyle="none",
                                       exportselection=False)
            self.cat_list.pack(side="left", fill="x", expand=True)
            sb = ttk.Scrollbar(lf, orient="vertical", command=self.cat_list.yview)
            sb.pack(side="right", fill="y")
            self.cat_list.configure(yscrollcommand=sb.set)
            self.cat_list.bind("<<ListboxSelect>>", self._on_pick)
            self.cat_info = tk.StringVar()
            ttk.Label(sec, textvariable=self.cat_info, style="Hint.TLabel").pack(anchor="w")
            ttk.Checkbutton(sec, text="Split by category - Excel list: one sheet per category;  full details or "
                                      "CSV: one file per category", variable=self.split).pack(anchor="w", pady=(4, 0))
            self._view = []
            self._fill_cats()
            self._toggle_cats()

        sec = ttk.LabelFrame(body, text="Format", padding=8)
        sec.pack(fill="x", pady=(8, 0))
        self.fmt = tk.StringVar(value="xlsx")
        ttk.Radiobutton(sec, text="Excel (.xlsx)", value="xlsx", variable=self.fmt).pack(side="left")
        ttk.Radiobutton(sec, text="CSV (.csv)", value="csv", variable=self.fmt).pack(side="left", padx=12)
        ttk.Label(body, text="When several files are made (CSV with more than one table, or full details split by "
                             "category),\nthey are saved together in a folder named like the file you choose.",
                  style="Hint.TLabel").pack(anchor="w", pady=(6, 0))

        btns = ttk.Frame(body)
        btns.pack(fill="x", pady=(10, 0))
        ttk.Button(btns, text="Cancel", command=self.destroy).pack(side="right")
        ttk.Button(btns, text="Download ...", style="Accent.TButton", command=self._ok).pack(side="right", padx=6)
        self.bind("<Escape>", lambda e: self.destroy())

        self.update_idletasks()
        x = parent.winfo_rootx() + max(0, (parent.winfo_width() - self.winfo_width()) // 2)
        y = parent.winfo_rooty() + max(0, (parent.winfo_height() - self.winfo_height()) // 3)
        # keep the dialog on the screen
        x = max(0, min(x, self.winfo_screenwidth() - self.winfo_width()))
        y = max(0, min(y, self.winfo_screenheight() - self.winfo_height() - 40))
        self.geometry(f"+{x}+{y}")
        try:
            self.wait_visibility()  # grab_set fails on a window that is not on screen yet
            self.grab_set()
        except tk.TclError:
            pass  # still usable, just not modal
        self.focus_set()
        self.wait_window()

    def _fill_cats(self):
        words = self.cat_find.get().lower().split()
        self._view = [name for name, _ in self.categories if all(w in name.lower() for w in words)]
        counts = dict(self.categories)
        state = self.cat_list.cget("state")
        self.cat_list.configure(state="normal")  # a disabled listbox ignores insert/delete
        self.cat_list.delete(0, "end")
        for i, name in enumerate(self._view):
            self.cat_list.insert("end", f"{name}   ({counts[name]:,})")
            if name in self.chosen:
                self.cat_list.selection_set(i)
        self.cat_list.configure(state=state)
        self._update_info()

    def _on_pick(self, event=None):
        picked = set(self.cat_list.curselection())
        for i, name in enumerate(self._view):
            (self.chosen.add if i in picked else self.chosen.discard)(name)
        self._update_info()

    def _pick_all(self, value):
        if self.cat_mode.get() != "some":
            self.cat_mode.set("some")
            self._toggle_cats()
        for name in self._view:
            (self.chosen.add if value else self.chosen.discard)(name)
        self._fill_cats()

    def _toggle_cats(self):
        state = "normal" if self.cat_mode.get() == "some" else "disabled"
        self.cat_list.configure(state=state)
        self.find_entry.configure(state=state)
        self._update_info()

    def _update_info(self):
        if self.cat_mode.get() == "all":
            self.cat_info.set("Every category is included.")
        else:
            self.cat_info.set(f"{len(self.chosen):,} of {len(self.categories):,} categories selected "
                              "(click to select / unselect).")

    def _ok(self):
        cats = None
        if self.categories and self.cat_mode.get() == "some":
            if not self.chosen:
                messagebox.showwarning(APP_NAME, "Select at least one category, or choose 'All categories'.",
                                       parent=self)
                return
            cats = sorted(self.chosen, key=str.lower)
        self.result = {"scope": self.scope.get(), "full": self.full.get(), "categories": cats,
                       "split": self.split.get(), "fmt": self.fmt.get()}
        self.destroy()


class ThrottleBar(ttk.Frame):
    """Live chart of recent requests to GeM: one bar per request (height = response time),
    red = HTTP 500/429 (throttled), purple = timeout, amber = slow, green = OK.
    The meter on the right shows how far the adaptive pace has slowed down from your base delay."""

    def __init__(self, parent):
        super().__init__(parent)
        self.HEIGHT = px(46)
        self.info = tk.StringVar(value="Server throttling: waiting for requests ...")
        ttk.Label(self, textvariable=self.info, style="Stat.TLabel", wraplength=px(480)).pack(anchor="w")
        legend = ttk.Frame(self)
        legend.pack(side="bottom", anchor="w")
        for kind, text in (("ok", "OK"), ("slow", "slow (8s+)"), ("throttled", "HTTP 500/429 - throttled"),
                           ("error", "timeout")):
            tk.Label(legend, text="■", fg=BAR_COLORS[kind]).pack(side="left")
            ttk.Label(legend, text=text, style="Hint.TLabel").pack(side="left", padx=(0, 8))
        body = ttk.Frame(self)
        body.pack(fill="x", pady=(2, 0))
        self.canvas = tk.Canvas(body, height=self.HEIGHT, bg="#f7f7f7", highlightthickness=1,
                                highlightbackground="#cccccc")
        self.canvas.pack(side="left", fill="x", expand=True)
        self.meter = tk.Canvas(body, width=px(18), height=self.HEIGHT, bg="#f7f7f7", highlightthickness=1,
                               highlightbackground="#cccccc")
        self.meter.pack(side="left", padx=(4, 0))
        self._version = -1

    def draw(self, monitor):
        events, total, throttled, pace, base, version = monitor.snapshot()
        if version == self._version:
            return
        self._version = version
        c = self.canvas
        c.delete("all")
        w = max(c.winfo_width(), 50)
        h = self.HEIGHT
        step = px(4)
        shown = events[-(w // step):]
        for i, (_, kind, secs, _) in enumerate(shown):
            x = w - (len(shown) - i) * step
            bar = max(4, min(h - 2, 4 + (h - 6) * min(secs, 30) / 30))
            if kind == "throttled":
                bar = h - 2  # throttling always shows as a full red bar
            c.create_rectangle(x, h - bar, x + step - 1, h, fill=BAR_COLORS[kind], width=0)
        recent = [e for e in events if e[0] > time.time() - 300]
        bad = sum(1 for e in recent if e[1] in ("throttled", "error"))
        pressure = 0.0 if pace <= base else min(1.0, (pace - base) / max(5.0, base * 4))
        m = self.meter
        m.delete("all")
        fill = "#2e9d4f" if pressure < 0.25 else ("#e0a100" if pressure < 0.6 else "#d93025")
        m.create_rectangle(px(2), h - max(3, h * pressure), px(16), h, fill=fill, width=0)
        state = "calm" if pressure < 0.25 else ("slowing down" if pressure < 0.6 else "heavily throttled")
        self.info.set(f"Server: {state}  |  pace {pace:.1f}s/request (base {base:.1f}s)  |  "
                      f"500s/timeouts last 5 min: {bad}  |  total {throttled:,} of {total:,} requests")

    def reset(self):
        self._version = -1
        self.canvas.delete("all")
        self.meter.delete("all")
        self.info.set("Server throttling: waiting for requests ...")


class App:
    def __init__(self, root):
        global UI_SCALE
        self.root = root
        UI_SCALE = min(4.0, max(1.0, root.winfo_fpixels("1i") / 96.0))
        self.data_dir = data_dir()
        self.db = Storage(self.data_dir / "gem_data.db")
        self.export_dir = self.data_dir / "exports"
        self.msgq = queue.Queue()
        self.engine = None
        self.convert_thread = None
        self.convert_cancel = None
        self.convert_progress = {}
        self.convert_started = None
        self.options = {"categories": [], "ministries": [], "states": [], "buyer_entities": []}
        self.cat_view = []           # (value, label) currently shown in the category list
        self.selected_cats = []      # (value, label) chosen by the user
        self._filter_job = None
        self.db_rows = None          # every contract in the database (db_report.prepare)
        self.db_progress = ([], None)
        self.db_results = []
        self.db_only = None          # (title, contract numbers) when showing one History run
        self._db_loading = False
        self._db_stale = True
        self._db_last_load = 0
        self._db_search_job = None
        self._db_waiters = []        # callbacks waiting for the database to load
        self._sort_state = {}
        self.history = []
        self.h_shown = []            # history entries that pass the History tab filters
        self.db.close_stale_runs()
        self.settings = self._load_settings()

        root.title(f"{APP_NAME} {APP_VERSION}")
        self._fit_window()
        self._style()

        DeveloperBanner(root, DEVELOPER_NAME).pack(fill="x", side="top")
        nb = ttk.Notebook(root)
        nb.pack(fill="both", expand=True, padx=6, pady=(6, 0))
        self.nb = nb
        # every tab scrolls when the window is too small for it (small screens, 150-200 % scaling)
        self.tab_scrape = TabScroller(nb, min_width=None)
        self.tab_convert = TabScroller(nb)
        self.tab_db = TabScroller(nb)
        self.tab_history = TabScroller(nb)
        self.tab_help = TabScroller(nb, padding=12)
        nb.add(self.tab_scrape, text="  1. Scrape contracts  ")
        nb.add(self.tab_convert, text="  2. Convert PDFs  ")
        nb.add(self.tab_db, text="  3. Database & search  ")
        nb.add(self.tab_history, text="  4. History  ")
        nb.add(self.tab_help, text="  Help  ")

        self._build_statusbar()
        self._build_scrape_tab()
        self._build_convert_tab()
        self._build_db_tab()
        self._build_history_tab()
        self._build_help_tab()
        self._apply_settings()
        nb.bind("<<NotebookTabChanged>>", self._on_tab_changed)

        root.bind_all("<MouseWheel>", self._on_wheel, add="+")
        root.protocol("WM_DELETE_WINDOW", self._on_close)
        root.report_callback_exception = self._report_error  # button / event errors too
        root.after(150, self._poll)
        root.after(300, self._load_options)

    # ------------------------------------------------------------------ screen
    def _fit_window(self):
        """Open at a comfortable size for this screen and scaling; maximised when the screen is small."""
        root = self.root
        sw, sh = root.winfo_screenwidth(), root.winfo_screenheight()
        want_w, want_h = px(1180), px(870)
        w, h = min(want_w, int(sw * 0.95)), min(want_h, int(sh * 0.88))  # leave room for the taskbar
        root.geometry(f"{w}x{h}+{max(0, (sw - w) // 2)}+{max(0, (sh - h) // 3)}")
        root.minsize(min(px(640), w), min(px(420), h))
        if want_w > sw * 0.95 or want_h > sh * 0.88:
            try:
                root.state("zoomed")
            except tk.TclError:
                pass

    def _on_wheel(self, event):
        """Mouse wheel scrolls the scrollable area under the pointer. Lists, tables and text boxes scroll
        themselves; everything else scrolls the nearest scrollable frame (or the tab page)."""
        w = event.widget
        if isinstance(w, str):  # e.g. the drop-down list of a combobox
            return
        try:
            if w.winfo_class() in ("Text", "Listbox", "Treeview", "TCombobox", "TSpinbox", "Spinbox"):
                return
        except tk.TclError:
            return
        steps = int(-event.delta / 120) or (-1 if event.delta > 0 else 1)
        while w is not None:
            if isinstance(w, (ScrollFrame, TabScroller)) and w.can_scroll():
                w.canvas.yview_scroll(steps, "units")
                return
            w = w.master

    def _relayout_tabs(self):
        try:
            self.root.nametowidget(self.nb.select()).relayout()
        except (KeyError, tk.TclError, AttributeError):
            pass

    # ------------------------------------------------------------------ style
    def _style(self):
        style = ttk.Style()
        for theme in ("vista", "winnative", "clam"):
            if theme in style.theme_names():
                style.theme_use(theme)
                break
        style.configure("Accent.TButton", font=("Segoe UI", 10, "bold"), padding=(14, 6))
        style.configure("Big.TLabel", font=("Segoe UI", 10, "bold"))
        style.configure("Stat.TLabel", font=("Segoe UI", 10))
        style.configure("StatV.TLabel", font=("Consolas", 10, "bold"))
        style.configure("Hint.TLabel", foreground="#555555")
        style.configure("Tile.TLabel", font=("Segoe UI", 15, "bold"))
        line = tkfont.nametofont("TkDefaultFont").metrics("linespace")
        style.configure("Treeview", rowheight=max(px(22), line + px(6)))
        self.root.option_add("*Font", ("Segoe UI", 9))

    # ------------------------------------------------------------ status bar
    def _build_statusbar(self):
        bar = ttk.Frame(self.root, padding=(8, 4))
        bar.pack(fill="x", side="bottom")
        self.opt_status = tk.StringVar(value="Loading search options from gem.gov.in ...")
        ttk.Label(bar, textvariable=self.opt_status).pack(side="left")
        ttk.Button(bar, text="Refresh categories / ministries", command=lambda: self._load_options(force=True)
                   ).pack(side="right")

    # ------------------------------------------------------------ scrape tab
    def _build_scrape_tab(self):
        t = self.tab_scrape.inner
        t.columnconfigure(0, weight=3)
        t.columnconfigure(1, weight=2)
        t.rowconfigure(1, weight=1)

        left = ttk.LabelFrame(t, text="Search  (same options as gem.gov.in/view_contracts)", padding=8)
        left.grid(row=0, column=0, sticky="nsew", padx=(0, 6))
        right = ttk.Frame(t)
        right.grid(row=0, column=1, sticky="nsew")

        # --- mode
        self.mode = tk.StringVar(value="category")
        mrow = ttk.Frame(left)
        mrow.pack(fill="x")
        for val, txt in (("category", "Bid / Category"), ("ministry", "Ministry / Organization"),
                         ("number", "Bid or Contract No.")):
            ttk.Radiobutton(mrow, text=txt, value=val, variable=self.mode, command=self._show_mode
                            ).pack(side="left", padx=(0, 14))

        self.mode_area = ttk.Frame(left)
        self.mode_area.pack(fill="both", expand=True, pady=(6, 0))
        self._build_category_frame()
        self._build_ministry_frame()
        self._build_number_frame()

        # --- dates
        dates = ttk.Frame(left)
        dates.pack(fill="x", pady=(8, 0))
        self.date_frame = dates
        ttk.Label(dates, text="Contract date from").grid(row=0, column=0, sticky="w")
        self.from_var = tk.StringVar()
        self.to_var = tk.StringVar()
        ttk.Entry(dates, textvariable=self.from_var, width=12).grid(row=0, column=1, padx=4)
        ttk.Label(dates, text="to").grid(row=0, column=2)
        self.to_entry = ttk.Entry(dates, textvariable=self.to_var, width=12)
        self.to_entry.grid(row=0, column=3, padx=4)
        ttk.Label(dates, text="(DD-MM-YYYY)", style="Hint.TLabel").grid(row=0, column=4, sticky="w")
        self.till_today = tk.BooleanVar(value=False)
        ttk.Checkbutton(dates, text="up to today", variable=self.till_today, command=self._toggle_today
                        ).grid(row=0, column=5, padx=(8, 0))
        quick = ttk.Frame(dates)
        quick.grid(row=1, column=0, columnspan=6, sticky="w", pady=(4, 0))
        for txt, days in (("Today", 0), ("Yesterday", -1), ("Last 7 days", 7), ("Last 30 days", 30),
                          ("Last 90 days", 90), ("This year", "year")):
            ttk.Button(quick, text=txt, width=11, command=lambda d=days: self._quick_date(d)).pack(side="left", padx=1)

        # --- options
        opts = ttk.LabelFrame(right, text="Options", padding=8)
        opts.pack(fill="x")
        opts.columnconfigure(1, weight=1)
        ttk.Label(opts, text="Data folder").grid(row=0, column=0, sticky="w")
        shown = str(self.data_dir)
        if len(shown) > 42:  # keep the panel narrow: show the end of long paths
            shown = "..." + shown[-40:]
        ttk.Label(opts, text=shown, style="Hint.TLabel").grid(row=0, column=1, sticky="w", padx=4)
        ttk.Button(opts, text="Open", width=6, command=lambda: open_path(self.data_dir)).grid(row=0, column=3, padx=(2, 0))

        self.dl_pdfs = tk.BooleanVar(value=True)
        self.read_live = tk.BooleanVar(value=True)
        self.auto_convert = tk.BooleanVar(value=True)
        ttk.Checkbutton(opts, text="Download contract PDFs", variable=self.dl_pdfs).grid(
            row=1, column=0, columnspan=4, sticky="w", pady=(6, 0))
        ttk.Checkbutton(opts, text="Scrape + add to Excel: read each PDF into the database while scraping\n"
                                   "(Excel is ready instantly; also spaces out requests)",
                        variable=self.read_live).grid(row=2, column=0, columnspan=4, sticky="w")
        ttk.Checkbutton(opts, text="Export Excel when finished (uses Convert tab settings)",
                        variable=self.auto_convert).grid(row=3, column=0, columnspan=4, sticky="w")

        grid = ttk.Frame(opts)
        grid.grid(row=4, column=0, columnspan=4, sticky="w", pady=(6, 0))
        ttk.Label(grid, text="Base delay between requests (ms)").grid(row=0, column=0, sticky="w")
        self.delay_var = tk.IntVar(value=1000)
        ttk.Spinbox(grid, from_=200, to=10000, increment=100, width=7, textvariable=self.delay_var
                    ).grid(row=0, column=1, padx=4, sticky="w")
        ttk.Label(grid, text="Parallel downloads").grid(row=1, column=0, sticky="w")
        self.par_var = tk.IntVar(value=2)
        ttk.Spinbox(grid, from_=1, to=6, width=7, textvariable=self.par_var).grid(row=1, column=1, padx=4, sticky="w")
        ttk.Label(grid, text="Days per search window").grid(row=2, column=0, sticky="w")
        self.window_var = tk.StringVar(value="Auto")
        ttk.Combobox(grid, textvariable=self.window_var, width=6, state="readonly",
                     values=["Auto", "1", "3", "7", "15", "30", "60", "90"]).grid(row=2, column=1, padx=4, sticky="w")
        ttk.Label(opts, text="The delay adapts automatically: it slows down when GeM answers with HTTP 500 and "
                             "speeds back up to the base delay when requests succeed.",
                  style="Hint.TLabel", wraplength=px(420)).grid(row=5, column=0, columnspan=4, sticky="w", pady=(4, 0))

        # --- controls
        ctl = ttk.Frame(right)
        ctl.pack(fill="x", pady=8)
        self.btn_start = ttk.Button(ctl, text="Start scraping", style="Accent.TButton", command=self._start_scrape)
        self.btn_start.pack(side="left")
        self.btn_pause = ttk.Button(ctl, text="Pause", command=self._pause_scrape, state="disabled")
        self.btn_pause.pack(side="left", padx=6)
        self.btn_stop = ttk.Button(ctl, text="Stop", command=self._stop_scrape, state="disabled")
        self.btn_stop.pack(side="left")
        ttk.Button(ctl, text="Failures ...", command=self._show_failures).pack(side="right")

        # --- progress
        prog = ttk.LabelFrame(right, text="Progress", padding=8)
        prog.pack(fill="both", expand=True)
        bar_row = ttk.Frame(prog)
        bar_row.pack(fill="x")
        self.sc_pct = tk.StringVar(value="")
        ttk.Label(bar_row, textvariable=self.sc_pct, style="StatV.TLabel", width=11, anchor="e").pack(side="right")
        self.sc_bar = ttk.Progressbar(bar_row, mode="determinate", maximum=1)
        self.sc_bar.pack(side="left", fill="x", expand=True, padx=(0, 6))
        self._bar_pulsing = False
        self.throttle_bar = ThrottleBar(prog)
        self.throttle_bar.pack(fill="x", pady=(6, 0))
        stats = ttk.Frame(prog)
        stats.pack(fill="x", pady=(6, 0))
        self.sc_stats = {}
        rows = [("status", "Status"), ("elapsed", "Elapsed"), ("eta", "Time left (est.)"),
                ("searches", "Searches done"), ("pages", "Result pages"), ("found", "Contracts found"),
                ("retries", "Server retries"),
                ("downloaded", "PDFs downloaded"), ("skipped", "Already had"), ("failed", "Failed"),
                ("read", "Read into Excel data"), ("speed", "Speed"), ("avg", "Avg per PDF"), ("data", "Downloaded")]
        stats.columnconfigure(1, weight=1)
        stats.columnconfigure(3, weight=1)
        half = (len(rows) + 1) // 2
        for i, (key, title) in enumerate(rows):
            r, c = i % half, (i // half) * 2
            ttk.Label(stats, text=title, style="Stat.TLabel").grid(row=r, column=c, sticky="w", padx=(0, 6))
            var = tk.StringVar(value="-")
            ttk.Label(stats, textvariable=var, style="StatV.TLabel", wraplength=px(170)).grid(
                row=r, column=c + 1, sticky="w", padx=(0, 10))
            self.sc_stats[key] = var
        ttk.Label(prog, text="Current search", style="Stat.TLabel").pack(anchor="w", pady=(6, 0))
        self.sc_current = tk.StringVar(value="-")
        ttk.Label(prog, textvariable=self.sc_current, style="StatV.TLabel", wraplength=px(420)).pack(anchor="w")

        # --- log
        logf = ttk.LabelFrame(t, text="Activity log", padding=4)
        logf.grid(row=1, column=0, columnspan=2, sticky="nsew", pady=(6, 0))
        self.sc_log = ScrolledText(logf, height=5, font=("Consolas", 9), state="disabled", wrap="none")
        self.sc_log.pack(fill="both", expand=True)

    def _build_category_frame(self):
        f = ttk.Frame(self.mode_area)
        self.cat_frame = f
        f.columnconfigure(0, weight=3, uniform="cats")
        f.columnconfigure(2, weight=2, uniform="cats")
        f.rowconfigure(2, weight=1)
        ttk.Label(f, text="Bid / Contract No. (optional - overrides the filters below)").grid(
            row=0, column=0, columnspan=3, sticky="w")
        self.cat_bno = tk.StringVar()
        ttk.Entry(f, textvariable=self.cat_bno).grid(row=1, column=0, columnspan=3, sticky="ew", pady=(0, 6))

        avail = ttk.Frame(f)
        avail.grid(row=2, column=0, sticky="nsew")
        avail.rowconfigure(2, weight=1)
        avail.columnconfigure(0, weight=1)
        ttk.Label(avail, text="Find category").grid(row=0, column=0, sticky="w")
        self.cat_search = tk.StringVar()
        self.cat_search.trace_add("write", lambda *a: self._schedule_filter())
        ttk.Entry(avail, textvariable=self.cat_search).grid(row=1, column=0, columnspan=2, sticky="ew")
        self.cat_list = tk.Listbox(avail, selectmode="extended", height=7, activestyle="none", exportselection=False)
        self.cat_list.grid(row=2, column=0, sticky="nsew")
        sb = ttk.Scrollbar(avail, orient="vertical", command=self.cat_list.yview)
        sb.grid(row=2, column=1, sticky="ns")
        self.cat_list.configure(yscrollcommand=sb.set)
        self.cat_list.bind("<Double-Button-1>", lambda e: self._add_cats())
        self.cat_count = tk.StringVar(value="")
        ttk.Label(avail, textvariable=self.cat_count, style="Hint.TLabel").grid(row=3, column=0, sticky="w")

        mid = ttk.Frame(f)
        mid.grid(row=2, column=1, padx=6)
        ttk.Button(mid, text="Add >", width=9, command=self._add_cats).pack(pady=2)
        ttk.Button(mid, text="< Remove", width=9, command=self._remove_cats).pack(pady=2)
        ttk.Button(mid, text="Clear all", width=9, command=self._clear_cats).pack(pady=2)

        sel = ttk.Frame(f)
        sel.grid(row=2, column=2, sticky="nsew")
        sel.rowconfigure(1, weight=1)
        sel.columnconfigure(0, weight=1)
        self.sel_title = tk.StringVar(value="Selected categories: none = ALL categories")
        ttk.Label(sel, textvariable=self.sel_title, style="Big.TLabel").grid(row=0, column=0, sticky="w")
        self.sel_list = tk.Listbox(sel, selectmode="extended", height=7, activestyle="none", exportselection=False)
        self.sel_list.grid(row=1, column=0, sticky="nsew")
        self.sel_list.bind("<Double-Button-1>", lambda e: self._remove_cats())
        ttk.Label(sel, text="No category selected = scrape every contract,\nexactly like the website with no category.",
                  style="Hint.TLabel").grid(row=2, column=0, sticky="w")

    def _build_ministry_frame(self):
        f = ttk.Frame(self.mode_area)
        self.min_frame = f
        f.columnconfigure(1, weight=1)
        self.ent_var = tk.StringVar()
        self.min_var = tk.StringVar()
        self.state_var = tk.StringVar()
        self.dept_var = tk.StringVar()
        self.org_var = tk.StringVar()
        rows = [("Name of Buyer Entity", self.ent_var), ("Ministry", self.min_var), ("State", self.state_var),
                ("Department", self.dept_var), ("Organization", self.org_var)]
        self.min_boxes = {}
        for i, (label, var) in enumerate(rows):
            ttk.Label(f, text=label).grid(row=i, column=0, sticky="w", pady=3)
            cb = ttk.Combobox(f, textvariable=var, height=20)
            cb.grid(row=i, column=1, sticky="ew", pady=3, padx=(6, 0))
            self.min_boxes[label] = cb
        self.min_boxes["Ministry"].configure(state="readonly")
        self.min_boxes["State"].configure(state="readonly")
        ent = self.min_boxes["Name of Buyer Entity"]
        ent.bind("<KeyRelease>", self._filter_entities)
        self.min_boxes["Ministry"].bind("<<ComboboxSelected>>", lambda e: self._load_departments())
        self.min_boxes["State"].bind("<<ComboboxSelected>>", lambda e: self._load_departments())
        self.min_boxes["Department"].bind("<<ComboboxSelected>>", lambda e: self._load_orgs())
        ttk.Button(f, text="Clear", command=self._clear_ministry).grid(row=5, column=1, sticky="e", pady=(4, 0))
        ttk.Label(f, text="Choose a Ministry and/or State (Department and Organization lists load\n"
                          "automatically), or type a Buyer Entity name. With no Department chosen, the app\n"
                          "searches every department of the Ministry/State one by one (much faster on GeM).",
                  style="Hint.TLabel").grid(row=6, column=0, columnspan=2, sticky="w", pady=(6, 0))

    def _build_number_frame(self):
        f = ttk.Frame(self.mode_area)
        self.num_frame = f
        ttk.Label(f, text="Bid number or Contract number").pack(anchor="w")
        self.num_var = tk.StringVar()
        ttk.Entry(f, textvariable=self.num_var, width=50).pack(anchor="w", pady=4)
        ttk.Label(f, text="Examples:  GEM/2026/B/7968448   or   GEMC-511687707842333\n"
                          "Dates and categories are not used for a number search (same as the website).",
                  style="Hint.TLabel").pack(anchor="w")

    def _show_mode(self):
        for fr in (self.cat_frame, self.min_frame, self.num_frame):
            fr.pack_forget()
        {"category": self.cat_frame, "ministry": self.min_frame, "number": self.num_frame}[self.mode.get()].pack(
            fill="both", expand=True)
        state = "disabled" if self.mode.get() == "number" else "normal"
        for child in self.date_frame.winfo_children():
            try:
                child.configure(state=state)
            except tk.TclError:
                for sub in child.winfo_children():
                    sub.configure(state=state)
        if state == "normal":
            self._toggle_today()

    # ------------------------------------------------------- category lists
    def _schedule_filter(self):
        if self._filter_job:
            self.root.after_cancel(self._filter_job)
        self._filter_job = self.root.after(200, self._filter_categories)

    def _filter_categories(self):
        self._filter_job = None
        words = self.cat_search.get().lower().split()
        cats = self.options.get("categories", [])
        view = [c for c in cats if all(w in c[1].lower() for w in words)] if words else list(cats)
        self.cat_view = view
        self.cat_list.delete(0, "end")
        if view:
            self.cat_list.insert("end", *[label for _, label in view])
        self.cat_count.set(f"{len(view):,} of {len(cats):,} categories")

    def _add_cats(self):
        have = {v for v, _ in self.selected_cats}
        for i in self.cat_list.curselection():
            v, label = self.cat_view[i]
            if v not in have:
                self.selected_cats.append((v, label))
                have.add(v)
        self._refresh_selected()

    def _remove_cats(self):
        for i in sorted(self.sel_list.curselection(), reverse=True):
            del self.selected_cats[i]
        self._refresh_selected()

    def _clear_cats(self):
        self.selected_cats = []
        self._refresh_selected()

    def _refresh_selected(self):
        self.sel_list.delete(0, "end")
        if self.selected_cats:
            self.sel_list.insert("end", *[label for _, label in self.selected_cats])
            self.sel_title.set(f"Selected categories: {len(self.selected_cats)}")
        else:
            self.sel_title.set("Selected categories: none = ALL categories")

    def _filter_entities(self, event=None):
        text = self.ent_var.get().lower().strip()
        names = self.options.get("buyer_entities", [])
        vals = [n for n in names if text in n.lower()][:300] if text else names[:300]
        self.min_boxes["Name of Buyer Entity"].configure(values=vals)

    def _clear_ministry(self):
        for v in (self.ent_var, self.min_var, self.state_var, self.dept_var, self.org_var):
            v.set("")
        self.min_boxes["Department"].configure(values=[])
        self.min_boxes["Organization"].configure(values=[])

    def _load_departments(self):
        ministry, state = self.min_var.get(), self.state_var.get()
        self.dept_var.set("")
        self.org_var.set("")
        self.min_boxes["Organization"].configure(values=[])
        if not ministry and not state:
            self.min_boxes["Department"].configure(values=[])
            return
        self.min_boxes["Department"].configure(values=["Loading ..."])

        def work():
            try:
                depts = GemClient(delay_ms=0, timeout=60).get_departments(ministry, state)
            except Exception as e:  # noqa: BLE001
                depts = []
                self.msgq.put(("log", f"Could not load departments: {e}"))
            self.msgq.put(("call", lambda: self._set_departments(ministry, state, depts)))
        threading.Thread(target=work, daemon=True).start()

    def _set_departments(self, ministry, state, depts):
        # ignore a slow answer for a ministry/state the user has since changed
        if (self.min_var.get(), self.state_var.get()) == (ministry, state):
            self.min_boxes["Department"].configure(values=[""] + depts)

    def _set_organizations(self, dept, orgs):
        if self.dept_var.get() == dept:
            self.min_boxes["Organization"].configure(values=[""] + orgs)

    def _load_orgs(self):
        dept = self.dept_var.get()
        self.org_var.set("")
        if not dept:
            self.min_boxes["Organization"].configure(values=[])
            return
        self.min_boxes["Organization"].configure(values=["Loading ..."])

        def work():
            try:
                orgs = GemClient(delay_ms=0, timeout=60).get_organizations(dept)
            except Exception as e:  # noqa: BLE001
                orgs = []
                self.msgq.put(("log", f"Could not load organizations: {e}"))
            self.msgq.put(("call", lambda: self._set_organizations(dept, orgs)))
        threading.Thread(target=work, daemon=True).start()

    # --------------------------------------------------------- search options
    def _load_options(self, force=False):
        cached, saved_at = self.db.get("search_options")
        if cached:
            cached["categories"] = [tuple(c) for c in cached.get("categories", [])]
        if cached and not force:
            self._set_options(cached, "saved")
            if time.time() - (saved_at or 0) < OPTIONS_MAX_AGE:
                return
        self.opt_status.set("Updating categories / ministries from gem.gov.in ...")

        def work():
            try:
                opts = GemClient(delay_ms=0, timeout=90).fetch_form_options()
                if not opts["categories"]:
                    raise ValueError("no categories found on the page")
                self.db.put("search_options", opts)
                self.msgq.put(("call", lambda: self._set_options(opts, "live")))
            except Exception as e:  # noqa: BLE001
                msg = f"Could not update search options ({e})."
                self.msgq.put(("call", lambda: self.opt_status.set(
                    msg + (" Using saved list." if cached else " Check your internet and press Refresh."))))
        threading.Thread(target=work, daemon=True).start()

    def _set_options(self, opts, source):
        self.options = opts
        self._filter_categories()
        self.min_boxes["Ministry"].configure(values=[""] + opts.get("ministries", []))
        self.min_boxes["State"].configure(values=[""] + opts.get("states", []))
        self._filter_entities()
        self.opt_status.set(
            f"Search options ({source}): {len(opts.get('categories', [])):,} categories, "
            f"{len(opts.get('ministries', []))} ministries, {len(opts.get('states', []))} states, "
            f"{len(opts.get('buyer_entities', [])):,} buyer entities - updated {opts.get('fetched_at', '?')}")

    # ----------------------------------------------------------- date helpers
    def _quick_date(self, d):
        today = date.today()
        if d == "year":
            start, end = date(today.year, 1, 1), today
        elif d == 0:
            start = end = today
        elif d == -1:
            start = end = today - timedelta(days=1)
        else:
            start, end = today - timedelta(days=d - 1), today
        self.till_today.set(False)
        self._toggle_today()
        self.from_var.set(start.strftime("%d-%m-%Y"))
        self.to_var.set(end.strftime("%d-%m-%Y"))

    def _toggle_today(self):
        if self.till_today.get():
            self.to_var.set(date.today().strftime("%d-%m-%Y"))
            self.to_entry.configure(state="disabled")
        else:
            self.to_entry.configure(state="normal")

    # ---------------------------------------------------------------- scrape
    def _int(self, var, default, low, high):
        try:
            return min(high, max(low, int(var.get())))
        except (tk.TclError, ValueError):
            return default

    def _build_config(self):
        mode = self.mode.get()
        cfg = ScrapeConfig(mode=mode, download_pdfs=self.dl_pdfs.get(),
                           read_while_scraping=self.read_live.get())
        cfg.delay_ms = self._int(self.delay_var, 1000, 0, 60000)
        cfg.downloads = self._int(self.par_var, 2, 1, 6)
        cfg.read_workers = self._int(self.workers_var, converter.default_workers(), 1, os.cpu_count() or 1)
        w = self.window_var.get()
        cfg.window_days = 0 if w == "Auto" else int(w)

        if mode == "category" and self.cat_bno.get().strip():
            mode = cfg.mode = "number"
            cfg.number = self.cat_bno.get().strip()
        if mode == "number":
            cfg.number = cfg.number or self.num_var.get().strip()
            if not cfg.number:
                raise ValueError("Enter a Bid number or Contract number.")
            return cfg

        f, t = parse_dmy(self.from_var.get()), parse_dmy(self.to_var.get())
        if self.till_today.get():
            t = date.today()
        if not f or not t:
            raise ValueError("Enter dates as DD-MM-YYYY (e.g. 05-10-2026).")
        if f > t:
            raise ValueError("'From' date is after 'To' date.")
        if t > date.today():
            t = date.today()
        cfg.from_date, cfg.to_date = f, t
        if mode == "category":
            cfg.categories = list(self.selected_cats)
        else:
            cfg.buyer_entity = self.ent_var.get().strip()
            cfg.ministry = self.min_var.get().strip()
            cfg.state = self.state_var.get().strip()
            cfg.department = self.dept_var.get().strip() if self.dept_var.get() != "Loading ..." else ""
            cfg.organization = self.org_var.get().strip() if self.org_var.get() != "Loading ..." else ""
            if not (cfg.buyer_entity or cfg.ministry or cfg.state):
                raise ValueError("Choose a Ministry or a State, or type a Buyer Entity name.")
        return cfg

    def _start_scrape(self):
        if self.engine and self.engine.is_running():
            return
        if self.convert_thread and self.convert_thread.is_alive():
            messagebox.showwarning(APP_NAME, "Please wait for the conversion to finish first.")
            return
        try:
            cfg = self._build_config()
        except ValueError as e:
            messagebox.showwarning(APP_NAME, str(e))
            return
        if cfg.mode == "category" and not cfg.categories:
            days = (cfg.to_date - cfg.from_date).days + 1
            if days > 7 and not messagebox.askyesno(
                    APP_NAME, f"No category is selected, so ALL contracts for {days} days will be scraped.\n\n"
                              "That can be tens of thousands of contracts and take many hours. Continue?"):
                return
        self._save_settings()
        self._log_clear(self.sc_log)
        self._log(self.sc_log, f"Starting: {describe(cfg)}. Data folder: {self.data_dir}")
        self.engine = ScrapeEngine(cfg, self.db, self.data_dir, log=lambda m: self.msgq.put(("sclog", m)),
                                   on_done=lambda eng: self.msgq.put(("scrape_done", eng)))
        self.throttle_bar.reset()
        self.engine.start()
        self.btn_start.configure(state="disabled")
        self.btn_pause.configure(state="normal", text="Pause")
        self.btn_stop.configure(state="normal")
        self._stop_pulsing()
        self.sc_bar.configure(value=0)
        self.sc_pct.set("")

    def _pause_scrape(self):
        if self.engine and self.engine.is_running() and not self.engine.stop_event.is_set():
            paused = self.engine.toggle_pause()
            self.btn_pause.configure(text="Resume" if paused else "Pause")
            self._log(self.sc_log, "Paused." if paused else "Resumed.")

    def _stop_scrape(self):
        if self.engine:
            self.engine.stop()
            self.btn_stop.configure(state="disabled")
            self.btn_pause.configure(state="disabled")
            self._log(self.sc_log, "Stopping (finishing the current request) ...")

    def _scrape_finished(self, eng):
        self.btn_start.configure(state="normal")
        self.btn_pause.configure(state="disabled", text="Pause")
        self.btn_stop.configure(state="disabled")
        self._update_scrape_stats()
        self._db_changed()
        s = eng.stats.snapshot()
        self._log(self.sc_log, f"Done in {fmt_duration(s['elapsed'])}: {s['found']} contracts found, "
                               f"{s['downloaded']} PDFs downloaded, {s['skipped']} already had, {s['failed']} failed.")
        if s["failed"] or s["failed_queries"]:
            self._log(self.sc_log, "Some downloads/searches failed - see 'Failures ...'. "
                                   "Run the same search again to retry them.")
        if self.auto_convert.get() and s["status"] in ("Finished", "Stopped") and (s["found"] or s["skipped"]):
            self.conv_pdf_var.set(str(self.data_dir / "pdfs"))
            self._log(self.sc_log, "Exporting to Excel (see the Convert tab) ...")
            self.nb.select(self.tab_convert)
            self._start_convert()

    def _show_failures(self):
        win = tk.Toplevel(self.root)
        win.title("Failures")
        win.geometry(f"{min(px(900), int(win.winfo_screenwidth() * 0.95))}x"
                     f"{min(px(420), int(win.winfo_screenheight() * 0.8))}")
        tree = ttk.Treeview(win, columns=("at", "kind", "ref", "detail"), show="headings")
        for col, title, width in (("at", "Time", 140), ("kind", "Type", 70), ("ref", "Contract / search", 260),
                                  ("detail", "Error", 420)):
            tree.heading(col, text=title)
            tree.column(col, width=px(width), anchor="w")
        sb = ttk.Scrollbar(win, orient="vertical", command=tree.yview)
        tree.configure(yscrollcommand=sb.set)
        bottom = ttk.Frame(win, padding=6)
        bottom.pack(side="bottom", fill="x")
        sb.pack(side="right", fill="y")
        tree.pack(fill="both", expand=True)

        def load():
            tree.delete(*tree.get_children())
            rows = self.db.failures()
            for at, kind, ref, detail in rows:
                tree.insert("", "end", values=(time.strftime("%d-%m-%Y %H:%M:%S", time.localtime(at)),
                                               kind, ref, detail))
            count.set(f"{len(rows):,} failure(s). PDFs: run the same search again to retry them.")

        def clear():
            if messagebox.askyesno(APP_NAME, "Clear the failure list?", parent=win):
                self.db.clear_failures()
                load()
        count = tk.StringVar()
        ttk.Label(bottom, textvariable=count).pack(side="left")
        ttk.Button(bottom, text="Clear list", command=clear).pack(side="right")
        ttk.Button(bottom, text="Refresh", command=load).pack(side="right", padx=6)
        load()

    @staticmethod
    def _scrape_progress(s):
        """Searches done, including an estimate for the running search; None when it can't be estimated."""
        total, done = s["queries_total"], s["queries_done"]
        if s["finished"]:
            return total if s["status"] == "Finished" else done
        if total <= 1:
            return None
        progress = done
        if done and done < total:
            # pages the finished searches needed on average -> how far the running search probably is
            avg_pages = (s["pages"] - s["query_pages"]) / done
            if avg_pages > 0:
                progress += min(0.95, s["query_pages"] / avg_pages)
        return progress

    def _stop_pulsing(self):
        if self._bar_pulsing:
            self.sc_bar.stop()
            self.sc_bar.configure(mode="determinate")
            self._bar_pulsing = False

    def _update_scrape_stats(self):
        if not self.engine:
            return
        s = self.engine.stats.snapshot()
        s["retries"] = self.engine.client.retry_count
        self.throttle_bar.draw(self.engine.monitor)
        if self.engine.reader:
            st_read = f"{s['read']:,}" + (f" ({s['read_errors']} unreadable)" if s["read_errors"] else "")
            if s["read_backlog"]:
                st_read += f", {s['read_backlog']} queued"
            self.sc_stats["read"].set(st_read)
        else:
            self.sc_stats["read"].set("off")
        total, done = s["queries_total"], s["queries_done"]
        running = not s["finished"]
        progress = self._scrape_progress(s)
        if progress is None:
            # one search (or the search list is still loading): GeM never says how many result pages there
            # are, so a percentage is impossible - keep the bar moving instead of sitting at 0 %
            if not self._bar_pulsing:
                self.sc_bar.configure(mode="indeterminate")
                self.sc_bar.start(15)
                self._bar_pulsing = True
            self.sc_pct.set(f"page {s['query_pages'] + 1:,}" if total else "starting")
        else:
            self._stop_pulsing()
            self.sc_bar.configure(maximum=max(total, 1), value=progress)
            self.sc_pct.set(f"{progress / max(total, 1) * 100:.1f} %")
        el = s["elapsed"]
        st = self.sc_stats
        st["status"].set(s["status"])
        st["elapsed"].set(fmt_duration(el))
        if not running:
            st["eta"].set("-")
        elif progress and total > progress:
            st["eta"].set(fmt_duration(el / progress * (total - progress)))
        else:
            st["eta"].set("unknown (single search)" if total == 1 else "calculating ...")
        st["searches"].set(f"{done:,} / {total:,}")
        current = s["current"] or "-"
        if running and s["query_pages"]:
            current += f"  -  page {s['query_pages']:,}, {s['query_found']:,} contracts so far"
        self.sc_current.set(current)
        st["pages"].set(f"{s['pages']:,}")
        st["found"].set(f"{s['found']:,}")
        st["downloaded"].set(f"{s['downloaded']:,}")
        st["skipped"].set(f"{s['skipped']:,}")
        st["failed"].set(f"{s['failed']:,}" + (f" (+{s['failed_queries']} searches)" if s["failed_queries"] else ""))
        st["speed"].set(f"{s['downloaded'] / el * 60:,.1f} /min" if el > 0 and s["downloaded"] else "-")
        st["avg"].set(f"{s['download_seconds'] / s['downloaded']:.1f} s" if s["downloaded"] else "-")
        st["data"].set(f"{s['bytes'] / 1048576:,.1f} MB")
        st["retries"].set(f"{s['retries']:,}")

    # ------------------------------------------------------------ convert tab
    def _build_convert_tab(self):
        t = self.tab_convert.inner
        t.columnconfigure(0, weight=1)
        t.rowconfigure(1, weight=1)

        top = ttk.LabelFrame(t, text="Files", padding=8)
        top.grid(row=0, column=0, sticky="ew")
        top.columnconfigure(1, weight=1)
        self.conv_pdf_var = tk.StringVar()
        self.conv_out_var = tk.StringVar()
        self.conv_name_var = tk.StringVar(value="gem_contracts")
        for i, (label, var, browse) in enumerate((("PDF folder", self.conv_pdf_var, True),
                                                  ("Save output to", self.conv_out_var, True),
                                                  ("File name", self.conv_name_var, False))):
            ttk.Label(top, text=label).grid(row=i, column=0, sticky="w", pady=2)
            ttk.Entry(top, textvariable=var).grid(row=i, column=1, sticky="ew", padx=4, pady=2)
            if browse:
                ttk.Button(top, text="Browse", width=8, command=lambda v=var: self._browse_dir(v)).grid(row=i, column=2)
        fmt_row = ttk.Frame(top)
        fmt_row.grid(row=3, column=0, columnspan=3, sticky="w", pady=(6, 0))
        ttk.Label(fmt_row, text="Formats:").pack(side="left")
        self.fmt_xlsx = tk.BooleanVar(value=True)
        self.fmt_csv = tk.BooleanVar(value=False)
        self.fmt_json = tk.BooleanVar(value=False)
        ttk.Checkbutton(fmt_row, text="Excel (.xlsx)", variable=self.fmt_xlsx).pack(side="left", padx=6)
        ttk.Checkbutton(fmt_row, text="CSV (one file per sheet)", variable=self.fmt_csv).pack(side="left", padx=6)
        ttk.Checkbutton(fmt_row, text="JSON", variable=self.fmt_json).pack(side="left", padx=6)
        ttk.Label(fmt_row, text="   CPU workers").pack(side="left")
        self.workers_var = tk.IntVar(value=converter.default_workers())
        ttk.Spinbox(fmt_row, from_=1, to=max(1, os.cpu_count() or 1), width=4, textvariable=self.workers_var
                    ).pack(side="left", padx=4)
        self.reparse = tk.BooleanVar(value=False)
        ttk.Checkbutton(fmt_row, text="Re-read all PDFs (ignore cache)", variable=self.reparse).pack(side="left", padx=6)

        # --- columns chooser
        cols = ttk.LabelFrame(t, text="What to export - sheets and columns (everything is selected by default)",
                              padding=8)
        cols.grid(row=1, column=0, sticky="nsew", pady=6)
        cols.columnconfigure(1, weight=1)
        cols.rowconfigure(0, weight=1)
        left = ttk.Frame(cols)
        left.grid(row=0, column=0, sticky="ns", padx=(0, 8))
        self.sheet_vars = {}
        self.col_vars = {}
        self.sheet_labels = {}
        for sheet in converter.SHEET_ORDER:
            self.sheet_vars[sheet] = tk.BooleanVar(value=True)
            self.col_vars[sheet] = {title: tk.BooleanVar(value=True) for _, title, _ in converter.SCHEMA[sheet]}
            row = ttk.Frame(left)
            row.pack(fill="x", pady=1)
            ttk.Checkbutton(row, variable=self.sheet_vars[sheet], command=self._refresh_sheet_labels).pack(side="left")
            lbl = ttk.Label(row, text=sheet, cursor="hand2", width=26)
            lbl.pack(side="left")
            lbl.bind("<Button-1>", lambda e, s=sheet: self._show_columns(s))
            self.sheet_labels[sheet] = lbl
        ttk.Separator(left).pack(fill="x", pady=6)
        ttk.Button(left, text="Select everything", command=lambda: self._select_all(True)).pack(fill="x", pady=1)
        ttk.Button(left, text="Clear everything", command=lambda: self._select_all(False)).pack(fill="x", pady=1)
        ttk.Label(left, text="Click a sheet name to pick its columns.", style="Hint.TLabel").pack(anchor="w", pady=4)

        right = ttk.Frame(cols)
        right.grid(row=0, column=1, sticky="nsew")
        right.rowconfigure(1, weight=1)
        right.columnconfigure(0, weight=1)
        head = ttk.Frame(right)
        head.grid(row=0, column=0, sticky="ew")
        self.col_title = tk.StringVar()
        ttk.Label(head, textvariable=self.col_title, style="Big.TLabel").pack(side="left")
        ttk.Button(head, text="None", width=6, command=lambda: self._sheet_cols(False)).pack(side="right")
        ttk.Button(head, text="All", width=6, command=lambda: self._sheet_cols(True)).pack(side="right", padx=4)
        self.col_area = ScrollFrame(right)
        self.col_area.grid(row=1, column=0, sticky="nsew", pady=(4, 0))
        self.current_sheet = None
        self._show_columns(converter.SHEET_ORDER[0])

        # --- run + progress
        bottom = ttk.Frame(t)
        bottom.grid(row=2, column=0, sticky="ew")
        bottom.columnconfigure(1, weight=1)
        btns = ttk.Frame(bottom)
        btns.grid(row=0, column=0, sticky="w")
        self.btn_convert = ttk.Button(btns, text="Convert now", style="Accent.TButton", command=self._start_convert)
        self.btn_convert.pack(side="left")
        self.btn_cancel_conv = ttk.Button(btns, text="Cancel", state="disabled", command=self._cancel_convert)
        self.btn_cancel_conv.pack(side="left", padx=6)
        ttk.Button(btns, text="Open output folder", command=lambda: open_path(self._conv_out())).pack(side="left")
        self.cv_bar = ttk.Progressbar(bottom, mode="determinate", maximum=1)
        self.cv_bar.grid(row=0, column=1, sticky="ew", padx=10)
        self.cv_status = tk.StringVar(value="Ready.")
        ttk.Label(bottom, textvariable=self.cv_status, style="StatV.TLabel").grid(row=1, column=0, columnspan=2,
                                                                                    sticky="w", pady=(4, 0))
        self.cv_log = ScrolledText(t, height=5, font=("Consolas", 9), state="disabled", wrap="none")
        self.cv_log.grid(row=3, column=0, sticky="ew", pady=(6, 0))

    def _browse_dir(self, var):
        d = filedialog.askdirectory(initialdir=var.get() or str(self.data_dir))
        if d:
            var.set(d)

    def _conv_out(self):
        return Path(self.conv_out_var.get().strip() or self.export_dir)

    def _show_columns(self, sheet):
        self.current_sheet = sheet
        for w in self.col_area.inner.winfo_children():
            w.destroy()
        cols = converter.SCHEMA[sheet]
        per_col = (len(cols) + 2) // 3
        for i, (_, title, _) in enumerate(cols):
            ttk.Checkbutton(self.col_area.inner, text=title, variable=self.col_vars[sheet][title],
                            command=self._refresh_sheet_labels).grid(row=i % per_col, column=i // per_col,
                                                                     sticky="w", padx=(0, 18), pady=1)
        self.col_area.canvas.yview_moveto(0)
        self._refresh_sheet_labels()

    def _sheet_cols(self, value):
        for v in self.col_vars[self.current_sheet].values():
            v.set(value)
        self._refresh_sheet_labels()

    def _select_all(self, value):
        for sheet in converter.SHEET_ORDER:
            self.sheet_vars[sheet].set(value)
            for v in self.col_vars[sheet].values():
                v.set(value)
        self._refresh_sheet_labels()

    def _refresh_sheet_labels(self):
        for sheet, lbl in self.sheet_labels.items():
            n = sum(v.get() for v in self.col_vars[sheet].values())
            mark = "> " if sheet == self.current_sheet else "  "
            lbl.configure(text=f"{mark}{sheet} ({n}/{len(self.col_vars[sheet])})")
        if self.current_sheet:
            n = sum(v.get() for v in self.col_vars[self.current_sheet].values())
            self.col_title.set(f"{self.current_sheet}: {n} of {len(self.col_vars[self.current_sheet])} columns")

    def _selection(self):
        return {sheet: [t for t, v in self.col_vars[sheet].items() if v.get()]
                for sheet in converter.SHEET_ORDER if self.sheet_vars[sheet].get()}

    def _start_convert(self):
        if self.convert_thread and self.convert_thread.is_alive():
            return
        if self.engine and self.engine.is_running():
            messagebox.showwarning(APP_NAME, "Scraping is running. With 'Scrape + add to Excel' on, PDFs are "
                                             "already being read - export when the scrape finishes.")
            return
        pdf_dir = Path(self.conv_pdf_var.get().strip() or (self.data_dir / "pdfs"))
        out_dir = self._conv_out()
        name = self.conv_name_var.get().strip() or "gem_contracts"
        name = "".join(ch for ch in name if ch not in '\\/:*?"<>|') or "gem_contracts"
        formats = [f for f, v in (("xlsx", self.fmt_xlsx), ("csv", self.fmt_csv), ("json", self.fmt_json)) if v.get()]
        selection = {k: v for k, v in self._selection().items() if v}
        if not formats:
            messagebox.showwarning(APP_NAME, "Choose at least one output format.")
            return
        if not selection:
            messagebox.showwarning(APP_NAME, "Select at least one sheet with at least one column.")
            return
        if not pdf_dir.exists() and not self.db.listing_count():
            messagebox.showwarning(APP_NAME, f"PDF folder not found:\n{pdf_dir}")
            return
        workers = self._int(self.workers_var, converter.default_workers(), 1, os.cpu_count() or 1)
        self._save_settings()
        self._log_clear(self.cv_log)
        self.convert_cancel = threading.Event()
        self.convert_progress = {"done": 0, "total": 0, "parsed": 0, "cached": 0, "errors": 0}
        self.convert_started = time.time()
        self.btn_convert.configure(state="disabled")
        self.btn_cancel_conv.configure(state="normal")
        use_cache = not self.reparse.get()

        def progress(done, total, parsed, cached, errors):
            self.convert_progress = {"done": done, "total": total, "parsed": parsed, "cached": cached,
                                     "errors": errors}

        def work():
            try:
                res = converter.convert(self.db, pdf_dir, out_dir, name, formats, selection, workers,
                                        progress=progress, cancel=self.convert_cancel, use_cache=use_cache,
                                        log=lambda m: self.msgq.put(("cvlog", m)))
                self.msgq.put(("convert_done", res))
            except Exception as e:  # noqa: BLE001
                self.msgq.put(("convert_done", e))
        self.convert_thread = threading.Thread(target=work, daemon=True, name="convert")
        self.convert_thread.start()

    def _cancel_convert(self):
        if self.convert_cancel:
            self.convert_cancel.set()
            self._log(self.cv_log, "Cancelling ...")

    def _update_convert_stats(self):
        p = self.convert_progress
        if not self.convert_started or not p:
            return
        el = time.time() - self.convert_started
        total, done = p.get("total", 0), p.get("done", 0)
        self.cv_bar.configure(maximum=max(total, 1), value=done)
        parsed = p.get("parsed", 0) + p.get("errors", 0)
        rate = parsed / el if el > 0 else 0
        left = (total - done) / rate if rate > 0 else None
        self.cv_status.set(f"{done:,}/{total:,} PDFs  |  read now: {p.get('parsed', 0):,}  from cache: "
                           f"{p.get('cached', 0):,}  errors: {p.get('errors', 0):,}  |  {rate:.1f} PDF/s  |  "
                           f"elapsed {fmt_duration(el)}  |  left {fmt_duration(left) if left is not None else '-'}")

    def _convert_finished(self, res):
        self._update_convert_stats()
        self.btn_convert.configure(state="normal")
        self.btn_cancel_conv.configure(state="disabled")
        started, self.convert_started = self.convert_started, None
        title = f"PDFs in {self.conv_pdf_var.get().strip() or self.data_dir / 'pdfs'}"
        self._db_changed()
        if isinstance(res, Exception):
            self.db.add_run("Convert", title, "Error", {"error": str(res)}, started=started)
            self._log(self.cv_log, f"ERROR: {res}")
            messagebox.showerror(APP_NAME, f"Conversion failed:\n{res}")
            return
        if res is None:
            self.db.add_run("Convert", title, "Cancelled", started=started)
            self.cv_status.set("Cancelled.")
            return
        self.db.add_run("Convert", title, "Finished", {k: res[k] for k in ("total", "parsed", "cached", "errors",
                                                                          "seconds", "files", "rows")},
                        started=started)
        rows = ", ".join(f"{k}: {v:,}" for k, v in res["rows"].items() if v)
        self._log(self.cv_log, f"Finished in {fmt_duration(res['seconds'])}. {rows}")
        for f in res["files"]:
            self._log(self.cv_log, f"Saved: {f}")
        if res["errors"]:
            self._log(self.cv_log, f"{res['errors']} PDF(s) could not be read - see the 'Errors' sheet.")
        self.cv_status.set(f"Done in {fmt_duration(res['seconds'])} - {res['total']:,} PDFs "
                           f"({res['cached']:,} from cache, {res['parsed']:,} read, {res['errors']:,} errors).")
        if res["files"] and messagebox.askyesno(APP_NAME, "Conversion finished. Open the output file now?"):
            open_path(res["files"][0])

    # ------------------------------------------------------------- tables
    def _make_tree(self, parent, columns, height=8):
        """Treeview with scrollbars and click-to-sort headings. columns: [(key, title, width, anchor)]"""
        frame = ttk.Frame(parent)
        tree = ttk.Treeview(frame, columns=[c[0] for c in columns], show="headings", height=height)
        for key, title, width, anchor in columns:
            tree.heading(key, text=title, command=lambda k=key: self._sort_tree(tree, k))
            tree.column(key, width=px(width), minwidth=px(40), anchor=anchor, stretch=anchor == "w")
        ys = ttk.Scrollbar(frame, orient="vertical", command=tree.yview)
        xs = ttk.Scrollbar(frame, orient="horizontal", command=tree.xview)
        tree.configure(yscrollcommand=ys.set, xscrollcommand=xs.set)
        tree.grid(row=0, column=0, sticky="nsew")
        ys.grid(row=0, column=1, sticky="ns")
        xs.grid(row=1, column=0, sticky="ew")
        frame.rowconfigure(0, weight=1)
        frame.columnconfigure(0, weight=1)
        return frame, tree

    def _sort_tree(self, tree, key):
        """Sort by a column (click again to reverse); the totals row stays on top."""
        desc = self._sort_state.get(str(tree)) == (key, False)
        items = [i for i in tree.get_children("") if "total" not in tree.item(i, "tags")]
        items.sort(key=lambda i: _sort_key(tree.set(i, key)), reverse=desc)
        for i in items:
            tree.move(i, "", "end")
        self._sort_state[str(tree)] = (key, desc)

    # ---------------------------------------------------------- database tab
    def _build_db_tab(self):
        t = self.tab_db.inner
        t.columnconfigure(0, weight=1)
        t.rowconfigure(1, weight=1)

        # --- totals
        top = ttk.LabelFrame(t, text="What is in the database  (every contract counted once, by contract number)",
                             padding=8)
        top.grid(row=0, column=0, sticky="ew")
        tiles = ttk.Frame(top)
        tiles.pack(fill="x")
        self.db_tiles = {}
        for i, (key, title) in enumerate((("found", "Unique contracts"), ("downloaded", "PDFs downloaded"),
                                          ("read", "Read into database"), ("pending", "Waiting to be read"),
                                          ("unreadable", "Unreadable PDFs"), ("failed", "Download failed"),
                                          ("missing", "No PDF yet"), ("percent", "Complete"))):
            tiles.columnconfigure(i, weight=1)
            ttk.Label(tiles, text=title, style="Hint.TLabel").grid(row=0, column=i, sticky="w")
            var = tk.StringVar(value="-")
            ttk.Label(tiles, textvariable=var, style="Tile.TLabel").grid(row=1, column=i, sticky="w")
            self.db_tiles[key] = var
        info = ttk.Frame(top)
        info.pack(fill="x", pady=(6, 0))
        self.db_dupe_info = tk.StringVar(value="")
        ttk.Label(info, textvariable=self.db_dupe_info, style="Hint.TLabel", wraplength=px(900)).pack(side="left")
        ttk.Button(info, text="Refresh", command=self._db_refresh).pack(side="right")
        self.db_status = tk.StringVar(value="Open this tab to load the database.")
        ttk.Label(info, textvariable=self.db_status, style="Hint.TLabel").pack(side="right", padx=8)

        panes = ttk.Panedwindow(t, orient="vertical")
        panes.grid(row=1, column=0, sticky="nsew", pady=(6, 0))

        # --- progress per category
        prog = ttk.LabelFrame(panes, text="Progress by category  (double-click a category to list its contracts)",
                              padding=6)
        panes.add(prog, weight=1)
        bar = ttk.Frame(prog)
        bar.pack(fill="x", pady=(0, 4))
        ttk.Label(bar, text="Find category").pack(side="left")
        self.prog_filter = tk.StringVar()
        self.prog_filter.trace_add("write", lambda *a: self._fill_progress())
        ttk.Entry(bar, textvariable=self.prog_filter, width=30).pack(side="left", padx=4)
        self.prog_hide_done = tk.BooleanVar(value=False)
        ttk.Checkbutton(bar, text="Hide completed categories", variable=self.prog_hide_done,
                        command=self._fill_progress).pack(side="left", padx=8)
        ttk.Button(bar, text="Download progress (Excel / CSV) ...", command=self._export_progress).pack(side="right")
        cols = [("category", "Category / search", 300, "w"), ("found", "Found", 70, "e"),
                ("downloaded", "PDFs", 70, "e"), ("read", "Read into DB", 85, "e"),
                ("pending", "Not read yet", 80, "e"), ("unreadable", "Unreadable", 75, "e"),
                ("failed", "Dl failed", 65, "e"), ("missing", "No PDF yet", 75, "e"),
                ("bar", "Progress (█ read  ▒ downloaded)", 190, "w"), ("shared", "Also in other cats", 105, "e"),
                ("last_added", "Last added", 115, "w")]
        frame, self.prog_tree = self._make_tree(prog, cols, height=7)
        frame.pack(fill="both", expand=True)
        self.prog_tree.tag_configure("total", font=("Segoe UI", 9, "bold"), background="#eef3fb")
        self.prog_tree.tag_configure("done", foreground="#1e7b3a")
        self.prog_tree.tag_configure("partial", foreground="#8a6100")
        self.prog_tree.tag_configure("none", foreground="#b3261e")
        self.prog_tree.bind("<Double-Button-1>", self._progress_open)
        ttk.Label(prog, text="Found = contracts in the search results  |  PDFs = downloaded  |  Read into DB = PDF "
                             "data saved in the database.  PDFs + Dl failed + No PDF yet = Found.",
                  style="Hint.TLabel").pack(anchor="w", pady=(2, 0))

        # --- search
        srch = ttk.LabelFrame(panes, text="Search the database", padding=6)
        panes.add(srch, weight=2)
        keep_split(panes, 1 / 3)
        f1 = ttk.Frame(srch)
        f1.pack(fill="x")
        ttk.Label(f1, text="Search").pack(side="left")
        self.q_text = tk.StringVar()
        self.q_text.trace_add("write", lambda *a: self._schedule_db_search())
        q = ttk.Entry(f1, textvariable=self.q_text, width=34)
        q.pack(side="left", padx=4)
        q.bind("<Return>", lambda e: self._db_search())
        ttk.Label(f1, text="Category").pack(side="left", padx=(6, 0))
        self.q_cat = tk.StringVar(value=ANY_CATEGORY)
        self.q_cat_box = ttk.Combobox(f1, textvariable=self.q_cat, width=34, state="readonly", values=[ANY_CATEGORY])
        self.q_cat_box.pack(side="left", padx=4)
        self.q_cat_box.bind("<<ComboboxSelected>>", lambda e: self._db_search())
        ttk.Label(f1, text="Status").pack(side="left", padx=(6, 0))
        self.q_status = tk.StringVar(value=ANY_STATUS)
        sb = ttk.Combobox(f1, textvariable=self.q_status, width=26, state="readonly",
                          values=[ANY_STATUS] + db_report.STATUSES)
        sb.pack(side="left", padx=4)
        sb.bind("<<ComboboxSelected>>", lambda e: self._db_search())

        f2 = ttk.Frame(srch)
        f2.pack(fill="x", pady=(4, 0))
        ttk.Label(f2, text="Contract date from").pack(side="left")
        self.q_from = tk.StringVar()
        self.q_to = tk.StringVar()
        e1 = ttk.Entry(f2, textvariable=self.q_from, width=12)
        e1.pack(side="left", padx=4)
        ttk.Label(f2, text="to").pack(side="left")
        e2 = ttk.Entry(f2, textvariable=self.q_to, width=12)
        e2.pack(side="left", padx=4)
        for e in (e1, e2):
            e.bind("<Return>", lambda ev: self._db_search())
        ttk.Label(f2, text="(DD-MM-YYYY, optional)", style="Hint.TLabel").pack(side="left")
        ttk.Button(f2, text="Search", command=self._db_search).pack(side="left", padx=(10, 2))
        ttk.Button(f2, text="Clear filters", command=self._db_clear_search).pack(side="left", padx=2)
        self.db_only_var = tk.StringVar(value="")
        self.db_only_frame = ttk.Frame(f2)
        ttk.Label(self.db_only_frame, textvariable=self.db_only_var, foreground="#1a5fb4").pack(side="left")
        ttk.Button(self.db_only_frame, text="Show all", command=self._db_clear_only).pack(side="left", padx=4)
        ttk.Label(srch, text="Searches contract no., bid no., ministry, department, organisation, products, brands, "
                            "seller, buyer email and categories. All words must match.",
                  style="Hint.TLabel").pack(anchor="w", pady=(2, 4))

        cols = [("no", "Contract No", 160, "w"), ("date", "Date", 85, "w"), ("categories", "Categories", 200, "w"),
                ("status", "In database", 150, "w"), ("org", "Organisation / Department", 220, "w"),
                ("products", "Products", 220, "w"), ("value", "Value (INR)", 100, "e"), ("seller", "Seller", 180, "w")]
        frame, self.res_tree = self._make_tree(srch, cols, height=8)
        frame.pack(fill="both", expand=True)
        self.res_tree.bind("<Double-Button-1>", lambda e: self._open_result_pdf())

        f3 = ttk.Frame(srch)
        f3.pack(fill="x", pady=(4, 0))
        self.db_count = tk.StringVar(value="")
        ttk.Label(f3, textvariable=self.db_count, style="StatV.TLabel").pack(side="left")
        ttk.Button(f3, text="Download (Excel / CSV) ...", style="Accent.TButton", command=self._db_export
                   ).pack(side="right")
        ttk.Button(f3, text="Open PDF", command=self._open_result_pdf).pack(side="right", padx=6)
        ttk.Label(srch, text="Download the search results or the whole database as Excel or CSV - one row per "
                            "contract or full details (every sheet), all or chosen categories, optionally split "
                            "by category. Each contract appears once.",
                  style="Hint.TLabel", wraplength=px(1000)).pack(anchor="w", pady=(2, 0))

    def _on_tab_changed(self, event=None):
        self.root.after_idle(self._relayout_tabs)
        current = self.nb.select()
        if current == str(self.tab_db) and (self._db_stale or self.db_rows is None):
            self._db_refresh()
        elif current == str(self.tab_history):
            self._history_load()

    def _db_changed(self):
        """New data arrived (scrape/convert finished): reload the Database tab if it is open."""
        self._db_stale = True
        current = self.nb.select()
        if current == str(self.tab_db):
            self._db_refresh()
        elif current == str(self.tab_history):
            self._history_load()

    def _with_db_rows(self, callback):
        """Run callback(rows) with up-to-date contract rows, loading the database first if needed."""
        if self.db_rows is not None and not self._db_stale:
            callback(self.db_rows)
            return
        self._db_waiters.append(callback)
        self._db_refresh()

    def _db_refresh(self):
        if self._db_loading:
            return
        self._db_loading = True
        self._db_last_load = time.time()
        self.db_status.set("Loading database ...")

        def work():
            try:
                self.db.backfill_parsed_index()
                rows = db_report.prepare(self.db.contract_rows(PARSER_VERSION))
                progress = db_report.category_progress(rows)
                self.msgq.put(("call", lambda: self._db_loaded(rows, progress, None)))
            except Exception as e:  # noqa: BLE001
                self.msgq.put(("call", lambda err=e: self._db_loaded(None, None, err)))
        threading.Thread(target=work, daemon=True, name="db-load").start()

    def _db_loaded(self, rows, progress, error):
        self._db_loading = False
        waiters, self._db_waiters = self._db_waiters, []
        if error is not None:
            self.db_status.set(f"Could not load the database: {error}")
            if waiters:
                messagebox.showerror(APP_NAME, f"Could not load the database:\n{error}")
            return
        self._db_stale = False
        self.db_rows = rows
        self.db_progress = progress
        cats, totals = progress
        for key, var in self.db_tiles.items():
            v = totals[key]
            var.set(f"{v:.1f}%" if key == "percent" else f"{v:,}")
        if totals["shared"]:
            self.db_dupe_info.set(
                f"{totals['shared']:,} contract(s) were found in more than one category. They are listed under "
                f"each category below (rows add up to {totals['category_sum']:,}) but counted only once in the "
                f"totals above and in every Excel download.")
        else:
            self.db_dupe_info.set("No contract is listed under more than one category. Re-scraping never adds a "
                                  "contract twice - each contract number is stored once.")
        self.db_status.set(f"Updated {time.strftime('%H:%M:%S')}")
        names = sorted(c["category"] for c in cats)
        self.q_cat_box.configure(values=[ANY_CATEGORY] + names)
        if self.q_cat.get() not in names:
            self.q_cat.set(ANY_CATEGORY)
        self._fill_progress()
        self._db_search()
        for callback in waiters:  # e.g. a Download dialog: run as its own event, not inside this handler
            self.root.after_idle(callback, rows)

    def _fill_progress(self):
        cats, totals = self.db_progress
        tree = self.prog_tree
        tree.delete(*tree.get_children())
        if totals is None:
            return
        words = self.prog_filter.get().lower().split()
        keys = [c for c in tree["columns"]]

        def values(r):
            return [db_report.bar(r["percent"], r["downloaded"] / r["found"] * 100 if r["found"] else 0)
                    if k == "bar" else
                    (f"{r[k]:,}" if isinstance(r[k], int) else r[k]) for k in keys]
        tree.insert("", "end", values=values(totals), tags=("total",))
        for r in cats:
            if words and not all(w in r["category"].lower() for w in words):
                continue
            if self.prog_hide_done.get() and r["found"] and r["read"] == r["found"]:
                continue
            tag = "done" if r["read"] == r["found"] else ("partial" if r["read"] else "none")
            tree.insert("", "end", values=values(r), tags=(tag,))
        sort = self._sort_state.get(str(tree))
        if sort:  # keep the user's sort order after a refresh
            self._sort_state[str(tree)] = (sort[0], not sort[1])
            self._sort_tree(tree, sort[0])

    def _progress_open(self, event=None):
        sel = self.prog_tree.focus()
        if not sel or "total" in self.prog_tree.item(sel, "tags"):
            self.q_cat.set(ANY_CATEGORY)
        else:
            self.q_cat.set(self.prog_tree.set(sel, "category"))
        self._db_search()

    def _export_progress(self):
        cats, totals = self.db_progress
        if totals is None:
            messagebox.showinfo(APP_NAME, "The database is still loading.")
            return
        shown = {self.prog_tree.set(i, "category") for i in self.prog_tree.get_children()}
        shown_cats = [c for c in cats if c["category"] in shown]
        choice = DownloadDialog(self.root, "Download category progress", [
            ("all", f"All categories ({len(cats):,})", True),
            ("shown", f"Only the categories shown in the table ({len(shown_cats):,})", bool(shown_cats))],
            detail=False).result
        if not choice:
            return
        picked = cats if choice["scope"] == "all" else shown_cats
        base = self._ask_save("gem_category_progress", choice["fmt"])
        if base:
            self._run_export(base, f"Category progress ({choice['fmt'].upper()}, {len(picked):,} categories)",
                             len(picked), lambda: downloads.write_tables(
                                 base, [("Category Progress", db_report.PROGRESS_COLUMNS, [totals] + picked)],
                                 choice["fmt"]))

    # --- search
    def _schedule_db_search(self):
        if self._db_search_job:
            self.root.after_cancel(self._db_search_job)
        self._db_search_job = self.root.after(300, self._db_search)

    def _db_filters(self):
        f, t = self.q_from.get().strip(), self.q_to.get().strip()
        df, dt = (parse_dmy(f) if f else None), (parse_dmy(t) if t else None)
        if (f and not df) or (t and not dt):
            return None
        cat = self.q_cat.get()
        status = self.q_status.get()
        return {"text": self.q_text.get(), "category": "" if cat == ANY_CATEGORY else cat,
                "status": "" if status == ANY_STATUS else status, "date_from": df, "date_to": dt,
                "only": self.db_only[1] if self.db_only else None}

    def _db_search(self):
        self._db_search_job = None
        if self.db_rows is None:
            return
        filters = self._db_filters()
        if filters is None:
            self.db_count.set("Dates must be DD-MM-YYYY, e.g. 05-10-2026.")
            return
        self.db_results = db_report.search(self.db_rows, **filters)
        tree = self.res_tree
        tree.delete(*tree.get_children())
        for i, r in enumerate(self.db_results[:DB_SHOW_MAX]):
            value = f"{r['value']:,.2f}" if isinstance(r["value"], (int, float)) else r["value"]
            tree.insert("", "end", iid=str(i), values=(r["no"], r["date"], r["categories"], r["status"],
                                                       r["org"] or r["department"], r["products"], value, r["seller"]))
        n = len(self.db_results)
        shown = f" (showing the newest {DB_SHOW_MAX:,} - Excel gets all)" if n > DB_SHOW_MAX else ""
        self.db_count.set(f"{n:,} of {len(self.db_rows):,} contracts match{shown}")

    def _db_clear_search(self):
        for v in (self.q_text, self.q_from, self.q_to):
            v.set("")
        self.q_cat.set(ANY_CATEGORY)
        self.q_status.set(ANY_STATUS)
        self._db_clear_only()

    def _db_clear_only(self):
        self.db_only = None
        self.db_only_frame.pack_forget()
        self._db_search()

    def _db_show_only(self, title, contract_nos):
        """Limit the search to one History run's contracts."""
        self.db_only = (title, set(contract_nos))
        self.db_only_var.set(f"Only: {title} ({len(contract_nos):,} contracts)")
        self.db_only_frame.pack(side="left", padx=(12, 0))
        self.q_cat.set(ANY_CATEGORY)
        self.nb.select(self.tab_db)
        if self.db_rows is not None and not self._db_stale:
            self._db_search()

    def _open_result_pdf(self):
        sel = self.res_tree.focus()
        if not sel:
            return
        r = self.db_results[int(sel)]
        if r["pdf"] and Path(r["pdf"]).exists():
            open_path(r["pdf"])
        else:
            messagebox.showinfo(APP_NAME, f"{r['no']}: the PDF is not downloaded yet ({r['status']}).")

    def _db_export(self):
        self._with_db_rows(self._db_export_dialog)

    def _db_export_dialog(self, rows):
        results = list(self.db_results)
        f = self._db_filters() or {}
        parts = [f'"{f["text"].strip()}"' if f.get("text", "").strip() else "", f.get("category", ""),
                 f.get("status", ""), self.db_only[0] if self.db_only else ""]
        search_desc = ", ".join(p for p in parts if p) or "no filters"
        choice = DownloadDialog(self.root, "Download contracts", [
            ("results", f"Current search results ({len(results):,} contracts - {search_desc})", bool(results)),
            ("all", f"Everything in the database ({len(rows):,} contracts)", bool(rows))],
            categories=self._category_counts(rows)).result
        if choice:
            picked, what = (results, f"search: {search_desc}") if choice["scope"] == "results" else (rows, "whole database")
            self._download_contracts(picked, choice, what, "gem_contracts")

    @staticmethod
    def _category_counts(rows):
        counts = {}
        for r in rows:
            for s in r["sources"]:
                counts[s] = counts.get(s, 0) + 1
        return sorted(counts.items(), key=lambda kv: kv[0].lower())

    def _download_contracts(self, rows, choice, what, name):
        """Download contracts chosen in a DownloadDialog (Database and History tabs)."""
        rows = downloads.filter_categories(rows, choice["categories"])
        if not rows:
            messagebox.showinfo(APP_NAME, "No contracts match these choices.")
            return
        base = self._ask_save(name, choice["fmt"])
        if not base:
            return
        fmt, full, split, cats = choice["fmt"], choice["full"], choice["split"], choice["categories"]
        selection = self._selection()
        title = (f"{'Full details' if full else 'Contract list'} ({fmt.upper()}"
                 f"{', split by category' if split else ''}) - {len(rows):,} contracts - {what}"
                 + (f" - categories: {', '.join(cats[:3])}{' ...' if len(cats) > 3 else ''}" if cats else ""))
        self._run_export(base, title, len(rows), lambda: downloads.download_contracts(
            self.db, rows, base, fmt, full, split, cats, selection))

    def _ask_save(self, name, fmt):
        """Ask where to save; returns the path without extension (several files go in a folder of that name)."""
        self.export_dir.mkdir(parents=True, exist_ok=True)
        ext = "." + fmt
        path = filedialog.asksaveasfilename(
            title="Save as", initialdir=str(self.export_dir), defaultextension=ext,
            initialfile=f"{name}_{time.strftime('%Y%m%d_%H%M')}{ext}",
            filetypes=[("Excel workbook", "*.xlsx")] if fmt == "xlsx" else [("CSV file", "*.csv")])
        return str(Path(path).with_suffix("")) if path else None

    def _run_export(self, base, title, count, job):
        """Write files in the background (job returns the written paths), record it in History, offer to open."""
        self.db_status.set(f"Writing {Path(base).name} ...")
        started = time.time()

        def work():
            try:
                files = [str(f) for f in job()]
                self.db.add_run("Download", title, "Saved", {"contracts": count, "files": files,
                                                                   "seconds": time.time() - started},
                                started=started)
                self.msgq.put(("call", lambda: self._export_done(files, None)))
            except Exception as e:  # noqa: BLE001
                self.msgq.put(("call", lambda err=e: self._export_done([], err)))
        threading.Thread(target=work, daemon=True, name="download").start()

    def _export_done(self, files, error):
        if error is not None:
            self.db_status.set("Download failed.")
            messagebox.showerror(APP_NAME, f"Could not save the file:\n{error}")
            return
        if self.nb.select() == str(self.tab_history):
            self._history_load()
        if len(files) == 1:
            self.db_status.set(f"Saved {Path(files[0]).name}")
            if messagebox.askyesno(APP_NAME, f"Saved:\n{files[0]}\n\nOpen it now?"):
                open_path(files[0])
            return
        folder = Path(os.path.commonpath(files))
        self.db_status.set(f"Saved {len(files)} files")
        if messagebox.askyesno(APP_NAME, f"Saved {len(files)} files in:\n{folder}\n\nOpen the folder now?"):
            open_path(folder)

    # ----------------------------------------------------------- history tab
    def _build_history_tab(self):
        t = self.tab_history.inner
        t.columnconfigure(0, weight=1)
        t.rowconfigure(1, weight=1)
        flt = ttk.LabelFrame(t, text="Filters", padding=6)
        flt.grid(row=0, column=0, sticky="ew")
        refill = lambda *a: self._history_fill()  # noqa: E731

        r1 = ttk.Frame(flt)
        r1.pack(fill="x")
        self.h_kind = tk.StringVar(value=HISTORY_ANY)
        self.h_status = tk.StringVar(value=HISTORY_ANY)
        self.h_cat = tk.StringVar(value=ANY_CATEGORY)
        for label, var, values, width in (
                ("Type", self.h_kind, [HISTORY_ANY, "Scrape", "Convert", "Download"], 11),
                ("Status", self.h_status, [HISTORY_ANY, "Finished", "Stopped", "Saved", "Running", "Interrupted",
                                           "Cancelled", "Error"], 11),
                ("Category", self.h_cat, [ANY_CATEGORY], 30)):
            ttk.Label(r1, text=label).pack(side="left", padx=(0 if label == "Type" else 8, 0))
            box = ttk.Combobox(r1, textvariable=var, values=values, width=width, state="readonly")
            box.pack(side="left", padx=4)
            box.bind("<<ComboboxSelected>>", refill)
            if label == "Category":
                self.h_cat_box = box
        ttk.Button(r1, text="Refresh", command=self._history_load).pack(side="right")
        ttk.Button(r1, text="Clear filters", command=self._history_clear_filters).pack(side="right", padx=4)
        ttk.Label(r1, text="Search").pack(side="left", padx=(8, 0))
        self.h_text = tk.StringVar()
        self.h_text.trace_add("write", refill)
        ttk.Entry(r1, textvariable=self.h_text, width=16).pack(side="left", padx=4, fill="x", expand=True)

        r2 = ttk.Frame(flt)
        r2.pack(fill="x", pady=(4, 0))
        ttk.Label(r2, text="Run date from").pack(side="left")
        self.h_from = tk.StringVar()
        self.h_to = tk.StringVar()
        ttk.Entry(r2, textvariable=self.h_from, width=11).pack(side="left", padx=4)
        ttk.Label(r2, text="to").pack(side="left")
        ttk.Entry(r2, textvariable=self.h_to, width=11).pack(side="left", padx=4)
        for v in (self.h_from, self.h_to):
            v.trace_add("write", refill)
        for txt, days in (("Today", 1), ("7 days", 7), ("30 days", 30), ("This month", "month"), ("All time", None)):
            ttk.Button(r2, text=txt, width=9, command=lambda d=days: self._history_quick_date(d)).pack(side="left",
                                                                                                    padx=1)
        self.h_new_only = tk.BooleanVar(value=False)
        ttk.Checkbutton(r2, text="Only runs that added new contracts", variable=self.h_new_only, command=refill
                        ).pack(side="left", padx=(10, 0))

        self.h_summary = tk.StringVar(value="")
        ttk.Label(t, textvariable=self.h_summary, style="Hint.TLabel").grid(row=3, column=0, sticky="w")

        panes = ttk.Panedwindow(t, orient="vertical")
        panes.grid(row=1, column=0, sticky="nsew", pady=6)
        top = ttk.Frame(panes)
        panes.add(top, weight=3)
        act = ttk.Frame(top)
        act.pack(fill="x", side="bottom", pady=(4, 0))
        self.h_sel_info = tk.StringVar(value="")
        ttk.Label(act, textvariable=self.h_sel_info, style="StatV.TLabel").pack(side="left")
        ttk.Button(act, text="Select all shown", command=self._history_select_all).pack(side="left", padx=8)
        ttk.Button(act, text="Clear selection", command=lambda: self.h_tree.selection_set(())).pack(side="left")
        ttk.Button(act, text="Download contracts (Excel / CSV) ...", style="Accent.TButton",
                   command=self._history_download_contracts).pack(side="right")
        ttk.Button(act, text="Download history list ...", command=self._history_export).pack(side="right", padx=6)
        cols = [("id", "#", 45, "e"), ("started", "Started", 120, "w"), ("kind", "Type", 95, "w"),
                ("title", "Search / details", 330, "w"), ("status", "Status", 80, "w"),
                ("duration", "Duration", 80, "e"), ("found", "Found", 70, "e"), ("new", "New", 65, "e"),
                ("downloaded", "PDFs dl", 65, "e"), ("skipped", "Already had", 80, "e"),
                ("failed", "Failed", 60, "e"), ("read", "Read", 65, "e"), ("contracts", "Unique contracts", 100, "e")]
        frame, self.h_tree = self._make_tree(top, cols, height=12)
        frame.pack(fill="both", expand=True)
        self.h_tree.tag_configure("bad", foreground="#b3261e")
        self.h_tree.bind("<<TreeviewSelect>>", lambda e: self._history_details())
        self.h_tree.bind("<Control-a>", lambda e: self._history_select_all())
        self.h_tree.bind("<Double-Button-1>", lambda e: self._history_show_contracts())

        det = ttk.LabelFrame(panes, text="Details", padding=6)
        panes.add(det, weight=1)
        keep_split(panes, 3 / 4)
        btns = ttk.Frame(det)
        btns.pack(fill="x")
        ttk.Button(btns, text="Show / download its contracts", command=self._history_show_contracts
                   ).pack(side="left")
        ttk.Button(btns, text="Run this search again", command=self._history_repeat).pack(side="left", padx=6)
        ttk.Button(btns, text="Open file", command=self._history_open_file).pack(side="left")
        ttk.Button(btns, text="Delete from history", command=self._history_delete).pack(side="right")
        self.h_detail = ScrolledText(det, height=6, font=("Consolas", 9), state="disabled", wrap="word")
        self.h_detail.pack(fill="both", expand=True, pady=(4, 0))

    def _history_load(self):
        try:
            self.history = self.db.runs()
        except Exception as e:  # noqa: BLE001
            self.h_summary.set(f"Could not load history: {e}")
            return
        for run in self.history:
            if run["kind"] == "Excel download":  # older name, before CSV downloads existed
                run["kind"] = "Download"
            run["categories"] = self._run_categories(run)
        cats = sorted({c for run in self.history for c in run["categories"]}, key=str.lower)
        self.h_cat_box.configure(values=[ANY_CATEGORY] + cats)
        if self.h_cat.get() not in cats:
            self.h_cat.set(ANY_CATEGORY)
        self._history_fill()

    @staticmethod
    def _run_categories(run):
        """Categories a scrape searched, named as in the Database tab."""
        cfg = run["config"] or {}
        if run["kind"] != "Scrape" or not cfg:
            return []
        if cfg.get("mode") == "category":
            return [label for _, label in cfg.get("categories") or []] or [ALL_CATEGORIES]
        if cfg.get("mode") == "ministry":
            return [source_label(ScrapeConfig(**{k: cfg.get(k) or "" for k in (
                "mode", "buyer_entity", "ministry", "state", "department", "organization")}), "")]
        return [NUMBER_SOURCE]

    @staticmethod
    def _history_values(run):
        s, cfg = run["stats"], run["config"] or {}
        dur = fmt_duration(run["finished"] - run["started"]) if run["finished"] else "running"
        num = lambda v: f"{v:,}" if isinstance(v, (int, float)) else ""  # noqa: E731
        if run["kind"] == "Convert":
            found, read, failed = s.get("total"), s.get("parsed"), s.get("errors")
        elif run["kind"] == "Download":
            found, read, failed = s.get("contracts"), None, None
        else:
            found, read, failed = s.get("found"), s.get("read"), s.get("failed")
        dates = ""
        if cfg.get("from_date") and cfg.get("to_date"):
            dates = " to ".join(date.fromisoformat(cfg[k]).strftime("%d-%m-%Y") for k in ("from_date", "to_date"))
        return {
            "id": run["id"], "started": time.strftime("%d-%m-%Y %H:%M", time.localtime(run["started"])),
            "kind": run["kind"], "title": run["title"] or "", "status": run["status"] or "", "duration": dur,
            "found": num(found), "new": num(s.get("new_results")), "downloaded": num(s.get("downloaded")),
            "skipped": num(s.get("skipped")), "failed": num(failed), "read": num(read),
            "contracts": num(run["contracts"]) if run["kind"] == "Scrape" else "",
            "categories": " | ".join(run.get("categories") or []), "date_range": dates,
            "file": " | ".join(s.get("files") or ([s["file"]] if s.get("file") else [])),
        }

    def _history_quick_date(self, days):
        today = date.today()
        if days is None:
            start = end = None
        elif days == "month":
            start, end = today.replace(day=1), today
        else:
            start, end = today - timedelta(days=days - 1), today
        self.h_from.set(start.strftime("%d-%m-%Y") if start else "")
        self.h_to.set(end.strftime("%d-%m-%Y") if end else "")

    def _history_clear_filters(self):
        self.h_kind.set(HISTORY_ANY)
        self.h_status.set(HISTORY_ANY)
        self.h_cat.set(ANY_CATEGORY)
        self.h_new_only.set(False)
        for v in (self.h_text, self.h_from, self.h_to):
            v.set("")
        self._history_fill()

    def _history_fill(self):
        tree = self.h_tree
        tree.delete(*tree.get_children())
        kind, status, cat = self.h_kind.get(), self.h_status.get(), self.h_cat.get()
        words = self.h_text.get().lower().split()
        f, t = self.h_from.get().strip(), self.h_to.get().strip()
        d_from, d_to = (parse_dmy(f) if f else None), (parse_dmy(t) if t else None)
        bad_date = (f and not d_from) or (t and not d_to)
        self.h_shown = []
        for run in self.history:
            if kind != HISTORY_ANY and run["kind"] != kind:
                continue
            if status != HISTORY_ANY and run["status"] != status:
                continue
            if cat != ANY_CATEGORY and cat not in run["categories"]:
                continue
            if self.h_new_only.get() and not run["stats"].get("new_results"):
                continue
            day = date.fromtimestamp(run["started"])
            if (d_from and day < d_from) or (d_to and day > d_to):
                continue
            v = self._history_values(run)
            if words and not all(w in " ".join(map(str, v.values())).lower() for w in words):
                continue
            tag = ("bad",) if run["status"] in ("Error", "Interrupted") else ()
            tree.insert("", "end", iid=str(run["id"]), values=[v[k] for k in tree["columns"]], tags=tag)
            self.h_shown.append(run)
        scrapes = [r for r in self.h_shown if r["kind"] == "Scrape"]
        self.h_summary.set(
            ("Dates must be DD-MM-YYYY - date filter ignored.  |  " if bad_date else "")
            + f"{len(self.h_shown):,} of {len(self.history):,} entries shown  |  {len(scrapes):,} scrapes shown, "
              f"{sum(r['stats'].get('new_results') or 0 for r in scrapes):,} new contracts added by them "
              f"(a contract found again later is never added or counted twice)")
        self._history_details()

    def _history_select_all(self):
        self.h_tree.selection_set(self.h_tree.get_children())
        return "break"

    def _history_selected_runs(self):
        ids = set(self.h_tree.selection())
        return [r for r in self.h_shown if str(r["id"]) in ids]

    def _history_download_contracts(self):
        selected = [r for r in self._history_selected_runs() if r["kind"] == "Scrape"]
        shown = [r for r in self.h_shown if r["kind"] == "Scrape"]
        if not shown:
            messagebox.showinfo(APP_NAME, "No scrape runs are shown. Contracts can be downloaded from scrape runs "
                                          "(change the filters, or scrape first).")
            return
        sel_ids = set().union(*(self.db.run_contract_ids(r["id"]) for r in selected)) if selected else set()
        shown_ids = set().union(*(self.db.run_contract_ids(r["id"]) for r in shown))

        def ask(rows):
            sel_rows = [r for r in rows if r["no"] in sel_ids]
            shown_rows = [r for r in rows if r["no"] in shown_ids]
            choice = DownloadDialog(self.root, "Download contracts from History", [
                ("selected", f"Selected runs ({len(selected):,} runs, {len(sel_rows):,} unique contracts)",
                 bool(sel_rows)),
                ("shown", f"All runs shown ({len(shown):,} runs, {len(shown_rows):,} unique contracts)",
                 bool(shown_rows))],
                categories=self._category_counts(shown_rows),
                note="A contract found by several runs is written once.").result
            if not choice:
                return
            runs = selected if choice["scope"] == "selected" else shown
            what = "runs " + ", ".join(f"#{r['id']}" for r in runs[:8]) + (" ..." if len(runs) > 8 else "")
            self._download_contracts(sel_rows if choice["scope"] == "selected" else shown_rows, choice, what,
                                     "gem_history_contracts")
        if not shown_ids:
            messagebox.showinfo(APP_NAME, "These runs have no saved contracts (they found none, or were run with "
                                          "an older version of the app).")
            return
        self._with_db_rows(ask)

    def _history_selected(self):
        """The entry the Details pane and single-run buttons act on: the focused one if selected, else the first
        selected."""
        sel = self.h_tree.selection()
        focus = self.h_tree.focus()
        iid = focus if focus in sel else (sel[0] if sel else None)
        return next((r for r in self.history if str(r["id"]) == iid), None) if iid else None

    def _history_details(self):
        n = len(self.h_tree.selection())
        self.h_sel_info.set(f"{n:,} selected" if n else "Ctrl/Shift-click to select several entries")
        run = self._history_selected()
        lines = []
        if run:
            v = self._history_values(run)
            lines.append(f"#{run['id']}  {run['kind']}  -  {run['status']}")
            lines.append(f"{run['title']}")
            fin = time.strftime("%d-%m-%Y %H:%M:%S", time.localtime(run["finished"])) if run["finished"] else "-"
            lines.append(f"Started {v['started']}, finished {fin}, took {v['duration']}")
            s = dict(run["stats"])
            files = s.pop("files", None) or ([s.pop("file")] if s.get("file") else [])
            labels = {"queries_done": "searches done", "queries_total": "searches planned", "pages": "result pages",
                      "found": "contracts in results (incl. repeats)", "new_results": "new contracts added",
                      "downloaded": "PDFs downloaded", "skipped": "PDFs already had", "failed": "failed downloads",
                      "failed_queries": "failed searches", "read": "PDFs read into database",
                      "read_errors": "unreadable PDFs", "bytes": "bytes downloaded", "total": "PDFs",
                      "parsed": "PDFs read now", "cached": "PDFs from cache", "errors": "errors",
                      "contracts": "contracts"}
            def show(val):
                if isinstance(val, dict):
                    return ", ".join(f"{k} {n:,}" for k, n in val.items() if n)
                return f"{val:,}" if isinstance(val, int) else str(val)
            stats = [f"{labels.get(k, k)}: {show(val)}"
                     for k, val in s.items() if k not in ("elapsed", "seconds") and val not in (None, "", {})]
            if run["kind"] == "Scrape":
                stats.append(f"unique contracts saved for this run: {run['contracts']:,}")
            if stats:
                lines.append("  " + "  |  ".join(stats))
            for f in files:
                lines.append(f"File: {f}")
            if run["kind"] == "Scrape":
                lines.append("Double-click or 'Show / download its contracts' to list them in the Database tab.")
        box = self.h_detail
        box.configure(state="normal")
        box.delete("1.0", "end")
        box.insert("1.0", "\n".join(lines) if lines else "Select an entry to see its details.")
        box.configure(state="disabled")

    def _history_show_contracts(self):
        run = self._history_selected()
        if not run:
            return
        if run["kind"] != "Scrape":
            messagebox.showinfo(APP_NAME, "Only scrape runs have a list of contracts.")
            return
        ids = self.db.run_contract_ids(run["id"])
        if not ids:
            messagebox.showinfo(APP_NAME, "No contracts were saved for this run (it found none, or it was run "
                                          "with an older version of the app).")
            return
        self._db_show_only(f"run #{run['id']}", ids)

    def _history_repeat(self):
        run = self._history_selected()
        cfg = run and run["config"]
        if not run or run["kind"] != "Scrape" or not cfg:
            messagebox.showinfo(APP_NAME, "Select a scrape run first.")
            return
        mode = cfg.get("mode", "category")
        self.mode.set(mode)
        if mode == "number":
            self.num_var.set(cfg.get("number", ""))
        elif mode == "category":
            self.selected_cats = [tuple(c) for c in cfg.get("categories") or []]
            self._refresh_selected()
            self.cat_bno.set("")
        else:
            for key, var in (("buyer_entity", self.ent_var), ("ministry", self.min_var), ("state", self.state_var),
                             ("department", self.dept_var), ("organization", self.org_var)):
                var.set(cfg.get(key) or "")
        for key, var in (("from_date", self.from_var), ("to_date", self.to_var)):
            if cfg.get(key):
                var.set(date.fromisoformat(cfg[key]).strftime("%d-%m-%Y"))
        self.till_today.set(False)
        self._show_mode()
        self.nb.select(self.tab_scrape)
        self._log(self.sc_log, f"Loaded search from History #{run['id']}: {run['title']}. Press Start to run it "
                               "(PDFs you already have are skipped).")

    def _history_open_file(self):
        run = self._history_selected()
        if not run:
            return
        files = run["stats"].get("files") or ([run["stats"]["file"]] if run["stats"].get("file") else [])
        existing = [f for f in files if Path(f).exists()]
        if existing:
            open_path(existing[0])
        else:
            messagebox.showinfo(APP_NAME, "This entry has no saved file (or it was moved / deleted).")

    def _history_delete(self):
        sel = self.h_tree.selection()
        if not sel:
            return
        if messagebox.askyesno(APP_NAME, f"Delete {len(sel)} entr{'y' if len(sel) == 1 else 'ies'} from History?\n\n"
                                         "Only the history entry is removed - contracts, PDFs and Excel files stay."):
            self.db.delete_runs([int(i) for i in sel])
            self._history_load()

    def _history_export(self):
        selected = self._history_selected_runs()
        if not self.h_shown:
            messagebox.showinfo(APP_NAME, "No history entries are shown.")
            return
        choice = DownloadDialog(self.root, "Download history list", [
            ("selected", f"Selected entries ({len(selected):,})", bool(selected)),
            ("shown", f"All entries shown ({len(self.h_shown):,})", True),
            ("all", f"All history ({len(self.history):,}, ignoring the filters)", True)], detail=False).result
        if not choice:
            return
        runs = {"selected": selected, "shown": self.h_shown, "all": self.history}[choice["scope"]]
        base = self._ask_save("gem_history", choice["fmt"])
        if not base:
            return
        rows = [self._history_values(r) for r in runs]
        self._run_export(base, f"History list ({choice['fmt'].upper()}, {len(rows):,} entries)", len(rows),
                         lambda: downloads.write_tables(base, [("Run History", downloads.HISTORY_COLUMNS, rows)],
                                                        choice["fmt"]))

    # ---------------------------------------------------------------- help
    def _build_help_tab(self):
        t = self.tab_help.inner
        t.columnconfigure(0, weight=1)
        t.rowconfigure(1, weight=1)
        head = ttk.Frame(t)
        head.grid(row=0, column=0, columnspan=2, sticky="ew", pady=(0, 8))
        ttk.Label(head, text=f"{APP_NAME} {APP_VERSION}", font=("Segoe UI", 15, "bold")).pack(anchor="w")
        ttk.Label(head, text="Download GeM contract PDFs, read them into one database and export exactly the "
                             "data you need to Excel.", style="Hint.TLabel").pack(anchor="w")

        # --- the guide
        box = ScrolledText(t, font=("Segoe UI", 10), wrap="word", padx=14, pady=10, borderwidth=1, relief="solid",
                           cursor="arrow")
        box.grid(row=1, column=0, sticky="nsew")
        box.tag_configure("h", font=("Segoe UI", 12, "bold"), foreground="#1a5fb4", spacing1=14, spacing3=4)
        box.tag_configure("item", lmargin1=10, lmargin2=28, spacing1=2)
        box.tag_configure("term", font=("Segoe UI", 10, "bold"))
        box.tag_configure("note", foreground="#555555", lmargin1=10, lmargin2=10, spacing1=4)
        for i, (title, items) in enumerate(HELP_SECTIONS):
            box.mark_set(f"sec{i}", "end-1c")
            box.mark_gravity(f"sec{i}", "left")
            box.insert("end", title + "\n", "h")
            for n, item in enumerate(items, start=1):
                if item.startswith("NOTE "):
                    box.insert("end", item[5:] + "\n", "note")
                    continue
                bullet = f"{n}.  " if title == "Quick start" else "•  "
                term, sep, rest = item.partition(": ")
                if sep and len(term) < 40:  # "Term: explanation" -> bold term
                    box.insert("end", bullet, "item")
                    box.insert("end", term + ": ", ("item", "term"))
                    box.insert("end", rest + "\n", "item")
                else:
                    box.insert("end", bullet + item + "\n", "item")
        box.configure(state="disabled")

        # --- side panel: topics, shortcuts, support
        side = ttk.Frame(t)
        side.grid(row=1, column=1, sticky="ns", padx=(10, 0))
        topics = ttk.LabelFrame(side, text="Topics", padding=6)
        topics.pack(fill="x")
        for i, (title, _) in enumerate(HELP_SECTIONS):
            link = ttk.Label(topics, text=title, foreground="#1a5fb4", cursor="hand2")
            link.pack(anchor="w", pady=1)
            link.bind("<Button-1>", lambda e, m=f"sec{i}": box.yview(m))

        quick = ttk.LabelFrame(side, text="Shortcuts", padding=6)
        quick.pack(fill="x", pady=8)
        for text, cmd in (("Open data folder", lambda: open_path(self.data_dir)),
                          ("Open exports folder", lambda: open_path(self._conv_out())),
                          ("Show failures ...", self._show_failures)):
            ttk.Button(quick, text=text, command=cmd).pack(fill="x", pady=1)

        support = ttk.LabelFrame(side, text="Support", padding=8)
        support.pack(fill="x")
        ttk.Label(support, text="Questions or problems? Email").pack(anchor="w")
        mail = ttk.Label(support, text=SUPPORT_EMAIL, foreground="#1a5fb4", cursor="hand2",
                         font=("Segoe UI", 9, "underline"))
        mail.pack(anchor="w")
        mail.bind("<Button-1>", lambda e: self._email_support())
        ttk.Button(support, text="Copy email address", command=self._copy_support_email).pack(anchor="w", pady=(4, 6))
        ttk.Label(support, text=f"{APP_NAME} is developed by {DEVELOPER_NAME}.", style="Hint.TLabel",
                  wraplength=px(230)).pack(anchor="w")
        ttk.Label(support, text=f"Version {APP_VERSION}", style="Hint.TLabel").pack(anchor="w")

    def _email_support(self):
        import webbrowser
        subject = f"{APP_NAME} {APP_VERSION} - question".replace(" ", "%20")
        if not webbrowser.open(f"mailto:{SUPPORT_EMAIL}?subject={subject}"):
            self._copy_support_email()

    def _copy_support_email(self):
        self.root.clipboard_clear()
        self.root.clipboard_append(SUPPORT_EMAIL)
        messagebox.showinfo(APP_NAME, f"Copied {SUPPORT_EMAIL} to the clipboard.")

    # ------------------------------------------------------------- logging
    def _log(self, widget, msg):
        widget.configure(state="normal")
        widget.insert("end", f"{time.strftime('%H:%M:%S')}  {msg}\n")
        lines = int(widget.index("end-1c").split(".")[0])
        if lines > LOG_MAX_LINES:
            widget.delete("1.0", f"{lines - LOG_MAX_LINES}.0")
        widget.see("end")
        widget.configure(state="disabled")

    @staticmethod
    def _log_clear(widget):
        widget.configure(state="normal")
        widget.delete("1.0", "end")
        widget.configure(state="disabled")

    def _poll(self):
        """Drain the message queue from worker threads; refresh stats twice a second.

        Always reschedules itself, even after an error - otherwise the whole window stops updating."""
        try:
            try:
                for _ in range(300):
                    kind, payload = self.msgq.get_nowait()
                    if kind in ("sclog", "log"):
                        self._log(self.sc_log, payload)
                    elif kind == "cvlog":
                        self._log(self.cv_log, payload)
                    else:
                        # handlers may open dialogs: run them as their own events so polling keeps going
                        self.root.after_idle(self._handle_message, kind, payload)
            except queue.Empty:
                pass
            now = time.time()
            if now - getattr(self, "_last_stats", 0) > 0.5:
                self._last_stats = now
                self._relayout_tabs()  # content can grow (e.g. another search mode): update the scrollbars
                if self.engine and self.engine.is_running():
                    self._update_scrape_stats()
                    self._db_stale = True
                    # live category progress while the Database tab is open
                    if self.nb.select() == str(self.tab_db) and now - self._db_last_load > DB_LIVE_REFRESH:
                        self._db_refresh()
                if self.convert_thread and self.convert_thread.is_alive():
                    self._update_convert_stats()
        except Exception:  # noqa: BLE001
            self._report_error(*sys.exc_info())
        finally:
            self.root.after(150, self._poll)

    def _handle_message(self, kind, payload):
        if kind == "call":
            payload()
        elif kind == "scrape_done":
            self._scrape_finished(payload)
        elif kind == "convert_done":
            self._convert_finished(payload)

    def _report_error(self, exc_type, exc, tb):
        """Unexpected error in the app itself: write it to error_log.txt (the .exe has no console) and say so."""
        import traceback
        text = "".join(traceback.format_exception(exc_type, exc, tb))
        log_file = self.data_dir / "error_log.txt"
        try:
            with open(log_file, "a", encoding="utf-8") as f:
                f.write(f"--- {time.strftime('%d-%m-%Y %H:%M:%S')}  {APP_NAME} {APP_VERSION}\n{text}\n")
        except OSError:
            pass
        try:
            self._log(self.sc_log, f"Unexpected error: {exc_type.__name__}: {exc} (details in {log_file})")
        except tk.TclError:
            pass
        if time.time() - getattr(self, "_last_error_box", 0) > 30:  # never a flood of popups
            self._last_error_box = time.time()
            messagebox.showerror(APP_NAME, f"Unexpected error: {exc}\n\nDetails were saved to:\n{log_file}\n\n"
                                           f"Please email that file to {SUPPORT_EMAIL}.")

    # ------------------------------------------------------------- settings
    def _load_settings(self):
        value, _ = self.db.get("settings", {})
        return value if isinstance(value, dict) else {}

    def _apply_settings(self):
        s = self.settings
        today = date.today()
        self.mode.set(s.get("mode", "category"))
        self.from_var.set(s.get("from", (today - timedelta(days=6)).strftime("%d-%m-%Y")))
        self.to_var.set(s.get("to", today.strftime("%d-%m-%Y")))
        self.till_today.set(s.get("till_today", False))
        self.dl_pdfs.set(s.get("download_pdfs", True))
        self.read_live.set(s.get("read_live", True))
        self.auto_convert.set(s.get("auto_convert", True))
        self.delay_var.set(s.get("delay_ms", 1000))
        self.par_var.set(s.get("downloads", 2))
        self.window_var.set(s.get("window", "Auto"))
        self.selected_cats = [tuple(c) for c in s.get("categories", [])]
        self._refresh_selected()
        for key, var in (("entity", self.ent_var), ("ministry", self.min_var), ("state", self.state_var),
                         ("department", self.dept_var), ("organization", self.org_var), ("number", self.num_var)):
            var.set(s.get(key, ""))
        self.conv_pdf_var.set(s.get("conv_pdf") or str(self.data_dir / "pdfs"))
        self.conv_out_var.set(s.get("conv_out") or str(self.export_dir))
        self.conv_name_var.set(s.get("conv_name", "gem_contracts"))
        self.fmt_xlsx.set(s.get("fmt_xlsx", True))
        self.fmt_csv.set(s.get("fmt_csv", False))
        self.fmt_json.set(s.get("fmt_json", False))
        self.workers_var.set(s.get("workers", converter.default_workers()))
        # only remember columns the user switched OFF, so new columns are selected by default
        for sheet, cols in (s.get("columns_off") or {}).items():
            if sheet in self.col_vars:
                for title in cols:
                    if title in self.col_vars[sheet]:
                        self.col_vars[sheet][title].set(False)
        for sheet in s.get("sheets_off", []):
            if sheet in self.sheet_vars:
                self.sheet_vars[sheet].set(False)
        self._refresh_sheet_labels()
        self._show_mode()

    def _save_settings(self):
        s = {
            "mode": self.mode.get(), "from": self.from_var.get(), "to": self.to_var.get(),
            "till_today": self.till_today.get(), "download_pdfs": self.dl_pdfs.get(), "read_live": self.read_live.get(),
            "auto_convert": self.auto_convert.get(), "window": self.window_var.get(),
            "categories": self.selected_cats, "entity": self.ent_var.get(), "ministry": self.min_var.get(),
            "state": self.state_var.get(), "department": self.dept_var.get(), "organization": self.org_var.get(),
            "number": self.num_var.get(), "conv_pdf": self.conv_pdf_var.get(), "conv_out": self.conv_out_var.get(),
            "conv_name": self.conv_name_var.get(), "fmt_xlsx": self.fmt_xlsx.get(), "fmt_csv": self.fmt_csv.get(),
            "fmt_json": self.fmt_json.get(),
            "columns_off": {sheet: [t for t, v in cols.items() if not v.get()] for sheet, cols in self.col_vars.items()},
            "sheets_off": [sheet for sheet, v in self.sheet_vars.items() if not v.get()],
        }
        for key, var in (("delay_ms", self.delay_var), ("downloads", self.par_var), ("workers", self.workers_var)):
            try:
                s[key] = int(var.get())
            except (tk.TclError, ValueError):
                pass
        try:
            self.db.put("settings", s)
        except Exception:  # noqa: BLE001 - never block closing the app on a settings write
            pass

    def _on_close(self):
        busy = (self.engine and self.engine.is_running()) or (self.convert_thread and self.convert_thread.is_alive())
        if busy and not messagebox.askyesno(APP_NAME, "A job is still running. Stop it and exit?"):
            return
        if self.engine:
            self.engine.stop()
        if self.convert_cancel:
            self.convert_cancel.set()
        self._save_settings()
        if busy:
            # give the jobs a few seconds to wind down, so History shows "Stopped" and the PDFs being
            # read are saved; a request still waiting on GeM is abandoned
            self.root.configure(cursor="watch")
            self.root.update()
            for t in (self.engine and self.engine.thread, self.convert_thread):
                if t is not None:
                    t.join(timeout=5)
        self.root.destroy()


def main():
    if sys.platform == "win32":
        # sharp text at any Windows scaling (125 %, 150 %, 200 % ...); the layout scales itself from the DPI
        import ctypes
        try:
            ctypes.windll.shcore.SetProcessDpiAwareness(1)
        except (AttributeError, OSError):
            try:
                ctypes.windll.user32.SetProcessDPIAware()  # Windows 7 / 8.0
            except (AttributeError, OSError):
                pass
    root = tk.Tk()
    App(root)
    root.mainloop()
