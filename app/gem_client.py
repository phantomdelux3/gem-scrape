"""
HTTP client for https://gem.gov.in/view_contracts.

- Gets its own session cookies automatically (no manual cookie needed).
- Adaptive rate limit per server: GeM answers bursts with HTTP 500 / timeouts, so every
  throttle response slows the pace down (x1.6) and a run of successes speeds it back up
  (x0.9) towards the user's base delay. Failed requests are retried with back-off.
- Every request is recorded in a RequestMonitor, which the app draws as the throttling bar.
- Reads the search-form options (categories, ministries, states, buyer entities) live from
  the website, and parses the result cards of a search page.
"""

from collections import deque
import html
import json
import random
import re
import threading
import time

import requests
from requests.adapters import HTTPAdapter

BASE_URL = "https://gem.gov.in"
SEARCH_HOST = "gem.gov.in"
USER_AGENT = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
              "(KHTML, like Gecko) Chrome/155.0.0.0 Safari/537.36")
RETRY_DELAYS = [5, 10, 20, 40, 60, 90]
THROTTLE_CODES = (403, 408, 429)
NON_ENGLISH_RE = re.compile("[ऀ-෿‌‍]+")


class StopRequested(Exception):
    """Raised inside long waits when the user presses Stop."""


class BadResponse(Exception):
    def __init__(self, msg, status=None):
        super().__init__(msg)
        self.status = status


def _text(fragment):
    """HTML fragment -> clean single-line English text."""
    s = re.sub(r"<[^>]+>", " ", fragment or "")
    s = html.unescape(s)
    s = NON_ENGLISH_RE.sub(" ", s)
    return re.sub(r"\s+", " ", s).strip()


# ---------------------------------------------------------------------------
# Rate control + monitoring
# ---------------------------------------------------------------------------
class AdaptivePace:
    """Minimum gap between requests to one server; grows on throttling, shrinks on success."""

    MAX_DELAY = 20.0
    SPEEDUP_EVERY = 10   # successes in a row before speeding up again

    def __init__(self, base_delay):
        self.base = max(0.0, base_delay)
        self.delay = self.base
        self.next_slot = 0.0
        self.streak = 0

    def on_success(self):
        self.streak += 1
        if self.streak >= self.SPEEDUP_EVERY and self.delay > self.base:
            self.delay = max(self.base, self.delay * 0.9)
            self.streak = 0

    def on_throttle(self):
        self.streak = 0
        self.delay = min(self.MAX_DELAY, max(self.delay, 0.3) * 1.6 + 0.5)


class RequestMonitor:
    """Thread-safe log of recent requests for the throttling bar (kind: ok | slow | throttled | error)."""

    SLOW_SECONDS = 8.0

    def __init__(self, size=600):
        self._lock = threading.Lock()
        self.events = deque(maxlen=size)
        self.total = 0
        self.throttled_total = 0
        self.version = 0          # bumps on every change, so the UI redraws only when needed
        self.pace = 0.0
        self.base = 0.0

    def record(self, kind, seconds, status=None):
        with self._lock:
            self.events.append((time.time(), kind, seconds, status))
            self.total += 1
            if kind in ("throttled", "error"):
                self.throttled_total += 1
            self.version += 1

    def set_pace(self, pace, base):
        with self._lock:
            if (pace, base) != (self.pace, self.base):
                self.pace, self.base = pace, base
                self.version += 1

    def snapshot(self):
        with self._lock:
            return list(self.events), self.total, self.throttled_total, self.pace, self.base, self.version


# ---------------------------------------------------------------------------
# Client
# ---------------------------------------------------------------------------
class GemClient:
    def __init__(self, delay_ms=1000, timeout=180, log=None, stop_event=None, pause_event=None,
                 pool_size=8, monitor=None):
        self.timeout = timeout
        self.log = log or (lambda msg: None)
        self.stop_event = stop_event or threading.Event()
        self.pause_event = pause_event  # set = paused
        self.monitor = monitor
        base = max(0, delay_ms) / 1000.0
        # the PDF file server (fulfilment.gem.gov.in) is separate and much more relaxed
        self._paces = {SEARCH_HOST: AdaptivePace(base)}
        self._file_base = base / 4
        self._rate_lock = threading.Lock()
        self.session = requests.Session()
        adapter = HTTPAdapter(pool_connections=4, pool_maxsize=pool_size)
        self.session.mount("https://", adapter)
        self.session.headers.update({
            "User-Agent": USER_AGENT,
            "Accept": "*/*",
            "Accept-Language": "en-GB,en-US;q=0.9,en;q=0.8",
            "Origin": BASE_URL,
            "Referer": f"{BASE_URL}/view_contracts",
        })
        self._session_ready = False
        self._session_lock = threading.Lock()
        self.retry_count = 0
        self.request_count = 0
        self._publish_pace()

    # ----- waiting helpers -------------------------------------------------
    def sleep(self, seconds):
        end = time.time() + seconds
        while True:
            if self.stop_event.is_set():
                raise StopRequested()
            left = end - time.time()
            if left <= 0:
                return
            time.sleep(min(0.2, left))

    def wait_if_paused(self):
        while self.pause_event is not None and self.pause_event.is_set():
            if self.stop_event.is_set():
                raise StopRequested()
            time.sleep(0.2)

    @property
    def current_delay(self):
        return self._paces[SEARCH_HOST].delay

    def _pace_for(self, host):
        with self._rate_lock:
            if host not in self._paces:
                self._paces[host] = AdaptivePace(self._file_base)
            return self._paces[host]

    def _throttle(self, host):
        pace = self._pace_for(host)
        with self._rate_lock:
            now = time.time()
            slot = max(now, pace.next_slot)
            pace.next_slot = slot + pace.delay
        if slot > now:
            self.sleep(slot - now)

    def _publish_pace(self):
        if self.monitor:
            p = self._paces[SEARCH_HOST]
            self.monitor.set_pace(p.delay, p.base)

    def _after(self, host, kind, seconds, status=None):
        pace = self._pace_for(host)
        with self._rate_lock:
            if kind in ("throttled", "error"):
                pace.on_throttle()
            else:
                pace.on_success()
        if self.monitor:
            self.monitor.record(kind, seconds, status)
        self._publish_pace()

    # ----- core request with retries ---------------------------------------
    def request(self, method, url, label, ajax=True, **kw):
        if not url.startswith("http"):
            url = BASE_URL + url
        host = url.split("/")[2]
        headers = kw.pop("headers", {})
        if ajax:
            headers.setdefault("X-Requested-With", "XMLHttpRequest")
            headers.setdefault("Content-Type", "application/x-www-form-urlencoded; charset=UTF-8")
        for attempt in range(len(RETRY_DELAYS) + 1):
            self.wait_if_paused()
            self._throttle(host)
            t0 = time.time()
            try:
                self.request_count += 1
                resp = self.session.request(method, url, headers=headers, timeout=self.timeout, **kw)
                took = time.time() - t0
                if resp.status_code >= 500 or resp.status_code in THROTTLE_CODES:
                    self._after(host, "throttled", took, resp.status_code)
                    raise BadResponse(f"HTTP {resp.status_code}", resp.status_code)
                if resp.status_code >= 400:
                    self._after(host, "ok", took, resp.status_code)
                    resp.raise_for_status()  # a real client error: do not retry
                self._after(host, "slow" if took >= RequestMonitor.SLOW_SECONDS else "ok", took, resp.status_code)
                return resp
            except (requests.ConnectionError, requests.Timeout, requests.exceptions.ChunkedEncodingError) as e:
                # ChunkedEncodingError = connection dropped mid-download: retry like any network error
                self._after(host, "error", time.time() - t0, None)
                err = e
            except BadResponse as e:
                err = e
            if attempt >= len(RETRY_DELAYS):
                raise err
            wait = RETRY_DELAYS[attempt] + random.uniform(0, 2)
            self.retry_count += 1
            reason = str(err) if isinstance(err, BadResponse) else type(err).__name__
            self.log(f"  ! {label}: {reason} - slowing down to {self.current_delay:.1f}s/request, "
                     f"retry {attempt + 1}/{len(RETRY_DELAYS)} in {wait:.0f}s")
            self.sleep(wait)

    # ----- session / form options -------------------------------------------
    def ensure_session(self):
        """Open the search page once: this sets the session cookies the site expects."""
        with self._session_lock:
            if self._session_ready:
                return
            self.request("GET", "/view_contracts", "open search page", ajax=False)
            self._session_ready = True

    def fetch_form_options(self):
        """Read categories, ministries, states and buyer entities from the live search page."""
        with self._session_lock:
            resp = self.request("GET", "/view_contracts", "load search options", ajax=False)
            self._session_ready = True
        page = resp.text
        return {
            "categories": self._select_options(page, "buyer_category"),
            "ministries": [v for v, _ in self._select_options(page, "buyer_ministry")],
            "states": [v for v, _ in self._select_options(page, "buyer_state")],
            "buyer_entities": self._buyer_entities(page),
            "fetched_at": time.strftime("%d-%b-%Y %H:%M"),
        }

    @staticmethod
    def _select_options(page, select_id):
        m = re.search(r'<select[^>]*id="%s"[^>]*>(.*?)</select>' % re.escape(select_id), page, re.S)
        if not m:
            return []
        out, seen = [], set()
        for value, label in re.findall(r'<option\s+value="([^"]*)"[^>]*>(.*?)</option>', m.group(1), re.S):
            value = html.unescape(value).strip()
            label = _text(label).strip('"').strip()
            if not value or value in seen:
                continue
            seen.add(value)
            out.append((value, label or value))
        return out

    @staticmethod
    def _buyer_entities(page):
        m = re.search(r"serviceNamesRate\s*=\s*(\[.*?\]);", page, re.S)
        if not m:
            return []
        try:
            names = json.loads(m.group(1))
        except json.JSONDecodeError:
            names = re.findall(r'"((?:[^"\\]|\\.)*)"', m.group(1))
        return sorted({html.unescape(n).strip() for n in names if n and n.strip()}, key=str.lower)

    def _json_names(self, url, label, data, key):
        self.ensure_session()
        r = self.request("POST", url, label, data=data)
        try:
            items = json.loads(r.text.strip() or "[]")
            return sorted({html.unescape(d.get(key) or "").strip() for d in items} - {""}, key=str.lower)
        except (ValueError, AttributeError):
            return []

    def get_departments(self, ministry="", state=""):
        return self._json_names("/view_contracts/get_dept", "load departments",
                                {"buyer_ministry": ministry, "buyer_state": state}, "buyer_departmentName")

    def get_organizations(self, department):
        return self._json_names("/view_contracts/get_org", "load organisations",
                                {"department": department}, "buyer_organizationName")

    # ----- search ---------------------------------------------------------
    def search_page(self, params, page, label):
        """One page of search results -> list of card dicts ([] when the page is empty)."""
        data = dict(params, page=str(page))
        for attempt in range(3):
            self.ensure_session()
            body = self.request("POST", "/view_contracts/contract_details", label, data=data).text
            if not body.strip():
                return []
            cards = parse_cards(body)
            if cards:
                return cards
            # a non-empty body without contracts is an error page: treat it like throttling
            self._after(SEARCH_HOST, "throttled", 0, None)
            self.retry_count += 1
            if attempt >= 1:
                # still wrong after a pause: the session has probably expired (long scrapes) - start a new one
                self.reset_session()
                self.log(f"  ! {label}: unexpected page content - renewing the session and retrying")
            else:
                self.log(f"  ! {label}: unexpected page content - retrying")
            self.sleep(RETRY_DELAYS[attempt])
        raise BadResponse("unexpected page content")

    def reset_session(self):
        """Forget the cookies; the next request opens the search page again for a fresh session."""
        with self._session_lock:
            self.session.cookies.clear()
            self._session_ready = False

    def download_pdf(self, contract_id):
        self.ensure_session()
        r = self.request("POST", "/view_contracts/sbtCaptcha", f"{contract_id} link", data={"oid": contract_id})
        try:
            data = r.json()
        except ValueError:
            data = json.loads(r.text.strip() or "{}")
        if str(data.get("status")) != "1" or not data.get("code"):
            raise BadResponse(f"no download link (status {data.get('status')})")
        m = re.search(r'href=\\?"([^"\\]+)', data["code"])
        if not m:
            raise BadResponse("no download link in response")
        url = html.unescape(m.group(1).replace("\\/", "/"))
        for attempt in range(3):
            pdf = self.request("GET", url, f"{contract_id} pdf", ajax=False, headers={"Referer": f"{BASE_URL}/"})
            if pdf.content[:5] == b"%PDF-":
                return pdf.content
            self.retry_count += 1
            self.sleep(RETRY_DELAYS[attempt])
        raise BadResponse("response is not a PDF")


CARD_SPLIT_RE = re.compile(r'<div class="border block">')
FIELD_RE = re.compile(r"<strong>\s*([^<:]+?)\s*:\s*</strong>\s*(?:&#8377;\s*)?<span[^>]*>(.*?)</span>", re.S)
FIELD_KEYS = {
    "Status of the Contract": "Status", "Organization Type": "OrgType", "Ministry": "Ministry",
    "Department": "Department", "Organization Name": "OrgName", "Office Zone": "OfficeZone",
    "Buyer Designation": "BuyerDesignation", "Buying Mode": "BuyingMode", "Bid Number": "BidNo",
    "Contract Date": "ContractDate", "Total": "Total",
}


def parse_cards(body):
    cards = []
    body = re.sub(r"<!--.*?-->", "", body, flags=re.S)  # the site comments out an HSN column
    for chunk in CARD_SPLIT_RE.split(body)[1:]:
        m = re.search(r"openCap\('([^']+)'\)", chunk)
        if not m:
            continue
        card = {"ContractNo": m.group(1)}
        for label, value in FIELD_RE.findall(chunk):
            key = FIELD_KEYS.get(_text(label))
            if key and key not in card:
                card[key] = _text(value) or None
        if card.get("Total"):
            try:
                card["Total"] = float(card["Total"].replace(",", ""))
            except ValueError:
                pass
        products, brands, models, qty = [], [], [], 0
        for row in re.findall(r"<tr>\s*(<td>.*?)</tr>", chunk, re.S):
            tds = [_text(t) for t in re.findall(r"<td>(.*?)</td>", row, re.S)]
            if len(tds) >= 5:
                products.append(tds[0])
                brands.append(tds[1])
                models.append(tds[2])
                try:
                    qty += float(tds[3].replace(",", ""))
                except ValueError:
                    pass
        card["Products"] = " | ".join(dict.fromkeys(p for p in products if p)) or None
        card["Brands"] = " | ".join(dict.fromkeys(b for b in brands if b and b != "-")) or None
        card["Models"] = " | ".join(dict.fromkeys(x for x in models if x and x != "-")) or None
        card["Quantity"] = int(qty) if qty and float(qty).is_integer() else (qty or None)
        cards.append(card)
    return cards
