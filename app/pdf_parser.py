"""
GeM contract PDF parser.

Turns one GeM contract PDF into plain Python data (contract fields, items, consignees,
specifications). Handles the current bilingual layout (Hindi|English labels, bold text that
extracts as doubled characters), the older English-only layout, Bid/RA and service contracts,
tables broken across pages and side-by-side boxes merged into one cell.
All values come out in English only - Hindi text and (cid:N) glyph codes are removed.
"""

from datetime import datetime
from pathlib import Path
import re
import unicodedata

import pdfplumber

PARSER_VERSION = 12  # bump to invalidate cached parse results after parser changes

# ---------- text helpers ----------
CID_RE = re.compile(r"\(cid:\d+\)")
DEVANAGARI_RE = re.compile("[ऀ-ॿ‌‍]+")
ILLEGAL_XML_RE = re.compile(r"[\x00-\x08\x0b\x0c\x0e-\x1f]")
DATE_RE = re.compile(r"^\d{1,2}-[A-Za-z]{3}-\d{4}$")
GSTIN_RE = re.compile(r"\b\d{2}[A-Z]{5}\d{4}[A-Z][A-Z\d]Z[A-Z\d]\b(?:\s*\([A-Z]\))?")
STATE_PIN_RE = re.compile(r"(?:([A-Za-z][A-Za-z .&()'-]*?),\s*)?([A-Z][A-Z .&'-]+?)\s*-\s*(\d{6})\b")


def strip_noise(s):
    """Remove (cid:N) glyph codes and Hindi text, keep line structure."""
    if not s:
        return ""
    s = CID_RE.sub(" ", s)
    s = DEVANAGARI_RE.sub(" ", s)
    return unicodedata.normalize("NFKC", s)  # also turns ligatures like 'ﬁ' into 'fi'


def clean_lines(s):
    """Noise-free, whitespace-collapsed lines that contain real content."""
    out = []
    for ln in strip_noise(s).splitlines():
        ln = re.sub(r"[ \t]+", " ", ln).strip()
        if ln and re.search(r"[A-Za-z0-9@]", ln):
            out.append(ln)
    return out


def norm_key(s):
    """Normalise a label for matching: lowercase alphanumerics with repeated chars collapsed.
    Collapsing makes bold (doubled) text like 'CCoonnttrraacctt' match 'Contract'."""
    s = re.sub(r"[^a-z0-9]", "", strip_noise(s).lower())
    return re.sub(r"(.)\1+", r"\1", s)


def _is_doubled(tok):
    return len(tok) >= 2 and len(tok) % 2 == 0 and tok[0::2] == tok[1::2]


def undouble(s):
    """Bold text in GeM PDFs is extracted with every character doubled ('22,,001166').
    Undo it only when every token is doubled and at least one is long enough to be sure."""
    if not s:
        return s
    toks = s.split()
    if not toks:
        return s
    doubled = [_is_doubled(t) for t in toks]
    if all(doubled) and any(len(t) >= 4 for t in toks):
        return " ".join(t[0::2] for t in toks)
    # mostly-bold text mixed with normal words ("BBuuyyeerr 11.. SSccooppee ooff wwoorrkk ::click here")
    if len(toks) >= 4 and sum(doubled) / len(toks) >= 0.6:
        return " ".join(t[0::2] if d else t for t, d in zip(toks, doubled))
    return s


def clean_value(s):
    if s is None:
        return None
    s = " ".join(clean_lines(s))
    s = undouble(s).strip(" ,;|")
    return s or None


def to_number(s):
    if s is None:
        return None
    s = str(s).replace(",", "").replace(" ", "")
    m = re.search(r"-?\d+(?:\.\d+)?", s)
    if not m or m.group(0) != s.strip():
        return None
    v = float(m.group(0))
    return int(v) if v.is_integer() else v


def parse_amount(raw, expected=None):
    """Parse a money value that may be bold (doubled). If an expected total is known
    (sum of line items), prefer whichever reading matches it."""
    if not raw:
        return None
    txt = " ".join(clean_lines(raw))
    plain = to_number(txt)
    single = to_number(" ".join(t[0::2] if _is_doubled(t) else t for t in txt.split()))
    if expected:
        for cand in (single, plain):
            if cand is not None and abs(cand - expected) < 1:
                return cand
    if all(_is_doubled(t) for t in txt.split()) and len(txt.replace(" ", "")) >= 4:
        return single
    return plain if plain is not None else single


def parse_date(s):
    if not s:
        return None
    s = s.strip()
    for fmt in ("%d-%b-%Y", "%d-%m-%Y", "%d/%m/%Y"):
        try:
            return datetime.strptime(s, fmt)
        except ValueError:
            pass
    return None


def split_location(address):
    """Return (district, state, pincode) from an address ending like 'Vadodara, GUJARAT-391520, India'."""
    if not address:
        return None, None, None
    matches = list(STATE_PIN_RE.finditer(address))
    if not matches:
        return None, None, None
    m = matches[-1]
    district = (m.group(1) or "").strip() or None
    if district and "," in district:
        district = district.split(",")[-1].strip() or None
    return district, m.group(2).strip().title(), m.group(3)


# ---------- label maps (human label -> output field), keyed by normalised label ----------
def _keymap(d):
    return {norm_key(k): v for k, v in d.items()}


HEADER_MAP = _keymap({
    "Contract No": "ContractNo",
    "Generated Date": "GeneratedDate",
    "Contract Generated Date": "GeneratedDate",
    "Bid/RA/PBP No.": "BidNo",
    "Bid/RA/PR No.": "BidNo",
    "Bid No": "BidNo",
    "Procurement Mode": "ProcurementMode",
    "Schedule Name": "BidScheduleName",
})

SECTION_MAPS = {
    "org": _keymap({
        "Type": "Org_Type", "Ministry": "Org_Ministry", "Department": "Org_Department",
        "Organisation Name": "Org_Name", "Office Zone": "Org_OfficeZone",
    }),
    "buyer": _keymap({
        "Name": "Buyer_Name", "Designation": "Buyer_Designation", "Contact No.": "Buyer_Contact",
        "Contact": "Buyer_Contact", "Email ID": "Buyer_Email", "GSTIN": "Buyer_GSTIN", "Address": "Buyer_Address",
    }),
    "fin": _keymap({
        "IFD Concurrence": "IFD_Concurrence",
        "Designation of Administrative Approval": "AdminApproval_Designation",
        "Designation of Financial Approval": "FinancialApproval_Designation",
    }),
    "paying": _keymap({
        "Name": "Paying_Name", "Role": "Paying_Role", "Payment Mode": "Paying_PaymentMode",
        "Designation": "Paying_Designation", "Email ID": "Paying_Email", "GSTIN": "Paying_GSTIN",
        "Address": "Paying_Address", "Contact No.": "Paying_Contact", "Contact": "Paying_Contact",
    }),
    "seller": _keymap({
        "GeM Seller ID": "Seller_GeMID", "Company Name": "Seller_Company", "Contact No.": "Seller_Contact",
        "Contact": "Seller_Contact", "Email ID": "Seller_Email", "Address": "Seller_Address",
        "MSME Registration number": "Seller_MSME_RegNo", "MSME verified": "Seller_MSME_Verified",
        "MSE Social Category": "Seller_MSE_SocialCategory", "MSE Gender": "Seller_MSE_Gender",
        "GSTIN": "Seller_GSTIN",
    }),
}

# Labels that can appear in any section of the contract
GLOBAL_MAP = _keymap({
    "Delivery Instructions": "DeliveryInstructions",
    "Service Start Date (latest by)": "Service_StartDate",
    "Service End Date": "Service_EndDate",
    "Billing Cycle": "Service_BillingCycle",
    "Advisory Bank": "ePBG_AdvisoryBank",
    "ePBG Percentage(%)": "ePBG_Percentage",
    "ePBG Percentage": "ePBG_Percentage",
    "Additional Required Data/Document(s)": "AdditionalRequiredDocs",
})

ITEM_MAP = _keymap({
    "Product Name": "ProductName", "Brand": "Brand", "Brand Type": "BrandType",
    "Catalogue Status": "CatalogueStatus", "Selling As": "SellingAs",
    "Category Name & Quadrant": "CategoryQuadrant", "Category Name": "Category",
    "Model": "Model", "HSN Code": "HSNCode", "Country Of Origin": "CountryOfOrigin",
    "View Product": "_view_product",  # a 'Click here' link on the PDF - dropped
})
# wrapped tax labels ('22 GGSSTT CCeessss 22 (9 per ...') that sometimes stick to the end of item text
TAX_TAIL_RE = re.compile(r"\s*(?:\b(\d)\1\s*)?(?:G{2}S{2}T{2}|I{2}T{2}C{2})\b.*$")

CONSIGNEE_MAP = _keymap({
    "Name": "Name", "Designation": "Designation", "Email ID": "Email", "Contact": "Contact",
    "Contact No.": "Contact", "GSTIN": "GSTIN", "Address": "Address",
})

# Section headings (first line of a table cell) -> section id
SECTION_HEADINGS = [
    ("organisationdetails", "org"),
    ("buyerdetails", "buyer"),
    ("financialapprovaldetail", "fin"),
    ("payingauthoritydetails", "paying"),
    ("serviceproviderdetails", "seller"),
    ("sellerdetails", "seller"),
    ("productdetails", "items"),
    ("servicedetails", "services"),
    ("consigneedetail", "consignee"),
    ("productspecificationfor", "spec"),
    ("epbgdetail", "epbg"),
    ("termsandconditions", "terms"),
]
SECTION_HEADINGS = [(norm_key(k), v) for k, v in SECTION_HEADINGS]
BLOCK_SECTIONS = {"org", "buyer", "fin", "paying", "seller"}

# Row markers, normalised the same way as the PDF text
K_TOTAL_ORDER = norm_key("Total Order Value")
K_TOTAL_CONTRACT = norm_key("Total Contract Value Including All Duties and Taxes")
K_CATEGORYNAME = norm_key("Category Name")
K_SERVICE_DESC = norm_key("Description/Nomenclature of Service")
K_SERVICE_DESC_SHORT = norm_key("Service Description")
K_DELIVERYSTART = norm_key("Delivery Start")
K_PRODUCTNAME = norm_key("Product Name")
K_DESCRIPTION = norm_key("Description")
K_TOTAL_INCL_ADDONS = norm_key("Total Value Including Addons")
K_TOTAL_WITHOUT_ADDONS = norm_key("Total Value without Addons")
# service-section rows that are not service parameters
SERVICE_SKIP = tuple(norm_key(s) for s in (
    "Total Amount (Formula)", "Total Addon Value", "Amount of Contract", "Billing Cycle", "Service Start Date",
    "Service End Date", "Techno-Commercial Clarification", "Advisory Bank", "ePBG Percentage", "The bidder shall",
    "Service Level Agreement", "SLA Details", "Special Terms and Conditions",
))


def _loose(phrase):
    """Regex for a phrase that may be printed bold (every character doubled)."""
    out = []
    for ch in phrase:
        if ch == " ":
            out.append(r"\s*")
        elif ch.isalnum():
            out.append(re.escape(ch) + "{1,2}")
        else:
            out.append(re.escape(ch) + "{1,2}\\s*")
    return "".join(out)


LOOSE_SERVICE_LABEL_RE = re.compile(
    r"^\W*" + _loose("Description/Nomenclature of Service") +
    r"(?:\s*" + _loose("Proposed for procurement using custom bid functionality") + r")?\s*", re.I)
ITEM_NUMBERS_RE = re.compile(r"(\d+(?:\.\d+)?)\s+([A-Za-z][\w./()-]*)\s+([\d,]+(?:\.\d+)?)\s+"
                             r"(NA|-|[\d,]+(?:\.\d+)?)\s+([\d,]+(?:\.\d+)?)\s*$")
QTY_UNIT_PRICE_RE = re.compile(r"(?:^|\s)(\d[\d,]*(?:\.\d+)?)\s+([A-Za-z]{1,12})\s+([\d,]+(?:\.\d+)?)(?=\s|$)")


def extract_item_numbers(blob):
    """When a page break squeezes an item row into one text blob, its numbers end up inside a
    line, e.g. '27 | |Selling As :: NA 3 EA 65,193 NA 195,579'.
    Returns ({ItemNo, Quantity, Unit, UnitPrice, TotalPrice}, remaining text without the numbers)."""
    lines = clean_lines(blob)
    found = {}
    for n, line in enumerate(lines):
        m = ITEM_NUMBERS_RE.search(line)
        if not m:
            continue
        lead = re.match(r"^(\d{1,4})\s+(?=\D)", line)
        start = 0
        if lead and lead.end() <= m.start():
            found["ItemNo"] = int(lead.group(1))
            start = lead.end()
        found.update({"Quantity": to_number(m.group(1)), "Unit": m.group(2),
                      "UnitPrice": to_number(m.group(3)), "TotalPrice": to_number(m.group(5))})
        lines[n] = line[start:m.start()]
        break
    if not found:
        # looser layout: '2 ||Category Name && 1,500 M 373.5 ||Input Tax ...' (qty unit unit-price only)
        for n, line in enumerate(lines):
            m = QTY_UNIT_PRICE_RE.search(line)
            if not m:
                continue
            lead = re.match(r"^(\d{1,4})\s+(?=\D)", line)
            if lead and lead.end() <= m.start():
                found["ItemNo"] = int(lead.group(1))
                line = line[lead.end():]
                m = QTY_UNIT_PRICE_RE.search(line)
            qty, price = to_number(m.group(1)), to_number(m.group(3))
            found.update({"Quantity": qty, "Unit": m.group(2), "UnitPrice": price,
                          "TotalPrice": round(qty * price, 2) if qty is not None and price is not None else None})
            lines[n] = line[:m.start()] + line[m.end():]
            break
    return found, "\n".join(lines)


def apply_item_numbers(item, nums):
    for field in ("Quantity", "Unit", "UnitPrice", "TotalPrice"):
        if nums.get(field) is not None and item.get(field) in (None, ""):
            item[field] = nums[field]
# Rows after the consignee table that are not consignees (Bid documents, notes, ...)
CONSIGNEE_STOP = tuple(norm_key(s) for s in (
    "Specifi", "Note", "Buyer BOQ Document", "Compliance of BOQ", "Seller Specification",
    "Buyer Added Bid", "Buyer uploaded ATC", "Corrigendum", "Product Specification",
))
DATE_IN_TEXT = r"\d{1,2}-[A-Za-z]{3}-\d{4}"
PHONE_RE = re.compile(r"\+?\d[\d\- ]{4,}\d-?\d*")


EMAIL_RE = re.compile(r"[\w.+-]+@[\w-]+(?:\.[\w-]+)+")


def sanitize_email(value):
    m = EMAIL_RE.search(value or "")
    return m.group(0) if m else value


def sanitize_consignee(info):
    if info.get("Email"):
        info["Email"] = sanitize_email(info["Email"])
    if info.get("Contact"):
        m = PHONE_RE.search(info["Contact"])
        info["Contact"] = m.group(0).strip() if m else (info["Contact"] if info["Contact"].strip() == "-" else None)
    if info.get("GSTIN"):
        m = GSTIN_RE.search(info["GSTIN"])
        info["GSTIN"] = m.group(0) if m else ("-" if info["GSTIN"].startswith("-") else info["GSTIN"])
    return info


def merge_consignee_blob(line, blob):
    """Fill a consignee row from the text blob that follows a page break, e.g.
    '1 |Contact : 0194-2430149 with Liquid Crystal - 5 01-Oct-2026 16-Oct-2026\\n|GSTIN : -\\n|Address : ...'"""
    lines = clean_lines(blob)
    if not lines:
        return
    for n, text in enumerate(lines):
        m = re.search(rf"(\S+)\s+(\d+(?:\.\d+)?)\s+({DATE_IN_TEXT})(?:\s+({DATE_IN_TEXT}))?\s*$", text)
        if not m:
            continue
        line.setdefault("LotNo", m.group(1))
        if line.get("Quantity") in (None, ""):
            line["Quantity"] = to_number(m.group(2))
        line.setdefault("DeliveryStart", m.group(3))
        lines[n] = text[:m.start()]
        end = m.group(4)
        if not end:  # the second date wrapped onto another line
            others = [d for k, t in enumerate(lines) if k != n for d in re.findall(DATE_IN_TEXT, t)]
            end = others[0] if others else None
            if end:
                lines = [t.replace(end, " ") for t in lines]
        if end:
            line.setdefault("DeliveryEnd", end)
        break
    lines = [re.sub(r"^\d+\s+(?=\|)", "", t) for t in lines]
    info = sanitize_consignee(parse_block("\n".join(lines), CONSIGNEE_MAP))
    for k, v in info.items():
        if v and not line.get(k):
            line[k] = v


def find_headings(first_line_norm):
    """Return [(position, section)] of section headings found in a cell's first line."""
    found = []
    for key, sec in SECTION_HEADINGS:
        pos = first_line_norm.find(key)
        if pos != -1 and not any(sec == s for _, s in found):
            found.append((pos, sec))
    return sorted(found)


# ---------- generic "Label : value" block parser ----------
LABEL_SPLIT_RE = re.compile(r"^(.{1,90}?)\s*:{1,2}\s*(.*)$")


def _prejoin(text):
    """Re-join labels that the PDF wraps across lines."""
    text = re.sub(r"(C+a+t+e+g+o+r+y+\s+N+a+m+e+\s*&+)\s*\n\s*", r"\1 ", text)
    text = re.sub(r"(\(+I+T+C+\)+\s*o+n+)\s*\n\s*", r"\1 ", text)
    # "|Organisation <value>\nName : <value2>"  /  "|Office <value>\nZone: <value2>"
    text = re.sub(r"(?m)^[ \t]*\|?[ \t]*Organisation[ \t]+([^:\n]+)\n[ \t]*Name[ \t]*:[ \t]*(.*)$", r"Organisation Name : \1 \2", text)
    text = re.sub(r"(?m)^[ \t]*\|?[ \t]*Office[ \t]+([^:\n]+)\n[ \t]*Zone[ \t]*:[ \t]*(.*)$", r"Office Zone : \1 \2", text)
    # "Contact\n<value>\nNo.:" (older layout wraps the label around its value)
    text = re.sub(r"(?m)^[ \t]*\|?[ \t]*Contact[ \t]*\n(?:([^:\n]*)\n)?[ \t]*No\.?[ \t]*:[ \t]*$",
                  lambda m: "Contact No. : " + (m.group(1) or "-"), text)
    return text


def parse_block(text, keymap, extra_maps=()):
    """Parse 'Label : value' pairs out of a block of text.

    Lines without a recognised label (wrapped values) are attached to the right label:
    to the following label if that label's own value is empty (GeM prints the value
    above 'Address :' / 'Payment Mode:' labels), otherwise to the preceding label."""
    maps = [keymap, *extra_maps]
    lines = clean_lines(_prejoin(strip_noise(text)))
    fields = []      # [field, [parts]]
    pending = []
    for line in lines:
        for seg in line.split("|"):
            seg = seg.strip()
            if not seg or not re.search(r"[A-Za-z0-9@]", seg):
                continue
            field, value = None, seg
            m = LABEL_SPLIT_RE.match(seg)
            if m:
                k = norm_key(m.group(1))
                for mp in maps:
                    if k in mp:
                        field, value = mp[k], m.group(2).strip()
                        break
                if field is None and re.search(r"gst|itc|ces", k) and "gstin" not in k:
                    # tax lines (GST / Cess / ITC) bleeding in from the next column: drop them, and
                    # the doubled cess number ("11") that the Hindi label leaves on the previous segment
                    target = pending if pending else (fields[-1][1] if fields else None)
                    if target:
                        target[-1] = re.sub(r"\s+(\d)\1$", "", target[-1])
                    continue
            if field is None:
                pending.append(seg)
                continue
            if pending:
                if not value or value.startswith(","):
                    # value printed above the label, or a value that continues the lines above it
                    value = " ".join(pending) + (" " + value if value else "")
                elif fields:
                    fields[-1][1].extend(pending)
                pending = []
            fields.append([field, [value] if value else []])
    if pending and fields:
        fields[-1][1].extend(pending)

    out = {}
    for field, parts in fields:
        val = clean_value(" ".join(parts))
        if val and field not in out:
            out[field] = val
    return out


# ---------- per-section helpers ----------
def parse_payment_timelines(lines_text):
    lines = clean_lines(lines_text)
    start = next((i for i, ln in enumerate(lines) if ln.lower().startswith("payments shall be made")), None)
    if start is None:
        return None, lines_text
    tail = " ".join(lines[start:])
    tail = re.sub(r"\|\s*Payment\b\s*", " ", tail)
    tail = re.sub(r"\bTimelines\s*:\s*", " ", tail)
    remaining = "\n".join(lines[:start])
    return re.sub(r"\s+", " ", tail).strip(), remaining


def parse_section_block(section, text, contract):
    if section == "paying":
        timelines, text = parse_payment_timelines(text)
        if timelines:
            contract.setdefault("Paying_Timelines", timelines)
    if section == "seller":
        flat = " ".join(clean_lines(text))
        m = re.search(r"MSME Status as\s+(.*?)\s+verified by buyer\s*:?", flat)
        if m:
            contract.setdefault("Seller_MSME_StatusVerifiedByBuyer", m.group(1).strip())
            text = re.sub(r"MSME Status as|verified by buyer\s*:?", " ", strip_noise(text))
            text = text.replace(m.group(1), " ")
        gstins = [re.sub(r"\s+", " ", g) for g in GSTIN_RE.findall(strip_noise(text))]
        if gstins:
            registered = next((g for g in gstins if g.endswith("(R)")), gstins[0])
            contract.setdefault("Seller_GSTIN", re.sub(r"\s*\([A-Z]\)", "", registered))
            if len(gstins) > 1:
                contract.setdefault("Seller_AllGSTINs", ", ".join(dict.fromkeys(gstins)))
    data = parse_block(text, SECTION_MAPS[section], (GLOBAL_MAP,))
    if section == "seller":
        if "Seller_GSTIN" in contract:
            data.pop("Seller_GSTIN", None)
        if "Seller_MSME_RegNo" in data:
            reg = GSTIN_RE.sub("", data["Seller_MSME_RegNo"]).strip(" ,")
            data["Seller_MSME_RegNo"] = reg.split()[0] if reg else None
    for k, v in data.items():
        if k.endswith("_Email"):
            v = sanitize_email(v)
        elif k.endswith("_GSTIN"):
            m = GSTIN_RE.search(v or "")
            if m:
                v = re.sub(r"\s*\([A-Z]\)", "", m.group(0))
            elif v and re.match(r"^(-|NA)\b", v):
                v = v.split()[0]
        if v and not contract.get(k):
            contract[k] = v


def split_merged_cell(page, bbox, first_section, second_section):
    """Two side-by-side boxes (e.g. Organisation | Buyer) sometimes come out as one table cell.
    The right box starts where its labels ('पद|Designation', 'ईमेल आईडी|Email ID', ...) start, so
    find that x position and split every line of words there."""
    try:
        words = page.crop(bbox).extract_words(x_tolerance=1.5)
        if not words:
            return None
        right_keys = set(SECTION_MAPS[second_section]) - set(SECTION_MAPS[first_section])
        starts = []
        for w in words:
            t = strip_noise(w["text"])
            if "|" not in t:
                continue
            k = norm_key(t.split("|")[-1].split(":")[0])
            if k and any(rk.startswith(k) for rk in right_keys):
                starts.append(w["x0"])
        if not starts:
            return None
        split_x = min(starts) - 0.5
        lines = {}
        for w in sorted(words, key=lambda w: (round(w["top"]), w["x0"])):
            key = next((t for t in lines if abs(t - w["top"]) < 3), w["top"])
            lines.setdefault(key, ([], []))[0 if w["x1"] <= split_x + 1 else 1].append(w["text"])
        left = "\n".join(" ".join(l) for l, _ in (lines[k] for k in sorted(lines)) if l)
        right = "\n".join(" ".join(r) for _, r in (lines[k] for k in sorted(lines)) if r)
        return left, right
    except Exception:
        return None


def map_item_header(cells):
    """Map item-table header cells (by index) to field names."""
    mapping = {}
    for idx, text in cells:
        k = norm_key(text)
        if not k:
            continue
        if k in ("#", "") or text.strip().strip("#") == "":
            mapping[idx] = "ItemNo"
        elif "itemdescription" in k:
            mapping[idx] = "Description"
        elif "category" in k:
            mapping[idx] = "Category"
        elif "model" in k:
            mapping[idx] = "Model"
        elif "hsn" in k:
            mapping[idx] = "HSNCode"
        elif "quantity" in k:
            mapping[idx] = "Quantity"
        elif "unitprice" in k:
            mapping[idx] = "UnitPrice"
        elif "taxbifurcation" in k:
            mapping[idx] = "TaxBifurcation"
        elif "inclusive" in k or "price" in k:
            mapping[idx] = "TotalPrice"
        elif k in ("unit", "unitinr") or k.endswith("unit"):
            mapping[idx] = "Unit"
    return mapping


GST_RE = re.compile(r"G+S+T+\s*\((\d+(?:\.\d+)?)%\)\s*:\s*([\d,.]+)")


def parse_tax(text):
    flat = " ".join(clean_lines(text))
    for m in GST_RE.finditer(flat):
        if re.search(r"o+n+\s*$", flat[:m.start()]):
            continue  # "ITC on GST (100%)"
        return to_number(m.group(1)), to_number(m.group(2))
    return None, None


def fill_item_from_desc(item, desc_text):
    data = parse_block(desc_text, ITEM_MAP)
    if data:
        for k, v in data.items():
            if v and not item.get(k):
                item[k] = v
    elif desc_text and not item.get("ProductName"):
        item["ProductName"] = clean_value(desc_text)


def finish_item(item):
    item.pop("_desc_done", None)
    item.pop("_view_product", None)
    for k, v in list(item.items()):
        if isinstance(v, str) and not k.startswith("_"):
            item[k] = TAX_TAIL_RE.sub("", v).strip() or None
    details = item.pop("_details", None)
    if details:
        item["ServiceDetails"] = "; ".join(details)[:2000]
    cq = item.pop("CategoryQuadrant", None)
    if cq:
        m = re.match(r"^(.*?)\s*\(([^()]*)\)\s*$", cq)
        if m:
            item.setdefault("Category", m.group(1).strip() or None)
            q = m.group(2).strip()
            item["Quadrant"] = q if q and q != "-" else None
        else:
            item.setdefault("Category", cq)
    for f in ("Quantity", "UnitPrice", "TotalPrice"):
        if isinstance(item.get(f), str):
            item[f] = to_number(item[f].replace(" ", ""))
    # older contracts only print the line total
    if item.get("UnitPrice") is None and item.get("TotalPrice") and item.get("Quantity"):
        item["UnitPrice"] = round(item["TotalPrice"] / item["Quantity"], 2)
    return item


# ---------- per-pdf processing ----------
def process_pdf(pdf_path):
    contract = {"SourceFile": str(pdf_path)}
    items, consignees, specs = [], [], []
    section = None
    item_header = None          # (table_id, {idx: field}, [fields in compact order])
    current_consignee = None
    consignee_services = False
    service_header = None
    spec_product, spec_group = None, None
    total_raw = None
    service_category = None

    with pdfplumber.open(pdf_path) as pdf:
        # Header lines (contract no, date, bid no, procurement mode) sit above the first table
        header_lines = []
        for line in clean_lines(pdf.pages[0].extract_text() or "")[:12]:
            if find_headings(norm_key(line)):
                break
            header_lines.append(line)
        for k, v in parse_block("\n".join(header_lines), HEADER_MAP).items():
            contract.setdefault(k, v)

        done = False
        for page in pdf.pages:
            if done:
                break
            for t_idx, table in enumerate(page.find_tables()):
                table_id = (page.page_number, t_idx)
                rows = table.extract()
                for r_idx, row in enumerate(rows):
                    bboxes = table.rows[r_idx].cells if r_idx < len(table.rows) else [None] * len(row)
                    cells = [(i, c, bboxes[i] if i < len(bboxes) else None)
                             for i, c in enumerate(row) if c is not None]
                    texts = [(i, c) for i, c, _ in cells if clean_lines(c) or c.strip() in ("-",)]
                    if not texts:
                        continue
                    first_lines = clean_lines(texts[0][1])
                    first_norm = norm_key(first_lines[0]) if first_lines else ""

                    # ---- GST invoice / delivery instructions rows can appear anywhere ----
                    joined = " ".join(" ".join(clean_lines(c)) for _, c in texts)
                    m_inv = re.search(r"raised in the name of\s*-?\s*([A-Za-z ]+)", joined)
                    if m_inv:
                        contract.setdefault("GSTInvoiceInNameOf", m_inv.group(1).strip())
                        continue

                    # ---- section headings ----
                    heading_row = False
                    for i, c, bbox in cells:
                        lines = clean_lines(c)
                        # headings are short, but bold (doubled) product names can make them long
                        if not lines or len(lines[0]) > 400:
                            continue
                        heads = find_headings(norm_key(lines[0]))
                        if not heads:
                            continue
                        heading_row = True
                        secs = [s for _, s in heads]
                        if len(secs) >= 2 and secs[0] in BLOCK_SECTIONS and secs[1] in BLOCK_SECTIONS:
                            halves = split_merged_cell(page, bbox, secs[0], secs[1]) if bbox else None
                            if halves:
                                parse_section_block(secs[0], "\n".join(clean_lines(halves[0])[1:]), contract)
                                parse_section_block(secs[1], "\n".join(clean_lines(halves[1])[1:]), contract)
                            else:
                                parse_section_block(secs[0], "\n".join(lines[1:]), contract)
                            section = secs[1]
                            continue
                        section = secs[0]
                        if section in BLOCK_SECTIONS:
                            parse_section_block(section, "\n".join(lines[1:]), contract)
                        elif section == "spec":
                            heading = undouble(" ".join(lines))
                            spec_product = re.sub(r"(?i)^.*?Product Specification for\s*", "", heading).strip() or None
                            spec_group = None
                        elif section == "terms":
                            done = True
                    if done:
                        break
                    if heading_row:
                        continue

                    # ---- totals (goods and services) ----
                    if K_TOTAL_ORDER in first_norm or K_TOTAL_CONTRACT in first_norm:
                        vals = [c for _, c in texts[1:] if clean_lines(c)]
                        if vals:
                            total_raw = vals[-1]
                        continue

                    if section in BLOCK_SECTIONS:
                        parse_section_block(section, "\n".join(c for _, c in texts), contract)
                        continue

                    # global labels (delivery instructions, service dates, ePBG ...)
                    g = parse_block("\n".join(c for _, c in texts), GLOBAL_MAP)
                    for k, v in g.items():
                        contract.setdefault(k, v)

                    if section == "items":
                        all_cells = [(i, c) for i, c, _ in cells]
                        if any("itemdescription" in norm_key(c) for _, c in texts):
                            mapping = map_item_header(all_cells)
                            mapping.setdefault(all_cells[0][0], "ItemNo")
                            item_header = (table_id, mapping, [mapping.get(i) for i, _ in all_cells])
                            continue
                        first_val = clean_value(texts[0][1]) or ""
                        has_product = K_PRODUCTNAME in norm_key(joined)
                        numbered = re.fullmatch(r"\d{1,4}", first_val) and (len(texts) >= 4 or has_product)
                        # an item whose number/price cells were pushed to the next page by a page break
                        unnumbered = (not numbered and has_product and item_header
                                      and not (clean_value(all_cells[0][1]) or "").strip())
                        if (numbered or unnumbered) and item_header:
                            tid, mapping, compact = item_header
                            row_map = dict(all_cells)
                            if tid == table_id and all(i in row_map for i in mapping):
                                values = {mapping[i]: row_map[i] for i in mapping}
                            else:
                                # item table continued on a new page: same columns, different x positions
                                values = {f: c for f, (_, c) in zip(compact, all_cells) if f}
                            item = {"ItemNo": to_number(first_val) if numbered else len(items) + 1}
                            fill_item_from_desc(item, values.get("Description", ""))
                            for f in ("Category", "Model", "HSNCode", "Unit"):
                                if values.get(f) and not item.get(f):
                                    item[f] = clean_value(values[f])
                            for f in ("Quantity", "UnitPrice", "TotalPrice"):
                                if values.get(f):
                                    item[f] = to_number(clean_value(values[f]))
                            if values.get("TaxBifurcation"):
                                rate, amt = parse_tax(values["TaxBifurcation"])
                                item["GST_Rate"], item["GST_Amount"] = rate, amt
                                if rate is None:
                                    item["TaxBifurcation"] = clean_value(values["TaxBifurcation"])
                            items.append(item)
                        elif len(texts) == 1 and has_product:
                            # a whole item squeezed into one text blob by a page break
                            nums, rest = extract_item_numbers(texts[0][1])
                            item = {"ItemNo": nums.get("ItemNo") or len(items) + 1}
                            fill_item_from_desc(item, rest)
                            apply_item_numbers(item, nums)
                            items.append(item)
                        elif items and any(k in ITEM_MAP for k in (norm_key(s.split(":")[0]) for s in re.split(r"[|\n]", strip_noise(joined)) if ":" in s)):
                            # item continued from the previous page
                            blob = "\n".join(c for _, c in texts)
                            nums, rest = extract_item_numbers(blob)
                            fill_item_from_desc(items[-1], rest)
                            apply_item_numbers(items[-1], nums)
                            if items[-1].get("GST_Rate") is None:
                                rate, amt = parse_tax(blob)
                                if rate is not None:
                                    items[-1]["GST_Rate"], items[-1]["GST_Amount"] = rate, amt
                        continue

                    if section == "services":
                        k0 = norm_key(texts[0][1])
                        if k0.startswith(K_CATEGORYNAME):
                            # each service category starts a new service line
                            service_category = clean_value(re.split(r":+", strip_noise(texts[0][1]), maxsplit=1)[-1])
                            items.append({"ItemNo": len(items) + 1, "ProductName": service_category,
                                          "Category": service_category})
                            service_header = None
                            continue
                        if k0.startswith(SERVICE_SKIP) or (len(texts) < 2 and not k0.startswith(K_SERVICE_DESC)):
                            continue
                        if not items:
                            items.append({"ItemNo": 1, "Category": service_category})
                        item = items[-1]
                        if k0.startswith(K_SERVICE_DESC):
                            # custom-bid services: the real description of the service
                            vals = [clean_value(c) for _, c in texts[1:] if clean_value(c)]
                            if not vals:  # whole row arrived as one text blob after a page break
                                rest = " ".join(clean_lines(texts[0][1]))
                                rest = LOOSE_SERVICE_LABEL_RE.sub("", rest).strip()
                                # the bold label is often interleaved with the value line by line:
                                # drop the bold (doubled) words, keep the normal text
                                rest = " ".join(t for t in rest.split()
                                                if not (_is_doubled(t) and re.search(r"[A-Za-z/]", t)))
                                vals = [rest] if rest else []
                            if item.get("_desc_done"):
                                item = {"ItemNo": len(items) + 1, "Category": service_category}
                                items.append(item)
                            item["_desc_done"] = True
                            if vals:
                                item["ProductName"] = vals[0]
                            words = [v for v in vals[1:] if to_number(v) is None]
                            nums = [to_number(v) for v in vals[1:] if to_number(v) is not None]
                            if words:
                                item["Unit"] = words[0]
                            if nums:
                                item["TotalPrice"] = nums[-1]
                            continue
                        if k0.startswith(K_DESCRIPTION) and len(texts) >= 2 and len(k0) < 40:
                            service_header = {i: clean_value(c) for i, c in texts[1:]}
                            continue
                        if k0.startswith(K_TOTAL_INCL_ADDONS) or k0.startswith(K_TOTAL_WITHOUT_ADDONS):
                            nums = [to_number(clean_value(c)) for _, c in texts[1:]]
                            nums = [n for n in nums if n is not None]
                            if nums and (k0.startswith(K_TOTAL_INCL_ADDONS) or item.get("TotalPrice") is None):
                                item["TotalPrice"] = nums[-1]
                            continue
                        # a service parameter row: "Type of Service | Outstation | 3 | 14000"
                        name = clean_value(texts[0][1])
                        param_vals = []
                        for i, c in texts[1:]:
                            v = clean_value(c)
                            if v is None:
                                continue
                            head = (service_header or {}).get(i)
                            hk = norm_key(head or "")
                            if head and (hk.startswith("numberof") or "quantity" in hk) and to_number(v) is not None:
                                item["Quantity"], item["Unit"] = to_number(v), item.get("Unit") or head
                            elif head and re.search(r"fare|price|rate|cost|amount", hk) and to_number(v) is not None:
                                item["UnitPrice"] = to_number(v)
                            elif head:
                                specs.append({"Product": item.get("ProductName"), "Group": "Service",
                                              "Specification": head, "Value": v})
                            else:
                                param_vals.append(v)
                        if name and param_vals:
                            specs.append({"Product": item.get("ProductName"), "Group": "Service",
                                          "Specification": name, "Value": " ".join(param_vals)})
                            details = item.setdefault("_details", [])
                            details.append(f"{name}: {' '.join(param_vals)}")
                        continue

                    if section == "consignee":
                        if first_norm.startswith(("sno", "slno")) or K_DELIVERYSTART in norm_key(joined):
                            if K_SERVICE_DESC_SHORT in norm_key(joined):
                                consignee_services = True
                            continue  # header row
                        if first_norm.startswith(CONSIGNEE_STOP):
                            section = "other"
                            continue
                        vals = [(i, c) for i, c in texts]
                        info_idx = next((n for n, (_, c) in enumerate(vals)
                                         if re.search(r"(?i)(email id|address|designation|contact)\s*:", strip_noise(c))), None)
                        if len(vals) == 1 and info_idx is not None and re.search(DATE_IN_TEXT, strip_noise(vals[0][1])):
                            # A consignee row that crosses a page break arrives as one text blob: either the
                            # rest of the previous row, or (if that row already has its dates) a whole new row.
                            if not consignees or consignees[-1].get("DeliveryEnd"):
                                prev_sno = consignees[-1].get("SNo") if consignees else 0
                                consignees.append({"SNo": (prev_sno or 0) + 1})
                            merge_consignee_blob(consignees[-1], vals[0][1])
                            current_consignee = {k: v for k, v in consignees[-1].items()
                                                 if k not in ("Item", "LotNo", "Quantity", "DeliveryStart", "DeliveryEnd")}
                            continue
                        if info_idx is not None:
                            info = sanitize_consignee(parse_block(vals[info_idx][1], CONSIGNEE_MAP))
                            if (len(vals) == 1 and consignees and current_consignee is not None
                                    and not any(info.get(k) for k in ("Email", "Designation", "Name"))):
                                # only the address/contact of the previous consignee, pushed onto the next page
                                for line in consignees:
                                    if line.get("SNo") == consignees[-1].get("SNo"):
                                        for k, v in info.items():
                                            line.setdefault(k, v)
                                current_consignee.update({k: v for k, v in info.items() if k not in current_consignee})
                                continue
                            sno = clean_value(vals[0][1]) if info_idx > 0 else None
                            prev_sno = consignees[-1].get("SNo") if consignees else 0
                            sno_num = to_number(sno) if sno else None
                            current_consignee = {"SNo": sno_num if sno_num is not None else (prev_sno or 0) + 1, **info}
                            rest = [clean_value(c) for _, c in vals[info_idx + 1:]]
                        elif current_consignee is not None:
                            rest = [clean_value(c) for _, c in vals]
                            has_date = rest and DATE_RE.match(rest[-1] or "")
                            has_qty = len(rest) >= 2 and any(to_number(r) is not None for r in rest[1:] if r)
                            if not (has_date or has_qty or (consignee_services and len(rest) >= 2)):
                                continue
                        else:
                            continue
                        rest = [r for r in rest if r is not None]
                        dates = []
                        while rest and DATE_RE.match(rest[-1] or ""):
                            dates.insert(0, rest.pop())
                        if not rest and info_idx is None:
                            continue
                        line = dict(current_consignee)
                        if len(rest) >= 3:
                            line["Item"], line["LotNo"], line["Quantity"] = rest[0], rest[1], rest[2]
                        elif len(rest) == 2:
                            line["Item"], line["Quantity"] = rest
                        elif rest:
                            line["Item"] = rest[0]
                        if len(dates) >= 2:
                            line["DeliveryStart"], line["DeliveryEnd"] = dates[0], dates[1]
                        elif dates:
                            line["DeliveryEnd"] = dates[0]
                        if isinstance(line.get("Quantity"), str):
                            q = to_number(line["Quantity"])
                            line["Quantity"] = q if q is not None else line["Quantity"]
                        consignees.append(line)
                        continue

                    if section == "spec" and spec_product:
                        vals = [clean_value(c) for _, c in texts]
                        vals = [v for v in vals if v is not None]
                        if not vals or "subspec" in norm_key(" ".join(vals)):
                            continue  # header row (Specification | Sub-Spec | Value)
                        if len(vals) >= 3:
                            spec_group, name, value = vals[0], vals[1], " ".join(vals[2:])
                        elif len(vals) == 2:
                            name, value = vals
                        else:
                            continue
                        if name.lower() == "image":
                            continue
                        specs.append({"Product": spec_product, "Group": spec_group, "Specification": name, "Value": value})
                        continue

                    if section == "epbg":
                        text = clean_value(joined)
                        if text and not g and ":" not in text and not contract.get("ePBG_Detail"):
                            contract["ePBG_Detail"] = text[:500]
                        continue

    # ---- finalise ----
    contract_no = contract.get("ContractNo") or Path(pdf_path).stem
    contract["ContractNo"] = contract_no
    items = [finish_item(i) for i in items]
    items = [i for i in items if any(i.get(k) not in (None, "") for k in
                                     ("ProductName", "Quantity", "TotalPrice", "ServiceDetails"))]
    if len(items) == 1:
        for line in consignees:
            if not line.get("Item"):
                line["Item"] = items[0].get("ProductName")
            if line.get("Quantity") in (None, "") and len(consignees) == 1:
                line["Quantity"] = items[0].get("Quantity")
    item_sum = sum(i["TotalPrice"] for i in items if isinstance(i.get("TotalPrice"), (int, float)))
    contract["TotalOrderValue"] = parse_amount(total_raw, item_sum or None) if total_raw else (item_sum or None)

    # de-duplicate specs (pdfplumber sometimes reports overlapping tables)
    seen = set()
    uniq_specs = []
    for s in specs:
        key = (s["Product"], s["Specification"], s["Value"])
        if key not in seen:
            seen.add(key)
            uniq_specs.append(s)

    return {"contract": contract, "items": items, "consignees": consignees, "specs": uniq_specs}


def safe_process(path_str):
    try:
        return path_str, process_pdf(Path(path_str)), None
    except Exception as e:  # noqa: BLE001 - report and keep going
        return path_str, None, f"{type(e).__name__}: {e}"
