from pathlib import Path
import re
import pdfplumber
import pandas as pd

# ---------- CONFIG ----------
PDF_DIR = Path("pdfs")                     # root folder containing all PDFs (recursively)
OUTPUT_XLSX = Path("gem_contracts.xlsx")   # output Excel file name

SECTION_MARKERS = [
    "Organisation Details",
    "Buyer Details",
    "Paying Authority Details",
    "Seller Details",
    "Product Details",
    "Consignee Detail",
]

# Mapping from English field labels to column names in Contracts sheet
ORG_FIELDS = {
    "Type": "Org_Type",
    "Ministry": "Org_Ministry",
    "Department": "Org_Department",
    "Organisation Name": "Org_OrganisationName",
    "Office Zone": "Org_OfficeZone",
}

BUYER_FIELDS = {
    "Designation": "Buyer_Designation",
    "Contact No.": "Buyer_ContactNo",
    "Contact No": "Buyer_ContactNo",
    "Email ID": "Buyer_EmailID",
    "GSTIN": "Buyer_GSTIN",
    "Address": "Buyer_Address",
}

PAYING_FIELDS = {
    "Role": "Paying_Role",
    "Payment Mode": "Paying_PaymentMode",
    "Designation": "Paying_Designation",
    "Email ID": "Paying_EmailID",
    "GSTIN": "Paying_GSTIN",
    "Address": "Paying_Address",
}

SELLER_FIELDS = {
    "GeM Seller ID": "Seller_GeMSellerID",
    "Company Name": "Seller_CompanyName",
    "Contact No.": "Seller_ContactNo",
    "Contact No": "Seller_ContactNo",
    "Email ID": "Seller_EmailID",
    "Address": "Seller_Address",
    "MSME Registration number": "Seller_MSMERegistrationNumber",
    "MSE Social Category": "Seller_MSESocialCategory",
    "MSE Gender": "Seller_MSEGender",
    "GSTIN": "Seller_GSTIN",
}

PRODUCT_FIELDS = {
    "Product Name": "ProductName",
    "Brand": "Brand",
    "Brand Type": "BrandType",
    "Catalogue Status": "CatalogueStatus",
    "Selling As": "SellingAs",
    "Category Name & Quadrant": "CategoryNameQuadrant",
    "Category Name and Quadrant": "CategoryNameQuadrant",
    "Model": "Model",
    "HSN Code": "HSNCode",
}

# ---------- HELPERS ----------

def extract_text_from_pdf(pdf_path: Path) -> str:
    text_parts = []
    with pdfplumber.open(pdf_path) as pdf:
        for page in pdf.pages:
            t = page.extract_text() or ""
            text_parts.append(t)
    return "\n".join(text_parts)

def normalize_text(raw_text: str) -> str:
    lines = []
    for line in raw_text.splitlines():
        line = re.sub(r"\s+", " ", line).strip()
        if line:
            lines.append(line)
    return "\n".join(lines)

def split_sections(text: str) -> dict:
    sections = {}
    positions = []

    for marker in SECTION_MARKERS:
        m = re.search(marker, text)
        if m:
            positions.append((marker, m.start()))

    positions.sort(key=lambda x: x[1])
    for i, (name, start) in enumerate(positions):
        end = positions[i+1][1] if i+1 < len(positions) else len(text)
        sections[name] = text[start:end]

    return sections

def parse_key_value_section(section_text: str, field_map: dict) -> dict:
    data = {}
    if not section_text:
        return data

    lines = [ln.strip() for ln in section_text.splitlines() if ln.strip()]
    i = 0
    while i < len(lines):
        line = lines[i]

        if ":" in line:
            raw_label, value = line.split(":", 1)
            raw_label = raw_label.strip()
            value = value.strip()

            if "|" in raw_label:
                eng_label = raw_label.split("|")[-1].strip()
            else:
                eng_label = raw_label.strip()
            eng_label = re.sub(r"\s+", " ", eng_label)

            col = field_map.get(eng_label)
            if col:
                extras = []
                j = i + 1
                while j < len(lines):
                    nxt = lines[j]
                    if ":" in nxt or any(m in nxt for m in SECTION_MARKERS):
                        break
                    extras.append(nxt.strip())
                    j += 1

                if extras:
                    value = value + " " + " ".join(extras)
                    i = j - 1

                data[col] = value
        i += 1

    return data

def parse_total_order_value(product_section: str):
    if not product_section:
        return None
    for line in product_section.splitlines():
        if "Total Order Value" in line:
            m = re.search(r"([\d,]+)\s*$", line)
            if m:
                return int(m.group(1).replace(",", ""))
    return None

def parse_items(product_section: str, contract_no: str):
    items = []
    if not product_section:
        return items

    lines = [ln.strip() for ln in product_section.splitlines() if ln.strip()]

    item = None
    item_no = 0
    i = 0

    while i < len(lines):
        line = lines[i]

        if "Product Name" in line and ":" in line:
            if item:
                items.append(item)

            item_no += 1
            item = {"ContractNo": contract_no, "ItemNo": item_no}
            _, value = line.split(":", 1)
            item["ProductName"] = value.strip()

        elif item is not None and ":" in line:
            raw_label, value = line.split(":", 1)
            if "|" in raw_label:
                eng_label = raw_label.split("|")[-1].strip()
            else:
                eng_label = raw_label.strip()
            eng_label = re.sub(r"\s+", " ", eng_label)

            col = PRODUCT_FIELDS.get(eng_label)
            if col:
                item[col] = value.strip()

        elif item is not None:
            m = re.match(r"^(\d+)\s+([A-Za-z]+)\s+([\d,]+)\s+(\S+)\s+([\d,]+)", line)
            if m:
                item["OrderedQuantity"] = int(m.group(1))
                item["Unit"] = m.group(2)
                item["UnitPriceINR"] = int(m.group(3).replace(",", ""))
                item["TaxBifurcationINR"] = m.group(4)
                item["PriceInclusiveINR"] = int(m.group(5).replace(",", ""))

        i += 1

    if item:
        items.append(item)

    return items

def extract_contract_date(text: str):
    m = re.search(r"\b(\d{1,2}-[A-Za-z]{3}-\d{4})\b", text)
    return m.group(1) if m else None

# ---------- MAIN PIPELINE ----------

def process_pdf(pdf_path: Path):
    print(f"Processing {pdf_path} ...")

    raw_text = extract_text_from_pdf(pdf_path)
    text = normalize_text(raw_text)

    sections = split_sections(text)

    contract_no = pdf_path.stem
    contract_date = extract_contract_date(text)

    row = {
        "ContractNo": contract_no,
        "GeneratedDate": contract_date,
    }

    row.update(parse_key_value_section(sections.get("Organisation Details", ""), ORG_FIELDS))
    row.update(parse_key_value_section(sections.get("Buyer Details", ""), BUYER_FIELDS))
    row.update(parse_key_value_section(sections.get("Paying Authority Details", ""), PAYING_FIELDS))
    row.update(parse_key_value_section(sections.get("Seller Details", ""), SELLER_FIELDS))

    product_section = sections.get("Product Details", "")
    total_order = parse_total_order_value(product_section)
    if total_order is not None:
        row["TotalOrderValueINR"] = total_order

    items = parse_items(product_section, contract_no)

    return row, items


def main():
    contracts = []
    items = []

    pdf_files = list(PDF_DIR.rglob("*.pdf"))

    if not pdf_files:
        print("No PDFs found!")
        return

    print(f"Found {len(pdf_files)} PDFs.")

    for pdf in pdf_files:
        try:
            contract_row, item_rows = process_pdf(pdf)
            contracts.append(contract_row)
            items.extend(item_rows)
        except Exception as e:
            print(f"ERROR processing {pdf}: {e}")

    df_contracts = pd.DataFrame(contracts)
    df_items = pd.DataFrame(items)

    with pd.ExcelWriter(OUTPUT_XLSX, engine="xlsxwriter") as writer:
        df_contracts.to_excel(writer, sheet_name="Contracts", index=False)
        df_items.to_excel(writer, sheet_name="Items", index=False)

    print("\nDONE!")
    print(f"Excel saved to: {OUTPUT_XLSX.resolve()}")


if __name__ == "__main__":
    main()
