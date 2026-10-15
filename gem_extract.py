#!/usr/bin/env python3
"""
gem_extract.py - command-line PDF -> Excel conversion (the desktop app uses the same code).

Usage:
    python gem_extract.py                      # all PDFs under pdfs/ -> data_exports/gem_contracts.xlsx
    python gem_extract.py -t 20                # test on the first 20 PDFs
    python gem_extract.py --no-cache           # re-read every PDF
    python gem_extract.py --formats xlsx csv json
"""

import argparse
import multiprocessing
import sys
from pathlib import Path

from app import converter
from app.storage import Storage


def main():
    parser = argparse.ArgumentParser(description="Extract GeM contract PDFs into Excel / CSV / JSON.")
    parser.add_argument("-t", "--test", type=int, help="process only the first N PDFs.")
    parser.add_argument("--no-cache", action="store_true", help="ignore cached results and re-read every PDF.")
    parser.add_argument("--workers", type=int, default=converter.default_workers(), help="parallel workers.")
    parser.add_argument("--pdf-dir", default="pdfs", help="folder to scan for PDFs (default: pdfs).")
    parser.add_argument("--out", default="data_exports/gem_contracts.xlsx", help="output path (name sets the base name).")
    parser.add_argument("--formats", nargs="+", default=["xlsx"], choices=["xlsx", "csv", "json"])
    parser.add_argument("--db", help="SQLite database (default: gem_data.db next to the output file). Use the app's "
                                     "'GeM Data/gem_data.db' to include its search results.")
    args = parser.parse_args()

    out = Path(args.out)
    db = Storage(args.db or out.parent / "gem_data.db")
    state = {"last": -1}

    def progress(done, total, parsed, cached, errors):
        pct = int(done * 100 / total) if total else 100
        if pct != state["last"] and (pct % 5 == 0 or done == total):
            state["last"] = pct
            print(f"\r{done}/{total} PDFs ({pct}%) - cached {cached}, read {parsed}, errors {errors}",
                  end="", flush=True)

    try:
        res = converter.convert(db, args.pdf_dir, out.parent, out.stem, args.formats, None, args.workers,
                                progress=progress, use_cache=not args.no_cache, limit=args.test, log=print)
    except PermissionError as e:
        print(f"\nERROR: {e}")
        return 1
    print()
    if res is None:
        return 1
    for f in res["files"]:
        print("Saved", Path(f).resolve())
    print(", ".join(f"{k}: {v}" for k, v in res["rows"].items()), f"| errors: {res['errors']}",
          f"| {res['seconds']:.1f}s")
    return 0


if __name__ == "__main__":
    multiprocessing.freeze_support()
    sys.exit(main())
