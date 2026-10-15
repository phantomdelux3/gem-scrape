"""Entry point for the GeM Contract Scraper desktop app (also used by PyInstaller).

    "GeM Scraper.exe"                                  -> opens the app
    "GeM Scraper.exe" --convert <pdf folder> <output>  -> converts without the window
                                                          (writes <output>\\convert_log.txt)
"""

import multiprocessing
import sys


def _headless_convert(pdf_dir, out_dir):
    from pathlib import Path

    from app import converter
    from app.storage import Storage, data_dir

    out = Path(out_dir)
    out.mkdir(parents=True, exist_ok=True)
    log_path = out / "convert_log.txt"
    with open(log_path, "w", encoding="utf-8") as log:
        try:
            db = Storage(data_dir() / "gem_data.db")
            res = converter.convert(db, pdf_dir, out, "gem_contracts", ["xlsx", "csv", "json"], None,
                                    log=lambda m: log.write(m + "\n"))
            log.write(f"RESULT {res['rows']} errors={res['errors']} seconds={res['seconds']:.1f}\n")
            return 0
        except Exception as e:  # noqa: BLE001
            log.write(f"ERROR {type(e).__name__}: {e}\n")
            return 1


if __name__ == "__main__":
    # Required so the PDF-reading worker processes start correctly inside the .exe
    multiprocessing.freeze_support()
    if len(sys.argv) >= 4 and sys.argv[1] == "--convert":
        sys.exit(_headless_convert(sys.argv[2], sys.argv[3]))
    from app.gui import main

    main()
