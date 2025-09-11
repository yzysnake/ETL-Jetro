from pathlib import Path
from datetime import datetime
from zoneinfo import ZoneInfo
import pandas as pd
from openpyxl import load_workbook

def read_allocation_pricesheet(folder: str = "put_your_excel_here"):
    """
    Returns
    -------
    allocation_df : pd.DataFrame  (reads 'wed' if today is Wednesday (America/Chicago), else 'mon-fri')
    price_sheet_df: pd.DataFrame  (reads 'script')
    """
    folder_path = Path(folder)
    if not folder_path.exists():
        raise FileNotFoundError(f"Folder not found: {folder_path.resolve()}")

    excel_paths = _find_excel_files(folder_path)
    if len(excel_paths) == 0 or len(excel_paths) > 2:
        raise ValueError(
            f"Expected 1–2 Excel files, found {len(excel_paths)} in {folder_path.resolve()}.\n"
            f"Files seen: {[p.name for p in excel_paths]}"
        )

    alloc_path = _pick_file_by_keyword(excel_paths, "allocation")
    price_path = _pick_file_by_keyword(excel_paths, "price")
    if alloc_path is None or price_path is None:
        raise ValueError(
            "Need one 'allocation' file and one 'price' file (case-insensitive).\n"
            f"allocation: {getattr(alloc_path, 'name', None)}\n"
            f"price     : {getattr(price_path, 'name', None)}"
        )

    # Pick allocation sheet name by weekday (America/Chicago)
    tz = ZoneInfo("America/Chicago")
    is_wed = datetime.now(tz).weekday() == 2  # Mon=0
    alloc_sheet = "wed" if is_wed else "mon-fri"

    # Validate tabs exist and are VISIBLE (ignore hidden/veryHidden/temp)
    _assert_visible_sheet(alloc_path, alloc_sheet)
    _assert_visible_sheet(price_path, "script")

    # Read exactly those sheets (raw; you can clean downstream)
    allocation_df  = pd.read_excel(alloc_path, sheet_name=alloc_sheet, header=None, engine="openpyxl")
    price_sheet_df = pd.read_excel(price_path, sheet_name="script", header=None, engine="openpyxl")

    return allocation_df, price_sheet_df


# -----------------------
# Helpers (kept minimal)
# -----------------------

def _find_excel_files(folder_path: Path):
    """*.xlsx/*.xlsm/*.xls, exclude Office temp/lock files like '~$...xlsx'."""
    pats = ("*.xlsx", "*.xlsm", "*.xls")
    files = []
    for pat in pats:
        files.extend(folder_path.glob(pat))
    return [p for p in files if p.is_file() and not p.name.startswith("~$")]

def _pick_file_by_keyword(paths, keyword: str):
    """First file whose stem contains keyword (case-insensitive)."""
    kw = keyword.lower()
    for p in paths:
        if kw in p.stem.lower():
            return p
    return None

def _visible_sheet_names(xlsx_path: Path):
    """List titles of visible sheets only (ignore hidden/veryHidden)."""
    wb = load_workbook(filename=xlsx_path, read_only=True, data_only=True)
    try:
        return [ws.title for ws in wb.worksheets if getattr(ws, "sheet_state", "visible") == "visible"]
    finally:
        wb.close()

def _assert_visible_sheet(xlsx_path: Path, sheet_name: str):
    vis = _visible_sheet_names(xlsx_path)
    if sheet_name not in vis:
        raise ValueError(
            f"Sheet '{sheet_name}' not found as a VISIBLE tab in {xlsx_path.name}.\n"
            f"Visible sheets: {vis}"
        )

def read_latest_po_csv(
    folder: str = r"C:\POs",
    delete_after: bool = False,
) -> pd.DataFrame:
    """
    Find the most recently modified .csv in `folder` (ignoring '~$' and hidden files),
    read it as one PO per line (no header, no delimiter sniffing), print PO #s, and
    return a cleaned DataFrame with columns:
        - 'PO #': original string (trimmed)
        - 'Store': left of first dash
        - 'Item' : right of first dash
    Rows are dropped if empty/NA-like OR without a dash (supports -, – or —).
    """
    folder_path = Path(folder)
    if not folder_path.exists():
        raise FileNotFoundError(f"Folder not found: {folder_path}")

    # Collect candidate CSVs
    csv_files = [
        p for p in folder_path.iterdir()
        if p.is_file()
        and p.suffix.lower() == ".csv"
        and not p.name.startswith(("~$", "."))
    ]
    if not csv_files:
        raise FileNotFoundError(f"No .csv files found in {folder_path}")

    # Pick most recently modified
    file_path = max(csv_files, key=lambda p: p.stat().st_mtime)
    mtime = datetime.fromtimestamp(file_path.stat().st_mtime).strftime("%Y-%m-%d %H:%M:%S")
    print(f"Reading latest file: {file_path.name} (modified {mtime})")

    # --- Robust single-column read: treat file as "one PO per line" ---
    lines = None
    for enc in ("utf-8-sig", "utf-16", "latin1"):
        try:
            with open(file_path, "r", encoding=enc, errors="strict") as f:
                lines = [ln.strip() for ln in f.read().splitlines()]
            break
        except UnicodeError:
            continue
    if lines is None:
        # Last resort: ignore decoding errors
        with open(file_path, "r", encoding="utf-8", errors="ignore") as f:
            lines = [ln.strip() for ln in f.read().splitlines()]

    raw = pd.DataFrame(lines, columns=[0])

    # First column only -> Series of strings
    s = raw.iloc[:, 0].astype(str).str.strip()

    # Clean/filter
    NA_STRINGS = {"", "na", "n/a", "nan", "none", "null", "nah"}
    has_value = ~s.str.lower().isin(NA_STRINGS)
    has_dash = s.str.contains(r"[-–—]", regex=True, na=False)
    s = s[has_value & has_dash]

    # Split into Store / Item on first dash-like char
    parts = s.str.split(r"[-–—]", n=1, expand=True)
    po_df = pd.DataFrame({
        "PO #": s.values,
        "Store": parts[0].str.strip(),
        "Item":  parts[1].str.strip()
    }).reset_index(drop=True)

    # Print POs
    if not po_df.empty:
        print("Received POs:")
        for po in po_df["PO #"]:
            print(po)
    else:
        print("No valid PO rows found (after cleaning).")

    return po_df