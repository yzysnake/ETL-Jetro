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
