import re
from pathlib import Path
import pandas as pd
from datetime import datetime


import json

def _clean_stem(stem: str) -> str:
    # lowercase, trim, collapse spaces
    return re.sub(r"\s+", " ", str(stem)).strip().lower()

def read_clean_file_name(folder: str = "put_your_excel_here"):
    """
    Read exactly one .xlsx from `folder` and return:
      - df: pandas DataFrame read with header=None
      - file_name: cleaned file name (no extension), lowercase, trimmed, spaces collapsed

    Raises:
      - FileNotFoundError if folder doesn't exist
      - ValueError if zero or more than one .xlsx found
    """
    folder_path = Path(folder)
    if not folder_path.exists():
        raise FileNotFoundError(f"Folder not found: {folder}")

    # collect .xlsx files (ignore Excel lock files '~$' and hidden files)
    xlsx_files = [
        p for p in folder_path.iterdir()
        if p.is_file()
        and p.suffix.lower() == ".xlsx"
        and not p.name.startswith("~$")
        and not p.name.startswith(".")
    ]

    if len(xlsx_files) == 0:
        raise ValueError(f"No .xlsx files found in: {folder}")
    if len(xlsx_files) > 1:
        names = ", ".join(sorted(f.name for f in xlsx_files))
        raise ValueError(f"Expected exactly one .xlsx file, found {len(xlsx_files)}: {names}")

    file_path = xlsx_files[0]
    # read with header=None exactly as requested
    df = pd.read_excel(file_path, header=None, engine="openpyxl")

    # cleaned file name (no extension)
    file_name = _clean_stem(file_path.stem)

    return df, file_name


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



def read_carrier_json(file_name: str, base_dir: str = "baby_flip_carrier_json") -> pd.DataFrame:
    """
    Choose and read a carrier JSON based on `file_name`:
      - contains 'salmon'   -> baby_flip_carrier_json/salmon_carrier.json
      - contains 'northern' -> baby_flip_carrier_json/northern_carrier.json
      - contains 'southern' -> baby_flip_carrier_json/southern_carrier.json

    JSON format example: {"114": 2, "123": 2, "142": 3}

    Returns:
      carrier_df: DataFrame with columns:
        - 'Store'         (string)
        - 'carrier code'  (string)
    """
    name = (file_name or "").lower()

    keys = ["salmon", "northern", "southern"]
    matches = [k for k in keys if k in name]
    if len(matches) != 1:
        raise ValueError(
            f"Expected exactly one of {keys} in file_name, found {matches or 'none'} (file_name={file_name!r})."
        )

    json_map = {
        "salmon":   "salmon_carrier.json",
        "northern": "northern_carrier.json",
        "southern": "southern_carrier.json",
    }
    json_path = Path(base_dir) / json_map[matches[0]]
    if not json_path.exists():
        raise FileNotFoundError(f"Carrier JSON not found: {json_path}")

    with open(json_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    if not isinstance(data, dict):
        raise ValueError(f"Carrier JSON must be a dict of store->code, got {type(data)}")

    carrier_df = pd.DataFrame({
        "Store": [str(k).strip() for k in data.keys()],
        "carrier code": [str(v).strip() for v in data.values()],
    })

    return carrier_df



