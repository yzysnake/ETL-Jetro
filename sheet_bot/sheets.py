# sheets.py
from __future__ import annotations

from typing import Any, Dict, List, Tuple
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials

from utils import (
    weekday_key,
    DAY_PREFIXES,
    is_int_str,
    clean,
    eqci,
    strip_trailing_dot_zero,
    a1,
)

# ===== CONFIG: set these =====
SERVICE_ACCOUNT_JSON = "service_account.json"
SPREADSHEET_ID = "1CItIL51Dx_d6mDOe3hiNgJX1cnjVeL2zqeFsO8mH5pE"   # <— replace with your ID
# ============================

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

# ---------- Auth / open ----------

def _client():
    creds = Credentials.from_service_account_file(SERVICE_ACCOUNT_JSON, scopes=SCOPES)
    return gspread.authorize(creds)

def open_spreadsheet():
    return _client().open_by_key(SPREADSHEET_ID)

def pick_today_worksheet(sh) -> gspread.Worksheet:
    """Choose today's tab by name prefix (Mon/Tues/Wed/Thurs/Thu/Fri)."""
    day = weekday_key()  # 'Mon'..'Fri'
    prefixes = DAY_PREFIXES[day]
    for ws in sh.worksheets():
        t = ws.title.strip().lower()
        if any(t.startswith(p) for p in prefixes):
            return ws
    return sh.get_worksheet(0)

def _get_all_values(ws: gspread.Worksheet) -> List[List[str]]:
    """Read the whole sheet as a 2D array of strings ('' for blanks)."""
    return ws.get_all_values()

# ---------- Section detection ----------

def _find_header_rows(values: List[List[str]]) -> List[int]:
    """
    Return 1-based row indexes that start a section (row containing a cell 'Note').
    """
    header_rows = []
    for r1, row in enumerate(values, start=1):
        if any(eqci(c, "Note") for c in row):
            header_rows.append(r1)
    return header_rows

def _infer_schema(header_row: List[str]) -> Dict[str, Any]:
    """
    From the header row, figure out the important columns (1-based indices):
      Note | Vendor # | Vendor Name | <store # ...> | [PO count] | Status
    """
    note_c = vnum_c = vname_c = status_c = None
    store_cols: List[int] = []

    for c1, raw in enumerate(header_row, start=1):
        name = clean(raw)
        low = name.lower()
        if eqci(name, "Note") and note_c is None:
            note_c = c1; continue
        if low in {"vendor #", "vendor#", "vendor no", "vendor number"} and vnum_c is None:
            vnum_c = c1; continue
        if low in {"vendor name", "vendor"} and vname_c is None:
            vname_c = c1; continue
        if low == "status":
            status_c = c1; continue
        if is_int_str(name):         # store columns like '165','446',...
            store_cols.append(c1); continue
        if low in {"po count", "pocount"}:
            continue                 # optional; ignore

    if not all([note_c, vnum_c, vname_c, status_c]):
        raise ValueError(
            f"Cannot infer section header: Note={note_c} Vendor#={vnum_c} "
            f"VendorName={vname_c} Status={status_c}"
        )
    return {
        "note_c": note_c,
        "vnum_c": vnum_c,
        "vname_c": vname_c,
        "status_c": status_c,
        "store_cols": store_cols,
    }

# ---------- Parse into normalized rows ----------

def parse_sections(ws: gspread.Worksheet) -> pd.DataFrame:
    """
    Parse the worksheet into one normalized DataFrame across its sections.
    Drops rows where Vendor # is empty.

    Returns columns:
      section, sheet_row, vendor_num, vendor_name, status, status_a1, stores(dict)
    """
    values = _get_all_values(ws)
    if not values:
        return pd.DataFrame([])

    header_rows = _find_header_rows(values)
    if not header_rows:
        return pd.DataFrame([])

    header_rows = sorted(header_rows)
    end_r = len(values)

    # Build [start, stop] (inclusive) per section
    ranges: List[Tuple[int, int]] = []
    for i, start_r in enumerate(header_rows):
        stop_r = (header_rows[i + 1] - 1) if i + 1 < len(header_rows) else end_r
        ranges.append((start_r, stop_r))

    recs: List[Dict[str, Any]] = []

    for sec_idx, (hdr_r, stop_r) in enumerate(ranges, start=1):
        header_row = values[hdr_r - 1]
        schema = _infer_schema(header_row)

        note_c    = schema["note_c"]
        vnum_c    = schema["vnum_c"]
        vname_c   = schema["vname_c"]
        status_c  = schema["status_c"]
        store_cols = schema["store_cols"]

        # Section label: cell under 'Note' in the first data row (if present)
        first_data_r = hdr_r + 1
        section_label = ""
        if first_data_r <= stop_r:
            row0 = values[first_data_r - 1]
            section_label = clean(row0[note_c - 1]) if note_c <= len(row0) else ""
        if not section_label:
            section_label = f"section_{sec_idx}"

        # Data rows
        for r in range(first_data_r, stop_r + 1):
            row = values[r - 1]

            vendor_num = clean(row[vnum_c - 1]) if vnum_c <= len(row) else ""
            if vendor_num == "":
                continue  # drop empty vendor rows

            vendor_name = clean(row[vname_c - 1]) if vname_c <= len(row) else ""
            status_val  = clean(row[status_c - 1]) if status_c <= len(row) else ""

            # stores dict: {header_store_code -> cell_value}
            stores: Dict[str, str] = {}
            for sc in store_cols:
                # defensive bounds check
                store_header = clean(header_row[sc - 1]) if sc <= len(header_row) else ""
                cell_val = clean(row[sc - 1]) if sc <= len(row) else ""
                stores[store_header] = cell_val

            recs.append({
                "section": section_label,
                "sheet_row": r,                   # 1-based sheet row
                "vendor_num": vendor_num,
                "vendor_name": vendor_name,
                "status": status_val,
                "status_a1": a1(r, status_c),     # exact Status cell to update later
                "stores": stores,
            })

    return pd.DataFrame.from_records(recs)

# ---------- Write-back helper ----------

def batch_update_status(ws: gspread.Worksheet, updates: List[Tuple[str, str]]):
    """
    updates = [(A1_range, new_value), ...]
    """
    if not updates:
        return
    data = [{"range": rng, "values": [[val]]} for (rng, val) in updates]
    ws.batch_update(data)

def build_po_tokens_for_ready(ws: gspread.Worksheet) -> List[Dict[str, Any]]:
    """
    For each row with Status == 'Ready', collect tokens:
       {vendor_num}-{store_number}-{po_value}
    Skips blanks and 'x'. Returns:
       [{vendor_num, vendor_name, section, tokens: [...]}, ...]
    """
    df = parse_sections(ws)
    if df.empty:
        return []

    ready = df[df["status"].str.strip().str.lower() == "ready"].copy()
    out: List[Dict[str, Any]] = []

    for _, row in ready.iterrows():
        vnum = row["vendor_num"]
        vname = row["vendor_name"]
        section = row["section"]
        tokens: List[str] = []

        for store, val in (row["stores"] or {}).items():
            sval = clean(val)
            if not sval or sval.lower() == "x":
                continue
            po = strip_trailing_dot_zero(sval)
            tokens.append(f"{vnum}-{store}-{po}")

        out.append({
            "vendor_num": vnum,
            "vendor_name": vname,
            "section": section,
            "tokens": tokens,
        })

    return out

def po_df_from_row(row: dict, po_col_name: str = "PO #") -> pd.DataFrame:
    """
    Build a 1-column DataFrame listing all PO values (unique) for one row,
    skipping blanks and 'x'. Trims trailing '.0' (e.g., '14.0' -> '14').
    """
    stores = row.get("stores", {}) or {}
    pos: List[str] = []

    for _, raw in stores.items():
        s = clean(raw)
        if not s or s.lower() == "x":
            continue
        s = strip_trailing_dot_zero(s)
        pos.append(s)

    # de-duplicate, keep order
    seen = set()
    pos_unique = []
    for p in pos:
        if p not in seen:
            seen.add(p)
            pos_unique.append(p)

    return pd.DataFrame({po_col_name: pos_unique})
