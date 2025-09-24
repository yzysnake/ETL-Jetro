import re
import numpy as np
import pandas as pd
from datetime import date, timedelta
from pathlib import Path

def clean_baby_flip_df(df: pd.DataFrame) -> pd.DataFrame:
    """
    Steps:
      1) First row -> header (treat headers as plain strings).
      2) Drop columns with empty/NA-like headers.
      3) Normalize NA-like cell strings to NaN.
      4) Drop rows with empty/NA 'Item'.
      5) Drop rows where 'Total' == 0 (numeric coercion only for this filter).
      6) Keep columns up to and including 'Lot #' (drop anything to the right).
      7) Drop rows where 'Lot #' is empty/NA.
      8) Drop 'Wgt' column if present.
      9) Rename the 3rd column to 'DESC'.
     10) For all columns strictly between 'DESC' and 'Lot #':
         values: parse as decimal → ceil → Int64
         headers: ensure not decimal by trimming trailing ".0" (integer-like -> integer string)
    """
    out = df.copy()

    # --- 1) headers from first row (as strings) ---
    header = out.iloc[0].astype(str).str.strip()
    out = out.iloc[1:].reset_index(drop=True)
    out.columns = [("" if h is None else str(h).strip()) for h in header]

    # --- 2) drop columns with empty/NA-like headers ---
    def _bad_colname(name: str) -> bool:
        s = (name or "").strip().lower()
        return s in {"", "nan", "na", "n/a", "none", "null", "nah"}
    out = out[[c for c in out.columns if not _bad_colname(c)]]

    # --- 3) normalize NA-like strings inside cells (avoid FutureWarning) ---
    NA_STRINGS = {"", "na", "n/a", "nan", "none", "null", "nah"}
    for c in out.columns:
        if out[c].dtype == "object":
            s = out[c].astype(str).str.strip()
            out[c] = s.mask(s.str.lower().isin(NA_STRINGS))

    # helper: case-insensitive exact lookup
    def _find_col(name: str):
        t = name.strip().lower()
        for c in out.columns:
            if str(c).strip().lower() == t:
                return c
        return None

    # robust numeric parser for filters / conversions
    def _to_numeric(series: pd.Series) -> pd.Series:
        s = series.astype(str).str.strip()
        s = s.str.replace(",", "", regex=True)                  # remove 1,234 commas
        s = s.str.replace(r"^\((.+)\)$", r"-\1", regex=True)    # (123.4) -> -123.4
        s = s.str.replace(r"^(.+)-$", r"-\1", regex=True)       # 123.4-  -> -123.4
        s = s.str.replace(r"[^0-9.\-]", "", regex=True)         # strip non-numeric
        return pd.to_numeric(s, errors="coerce")

    # --- 4) drop rows with empty/NA 'Item' ---
    item_col = _find_col("Item")
    if item_col is not None:
        out = out[out[item_col].notna() & (out[item_col].astype(str).str.strip() != "")]


    # --- 6) keep through 'Lot #' (inclusive), drop columns to its right ---
    lot_idx, lot_name = None, None
    for i, c in enumerate(out.columns):
        if re.fullmatch(r"lot\s*#?", str(c).strip().lower()):  # "Lot #", "Lot#", "Lot"
            lot_idx, lot_name = i, c
            break
    if lot_idx is not None:
        out = out.iloc[:, :lot_idx + 1]

    # --- 7) drop rows with empty/NA 'Lot #' ---
    if lot_name is not None and lot_name in out.columns:
        out = out[out[lot_name].notna() & (out[lot_name].astype(str).str.strip() != "")]

    # --- 8) drop 'Wgt' if present ---
    for c in list(out.columns):
        if str(c).strip().lower() == "wgt":
            out = out.drop(columns=[c])
            break

    # --- 9) rename 3rd column to 'DESC' ---
    if out.shape[1] >= 3 and out.columns[2] != "DESC":
        out = out.rename(columns={out.columns[2]: "DESC"})

    # --- 10) FINAL: process between 'DESC' and 'Lot #' ---
    cols = list(out.columns)
    desc_col = _find_col("DESC")
    # recompute current 'Lot #' name
    lot_col = None
    for c in out.columns:
        if re.fullmatch(r"lot\s*#?", str(c).strip().lower()):
            lot_col = c
            break

    if desc_col is not None and lot_col is not None:
        i_desc = cols.index(desc_col)
        i_lot  = cols.index(lot_col)
        between_cols = cols[i_desc + 1 : i_lot] if i_desc < i_lot else cols[i_lot + 1 : i_desc]

        # values: decimal -> ceil -> Int64
        for c in between_cols:
            nums = _to_numeric(out[c])
            out[c] = np.ceil(nums).astype("Int64")

        # headers: trim trailing ".0" (integer-like -> integer string)
        def _clean_header_label(lbl) -> str:
            s = str(lbl).strip()
            # numeric-looking?
            if re.fullmatch(r"[+-]?\d+(?:\.\d+)?", s):
                try:
                    f = float(s)
                    if f.is_integer():
                        return str(int(f))  # 114.0 -> '114'
                    # leave non-integer decimals as-is (e.g., '114.5')
                    return s
                except Exception:
                    return s
            return s

        rename_map = {}
        for c in between_cols:
            new_name = _clean_header_label(c)
            if new_name != c:
                rename_map[c] = new_name
        if rename_map:
            out = out.rename(columns=rename_map)

    out = out.reset_index(drop=True)
    return out

def build_baby_flip_df_cleaned_pivot(baby_flip_df_cleaned: pd.DataFrame) -> pd.DataFrame:
    df = baby_flip_df_cleaned.copy()

    # helpers
    def find_col_exact(name: str):
        t = name.strip().lower()
        for c in df.columns:
            if str(c).strip().lower() == t:
                return c
        return None

    def find_lot_col():
        for c in df.columns:
            if re.fullmatch(r"lot\s*#?", str(c).strip().lower()):  # "Lot #", "Lot#", "Lot"
                return c
        return None

    item_col = find_col_exact("Item")
    desc_col = find_col_exact("DESC")
    pack_col = find_col_exact("pack size")
    lot_col  = find_lot_col()

    if item_col is None or desc_col is None or pack_col is None or lot_col is None:
        missing = [n for n, v in [
            ("Item", item_col), ("DESC", desc_col), ("pack size", pack_col), ("Lot #", lot_col)
        ] if v is None]
        raise KeyError(f"Missing required column(s): {', '.join(missing)}")

    # store columns are strictly between DESC and Lot # (exclusive)
    cols = list(df.columns)
    i_desc = cols.index(desc_col)
    i_lot  = cols.index(lot_col)
    if i_desc < i_lot:
        store_cols = cols[i_desc + 1 : i_lot]
    else:
        store_cols = cols[i_lot + 1 : i_desc]

    # exclude 'pack size' from store columns if it happens to be there
    store_cols = [c for c in store_cols if c not in (None, "", pack_col)]

    # melt: keep Item, DESC, pack size, Lot # as id_vars so they flow through
    long_df = df.melt(
        id_vars=[item_col, desc_col, pack_col, lot_col],
        value_vars=store_cols,
        var_name="Store",
        value_name="Value"
    )

    # ---- NEW: coerce Store to integer codes (e.g., 114.0 -> 114) ----
    store_num = pd.to_numeric(long_df["Store"], errors="coerce")
    long_df = long_df[store_num.notna()].copy()
    # If any float slipped in (e.g., 114.0), round then cast to Int64
    long_df["Store"] = store_num.round().astype("Int64")

    # numeric values only, drop NaNs before summing
    long_df["Value"] = pd.to_numeric(long_df["Value"], errors="coerce")
    long_df = long_df.dropna(subset=["Value"])

    # group & sum
    pivot = (long_df
             .groupby([item_col, desc_col, pack_col, lot_col, "Store"], as_index=False, dropna=False)["Value"]
             .sum())

    # rename columns and order
    pivot = pivot.rename(columns={
        item_col: "Item",
        desc_col: "DESC",
        pack_col: "pack size",
        lot_col:  "Lot #",
    })
    pivot = pivot[["Item", "DESC", "pack size", "Lot #", "Store", "Value"]]

    # drop Value == 0 and sort by Item, then Store
    pivot = pivot[pivot["Value"] != 0].sort_values(["Item", "Store"], ascending=[True, True]).reset_index(drop=True)

    return pivot


def build_baby_flip_output(baby_flip_df_cleaned_pivot: pd.DataFrame,
                           po_df: pd.DataFrame,
                           carrier_df: pd.DataFrame) -> pd.DataFrame:
    df = baby_flip_df_cleaned_pivot.copy()

    # ---------- helpers ----------
    def find_exact_ci(frame, name: str):
        tgt = name.strip().lower()
        for c in frame.columns:
            if str(c).strip().lower() == tgt:
                return c
        return None

    def find_lot(frame):
        for c in frame.columns:
            if re.fullmatch(r"lot\s*#?", str(c).strip().lower()):   # "Lot #", "Lot#", "Lot"
                return c
        return None

    # pivot columns (case-insensitive)
    lot_col   = find_lot(df)
    store_col = find_exact_ci(df, "Store")
    val_col   = find_exact_ci(df, "Value")
    desc_col  = find_exact_ci(df, "DESC")
    pack_col  = find_exact_ci(df, "Pack Size") or find_exact_ci(df, "pack size")

    missing = [n for n, v in [
        ("Lot #", lot_col),
        ("Store", store_col),
        ("Value", val_col),
        ("DESC", desc_col),
        ("Pack Size", pack_col),
    ] if v is None]
    if missing:
        raise KeyError(f"Missing required column(s) in pivot: {', '.join(missing)}")

    # working frame with normalized names
    out = df[[lot_col, store_col, val_col, pack_col, desc_col]].rename(
        columns={
            lot_col:  "Lot #",
            store_col:"Store",
            val_col:  "Value",
            pack_col: "Pack Size",
            desc_col: "DESC",
        }
    ).copy()

    # ---------- join PO # on Store ----------
    po_store = find_exact_ci(po_df, "Store")
    po_num   = (find_exact_ci(po_df, "PO #")
                or find_exact_ci(po_df, "PO#")
                or find_exact_ci(po_df, "po #"))
    if po_store is None or po_num is None:
        raise KeyError("po_df must contain 'Store' and 'PO #' columns.")
    right_po = po_df[[po_store, po_num]].rename(columns={po_store: "Store", po_num: "PO #"}).copy()
    right_po["Store"] = right_po["Store"].astype(str).str.strip()
    out["Store"]      = out["Store"].astype(str).str.strip()
    out = out.merge(right_po, on="Store", how="left")

    # ---------- join carrier on Store ----------
    car_store = find_exact_ci(carrier_df, "Store")
    car_code  = find_exact_ci(carrier_df, "carrier code") or find_exact_ci(carrier_df, "carrier_code")
    if car_store is None or car_code is None:
        raise KeyError("carrier_df must contain 'Store' and 'carrier code' (or 'carrier_code') columns.")
    right_car = carrier_df[[car_store, car_code]].rename(columns={car_store: "Store", car_code: "carrier code"}).copy()
    right_car["Store"] = right_car["Store"].astype(str).str.strip()
    out = out.merge(right_car, on="Store", how="left")

    # ---------- Invoice Date (today, M/D/YYYY no leading zeros) ----------
    today = date.today()
    out["Invoice Date"] = f"{today.month}/{today.day}/{today.year}"

    # ---------- weight = Value * Pack Size ----------
    out["Value"]     = pd.to_numeric(out["Value"], errors="coerce")
    out["Pack Size"] = pd.to_numeric(out["Pack Size"], errors="coerce")
    out["weight"]    = (out["Value"] * out["Pack Size"]).astype("Int64")

    # ---------- final header names ----------
    out = out.rename(columns={
        "DESC": "DESC",
        "Lot #":       "LOT#",
        "Pack Size":   "pack size",
    })

    # ---------- ensure Store is integer for sorting & display ----------
    out["Store"] = pd.to_numeric(out["Store"], errors="coerce").round().astype("Int64")

    # ---------- order columns exactly as requested ----------
    cols_order = ["Store", "PO #", "Invoice Date", "DESC", "Value", "LOT#", "weight", "pack size", "carrier code"]
    out = out[cols_order]

    # ---------- sort by Store ascending ----------
    def _lot_last4(value):
        """
        Extract the last 4 digits of the LAST numeric chunk in LOT#.
        Examples:
          '498-68594 39024' -> 9024
          'ABC 123'         -> 12
          None / no digits  -> NaN (sorted last)
        """
        if pd.isna(value):
            return pd.NA
        s = str(value)
        nums = re.findall(r'\d+', s)
        if not nums:
            return pd.NA
        last_num = nums[-1]  # e.g., '39024'
        last4 = last_num[-4:]  # e.g., '9024'
        try:
            return int(last4)
        except ValueError:
            return pd.NA

    # ---------- sort by Store ascending, then LOT# by last 4 digits ----------
    # (Keeps rows with missing/invalid LOT# at the end within each Store)
    out = out.copy()
    out["_lot_last4"] = out["LOT#"].apply(_lot_last4)

    # If you want missing LOT# to sort last within each Store:
    out["_lot_last4_sort"] = out["_lot_last4"].fillna(10 ** 9)

    out = (
        out.sort_values(["Store", "_lot_last4_sort"], ascending=[True, True])
        .drop(columns=["_lot_last4", "_lot_last4_sort"])
        .reset_index(drop=True)
    )

    return out



def build_baby_flip_output_path(file_name: str, folder: str = "output_folder") -> Path:
    """
    Creates: <folder>/<file_name> IBTs <MM-DD-YY>.xlsx
    - trims/collapses spaces in file_name
    - ensures .xlsx suffix
    """
    today_str = date.today().strftime("%m-%d-%y")  # e.g., 08-29-25
    base = re.sub(r"\s+", " ", str(file_name)).strip()
    path = Path(folder) / f"{base} IBTs {today_str}.xlsx"
    return path

import importlib

HIDDEN_CHARS = {"\u200b": "", "\ufeff": "", "\xa0": " "}

def _clean_str(x):
    if pd.isna(x): return ""
    s = str(x)
    for k, v in HIDDEN_CHARS.items(): s = s.replace(k, v)
    return s.strip()

def _clean_headers(df):
    return {c: _clean_str(c) for c in df.columns}

def _drop_empty_rows(df):
    return df.loc[~df.replace("", np.nan).isna().all(axis=1)].copy()

def _reorder_columns(df, expected_cols):
    in_expected = [c for c in expected_cols if c in df.columns]
    rest = [c for c in df.columns if c not in expected_cols]
    return df[in_expected + rest]

def _to_numeric_if_possible(s: pd.Series):
    """Try to convert a column to numeric; if mostly numeric, return numeric series, else original."""
    num = pd.to_numeric(s, errors="coerce")
    # If at least half of the non-null values are numeric, keep numeric
    if num.notna().sum() >= max(1, int(s.notna().sum() * 0.5)):
        return num
    return s

def write_baby_flip_output_excel(
    output_path: str | Path,
    baby_flip_df_output: pd.DataFrame,
    baby_flip_df: pd.DataFrame,
    baby_flip_df_cleaned: pd.DataFrame,
    baby_flip_df_cleaned_pivot: pd.DataFrame,
    po_df: pd.DataFrame,
    carrier_df: pd.DataFrame,
    invoice_date_col: str = "Invoice Date",
) -> Path:
    """
    Sheets:
      - 'Araho Sheet'       -> baby_flip_df_output  (Store -> Column; numeric cols written as numbers; Invoice Date = TODAY())
      - 'RD master'         -> baby_flip_df
      - 'RD clean'          -> baby_flip_df_cleaned
      - 'Last Level Master' -> baby_flip_df_cleaned_pivot
      - 'PO#'               -> po_df        (NO header)
      - 'carriers'          -> carrier_df   (NO header)
    """
    path = Path(output_path)
    if path.suffix.lower() != ".xlsx":
        path = path.with_suffix(".xlsx")
    path.parent.mkdir(parents=True, exist_ok=True)

    # Require xlsxwriter for reliable formatting
    if importlib.util.find_spec("xlsxwriter") is None:
        engine = "openpyxl"
    else:
        engine = "xlsxwriter"

    # ---------- Prepare Araho Sheet ----------
    araho = baby_flip_df_output.copy()

    # Rename Store -> Column (case-insensitive)
    store_col = next((c for c in araho.columns if str(c).strip().lower() == "store"), None)
    if store_col is not None:
        araho = araho.rename(columns={store_col: "Column"})

    # Identify columns by intended type
    # Text columns we should NOT force to numeric
    text_cols = set()
    for cand in ["PO #", "description", "LOT#", invoice_date_col]:
        for c in araho.columns:
            if str(c).strip().lower() == cand.strip().lower():
                text_cols.add(c)

    # Try to make everything else numeric (so Excel stores numbers as numbers)
    for c in araho.columns:
        if c in text_cols:
            continue
        araho[c] = _to_numeric_if_possible(araho[c])

    with pd.ExcelWriter(path, engine=engine, date_format="m/d/yyyy", datetime_format="m/d/yyyy") as writer:
        # ----- Araho Sheet -----
        araho.to_excel(writer, sheet_name="Araho Sheet", index=False)

        ws = writer.sheets["Araho Sheet"]
        # Only create formats if using xlsxwriter
        if engine == "xlsxwriter":
            wb = writer.book
            date_fmt = wb.add_format({"num_format": "m/d/yyyy"})
            # autosize + set date formulas
            araho_cols = list(araho.columns)
            # find the index of invoice date if present
            date_idx = None
            for i, c in enumerate(araho_cols):
                if str(c).strip().lower() == invoice_date_col.strip().lower():
                    date_idx = i
                    break

            # autosize (use header+data lengths)
            for i, col in enumerate(araho_cols):
                # Don't force a text format on numeric columns — just autosize
                try:
                    max_len = max(len(str(col)), int(araho[col].astype(str).map(len).max() or 0))
                except Exception:
                    max_len = len(str(col))
                ws.set_column(i, i, min(max_len + 2, 60))

            # If Invoice Date exists, overwrite cells with TODAY() so it's a true Excel date
            if date_idx is not None:
                for r in range(len(araho)):
                    ws.write_formula(r + 1, date_idx, "=TODAY()", date_fmt)

            ws.freeze_panes(1, 0)

        # ----- Other sheets -----
        # pandas will write numeric dtypes as Excel numbers automatically.
        baby_flip_df.to_excel(writer, sheet_name="RD master", index=False)
        baby_flip_df_cleaned.to_excel(writer, sheet_name="RD clean", index=False)
        baby_flip_df_cleaned_pivot.to_excel(writer, sheet_name="Last Level Master", index=False)

        # ----- PO# (headerless) -----
        po_tmp = po_df.copy()
        po_store = next((c for c in po_tmp.columns if str(c).strip().lower() == "store"), None)
        if po_store is not None:
            po_tmp[po_store] = _to_numeric_if_possible(po_tmp[po_store])
        po_tmp.to_excel(writer, sheet_name="PO#", index=False, header=False)

        # autosize for PO#
        ws_po = writer.sheets["PO#"]
        for i, col in enumerate(po_tmp.columns):
            try:
                content_len = int(po_tmp[col].astype(str).map(len).max() or 0)
            except Exception:
                content_len = 0
            ws_po.set_column(i, i, min(content_len + 2, 60))

        # ----- carriers (headerless) -----
        car_tmp = carrier_df.copy()
        car_store = next((c for c in car_tmp.columns if str(c).strip().lower() == "store"), None)
        car_code  = next((c for c in car_tmp.columns if str(c).strip().lower().replace("_"," ") == "carrier code"), None)
        if car_store is not None:
            car_tmp[car_store] = _to_numeric_if_possible(car_tmp[car_store])
        if car_code is not None:
            car_tmp[car_code] = _to_numeric_if_possible(car_tmp[car_code])

        car_tmp.to_excel(writer, sheet_name="carriers", index=False, header=False)

        # autosize for carriers
        ws_car = writer.sheets["carriers"]
        for i, col in enumerate(car_tmp.columns):
            try:
                content_len = int(car_tmp[col].astype(str).map(len).max() or 0)
            except Exception:
                content_len = 0
            ws_car.set_column(i, i, min(content_len + 2, 60))

    return path