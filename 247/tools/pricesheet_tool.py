import re
from pathlib import Path
from datetime import datetime
from zoneinfo import ZoneInfo
from decimal import Decimal, InvalidOperation
import pandas as pd

def clean_price_sheet_df(price_sheet_df: pd.DataFrame) -> pd.DataFrame:
    """
    Steps:
      0) Drop the very first row.
      1) Use the (new) first row as the header, then drop that header row.
      2) For all columns to the RIGHT of 'FOB', strip trailing .0/.00 in numeric column names.
      3) Drop rows where 'Item#' is 0/'0'/''/NaN (fallback to first column if not found).
      4) Drop 'Item Name' and 'FOB' columns.
    """
    if price_sheet_df.shape[0] < 2:
        raise ValueError("Not enough rows to drop the first row and promote header.")

    # 0) Drop the very first row
    df = price_sheet_df.copy().iloc[1:].reset_index(drop=True)

    # 1) Promote the new first row to header
    header = df.iloc[0].astype(str).str.strip().tolist()
    df = df.iloc[1:].reset_index(drop=True)
    df.columns = header

    # 2) Normalize column names to the RIGHT of 'FOB'
    fob_idx = _get_column_index(df.columns, "FOB")
    if fob_idx is None:
        raise ValueError(f"'FOB' column not found. Available columns: {list(df.columns)}")

    new_cols = list(df.columns)
    for i in range(fob_idx + 1, len(new_cols)):  # only right of FOB
        new_cols[i] = _strip_trailing_decimal_in_colname(new_cols[i])
    new_cols = _dedupe(new_cols)  # ensure uniqueness after renaming
    df.columns = new_cols

    # 3) Keep only non-empty/non-zero Item#
    item_col = _resolve_column_name(df.columns, "Item#") or df.columns[0]
    mask_keep = ~df[item_col].apply(_is_zero_or_empty_like)
    df = df.loc[mask_keep].reset_index(drop=True)

    # 4) Drop 'Item Name' and 'FOB'
    cols_to_drop = []
    for name in ("Item Name", "FOB"):
        col = _resolve_column_name(df.columns, name)
        if col is not None:
            cols_to_drop.append(col)
    if cols_to_drop:
        df = df.drop(columns=cols_to_drop)

    return df

def build_price_sheet_long(cleaned_price_sheet_df: pd.DataFrame) -> pd.DataFrame:
    """
    Convert cleaned_price_sheet_df from wide to long.

    Output columns:
      - Store#: headers from cleaned_price_sheet_df (all columns except 'Item#')
      - Item#:  from the 'Item#' column
      - Vendor#: constant 81214
      - Cost:   cell value at (Item#, Store#)

    Notes:
      - Drops rows where Cost is NaN.
      - Post-processing: Store# 490 -> 498; remove Store# 457 and 453.
    """
    if "Item#" not in cleaned_price_sheet_df.columns:
        raise ValueError("Expected 'Item#' column in cleaned_price_sheet_df.")

    # All store columns are everything except 'Item#'
    store_cols = [c for c in cleaned_price_sheet_df.columns if c != "Item#"]
    if not store_cols:
        raise ValueError("No store columns found (expected columns besides 'Item#').")

    # Melt wide -> long
    long_df = cleaned_price_sheet_df.melt(
        id_vars=["Item#"],
        value_vars=store_cols,
        var_name="Store#",
        value_name="Cost",
    )

    # Add constant Vendor#
    long_df["Vendor#"] = 81214

    # Reorder columns
    long_df = long_df[["Store#", "Item#", "Vendor#", "Cost"]]

    # Drop rows with no cost
    long_df = long_df.dropna(subset=["Cost"]).reset_index(drop=True)

    # ---- Post-processing on Store# ----
    # Normalize to string for safe comparison
    long_df["Store#"] = long_df["Store#"].astype(str).str.strip()

    # Replace 490 -> 498
    long_df["Store#"] = long_df["Store#"].replace({"490": "498"})

    # Filter out 457 and 453
    long_df = long_df[~long_df["Store#"].isin(["457", "453"])].reset_index(drop=True)

    return long_df

def write_DLPM_file(cleaned_price_sheet_long_df: pd.DataFrame, initials: str, folder: str = "output_folder") -> Path:
    """
    Build a keystroke TXT from cleaned_price_sheet_long_df.

    Inputs
    ------
    cleaned_price_sheet_long_df : DataFrame with columns ['Store#','Item#','Vendor#','Cost']
    initials : str (e.g., 'p.y')
    folder   : output folder (default 'output_folder')

    Output
    ------
    Path to the written TXT file named: 'MM-DD-YY 247DLPM.txt'
    (Inside the file, the date is 'mm/dd/yy'.)
    """
    required = {"Store#", "Item#", "Vendor#", "Cost"}
    missing = required - set(cleaned_price_sheet_long_df.columns)
    if missing:
        raise ValueError(f"Missing required columns: {sorted(missing)}")

    # Date strings
    tz = ZoneInfo("America/Chicago")
    today = datetime.now(tz)
    date_for_file = today.strftime("%m-%d-%y")   # filename-friendly
    date_for_text = today.strftime("%m/%d/%y")   # as in Excel TEXT(TODAY(),"mm/dd/yy")

    # Output path
    out_dir = Path(folder)
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / f"{date_for_file} 247DLPM.txt"

    # Helpers
    def _fmt_item(item) -> str:
        # Try to render as zero-padded 7-digit if numeric, else pass through as string
        s = str(item).strip()
        try:
            n = int(float(s))
            return f"{n:07d}"
        except Exception:
            return s

    def _fmt_cost(val) -> str:
        try:
            return f"{Decimal(str(val)):.2f}"
        except (InvalidOperation, ValueError):
            # Fallback—best effort
            try:
                return f"{float(val):.2f}"
            except Exception:
                return str(val)

    # Build lines
    lines = []
    for _, row in cleaned_price_sheet_long_df.iterrows():
        store = str(row["Store#"]).strip()
        item  = _fmt_item(row["Item#"])
        vendor = str(row["Vendor#"]).strip()
        cost = _fmt_cost(row["Cost"])

        # Template per row
        lines.extend([
            "Key Tab",
            f"Type {store}-{item}",
            "Key Tab",
            "Key Delete",
            "Type H",
            "Key Tab",
            "Type A",
            "Key Enter",
            f"Type {date_for_text}",
            "Key Tab",
            "Key Tab",
            "Key Tab",
            f"Type {initials}",
            "Key Tab",
            "Key Tab",
            "Key Tab",
            "Key Tab",
            f"Type {vendor}",
            "Key Tab",
            "Key Tab",
            "Key Tab",
            "Key Tab",
            "Key Tab",
            f"Type {cost}",
            "Key Enter",
            "Type n",
            "Key Enter",
            "Key Enter",
            "Key Enter",
            "Key Enter",
            "Key Enter",
            "Key Enter",
        ])

    # Write file
    out_path.write_text("\n".join(lines), encoding="utf-8")
    return out_path

# ---------- helpers ----------

def _get_column_index(columns, target: str):
    try:
        return list(columns).index(target)
    except ValueError:
        pass
    lower_cols = [str(c).strip().lower() for c in columns]
    t = target.strip().lower()
    return lower_cols.index(t) if t in lower_cols else None

def _resolve_column_name(columns, target: str):
    if target in columns:
        return target
    lower_map = {str(c).strip().lower(): c for c in columns}
    return lower_map.get(target.strip().lower())

def _strip_trailing_decimal_in_colname(name: str) -> str:
    s = str(name).strip()
    if re.fullmatch(r"-?\d+(?:\.\d+)?", s):
        try:
            f = float(s)
            if f.is_integer():
                return str(int(f))
        except Exception:
            pass
    return re.sub(r"(?<=\d)\.0+$", "", s)

def _is_zero_or_empty_like(val) -> bool:
    if pd.isna(val):
        return True
    s = str(val).strip()
    if s == "" or s == "0":
        return True
    try:
        return float(s) == 0.0
    except Exception:
        return False

def _dedupe(names):
    seen = {}
    out = []
    for n in names:
        base = str(n)
        if base not in seen:
            seen[base] = 0
            out.append(base)
        else:
            seen[base] += 1
            out.append(f"{base}.{seen[base]}")
    return out
