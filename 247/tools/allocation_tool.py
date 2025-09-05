import re
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
