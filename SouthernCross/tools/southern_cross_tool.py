from pathlib import Path
from datetime import date,datetime
from typing import Optional, Any
from typing import List
import pandas as pd
import numpy as np
import re

def clean_southern_cross_df(df: pd.DataFrame, file_name: str) -> pd.DataFrame:
    """
    Steps:
      1) First row -> header.
      2) Clean headers: strip, remove trailing '.0' or '.00'; drop blank headers.
      3) Normalize ALL cells:
         - Empty/NaN -> 0
         - 'x.0'/'x.00' (string) -> int x
         - float  n.0  -> int n
         - keep non-integer decimals (e.g., 14.5) as float
      4) Drop 'Description' column if present.
      5) Drop 'LOT #' and every column to its right (keep only left of it).
      6) Drop rows where 'Item' == 0 (after normalization).
      7) Reorder columns alphabetically (case-insensitive) with 'Item' fixed at far left.
    """
    out = df.copy()
    if out.empty:
        return out

    # --- helpers ---
    def _strip_trailing_dot_zero_text(s: str) -> str:
        # remove exactly one '.0' or '.00' at the end of a string token
        return re.sub(r"(?:\.0{1,2})$", "", s.strip())

    def _is_bad_colname(c: Optional[str]) -> bool:
        if pd.isna(c):
            return True
        s = str(c).strip()
        return s == "" or s.lower() == "nan"

    def _norm(s: str) -> str:
        return re.sub(r"\s+", "", str(s)).upper()

    def _coerce_value(v: Any) -> Any:
        """Coerce a single cell based on the user's rules."""
        # Treat NaN/None/'' as 0
        if v is None or (isinstance(v, float) and np.isnan(v)):
            return 0
        if isinstance(v, str):
            s = v.strip()
            if s == "" or s.lower() in {"nan", "na", "none"}:
                return 0
            # remove textual trailing .0/.00
            s2 = _strip_trailing_dot_zero_text(s)
            # try to parse as number
            try:
                num = float(s2)
                # if integer-valued -> int
                if num.is_integer():
                    return int(num)
                return num
            except ValueError:
                # not numeric; keep cleaned string
                return s2
        # numeric types
        if isinstance(v, (int, np.integer)):
            return int(v)
        if isinstance(v, (float, np.floating)):
            if np.isnan(v):
                return 0
            if float(v).is_integer():
                return int(v)
            return float(v)
        # other types -> keep as-is
        return v

    # 1) First row -> header
    raw_cols = out.iloc[0].astype(str).tolist()
    out = out.iloc[1:].reset_index(drop=True)

    # 2) Clean headers (strip + drop trailing .0/.00)
    clean_cols = [_strip_trailing_dot_zero_text(str(c)) for c in raw_cols]
    out.columns = clean_cols
    good_cols = [c for c in out.columns if not _is_bad_colname(c)]
    out = out.loc[:, good_cols]

    # 3) Normalize ALL cells per rules
    # Use applymap on the entire frame (it’s safe even if dtypes are mixed)
    out = out.map(_coerce_value)

    # 4) Drop 'Description' if present (tolerant)
    desc_cols = [c for c in out.columns if _norm(c) == "DESCRIPTION"]
    if desc_cols:
        out = out.drop(columns=desc_cols)

    # 5) Drop 'LOT #' and everything to the right
    lot_idx = None
    for i, c in enumerate(out.columns):
        if _norm(c) in {"LOT#", "LOT", "LOTNO"}:
            lot_idx = i
            break
    if lot_idx is not None:
        out = out.iloc[:, :max(lot_idx, 0)]

    # 6) Drop rows where 'Item' == 0
    item_col = None
    for c in out.columns:
        if _norm(c) == "ITEM":
            item_col = c
            break
    if item_col is None:
        raise KeyError("Required column 'Item' was not found after header normalization.")

    out = out[out[item_col] != 0].reset_index(drop=True)

    # 7) Reorder columns A→Z, with 'Item' fixed on the left
    remaining = [c for c in out.columns if c != item_col]
    remaining_sorted = sorted(remaining, key=lambda x: str(x).lower())
    out = out[[item_col] + remaining_sorted]

    return out

def build_southern_cross_df_cleaned_pivot(cleaned_southern_cross_df: pd.DataFrame) -> pd.DataFrame:
    """
    Build long-form pivot:
      - id: 'Item#'
      - Branch: every other column
      - value: 'Distro Size' (sum duplicates)
      - Empty/NaN -> 0; values coerced to int
      - Sort Branch ascending (numeric if possible)
      - Columns order: Branch, Item, Distro Size
      - Rename Item -> Item
      - Finally, drop rows where Distro Size == 0
    """
    if "Item" not in cleaned_southern_cross_df.columns:
        raise ValueError("Expected 'Item' column in cleaned_allocation_df.")

    df = cleaned_southern_cross_df.copy()

    branch_cols: List[str] = [c for c in df.columns if c != "Item"]
    if not branch_cols:
        return pd.DataFrame(columns=["Branch", "Item", "Distro Size"])

    long_df = df.melt(
        id_vars=["Item"],
        value_vars=branch_cols,
        var_name="Branch",
        value_name="Distro Size",
    )

    # Normalize Branch header text
    long_df["Branch"] = (
        long_df["Branch"].astype(str).str.strip().str.replace(r"\.0$", "", regex=True)
    )

    # Coerce values -> numeric -> int, empty/NaN -> 0
    long_df["Distro Size"] = (
        pd.to_numeric(long_df["Distro Size"], errors="coerce")
        .fillna(0)
        .astype(int)
    )

    agg_df = (
        long_df.groupby(["Branch", "Item"], as_index=False)["Distro Size"]
        .sum()
    )

    # Sort Branch numerically when possible
    branch_num = pd.to_numeric(agg_df["Branch"], errors="coerce")
    agg_df = (
        agg_df.assign(_branch_num=branch_num)
        .sort_values(by=["_branch_num", "Branch"])
        .drop(columns="_branch_num")
    )

    # Reorder
    agg_df = agg_df.loc[:, ["Branch", "Item", "Distro Size"]]

    # Drop zero rows
    agg_df = agg_df[agg_df["Distro Size"] != 0].reset_index(drop=True)

    return agg_df


def build_southern_cross_output(df: pd.DataFrame, edd: str, buyer: str = "P2M", supplier: int = 80104) -> pd.DataFrame:
    """
    Append ACME output columns:
      - 'Supplier On Record' = supplier (all rows)
      - 'Expected Delivery Date' = edd (string like '9/15/2025', all rows)
      - 'WW Buyer' = buyer (all rows)
      - 'Warehouse','AdditionalXDCK','AmountCode','XDCK','POSTXDCK','FOB' = blank (all rows)
    Then:
      - For column 'Branch' (case-insensitive): if the cell is exactly two digits (e.g. '86'), change to three digits by prefixing '1' (-> '186').
    """
    if not isinstance(edd, str) or not edd.strip():
        raise ValueError("edd must be a non-empty string in date format like '9/15/2025'.")

    out = df.copy()

    # Add required columns
    out["Supplier On Record"] = supplier
    out["Expected Delivery Date"] = edd
    out["WW Buyer"] = buyer
    out["Warehouse"] = ""
    out["AdditionalXDCK"] = ""
    out["AmountCode"] = ""
    out["XDCK"] = ""
    out["POSTXDCK"] = ""
    out["FOB"] = ""

    # Fix Branch values (two digits -> prefix '1')
    branch_col = next((c for c in out.columns if c.lower() == "branch"), None)
    if branch_col is None:
        raise ValueError("Column 'Branch' not found in the dataframe.")

    # Work with strings to check digit count, then convert back to numeric if possible
    s = out[branch_col].astype(str).str.strip()
    mask_two_digits = s.str.fullmatch(r"\d{2}")
    s.loc[mask_two_digits] = "1" + s.loc[mask_two_digits]
    # Convert back to numeric where possible (keeps numbers as numbers for Excel)
    out[branch_col] = pd.to_numeric(s)

    return out


def build_southern_cross_output_path(file_name: str, folder: str = "output_folder") -> Path:
    path = Path(folder) / f" Mega Script {file_name}.xlsx"
    return path



CANONICAL_COLS = [
    'Branch','Item','Description','Distro Size','Supplier On Record','Expected Delivery Date',
    'WW Buyer','Warehouse','AdditionalXDCK','AmountCode','XDCK','POSTXDCK','FOB'
]

def write_southern_cross_output_excel(df: pd.DataFrame, path: str) -> None:
    df = df.copy().reindex(columns=CANONICAL_COLS)

    # numeric cols
    for c in ['Branch','Item','Distro Size']:
        df[c] = pd.to_numeric(df[c], errors='coerce').fillna(0).astype(int)
    for c in ['XDCK','FOB']:
        df[c] = pd.to_numeric(df[c], errors='coerce')

    # ensure real dates (accepts both 2- and 4-digit year strings)
    if 'Expected Delivery Date' in df.columns:
        df['Expected Delivery Date'] = pd.to_datetime(df['Expected Delivery Date'], errors='coerce').dt.date

    # text -> blanks
    for c in ['Description','Supplier On Record','WW Buyer','Warehouse','AdditionalXDCK','AmountCode','POSTXDCK']:
        df[c] = df[c].astype(object).where(df[c].notna(), "")

    # sort
    df = df.sort_values(by=['Branch','Item','Distro Size'], ascending=[True, True, True]).reset_index(drop=True)

    # write workbook/sheets
    with pd.ExcelWriter(path, engine='openpyxl') as writer:
        df.to_excel(writer, index=False, sheet_name='Scripting')
        writer.book.create_sheet("ANOMALY")
        writer.book.create_sheet("STORE CLUSTER")

        # >>> Format the date column as m/d/yyyy (no leading zeros, 4-digit year)
        ws = writer.book['Scripting']
        date_col_idx = df.columns.get_loc('Expected Delivery Date') + 1
        for r in range(2, ws.max_row + 1):
            cell = ws.cell(row=r, column=date_col_idx)
            if cell.value is not None:
                cell.number_format = "m/d/yyyy"

        # blank NaNs in XDCK/FOB
        for name in ['XDCK','FOB']:
            ci = df.columns.get_loc(name) + 1
            for r in range(2, ws.max_row + 1):
                if ws.cell(row=r, column=ci).value is None:
                    ws.cell(row=r, column=ci).value = ""


def _fmt_item_code(item_value):
    if pd.isna(item_value):
        return ""
    s = str(item_value).strip()
    if s.endswith(".0"):
        s = s[:-2]
    digits = "".join(ch for ch in s if ch.isdigit())
    if digits == "":
        return s
    return digits.zfill(7)

def _fmt_edd_mmddyy(edd_str):
    try:
        dt = datetime.strptime(edd_str.strip(), "%m/%d/%Y")
    except Exception:
        try:
            dt = datetime.strptime(edd_str.strip(), "%-m/%-d/%Y")
        except Exception:
            return edd_str
    return dt.strftime("%m/%d/%y")

def write_ADPO_X_file(cleaned_acme_output_df: pd.DataFrame, folder: str = "output_folder") -> Path:
    if cleaned_acme_output_df.empty:
        raise ValueError("cleaned_acme_output_df is empty.")

    df = cleaned_acme_output_df.copy()
    branch_num = pd.to_numeric(df["Branch"], errors="coerce")
    df = (
        df.assign(_branch_num=branch_num)
          .sort_values(by=["_branch_num", "Branch", "Item"])
          .drop(columns="_branch_num")
          .reset_index(drop=True)
    )

    supplier = str(df["Supplier On Record"].iloc[0]).strip()
    if supplier.endswith(".0"):
        supplier = supplier[:-2]
    supplier_digits = "".join(ch for ch in supplier if ch.isdigit()) or supplier

    buyer = str(df["WW Buyer"].iloc[0]).strip() or "P20"
    today_str = date.today().strftime("%Y-%m-%d")

    def clipboard_block_lines():
        block = (
            "wait 3000\n"
            "EditSelect 13,39,13,47\n"
            "key EditCopy\n"
            "wait 1000\n"
            f"FileSpec clipboard,C:\\POs\\VendorNo-{supplier_digits}-{today_str}.csv,append\n"
            "key EditSaveClipboard\n"
            "wait 1000\n"
            f"FileSpec clipboard,\\\\10.1.12.12\\faxshare\\DailyPOCount\\POs\\{today_str}_{buyer}.csv,append\n"
            "key EditSaveClipboard\n"
            "key PA2\n"
            "type \"adpo,x\"\n"
            "key enter"
        )
        return [ln for ln in block.splitlines() if ln.strip() != ""]

    lines = []

    for branch, g in df.groupby("Branch", sort=False):
        edd_str = str(g["Expected Delivery Date"].iloc[0]).strip()
        edd_mmddyy = _fmt_edd_mmddyy(edd_str)

        lines += [
            "Key tab",
            f"Type {buyer}",
            f"Type {branch}",
            f"Type {supplier_digits}",
            "Key Enter",
        ]

        for _, row in g.iterrows():
            item_code = _fmt_item_code(row["Item"])
            qty = row["Distro Size"]
            try:
                qty_int = int(pd.to_numeric(qty, errors="coerce"))
            except Exception:
                qty_int = 0

            lines += [
                f"Type  {branch}-{item_code}",
                "Key enter",
                "Key tab",
                "Key delete",
                "Key delete",
                "Key delete",
                "Key delete",
                f"Type  {qty_int}",
                "Key Enter",
                "Key PF24",
            ]

        lines += [
            f"Type  {branch}-0990033",
            "Key Enter",
            "Key tab",
            "Key delete",
            "Key delete",
            "Key delete",
            "Key delete",
            "Type 0",
            "Key Enter",
            "Key PF13",
            "Key Enter",
            f"Type {edd_mmddyy}",
            "Key Enter",
            "Key Enter",
        ]

        lines.extend(clipboard_block_lines())

    # Build text
    script_text = "\n".join(str(ln).replace("\r", "") for ln in lines)

    # Remove any spaces before newline globally (handles 'Key Enter \n' etc.)
    script_text = re.sub(r"[ \t]+(\n)", r"\1", script_text)

    # Also remove any blank lines
    script_text = re.sub(r"\n{2,}", "\n", script_text)

    out_dir = Path(folder)
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / f"{today_str}_ADPO_X_Vendor{supplier_digits}.txt"
    with open(out_path, "w", encoding="utf-8", newline="\n") as f:
        f.write(script_text)
    return out_path


















