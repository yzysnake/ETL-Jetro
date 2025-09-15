import re
import pandas as pd
from typing import List
from pathlib import Path
from datetime import date, timedelta,datetime

def clean_allocation_df(allocation_df: pd.DataFrame) -> pd.DataFrame:
    """
    Steps:
      1) Drop the first row.
      2) Make the (new) first row the header (original second row), then remove that header row from the body.
      3) Drop the 'Total' column and everything to its right (keep only columns left of 'Total').
      4) Clean header names by removing a trailing '.0' and stripping whitespace.
      5) Drop the last row.
      6) Drop the 'Item Description' column (case/space-insensitive) if present.
    """
    if allocation_df.shape[0] < 2:
        raise ValueError("Expected at least 2 rows to promote the second row to header.")

    # 1) Drop the first row
    df = allocation_df.drop(allocation_df.index[0]).reset_index(drop=True)

    # 2) Promote the new first row to header, then drop that row from the body
    new_cols = df.iloc[0].astype(str).str.strip().tolist()
    df = df.drop(df.index[0]).reset_index(drop=True)
    df.columns = new_cols

    # 3) Keep only columns left of 'Total'
    norm_cols = [str(c).strip().casefold() for c in df.columns]
    try:
        total_idx = norm_cols.index("total")
        df = df.iloc[:, :total_idx]
    except ValueError:
        pass  # 'Total' not found -> keep all columns

    # 4) Clean header names: remove trailing '.0' and strip whitespace
    df.columns = [re.sub(r"\.0$", "", str(c)).strip() for c in df.columns]

    # 5) Drop the last row (if any)
    if df.shape[0] > 0:
        df = df.iloc[:-1, :].reset_index(drop=True)

    # 6) Drop 'Item Description' (case/space-insensitive) if present
    norm_map = {i: str(c).strip().casefold() for i, c in enumerate(df.columns)}
    drop_idx = [i for i, name in norm_map.items() if name == "item description"]
    if drop_idx:
        keep_cols = [c for i, c in enumerate(df.columns) if i not in drop_idx]
        df = df.loc[:, keep_cols]

    return df


def build_allocation_df_cleaned_pivot(cleaned_allocation_df: pd.DataFrame) -> pd.DataFrame:
    """
    Build long-form pivot:
      - id: 'Item#'
      - Branch: every other column
      - value: 'Distro Size' (sum duplicates)
      - Empty/NaN -> 0; values coerced to int
      - Sort Branch ascending (numeric if possible)
      - Columns order: Branch, Item#, Distro Size
      - Rename Item# -> Item
      - Finally, drop rows where Distro Size == 0
    """
    if "Item#" not in cleaned_allocation_df.columns:
        raise ValueError("Expected 'Item#' column in cleaned_allocation_df.")

    df = cleaned_allocation_df.copy()

    branch_cols: List[str] = [c for c in df.columns if c != "Item#"]
    if not branch_cols:
        return pd.DataFrame(columns=["Branch", "Item", "Distro Size"])

    long_df = df.melt(
        id_vars=["Item#"],
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
        long_df.groupby(["Branch", "Item#"], as_index=False)["Distro Size"]
        .sum()
    )

    # Sort Branch numerically when possible
    branch_num = pd.to_numeric(agg_df["Branch"], errors="coerce")
    agg_df = (
        agg_df.assign(_branch_num=branch_num)
        .sort_values(by=["_branch_num", "Branch"])
        .drop(columns="_branch_num")
    )

    # Reorder + rename
    agg_df = agg_df.loc[:, ["Branch", "Item#", "Distro Size"]].rename(columns={"Item#": "Item"})

    # Drop zero rows
    agg_df = agg_df[agg_df["Distro Size"] != 0].reset_index(drop=True)

    return agg_df


def build_allocation_output(
    df: pd.DataFrame,
    edd: str ,
    buyer: str = "P2M",
    supplier: int = 79906,

) -> pd.DataFrame:
    """
    Append output columns:
      - 'Supplier On Record' = supplier (all rows)
      - 'Expected Delivery Date' = edd (string like '9/15/2025', auto if omitted)
      - 'WW Buyer' = buyer (all rows)
      - 'Warehouse','AdditionalXDCK','AmountCode','XDCK','POSTXDCK','FOB' = blank (all rows)

    If `edd` is None or blank, it will be set to two days after today,
    rolling forward to Monday if that lands on a weekend.
    """
    # Determine EDD
    if not isinstance(edd, str) or not edd.strip():
        raise ValueError("edd must be a non-empty string in date format like '9/15/2025'.")

    out = df.copy()
    out["Supplier On Record"] = supplier
    out["Expected Delivery Date"] = edd
    out["WW Buyer"] = buyer
    out["Warehouse"] = ""
    out["AdditionalXDCK"] = ""
    out["AmountCode"] = ""
    out["XDCK"] = ""
    out["POSTXDCK"] = ""
    out["FOB"] = ""
    return out


def build_allocation_output_path(folder: str = "output_folder") -> Path:
    path = Path(folder) / f" Mega Script 247 Allocation {date.today()}.xlsx"
    return path

CANONICAL_COLS = [
    'Branch','Item','Description','Distro Size','Supplier On Record','Expected Delivery Date',
    'WW Buyer','Warehouse','AdditionalXDCK','AmountCode','XDCK','POSTXDCK','FOB'
]

def write_allocation_output_excel(df: pd.DataFrame, path: str) -> None:
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

def write_ADPO_X_file(cleaned_allocation_output_df: pd.DataFrame, folder: str = "output_folder") -> Path:
    if cleaned_allocation_output_df.empty:
        raise ValueError("cleaned_allocation_output_df is empty.")

    df = cleaned_allocation_output_df.copy()
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

    buyer = str(df["WW Buyer"].iloc[0]).strip() or "P2M"
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