import re
import pandas as pd
from typing import List
from pathlib import Path
from datetime import date, timedelta

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


def _compute_default_edd() -> str:
    """Two days after today; if weekend, roll forward to Monday. Format m/d/YYYY."""
    edd_date = date.today() + timedelta(days=2)
    # If Saturday (5) or Sunday (6), roll to Monday
    while edd_date.weekday() >= 5:
        edd_date += timedelta(days=1)
    return f"{edd_date.month}/{edd_date.day}/{edd_date.year}"

def build_allocation_output(
    df: pd.DataFrame,
    edd: str | None = None,
    buyer: str = "P2E",
    supplier: int = 81214,
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
    if edd is None or not str(edd).strip():
        edd_str = _compute_default_edd()
    else:
        # Use provided string as-is
        edd_str = str(edd).strip()

    out = df.copy()
    out["Supplier On Record"] = supplier
    out["Expected Delivery Date"] = edd_str
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