import re
import pandas as pd
from pathlib import Path
from datetime import date,datetime

def clean_phillips_df(df: pd.DataFrame, file_name: str) -> pd.DataFrame:
    """
    Transform df by:
      1) Using the first row as header, then removing that row
      2) Filter by 'dock' based on file_name:
           - contains '407'  -> keep dock == 407
           - contains '436'  -> keep dock == 436
           - otherwise -> raise ValueError
      3) Dropping the first and second column
      4) Keeping columns up to and including 'Distro Size' (dropping those to its right)
      5) Dropping any rows where 'Distro Size' == 0
    """
    df = df.copy()

    # 1) First row -> header
    df.columns = df.iloc[0]
    df = df.drop(df.index[0])
    df.columns = [str(c).strip() for c in df.columns]

    # 2) Filter by dock using file_name
    name_l = file_name.lower()
    if ("436" in name_l) and ("407" in name_l):
        raise ValueError("file_name contains both '436' and '407' — ambiguous which dock filter to apply.")
    if "436" in name_l:
        allowed_docks = {436}
    elif "407" in name_l:
        allowed_docks = {407}
    elif "189" in name_l:
        allowed_docks = {189}
    else:
        raise ValueError("file_name must contain either '436' or '407' or '189' to decide dock filtering.")

    if "dock" not in df.columns:
        raise ValueError("'dock' column not found in the dataframe headers.")

    # Robust dock filtering (handle numeric vs string)
    dock_series = pd.to_numeric(df["dock"], errors="coerce")
    df = df[dock_series.isin(allowed_docks)]

    # 3) Drop the first column
    df = df.iloc[:, 1:]

    # 4) Keep up to and including 'Distro Size'
    if "Distro Size" not in df.columns:
        raise ValueError("'Distro Size' column not found after column drops. "
                         f"Available columns: {list(df.columns)}")
    keep_upto = df.columns.get_loc("Distro Size")
    df = df.iloc[:, : keep_upto + 1]

    # 5) Drop rows where 'Distro Size' == 0
    ds_numeric = pd.to_numeric(df["Distro Size"], errors="coerce")
    df = df[ds_numeric.ne(0)]

    # 6) Rename column's name
    df = df.rename(columns={'dock': 'Warehouse'})

    # Clean index
    df = df.reset_index(drop=True)
    return df


def build_phillips_output(df: pd.DataFrame, edd: str, buyer: str = "P20", supplier: int = 53459) -> pd.DataFrame:
    """
    Append ACME output columns:
      - 'Supplier On Record' = supplier (all rows)
      - 'Expected Delivery Date' = edd (string like '9/15/2025', all rows)
      - 'WW Buyer' = buyer (all rows)
      - Keep the df's original 'Warehouse' column (do not overwrite it)
      - 'AdditionalXDCK','AmountCode','XDCK','POSTXDCK','FOB' = blank (all rows)
    Then:
      - For column 'Branch' (case-insensitive): if the cell is exactly two digits (e.g. '86'),
        change to three digits by prefixing '1' (-> '186').
    """
    if not isinstance(edd, str) or not edd.strip():
        raise ValueError("edd must be a non-empty string in date format like '9/15/2025'.")

    out = df.copy()

    # Add required columns
    out["Supplier On Record"] = supplier
    out["Expected Delivery Date"] = edd
    out["WW Buyer"] = buyer

    # Keep Warehouse from df (don't overwrite)
    if "Warehouse" not in out.columns:
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

    s = out[branch_col].astype(str).str.strip()
    mask_two_digits = s.str.fullmatch(r"\d{2}")
    s.loc[mask_two_digits] = "1" + s.loc[mask_two_digits]

    # Convert back to numeric if possible
    out[branch_col] = pd.to_numeric(s)

    return out

def build_phillips_output_path(file_name: str, folder: str = "output_folder") -> Path:
    path = Path(folder) / f" Mega Script {file_name}.xlsx"
    return path

CANONICAL_COLS = [
    'Branch','Item','Description','Distro Size','Supplier On Record','Expected Delivery Date',
    'WW Buyer','Warehouse','AdditionalXDCK','AmountCode','XDCK','POSTXDCK','FOB'
]

def write_phillips_output_excel(df: pd.DataFrame, path: str) -> None:
    df = df.copy().reindex(columns=CANONICAL_COLS)

    # numeric cols
    for c in ['Branch','Item','Distro Size', 'Warehouse']:
        df[c] = pd.to_numeric(df[c], errors='coerce').fillna(0).astype(int)
    for c in ['XDCK','FOB']:
        df[c] = pd.to_numeric(df[c], errors='coerce')

    # ensure real dates (accepts both 2- and 4-digit year strings)
    if 'Expected Delivery Date' in df.columns:
        df['Expected Delivery Date'] = pd.to_datetime(df['Expected Delivery Date'], errors='coerce').dt.date

    # text -> blanks
    for c in ['Supplier On Record','WW Buyer','AdditionalXDCK','AmountCode','POSTXDCK']:
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