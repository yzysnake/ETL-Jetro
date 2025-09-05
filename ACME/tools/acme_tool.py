import pandas as pd
from pathlib import Path

def clean_acme_df(df: pd.DataFrame, file_name: str) -> pd.DataFrame:
    """
    Transform df by:
      1) Using the first row as header, then removing that row
      2) Filter by 'dock' based on file_name:
           - contains 'il'  -> keep dock == 189
           - contains 'fl'  -> keep dock in {407, 409}
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
    if ("il" in name_l) and ("fl" in name_l):
        raise ValueError("file_name contains both 'il' and 'fl' — ambiguous which dock filter to apply.")
    if "il" in name_l:
        allowed_docks = {189}
    elif "fl" in name_l:
        allowed_docks = {407, 499}
    else:
        raise ValueError("file_name must contain either 'il' or 'fl' to decide dock filtering.")

    if "dock" not in df.columns:
        raise ValueError("'dock' column not found in the dataframe headers.")

    # Robust dock filtering (handle numeric vs string)
    dock_series = pd.to_numeric(df["dock"], errors="coerce")
    df = df[dock_series.isin(allowed_docks)]

    # 3) Drop the first and second column
    df = df.iloc[:, 2:]

    # 4) Keep up to and including 'Distro Size'
    if "Distro Size" not in df.columns:
        raise ValueError("'Distro Size' column not found after column drops. "
                         f"Available columns: {list(df.columns)}")
    keep_upto = df.columns.get_loc("Distro Size")
    df = df.iloc[:, : keep_upto + 1]

    # 5) Drop rows where 'Distro Size' == 0
    ds_numeric = pd.to_numeric(df["Distro Size"], errors="coerce")
    df = df[ds_numeric.ne(0)]

    # Clean index
    df = df.reset_index(drop=True)
    return df


def build_acme_output(df: pd.DataFrame, edd: str, buyer: str = "P20", supplier: int = 44602) -> pd.DataFrame:
    """
    Append ACME output columns:
      - 'Supplier On Record' = supplier (all rows)
      - 'Expected Delivery Date' = edd (string like '9/15/2025', all rows)
      - 'WW Buyer' = buyer (all rows)
      - 'Warehouse','AdditionalXDCK','AmountCode','XDCK','POSTXDCK','FOB' = blank (all rows)
    """
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


def build_acme_output_path(file_name: str, folder: str = "output_folder") -> Path:
    path = Path(folder) / f" Mega Script {file_name}.xlsx"
    return path



CANONICAL_COLS = [
    'Branch','Item','Description','Distro Size','Supplier On Record','Expected Delivery Date',
    'WW Buyer','Warehouse','AdditionalXDCK','AmountCode','XDCK','POSTXDCK','FOB'
]

def write_acme_output_excel(df: pd.DataFrame, path: str) -> None:
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





















