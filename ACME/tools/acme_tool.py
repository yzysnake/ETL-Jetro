import re
import pandas as pd
from pathlib import Path
from datetime import date,datetime

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
        allowed_docks = {189, 436}
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


















