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
    else:
        raise ValueError("file_name must contain either '436' or '407' to decide dock filtering.")

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

