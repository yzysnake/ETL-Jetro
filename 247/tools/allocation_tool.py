import re
import pandas as pd

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