import re
import pandas as pd

def clean_allocation_df(allocation_df: pd.DataFrame) -> pd.DataFrame:
    """
    Steps:
      1) Drop the first row.
      2) Make the (new) first row the header (this is the original second row), then remove that header row from the body.
      3) Drop the 'Total' column and everything to its right (i.e., keep only columns strictly to the left of 'Total').
      4) Clean header names by removing a trailing '.0' (e.g., '142.0' -> '142') and stripping whitespace.
      5) Drop the last row.
    """
    if allocation_df.shape[0] < 2:
        raise ValueError("Expected at least 2 rows to promote the second row to header.")

    # 1) Drop the first row
    df = allocation_df.drop(allocation_df.index[0]).reset_index(drop=True)

    # 2) Promote the new first row to header, then drop that row from the body
    new_cols = df.iloc[0].astype(str).str.strip().tolist()
    df = df.drop(df.index[0]).reset_index(drop=True)
    df.columns = new_cols

    # 3) Drop 'Total' and everything to its right
    #    (case-insensitive, ignores surrounding spaces)
    norm_cols = [str(c).strip().casefold() for c in df.columns]
    try:
        total_idx = norm_cols.index("total")  # position of 'Total'
        # Keep only columns strictly to the left of 'Total'
        df = df.iloc[:, :total_idx]
    except ValueError:
        # 'Total' not found -> do nothing per your steps
        pass

    # 4) Clean header names: remove trailing '.0' and strip whitespace
    df.columns = [re.sub(r"\.0$", "", str(c)).strip() for c in df.columns]

    # 5) Drop the last row (only if there is at least one row)
    if df.shape[0] > 0:
        df = df.iloc[:-1, :].reset_index(drop=True)

    return df
