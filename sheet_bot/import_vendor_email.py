# import_vendor_email.py
from __future__ import annotations

from pathlib import Path
from typing import Dict, List, Iterable, Union
import re
import pandas as pd
from utils import clean, strip_trailing_dot_zero

EMAIL_RE = re.compile(r"[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,}")

def _unique_preserve(seq: Iterable[str]) -> List[str]:
    seen = set()
    out: List[str] = []
    for x in seq:
        if x not in seen:
            seen.add(x)
            out.append(x)
    return out

def _parse_email_cell(val: Union[str, float, int]) -> List[str]:
    s = clean(val)
    if not s:
        return []
    # pick up emails even if separated by spaces/commas/semicolons/newlines
    return [m.group(0).lower() for m in EMAIL_RE.finditer(s)]

def _strip_df_strings(df: pd.DataFrame) -> pd.DataFrame:
    """
    Trim whitespace in all string cells without the applymap deprecation warning.
    Uses DataFrame.map if present (pandas ≥ 2.2), else falls back to applymap.
    """
    if hasattr(df, "map"):  # pandas ≥ 2.2
        return df.map(lambda x: x.strip() if isinstance(x, str) else x)  # type: ignore[attr-defined]
    return df.applymap(lambda x: x.strip() if isinstance(x, str) else x)

def load_recipients(
    xlsx_path: str | Path,
    sheet_name: str | int = 0,
) -> Dict[str, Dict[str, List[str]]]:
    """
    Build: { vendor_num: {"to": [emails...], "cc": [] } }

    - Column 0: Vendor # (normalized as string, trims trailing '.0')
    - Column 1: Vendor Name (not used here)
    - Columns 2+: one email per cell (header may be blank)
    """
    xlsx_path = Path(xlsx_path)
    if not xlsx_path.exists():
        raise FileNotFoundError(f"Email workbook not found: {xlsx_path}")

    # Read everything as str so we can clean uniformly
    df = pd.read_excel(xlsx_path, sheet_name=sheet_name, dtype=str, header=0)
    df = df.fillna("")
    df = _strip_df_strings(df)

    if df.shape[1] < 2:
        raise ValueError("Expected at least two columns: Vendor # and Vendor Name")

    recipients: Dict[str, Dict[str, List[str]]] = {}
    email_cols = list(range(2, df.shape[1]))

    for _, row in df.iterrows():
        raw_vendor = row.iloc[0]
        vendor_num = strip_trailing_dot_zero(clean(raw_vendor))
        if not vendor_num:
            continue  # skip rows without a vendor number



        to_emails: List[str] = []
        for c in email_cols:
            to_emails.extend(_parse_email_cell(row.iloc[c]))

        to_emails = _unique_preserve([e for e in to_emails if e])

        # NO CCs here — let the mailer add defaults.
        recipients[vendor_num] = {"to": to_emails, "cc": []}

    return recipients
