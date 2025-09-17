from __future__ import annotations
from pathlib import Path
from datetime import datetime
import pandas as pd
from typing import List

def write_ADPO_I_file(
    big_flip_df_output: pd.DataFrame,
    out_dir: str | Path = "output_folder",
    XDCK_letter: str = "M",
    XDCK_warehouse_number: str = "498",
    freight_type: str = "W",
    buyer_code: str = "P20",
    filename: str = "output"
) -> Path:
    """
    Build an ADPO_I macro script from `big_flip_df_output` and write a .txt file.
    """

    # ---- helpers -------------------------------------------------------------

    def _today_iso() -> str:
        return datetime.now().strftime("%Y-%m-%d")

    def _fmt_date_mdy2(x) -> str:
        if pd.isna(x):
            return ""
        if isinstance(x, (pd.Timestamp, datetime)):
            dt = x
        else:
            try:
                dt = pd.to_datetime(x)
            except Exception:
                return str(x).strip()
        return dt.strftime("%m/%d/%y")

    def _fmt_item_code(x) -> str:
        if pd.isna(x):
            return ""
        s = str(x).strip()
        if s.endswith(".0"):
            s = s[:-2]
        digits = "".join(ch for ch in s if ch.isdigit())
        return digits.zfill(7) if digits else s

    def _num_like_to_clean_str(x) -> str:
        """
        Normalize values like '39.0', '039.00', '1,234.0' -> '39', '39', '1234'
        If truly non-numeric, returns the stripped string with trailing .0/.00 removed.
        """
        if pd.isna(x):
            return ""
        s = str(x).strip().replace(",", "")
        try:
            f = float(s)
            # keep as int string if it's an integer (e.g., 39.0 -> '39')
            if f.is_integer():
                return str(int(f))
            # if not integer, trim trailing zeros (e.g., 39.50 -> '39.5')
            ss = s
            if "." in ss:
                ss = ss.rstrip("0").rstrip(".")
            return ss
        except Exception:
            # fallback: just trim a single '.0'/'..00' tail if present
            if s.endswith(".0") or s.endswith(".00"):
                while s.endswith("0"):
                    s = s[:-1]
                if s.endswith("."):
                    s = s[:-1]
            return s

    lines: List[str] = []

    def add(line: str) -> None:
        lines.append(line.rstrip())

    # ---- building blocks -----------------------------------------------------

    def outer_cycle_start(branch_val: str) -> None:
        add("")
        add("Key tab")
        add(f"Type {buyer_code}")
        add(f"Type {branch_val}")
        add("Type 20000")
        add("Key Enter")

    def outer_cycle_end_without_FOB(expected_date: str, xdck: str) -> None:
        xdck_clean = _num_like_to_clean_str(xdck)
        add("")
        add(f"Type {XDCK_warehouse_number}-0990033")
        add("Key enter")
        add("Key tab")
        add("Key delete")
        add("Key delete")
        add("Key delete")
        add("Key delete")
        add("Type 0")
        add("Key Enter")
        add("Key PF13")
        add("Key Enter")
        add("wait 500")
        add("wait 500")
        add(f"Type {expected_date}")
        add("Key PF2")
        add("wait 500")
        add(f"Type {XDCK_letter}")
        add("key pf2")
        add("wait 1500")
        add("key cursorup")
        add("key cursorup")
        add("wait 500")
        add("key cursorup")
        add("key cursorup")
        add("key tab")
        add("wait 500")
        add("key cursordown")
        add(f"Type {expected_date}")
        add("Key Tab")
        add("key tab")
        add("key tab")
        add("wait 500")
        add("key tab")
        add("Key cursordown")
        add("Key tab")
        add("")
        add("key delete")
        add("wait 500")
        add("key delete")
        add("key delete")
        add("key delete")
        add(f"Type {xdck_clean}")
        add("wait 500")
        add("key tab")
        add(f"type {freight_type}")
        add("Key tab")
        add("key tab")
        add("wait 500")
        add("key tab")
        add("wait 500")
        add("Key cursordown")
        add("wait 500")
        add("Key cursordown")
        add("key tab")
        add("")
        add("key Enter")
        add("wait 500")
        add("key Enter")
        add("wait 3000")
        add("EditSelect 13,39,13,47")
        add("key EditCopy")
        add("wait 1000")
        today_iso = _today_iso()
        add(f"FileSpec clipboard,C:\\POs\\{today_iso}_114544_{buyer_code}.csv,append")
        add("key EditSaveClipboard")
        add("wait 1000")
        add(f"FileSpec clipboard,\\\\10.1.12.12\\faxshare\\DailyPOCount\\POs\\{today_iso}_{buyer_code}.csv,append")
        add("key EditSaveClipboard")

    def outer_cycle_end_with_FOB(expected_date: str, xdck: str, fob: str) -> None:
        xdck_clean = _num_like_to_clean_str(xdck)
        fob_clean = _num_like_to_clean_str(fob)
        add("")
        add(f"Type {XDCK_warehouse_number}-0990033")
        add("Key enter")
        add("Key tab")
        add("Key delete")
        add("Key delete")
        add("Key delete")
        add("Key delete")
        add("Type 0")
        add("Key Enter")
        add("Key PF13")
        add("Key Enter")
        add("wait 500")
        add("wait 500")
        add(f"Type {expected_date}")
        add("Key PF2")
        add("wait 500")
        add(f"Type {XDCK_letter}")
        add("key pf2")
        add("wait 1500")
        add("key cursorup")
        add("key cursorup")
        add("wait 500")
        add("key cursorup")
        add("key cursorup")
        add("key tab")
        add("wait 500")
        add("key cursordown")
        add(f"Type {expected_date}")
        add("Key Tab")
        add("key delete")
        add("key delete")
        add("key delete")
        add("key delete")
        add(f"type {fob_clean}")
        add("wait 500")
        add("key tab")
        add(f"type {freight_type}")
        add("Key cursordown")
        add("Key tab")
        add("key tab")
        add("")
        add("key delete")
        add("wait 500")
        add("key delete")
        add("key delete")
        add("key delete")
        add(f"Type {xdck_clean}")
        add("wait 500")
        add("key tab")
        add(f"type {freight_type}")
        add("Key tab")
        add("key tab")
        add("wait 500")
        add("key tab")
        add("wait 500")
        add("Key cursordown")
        add("wait 500")
        add("Key cursordown")
        add("key tab")
        add("")
        add("key Enter")
        add("wait 500")
        add("key Enter")
        add("wait 3000")
        add("EditSelect 13,39,13,47")
        add("key EditCopy")
        add("wait 1000")
        today_iso = _today_iso()
        add(f"FileSpec clipboard,C:\\POs\\{today_iso}_114544_{buyer_code}.csv,append")
        add("key EditSaveClipboard")
        add("wait 1000")
        add(f"FileSpec clipboard,\\\\10.1.12.12\\faxshare\\DailyPOCount\\POs\\{today_iso}_{buyer_code}.csv,append")
        add("key EditSaveClipboard")

    def inside_cycle(item_code: str, distro_size: str | int) -> None:
        add("")
        add(f"Type {XDCK_warehouse_number}-{item_code}")
        add("Key enter")
        add("Key tab")
        add("Key delete")
        add("Key delete")
        add("Key delete")
        add("Key delete")
        add(f"Type {distro_size}")
        add("Key Enter")
        add("Key PF24")

    # ---- prepare & iterate ---------------------------------------------------

    df = big_flip_df_output.copy()

    df["Branch"] = df["Branch"].astype(str).str.strip()
    df["XDCK"] = df["XDCK"].astype(str).str.strip()
    if "FOB" in df.columns:
        df["FOB"] = df["FOB"].astype(str).str.strip()
        df.loc[df["FOB"].isin(["", "nan", "NaN", "None"]), "FOB"] = ""

    for branch_val, g in df.groupby("Branch", sort=True):
        first = g.iloc[0]
        expected_date = _fmt_date_mdy2(first["Expected Delivery Date"])
        xdck_val = str(first["XDCK"]).strip()
        fob_val = str(first["FOB"]).strip() if "FOB" in g.columns else ""

        outer_cycle_start(branch_val)

        for _, row in g.iterrows():
            item7 = _fmt_item_code(row["Item"])
            distro = row["Distro Size"]
            inside_cycle(item7, distro)

        if fob_val:
            outer_cycle_end_with_FOB(expected_date, xdck_val, fob_val)
        else:
            outer_cycle_end_without_FOB(expected_date, xdck_val)

    # ---- write file ----------------------------------------------------------

    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    today_str = _today_iso()
    out_path = out_dir / f"{today_str}_ADPO_I_{filename}.txt"

    text = "\n".join(lines) + "\n"
    out_path.write_text(text, encoding="utf-8")
    return out_path
