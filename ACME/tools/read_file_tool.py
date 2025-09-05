import re
from pathlib import Path
import pandas as pd


def _clean_stem(stem: str) -> str:
    # lowercase, trim, collapse spaces
    return re.sub(r"\s+", " ", str(stem)).strip()

def read_clean_file_name(folder: str = "put_your_excel_here"):
    """
    Read exactly one .xlsx from `folder` and return:
      - df: pandas DataFrame read with header=None
      - file_name: cleaned file name (no extension), lowercase, trimmed, spaces collapsed

    Raises:
      - FileNotFoundError if folder doesn't exist
      - ValueError if zero or more than one .xlsx found
    """
    folder_path = Path(folder)
    if not folder_path.exists():
        raise FileNotFoundError(f"Folder not found: {folder}")

    # collect .xlsx files (ignore Excel lock files '~$' and hidden files)
    xlsx_files = [
        p for p in folder_path.iterdir()
        if p.is_file()
        and p.suffix.lower() == ".xlsx"
        and not p.name.startswith("~$")
        and not p.name.startswith(".")
    ]

    if len(xlsx_files) == 0:
        raise ValueError(f"No .xlsx files found in: {folder}")
    if len(xlsx_files) > 1:
        names = ", ".join(sorted(f.name for f in xlsx_files))
        raise ValueError(f"Expected exactly one .xlsx file, found {len(xlsx_files)}: {names}")

    file_path = xlsx_files[0]
    # read with header=None exactly as requested
    df = pd.read_excel(file_path, header=None, engine="openpyxl")

    # cleaned file name (no extension)
    file_name = _clean_stem(file_path.stem)

    return df, file_name
