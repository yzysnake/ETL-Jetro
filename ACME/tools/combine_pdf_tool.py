import os
import glob
import re
from datetime import datetime
from PyPDF2 import PdfMerger

def combine_pdf(pdf_folder="output_folder/pdf_folder", output_destination="output_folder"):
    """
    Combine all PDFs under ./output_folder/<pdf_folder> into one PDF and
    save it in ./output_folder (or another relative output_destination).

    Only relative paths are allowed. Assumes ./output_folder exists.

    Args:
        pdf_folder (str): subdirectory inside ./output_folder to read PDFs from (default: 'pdf_folder')
        output_destination (str): destination directory (relative) to drop merged PDF (default: 'output_folder')
    """
    # Disallow absolute paths
    if os.path.isabs(pdf_folder) or os.path.isabs(output_destination):
        raise ValueError("Absolute paths are not allowed. Use relative paths only.")

    base_root = ""  # required to exist in current directory
    src_dir = os.path.join(base_root, pdf_folder)
    dest_dir = os.path.join(output_destination)

    # Ensure destination exists (create if missing). Do NOT touch other dirs.
    os.makedirs(dest_dir, exist_ok=True)

    # Collect PDFs (sorted for stable order)
    pattern = os.path.join(src_dir, "*.pdf")
    all_files = sorted(glob.glob(pattern))
    num_files = len(all_files)
    print(f"Merging {num_files} PDF(s) from: {src_dir}")

    if num_files == 0:
        raise FileNotFoundError(f"No PDFs found in: {src_dir}")

    # Print last two numeric groups from filename (e.g., '114-28937')
    last_two_ids = []
    for f in all_files:
        base = os.path.splitext(os.path.basename(f))[0]
        parts = [p for p in base.split("-") if p.isdigit()]
        if len(parts) >= 2:
            keep = "-".join(parts[-2:])
        else:
            m = re.search(r"(\d+)-(\d+)$", base)
            keep = "-".join(m.groups()) if m else base
        last_two_ids.append(keep)

    # Merge
    merger = PdfMerger()
    for pdf in all_files:
        merger.append(pdf)

    date_str = datetime.today().strftime("%m-%d-%y")  # e.g., 09-11-25
    out_file = os.path.join(dest_dir, f"{num_files} orders {date_str}.pdf")
    merger.write(out_file)
    print(out_file)

    merger.close()

    return out_file


