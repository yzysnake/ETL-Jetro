from __future__ import annotations

from typing import Any, Iterable, Optional
from pathlib import Path
from datetime import datetime
import pandas as pd
import os
import re
import glob
import shutil
import time
from PyPDF2 import PdfMerger

# ===================== General config =====================

TZ_NAME = "America/Chicago"

def now_chicago() -> datetime:
    try:
        from zoneinfo import ZoneInfo
        return datetime.now(ZoneInfo(TZ_NAME))
    except Exception:
        return datetime.now()

def weekday_key(dt: datetime | None = None) -> str:
    dt = dt or now_chicago()
    return dt.strftime("%a")[:3]  # 'Mon','Tue','Wed','Thu','Fri'

DAY_PREFIXES = {
    "Mon": ["mon"],
    "Tue": ["tues", "tue"],
    "Wed": ["wed"],
    "Thu": ["thurs", "thu"],
    "Fri": ["fri"],
}

def clean(x: Any) -> str:
    return str(x).strip() if x is not None else ""

def eqci(a: Any, b: Any) -> bool:
    return clean(a).lower() == clean(b).lower()

def is_int_str(s: Any) -> bool:
    s = clean(s)
    return s.isdigit() if s else False

def strip_trailing_dot_zero(s: str) -> str:
    """
    Convert '14.0' -> '14'; keep other decimals like '14.50' intact.
    """
    s = clean(s)
    return s[:-2] if s.endswith(".0") else s

def a1(row_idx_1based: int, col_idx_1based: int) -> str:
    """
    Convert 1-based (row, col) to A1 notation (e.g., 3,28 -> AB3).
    """
    if row_idx_1based < 1 or col_idx_1based < 1:
        raise ValueError("A1 coordinates must be 1-based integers.")
    col = ""
    n = col_idx_1based
    while n:
        n, r = divmod(n - 1, 26)
        col = chr(65 + r) + col
    return f"{col}{row_idx_1based}"

# ===================== Status constants =====================

# Use exactly the strings in Sheet dropdown
STATUS_READY = "Ready"
STATUS_SENDING = "SENDING"  # (no ellipsis)
STATUS_SENT = "Sent"
STATUS_ERROR = "ERROR"

# ===================== PDF watcher =====================

def retrieve_pdf(
    po_df: pd.DataFrame,
    output_folder: str | Path,
    *watch_folders: str | Path,
    pdf_folder: Optional[str | Path] = "pdf_folder",   # can be simple name OR full path
    po_col_candidates: Iterable[str] = ("PO #", "PO#", "PO_Number", "PO"),
    poll_interval: float = 5.0,
    settle_time: float = 3.0,
    open_retry: int = 5,
    open_retry_sleep: float = 1.0,
    max_wait_seconds: Optional[int] = None,
    case_insensitive: bool = True,
    verbose: bool = True,
) -> pd.DataFrame:
    """
    Monitor watch_folders for PDFs whose names end with `-<PO>.pdf` and move them to the destination dir.

    Destination resolution:
      - If `pdf_folder` is None: destination = <output_folder>
      - If `pdf_folder` is a simple name (no path separators, not absolute):
          destination = <output_folder>/<pdf_folder>
      - If `pdf_folder` is a full/absolute path OR contains path separators:
          destination = <pdf_folder>  (used as-is; NOT joined with output_folder)

    The destination directory is created if it does not already exist.

    Returns a status DataFrame with columns:
      ['PO', 'found_path', 'moved_to', 'status', 'finished_at']
    """
    # ---- 1) Pick PO column
    po_col = next((c for c in po_col_candidates if c in po_df.columns), None)
    if po_col is None:
        raise ValueError(f"po_df must contain one of columns: {po_col_candidates}")

    # ---- 2) Normalize POs
    raw_pos = (
        po_df[po_col]
        .astype(str)
        .str.strip()
        .replace({"": None})
        .dropna()
        .tolist()
    )
    target_pos = list(dict.fromkeys(raw_pos))  # keep order, unique
    if not target_pos:
        raise ValueError("No valid PO values found in po_df.")

    # ---- 3) Resolve destination directory (NO extra nesting)
    output_folder = Path(output_folder)

    def is_simple_name(p: str | Path) -> bool:
        p = str(p)
        return (os.sep not in p) and (os.altsep not in p if os.altsep else True) and (not os.path.isabs(p))

    if pdf_folder is None:
        dest_dir = output_folder
    else:
        pdf_folder = Path(pdf_folder)
        if pdf_folder.is_absolute() or not is_simple_name(pdf_folder):
            dest_dir = Path(pdf_folder)           # use as-is
        else:
            dest_dir = output_folder / pdf_folder # join to output_folder

    dest_dir.mkdir(parents=True, exist_ok=True)

    # ---- 4) Watch dirs
    watch_dirs = [Path(p) for p in watch_folders]
    if not watch_dirs:
        raise ValueError("Please provide at least one watch folder.")
    for d in watch_dirs:
        if d.exists() and not d.is_dir():
            raise ValueError(f"Not a directory: {d}")

    # ---- 5) Compile patterns: '-<PO>.pdf' at filename end
    def compile_pattern(po: str):
        esc = re.escape(po)
        flags = re.IGNORECASE if case_insensitive else 0
        return re.compile(rf"-{esc}\.pdf$", flags)

    patterns = {po: compile_pattern(po) for po in target_pos}

    # ---- 6) Status DF
    status = pd.DataFrame(
        {
            "PO": target_pos,
            "found_path": [None] * len(target_pos),
            "moved_to": [None] * len(target_pos),
            "status": ["waiting"] * len(target_pos),
            "finished_at": [None] * len(target_pos),
        }
    )

    def idx_of(po: str) -> int:
        m = status.index[status["PO"] == po]
        if len(m) == 0:
            raise KeyError(f"PO not tracked: {po}")
        return int(m[0])

    # ---- 7) Stable-file check
    def is_file_stable(p: Path, window: float) -> bool:
        try:
            size1 = p.stat().st_size
        except FileNotFoundError:
            return False
        t0 = time.time()
        while time.time() - t0 < window:
            time.sleep(0.5)
            try:
                size2 = p.stat().st_size
            except FileNotFoundError:
                return False
            if size2 != size1:
                size1 = size2
                t0 = time.time()
        return True

    # ---- 8) PRE-CHECK: mark POs already present in destination as done
    try:
        for existing in dest_dir.iterdir():
            if not existing.is_file() or not existing.name.lower().endswith(".pdf"):
                continue
            for po in target_pos:
                # If already marked done, skip
                if (status["PO"] == po).any():
                    i = idx_of(po)
                    if status.at[i, "status"] == "done":
                        continue
                # Match pattern
                if patterns[po].search(existing.name):
                    i = idx_of(po)
                    status.at[i, "found_path"] = str(existing)
                    status.at[i, "moved_to"] = str(existing)
                    status.at[i, "status"] = "done"
                    status.at[i, "finished_at"] = datetime.now().isoformat(timespec="seconds")
                    break
    except Exception as e:
        if verbose:
            print(f"[WARN] Destination pre-check failed: {e}")

    remaining = set(status.loc[status["status"] != "done", "PO"].tolist())

    # Helper for logging
    def print_progress():
        if not verbose:
            return
        waiting = sorted(remaining)
        done = status[status["status"] == "done"]["PO"].tolist()
        print(
            f"[{datetime.now().strftime('%H:%M:%S')}] "
            f"waiting: {len(waiting)} -> {waiting[:10]}{'...' if len(waiting) > 10 else ''} | "
            f"done: {len(done)}"
        )

    print_progress()

    # ---- 9) INITIAL SWEEP: move any already-existing matches in watch folders
    def sweep_once():
        nonlocal remaining
        any_found = False
        for wd in watch_dirs:
            if not wd.exists() or not wd.is_dir():
                continue
            try:
                for entry in wd.iterdir():
                    if not entry.is_file() or not entry.name.lower().endswith(".pdf"):
                        continue
                    # If this file is already in dest_dir (same path), skip
                    if entry.parent.resolve() == dest_dir.resolve():
                        continue

                    for po in list(remaining):
                        if patterns[po].search(entry.name):
                            any_found = True
                            i = idx_of(po)
                            status.at[i, "found_path"] = str(entry)

                            if verbose:
                                print(f"[HIT] (initial sweep) PO={po} at {entry}")
                                print(f"      Waiting for stability: {settle_time}s")
                            if not is_file_stable(entry, settle_time):
                                if verbose:
                                    print(f"[WARN] Unstable or vanished: {entry}")
                                continue

                            dest = dest_dir / entry.name
                            if dest.exists():
                                status.at[i, "moved_to"] = str(dest)
                                status.at[i, "status"] = "done"
                                status.at[i, "finished_at"] = datetime.now().isoformat(timespec="seconds")
                                remaining.discard(po)
                                if verbose:
                                    print(f"[SKIP MOVE] Already in destination: {dest} — marked done")
                                break

                            ok = False
                            last_err = None
                            for _ in range(open_retry):
                                try:
                                    shutil.move(str(entry), str(dest))
                                    ok = True
                                    break
                                except Exception as e:
                                    last_err = e
                                    time.sleep(open_retry_sleep)

                            if not ok:
                                if verbose:
                                    print(f"[ERROR] Move failed: {entry} -> {dest} | {last_err}")
                                continue

                            status.at[i, "moved_to"] = str(dest)
                            status.at[i, "status"] = "done"
                            status.at[i, "finished_at"] = datetime.now().isoformat(timespec="seconds")
                            remaining.discard(po)

                            if verbose:
                                print(f"[OK] Moved (initial sweep): {entry.name} -> {dest}")
                            break
            except PermissionError:
                if verbose:
                    print(f"[WARN] Permission denied (temporary): {wd}")
            except FileNotFoundError:
                if verbose:
                    print(f"[WARN] Directory unavailable (temporary): {wd}")
            except Exception as e:
                if verbose:
                    print(f"[WARN] Initial sweep error: {wd} | {e}")
        return any_found

    initial_found = sweep_once()
    if initial_found and verbose:
        print_progress()

    # If everything is already satisfied, return early
    if not remaining:
        if verbose:
            print(f"[DONE] All POs satisfied from destination and initial sweep. Output dir: {dest_dir}")
        return status

    # ---- 10) POLLING LOOP
    start = time.time()
    while remaining:
        if max_wait_seconds is not None and (time.time() - start) > max_wait_seconds:
            if verbose:
                print("[INFO] Reached max_wait_seconds. Returning current status.")
            break

        any_found = False

        for wd in watch_dirs:
            if not wd.exists() or not wd.is_dir():
                continue

            try:
                for entry in wd.iterdir():
                    if not entry.is_file() or not entry.name.lower().endswith(".pdf"):
                        continue
                    if entry.parent.resolve() == dest_dir.resolve():
                        continue

                    for po in list(remaining):
                        if patterns[po].search(entry.name):
                            any_found = True
                            i = idx_of(po)
                            status.at[i, "found_path"] = str(entry)

                            if verbose:
                                print(f"[HIT] PO={po} at {entry}")
                                print(f"      Waiting for stability: {settle_time}s")
                            if not is_file_stable(entry, settle_time):
                                if verbose:
                                    print(f"[WARN] Unstable or vanished: {entry}")
                                continue

                            dest = dest_dir / entry.name
                            if dest.exists():
                                status.at[i, "moved_to"] = str(dest)
                                status.at[i, "status"] = "done"
                                status.at[i, "finished_at"] = datetime.now().isoformat(timespec="seconds")
                                remaining.discard(po)
                                if verbose:
                                    print(f"[SKIP MOVE] Already in destination: {dest} — marked done")
                                break

                            ok = False
                            last_err = None
                            for _ in range(open_retry):
                                try:
                                    shutil.move(str(entry), str(dest))
                                    ok = True
                                    break
                                except Exception as e:
                                    last_err = e
                                    time.sleep(open_retry_sleep)

                            if not ok:
                                if verbose:
                                    print(f"[ERROR] Move failed: {entry} -> {dest} | {last_err}")
                                continue

                            status.at[i, "moved_to"] = str(dest)
                            status.at[i, "status"] = "done"
                            status.at[i, "finished_at"] = datetime.now().isoformat(timespec="seconds")
                            remaining.discard(po)

                            if verbose:
                                print(f"[OK] Moved: {entry.name} -> {dest}")
                            break

            except PermissionError:
                if verbose:
                    print(f"[WARN] Permission denied (temporary): {wd}")
            except FileNotFoundError:
                if verbose:
                    print(f"[WARN] Directory unavailable (temporary): {wd}")
            except Exception as e:
                if verbose:
                    print(f"[WARN] Scan error: {wd} | {e}")

        if remaining:
            if any_found:
                print_progress()
            time.sleep(poll_interval)

    if verbose:
        done = status[status["status"] == "done"].shape[0]
        total = status.shape[0]
        print(f"[DONE] Completed {done}/{total}. Output dir: {dest_dir}")

    return status

# ===================== PDF combiner =====================

def combine_pdf(pdf_folder: str = "output_folder/pdf_folder", output_destination: str = "output_folder") -> str:
    """
    Combine all PDFs under ./output_folder/<pdf_folder> into one PDF and
    save it in ./output_folder (or another relative output_destination).
    Only **relative** paths are allowed.

    Returns: relative path string of the merged PDF.
    """
    # Disallow absolute paths
    if os.path.isabs(pdf_folder) or os.path.isabs(output_destination):
        raise ValueError("Absolute paths are not allowed. Use relative paths only.")

    base_root = ""  # cwd
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

    # Merge
    merger = PdfMerger()
    for pdf in all_files:
        merger.append(pdf)

    date_str = datetime.today().strftime("%m-%d-%y")  # e.g., 09-18-25
    out_file = os.path.join(dest_dir, f"{num_files} orders {date_str}.pdf")
    merger.write(out_file)
    print(out_file)
    merger.close()

    return out_file
