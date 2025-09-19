# monitor.py
from __future__ import annotations
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Dict, List, Tuple
import time
import traceback
import re
import os
import shutil
from utils import now_chicago
from import_vendor_email import load_recipients




import sheets
from utils import (
    STATUS_READY,
    STATUS_SENDING,
    STATUS_SENT,
    STATUS_ERROR,
    clean,
    retrieve_pdf,
    combine_pdf,
)

from email_generator import generate_body, combine_body_signature
from email_sender import send_email_with_graph

# =================== CONFIG ===================
POLL_SECONDS = 60
MAX_WORKERS = 4                       # how many vendors to run in parallel
MAX_WAIT_SECONDS_PER_VENDOR = 300     # 5 minutes

OUTPUT_BASE = Path("output_folder")        # relative; will be created if missing
VENDOR_SUBDIR_PREFIX = "pdf_vendor_"       # per-vendor folder under OUTPUT_BASE
EMAIL_HTML_DIR = Path("email_html")        # where we put body/signature/main html (relative)

# Your signature file (put your HTML signature there)
SIGNATURE_PATH = EMAIL_HTML_DIR / "signature.html"

RECIPIENTS = load_recipients(r"vendor_email_sheet.xlsx")

# Watch folders (shared/network/local). Use absolute Windows/UNC paths here.
WATCH_FOLDERS = [
    Path(r"\\10.1.12.17\PO_Share\P2E"),
    Path(r"\\10.1.12.17\PO_Share\P2M"),
    Path(r"\\10.1.12.17\PO_Share\P20"),
    Path(r"\\10.1.12.17\PO_Share\P2Y"),
]
# ==============================================

# delete outputs after successful email
CLEANUP_AFTER_SEND = True

import re  # ensure this import exists

def _store_po_items_from_moves(moved_status_df) -> list[str]:
    """
    Build items like '142-57466' from rows where status == 'done'.
    Parses the store code from the moved filename: ...-<store>-<PO>.pdf
    Falls back to the PO alone if parsing fails.
    """
    items: list[str] = []
    if moved_status_df is None or moved_status_df.empty:
        return items

    done = moved_status_df[moved_status_df["status"] == "done"]
    for _, row in done.iterrows():
        po = str(row.get("PO", "")).strip()
        path = str(row.get("moved_to") or row.get("found_path") or "").strip()
        if po and path:
            m = re.search(rf"-([0-9]+)-{re.escape(po)}\.pdf$", path, flags=re.IGNORECASE)
            if m:
                store = m.group(1)
                items.append(f"{store}-{po}")
            else:
                items.append(po)
    return items


def _store_codes_from_header(header: str) -> list[str]:
    """
    Return ALL numeric store codes from a header.
    Examples:
      '452'        -> ['452']
      '452/490'    -> ['452', '490']
      'Store 142 ' -> ['142']
    """
    return re.findall(r"\d+", str(header))

def _within(base: Path, target: Path) -> bool:
    """
    True if target is inside base (after resolving). Safety check before deleting.
    """
    try:
        base_abs = base.resolve()
        target_abs = target.resolve()
        return os.path.commonpath([str(base_abs), str(target_abs)]) == str(base_abs)
    except Exception:
        return False

def _cleanup_vendor_output(vendor_dir_rel: Path, merged_pdf: Path):
    """
    Remove the vendor subfolder (e.g., output_folder/pdf_vendor_<vendor>).
    If the merged PDF lives inside that folder (now it does), rmtree will remove it.
    """
    # If merged PDF is outside the vendor folder but still under OUTPUT_BASE, remove it.
    try:
        if merged_pdf and merged_pdf.exists():
            if _within(vendor_dir_rel, merged_pdf):
                # merged lives inside vendor_dir_rel -> will be removed by rmtree below
                pass
            elif _within(OUTPUT_BASE, merged_pdf):
                merged_pdf.unlink()
                print(f"[cleanup] Removed merged PDF: {merged_pdf}")
            else:
                print(f"[cleanup] Skip (outside OUTPUT_BASE): {merged_pdf}")
    except Exception as e:
        print(f"[cleanup] Could not remove merged PDF {merged_pdf}: {e}")

    # delete the vendor subfolder tree
    try:
        if vendor_dir_rel and vendor_dir_rel.exists():
            if _within(OUTPUT_BASE, vendor_dir_rel):
                shutil.rmtree(vendor_dir_rel)
                print(f"[cleanup] Removed vendor folder: {vendor_dir_rel}")
            else:
                print(f"[cleanup] Skip (outside OUTPUT_BASE): {vendor_dir_rel}")
    except Exception as e:
        print(f"[cleanup] Could not remove vendor folder {vendor_dir_rel}: {e}")


    # delete the vendor subfolder tree
    try:
        if vendor_dir_rel and vendor_dir_rel.exists():
            if _within(OUTPUT_BASE, vendor_dir_rel):
                shutil.rmtree(vendor_dir_rel)
                print(f"[cleanup] Removed vendor folder: {vendor_dir_rel}")
            else:
                print(f"[cleanup] Skip (outside OUTPUT_BASE): {vendor_dir_rel}")
    except Exception as e:
        print(f"[cleanup] Could not remove vendor folder {vendor_dir_rel}: {e}")

def _tokens_from_row(row: dict) -> List[str]:
    """For logging: {vendor-store-po} tokens, skipping blank/x and trimming '.0'."""
    tokens = []
    vnum = row.get("vendor_num", "")
    stores = row.get("stores", {}) or {}
    for store_header, val in stores.items():
        sval = clean(val)
        if not sval or sval.lower() == "x":
            continue
        if sval.endswith(".0"):
            sval = sval[:-2]
        for code in _store_codes_from_header(store_header):
            tokens.append(f"{vnum}-{code}-{sval}")
    return tokens



def _po_df_from_row(row: dict):
    """Use the helper we added to sheets.py earlier."""
    return sheets.po_df_from_row(row, po_col_name="PO #")


def _combine_vendor_pdfs(vendor_num: str, vendor_dir_rel: Path) -> Path:
    """
    Merge PDFs and write the merged file INSIDE the vendor folder
    (e.g., output_folder/pdf_vendor_<vendor>/...pdf).
    """
    merged_path_str = combine_pdf(
        pdf_folder=str(vendor_dir_rel),        # e.g., 'output_folder/pdf_vendor_10001'
        output_destination=str(vendor_dir_rel) # <— was OUTPUT_BASE
    )
    return Path(merged_path_str)

def _normalize_po(s: str) -> str:
    s = clean(s)
    return s[:-2] if s.endswith(".0") else s

def _normalize_store_code(s: str) -> str:
    """
    Extract the first number sequence from the store header.
    Works for '142', '142 ', 'Store 142', etc.
    """
    s = clean(s)
    m = re.search(r"\d+", s)
    return m.group(0) if m else s

def _store_po_items(row: dict) -> List[str]:
    """
    Build items like '142-57466' (no spaces). Skips blanks and 'x'.
    Expands headers like '452/490' -> '452-PO' and '490-PO'.
    """
    items: List[str] = []
    stores = row.get("stores", {}) or {}
    for store_header, val in stores.items():
        sval = clean(val)
        if not sval or sval.lower() == "x":
            continue
        if sval.endswith(".0"):
            sval = sval[:-2]
        for code in _store_codes_from_header(store_header):
            items.append(f"{code}-{sval}")
    return items



def _send_vendor_email(
    row: dict,
    merged_pdf: Path,
    recipients: Dict[str, List[str]],
    store_po_items: list[str] | None = None,   # <-- NEW param
) -> None:
    """
    Use email sender; attach the merged PDF.
    send_email_with_graph needs an HTML file path and an attachments_dir + filenames.
    """
    # Prefer items based on what actually arrived; else derive from the row
    if store_po_items is None:
        store_po_items = _store_po_items(row)
    if not store_po_items:
        raise RuntimeError("No Store–PO items to include in the email body.")

    # Subject: "{N} Order(s) - MM/DD/YY"
    n = len(store_po_items)
    date_str = now_chicago().strftime("%m/%d/%y")
    subject = f"{n} {'Order' if n == 1 else 'Orders'} - {date_str}"

    # Write body.html and combine with signature -> main.html
    EMAIL_HTML_DIR.mkdir(parents=True, exist_ok=True)
    body_path = generate_body(
        items=store_po_items,
        out_dir=str(EMAIL_HTML_DIR),
        filename=f"body_{row['vendor_num']}.html",
    )
    main_html = combine_body_signature(
        body_path=str(body_path),
        signature_path=str(SIGNATURE_PATH),
        out_path=str(EMAIL_HTML_DIR / f"main_{row['vendor_num']}.html"),
        padding_px=24,
    )

    # Attach merged PDF (we now place it inside vendor_dir_rel)
    attachments_dir = str(merged_pdf.parent)
    attachment_names = [merged_pdf.name]

    to = recipients.get("to", [])
    cc = recipients.get("cc", [])
    if not to:
        raise RuntimeError(f"No recipients configured for vendor {row.get('vendor_num')}")

    send_email_with_graph(
        subject=subject,
        html_path=str(main_html),
        to_list=to,
        cc_list=cc,
        attachments_dir=attachments_dir,
        attachment_names=attachment_names,
        save_to_sent=True,
    )

def _clean_addresses(lst: List[str]) -> List[str]:
    """Trim, drop empties/None."""
    return [a.strip() for a in (lst or []) if isinstance(a, str) and a.strip()]

def _get_vendor_recipients(vendor_num: str) -> Dict[str, List[str]]:
    """Return cleaned recipients dict; empty lists if vendor not configured."""
    rec = RECIPIENTS.get(str(vendor_num), {}) or {}
    return {
        "to": _clean_addresses(rec.get("to", [])),
        "cc": _clean_addresses(rec.get("cc", [])),
    }

def process_vendor_row(row: dict) -> Tuple[str, str, str]:
    """
    Worker thread for a single vendor row.
    Returns (status_a1, final_status, message).
    """
    status_a1 = row.get("status_a1", "")
    vendor = str(row.get("vendor_num", "")).strip() or "UNKNOWN"
    try:
        print(f"[worker] Start vendor {vendor} at {status_a1}")
        print(f"[worker] Tokens: {_tokens_from_row(row)}")

        # 0) Fail fast if no recipients configured (BEFORE touching any folders)
        recipients = _get_vendor_recipients(vendor)
        if not recipients["to"]:
            return status_a1, STATUS_ERROR, f"No recipients configured for vendor {vendor}"

        # Prepare vendor-specific RELATIVE folder under OUTPUT_BASE
        OUTPUT_BASE.mkdir(parents=True, exist_ok=True)
        vendor_dir_rel = OUTPUT_BASE / f"{VENDOR_SUBDIR_PREFIX}{vendor}"
        vendor_dir_rel.mkdir(parents=True, exist_ok=True)

        # Build PO DataFrame
        po_df = _po_df_from_row(row)
        pos = po_df["PO #"].tolist()
        if not pos:
            return status_a1, STATUS_ERROR, "No PO numbers extracted."

        # Watch/move PDFs for this vendor into vendor_dir_rel
        moved_status = retrieve_pdf(
            po_df,                  # positional #1
            OUTPUT_BASE,            # positional #2
            *WATCH_FOLDERS,         # varargs
            pdf_folder=vendor_dir_rel.name,
            poll_interval=5.0,
            settle_time=3.0,
            max_wait_seconds=MAX_WAIT_SECONDS_PER_VENDOR,
            verbose=True,
        )

        # Check completeness
        needed = set(pos)
        got = set(moved_status.loc[moved_status["status"] == "done", "PO"].tolist())
        missing = needed - got
        if missing:
            return status_a1, STATUS_ERROR, f"Missing PDFs for POs: {sorted(missing)}"

        # Combine vendor PDFs into one (in vendor folder)
        merged_pdf = _combine_vendor_pdfs(vendor, vendor_dir_rel)

        # NEW: build email lines from files that actually arrived
        arrived_items = _store_po_items_from_moves(moved_status)

        # Send email (HTML + attachment) using arrived_items
        _send_vendor_email(row, merged_pdf, recipients, store_po_items=arrived_items)

        # cleanup
        if 'CLEANUP_AFTER_SEND' in globals() and CLEANUP_AFTER_SEND:
            _cleanup_vendor_output(vendor_dir_rel, merged_pdf)

        return status_a1, STATUS_SENT, f"OK merged={merged_pdf.name}"

    except Exception as e:
        tb = traceback.format_exc(limit=3)
        return status_a1, STATUS_ERROR, f"{e}\n{tb}"


def main():
    sh = sheets.open_spreadsheet()
    print("Opened spreadsheet (concurrent monitor).")

    active: Dict[str, object] = {}  # status_a1 -> Future

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
        while True:
            try:
                ws = sheets.pick_today_worksheet(sh)
                print(f"[loop] Tab: {ws.title}")

                df = sheets.parse_sections(ws)
                if not df.empty:
                    # Find new Ready rows not already active
                    ready_df = df[df["status"] == STATUS_READY]
                    new_rows = [row for _, row in ready_df.iterrows() if row["status_a1"] not in active]

                    if new_rows:
                        # Lock them: Ready -> SENDING (batch)
                        locks = [(row["status_a1"], STATUS_SENDING) for row in new_rows]
                        sheets.batch_update_status(ws, locks)
                        print(f"[lock] Marked {len(locks)} row(s) as {STATUS_SENDING}")

                        # Submit workers
                        for row in new_rows:
                            d = row.to_dict()
                            fut = pool.submit(process_vendor_row, d)
                            active[d["status_a1"]] = fut
                            print(f"[queue] Vendor {d['vendor_num']} at {d['status_a1']} queued")

                    # Collect finished
                    finished, updates = [], []
                    for a1, fut in list(active.items()):
                        if fut.done():
                            finished.append(a1)
                            try:
                                a1addr, final_status, msg = fut.result()
                                print(f"[done] {a1addr} -> {final_status} | {msg}")
                                updates.append((a1addr, final_status))
                            except Exception as e:
                                print(f"[done] {a1} raised: {e}")
                                updates.append((a1, STATUS_ERROR))

                    # Write back final statuses
                    if updates:
                        sheets.batch_update_status(ws, updates)
                        print(f"[write] Updated {len(updates)} row(s).")

                    # Remove finished from active map
                    for a1 in finished:
                        active.pop(a1, None)

                if active:
                    print(f"[loop] Active jobs: {len(active)}")

            except Exception as e:
                print(f"[loop-error] {e}")

            time.sleep(POLL_SECONDS)


if __name__ == "__main__":
    main()
