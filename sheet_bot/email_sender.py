import os
import json
import base64
import mimetypes
from pathlib import Path
from typing import Iterable, List, Optional
import msal
import requests
import re

EMAIL_RE = re.compile(r"^[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,}$")
# DEFAULT_CC = ["dmartinez@jetrord.com", "nrivera@jetrord.com"]
DEFAULT_CC = ["nrivera@jetrord.com","dmartinez@jetrord.com"]
# DEFAULT_CC = []

# Read IDs from environment
CLIENT_ID = os.environ["CLIENT_ID"]
TENANT_ID = os.environ["TENANT_ID"]

AUTHORITY = f"https://login.microsoftonline.com/{TENANT_ID}"
# Use fully-qualified Graph scopes
SCOPES = [
    "https://graph.microsoft.com/User.Read",
    "https://graph.microsoft.com/Mail.Send",
]
TOKEN_CACHE_PATH = Path("../.msal_token_cache.bin")  # local cache file


# ---------- Auth helpers ----------
def _ensure_token_cache():
    cache = msal.SerializableTokenCache()
    if TOKEN_CACHE_PATH.exists():
        TOKEN_CACHE_PATH.read_text(encoding="utf-8")  # touch for locks on some FS
        cache.deserialize(TOKEN_CACHE_PATH.read_text(encoding="utf-8"))
    return cache

def _persist_cache(cache: msal.SerializableTokenCache):
    if cache.has_state_changed:
        TOKEN_CACHE_PATH.write_text(cache.serialize(), encoding="utf-8")

def _get_access_token() -> str:
    cache = _ensure_token_cache()
    app = msal.PublicClientApplication(
        CLIENT_ID, authority=AUTHORITY, token_cache=cache
    )

    # Try silent first
    accounts = app.get_accounts()
    result = app.acquire_token_silent(SCOPES, account=accounts[0]) if accounts else None

    # Device-code flow if needed
    if not result:
        flow = app.initiate_device_flow(scopes=SCOPES)
        if "user_code" not in flow:
            raise RuntimeError(f"Failed to create device flow: {flow}")
        print(flow["message"])  # Visit the URL and enter the code
        result = app.acquire_token_by_device_flow(flow)

    if "access_token" not in result:
        raise RuntimeError(f"Auth failed: {result.get('error_description', result)}")

    _persist_cache(cache)
    return result["access_token"]


# ---------- Formatting helpers ----------
def _as_recipients(addresses: Iterable[str]) -> List[dict]:
    """
    Normalize to Graph recipients:
      - split on ';' or ','
      - trim
      - validate via EMAIL_RE
      - dedupe case-insensitively
    """
    out: List[dict] = []
    bad: List[str] = []
    seen = set()
    for item in addresses or []:
        for addr in re.split(r"[;,]", str(item)):
            addr = addr.strip()
            if not addr:
                continue
            if not EMAIL_RE.fullmatch(addr):
                bad.append(addr); continue
            key = addr.lower()
            if key in seen:
                continue
            seen.add(key)
            out.append({"emailAddress": {"address": addr}})
    if bad:
        print(f"[WARN] Skipping invalid recipients: {bad}")
    return out


def _file_attachment(path: Path) -> dict:
    data = path.read_bytes()
    ctype, _ = mimetypes.guess_type(path.name)
    if not ctype:
        ctype = "application/octet-stream"
    return {
        "@odata.type": "#microsoft.graph.fileAttachment",
        "name": path.name,
        "contentType": ctype,
        "contentBytes": base64.b64encode(data).decode("ascii"),
    }


def send_email_with_graph(
    subject: str,
    html_path: str | Path,
    to_list: List[str],
    cc_list: Optional[List[str]] = None,   # <-- default None, we’ll apply DEFAULT_CC below
    attachments_dir: str | Path = "attachment_folder",
    attachment_names: Optional[Iterable[str]] = None,
    save_to_sent: bool = True,
):
    # 1) Read HTML body
    html_p = Path(html_path)
    html_body = html_p.read_text(encoding="utf-8")

    # 2) Resolve requested attachments (unchanged)
    attach_objs = []
    base_dir = Path(attachments_dir)
    if not base_dir.exists():
        base_dir.mkdir(parents=True, exist_ok=True)
        print(f"[INFO] Attachment folder created (no files attached): {base_dir}")

    if attachment_names:
        for name in attachment_names:
            fn = str(name).strip()
            if not fn:
                continue
            p = base_dir / fn
            if p.exists() and p.is_file():
                try:
                    attach_objs.append(_file_attachment(p))
                except Exception as e:
                    print(f"[WARN] Skipping unreadable attachment {p}: {e}")
            else:
                print(f"[WARN] Attachment not found in folder, skipping: {p}")
    else:
        print("[INFO] No attachment_names provided; sending without attachments.")

    # 3) Validate & compile recipients
    to_recips = _as_recipients(to_list)
    if not to_recips:
        raise RuntimeError("No valid 'To' recipients after validation.")

    # Apply default CCs when cc_list is None or empty; also include any provided CCs
    cc_effective = (cc_list or []) + DEFAULT_CC
    cc_recips = _as_recipients(cc_effective)

    # 4) Build message and send
    message = {
        "subject": subject,
        "body": {"contentType": "HTML", "content": html_body},
        "toRecipients": to_recips,
        "ccRecipients": cc_recips,
        "attachments": attach_objs,
    }

    token = _get_access_token()
    resp = requests.post(
        "https://graph.microsoft.com/v1.0/me/sendMail",
        headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
        data=json.dumps({"message": message, "saveToSentItems": save_to_sent}),
        timeout=30,
    )
    if not resp.ok:
        raise RuntimeError(f"Graph sendMail failed [{resp.status_code}]: {resp.text}")

    print(
        f"✔ Email sent to {len(to_recips)} recipient(s); "
        f"CC: {len(cc_recips)}; attachments: {len(attach_objs)}."
    )


