import re
from pathlib import Path
from typing import Iterable, Union
from html import escape

def generate_body(items: Iterable[Union[str, int, float]],
                  out_dir: str = "./email_html",
                  filename: str = "body.html") -> Path:
    """
    Create/overwrite ./email_html/body.html with:
      Greetings,
      Please confirm the following POs:
      <each item on its own line>
    """
    items = [escape(str(x).strip()) for x in (items or []) if str(x).strip()]
    po_lines = "<br>\n        ".join(items)

    html = f"""<!doctype html>
<html>
  <head>
    <meta charset="utf-8">
    <title>Email Body</title>
  </head>
  <body style="margin:0;">
    <div style="font-family:Segoe UI, Arial, Helvetica, sans-serif; font-size:14px; line-height:1.6;">
      <p style="margin:0 0 12px 0;">Greetings,</p>
      <p style="margin:0 0 12px 0;">Please confirm the following POs:</p>
      <div style="white-space:normal;">
        {po_lines}
      </div>
    </div>
  </body>
</html>
"""
    p = Path(out_dir)
    p.mkdir(parents=True, exist_ok=True)
    file_path = p / filename
    file_path.write_text(html, encoding="utf-8")
    return file_path


def combine_body_signature(
    body_path: str = "./email_html/body.html",
    signature_path: str = "./email_html/signature.html",
    out_path: str = "./email_html/main.html",
    padding_px: int = 24,
) -> Path:
    """
    Combine body.html and signature.html into main.html:
    - Single full-width container
    - Body first, then spacing, then signature
    - No divider
    """

    body_p = Path(body_path)
    sig_p = Path(signature_path)
    out_p = Path(out_path)
    if not body_p.exists():
        raise FileNotFoundError(f"Body file not found: {body_p}")
    if not sig_p.exists():
        raise FileNotFoundError(f"Signature file not found: {sig_p}")

    def inner(html_text: str) -> str:
        m = re.search(r"<body[^>]*>(.*?)</body>", html_text, flags=re.I | re.S)
        return (m.group(1) if m else html_text).strip()

    body_inner = inner(body_p.read_text(encoding="utf-8"))
    sig_inner  = inner(sig_p.read_text(encoding="utf-8"))

    combined = f"""<!doctype html>
<html>
  <head>
    <meta charset="utf-8">
    <title>Email</title>
    <meta http-equiv="x-ua-compatible" content="ie=edge">
    <meta name="viewport" content="width=device-width, initial-scale=1">
  </head>
  <body style="margin:0;">
    <div style="padding:{padding_px}px; font-family:Segoe UI, Arial, Helvetica, sans-serif; font-size:14px; line-height:1.6;">
      {body_inner}
      <div style="height:24px;"></div>
      {sig_inner}
    </div>
  </body>
</html>
"""
    out_p.parent.mkdir(parents=True, exist_ok=True)
    out_p.write_text(combined, encoding="utf-8")
    return out_p

