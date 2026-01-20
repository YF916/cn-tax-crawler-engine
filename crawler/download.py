# crawler/download.py
import re
from pathlib import Path

from config import ATTACH_DIR, ATTACH_TIMEOUT, MAX_RETRIES
from net import build_attachment_headers, request_with_retry_plain


def is_permanent_attachment_error(resp) -> bool:
    """ 判断是否为“参数缺失/业务错误”导致的附件不可下载（不应重试/不应计入风控）。 """
    try:
        text = (resp.text or "")[:2000]
    except Exception:
        return False
    return "oid can not be null" in text


def download_one_attachment(page_session, rate, state, msg_id: str, att: dict) -> str:
    url = att.get("url", "")
    title = att.get("标题", "") or "file"
    fid = att.get("fileId") or (url.split("fileId=")[-1].split("&")[0] if "fileId=" in url else "unknown")

    ext = ".bin"
    m = re.search(r"\.([A-Za-z0-9]{1,8})$", title)
    if m:
        ext = "." + m.group(1)

    save_dir = Path(ATTACH_DIR) / msg_id
    save_dir.mkdir(parents=True, exist_ok=True)

    save_path = save_dir / f"{fid}{ext}"

    if save_path.exists() and save_path.stat().st_size > 1024:
        return str(save_path)

    headers = build_attachment_headers(page_session, msg_id)

    resp = request_with_retry_plain(
        rate, state,
        "GET", url,
        headers=headers,
        timeout=ATTACH_TIMEOUT,
        max_retries=MAX_RETRIES,
        stream=True,
        allow_redirects=True,
        block_if_html=True,
        is_permanent_attachment_error=is_permanent_attachment_error,
        # cookies=page_session.cookies.get_dict(),  # 如需尝试再打开
    )

    # 永久失败：直接跳过（不保存文件）
    if is_permanent_attachment_error(resp):
        raise Exception("attachment permanent invalid: oid can not be null")

    ct = (resp.headers.get("Content-Type") or "").lower()
    if "text/html" in ct:
        raise Exception(f"attachment blocked: content-type={ct}")

    size = 0
    with save_path.open("wb") as f:
        for chunk in resp.iter_content(chunk_size=1024 * 64):
            if not chunk:
                continue
            f.write(chunk)
            size += len(chunk)

    if size < 100:
        raise Exception(f"attachment too small: {size} bytes")

    return str(save_path)
