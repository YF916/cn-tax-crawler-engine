# crawler/storage.py
import json
import os
from pathlib import Path
from datetime import datetime

from config import START_PAGE



def atomic_write_json(path: Path, data: dict) -> None:
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)

    tmp = path.with_suffix(path.suffix + ".tmp")
    with tmp.open("w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

    os.replace(tmp, path)


# ===================== JSON DB：按 id 存记录 =====================
def load_db(path: Path) -> dict:
    path = Path(path)
    if not path.exists():
        return {
            "meta": {
                "count": 0,
                "max_question_length": 0,
                "max_question_id": "",
            },
            "records": {}
        }
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def save_db_atomic(path: Path, db: dict) -> None:
    atomic_write_json(path, db)


def upsert_record(db: dict, record: dict) -> None:
    rid = record["id"]
    record["status"] = "ok"
    is_new = rid not in db["records"]
    db["records"][rid] = record

    if is_new:
        db["meta"]["count"] += 1

    # 更新 max_question_length
    q = record.get("问题内容") or ""
    q_len = len(q)
    if q_len > db["meta"].get("max_question_length", 0):
        db["meta"]["max_question_length"] = q_len
        db["meta"]["max_question_id"] = rid


def update_attachment_local_path(db: dict, msg_id: str, url: str, local_path: str) -> bool:
    """
    在 db.records[msg_id]["附件"] 里按 url/fileId 找到对应附件，写入 local_path。
    找不到也不算致命，返回 False。
    """
    rec = db["records"].get(msg_id)
    if not rec:
        return False

    atts = rec.get("附件") or []
    if not isinstance(atts, list):
        return False

    # 优先用 url 精确匹配
    for att in atts:
        if isinstance(att, dict) and att.get("url") == url:
            att["local_path"] = local_path
            return True

    # 兜底：按 fileId 匹配
    fid = ""
    if "fileId=" in url:
        fid = url.split("fileId=")[-1].split("&")[0]

    if fid:
        for att in atts:
            if isinstance(att, dict) and (att.get("fileId") == fid or att.get("id") == fid):
                att["local_path"] = local_path
                return True

    return False


# ===================== STATE：failed_pages/failed_ids + 断点续跑 =====================
def load_state(path: Path) -> dict:
    path = Path(path)
    if not path.exists():
        return {
            "next_page": START_PAGE,
            "failed_pages": [],
            "failed_ids": [],
            "failed_attachments": [],  # 结构化：{"id","url","标题","fileId"}
            "null_msg_ids": [],
            "consec_403": 0,
            "cooldown_until": 0.0,
            "last_saved_at": "",
            # end_page 将在运行时写入
        }

    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def save_state_atomic(path: Path, st: dict) -> None:
    st["last_saved_at"] = datetime.now().isoformat(timespec="seconds")
    atomic_write_json(path, st)


def dedup_list(lst: list) -> list:
    seen = set()
    out = []
    for x in lst:
        k = json.dumps(x, ensure_ascii=False, sort_keys=True) if isinstance(x, dict) else str(x)
        if k not in seen:
            seen.add(k)
            out.append(x)
    return out


def add_unique(lst: list, x) -> None:
    # dict 的唯一性按序列化 key
    if isinstance(x, dict):
        k = json.dumps(x, ensure_ascii=False, sort_keys=True)
        for y in lst:
            ky = json.dumps(y, ensure_ascii=False, sort_keys=True) if isinstance(y, dict) else str(y)
            if ky == k:
                return
        lst.append(x)
        return

    if x not in lst:
        lst.append(x)


def normalize_failed_attachment_item(x):
    """
    兼容旧格式：
    - "msgid:url"
    新格式：
    - {"id":..., "url":..., "标题":..., "fileId":...}
    """
    if isinstance(x, dict):
        if "id" in x and "url" in x:
            return x
        return None

    if isinstance(x, str):
        if ":" in x:
            msg_id, url = x.split(":", 1)
            return {"id": msg_id.strip(), "url": url.strip(), "标题": "", "fileId": ""}

    return None
