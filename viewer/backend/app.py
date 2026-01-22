from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware
from pathlib import Path
import json
from datetime import datetime
from typing import Optional
from fastapi import HTTPException
from fastapi.responses import FileResponse

ROOT = Path(__file__).resolve().parents[2]
DATA_DIR = ROOT / "data"
ATT_DIR = ROOT / "attachments"

STATE_PATH = DATA_DIR / "crawl_state.json"
QA_PATH = DATA_DIR / "qa_db.json"

app = FastAPI(title="CN Tax Crawler Viewer")

# CORS 中间件
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # 允许所有来源访问；本地先放开；上线再收紧
    allow_credentials=True, # 允许携带“凭证”
    allow_methods=["*"], # 允许所有 HTTP 方法
    allow_headers=["*"], # 允许前端发送任何请求头
)

def read_json(path: Path) -> dict:
    if not path.exists():
        return {}
    return json.loads(path.read_text(encoding="utf-8"))

# file 最后修改时间
def file_mtime_iso(path: Path) -> Optional[str]:
    if not path.exists():
        return None
    ts = path.stat().st_mtime # modification time
    return datetime.fromtimestamp(ts).isoformat(timespec="seconds")

@app.get("/api/overview")
def overview():
    state = read_json(STATE_PATH)
    qa = read_json(QA_PATH)

    records = (qa.get("records") or {})
    meta = (qa.get("meta") or {})

    failed_attachments = state.get("failed_attachments") or []

    return {
        "state": {
            "next_page": state.get("next_page"),
            "end_page": state.get("end_page"),
            "last_saved_at": state.get("last_saved_at"),
            "consec_403": state.get("consec_403"),
            "cooldown_until": state.get("cooldown_until"),
            "failed_pages": len(state.get("failed_pages") or []),
            "failed_ids": len(state.get("failed_ids") or []),
            "failed_attachments": len(failed_attachments),
        },
        "qa": {
            "count": meta.get("count", len(records)),
            "max_question_length": meta.get("max_question_length"),
            "max_question_id": meta.get("max_question_id"),
        },
        "files": {
            "crawl_state_mtime": file_mtime_iso(STATE_PATH),
            "qa_db_mtime": file_mtime_iso(QA_PATH),
        }
    }

@app.get("/api/failed_attachments")
def failed_attachments():
    state = read_json(STATE_PATH)
    return state.get("failed_attachments") or []

# /api/qa?q=增值税&status=ok&page=2&page_size=20
@app.get("/api/qa")
def qa_list(
    q: str = Query(default="", description="Search in 标题/问题内容/答复内容"),
    status: str = Query(default="", description="Filter by status, e.g. ok/failed"),
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=20, ge=1, le=200),
):
    qa = read_json(QA_PATH)
    records: dict = qa.get("records") or {}

    items = list(records.values())

    if status:
        items = [x for x in items if (x.get("status") or "") == status]

    if q:
        q2 = q.strip()
        def hit(x):
            return (q2 in (x.get("标题") or "")
                    or q2 in (x.get("问题内容") or "")
                    or q2 in (x.get("答复内容") or ""))
        items = [x for x in items if hit(x)]

    total = len(items)
    start = (page - 1) * page_size
    end = start + page_size

    def has_local_attachments(x):
        msg_id = x.get("id")
        if not msg_id:
            return False
        p = ATT_DIR / msg_id
        return p.exists() and any(p.iterdir())

    # 返回列表只带轻量字段
    rows = []
    for x in items[start:end]:
        rows.append({
            "id": x.get("id"),
            "标题": x.get("标题"),
            "留言时间": x.get("留言时间"),
            "纳税人所属地": x.get("纳税人所属地"),
            "答复时间": x.get("答复时间"),
            "答复机构": x.get("答复机构"),
            "status": x.get("status"),
            "url": x.get("url"),
            "附件数量": len(x.get("附件") or []),
            "本地附件": has_local_attachments(x),
        })

    return {"total": total, "page": page, "page_size": page_size, "rows": rows}

@app.get("/api/qa/{msg_id}")
def qa_detail(msg_id: str):
    qa = read_json(QA_PATH)
    records: dict = qa.get("records") or {}
    return records.get(msg_id) or {}

@app.get("/api/attachments")
def attachments_index():
    # 聚合 attachments/<msg_id>/ 下文件数量和大小
    out = []
    if not ATT_DIR.exists():
        return out
    for d in ATT_DIR.iterdir():
        if not d.is_dir():
            continue
        files = [p for p in d.iterdir() if p.is_file()]
        total_size = sum(p.stat().st_size for p in files)
        out.append({
            "msg_id": d.name,
            "file_count": len(files),
            "total_size": total_size,
        })
    # 文件多的话按 file_count 降序
    out.sort(key=lambda x: x["file_count"], reverse=True)
    return out

@app.get("/api/attachments/{msg_id}")
def attachments_by_msg(msg_id: str):
    base = ATT_DIR / msg_id
    if not base.exists() or not base.is_dir():
        return []

    out = []
    for p in base.iterdir():
        if not p.is_file():
            continue
        out.append({
            "filename": p.name,
            "fileId": p.stem,     # 去掉 .docx / .pdf
            "size": p.stat().st_size,
        })
    return out

@app.get("/api/file/{msg_id}/{filename}")
def download_file(msg_id: str, filename: str):
    base = (ATT_DIR / msg_id).resolve()
    file_path = (base / filename).resolve()

    # 防止 ../../ 越权
    if not str(file_path).startswith(str(base)):
        raise HTTPException(status_code=403, detail="Invalid path")

    if not file_path.exists():
        raise HTTPException(status_code=404, detail="File not found")

    return FileResponse(file_path, filename=filename)
