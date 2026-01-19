import os
import re
import json
import time
import random
import requests
from datetime import datetime
from urllib.parse import urljoin
from lxml import html as lxml_html
from pathlib import Path

# ===================== 配置区 =====================
BASE_SITE = "https://12366.chinatax.gov.cn"
BASE_URL_LIST = "https://12366.chinatax.gov.cn/nszx/onlinemessage/messagelist"
BASE_URL_DETAIL = "https://12366.chinatax.gov.cn/nszx/onlinemessage/detail"

# DB_FILE = "qa_db.json"                 # JSON 问答库
# STATE_FILE = "crawl_state.json"        # 运行状态（断点、失败队列、cooldown）
#
# ATTACH_DIR = "attachments"
BASE_DIR = Path(__file__).resolve().parents[1]

DATA_DIR = BASE_DIR / "data"
ATTACH_DIR = BASE_DIR / "attachments"

DB_FILE = DATA_DIR / "qa_db.json"
STATE_FILE = DATA_DIR / "crawl_state.json"

DATA_DIR.mkdir(exist_ok=True)
ATTACH_DIR.mkdir(exist_ok=True)
DOWNLOAD_ATTACHMENTS = True

START_PAGE = 1
END_PAGE = 2

# 全局限速：30 req/min ~= 2s/req（列表/详情/附件都算）
TARGET_RPM = 30

# 403：连续阈值与冷却
CONSEC_403_THRESHOLD = 6
COOLDOWN_SECONDS = 20 * 60

# 每个请求重试次数（每次失败都会阶梯延迟）
MAX_RETRIES = 3

# requests timeout
TIMEOUT = (20, 120)    # (connect, read)
ATTACH_TIMEOUT = (20, 180)

def detect_end_page_from_first(session, rate, state) -> int:
    """
    调用第一页接口，直接读取 maxPage。
    """
    data = fetch_page(session, rate, state, 1)
    max_page = data.get("maxPage")

    if isinstance(max_page, int) and max_page > 0:
        return max_page

    # 兜底，防止接口异常
    return 1

# ===================== JSON DB：按 id 存记录 =====================
def load_db(path: str) -> dict:
    if not os.path.exists(path):
        return {
            "meta": {
                "count": 0,
                "max_question_length": 0,
                "max_question_id": "",
            },
            "records": {}
        }
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def save_db_atomic(path: Path, db: dict) -> None:
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)

    tmp = path.with_suffix(path.suffix + ".tmp")

    with tmp.open("w", encoding="utf-8") as f:
        json.dump(db, f, ensure_ascii=False, indent=2)

    os.replace(tmp, path)

def upsert_record(db: dict, record: dict):
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
def load_state(path: str) -> dict:
    if not os.path.exists(path):
        return {
            "next_page": START_PAGE,
            "failed_pages": [],
            "failed_ids": [],
            "failed_attachments": [],  # 结构化：{"id","url","标题","fileId"}
            "null_msg_ids": [],
            "consec_403": 0,
            "cooldown_until": 0.0,
            "last_saved_at": "",
        }
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)

def save_state_atomic(path: Path, st: dict):
    st["last_saved_at"] = datetime.now().isoformat(timespec="seconds")
    tmp = path.with_suffix(path.suffix + ".tmp")
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(st, f, ensure_ascii=False, indent=2)
    os.replace(tmp, path)

def dedup_list(lst: list):
    seen = set()
    out = []
    for x in lst:
        k = json.dumps(x, ensure_ascii=False, sort_keys=True) if isinstance(x, dict) else str(x)
        if k not in seen:
            seen.add(k)
            out.append(x)
    return out

def add_unique(lst: list, x):
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


# ===================== Session & headers =====================
def build_session() -> requests.Session:
    s = requests.Session()
    s.headers.update({
        "User-Agent": (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/142.0.0.0 Safari/537.36"
        ),
        "Accept": "application/json, text/javascript, */*; q=0.01",
        "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
        "Accept-Encoding": "gzip, deflate",  # 不带 br 更稳
        "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
        "X-Requested-With": "XMLHttpRequest",
        "Origin": "https://12366.chinatax.gov.cn",
        "Referer": "https://12366.chinatax.gov.cn/nszx/onlinemessage/main",
        "Connection": "keep-alive",
    })
    return s

def build_attachment_headers(page_session: requests.Session, msg_id: str) -> dict:
    # 不要带 X-Requested-With / Content-Type（这俩会让服务端以为你在走 Ajax）
    return {
        "User-Agent": page_session.headers.get("User-Agent", ""),
        "Accept": "*/*",
        "Accept-Language": page_session.headers.get("Accept-Language", "zh-CN,zh;q=0.9,en;q=0.8"),
        "Accept-Encoding": page_session.headers.get("Accept-Encoding", "gzip, deflate"),
        "Connection": "keep-alive",
        "Referer": f"{BASE_URL_DETAIL}?id={msg_id}",
        "Origin": "https://12366.chinatax.gov.cn",
    }


# ===================== 限速器：全局 30 req/min =====================
class RateLimiter:
    def __init__(self, rpm: int):
        self.interval = 60.0 / max(1, rpm)
        self.last_ts = 0.0

    def wait(self):
        now = time.time()
        if self.last_ts == 0:
            self.last_ts = now
            return
        elapsed = now - self.last_ts
        if elapsed < self.interval:
            time.sleep((self.interval - elapsed) + random.uniform(0.05, 0.25))
        self.last_ts = time.time()


class PermanentAttachmentError(Exception):
    """附件永久无效（不应重试，不应计入风控）"""
    pass


def backoff_sleep(attempt: int):
    # 阶梯：15 / 30 / 45或50 + jitter
    if attempt == 1:
        time.sleep(15 + random.uniform(0, 5))
    elif attempt == 2:
        time.sleep(30 + random.uniform(0, 8))
    else:
        time.sleep(random.choice([45, 50]) + random.uniform(0, 10))


# ===================== 403 冷却 =====================
def maybe_cooldown(state: dict):
    now = time.time()
    until = float(state.get("cooldown_until", 0.0) or 0.0)
    if now < until:
        remaining = int(until - now)
        print(f"[cooldown] 403 触发冷却，剩余 {remaining}s（约 {remaining//60} 分钟）")
        while time.time() < until:
            time.sleep(min(30, until - time.time()))


# ===================== 统一请求：重试 + 403 cooldown + 504/timeout =====================
def request_with_retry(session, rate, state, method, url, *,
                       timeout=TIMEOUT, max_retries=MAX_RETRIES,
                       **kwargs):

    last_exc = None
    for attempt in range(1, max_retries + 1):
        maybe_cooldown(state)
        rate.wait()
        try:
            resp = session.request(method, url, timeout=timeout, **kwargs)

            if resp.status_code == 403:
                state["consec_403"] = int(state.get("consec_403", 0)) + 1
                print(f"[403] attempt={attempt} consec_403={state['consec_403']} url={resp.url}")
                if state["consec_403"] >= CONSEC_403_THRESHOLD:
                    state["cooldown_until"] = time.time() + COOLDOWN_SECONDS
                    print(f"[403] 达到阈值，进入 cooldown {COOLDOWN_SECONDS//60} 分钟")
                backoff_sleep(attempt)
                continue

            if resp.status_code in (502, 503, 504):
                print(f"[{resp.status_code}] attempt={attempt} url={resp.url}")
                backoff_sleep(attempt)
                continue

            resp.raise_for_status()

            state["consec_403"] = 0
            state["cooldown_until"] = 0.0
            return resp

        except requests.exceptions.Timeout as e:
            last_exc = e
            print(f"[timeout] attempt={attempt} {type(e).__name__}: {e}")
            backoff_sleep(attempt)
        except requests.exceptions.RequestException as e:
            last_exc = e
            print(f"[request error] attempt={attempt} {type(e).__name__}: {e}")
            backoff_sleep(attempt)

    raise last_exc


def request_with_retry_plain(rate: RateLimiter, state: dict,
                             method: str, url: str, *,
                             timeout=TIMEOUT,
                             max_retries=MAX_RETRIES,
                             block_if_html=False,
                             **kwargs) -> requests.Response:
    last_exc = None

    for attempt in range(1, max_retries + 1):
        maybe_cooldown(state)
        rate.wait()

        try:
            resp = requests.request(method, url, timeout=timeout, **kwargs)

            if is_permanent_attachment_error(resp):
                return resp

            if resp.status_code == 403:
                if is_permanent_attachment_error(resp):
                    return resp

                state["consec_403"] = int(state.get("consec_403", 0)) + 1
                print(f"[403] attempt={attempt} consec_403={state['consec_403']} url={resp.url}")

                if state["consec_403"] >= CONSEC_403_THRESHOLD:
                    state["cooldown_until"] = time.time() + COOLDOWN_SECONDS
                    print(f"[403] 达到阈值，进入 cooldown {COOLDOWN_SECONDS//60} 分钟")

                backoff_sleep(attempt)
                continue

            if resp.status_code in (502, 503, 504):
                print(f"[{resp.status_code}] attempt={attempt} url={resp.url}")
                backoff_sleep(attempt)
                continue

            if block_if_html:
                ct = (resp.headers.get("Content-Type") or "").lower()
                if "text/html" in ct:
                    # 把 HTML 拦截也当成“可重试失败”，并复用 cooldown 机制
                    state["consec_403"] = int(state.get("consec_403", 0)) + 1
                    print(f"[html-block] attempt={attempt} ct={ct} consec_403={state['consec_403']} url={resp.url}")

                    if state["consec_403"] >= CONSEC_403_THRESHOLD:
                        state["cooldown_until"] = time.time() + COOLDOWN_SECONDS
                        print(f"[html-block] 达到阈值，进入 cooldown {COOLDOWN_SECONDS//60} 分钟")

                    backoff_sleep(attempt)
                    continue

            resp.raise_for_status()

            # 成功：清空 403 计数与冷却
            state["consec_403"] = 0
            state["cooldown_until"] = 0.0
            return resp

        except requests.exceptions.Timeout as e:
            last_exc = e
            print(f"[timeout] attempt={attempt} {type(e).__name__}: {e}")
            backoff_sleep(attempt)

        except requests.exceptions.RequestException as e:
            last_exc = e
            print(f"[request error] attempt={attempt} {type(e).__name__}: {e}")
            backoff_sleep(attempt)

    if isinstance(last_exc, BaseException):
        raise last_exc
    raise RuntimeError(f"request failed after {max_retries} retries: {method} {url}")


# ===================== 列表/详情 =====================
def fetch_page(session, rate, state, page: int) -> dict:
    payload = {
        "currentPage": page,
        "nr": "",
        "jg": "",
        "zxjg": "",
        "lykssj": "",
        "lyjssj": "",
        "dfkssj": "",
        "dfjssj": "",
    }
    resp = request_with_retry(
        session, rate, state,
        "POST", BASE_URL_LIST,
        data=payload,
        timeout=TIMEOUT,
        max_retries=MAX_RETRIES,
    )
    return resp.json()

def fetch_detail_html(session, rate, state, msg_id: str) -> str:
    resp = request_with_retry(
        session, rate, state,
        "GET", BASE_URL_DETAIL,
        params={"id": msg_id},
        timeout=TIMEOUT,
        max_retries=MAX_RETRIES,
    )
    return resp.text


# ===================== 解析：详情页全字段 =====================
def extract_by_th_label(tree, label: str) -> str:
    tables = tree.xpath("//table[contains(@class, 'tabform')]")
    if not tables:
        return ""
    table = tables[0]

    tr_list = table.xpath(".//tr[th[contains(normalize-space(.), $label)]]", label=label)
    if not tr_list:
        return ""

    tr = tr_list[0]

    texts = tr.xpath(".//td//textarea//text()")
    texts = [t.strip() for t in texts if t.strip()]
    if texts:
        return "\n".join(texts)

    inputs = tr.xpath(".//td//input/@value")
    inputs = [t.strip() for t in inputs if t.strip()]
    if inputs:
        return inputs[0]

    texts = tr.xpath(".//td//text()")
    texts = [t.strip() for t in texts if t.strip()]
    if texts:
        return "\n".join(texts)

    return ""

def extract_title(tree) -> str:
    return tree.xpath("string(//div[contains(@class,'articletitle')]//h1)").strip()

def extract_leave_time(tree) -> str:
    return tree.xpath("string(//*[@id='cjsj'])").strip()

def extract_nsrssd_from_script(html_text: str) -> str:
    m = re.search(r'var\s+jgmc\s*=\s*"([^"]+)"\s*;', html_text)
    if not m:
        return ""
    jgmc = m.group(1).strip()
    if ("黑龙江" in jgmc) or ("内蒙古" in jgmc):
        return jgmc[:3]
    return jgmc[:2]

def extract_attachments_from_script(html_text: str):
    attachments = []
    m = re.search(r"var\s+fj\s*=\s*\[(.*?)\];", html_text, re.S)
    if not m:
        return attachments

    inner = m.group(1)
    item_pattern = re.compile(r"\{[^}]*?id:'([^']+)'[^}]*?wjmc:'([^']+)'[^}]*?\}")
    for fid, name in item_pattern.findall(inner):
        url = urljoin(BASE_SITE, f"/filecenter/fileupload/download?fileId={fid}&type=1")
        attachments.append({"标题": name, "url": url, "fileId": fid})
    return attachments

def parse_detail(html_text: str) -> dict:
    tree = lxml_html.fromstring(html_text)

    title = extract_title(tree)
    leave_time = extract_leave_time(tree)
    nsrssd = extract_nsrssd_from_script(html_text)

    question_text = extract_by_th_label(tree, "问题内容")
    answer_text = extract_by_th_label(tree, "答复内容")
    reply_org = extract_by_th_label(tree, "答复机构")
    reply_time = extract_by_th_label(tree, "答复时间")

    attachments = extract_attachments_from_script(html_text)

    return {
        "标题": title,
        "留言时间": leave_time,
        "纳税人所属地": nsrssd,
        "答复时间": reply_time,
        "问题内容": question_text,
        "答复内容": answer_text,
        "答复机构": reply_org,
        "附件": attachments,
    }


# ===================== 附件下载（失败不阻塞主流程） =====================

def is_permanent_attachment_error(resp) -> bool:
    """ 判断是否为“参数缺失/业务错误”导致的附件不可下载（不应重试/不应计入风控）。 """
    ct = (resp.headers.get("Content-Type") or "").lower()
    text = ""
    try: # 小心：stream=True 时尽量别读太多，这里只读少量即可
        text = resp.text[:2000]
    except Exception:
        return False
    if "oid can not be null" in text:
        return True
    return False


def download_one_attachment(page_session, rate, state, msg_id: str, att: dict) -> str:
    url = att.get("url", "")
    title = att.get("标题", "") or "file"
    fid = att.get("fileId") or (url.split("fileId=")[-1].split("&")[0] if "fileId=" in url else "unknown")

    ext = ".bin"
    m = re.search(r"\.([A-Za-z0-9]{1,8})$", title)
    if m:
        ext = "." + m.group(1)

    save_dir = os.path.join(ATTACH_DIR, msg_id)
    os.makedirs(save_dir, exist_ok=True)
    save_path = os.path.join(save_dir, f"{fid}{ext}")

    if os.path.exists(save_path) and os.path.getsize(save_path) > 1024:
        return save_path

    headers = build_attachment_headers(page_session, msg_id)

    # 关键：附件请求用 requests.request（plain），不要用 page_session.request
    # 可选：如果你确认“带 cookie 也能下”，可以把 cookies=page_session.cookies.get_dict() 加上；
    # 但你刚验证过“不带 session 更稳”，所以这里默认不带 cookie。
    resp = request_with_retry_plain(
        rate, state,
        "GET", url,
        headers=headers,
        timeout=ATTACH_TIMEOUT,
        max_retries=MAX_RETRIES,
        stream=True,
        allow_redirects=True,
        block_if_html=True,
        # cookies=page_session.cookies.get_dict(),  # ← 如需尝试再打开
    )

    # 永久失败：直接跳过（不保存文件）
    if is_permanent_attachment_error(resp):
        raise Exception("attachment permanent invalid: oid can not be null")

    ct = (resp.headers.get("Content-Type") or "").lower()
    if "text/html" in ct:
        raise Exception(f"attachment blocked: content-type={ct}")

    size = 0
    with open(save_path, "wb") as f:
        for chunk in resp.iter_content(chunk_size=1024 * 64):
            if not chunk:
                continue
            f.write(chunk)
            size += len(chunk)

    if size < 100:
        raise Exception(f"attachment too small: {size} bytes")

    return save_path




# ===================== 主逻辑：Forward -> Backfill pages -> Backfill ids -> Backfill attachments =====================
def main():

    session = build_session()
    rate = RateLimiter(TARGET_RPM)

    db = load_db(DB_FILE)
    state = load_state(STATE_FILE)

    # ---------- 自动写入 end_page（来自 maxPage） ----------
    if not state.get("end_page"):
        try:
            end_page = detect_end_page_from_first(session, rate, state)
            state["end_page"] = int(end_page)
            save_state_atomic(STATE_FILE, state)
            print(f"[state] 已从 maxPage 写入 end_page={state['end_page']}")
        except Exception as e:
            # 理论上不该失败，但兜底一下
            state["end_page"] = END_PAGE
            save_state_atomic(STATE_FILE, state)
            print(f"[state] 读取 maxPage 失败，回退到手动 END_PAGE={END_PAGE}, err={e}")
    else:
        print(f"[state] 使用已保存的 end_page={state['end_page']}")

    end_page = int(state.get("end_page", END_PAGE))

    # 去重防膨胀
    state["failed_pages"] = dedup_list(state.get("failed_pages", []))
    state["failed_ids"] = dedup_list(state.get("failed_ids", []))

    # failed_attachments 兼容旧格式并去重
    fa_norm = []
    for x in state.get("failed_attachments", []):
        it = normalize_failed_attachment_item(x)
        if it:
            fa_norm.append(it)
    state["failed_attachments"] = dedup_list(fa_norm)

    save_state_atomic(STATE_FILE, state)

    print(f"DB已有记录：{len(db['records'])}")
    print(f"从 next_page={state.get('next_page', START_PAGE)} forward 到 {end_page}")

    # ---------- Forward ----------
    page = max(int(state.get("next_page", START_PAGE)), START_PAGE)
    do_forward = page <= end_page

    if not do_forward:
        print("[resume] forward 已完成，直接 backfill 剩余失败项")

    if do_forward:
        while page <= end_page:
            maybe_cooldown(state)

            try:
                data = fetch_page(session, rate, state, page)
            except Exception as e:
                print(f"[列表失败] page={page} err={e}")
                add_unique(state["failed_pages"], page)
                state["next_page"] = page + 1
                save_state_atomic(STATE_FILE, state)
                page += 1
                continue

            page_set = data.get("pageSet") or []
            print(f"[forward] page={page} items={len(page_set)} consec_403={state.get('consec_403', 0)}")

            if not page_set:
                state["next_page"] = page + 1
                save_state_atomic(STATE_FILE, state)
                page += 1
                continue

            for raw in page_set:
                msg_id = raw.get("id")
                if not msg_id:
                    continue
                if msg_id in db["records"]:
                    continue

                try:
                    html_text = fetch_detail_html(session, rate, state, msg_id)
                    detail = parse_detail(html_text)
                except Exception as e:
                    print(f"[详情失败] id={msg_id} err={e}")
                    add_unique(state["failed_ids"], msg_id)
                    continue

                skip_whole_msg = False

                if DOWNLOAD_ATTACHMENTS:
                    for att in (detail.get("附件") or []):
                        try:
                            local_path = download_one_attachment(session, rate, state, msg_id, att)
                            att["local_path"] = local_path
                        except Exception as e:
                            em = (str(e) or "").lower()

                            # ✅ 命中 null：记录“问答 id”，然后跳过整个问答
                            if ("oid can not be null" in em) or ("permanent invalid" in em) or ("permanentalid" in em):
                                add_unique(state["null_msg_ids"], msg_id)  # 存问答 id，不存附件
                                save_state_atomic(STATE_FILE, state)  # 立刻落盘，防止你中途暂停丢
                                print(f"[问答跳过-null附件] id={msg_id} 因附件oid-null，已记录到 state.null_attachments")
                                skip_whole_msg = True
                                break  # 结束附件循环

                            item = {
                                "id": msg_id,
                                "url": att.get("url", ""),
                                "标题": att.get("标题", ""),
                                "fileId": att.get("fileId", ""),
                            }
                            add_unique(state["failed_attachments"], item)
                            print(f"[附件失败] {msg_id} {att.get('url','')} err={e}")

                if skip_whole_msg:
                    continue

                record = {"id": msg_id, **detail, "url": f"{BASE_URL_DETAIL}?id={msg_id}"}
                upsert_record(db, record)
                save_db_atomic(DB_FILE, db)

            state["next_page"] = page + 1
            save_state_atomic(STATE_FILE, state)
            page += 1

    print("\n=== forward 完成，开始 backfill（最后一次性补） ===")

    # ---------- Backfill pages ----------
    failed_pages = sorted(dedup_list(state.get("failed_pages", [])))
    state["failed_pages"] = []
    save_state_atomic(STATE_FILE, state)

    for p in failed_pages:
        maybe_cooldown(state)
        try:
            data = fetch_page(session, rate, state, p)
        except Exception as e:
            print(f"[backfill 列表仍失败] page={p} err={e}")
            add_unique(state["failed_pages"], p)
            save_state_atomic(STATE_FILE, state)
            continue

        page_set = data.get("pageSet") or []
        print(f"[backfill pages] page={p} items={len(page_set)}")

        for raw in page_set:
            msg_id = raw.get("id")
            if not msg_id or msg_id in db["records"]:
                continue

            try:
                html_text = fetch_detail_html(session, rate, state, msg_id)
                detail = parse_detail(html_text)
            except Exception as e:
                print(f"[backfill 详情失败] id={msg_id} err={e}")
                add_unique(state["failed_ids"], msg_id)
                continue

            skip_whole_msg = False

            if DOWNLOAD_ATTACHMENTS:
                for att in (detail.get("附件") or []):
                    try:
                        local_path = download_one_attachment(session, rate, state, msg_id, att)
                        att["local_path"] = local_path
                    except Exception as e:
                        em = (str(e) or "").lower()

                        # ✅ 命中 null：记录“问答 id”，然后跳过整个问答
                        if ("oid can not be null" in em) or ("permanent invalid" in em) or ("permanentalid" in em):
                            add_unique(state["null_msg_ids"], msg_id)  # 存问答 id，不存附件
                            save_state_atomic(STATE_FILE, state)  # 立刻落盘，防止你中途暂停丢
                            print(f"[问答跳过-null附件] id={msg_id} 因附件oid-null，已记录到 state.null_attachments")
                            skip_whole_msg = True
                            break  # 结束附件循环

                        item = {
                            "id": msg_id,
                            "url": att.get("url", ""),
                            "标题": att.get("标题", ""),
                            "fileId": att.get("fileId", ""),
                        }
                        add_unique(state["failed_attachments"], item)
                        print(f"[backfill 附件失败] {msg_id} {att.get('url','')} err={e}")

            if skip_whole_msg:
                continue

            record = {"id": msg_id, **detail, "url": f"{BASE_URL_DETAIL}?id={msg_id}"}
            upsert_record(db, record)
            save_db_atomic(DB_FILE, db)

        save_state_atomic(STATE_FILE, state)

    # ---------- Backfill ids ----------
    failed_ids = dedup_list(state.get("failed_ids", []))
    state["failed_ids"] = []
    save_state_atomic(STATE_FILE, state)

    for msg_id in failed_ids:
        if msg_id in db["records"]:
            continue
        maybe_cooldown(state)

        try:
            html_text = fetch_detail_html(session, rate, state, msg_id)
            detail = parse_detail(html_text)
        except Exception as e:
            print(f"[backfill 详情仍失败] id={msg_id} err={e}")
            add_unique(state["failed_ids"], msg_id)
            save_state_atomic(STATE_FILE, state)
            continue

        skip_whole_msg = False

        if DOWNLOAD_ATTACHMENTS:
            for att in (detail.get("附件") or []):
                try:
                    local_path = download_one_attachment(session, rate, state, msg_id, att)
                    att["local_path"] = local_path
                except Exception as e:
                    em = (str(e) or "").lower()

                    # ✅ 命中 null：记录“问答 id”，然后跳过整个问答
                    if ("oid can not be null" in em) or ("permanent invalid" in em) or ("permanentalid" in em):
                        add_unique(state["null_msg_ids"], msg_id)  # 存问答 id，不存附件
                        save_state_atomic(STATE_FILE, state)  # 立刻落盘，防止你中途暂停丢
                        print(f"[问答跳过-null附件] id={msg_id} 因附件oid-null，已记录到 state.null_attachments")
                        skip_whole_msg = True
                        break  # 结束附件循环

                    item = {
                        "id": msg_id,
                        "url": att.get("url", ""),
                        "标题": att.get("标题", ""),
                        "fileId": att.get("fileId", ""),
                    }
                    add_unique(state["failed_attachments"], item)
                    print(f"[backfill 附件失败] {msg_id} {att.get('url','')} err={e}")

        if skip_whole_msg:
            continue

        record = {"id": msg_id, **detail, "url": f"{BASE_URL_DETAIL}?id={msg_id}"}
        upsert_record(db, record)
        save_db_atomic(DB_FILE, db)
        save_state_atomic(STATE_FILE, state)

    # ---------- Backfill attachments（末尾统一补） ----------
    if DOWNLOAD_ATTACHMENTS:
        print("\n=== backfill attachments（末尾统一补） ===")

        # 重新规范化+去重（兼容旧格式）
        fa_norm = []
        for x in state.get("failed_attachments", []):
            it = normalize_failed_attachment_item(x)
            if it and it.get("id") and it.get("url"):
                fa_norm.append(it)
        fa_norm = dedup_list(fa_norm)

        state["failed_attachments"] = []
        save_state_atomic(STATE_FILE, state)

        still_failed = []

        for item in fa_norm:
            msg_id = item["id"]
            url = item["url"]
            title = item.get("标题", "")
            fid = item.get("fileId", "")

            # 如果该 msg_id 记录还不存在，那附件也没法“挂回去”，先留着
            if msg_id not in db["records"]:
                still_failed.append(item)
                continue

            # 构造一个 att dict 给下载函数
            att = {"url": url, "标题": title, "fileId": fid}

            try:
                local_path = download_one_attachment(session, rate, state, msg_id, att)
                ok = update_attachment_local_path(db, msg_id, url, local_path)
                # 即使没匹配到对应附件项，也不阻塞；至少文件下来了
                if not ok:
                    # 兜底：把这个 local_path 记在 item 上，方便你手动对齐
                    item["local_path"] = local_path
                save_db_atomic(DB_FILE, db)
            except Exception as e:
                print(f"[backfill 附件仍失败] {msg_id} {url} err={e}")
                still_failed.append(item)

            # 状态也间歇落盘
            if len(still_failed) % 50 == 0:
                state["failed_attachments"] = still_failed[:]
                save_state_atomic(STATE_FILE, state)

        state["failed_attachments"] = still_failed
        save_state_atomic(STATE_FILE, state)

    # 收尾去重
    state["failed_pages"] = dedup_list(state.get("failed_pages", []))
    state["failed_ids"] = dedup_list(state.get("failed_ids", []))
    state["failed_attachments"] = dedup_list(state.get("failed_attachments", []))
    save_state_atomic(STATE_FILE, state)

    print("\n=== 结束 ===")
    print(f"DB记录数：{db['meta']['count']}")
    print(f"仍失败 pages：{len(state['failed_pages'])}")
    print(f"仍失败 ids：{len(state['failed_ids'])}")
    print(f"仍失败 attachments：{len(state['failed_attachments'])}")
    print(f"DB文件：{DB_FILE}")
    print(f"STATE文件：{STATE_FILE}")

if __name__ == "__main__":
    main()
