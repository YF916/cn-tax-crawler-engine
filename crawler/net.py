# crawler/net.py
import time
import random
import requests

from config import (
    BASE_URL_LIST, BASE_URL_DETAIL,
    TARGET_RPM,
    CONSEC_403_THRESHOLD, COOLDOWN_SECONDS,
    MAX_RETRIES,
    TIMEOUT
)


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
    def __init__(self, rpm: int = TARGET_RPM):
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
def request_with_retry(session, rate: RateLimiter, state: dict, method: str, url: str, *,
                       timeout=TIMEOUT, max_retries=MAX_RETRIES, **kwargs):

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
                             is_permanent_attachment_error=None,
                             **kwargs) -> requests.Response:
    """
    用 requests.request（非 session）发请求；
    is_permanent_attachment_error: 可注入一个函数(resp)->bool，命中则不重试直接返回
    """
    last_exc = None

    for attempt in range(1, max_retries + 1):
        maybe_cooldown(state)
        rate.wait()

        try:
            resp = requests.request(method, url, timeout=timeout, **kwargs)

            if callable(is_permanent_attachment_error) and is_permanent_attachment_error(resp):
                return resp

            if resp.status_code == 403:
                if callable(is_permanent_attachment_error) and is_permanent_attachment_error(resp):
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
                    state["consec_403"] = int(state.get("consec_403", 0)) + 1
                    print(f"[html-block] attempt={attempt} ct={ct} consec_403={state['consec_403']} url={resp.url}")

                    if state["consec_403"] >= CONSEC_403_THRESHOLD:
                        state["cooldown_until"] = time.time() + COOLDOWN_SECONDS
                        print(f"[html-block] 达到阈值，进入 cooldown {COOLDOWN_SECONDS//60} 分钟")

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

    if isinstance(last_exc, BaseException):
        raise last_exc
    raise RuntimeError(f"request failed after {max_retries} retries: {method} {url}")


# ===================== 列表/详情 =====================
def fetch_page(session, rate: RateLimiter, state: dict, page: int) -> dict:
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


def fetch_detail_html(session, rate: RateLimiter, state: dict, msg_id: str) -> str:
    resp = request_with_retry(
        session, rate, state,
        "GET", BASE_URL_DETAIL,
        params={"id": msg_id},
        timeout=TIMEOUT,
        max_retries=MAX_RETRIES,
    )
    return resp.text


def detect_end_page_from_first(session, rate: RateLimiter, state: dict) -> int:
    """
    调用第一页接口，直接读取 maxPage。
    """
    data = fetch_page(session, rate, state, 1)
    max_page = data.get("maxPage")

    if isinstance(max_page, int) and max_page > 0:
        return max_page

    return 1
