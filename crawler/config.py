# crawler/config.py
from pathlib import Path

# ===================== 站点配置 =====================
BASE_SITE = "https://12366.chinatax.gov.cn"
BASE_URL_LIST = "https://12366.chinatax.gov.cn/nszx/onlinemessage/messagelist"
BASE_URL_DETAIL = "https://12366.chinatax.gov.cn/nszx/onlinemessage/detail"

# ===================== 路径配置 =====================
# 运行：python crawler/main_legacy.py 时
# __file__ = <repo>/crawler/config.py
# parents[1] = <repo>
BASE_DIR = Path(__file__).resolve().parents[1]

DATA_DIR = BASE_DIR / "data"
ATTACH_DIR = BASE_DIR / "attachments"

DB_FILE = DATA_DIR / "qa_db.json"
STATE_FILE = DATA_DIR / "crawl_state.json"

# 自动创建目录
DATA_DIR.mkdir(parents=True, exist_ok=True)
ATTACH_DIR.mkdir(parents=True, exist_ok=True)

# ===================== 运行开关 =====================
DOWNLOAD_ATTACHMENTS = True

START_PAGE = 1
END_PAGE = 5000

# 全局限速：30 req/min ~= 2s/req（列表/详情/附件都算）
TARGET_RPM = 30

# 403：连续阈值与冷却
CONSEC_403_THRESHOLD = 6
COOLDOWN_SECONDS = 20 * 60

# 每个请求重试次数（每次失败都会阶梯延迟）
MAX_RETRIES = 3

# requests timeout
TIMEOUT = (20, 120)       # (connect, read)
ATTACH_TIMEOUT = (20, 180)
