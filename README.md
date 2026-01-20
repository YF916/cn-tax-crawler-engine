# cn-tax-crawler-engine

一个**工程化、可断点续跑、具备反爬与风控策略**的中国税务问答爬虫，用于系统性抓取 `12366.chinatax.gov.cn` 的在线问答、详情页及其附件，并结构化存储为 JSON 数据集。

---

## 项目特点

* **断点续跑**（`state.json`）
* **Forward + Backfill 双阶段策略**
* **全局限速（RPM）**
* **403 风控冷却机制**
* **失败分级重试（page / msg / attachment）**
* **附件下载专用策略（规避 Ajax 拦截）**
* **原子写文件（防止中途中断损坏数据）**

适合 **长期稳定运行**，而非一次性脚本。

---

## 项目结构

```
cn-tax-crawler-engine/
├── crawler/
│   ├── main.py          # 主流程入口
│   ├── config.py        # 全局配置（路径 / 限速 / 重试）
│   ├── storage.py       # DB & state 读写 / 原子写
│   ├── net.py           # HTTP / retry / cooldown / RateLimiter
│   ├── parse.py         # HTML 解析逻辑
│   ├── download.py      # 附件下载逻辑
│   └── __init__.py
│
├── data/
│   ├── qa_db.json       # 问答数据库（自动生成）
│   └── crawl_state.json # 爬虫运行状态（自动生成）
│
├── attachments/         # 附件下载目录（按 msg_id 分目录）
├── requirements.txt
├── README.md
└── .gitignore
```

---

## 安装依赖

建议使用虚拟环境：

```bash
python -m venv venv
source venv/bin/activate   # macOS / Linux
```

安装依赖：

```bash
pip install -r requirements.txt
```

### `requirements.txt`

```txt
requests>=2.31.0
lxml>=5.0.0
```

---

## 运行方式

在 **项目根目录** 运行：

```bash
python crawler/main.py
```

无需额外参数，所有配置集中在 `crawler/config.py`。

---

## 主流程说明（MAIN）

```
MAIN
build_session()
  ↓
load_db(qa_db.json)
  ↓
load_state(crawl_state.json)
  ↓
获取 END_PAGE = 接口返回 maxPage
  ↓
判断是否还有 page 未抓
  ├─ 有 → 进入 FORWARD
  └─ 无 → 直接 BACKFILL 失败项
```

---

## FORWARD 阶段（主抓取）

```
FORWARD
maybe_cooldown() ← 如果之前触发 403 / html-block
  ↓
fetch_page(page)
├─ 失败（403 / 502 / 503 / 504 / timeout）
│   ↓
│   记录 failed_pages += page
│   更新 state.next_page
│   跳过该页，继续向前
└─ 成功
    ↓
    遍历 page_set 中的 msg
        ↓
        判断 msg_id 是否已存在
        fetch_detail_html(msg_id)
        parse_detail(html)
        ├─ 失败
        │   ↓
        │   记录 failed_ids += msg_id
        │   跳过该问答
        └─ 成功
            ↓
            下载附件（如开启）
            upsert_record(db, record)
            更新 meta 统计
```

当 `page > END_PAGE` 时，FORWARD 结束，进入 BACKFILL。

---

## BACKFILL 阶段（补失败项）

```
BACKFILL
├─ failed_pages
│   ↓
│   fetch_page(page)
├─ failed_ids
│   ↓
│   fetch_detail_html(msg_id)
└─ failed_attachments
    ↓
    download_one_attachment()
```

* 仍失败的项会重新放回对应失败队列
* 多次运行会逐步“收敛”失败集

---

## 风控与重试策略

### 1. 全局限速

* `RateLimiter`：**30 requests / minute**
* 列表 / 详情 / 附件请求统一计数

---

### 2. Retry 策略

对以下错误进行重试：

* HTTP `502 / 503 / 504`
* `requests.exceptions.Timeout`
* `requests.exceptions.RequestException`

退避策略：

```
第 1 次失败 → sleep 15s + jitter
第 2 次失败 → sleep 30s + jitter
第 3+ 次失败 → sleep 45/50s + jitter
```

---

### 3. 403 冷却机制

```
每次 403:
  state.consec_403 += 1

当 consec_403 >= 阈值:
  state.cooldown_until = now + COOLDOWN_SECONDS
```

* 冷却期间 **暂停所有请求**
* 任一请求成功后自动清零

---

## 附件下载策略

### 接口差异

| 类型   | headers                          | request 方式           |
| ---- | -------------------------------- | -------------------- |
| 问答接口 | Ajax headers（含 X-Requested-With） | `session.request()`  |
| 附件接口 | **非 Ajax headers**               | `requests.request()` |

❗ **附件接口如果使用 Ajax headers，会返回 `200 + text/html` 的拦截页**

---

### 下载规则

* 本地已存在且 **> 1KB** → 跳过
* 下载后总 size **< 100 bytes** → 判定失败
* HTML 返回页 → 视为被拦截，走 retry / cooldown

---

## `crawl_state.json` 结构

```json
{
  "next_page": 1,
  "failed_pages": [],
  "failed_ids": [],
  "failed_attachments": [],
  "consec_403": 0,
  "cooldown_until": 0.0,
  "last_saved_at": "2025-12-17T14:33:04",
  "end_page": 4577
}
```

---

## `qa_db.json` 结构

```json
{
  "meta": {
    "count": 100,
    "max_question_length": 100,
    "max_question_id": "..."
  },
  "records": {
    "msg_id": {
      "id": "...",
      "标题": "...",
      "留言时间": "...",
      "问题内容": "...",
      "答复内容": "...",
      "附件": [
        {
          "标题": "...",
          "url": "...",
          "fileId": "...",
          "local_path": "attachments/..."
        }
      ],
      "url": "...",
      "status": "ok"
    }
  }
}
```

---

## 设计原则总结

* **Forward 不阻塞**：失败即跳，保证整体进度
* **Backfill 兜底**：逐步补齐失败项
* **状态持久化**：任何时刻可安全中断
* **附件单独风控**：避免污染问答主流程

---

## 适用场景

* 法律 / 税务问答数据集构建
* RAG / 搜索 / 知识库原始语料
* 长时间、低频、稳定爬取任务




