#### cn-tax-crawler-engine

```
MAIN
build_session()           
  ↓
load_db(问答.json)
  ↓
load_state(state.json)
  ↓
获取 END_PAGE = maxPage
  ↓
判断前面是否还有 page
  ├─ 有 → FORWARD 继续向前
  └─ 无 → BACKFIll 失败项 failed_pages/failed_ids/failed_attachments
```

```
FORWARD
maybe_cooldown() ← 如果之前 403 / html-block，先冷却
  ↓
fetch_page(page)
├─ 失败（403 / 502 / 503 / 504 / 其他异常）
    ↓
    记录 failed_pages += page
    更新 state.next_page = page + 1
    跳过这一页，继续向前
└─ 成功
    ↓
    判断 page_set 是否有问答
    ├─ 有 → 遍历 page_set 中的每条问答 msg
              ↓
            判断 msg_id 是否已存在
            fetch_detail_html(msg_id)
            parse_detail(html)
            ├─ 失败
                ↓
                记录 failed_ids += msg_id
                跳过这一条问答，继续向前
            └─ 成功
                ↓
                下载附件 download_one_attachment()
                获取问答 record
                upsert_record(db, record)
                更新 count += 1 & max_question_length & max_question_id
    └─ 无 → state.next_page = page + 1 → page += 1
  ↓
page > END_PAGE → FORWARD 完成，开始 BACKFIll
```

```
BACKFIll
├─ failed_pages
    ↓
    maybe_cooldown()
    fetch_page(page)
├─ failed_ids
    ↓
    maybe_cooldown()
    fetch_detail_html(msg_id)
└─ failed_attachments
    ↓
    download_one_attachment()
```

```
1. 全局限速器 RateLimiter（30 req/min）
2. Retry 策略: 502, 503, 504, Timeout, RequestException
    失败第 1 次 → sleep 15 + rand
    失败第 2 次 → sleep 30 + rand
    失败第 3+ 次 → sleep 45/50 + rand
    (直到 MAX_RETRIES)
3. 403 冷却策略:
    每次请求收到 403: state.consec_403 += 1
    如果 consec_403 >= CONSEC_403_THRESHOLD: state.cooldown_until = now + COOLDOWN_SECONDS
    请求成功后清零 consec_403 = cooldown_until = 0
4. Forward 策略:
    page/msg/attachment 失败 → 记录 failed_pages/failed_ids/failed_attachments 后直接跳过
5. Backfill 策略:
    按 pages → msg_ids → attachments 顺序重试，如果仍失败，重新放回记录
6. 附件下载策略:
    本地存在且 > 1 KB 就跳过
    下载总 size < 100 bytes 就判失败
```

```
debug 问答接口 vs. 附件接口:
├─ 用不同的 headers:
    主要区别
    ├─ 问答: "Accept": "application/json, text/javascript, */*; q=0.01", "X-Requested-With": "XMLHttpRequest", ...
    └─ 附件: "Accept": "*/*", 不要 X-Requested-With, ...
    附件不能用 Ajax headers，否则服务器会返回 HTML 拦截页: 200 + text/html;charset=utf-8
└─ 用不同的 request() 路径:
    ├─ 问答: session.request() 积累 cookies	
    └─ 附件: requests.request() 不保存 cookies
```

```
可能会遇到的报错:
HTTP 403 Forbidden 访问频率过高/被标记异常
HTTP 502 Bad Gateway 
HTTP 503 Service Unavailable
HTTP 504 Gateway Timeout
requests.exceptions.Timeout
requests.exceptions.RequestException
```

```
state.json 格式
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

```
问答.json 格式
{
  "meta": {
    "count": 100,
    "max_question_length": 100,
    "max_question_id": "..."
  },
  "records": {
    "cec5f02383714e1aad4f89fa783a0799": {
      "id": "cec5f02383714e1aad4f89fa783a0799",
      "标题": "...",
      "留言时间": "...",
      "纳税人所属地": "...",
      "答复时间": "...",
      "问题内容": "...",
      "答复内容": "...",
      "答复机构": "...",
      "附件": [
        {
          "标题": "...",
          "url": "https://12366.chinatax.gov.cn/filecenter/fileupload/download?fileId=8ae8f9f098f5b2cb019a1a8a598b2e73&type=1",
          "fileId": "8ae8f9f098f5b2cb019a1a8a598b2e73",
          "local_path": "attachments/c0017e6e18954f6cb78ea89d4e7087c5/8ae8f9f098f5b2cb019a1a8a598b2e73.JPG"
        },
        {...}
      ]
      "url": "https://12366.chinatax.gov.cn/nszx/onlinemessage/detail?id=cec5f02383714e1aad4f89fa783a0799",
      "status": "ok"
    },
    "57d3b6e5c6284861bb2cec7875330eae": { ... },
    ...
  }
}
```

