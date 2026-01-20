# crawler/main.py
from config import (
    BASE_URL_DETAIL,
    DB_FILE, STATE_FILE,
    DOWNLOAD_ATTACHMENTS,
    START_PAGE, END_PAGE,
    TARGET_RPM,
)
from storage import (
    load_db, save_db_atomic, upsert_record,
    load_state, save_state_atomic,
    dedup_list, add_unique, normalize_failed_attachment_item,
    update_attachment_local_path,
)
from net import (
    build_session, RateLimiter,
    fetch_page, fetch_detail_html,
    detect_end_page_from_first,
    maybe_cooldown,
)
from parse import parse_detail
from download import download_one_attachment


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
                                add_unique(state["null_msg_ids"], msg_id)
                                save_state_atomic(STATE_FILE, state)
                                print(f"[问答跳过-null附件] id={msg_id} 因附件oid-null，已记录到 state.null_msg_ids")
                                skip_whole_msg = True
                                break

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

                        if ("oid can not be null" in em) or ("permanent invalid" in em) or ("permanentalid" in em):
                            add_unique(state["null_msg_ids"], msg_id)
                            save_state_atomic(STATE_FILE, state)
                            print(f"[问答跳过-null附件] id={msg_id} 因附件oid-null，已记录到 state.null_msg_ids")
                            skip_whole_msg = True
                            break

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

                    if ("oid can not be null" in em) or ("permanent invalid" in em) or ("permanentalid" in em):
                        add_unique(state["null_msg_ids"], msg_id)
                        save_state_atomic(STATE_FILE, state)
                        print(f"[问答跳过-null附件] id={msg_id} 因附件oid-null，已记录到 state.null_msg_ids")
                        skip_whole_msg = True
                        break

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

            if msg_id not in db["records"]:
                still_failed.append(item)
                continue

            att = {"url": url, "标题": title, "fileId": fid}

            try:
                local_path = download_one_attachment(session, rate, state, msg_id, att)
                ok = update_attachment_local_path(db, msg_id, url, local_path)
                if not ok:
                    item["local_path"] = local_path
                save_db_atomic(DB_FILE, db)
            except Exception as e:
                print(f"[backfill 附件仍失败] {msg_id} {url} err={e}")
                still_failed.append(item)

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
