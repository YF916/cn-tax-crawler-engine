# crawler/parse.py
import re
from urllib.parse import urljoin
from lxml import html as lxml_html

from config import BASE_SITE



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
