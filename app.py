# -*- coding: utf-8 -*-
import os
import re
import csv
import io
import time
import json
import math
import datetime as dt
from typing import List, Dict, Tuple, Optional

# ---------- Playwright ----------
from playwright.sync_api import sync_playwright

# ---------- Drive (OAuth) ----------
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload, MediaFileUpload
from google.oauth2.credentials import Credentials

# ---------- Requests / Parser ----------
from bs4 import BeautifulSoup

# =========================
# Config
# =========================
QOO10_URL = "https://www.qoo10.jp/gmkt.inc/Mobile/Bestsellers/Default.aspx?group_code=2"  # Beauty/ビューティ
DATA_DIR = "data"
TOP_LIMIT_SAVE = 200   # 저장 상한
TOP_LIMIT_COMPARE = 30 # 비교 구간 상한 (급상승/뉴랭커/급하락 산정용)

# Slack / Drive ENV
SLACK_WEBHOOK_URL = os.getenv("SLACK_WEBHOOK_URL", "")
GDRIVE_FOLDER_ID   = os.getenv("GDRIVE_FOLDER_ID", "")

GOOGLE_CLIENT_ID     = os.getenv("GOOGLE_CLIENT_ID", "")
GOOGLE_CLIENT_SECRET = os.getenv("GOOGLE_CLIENT_SECRET", "")
GOOGLE_REFRESH_TOKEN = os.getenv("GOOGLE_REFRESH_TOKEN", "")

# =========================
# Utils
# =========================
def kst_today() -> dt.date:
    # GitHub runner 기준 UTC → KST(+9)
    return (dt.datetime.utcnow() + dt.timedelta(hours=9)).date()

def ensure_dir(p: str):
    os.makedirs(p, exist_ok=True)

def clean_name(raw: str) -> str:
    if not raw:
        return raw
    name = raw
    # 公式 제거
    name = re.sub(r"^\s*公式\s*", "", name)
    # 【...】 / [...] / (...) 제거 (내용 포함 통째 제거)
    name = re.sub(r"【.*?】", "", name)
    name = re.sub(r"\[.*?\]", "", name)
    name = re.sub(r"\(.*?\)", "", name)
    # 공백 정리
    name = re.sub(r"\s+", " ", name).strip()
    return name

def extract_price(text: str) -> Optional[int]:
    if not text:
        return None
    # ¥1,234 또는 1,234円 패턴 우선
    m = re.search(r"[¥￥]\s?([0-9,]+)", text)
    if not m:
        m = re.search(r"([0-9,]+)\s*円", text)
    if not m:
        return None
    return int(m.group(1).replace(",", ""))

def extract_code_from_url(url: str) -> Optional[str]:
    if not url:
        return None
    # /item/<digits>
    m = re.search(r"/item/(\d+)", url)
    if m:
        return m.group(1)
    # ?no=123 등 예비
    m = re.search(r"[?&](?:no|goods_code|goodscode|gdno)=(\d+)", url, re.I)
    if m:
        return m.group(1)
    # 마지막 숫자 토큰
    m = re.search(r"(\d+)(?:$|\?)", url)
    return m.group(1) if m else None

def drive_client():
    if not (GOOGLE_CLIENT_ID and GOOGLE_CLIENT_SECRET and GOOGLE_REFRESH_TOKEN):
        return None
    creds = Credentials(
        None,
        refresh_token=GOOGLE_REFRESH_TOKEN,
        token_uri="https://oauth2.googleapis.com/token",
        client_id=GOOGLE_CLIENT_ID,
        client_secret=GOOGLE_CLIENT_SECRET,
        scopes=["https://www.googleapis.com/auth/drive.file", "https://www.googleapis.com/auth/drive.readonly"]
    )
    return build("drive", "v3", credentials=creds)

def drive_find_prev_csv(service, prefix: str, today_name: str) -> Optional[Tuple[str, str]]:
    # 오늘 파일 제외하고, prefix로 시작하는 최신 CSV 하나
    q = f"'{GDRIVE_FOLDER_ID}' in parents and name contains '{prefix}' and mimeType='text/csv'"
    files = service.files().list(q=q, orderBy="createdTime desc",
                                 fields="files(id,name,createdTime)").execute().get("files", [])
    for f in files:
        if f["name"] != today_name:
            return f["id"], f["name"]
    return None

def drive_download_csv(service, file_id: str) -> List[Dict]:
    request = service.files().get_media(fileId=file_id)
    fh = io.BytesIO()
    downloader = MediaIoBaseDownload(fh, request)
    done = False
    while not done:
        status, done = downloader.next_chunk()
    fh.seek(0)
    text = fh.read().decode("utf-8", errors="ignore")
    rows = []
    reader = csv.DictReader(io.StringIO(text))
    for r in reader:
        rows.append(r)
    return rows

def drive_upload_csv(service, path: str, filename: str) -> Optional[str]:
    meta = {"name": filename, "parents": [GDRIVE_FOLDER_ID]}
    media = MediaFileUpload(path, mimetype="text/csv", resumable=True)
    try:
        f = service.files().create(body=meta, media_body=media, fields="id").execute()
        return f.get("id")
    except Exception as e:
        print("[Drive] 업로드 실패:", e)
        return None

# =========================
# Scraping (Playwright)
# =========================
def fetch_qoo10() -> List[Dict]:
    """
    모바일 베스트셀러 페이지(뷰티: group_code=2)에서 200위까지 파싱
    반환: [{rank, name, price, url, code}]
    """
    items: List[Dict] = []
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True, args=["--disable-dev-shm-usage"])
        ua = "Mozilla/5.0 (iPhone; CPU iPhone OS 15_2 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/15.2 Mobile/15E148 Safari/604.1"
        context = browser.new_context(
            user_agent=ua,
            locale="ja-JP",
            viewport={"width": 390, "height": 844},
        )
        page = context.new_page()
        print("수집 시작:", QOO10_URL)
        page.goto(QOO10_URL, wait_until="domcontentloaded", timeout=60_000)

        # 간헐적 쿠키/팝업
        for sel in ["button#onetrust-accept-btn-handler", "button[aria-label='close']", "button:has-text('同意')"]:
            try:
                if page.locator(sel).first.is_visible():
                    page.locator(sel).first.click()
                    time.sleep(0.5)
            except:
                pass

        # 컨테이너 등장 대기
        # 모바일 랭킹 목록에서 상품 a[href*="/item/"] 이 충분히 보일 때까지
        page.wait_for_timeout(1000)
        for _ in range(40):  # 약간 내려주며 로딩 안정화
            page.evaluate("window.scrollBy(0, 1000)")
            page.wait_for_timeout(150)
        # 최종 파싱
        html = page.content()
        browser.close()

    soup = BeautifulSoup(html, "lxml")

    # a[href*="/item/"] 가 상품 상세로 연결됨
    anchors = soup.select('a[href*="/item/"]')
    # 중복 제거 (동일 카드 내 중복 앵커 제거 위한 href uniq)
    seen = set()
    cards = []
    for a in anchors:
        href = a.get("href", "")
        if "/item/" not in href:
            continue
        if href in seen:
            continue
        # 주변에 가격/이름이 같이 있는지 확인 위해 카드(div/li) 상위로
        seen.add(href)
        cards.append(a)

    def nearest_text(el, selectors: List[str]) -> str:
        # a 태그 주변/부모에서 텍스트 탐색 (가격/상품명)
        # 우선 부모 3단계까지 검색
        cur = el
        txt = ""
        for up in range(0, 3):
            parent = cur.parent if cur else None
            if not parent:
                break
            for sel in selectors:
                cand = parent.select_one(sel)
                if cand and cand.get_text(strip=True):
                    return cand.get_text(" ", strip=True)
            cur = parent
        # 못 찾으면 엘리먼트 자체 텍스트
        return el.get_text(" ", strip=True)

    # 이름/가격 셀렉터 후보 (모바일에서 자주 보임)
    name_selectors = [
        ".sbj", ".tit", ".prd_tit", ".name", "p", "div.title", "span.title"
    ]
    price_selectors = [
        ".prc", ".price", ".org", ".dq_price", ".won", "em", "strong", "b", "span"
    ]

    rank = 1
    for a in cards:
        url = a.get("href", "")
        code = extract_code_from_url(url)
        raw_name = nearest_text(a, name_selectors)
        name = clean_name(raw_name)
        raw_price = nearest_text(a, price_selectors)
        price = extract_price(raw_price)

        if not name or not price or not code:
            continue

        items.append({
            "rank": rank,
            "name": name,
            "price": price,
            "url": url,
            "code": code
        })
        rank += 1
        if rank > TOP_LIMIT_SAVE:
            break

    return items

# =========================
# Compare
# =========================
def compare(today: List[Dict], prev: List[Dict]):
    # code 기준으로 비교
    prev_map = {r.get("code"): r for r in prev if r.get("code")}
    today_map = {r.get("code"): r for r in today if r.get("code")}

    rising = []
    falling = []
    newcomers = []
    outs = []

    # 급상승/급하락: 양쪽 모두에 존재하는 대상 중 상위 TOP_LIMIT_COMPARE 내에서만 비교
    for t in today:
        c = t["code"]
        if c in prev_map:
            pr = int(prev_map[c].get("rank", 9999))
            tr = int(t["rank"])
            if pr <= TOP_LIMIT_COMPARE and tr <= TOP_LIMIT_COMPARE:
                diff = pr - tr  # +면 상승
                if diff > 0:
                    rising.append({
                        "name": t["name"], "curr": tr, "prev": pr, "diff": diff
                    })
                elif diff < 0:
                    falling.append({
                        "name": t["name"], "curr": tr, "prev": pr, "diff": -diff
                    })
        else:
            # 뉴랭커: 전일 Top30 밖(또는 미등장) → 당일 Top30 진입
            if int(t["rank"]) <= TOP_LIMIT_COMPARE:
                newcomers.append({
                    "name": t["name"], "curr": int(t["rank"])
                })

    # 아웃: 전일 Top30인데 오늘 Top30 밖
    for p in prev:
        c = p.get("code")
        if not c:
            continue
        pr = int(p.get("rank", 9999))
        if pr <= TOP_LIMIT_COMPARE and c not in today_map:
            outs.append({"name": p["name"], "prev": pr})

    # 정렬 및 제한
    rising.sort(key=lambda x: (-x["diff"], x["curr"], x["prev"], x["name"]))
    falling.sort(key=lambda x: (-x["diff"], x["prev"], x["curr"], x["name"]))
    newcomers.sort(key=lambda x: (x["curr"], x["name"]))

    rising = rising[:3]
    newcomers = newcomers[:3]
    falling = falling[:5]

    inout_cnt = len(newcomers) + len(outs)
    return rising, newcomers, falling, inout_cnt

# =========================
# Slack
# =========================
def post_slack(payload: Dict):
    if not SLACK_WEBHOOK_URL:
        print("[Slack] WEBHOOK 미설정, 출력만")
        print(json.dumps(payload, ensure_ascii=False, indent=2))
        return
    import requests
    r = requests.post(SLACK_WEBHOOK_URL, json=payload, timeout=20)
    print("[Slack] status:", r.status_code)

def slack_blocks(title: str, today: List[Dict], rising, newcomers, falling, inout_cnt) -> Dict:
    def top10_lines():
        lines = []
        for i, r in enumerate(today[:10], 1):
            price = f"¥{format(r['price'], ',')}"
            # Slack 링크: <url|text>
            line = f"{i}. <{r['url']}|{r['name']}> — {price}"
            lines.append(line)
        return "\n".join(lines) if lines else "- 해당 없음"

    def section_lines(lst, kind):
        rows = []
        if kind == "rising":
            for x in lst:
                rows.append(f"- {x['name']} {x['prev']}위 → {x['curr']}위 (↑{x['diff']})")
        elif kind == "new":
            for x in lst:
                rows.append(f"- {x['name']} NEW → {x['curr']}위")
        elif kind == "fall":
            for x in lst:
                rows.append(f"- {x['name']} {x['prev']}위 → {x['curr']}위 (↓{x['diff']})")
        return "\n".join(rows) if rows else "- 해당 없음"

    blocks = [
        {"type": "section", "text": {"type": "mrkdwn", "text": f"*{title}*"}},
        {"type": "divider"},
        {"type": "section", "text": {"type": "mrkdwn", "text": "*TOP 10*"}},
        {"type": "section", "text": {"type": "mrkdwn", "text": top10_lines()}},
        {"type": "section", "text": {"type": "mrkdwn", "text": "*🔥 급상승*"}},
        {"type": "section", "text": {"type": "mrkdwn", "text": section_lines(rising, "rising")}},
        {"type": "section", "text": {"type": "mrkdwn", "text": "*🆕 뉴랭커*"}},
        {"type": "section", "text": {"type": "mrkdwn", "text": section_lines(newcomers, "new")}},
        {"type": "section", "text": {"type": "mrkdwn", "text": "*📉 급하락*"}},
        {"type": "section", "text": {"type": "mrkdwn", "text": section_lines(falling, "fall")}},
        {"type": "section", "text": {"type": "mrkdwn", "text": "*📦 랭크 인&아웃*"}},
        {"type": "section", "text": {"type": "mrkdwn", "text": f"{inout_cnt}개의 제품이 인&아웃 되었습니다."}},
    ]
    return {"blocks": blocks}

# =========================
# Main
# =========================
def main():
    ensure_dir(DATA_DIR)
    today = kst_today()
    ymd = today.strftime("%Y-%m-%d")
    title = f"큐텐 재팬 뷰티 랭킹 — {ymd}"

    # 1) 수집
    products = fetch_qoo10()
    if not products:
        raise RuntimeError("Qoo10 수집 결과 0건. 셀렉터/렌더링 점검 필요")

    # 순위 보정(1..N)
    for i, r in enumerate(products, 1):
        r["rank"] = i

    # CSV 저장 (data/)
    csv_name = f"큐텐재팬_뷰티_랭킹_{ymd}.csv"
    csv_path = os.path.join(DATA_DIR, csv_name)
    with open(csv_path, "w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["date", "rank", "name", "price", "url", "code"])
        for r in products:
            writer.writerow([ymd, r["rank"], r["name"], r["price"], r["url"], r["code"]])

    print("[저장] CSV:", csv_path)

    # 2) Drive 업로드 + 전일 파일 가져와 비교
    rising = newcomers = falling = []
    inout_cnt = 0

    service = drive_client()
    if service and GDRIVE_FOLDER_ID:
        file_id = drive_upload_csv(service, csv_path, csv_name)
        if file_id:
            print("[Drive] 업로드 완료:", csv_name)
        else:
            print("[Drive] 업로드 실패")

        # 전일 파일(최신 이전본) 검색
        prev = drive_find_prev_csv(service, "큐텐재팬_뷰티_랭킹_", csv_name)
        prev_rows = []
        if prev:
            pid, pname = prev
            try:
                prev_rows = drive_download_csv(service, pid)
            except Exception as e:
                print("[Drive] 이전 CSV 다운로드 실패:", e)

        prev_items = []
        for r in prev_rows:
            try:
                prev_items.append({
                    "rank": int(r.get("rank", "9999")),
                    "name": r.get("name", ""),
                    "price": int(r.get("price", "0") or 0),
                    "url": r.get("url", ""),
                    "code": r.get("code", ""),
                })
            except:
                pass

        rising, newcomers, falling, inout_cnt = compare(products, prev_items)

    # 3) Slack 메시지 전송
    payload = slack_blocks(title, products, rising, newcomers, falling, inout_cnt)
    post_slack(payload)

if __name__ == "__main__":
    main()
