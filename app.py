# app.py
# Qoo10 Japan (뷰티) 베스트셀러 수집 → CSV 저장 → 전일 대비 변화 집계 → Slack 전송 → Google Drive 업로드
# - /item/<숫자> 링크만 수집(카테고리/탭/프로모/자바스크립트 링크 필터링)
# - 슬랙 메시지: TOP 10 (하이퍼링크), 🔥 급상승/ 🆕 뉴랭커/ 📉 급하락(각 5개 제한), 링크 인&아웃 요약
# - 전일 CSV가 로컬에 없으면, 드라이브에서 최신 전일 파일을 한 번 시도해 다운로드(있으면 비교)

from __future__ import annotations

import os
import re
import io
import csv
import sys
import time
import json
import shutil
import logging
from datetime import date, datetime
from typing import List, Dict, Optional, Tuple

import requests
from playwright.sync_api import sync_playwright

# Google Drive
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload, MediaIoBaseDownload
from google.oauth2.credentials import Credentials

# ---------------------------
# 설정
# ---------------------------
QOO10_BEAUTY_URL = "https://www.qoo10.jp/gmkt.inc/Mobile/Bestsellers/Default.aspx?group_code=2"
DATA_DIR = "data"
DEBUG_DIR = "data/debug"

MAX_RANK = int(os.getenv("QOO10_MAX_RANK", "200"))

SLACK_WEBHOOK_URL = os.getenv("SLACK_WEBHOOK_URL", "")
G_CLIENT_ID = os.getenv("GOOGLE_CLIENT_ID", "")
G_CLIENT_SECRET = os.getenv("GOOGLE_CLIENT_SECRET", "")
G_REFRESH_TOKEN = os.getenv("GOOGLE_REFRESH_TOKEN", "")
GDRIVE_FOLDER_ID = os.getenv("GDRIVE_FOLDER_ID", "")

# Slack 표시 제한
SHOW_TOP_N = 10
MAX_RISERS = 5
MAX_FALLERS = 5
MAX_NEW = 5

# 로깅
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)

# ---------------------------
# 유틸
# ---------------------------
def ensure_dirs():
    os.makedirs(DATA_DIR, exist_ok=True)
    os.makedirs(DEBUG_DIR, exist_ok=True)

def yen(n: Optional[int]) -> str:
    if n is None:
        return "-"
    return f"¥{n:,}"

def percent_str(p: Optional[int]) -> str:
    if p is None:
        return ""
    return f" (↓{p}%)"

def slack_link(url: str, text: str) -> str:
    # Slack 링크 포맷: <url|text>
    safe = text.replace(">", "›").replace("|", "¦")
    return f"<{url}|{safe}>"

def today_str() -> str:
    return str(date.today())

def latest_prev_csv(prefix: str) -> Optional[str]:
    """
    data 폴더에서 prefix로 시작하는 가장 최근(오늘 이전) CSV 찾기
    파일명 예: 큐텐재팬_뷰티_랭킹_2025-08-17.csv
    """
    if not os.path.isdir(DATA_DIR):
        return None
    files = [f for f in os.listdir(DATA_DIR) if f.startswith(prefix) and f.endswith(".csv")]
    if not files:
        return None
    # 날짜 파싱
    cand: List[Tuple[str, datetime]] = []
    for f in files:
        m = re.search(r"(\d{4}-\d{2}-\d{2})", f)
        if not m:
            continue
        d = datetime.strptime(m.group(1), "%Y-%m-%d")
        if d.date() < date.today():
            cand.append((f, d))
    if not cand:
        return None
    cand.sort(key=lambda x: x[1], reverse=True)
    return os.path.join(DATA_DIR, cand[0][0])

def build_drive() -> Optional[any]:
    if not (G_CLIENT_ID and G_CLIENT_SECRET and G_REFRESH_TOKEN):
        return None
    creds = Credentials(
        token=None,
        refresh_token=G_REFRESH_TOKEN,
        token_uri="https://oauth2.googleapis.com/token",
        client_id=G_CLIENT_ID,
        client_secret=G_CLIENT_SECRET,
        scopes=["https://www.googleapis.com/auth/drive.file", "https://www.googleapis.com/auth/drive.readonly"]
    )
    return build("drive", "v3", credentials=creds, cache_discovery=False)

def drive_upload_csv(drive, filepath: str, folder_id: str) -> Optional[str]:
    if drive is None or not os.path.isfile(filepath):
        return None
    fname = os.path.basename(filepath)
    file_metadata = {"name": fname, "parents": [folder_id]}
    media = MediaFileUpload(filepath, mimetype="text/csv", resumable=True)
    file = drive.files().create(body=file_metadata, media_body=media, fields="id").execute()
    return file.get("id")

def drive_download_latest_prev(drive, folder_id: str, prefix: str) -> Optional[str]:
    """
    드라이브 폴더에서 prefix 포함 CSV 중, 오늘 이전 날짜가 포함된 가장 최근 파일을 받아 data/ 에 저장
    """
    if drive is None:
        return None

    query = f"'{folder_id}' in parents and mimeType='text/csv' and name contains '{prefix}'"
    resp = drive.files().list(q=query, orderBy="createdTime desc", pageSize=50, fields="files(id,name)").execute()
    files = resp.get("files", [])
    for f in files:
        m = re.search(r"(\d{4}-\d{2}-\d{2})", f["name"])
        if not m:
            continue
        d = datetime.strptime(m.group(1), "%Y-%m-%d").date()
        if d < date.today():
            # download
            request = drive.files().get_media(fileId=f["id"])
            local = os.path.join(DATA_DIR, f["name"])
            with io.FileIO(local, "wb") as fh:
                downloader = MediaIoBaseDownload(fh, request)
                done = False
                while not done:
                    status, done = downloader.next_chunk()
            return local
    return None

# ---------------------------
# Qoo10 파싱 헬퍼
# ---------------------------
def extract_price_yen(text: str) -> Optional[int]:
    """
    '¥3,200' / '3,200円' 등 엔화 가격 파싱 (첫 번째 항목)
    """
    m = re.search(r'(?:¥\s*|)(\d{1,3}(?:,\d{3})+|\d+)\s*円|¥\s*(\d{1,3}(?:,\d{3})+|\d+)', text)
    if not m:
        return None
    num = m.group(1) or m.group(2)
    return int(num.replace(',', ''))

def extract_discount_percent(text: str) -> Optional[int]:
    m = re.search(r'(\d{1,2}|100)\s*%[Oo][Ff][Ff]|↓\s*(\d{1,2}|100)\s*%', text)
    if not m:
        return None
    v = m.group(1) or m.group(2)
    try:
        return int(v)
    except:
        return None

def clean_brand(s: str) -> str:
    s = s.strip()
    s = re.sub(r'^\s*公式\s*', '', s)
    return s

def is_item_url(href: str) -> bool:
    if not href:
        return False
    if "javascript:" in href:
        return False
    if "/item/" not in href:
        return False
    # 카테고리/딜/스페셜 등은 제외
    if "/Mobile/Category" in href or "/Mobile/Deal" in href or "/Mobile/Special" in href:
        return False
    return True

def extract_product_id(href: str) -> Optional[str]:
    m = re.search(r'/item/(?:[^/]+/)?(\d+)', href)
    return m.group(1) if m else None

# ---------------------------
# 수집 (핵심)
# ---------------------------
def fetch_qoo10_beauty(max_count: int = MAX_RANK) -> List[Dict]:
    items: List[Dict] = []
    seen: set[str] = set()

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        ctx = browser.new_context(user_agent=(
            "Mozilla/5.0 (Linux; Android 12; Pixel 5) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/120.0.0.0 Mobile Safari/537.36"
        ))
        page = ctx.new_page()
        page.goto(QOO10_BEAUTY_URL, wait_until="domcontentloaded", timeout=60000)

        # 상품 앵커 대기
        page.wait_for_selector("a[href*='/item/']", timeout=30000)

        # lazy load 해소를 위해 스크롤
        last_h = 0
        for _ in range(12):
            page.evaluate("window.scrollBy(0, document.body.scrollHeight)")
            time.sleep(0.6)
            h = page.evaluate("document.body.scrollHeight")
            if h == last_h:
                break
            last_h = h

        anchors = page.query_selector_all("a[href]")
        for a in anchors:
            href = (a.get_attribute("href") or "").strip()
            if not is_item_url(href):
                continue
            pid = extract_product_id(href)
            if not pid or pid in seen:
                continue

            li = a.closest("li") or a
            text = (li.inner_text() or "").strip()
            lines = [s.strip() for s in text.splitlines() if s.strip()]
            if not lines:
                continue

            # 이름/브랜드 힌트 추정
            name = lines[0]
            brand = ""
            if len(lines) >= 2 and len(lines[0]) <= 15 and len(lines[1]) >= 6:
                brand = clean_brand(lines[0])
                name = lines[1]

            price = extract_price_yen(text)
            if price is None:
                continue

            disc = extract_discount_percent(text)

            url = href if href.startswith("http") else ("https://www.qoo10.jp" + href)

            items.append({
                "date": today_str(),
                "rank": None,  # 나중에 채움
                "brand": brand,
                "name": name,
                "price": price,
                "orig_price": None,
                "discount_percent": disc,
                "url": url,
                "product_code": pid
            })
            seen.add(pid)
            if len(items) >= max_count:
                break

        ctx.close()
        browser.close()

    # 랭크 채우기
    for i, it in enumerate(items, start=1):
        it["rank"] = i

    return items

# ---------------------------
# CSV 저장/로드
# ---------------------------
def csv_filename_for_today() -> str:
    return os.path.join(DATA_DIR, f"큐텐재팬_뷰티_랭킹_{today_str()}.csv")

CSV_HEADERS = ["date", "rank", "brand", "name", "price", "orig_price", "discount_percent", "url", "product_code"]

def save_csv(path: str, rows: List[Dict]):
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=CSV_HEADERS)
        w.writeheader()
        for r in rows:
            row = {k: r.get(k, "") for k in CSV_HEADERS}
            w.writerow(row)

def load_csv(path: str) -> List[Dict]:
    out: List[Dict] = []
    with open(path, "r", encoding="utf-8") as f:
        for i, row in enumerate(csv.DictReader(f)):
            try:
                out.append({
                    "date": row.get("date", ""),
                    "rank": int(row.get("rank", "0") or 0),
                    "brand": row.get("brand", ""),
                    "name": row.get("name", ""),
                    "price": int(row.get("price", "0") or 0),
                    "orig_price": (int(row["orig_price"]) if row.get("orig_price") else None),
                    "discount_percent": (int(row["discount_percent"]) if row.get("discount_percent") else None),
                    "url": row.get("url", ""),
                    "product_code": row.get("product_code", ""),
                })
            except Exception as e:
                logging.warning("CSV load skip line %d: %s", i+2, e)
    return out

# ---------------------------
# 전일 비교(급상승/뉴랭커/급하락)
# ---------------------------
def compare_previous(curr: List[Dict], prev: List[Dict]) -> Tuple[List[Dict], List[Dict], List[Dict], int]:
    # product_code를 키로 랭크 비교
    prev_by_code = {r["product_code"]: r for r in prev if r.get("product_code")}
    curr_by_code = {r["product_code"]: r for r in curr if r.get("product_code")}

    # 상승폭 = prev_rank - curr_rank (값이 클수록 급상승)
    movers = []
    for c in curr:
        code = c["product_code"]
        p = prev_by_code.get(code)
        if not p:
            continue
        delta = p["rank"] - c["rank"]
        if delta > 0:
            movers.append((delta, c))
    movers.sort(key=lambda x: x[0], reverse=True)
    risers = [m[1] for m in movers[:MAX_RISERS]]

    # 뉴랭커: prev에는 없고 curr에만 있는 항목
    newcomers = [c for c in curr if c["product_code"] not in prev_by_code][:MAX_NEW]

    # 급하락: prev에는 있었고 curr에도 있는데 (curr_rank - prev_rank) 양수 큰 순
    fallers_all = []
    for pcode, p in prev_by_code.items():
        c = curr_by_code.get(pcode)
        if not c:
            continue
        drop = c["rank"] - p["rank"]
        if drop > 0:
            fallers_all.append((drop, c))
    fallers_all.sort(key=lambda x: x[0], reverse=True)
    fallers = [f[1] for f in fallers_all[:MAX_FALLERS]]

    # 인&아웃: out 개수(전일 존재, 금일 없음)
    out_count = sum(1 for pcode in prev_by_code if pcode not in curr_by_code)

    return risers, newcomers, fallers, out_count

# ---------------------------
# Slack 메시지
# ---------------------------
def build_slack_message(curr: List[Dict], risers: List[Dict], newcomers: List[Dict], fallers: List[Dict], out_count: int) -> str:
    title = f"*큐텐 재팬 뷰티 랭킹 — {today_str()}*"
    lines: List[str] = [title, "", "*TOP 10*"]

    for r in curr[:SHOW_TOP_N]:
        name = f"{(r['brand'] + ' ') if r['brand'] else ''}{r['name']}"
        price_part = f"{yen(r['price'])}{percent_str(r['discount_percent'])}"
        line = f"{r['rank']}. {slack_link(r['url'], name)} — {price_part}"
        lines.append(line)

    # 섹션들
    def section(title_emoji: str, rows: List[Dict]):
        lines.append("")
        lines.append(f"{title_emoji}")
        if not rows:
            lines.append("- 해당 없음")
            return
        for r in rows:
            name = f"{(r['brand'] + ' ') if r['brand'] else ''}{r['name']}"
            lines.append(f"- {slack_link(r['url'], name)}")

    section("🔥 급상승", risers)
    section("🆕 뉴랭커", newcomers)
    section("📉 급하락", fallers)

    lines.append("")
    lines.append("🔗 *링크 인&아웃*")
    lines.append(f"{out_count}개의 제품이 인&아웃 되었습니다.")

    return "\n".join(lines)

def post_slack(text: str):
    if not SLACK_WEBHOOK_URL:
        logging.info("[Slack] 웹훅 미설정. 메시지 출력만 합니다.")
        print(text)
        return
    resp = requests.post(SLACK_WEBHOOK_URL, data=json.dumps({"text": text}), headers={"Content-Type": "application/json"})
    if resp.status_code >= 300:
        logging.warning("[Slack] 전송 실패: %s %s", resp.status_code, resp.text)

# ---------------------------
# 메인
# ---------------------------
def main():
    ensure_dirs()
    logging.info("수집 시작: %s", QOO10_BEAUTY_URL)

    # 수집
    items = fetch_qoo10_beauty(MAX_RANK)
    if not items:
        raise RuntimeError("Qoo10 수집 결과 0건. 셀렉터/렌더링 점검 필요")

    logging.info("수집 완료: %d", len(items))

    # CSV 저장
    csv_path = csv_filename_for_today()
    save_csv(csv_path, items)
    logging.info("CSV 저장: %s", csv_path)

    # 전일 CSV 확보(로컬 → 없을 시 드라이브에서 시도)
    prefix = "큐텐재팬_뷰티_랭킹_"
    prev_path = latest_prev_csv(prefix)

    drive = None
    if not prev_path and GDRIVE_FOLDER_ID:
        drive = build_drive()
        try:
            prev_path = drive_download_latest_prev(drive, GDRIVE_FOLDER_ID, prefix)
            if prev_path:
                logging.info("드라이브에서 전일 CSV 다운로드: %s", prev_path)
        except Exception as e:
            logging.warning("드라이브 전일 CSV 다운로드 실패: %s", e)

    # 비교
    risers: List[Dict] = []
    newcomers: List[Dict] = []
    fallers: List[Dict] = []
    out_count = 0

    if prev_path and os.path.isfile(prev_path):
        prev_rows = load_csv(prev_path)
        risers, newcomers, fallers, out_count = compare_previous(items, prev_rows)
    else:
        logging.info("전일 CSV 없음 → 변화 섹션은 비움")

    # Slack 메시지
    msg = build_slack_message(items, risers, newcomers, fallers, out_count)
    post_slack(msg)
    logging.info("Slack 전송 완료")

    # 드라이브 업로드
    if drive is None and GDRIVE_FOLDER_ID:
        drive = build_drive()
    if drive and GDRIVE_FOLDER_ID:
        try:
            file_id = drive_upload_csv(drive, csv_path, GDRIVE_FOLDER_ID)
            if file_id:
                logging.info("Google Drive 업로드 완료: %s", file_id)
        except Exception as e:
            logging.warning("Google Drive 업로드 실패: %s", e)

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        logging.exception("실행 실패: %s", e)
        sys.exit(1)
