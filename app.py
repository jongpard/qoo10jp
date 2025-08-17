# app.py
import os
import re
import csv
import sys
import json
import time
import math
import shutil
import random
import logging
import pathlib
import datetime as dt
from typing import List, Dict, Optional, Tuple

import requests
from bs4 import BeautifulSoup

# 번역(옵션) - googletrans (무료). 실패해도 전체 로직에는 영향 없도록 방어.
try:
    from googletrans import Translator  # pip install googletrans==4.0.0-rc1
except Exception:  # pragma: no cover
    Translator = None  # type: ignore

# 구글 드라이브 업/다운
from googleapiclient.discovery import build  # pip install google-api-python-client google-auth-httplib2 google-auth-oauthlib
from googleapiclient.http import MediaFileUpload
from google.oauth2.credentials import Credentials

# Playwright 폴백
from playwright.sync_api import sync_playwright

# ---------------------------- 설정 ----------------------------
QOO10_URL = "https://www.qoo10.jp/gmkt.inc/Mobile/Bestsellers/Default.aspx?group_code=2"
OUT_DIR = pathlib.Path("data")
OUT_DIR.mkdir(parents=True, exist_ok=True)

KST = dt.timezone(dt.timedelta(hours=9))
TODAY = dt.datetime.now(KST).date()
DATE_STR = TODAY.isoformat()

CSV_NAME = f"큐텐재팬_뷰티_랭킹_{DATE_STR}.csv"
CSV_PATH = OUT_DIR / CSV_NAME

# 슬랙 출력 개수 제한
TOP_N = 10
RISING_LIMIT = 5
NEWCOMER_LIMIT = 5
FALLING_LIMIT = 5

# 번역 줄 노출(길이 절약을 위해 기본 False)
SHOW_TRANSLATION = os.getenv("SLACK_TRANSLATE_JA2KO", "").strip() == "1"

# 로깅
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s | %(message)s",
)
log = logging.getLogger("qoo10jp")

# --------------------- 유틸 ---------------------
_jp_bracket = re.compile(r"[【\[].*?[】\]]|\(.*?\)|（.*?）")
_num_clean = re.compile(r"[^\d]+")

def parse_int(text: str) -> Optional[int]:
    if not text:
        return None
    m = _num_clean.sub("", str(text))
    if not m:
        return None
    try:
        return int(m)
    except Exception:
        return None

def parse_price(text: str) -> Optional[int]:
    """
    "¥1,110" -> 1110
    """
    if not text:
        return None
    return parse_int(text)

def extract_code_from_url(url: str) -> Optional[str]:
    # https://www.qoo10.jp/item/.../<code>?....
    m = re.search(r"/(\d+)(?:\?|$)", url)
    return m.group(1) if m else None

def clean_name(name: str) -> str:
    s = name or ""
    s = s.replace("公式", "")
    s = _jp_bracket.sub("", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s

def now_kst_str():
    return dt.datetime.now(KST).strftime("%Y-%m-%d %H:%M:%S")

# --------------------- 번역(옵션) ---------------------
def ja_to_ko_batch(texts: List[str]) -> List[str]:
    if not SHOW_TRANSLATION:
        return ["" for _ in texts]
    if not texts:
        return []
    if Translator is None:
        return ["" for _ in texts]
    try:
        tr = Translator()
        out = []
        for t in texts:
            if not t:
                out.append("")
                continue
            try:
                r = tr.translate(t, src="ja", dest="ko")
                out.append(r.text)
            except Exception:
                out.append("")
        return out
    except Exception:
        return ["" for _ in texts]

# --------------------- HTTP 수집 ---------------------
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) "
                  "Chrome/119.0.0.0 Safari/537.36",
    "Accept-Language": "ja-JP,ja;q=0.9,ko-KR;q=0.8,ko;q=0.7,en;q=0.6",
}

def fetch_http() -> str:
    log.info("HTTP 수집 시도: %s", QOO10_URL)
    r = requests.get(QOO10_URL, headers=HEADERS, timeout=20)
    r.raise_for_status()
    html = r.text
    if "Bestsellers" not in html and "Best Sellers" not in html:
        raise RuntimeError("HTTP 응답이 비정상으로 보임(키워드 미검출).")
    return html

# --------------------- Playwright 폴백 ---------------------
def fetch_playwright() -> str:
    log.info("[Playwright] 폴백 진입")
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        ctx = browser.new_context(locale="ja-JP")
        page = ctx.new_page()
        page.goto(QOO10_URL, wait_until="domcontentloaded", timeout=30_000)
        # 모바일 페이지는 랭킹 섹션이 바로 노출됨. 가벼운 대기 + 스크롤로 더 로드.
        page.wait_for_timeout(1500)
        # 200위까지 표시되도록 충분히 스크롤
        for _ in range(10):
            page.mouse.wheel(0, 2000)
            page.wait_for_timeout(600)
        content = page.content()
        ctx.close()
        browser.close()
        return content

# --------------------- 파서 ---------------------
def parse_qoo10(html: str) -> List[Dict]:
    """
    매우 다양한 DOM 변형을 고려하여 최대한 보수적으로 파싱.
    - 랭킹 숫자, 상품명, 가격, 링크
    """
    soup = BeautifulSoup(html, "html.parser")

    # 상품 카드 후보들
    candidates = []
    # 일반적으로 li 요소 내 a[href*="/item/"] 존재
    for a in soup.select('a[href*="/item/"]'):
        card = a.find_parent(["li", "div"])
        if card and card not in candidates:
            candidates.append(card)

    items: List[Dict] = []
    seen_urls = set()

    rank_guess = 0
    for c in candidates:
        a = c.select_one('a[href*="/item/"]')
        if not a:
            continue
        url = a.get("href") or ""
        if not url or url in seen_urls:
            continue
        seen_urls.add(url)

        # 이름: a 텍스트 또는 카드 내 대표 텍스트
        name = a.get_text(" ", strip=True)
        if not name:
            name = c.get_text(" ", strip=True)

        # 가격: "¥1,110" 형태 찾기
        price_node = None
        # 가격 후보 텍스트
        for cand in c.find_all(text=True):
            t = str(cand).strip()
            if t.startswith("¥") and any(ch.isdigit() for ch in t):
                price_node = t
                break
        price = parse_price(price_node) if price_node else None

        # 카드 내 랭킹 숫자 추정 (없으면 누적 카운터)
        rank = None
        # 숫자만 있는 작은 배지/스팬 탐색
        for s in c.select("em, i, b, strong, span"):
            t = (s.get_text() or "").strip()
            if t.isdigit():
                vi = int(t)
                if 1 <= vi <= 500:
                    rank = vi
                    break
        if rank is None:
            rank_guess += 1
            rank = rank_guess

        code = extract_code_from_url(url)

        items.append({
            "rank": rank,
            "name": name,
            "price": price,
            "url": url,
            "code": code,
        })

    # 랭킹으로 정렬 + 중복 제거
    items.sort(key=lambda x: x["rank"])
    uniq = []
    used_rank = set()
    for it in items:
        r = it["rank"]
        if r in used_rank:
            continue
        used_rank.add(r)
        uniq.append(it)

    # 상위 200까지만 사용(길이 과도 방지)
    return uniq[:200]

# --------------------- 전/당일 비교 ---------------------
def read_prev_from_drive() -> Optional[pathlib.Path]:
    """
    구글드라이브 폴더에서 오늘 이전 날짜의 '큐텐재팬_뷰티_랭킹_YYYY-MM-DD.csv' 중 가장 최근 것을 받아온다.
    실패하면 None.
    """
    folder_id = os.getenv("GDRIVE_FOLDER_ID", "").strip()
    if not folder_id:
        log.info("[Drive] 폴더 ID 없음 → 전일 비교 생략")
        return None

    try:
        creds = Credentials(
            None,
            refresh_token=os.getenv("GOOGLE_REFRESH_TOKEN"),
            token_uri="https://oauth2.googleapis.com/token",
            client_id=os.getenv("GOOGLE_CLIENT_ID"),
            client_secret=os.getenv("GOOGLE_CLIENT_SECRET"),
            scopes=["https://www.googleapis.com/auth/drive.readonly"],
        )
        svc = build("drive", "v3", credentials=creds)

        prefix = "큐텐재팬_뷰티_랭킹_"
        q = f"'{folder_id}' in parents and name contains '{prefix}' and mimeType='text/csv' and trashed=false"
        files = svc.files().list(q=q, orderBy="name desc", pageSize=50, fields="files(id,name)").execute().get("files", [])

        target = None
        for f in files:
            # 오늘 파일은 패스
            if DATE_STR in f["name"]:
                continue
            target = f
            break
        if not target:
            log.info("[Drive] 전일 CSV 없음")
            return None

        # 다운로드
        tmp = OUT_DIR / f"_prev_{target['name']}"
        req = svc.files().get_media(fileId=target["id"])
        fh = open(tmp, "wb")
        downloader = requests.Response()
        from googleapiclient.http import MediaIoBaseDownload
        import io
        stream = io.BytesIO()
        downloader = MediaIoBaseDownload(stream, req)
        done = False
        while not done:
            status, done = downloader.next_chunk()
        fh.write(stream.getvalue())
        fh.close()
        log.info("[Drive] 전일 CSV 다운로드: %s", tmp)
        return tmp
    except Exception as e:
        log.warning("[Drive] 전일 CSV 가져오기 실패: %s", e)
        return None

def load_csv(path: pathlib.Path) -> List[Dict]:
    rows: List[Dict] = []
    if not path or not path.exists():
        return rows
    with open(path, "r", encoding="utf-8", newline="") as f:
        rd = csv.DictReader(f)
        for r in rd:
            try:
                rows.append({
                    "date": r.get("date"),
                    "rank": int(r.get("rank", "0")),
                    "name": r.get("name", ""),
                    "price": int(r.get("price") or 0),
                    "url": r.get("url", ""),
                    "code": r.get("code", ""),
                })
            except Exception:
                continue
    return rows

def save_csv(path: pathlib.Path, items: List[Dict]):
    with open(path, "w", encoding="utf-8", newline="") as f:
        wr = csv.writer(f)
        wr.writerow(["date", "rank", "name", "price", "url", "code"])
        for it in items:
            wr.writerow([DATE_STR, it["rank"], it["name"], it.get("price") or "", it["url"], it.get("code") or ""])

def analyze(prev: List[Dict], curr: List[Dict]) -> Tuple[List[Tuple[Dict,int]], List[Dict], List[Tuple[Dict,int]], int]:
    """
    return: (rising, newcomers, falling, inout_count)
    - rising: [(item, +delta)]
    - newcomers: [item]
    - falling: [(item, -delta)]
    """
    prev_map = {}
    for r in prev:
        prev_map[r.get("code") or r["url"]] = r

    rising: List[Tuple[Dict,int]] = []
    newcomers: List[Dict] = []
    falling: List[Tuple[Dict,int]] = []
    inout = 0

    # 현행 top30 대상
    curr_top30 = [x for x in curr if x["rank"] <= 30]
    prev_top30_map = { (r.get("code") or r["url"]): r for r in prev if r["rank"] <= 30 }

    # newcomers / rising / falling
    for it in curr:
        key = it.get("code") or it["url"]
        pv = prev_map.get(key)
        if pv:
            delta = pv["rank"] - it["rank"]
            if delta > 0:
                rising.append((it, delta))
            elif delta < 0:
                falling.append((it, -delta))
        else:
            # 이전에 없었고 현재 top30에 들면 뉴랭커
            if it["rank"] <= 30:
                newcomers.append(it)

    # 랭크 아웃 카운트(전일 top30인데 오늘 >30 또는 미등장)
    for k, pv in prev_top30_map.items():
        cur = next((x for x in curr if (x.get("code") or x["url"]) == k), None)
        if (cur is None) or (cur["rank"] > 30):
            inout += 1

    # 정렬
    rising.sort(key=lambda x: (-x[1], x[0]["rank"], prev_map[(x[0].get("code") or x[0]["url"])]["rank"], clean_name(x[0]["name"])))
    newcomers.sort(key=lambda x: x["rank"])
    falling.sort(key=lambda x: (-x[1], x[0]["rank"], clean_name(x[0]["name"])))

    return rising, newcomers, falling, inout

# --------------------- Slack 메시지 ---------------------
def _fmt_price_yen(v: Optional[int]) -> str:
    return f"¥{v:,}" if isinstance(v, int) and v > 0 else "¥-"

def _line_linked(rank: int, name: str, price: Optional[int], url: str) -> str:
    txt = f"{rank}. {clean_name(name)} — {_fmt_price_yen(price)}"
    return f"<{url}|{txt}>"

def build_slack_message(
    date_str: str,
    top_items: List[Dict],
    rising: List[Tuple[Dict,int]],
    newcomers: List[Dict],
    falling: List[Tuple[Dict,int]],
    inout_count: int,
) -> str:
    lines: List[str] = []
    lines.append(f"*큐텐 재팬 뷰티 랭킹 — {date_str}*")
    lines.append("")

    # TOP 10
    lines.append("*TOP 10*")
    for it in top_items[:TOP_N]:
        lines.append(_line_linked(it["rank"], it["name"], it.get("price"), it["url"]))
        if SHOW_TRANSLATION and it.get("name_ko"):
            if it["name_ko"]:
                lines.append(it["name_ko"])

    # 급상승
    lines.append("")
    lines.append("🔥 *급상승*")
    if rising:
        for it, delta in rising[:RISING_LIMIT]:
            lines.append(f"- {clean_name(it['name'])} → *{it['rank']}위* (▲{delta})")
    else:
        lines.append("- 해당 없음")

    # 뉴랭커
    lines.append("")
    lines.append("🆕 *뉴랭커*")
    if newcomers:
        for it in newcomers[:NEWCOMER_LIMIT]:
            lines.append(f"- {clean_name(it['name'])} NEW → *{it['rank']}위*")
    else:
        lines.append("- 해당 없음")

    # 급하락
    lines.append("")
    lines.append("📉 *급하락*")
    if falling:
        for it, delta in falling[:FALLING_LIMIT]:
            lines.append(f"- {clean_name(it['name'])} → *{it['rank']}위* (▼{delta})")
    else:
        lines.append("- 해당 없음")

    # 랭크 인&아웃 개수 문장
    lines.append("")
    lines.append(f"📎 *링크 인&아웃*\n{inout_count}개의 제품이 인&아웃 되었습니다.")

    return "\n".join(lines)

# --------------------- Slack 전송 ---------------------
def send_slack(text: str):
    url = os.getenv("SLACK_WEBHOOK_URL", "").strip()
    if not url:
        log.warning("SLACK_WEBHOOK_URL 미설정 → 슬랙 전송 생략")
        return
    try:
        resp = requests.post(url, json={"text": text}, timeout=10)
        if resp.status_code >= 400:
            log.warning("Slack 전송 실패: %s %s", resp.status_code, resp.text[:200])
        else:
            log.info("Slack 전송 완료")
    except Exception as e:
        log.warning("Slack 전송 중 예외: %s", e)

# --------------------- Drive 업로드 ---------------------
def drive_upload_csv(path: pathlib.Path) -> Optional[str]:
    folder_id = os.getenv("GDRIVE_FOLDER_ID", "").strip()
    if not folder_id or not path.exists():
        return None
    try:
        creds = Credentials(
            None,
            refresh_token=os.getenv("GOOGLE_REFRESH_TOKEN"),
            token_uri="https://oauth2.googleapis.com/token",
            client_id=os.getenv("GOOGLE_CLIENT_ID"),
            client_secret=os.getenv("GOOGLE_CLIENT_SECRET"),
            scopes=["https://www.googleapis.com/auth/drive.file"],
        )
        svc = build("drive", "v3", credentials=creds)
        media = MediaFileUpload(str(path), mimetype="text/csv", resumable=True)
        body = {"name": path.name, "parents": [folder_id]}
        f = svc.files().create(body=body, media_body=media, fields="id").execute()
        file_id = f.get("id")
        log.info("Google Drive 업로드 완료: %s", file_id)
        return file_id
    except Exception as e:
        log.warning("Google Drive 업로드 실패: %s", e)
        return None

# --------------------- 메인 ---------------------
def fetch_products() -> List[Dict]:
    # 1) HTTP
    try:
        html = fetch_http()
    except Exception as e:
        log.warning("HTTP 실패 → Playwright 폴백: %s", e)
        html = fetch_playwright()

    items = parse_qoo10(html)
    if not items:
        raise RuntimeError("Qoo10 수집 결과 0건. 셀렉터/렌더링 점검 필요")

    # 번역 필드(옵션)
    if SHOW_TRANSLATION:
        names = [clean_name(x["name"]) for x in items[:TOP_N]]
        kos = ja_to_ko_batch(names)
        for i, it in enumerate(items[:TOP_N]):
            it["name_ko"] = kos[i] if i < len(kos) else ""
    else:
        for it in items[:TOP_N]:
            it["name_ko"] = ""

    return items

def main():
    log.info("수집 시작: %s", QOO10_URL)
    items = fetch_products()
    log.info("수집 완료: %d", len(items))

    # 저장
    save_csv(CSV_PATH, items)
    drive_upload_csv(CSV_PATH)

    # 비교용 전일 CSV
    prev_local = read_prev_from_drive()
    prev = load_csv(prev_local) if prev_local else []

    rising, newcomers, falling, inout = analyze(prev, items)

    # 슬랙 메시지
    top_items = items[:TOP_N]
    msg = build_slack_message(DATE_STR, top_items, rising, newcomers, falling, inout)
    send_slack(msg)

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        log.exception("실행 실패: %s", e)
        sys.exit(1)
