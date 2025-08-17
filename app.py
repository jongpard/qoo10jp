# app.py
# -*- coding: utf-8 -*-
"""
Qoo10 Japan Beauty Ranking Scraper
- 수집: Qoo10 모바일 랭킹(뷰티 그룹) 기준 최대 200개
- 파일명: 큐텐재팬_뷰티_랭킹_YYYY-MM-DD.csv (KST)
- Slack 포맷: TOP10 → 급상승(상위 3) → 뉴랭커(상위 3) → 급하락(상위 5) → 랭크 인&아웃(개수만)
- 비교 기준: 전일 CSV (Drive 폴더 내 prefix 매칭, 가장 최근 날짜)
- 할인율: 소수점 없이 버림, 괄호 표기 (↓27%)
- 제품코드: URL 끝의 숫자 id
"""

from __future__ import annotations

import os
import re
import csv
import time
import json
import math
import traceback
import datetime as dt
from pathlib import Path
from typing import List, Dict, Tuple, Optional

import requests
from bs4 import BeautifulSoup

# Playwright (동적 렌더링 폴백)
from playwright.sync_api import sync_playwright

# Slack
import urllib.request
import urllib.error
import urllib.parse

# Google Drive (OAuth)
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
from google.oauth2.credentials import Credentials


# =========================
# 기본 설정 / 경로 / 시간대
# =========================
KST = dt.timezone(dt.timedelta(hours=9))
TODAY = dt.datetime.now(KST).date()

BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data"
DATA_DIR.mkdir(parents=True, exist_ok=True)

# Qoo10 모바일 뷰티 랭킹 URL
QOO10_URL = "https://www.qoo10.jp/gmkt.inc/Mobile/Bestsellers/Default.aspx?group_code=2"

# Slack 번역 옵션 (일본어만 → 한국어)
ENABLE_JA2KO = os.getenv("SLACK_TRANSLATE_JA2KO", "").strip() in ("1", "true", "TRUE")


def log(msg: str):
    print(msg, flush=True)


# =========================
# 유틸: 텍스트/숫자 정규화
# =========================
JP_BRACKET_PATTERNS = [
    r"【.*?】", r"［.*?］", r"〔.*?〕", r"〈.*?〉", r"《.*?》", r"「.*?」", r"『.*?』", r"\(.*?\)", r"（.*?）"
]

def normalize_text(s: str) -> str:
    if not s:
        return ""
    s = s.replace("\u3000", " ")  # 전각 공백
    # "公式" 제거
    s = re.sub(r"^\s*公式\s*", "", s).strip()
    # 각종 괄호 블록 제거
    for pat in JP_BRACKET_PATTERNS:
        s = re.sub(pat, " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s

def parse_price(text: str) -> Optional[int]:
    if not text:
        return None
    m = re.findall(r"[\d,]+", text.replace(",", ""))
    if not m:
        return None
    try:
        return int(m[-1])
    except:
        return None

def extract_pid(url: str) -> str:
    # .../item/.../<PID> (일반/모바일 모두 숫자 id가 끝에 존재)
    if not url:
        return ""
    m = re.search(r"/(\d+)(?:\?|$)", url)
    return m.group(1) if m else ""

def to_percent_floor(off: float) -> int:
    try:
        if off < 0:
            off = 0
        return math.floor(off)
    except:
        return 0

def has_japanese(text: str) -> bool:
    if not text:
        return False
    # 히라가나, 가타카나, 일부 한자 범위
    return re.search(r"[\u3040-\u30FF\u4E00-\u9FFF]", text) is not None


# =========================
# Slack
# =========================
def slack_post(text: str):
    url = os.environ.get("SLACK_WEBHOOK_URL", "")
    if not url:
        log("[Slack] SLACK_WEBHOOK_URL 미설정")
        return
    payload = json.dumps({"text": text}).encode("utf-8")
    req = urllib.request.Request(url, data=payload, headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            log(f"[Slack] 전송 완료: {resp.status}")
    except Exception as e:
        log(f"[Slack] 전송 실패: {e}")


# =========================
# Translator (선택)
# =========================
def translate_ja_to_ko_batch(lines: List[str]) -> List[str]:
    if not ENABLE_JA2KO:
        return ["" for _ in lines]
    try:
        from googletrans import Translator  # 가벼운 번역기(신뢰성은 낮으나 무료)
        tr = Translator()
        outs = []
        for s in lines:
            if not s or not has_japanese(s):
                outs.append("")
                continue
            try:
                res = tr.translate(s, src="ja", dest="ko")
                outs.append(res.text)
            except Exception:
                outs.append("")
        return outs
    except Exception as e:
        log(f"[Translate] 사용 안함/오류: {e}")
        return ["" for _ in lines]


# =========================
# Google Drive
# =========================
def _drive_service():
    creds = Credentials(
        None,
        refresh_token=os.getenv("GOOGLE_REFRESH_TOKEN"),
        token_uri="https://oauth2.googleapis.com/token",
        client_id=os.getenv("GOOGLE_CLIENT_ID"),
        client_secret=os.getenv("GOOGLE_CLIENT_SECRET"),
        scopes=["https://www.googleapis.com/auth/drive.file"],
    )
    return build("drive", "v3", credentials=creds, cache_discovery=False)

def drive_upload_file(path: Path) -> Optional[str]:
    try:
        service = _drive_service()
        folder_id = os.getenv("GDRIVE_FOLDER_ID", "").strip()
        media = MediaFileUpload(str(path), mimetype="text/csv", resumable=True)
        file_metadata = {"name": path.name}
        if folder_id:
            file_metadata["parents"] = [folder_id]
        file = service.files().create(body=file_metadata, media_body=media, fields="id").execute()
        fid = file.get("id")
        log(f"[Drive] 업로드 완료: {fid}")
        return fid
    except Exception as e:
        log(f"[Drive] 업로드 실패 (무시): {e}")
        return None

def drive_find_latest_prev(prefix: str) -> Optional[Path]:
    """Drive 안에서 prefix로 시작하고, 오늘 날짜 이전 파일 중 가장 최근 파일을 다운로드."""
    try:
        service = _drive_service()
        folder_id = os.getenv("GDRIVE_FOLDER_ID", "").strip()
        q = f"name contains '{prefix}'"
        if folder_id:
            q = f"({q}) and '{folder_id}' in parents"

        results = service.files().list(
            q=q,
            fields="files(id, name, modifiedTime)",
            orderBy="modifiedTime desc",
            pageSize=50
        ).execute()
        files = results.get("files", [])

        target = None
        for f in files:
            name = f["name"]
            # 파일명에서 날짜 추출
            m = re.search(r"(\d{4}-\d{2}-\d{2})", name)
            if not m:
                continue
            d = dt.datetime.strptime(m.group(1), "%Y-%m-%d").date()
            if d < TODAY:
                target = f
                break

        if not target:
            return None

        out = DATA_DIR / target["name"]
        with open(out, "wb") as fp:
            req = _drive_service().files().get_media(fileId=target["id"]).execute()
            fp.write(req)
        log(f"[Drive] 전일 파일 다운로드: {out.name}")
        return out
    except Exception as e:
        log(f"[Drive] 전일 탐색/다운로드 실패(무시): {e}")
        return None


# =========================
# CSV IO
# =========================
def save_csv(items: List[Dict], prefix: str) -> Path:
    path = DATA_DIR / f"{prefix}_{TODAY.isoformat()}.csv"
    with path.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["date", "rank", "brand", "product_name", "price", "url", "product_code"])
        for it in items:
            w.writerow([
                TODAY.isoformat(),
                it.get("rank"),
                it.get("brand", ""),
                it.get("name", ""),
                it.get("price", 0),
                it.get("url", ""),
                it.get("product_code", ""),
            ])
    log(f"[CSV] 저장: {path}")
    return path

def load_rows(csv_path: Optional[Path]) -> List[Dict]:
    if not csv_path or not csv_path.exists():
        return []
    rows = []
    with csv_path.open("r", encoding="utf-8") as f:
        r = csv.DictReader(f)
        for row in r:
            rows.append(row)
    return rows


# =========================
# 수집 (HTTP → 실패 시 Playwright)
# =========================
def fetch_by_http(url: str, timeout: int = 15) -> List[Dict]:
    """Qoo10 모바일 랭킹(뷰티) HTML 파싱 (정적). 필요 시 더보기/스크롤은 Playwright에서 담당."""
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
    }
    res = requests.get(url, headers=headers, timeout=timeout)
    if res.status_code != 200:
        return []
    soup = BeautifulSoup(res.text, "html.parser")
    return parse_qoo10_mobile_cards(soup)

def parse_qoo10_mobile_cards(soup: BeautifulSoup) -> List[Dict]:
    """모바일 페이지 카드 파싱. (상위 일부만 뜰 수 있음. 최대 40~60개 정도)"""
    items = []
    cards = soup.select("ul#best_prd_list li a[href*='/item/']")
    seen = set()
    rank = 1
    for a in cards:
        href = a.get("href") or ""
        if "/item/" not in href:
            continue
        pid = extract_pid(href)
        if not pid or pid in seen:
            continue
        seen.add(pid)

        name_el = a.select_one(".prd_txt, .name, .tit")
        brand_el = a.select_one(".brand, .brand_name")
        price_el = a.select_one(".price, .sale_price, .won.prc")

        name = normalize_text(name_el.get_text(strip=True) if name_el else "")
        brand = normalize_text(brand_el.get_text(strip=True) if brand_el else "")
        pr = parse_price(price_el.get_text(strip=True) if price_el else "")

        items.append({
            "rank": rank,
            "brand": brand,
            "name": name,
            "price": pr or 0,
            "url": urllib.parse.urljoin("https://www.qoo10.jp", href),
            "product_code": pid,
        })
        rank += 1
    return items

def fetch_by_playwright(url: str, target_count: int = 200) -> List[Dict]:
    """모바일 페이지에서 스크롤/더보기 등을 통해 200개 근접 수집."""
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True, args=["--no-sandbox"])
        ctx = browser.new_context(viewport={"width": 390, "height": 844})
        page = ctx.new_page()
        page.goto(url, wait_until="networkidle", timeout=60_000)

        # 최대한 아래로 스크롤하여 더 많은 카드 로드
        last_h = 0
        same_count = 0
        for _ in range(40):
            page.mouse.wheel(0, 4000)
            time.sleep(0.6)
            h = page.evaluate("document.body.scrollHeight")
            if h == last_h:
                same_count += 1
                if same_count >= 3:
                    break
            else:
                same_count = 0
            last_h = h

        html = page.content()
        browser.close()

    soup = BeautifulSoup(html, "html.parser")
    items = parse_qoo10_mobile_cards(soup)

    # 혹시 너무 적으면 링크 수집 보강 (다양한 컨테이너)
    if len(items) < target_count:
        extras = []
        links = soup.select("a[href*='/item/']")
        seen = {it["product_code"] for it in items}
        for a in links:
            href = a.get("href") or ""
            pid = extract_pid(href)
            if not pid or pid in seen:
                continue
            name = normalize_text(a.get_text(" ", strip=True))
            if len(name) < 2:
                continue
            price_el = a.select_one(".price, .sale_price, .won.prc")
            pr = parse_price(price_el.get_text(strip=True) if price_el else "")
            extras.append({
                "brand": "",
                "name": name,
                "price": pr or 0,
                "url": urllib.parse.urljoin("https://www.qoo10.jp", href),
                "product_code": pid,
            })
            seen.add(pid)
            if len(items) + len(extras) >= target_count:
                break
        # 순위 보정
        rank = len(items) + 1
        for it in extras:
            it["rank"] = rank
            rank += 1
        items.extend(extras)

    # 최종 순위 재정렬 (rank 필드 기준)
    items = sorted(items, key=lambda x: x["rank"])[:target_count]
    return items

def fetch_products() -> List[Dict]:
    log(f"수집 시작: {QOO10_URL}")
    items = fetch_by_http(QOO10_URL)
    if len(items) < 60:
        log("[HTTP] 결과 적음 → Playwright 폴백 진입")
        items = fetch_by_playwright(QOO10_URL, target_count=200)

    log(f"수집 완료: {len(items)}")
    if len(items) == 0:
        raise RuntimeError("Qoo10 수집 결과 0건. 셀렉터/렌더링 점검 필요")
    return items


# =========================
# 비교/분석
# =========================
def analyze(today_rows: List[Dict], prev_rows: List[Dict],
            top_new_threshold: int = 30,
            limit_rise: int = 3, limit_new: int = 3, limit_fall: int = 5) -> Tuple[List, List, List, int]:
    """
    - 급상승: prev에도 있고 today에도 있는 제품 중 rank 개선(prev - curr > 0) 큰 순 (tie: curr asc → prev asc → 이름)
    - 뉴랭커: prev 없거나 prev_rank > threshold 이고 today_rank <= threshold → curr asc
    - 급하락: prev/today 모두 있고 curr - prev > 0 → 내림차순, 상위 5
    - 인&아웃 수: (인 + 아웃) 개수
    """
    def key_name(d): return d.get("product_name","")

    def rows_to_map(rows):
        mp = {}
        for r in rows:
            code = r.get("product_code") or ""
            try:
                mp[code] = {
                    "rank": int(r.get("rank") or 9999),
                    "name": r.get("product_name",""),
                    "brand": r.get("brand",""),
                    "url": r.get("url",""),
                }
            except:
                pass
        return mp

    T = rows_to_map(today_rows)
    P = rows_to_map(prev_rows)

    rises = []
    new_rankers = []
    falls = []

    # 급상승/하락 후보
    for code, t in T.items():
        if code in P:
            curr = t["rank"]; prev = P[code]["rank"]
            if prev > curr:
                rises.append({
                    "name": t["name"], "curr": curr, "prev": prev, "delta": prev - curr
                })
            elif curr > prev:
                falls.append({
                    "name": t["name"], "curr": curr, "prev": prev, "delta": curr - prev
                })

    rises.sort(key=lambda x: (-x["delta"], x["curr"], x["prev"], x["name"]))
    falls.sort(key=lambda x: (-x["delta"], x["prev"], x["curr"], x["name"]))
    rises = rises[:limit_rise]
    falls = falls[:limit_fall]

    # 뉴랭커
    for code, t in T.items():
        curr = t["rank"]
        if curr <= top_new_threshold:
            if (code not in P) or (P[code]["rank"] > top_new_threshold):
                new_rankers.append({"name": t["name"], "curr": curr})
    new_rankers.sort(key=lambda x: x["curr"])
    new_rankers = new_rankers[:limit_new]

    # 인/아웃 카운트
    ins = 0; outs = 0
    # in: prev>30 또는 미등장 → today<=30
    for code, t in T.items():
        curr = t["rank"]
        if curr <= top_new_threshold:
            if (code not in P) or (P[code]["rank"] > top_new_threshold):
                ins += 1
    # out: prev<=30 → today>30 또는 미등장
    for code, p in P.items():
        if p["rank"] <= top_new_threshold:
            if (code not in T) or (T[code]["rank"] > top_new_threshold):
                outs += 1

    return rises, new_rankers, falls, (ins + outs)


# =========================
# Slack 포맷 빌더
# =========================
def line_top(rank: int, name: str, url: str, price: Optional[int], off_pct: Optional[int]) -> str:
    txt = f"{rank}. <{url}|{name}>"
    if price and price > 0:
        txt += f" — ¥{price:,}"
    if off_pct is not None and off_pct > 0:
        txt += f" (↓{off_pct}%)"
    return txt

def build_slack(today_rows: List[Dict], rises, new_rankers, falls, inout_cnt: int) -> str:
    title = f"*큐텐 재팬 뷰티 랭킹 — {TODAY.isoformat()}*"
    # TOP 10
    top10 = []
    for r in today_rows:
        try:
            rk = int(r["rank"])
        except:
            continue
        if rk > 10:
            continue
        name = r["product_name"]
        url = r["url"]
        price = int(r.get("price") or 0)

        # 할인율은 CSV에 없으니 (모바일 카드에 표시가 일관되지 않아) 0으로 표시
        # 필요 시 수집 단계에서 orig_price/percent를 넣어 확장 가능
        off = None

        top10.append(line_top(rk, name, url, price, off))

    # 번역(옵션): TOP10 이름만
    trans_lines = []
    if ENABLE_JA2KO:
        src_names = [re.sub(r"^(\d+)\.\s+<[^|]+\|", "", ln).split(">")[0] for ln in top10]
        kos = translate_ja_to_ko_batch(src_names)
        for i, ko in enumerate(kos):
            if ko:
                top10[i] = f"{top10[i]}\n{ko}"

    # 급상승
    sec_rise = ["- 해당 없음"] if not rises else [f"- {x['name']} {x['prev']}위 → {x['curr']}위 (↑{x['delta']})" for x in rises]
    # 뉴랭커
    sec_new = ["- 해당 없음"] if not new_rankers else [f"- {x['name']} NEW → {x['curr']}위" for x in new_rankers]
    # 급하락 (최대 5개)
    sec_fall = ["- 해당 없음"] if not falls else [f"- {x['name']} {x['prev']}위 → {x['curr']}위 (↓{x['delta']})" for x in falls]

    # 전체 메시지
    parts = [
        title,
        "",
        "*TOP 10*",
        *top10,
        "",
        "🔥 *급상승*",
        *sec_rise,
        "",
        "🆕 *뉴랭커*",
        *sec_new,
        "",
        "📉 *급하락*",
        *sec_fall,
        "",
        "🔁 *랭크 인&아웃*",
        f"{inout_cnt}개의 제품이 인&아웃 되었습니다.",
    ]
    return "\n".join(parts)


# =========================
# 파이프라인
# =========================
def qoo10_pipeline(items: List[Dict]):
    log(f"[QOO10] collected items: {len(items)}")
    (DATA_DIR / "debug_items.json").write_text(json.dumps(items, ensure_ascii=False, indent=2), encoding="utf-8")

    if len(items) == 0:
        raise RuntimeError("QOO10 수집 결과가 0건입니다. 셀렉터/렌더링 점검 필요")

    # 정규화 + PID
    for it in items:
        it["name"] = normalize_text(it.get("name",""))
        it["brand"] = normalize_text(it.get("brand",""))
        it["product_code"] = extract_pid(it.get("url",""))

    # CSV 저장
    csv_path = save_csv(items, prefix="큐텐재팬_뷰티_랭킹")

    # Drive 업로드 (무시 가능)
    drive_upload_file(csv_path)

    # 오늘/전일 로드
    today_rows = load_rows(csv_path)
    prev_path  = drive_find_latest_prev("큐텐재팬_뷰티_랭킹_")
    prev_rows  = load_rows(prev_path) if prev_path else []

    rises, new_rankers, falls, inout_cnt = analyze(today_rows, prev_rows,
                                                   top_new_threshold=30,
                                                   limit_rise=3, limit_new=3, limit_fall=5)
    msg = build_slack(today_rows, rises, new_rankers, falls, inout_cnt)
    slack_post(msg)


# =========================
# main
# =========================
def main():
    try:
        items = fetch_products()
        qoo10_pipeline(items)
    except Exception as e:
        log("[에러] " + repr(e))
        traceback.print_exc()
        raise

if __name__ == "__main__":
    main()
