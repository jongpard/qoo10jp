# -*- coding: utf-8 -*-
import os
import re
import csv
import json
import time
import math
import html
import uuid
import base64
import logging
import pathlib
import datetime as dt
from typing import List, Dict, Optional
from dataclasses import dataclass

import requests
from bs4 import BeautifulSoup

# Playwright (sync)
from playwright.sync_api import sync_playwright

# Google Drive (OAuth with refresh token)
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload

# ------------------------------
# 기본 설정
# ------------------------------
URL = "https://www.qoo10.jp/gmkt.inc/Mobile/Bestsellers/Default.aspx?group_code=2"  # 뷰티 카테고리(모바일)
DATA_DIR = pathlib.Path("data")
DATA_DIR.mkdir(parents=True, exist_ok=True)

TODAY_KST = dt.datetime.utcnow() + dt.timedelta(hours=9)
DATE_STR = TODAY_KST.strftime("%Y-%m-%d")
CSV_NAME = f"큐텐재팬_뷰티_랭킹_{DATE_STR}.csv"
CSV_PATH = DATA_DIR / CSV_NAME

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Linux; Android 13; Pixel 7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Mobile Safari/537.36",
    "Accept-Language": "ja,en;q=0.9,ko;q=0.8",
}

# Slack & Drive env
SLACK_WEBHOOK_URL = os.getenv("SLACK_WEBHOOK_URL", "")
GDRIVE_FOLDER_ID = os.getenv("GDRIVE_FOLDER_ID", "")

GOOGLE_CLIENT_ID = os.getenv("GOOGLE_CLIENT_ID", "")
GOOGLE_CLIENT_SECRET = os.getenv("GOOGLE_CLIENT_SECRET", "")
GOOGLE_REFRESH_TOKEN = os.getenv("GOOGLE_REFRESH_TOKEN", "")

# 옵션
TOP_N_FOR_SLACK = 10            # TOP10 출력
MAX_DROP_FOR_SLACK = 5          # 급하락 최대 5개
FORCE_PLAYWRIGHT = os.getenv("FORCE_PLAYWRIGHT", "").strip() == "1"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)

# ------------------------------
# 유틸
# ------------------------------
def kst_now() -> dt.datetime:
    return dt.datetime.utcnow() + dt.timedelta(hours=9)

def yen_to_int(txt: str) -> Optional[int]:
    if not txt:
        return None
    txt = txt.replace(",", "").replace("¥", "").replace("円", "").strip()
    m = re.search(r"(\d+)", txt)
    return int(m.group(1)) if m else None

def percent_to_int(txt: str) -> Optional[int]:
    if not txt:
        return None
    m = re.search(r"(\d+)", txt.replace("-", ""))
    return int(m.group(1)) if m else None

def clean_name(name: str) -> str:
    # Qoo10에서 붙는 "公式" 등 제거
    name = name.strip()
    name = re.sub(r"^\s*公式\s*", "", name)
    # 과한 괄호 제거 규칙(원하시면 조정)
    name = re.sub(r"\s*【.*?】", "", name)
    name = re.sub(r"\s*\(.*?\)", "", name)
    return name.strip()

# ------------------------------
# 수집 (HTTP / Playwright)
# ------------------------------
def fetch_http() -> str:
    logging.info("HTTP 요청 시작: %s", URL)
    r = requests.get(URL, headers=HEADERS, timeout=30)
    r.raise_for_status()
    return r.text

def fetch_playwright() -> str:
    logging.info("Playwright 수집 시작")
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        ctx = browser.new_context(
            user_agent=HEADERS["User-Agent"],
            locale="ja-JP",
            viewport={"width": 412, "height": 860},
        )
        page = ctx.new_page()
        page.goto(URL, wait_until="domcontentloaded", timeout=40_000)

        # 목록이 로드될 때까지 대기 (모바일 베스트셀러 영역)
        # 다양한 DOM 변형에 대응해 후보 셀렉터 준비
        candidates = [
            "#lstBest li",            # 기존 모바일 리스트
            ".best_list li",
            "ul li[id*='best']",
            "li .thumb"               # 최후 fallback
        ]
        found = False
        for sel in candidates:
            try:
                page.wait_for_selector(sel, timeout=8_000)
                found = True
                break
            except Exception:
                continue
        if not found:
            # 그래도 첫화면 HTML 반환 (파싱 쪽에서 0건처리 시도)
            logging.warning("Playwright: 예상 리스트 셀렉터 탐지 실패")
        html = page.content()
        ctx.close()
        browser.close()
        return html

# ------------------------------
# 파서
# ------------------------------
def parse_qoo10(html_text: str) -> List[Dict]:
    """
    모바일 베스트셀러 목록 파싱
    반환: [{rank, brand, name, price, orig_price, discount_percent, url, product_code}]
    """
    soup = BeautifulSoup(html_text, "lxml")

    # 주요 리스트 선택자 후보
    containers = []
    for sel in ["#lstBest", ".best_list", "ul"]:
        nodes = soup.select(sel)
        if nodes:
            containers.extend(nodes)

    items: List[Dict] = []
    rank_seen = set()

    def extract_card(li) -> Optional[Dict]:
        # 링크/상품코드
        a = li.select_one("a[href*='/item/']")
        if not a:
            a = li.select_one("a[href]")
        if not a:
            return None
        href = a.get("href", "")
        if not href.startswith("http"):
            href = "https://www.qoo10.jp" + href
        m = re.search(r"/item/(\d+)", href)
        pid = m.group(1) if m else ""

        # 타이틀
        title_el = li.select_one(".tit, .title, .goods_name, .prdName, .name")
        title = (title_el.get_text(" ", strip=True) if title_el else a.get_text(" ", strip=True)).strip()

        # 가격
        price_el = li.select_one(".price, .prc, .num, .won, .sale")
        price = yen_to_int(price_el.get_text(" ", strip=True)) if price_el else None

        # 정가/할인
        orig_el = li.select_one(".org, .strike, .through")
        orig_price = yen_to_int(orig_el.get_text(" ", strip=True)) if orig_el else None

        disc_el = li.select_one(".per, .discount, .rate")
        discount_percent = percent_to_int(disc_el.get_text(" ", strip=True)) if disc_el else None

        # 브랜드 (있으면)
        brand_el = li.select_one(".brand, .shop, .mall")
        brand = brand_el.get_text(" ", strip=True) if brand_el else ""

        item = {
            "rank": None,  # 후속에서 채움
            "brand": brand,
            "name": clean_name(title),
            "price": price,
            "orig_price": orig_price,
            "discount_percent": discount_percent,
            "url": href,
            "product_code": pid,
        }
        return item

    # li 카드 탐색
    li_nodes = soup.select("#lstBest li") or soup.select(".best_list li") or soup.select("ul li")
    rank = 0
    for li in li_nodes:
        item = extract_card(li)
        if not item:
            continue
        rank += 1
        item["rank"] = rank
        if rank in rank_seen:
            continue
        rank_seen.add(rank)
        items.append(item)

    # rank가 안붙었다면 fallback: 처음 200개까지만 순번 부여
    if items and items[0].get("rank") is None:
        for i, it in enumerate(items, start=1):
            it["rank"] = i

    # 정리
    items = [x for x in items if x.get("name")]
    return items

# ------------------------------
# 수집 오케스트레이션 (HTTP → 0건이면 Playwright 폴백)
# ------------------------------
def fetch_products() -> List[Dict]:
    if FORCE_PLAYWRIGHT:
        logging.info("FORCE_PLAYWRIGHT=1 → Playwright 강제 사용")
        html = fetch_playwright()
        items = parse_qoo10(html)
    else:
        # 1) HTTP
        try:
            html = fetch_http()
            items = parse_qoo10(html)
            if not items:
                logging.info("HTTP 파싱 0건 → Playwright 폴백 시도")
                html = fetch_playwright()
                items = parse_qoo10(html)
        except Exception as e:
            logging.warning("HTTP 단계 예외 → Playwright 폴백: %s", e)
            html = fetch_playwright()
            items = parse_qoo10(html)

    if not items:
        raise RuntimeError("Qoo10 수집 결과 0건. 셀렉터/렌더링 점검 필요")

    return items

# ------------------------------
# CSV 저장 / 읽기
# ------------------------------
def save_csv(items: List[Dict], path: pathlib.Path) -> None:
    fields = ["date", "rank", "brand", "name", "price", "orig_price", "discount_percent", "url", "product_code"]
    with path.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        for it in items:
            row = {
                "date": DATE_STR,
                "rank": it.get("rank"),
                "brand": it.get("brand", ""),
                "name": it.get("name", ""),
                "price": it.get("price"),
                "orig_price": it.get("orig_price"),
                "discount_percent": it.get("discount_percent"),
                "url": it.get("url"),
                "product_code": it.get("product_code"),
            }
            w.writerow(row)

def load_csv(path: pathlib.Path) -> List[Dict]:
    if not path.exists():
        return []
    rows = []
    with path.open("r", encoding="utf-8") as f:
        r = csv.DictReader(f)
        for row in r:
            # 타입 보정
            for k in ("rank", "price", "orig_price", "discount_percent"):
                if row.get(k) not in (None, ""):
                    try:
                        row[k] = int(row[k])
                    except Exception:
                        row[k] = None
            rows.append(row)
    return rows

def yesterday_csv_path() -> Optional[pathlib.Path]:
    # 같은 폴더 내 어제 파일을 찾는 간단한 방식
    y = (kst_now() - dt.timedelta(days=1)).strftime("%Y-%m-%d")
    p = DATA_DIR / f"큐텐재팬_뷰티_랭킹_{y}.csv"
    return p if p.exists() else None

# ------------------------------
# 랭킹 비교
# ------------------------------
@dataclass
class DiffItem:
    name: str
    brand: str
    prev_rank: Optional[int]
    curr_rank: Optional[int]
    url: str

def build_index(rows: List[Dict]) -> Dict[str, Dict]:
    """비교 키: product_code→ 없으면 url→ 없으면 name"""
    idx = {}
    for r in rows:
        key = r.get("product_code") or r.get("url") or r.get("name")
        if key:
            idx[key] = r
    return idx

def compute_diffs(prev_rows: List[Dict], curr_rows: List[Dict]):
    prev_idx = build_index(prev_rows)
    curr_idx = build_index(curr_rows)

    # 급상승(전일/당일 모두 존재, 순위 개선)
    risers: List[DiffItem] = []
    # 급하락(전일/당일 모두 존재, 순위 하락)
    fallers: List[DiffItem] = []
    # 뉴랭커(전일 Top30 밖/미등장 → 당일 Top30 진입)
    newcomers: List[DiffItem] = []
    # 인&아웃 수 (차트인 + 랭크아웃)
    inout_count = 0

    # 전일↔당일 매칭
    for key, cur in curr_idx.items():
        prev = prev_idx.get(key)
        if prev and prev.get("rank") and cur.get("rank"):
            delta = prev["rank"] - cur["rank"]
            if delta > 0:
                risers.append(DiffItem(
                    name=cur["name"], brand=cur.get("brand",""),
                    prev_rank=prev["rank"], curr_rank=cur["rank"], url=cur["url"]
                ))
            elif delta < 0:
                fallers.append(DiffItem(
                    name=cur["name"], brand=cur.get("brand",""),
                    prev_rank=prev["rank"], curr_rank=cur["rank"], url=cur["url"]
                ))

    # 뉴랭커: 전일 Top30 밖/미등장 → 오늘 ≤30
    for key, cur in curr_idx.items():
        if cur.get("rank") and cur["rank"] <= 30:
            prev = prev_idx.get(key)
            if (not prev) or (prev.get("rank") is None) or prev["rank"] > 30:
                newcomers.append(DiffItem(
                    name=cur["name"], brand=cur.get("brand",""),
                    prev_rank=prev["rank"] if prev else None,
                    curr_rank=cur["rank"], url=cur["url"]
                ))

    # 인&아웃 개수
    ins = len([d for d in newcomers if d.curr_rank and d.curr_rank <= 30])
    outs = 0
    for key, prv in prev_idx.items():
        if prv.get("rank") and prv["rank"] <= 30:
            cur = curr_idx.get(key)
            if (not cur) or (cur.get("rank") is None) or (cur["rank"] > 30):
                outs += 1
    inout_count = ins + outs

    # 정렬 규칙
    risers.sort(key=lambda x: (x.prev_rank - x.curr_rank), reverse=True)  # 개선폭 desc
    fallers.sort(key=lambda x: (x.curr_rank - x.prev_rank), reverse=True)  # 하락폭 desc
    newcomers.sort(key=lambda x: x.curr_rank)

    return risers, newcomers, fallers, inout_count

# ------------------------------
# Slack
# ------------------------------
def slack_post(text: str):
    if not SLACK_WEBHOOK_URL:
        logging.info("[Slack] Webhook 미설정 → 스킵")
        return
    try:
        requests.post(SLACK_WEBHOOK_URL, json={"text": text}, timeout=10)
    except Exception as e:
        logging.warning("Slack 전송 실패: %s", e)

def fmt_top10(rows: List[Dict]) -> str:
    lines = []
    for r in rows[:TOP_N_FOR_SLACK]:
        price = f"¥{r['price']:,}" if r.get("price") else ""
        disc = f" (↓{r['discount_percent']}%)" if r.get("discount_percent") not in (None, "") else ""
        name = r["name"]
        brand = r.get("brand","")
        # 제품명 하이퍼링크 (Slack 마크다운)
        link = f"<{r['url']}|{brand + ' ' if brand else ''}{name}>"
        lines.append(f"{r['rank']}. {link} — {price}{disc}")
    return "\n".join(lines)

def fmt_diff_line(d: DiffItem, updown: str) -> str:
    # - 제품명 71위 → 7위 (↑64)
    arrow = "↑" if updown == "up" else "↓"
    return f"- {d.name} {d.prev_rank if d.prev_rank else 'OUT'}위 → {d.curr_rank if d.curr_rank else 'OUT'}위 ({arrow}{abs((d.prev_rank or 0) - (d.curr_rank or 0))})"

def build_slack_message(curr_rows: List[Dict], prev_rows: List[Dict]) -> str:
    header = f"*Qoo10 재팬 뷰티 랭킹 — {DATE_STR}*"
    top10 = fmt_top10(curr_rows)

    risers, newcomers, fallers, inout_count = compute_diffs(prev_rows, curr_rows)

    if risers:
        risers_txt = "\n".join([fmt_diff_line(x, "up") for x in risers[:3]])
    else:
        risers_txt = "- 해당 없음"

    if newcomers:
        newcomers_txt = "\n".join([fmt_diff_line(x, "up") for x in newcomers[:3]])
    else:
        newcomers_txt = "- 해당 없음"

    if fallers:
        fallers_txt = "\n".join([fmt_diff_line(x, "down") for x in fallers[:MAX_DROP_FOR_SLACK]])
    else:
        fallers_txt = "- 해당 없음"

    tail = f"{inout_count}개의 제품이 인&아웃 되었습니다."

    blocks = [
        header,
        "",
        "*TOP 10*",
        top10 or "- 데이터 없음",
        "",
        "🔥 *급상승*",
        risers_txt,
        "",
        "🆕 *뉴랭커*",
        newcomers_txt,
        "",
        "📉 *급하락*",
        fallers_txt,
        "",
        f"📦 *랭크 인&아웃*",
        tail,
    ]
    return "\n".join(blocks)

# ------------------------------
# Google Drive 업로드
# ------------------------------
def drive_upload(file_path: pathlib.Path) -> Optional[str]:
    if not (GOOGLE_CLIENT_ID and GOOGLE_CLIENT_SECRET and GOOGLE_REFRESH_TOKEN and GDRIVE_FOLDER_ID):
        logging.info("[Drive] env 미설정 → 스킵")
        return None
    try:
        creds = Credentials(
            None,
            refresh_token=GOOGLE_REFRESH_TOKEN,
            token_uri="https://oauth2.googleapis.com/token",
            client_id=GOOGLE_CLIENT_ID,
            client_secret=GOOGLE_CLIENT_SECRET,
        )
        service = build("drive", "v3", credentials=creds)
        media = MediaFileUpload(str(file_path), resumable=False)
        body = {"name": file_path.name, "parents": [GDRIVE_FOLDER_ID]}
        f = service.files().create(body=body, media_body=media, fields="id").execute()
        fid = f.get("id")
        logging.info("Google Drive 업로드 완료: %s", fid)
        return fid
    except Exception as e:
        logging.warning("Google Drive 업로드 실패: %s", e)
        return None

# ------------------------------
# main
# ------------------------------
def main():
    logging.info("수집 시작: %s", URL)
    t0 = time.time()

    items = fetch_products()
    logging.info("수집 개수: %d", len(items))

    # 저장
    save_csv(items, CSV_PATH)

    # 전일 CSV 로딩
    prev_path = yesterday_csv_path()
    prev_rows = load_csv(prev_path) if prev_path else []

    # Slack 메시지 구성/발송
    msg = build_slack_message(items, prev_rows)
    slack_post(msg)

    # Google Drive 업로드
    drive_upload(CSV_PATH)

    logging.info("총 소요: %.1fs", time.time() - t0)

if __name__ == "__main__":
    main()
