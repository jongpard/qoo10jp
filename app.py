# -*- coding: utf-8 -*-
"""
Qoo10 JP Beauty Bestsellers (g=2)
- 1차: 모바일 베스트셀러 정적 HTML
- 실패 시: 데스크톱 Playwright 폴백
- CSV: 큐텐재팬_뷰티_랭킹_YYYY-MM-DD.csv (KST)
- 비교 키: product_code(商品番号) 우선, 없으면 URL
- 포맷:
  * '公式' 토큰 제거(브랜드/상품명, CSV·Slack 모두)
  * 가격: '...円'에 붙은 금액만 인식(판매수/리뷰수 숫자 배제), sale=최솟값, orig=최댓값
  * Slack: 제품명에서 괄호류([]【】()（）) 내용 제거
  * Slack 모든 섹션 각 항목 아래 1줄 한국어 번역(옵션, SLACK_TRANSLATE_JA2KO=1)
  * 번역 엔진: Google GenAI SDK (gemini-2.5-flash 배치 처리)
  * 수집 상한: QOO10_MAX_RANK (기본 200)
"""

import os
import re
import io
import time
import math
import json
import pytz
import traceback
import datetime as dt
from dataclasses import dataclass
from typing import List, Dict, Optional, Tuple

import requests
import pandas as pd
from bs4 import BeautifulSoup

# Google GenAI 공식 SDK
from google import genai
from google.genai import errors

# ---------- Config ----------
KST = pytz.timezone("Asia/Seoul")
MOBILE_URLS = [
    "https://www.qoo10.jp/gmkt.inc/Mobile/Bestsellers/Default.aspx?group_code=2",
    "https://www.qoo10.jp/gmkt.inc/Mobile/Bestsellers/Default.aspx?__ar=Y&group_code=2",
    "https://www.qoo10.jp/gmkt.inc/mobile/bestsellers/default.aspx?group_code=2",
]
DESKTOP_URL = "https://www.qoo10.jp/gmkt.inc/Bestsellers/?g=2"
MAX_RANK = int(os.getenv("QOO10_MAX_RANK", "200"))

# ---------- time/utils ----------
def now_kst(): return dt.datetime.now(KST)
def today_kst_str(): return now_kst().strftime("%Y-%m-%d")
def yesterday_kst_str(): return (now_kst() - dt.timedelta(days=1)).strftime("%Y-%m-%d")
def build_filename(d): return f"큐텐재팬_뷰티_랭킹_{d}.csv"
def clean_text(s): return re.sub(r"\s+", " ", (s or "")).strip()
def slack_escape(s): return s.replace("&","&amp;").replace("<","&lt;").replace(">","&gt;")

# ---------- '公式' 제거 / 괄호 제거 ----------
OFFICIAL_PAT = re.compile(r"^\s*(公式|公式ショップ|公式ストア)\s*", re.I)
BRACKETS_PAT = re.compile(r"(\[.*?\]|【.*?】|（.*?）|\(.*?\))")

# ----- 일본어 감지
JP_CHAR_RE = re.compile(r"[\u3040-\u30FF\u3400-\u4DBF\u4E00-\u9FFF\uF900-\uFAFF]")
def contains_japanese(s: str) -> bool:
    return bool(JP_CHAR_RE.search(s or ""))

def remove_official_token(s: str) -> str:
    if not s: return ""
    s = clean_text(s)
    s = OFFICIAL_PAT.sub("", s)
    return s

def strip_brackets_for_slack(s: str) -> str:
    if not s: return ""
    return clean_text(BRACKETS_PAT.sub("", s))

# ---------- price/discount ----------
YEN_AMOUNT_RE = re.compile(r"(?:¥|)(\d{1,3}(?:,\d{3})+|\d+)\s*円")
PCT_RE = re.compile(r"(\d+)\s*% ?OFF", re.I)

def parse_jpy_amounts(text: str) -> List[int]:
    return [int(m.group(1).replace(",", "")) for m in YEN_AMOUNT_RE.finditer(text or "")]

def compute_prices(block_text: str) -> Tuple[Optional[int], Optional[int], Optional[int]]:
    amounts_all = parse_jpy_amounts(block_text)
    amounts = [a for a in amounts_all if a > 0]

    sale = orig = None
    if amounts:
        sale = min(amounts)
        if len(amounts) >= 2:
            orig = max(amounts)
            if orig == sale:
                orig = None
    pct = None
    m = PCT_RE.search(block_text)
    if m:
        pct = int(m.group(1))
    elif orig and sale and orig > 0:
        pct = max(0, int(math.floor((1 - sale / orig) * 100)))
    return sale, orig, pct

# ---------- product code ----------
GOODS_CODE_RE = re.compile(r"(?:[?&](?:goods?_?code|goodsno)=(\d+))", re.I)
ITEM_PATH_RE  = re.compile(r"/(?:Item|item)/(?:.*?/)?(\d+)(?:[/?#]|$)")

def extract_goods_code(url: str, block_text: str = "") -> str:
    if not url: return ""
    m = GOODS_CODE_RE.search(url)
    if m: return m.group(1)
    m2 = ITEM_PATH_RE.search(url)
    if m2: return m2.group(1)
    m3 = re.search(r"商品番号\s*[:：]\s*(\d+)", block_text or "")
    return m3.group(1) if m3 else ""

# ---------- brand ----------
def bs_pick_brand(container) -> str:
    if not container: return ""
    for a in container.select("a"):
        href = (a.get("href") or "").lower()
        if ("goods.aspx" in href) or ("/item/" in href) or ("/goods" in href):
            continue
        t = remove_official_token(a.get_text(" ", strip=True))
        if 1 <= len(t) <= 40 and t not in ("公式",):
            return t
    txt = remove_official_token(container.get_text(" ", strip=True))
    m = re.match(r"([^\s\[]{2,})", txt)
    return m.group(1) if m else ""

# ---------- model ----------
@dataclass
class Product:
    rank: Optional[int]
    brand: str
    title: str
    price: Optional[int]
    orig_price: Optional[int]
    discount_percent: Optional[int]
    url: str
    product_code: str = ""

# ---------- parse (mobile static) ----------
def parse_mobile_html(html: str) -> List[Product]:
    soup = BeautifulSoup(html, "lxml")
    anchors = soup.select("a[href*='Goods.aspx'], a[href*='/Item/'], a[href*='/item/']")
    items: List[Product] = []
    seen = set()

    for a in anchors:
        href = a.get("href", "")
        if not href: continue
        container = a.find_parent("li") or a.find_parent("div")
        block_text = clean_text(container.get_text(" ", strip=True)) if container else clean_text(a.get_text(" ", strip=True))

        if href.startswith("//"): href = "https:" + href
        elif href.startswith("/"): href = "https://www.qoo10.jp" + href

        code = extract_goods_code(href, block_text)
        key = code or href
        if key in seen: continue
        seen.add(key)

        name = remove_official_token(a.get_text(" ", strip=True))
        brand = remove_official_token(bs_pick_brand(container))
        sale, orig, pct = compute_prices(block_text)

        items.append(Product(
            rank=len(items)+1, brand=brand, title=name,
            price=sale, orig_price=orig, discount_percent=pct,
            url=href, product_code=code
        ))
        if len(items) >= MAX_RANK: break
    return items

# ---------- fetchers ----------
def fetch_by_http_mobile() -> List[Product]:
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
        "Accept-Language": "ja,en-US;q=0.9,en;q=0.8,ko;q=0.7",
        "Cache-Control": "no-cache", "Pragma": "no-cache",
    }
    last_err = None
    for url in MOBILE_URLS:
        try:
            r = requests.get(url, headers=headers, timeout=20)
            r.raise_for_status()
            items = parse_mobile_html(r.text)
            if len(items) >= 10:
                print(f"[HTTP 모바일] {url} → {len(items)}개")
                return items[:MAX_RANK]
        except Exception as e:
            last_err = e
    if last_err: print("[HTTP 모바일 오류]", last_err)
    return []

def fetch_by_playwright() -> List[Product]:
    from playwright.sync_api import sync_playwright
    with sync_playwright() as p:
        browser = p.chromium.launch(
            headless=True,
            args=["--disable-blink-features=AutomationControlled","--no-sandbox","--disable-dev-shm-usage"],
        )
        context = browser.new_context(
            viewport={"width":1366,"height":900},
            locale="ja-JP",
            timezone_id="Asia/Tokyo",
            user_agent=("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123 Safari/537.36"),
            extra_http_headers={"Accept-Language":"ja,en-US;q=0.9,en;q=0.8,ko;q=0.7"},
        )
        context.add_init_script("Object.defineProperty(navigator,'webdriver',{get:()=>undefined});")
        page = context.new_page()
        page.goto(DESKTOP_URL, wait_until="domcontentloaded", timeout=60_000)
        try: page.wait_for_load_state("networkidle", timeout=25_000)
        except: pass

        data = page.evaluate("""
            () => {
              const as = Array.from(document.querySelectorAll("a[href*='Goods.aspx'], a[href*='/Item/'], a[href*='/item/']"));
              const rows = [];
              const seen = new Set();
              for (const a of as) {
                const href = a.getAttribute('href') || '';
                const name = (a.textContent || '').replace(/\\s+/g,' ').trim();
                const li = a.closest('li') || a.closest('div');
                if (!href || !name || !li) continue;

                let brand = '';
                const anchors = Array.from(li.querySelectorAll('a'));
                for (const b of anchors) {
                  const h = (b.getAttribute('href') || '').toLowerCase();
                  if (h.includes('goods.aspx') || h.includes('/item/')) continue;
                  const t = (b.textContent || '').replace(/\\s+/g,' ').trim();
                  if (t.length >= 1 && t.length <= 40) { brand = t; break; }
                }
                const block = (li.innerText || '').replace(/\\s+/g,' ').trim();
                const key = href + '|' + name;
                if (seen.has(key)) continue;
                seen.add(key);
                rows.push({href, name, brand, block});
              }
              return rows.slice(0, 500);
            }
        """)
        context.close(); browser.close()

    items: List[Product] = []
    seen = set()
    for row in data:
        href = row.get("href","")
        name = remove_official_token(row.get("name",""))
        brand = remove_official_token(row.get("brand",""))
        block_text = clean_text(row.get("block",""))

        if href.startswith("//"): href = "https:" + href
        elif href.startswith("/"): href = "https://www.qoo10.jp" + href

        code = extract_goods_code(href, block_text)
        key = code or href
        if key in seen: continue
        seen.add(key)

        sale, orig, pct = compute_prices(block_text)

        items.append(Product(
            rank=len(items)+1, brand=brand, title=name,
            price=sale, orig_price=orig, discount_percent=pct,
            url=href, product_code=code
        ))
        if len(items) >= MAX_RANK: break
    return items

def fetch_products() -> List[Product]:
    items = fetch_by_http_mobile()
    if len(items) >= 10:
        return items
    print("[Playwright 폴백 진입]")
    return fetch_by_playwright()

# ---------- Drive ----------
def normalize_folder_id(raw: str) -> str:
    if not raw: return ""
    s = raw.strip()
    m = re.search(r"/folders/([a-zA-Z0-9_-]{10,})", s) or re.search(r"[?&]id=([a-zA-Z0-9_-]{10,})", s)
    return (m.group(1) if m else s)

def build_drive_service():
    from googleapiclient.discovery import build
    from google.oauth2.credentials import Credentials
    cid  = os.getenv("GOOGLE_CLIENT_ID")
    csec = os.getenv("GOOGLE_CLIENT_SECRET")
    rtk  = os.getenv("GOOGLE_REFRESH_TOKEN")
    if not (cid and csec and rtk):
        raise RuntimeError("OAuth 자격정보가 없습니다.")
    creds = Credentials(None, refresh_token=rtk, token_uri="https://oauth2.googleapis.com/token",
                        client_id=cid, client_secret=csec)
    svc = build("drive", "v3", credentials=creds, cache_discovery=False)
    try:
        about = svc.about().get(fields="user(displayName,emailAddress)").execute()
        u = about.get("user", {})
        print(f"[Drive] user={u.get('displayName')} <{u.get('emailAddress')}>")
    except Exception as e:
        print("[Drive] whoami 실패:", e)
    return svc

def drive_upload_csv(service, folder_id: str, name: str, df: pd.DataFrame) -> str:
    from googleapiclient.http import MediaIoBaseUpload
    q = f"name = '{name}' and '{folder_id}' in parents and trashed = false"
    res = service.files().list(q=q, fields="files(id,name)",
                               supportsAllDrives=True, includeItemsFromAllDrives=True).execute()
    file_id = res.get("files", [{}])[0].get("id") if res.get("files") else None
    buf = io.BytesIO(); df.to_csv(buf, index=False, encoding="utf-8-sig"); buf.seek(0)
    media = MediaIoBaseUpload(buf, mimetype="text/csv", resumable=False)
    if file_id:
        service.files().update(fileId=file_id, media_body=media, supportsAllDrives=True).execute()
        return file_id
    meta = {"name": name, "parents": [folder_id], "mimeType": "text/csv"}
    created = service.files().create(body=meta, media_body=media, fields="id",
                                     supportsAllDrives=True).execute()
    return created["id"]

def drive_download_csv(service, folder_id: str, name: str) -> Optional[pd.DataFrame]:
    from googleapiclient.http import MediaIoBaseDownload
    res = service.files().list(q=f"name = '{name}' and '{folder_id}' in parents and trashed = false",
                               fields="files(id,name)", supportsAllDrives=True,
                               includeItemsFromAllDrives=True).execute()
    files = res.get("files", [])
    if not files: return None
    fid = files[0]["id"]
    req = service.files().get_media(fileId=fid, supportsAllDrives=True)
    fh = io.BytesIO(); dl = MediaIoBaseDownload(fh, req); done=False
    while not done: _, done = dl.next_chunk()
    fh.seek(0); return pd.read_csv(fh)

# ---------- Slack / Gemini Translate ----------
def fmt_currency_jpy(v) -> str:
    try: return f"¥{int(round(float(v))):,}"
    except: return "¥0"

def slack_post(text: str):
    url = os.getenv("SLACK_WEBHOOK_URL")
    if not url:
        print("[경고] SLACK_WEBHOOK_URL 미설정 → 콘솔 출력\n", text); return
    r = requests.post(url, json={"text": text}, timeout=20)
    if r.status_code >= 300:
        print("[Slack 실패]", r.status_code, r.text)

def translate_ja_to_ko_batch(lines: List[str]) -> List[str]:
    """
    SLACK_TRANSLATE_JA2KO=1 일 때, Gemini official SDK(google-genai)를 사용하여
    일본어가 포함된 라인을 일괄 한국어로 번역합니다.
    """
    flag = os.getenv("SLACK_TRANSLATE_JA2KO", "1").lower() in ("1", "true", "yes")
    texts = [(l or "").strip() for l in lines]
    if not flag or not texts:
        print("[Translate] OFF")
        return ["" for _ in texts]

    targets_idx = []
    targets_text = []
    for idx, text in enumerate(texts):
        if contains_japanese(text):
            targets_idx.append(idx)
            targets_text.append(text)

    if not targets_text:
        return ["" for _ in texts]

    api_key = os.getenv("GEMINI_API_KEY")
    if not api_key:
        print("[Translate] WARN: GEMINI_API_KEY 환경변수가 없습니다.")
        return ["" for _ in texts]

    out = ["" for _ in texts]

    try:
        client = genai.Client(api_key=api_key)
        prompt = (
            "다음은 Qoo10 Japan의 뷰티/화장품 상품명 리스트입니다. "
            "각 항목을 한국 소비자가 알기 쉬운 자연스러운 한국어 표기법으로 1:1 번역하세요.\n"
            "결과는 원본 순서와 개수가 정확히 일치하는 JSON 문자열 배열(List of strings)로만 응답하세요.\n"
            "설명, 주석, 마크다운 코드 블록(```) 등은 일체 포함하지 마세요.\n\n"
            f"[입력 목록]\n{json.dumps(targets_text, ensure_ascii=False)}"
        )

        for attempt in range(1, 4):
            try:
                res = client.models.generate_content(
                    model="gemini-2.5-flash",
                    contents=prompt
                )
                if res and res.text:
                    res_text = res.text.strip()
                    res_text = re.sub(r"^```(?:json)?\s*", "", res_text)
                    res_text = re.sub(r"\s*```$", "", res_text)
                    translated_list = json.loads(res_text)
                    if isinstance(translated_list, list) and len(translated_list) == len(targets_text):
                        for i, translated in zip(targets_idx, translated_list):
                            out[i] = str(translated or "").strip()
                        print(f"[Translate] Gemini API 성공 ({len(targets_text)}건 번역)")
                        return out
                    else:
                        print(f"[Translate] Gemini 응답 개수 불일치 (시도 {attempt}/3)")
            except errors.APIError as e:
                print(f"[Translate] Gemini API 오류 (시도 {attempt}/3): {e}")
                if "429" in str(e) or "RESOURCE_EXHAUSTED" in str(e):
                    time.sleep(2 * attempt)
            except Exception as e:
                print(f"[Translate] 파싱/호출 예외 (시도 {attempt}/3): {e}")
                time.sleep(1)

    except Exception as e:
        print("[Translate] Gemini 클라이언트 초기화 실패:", e)

    print("[Translate] Gemini 번역 실패 (원문 처리)")
    return out

# ---------- compare/message ----------
def to_dataframe(products: List[Product], date_str: str) -> pd.DataFrame:
    return pd.DataFrame([{
        "date": date_str,
        "rank": p.rank,
        "brand": p.brand,
        "product_name": p.title,
        "price": p.price,
        "orig_price": p.orig_price,
        "discount_percent": p.discount_percent,
        "url": p.url,
        "product_code": p.product_code,
    } for p in products])

def build_sections(df_today: pd.DataFrame, df_prev: Optional[pd.DataFrame]) -> Dict[str, List[str]]:
    S = {"top10": [], "falling": [], "inout_count": 0}

    def _plain_name(row):
        nm = strip_brackets_for_slack(clean_text(row.get("product_name", "")))
        br = clean_text(row.get("brand", ""))
        if br and not nm.lower().startswith(br.lower()):
            nm = f"{br} {nm}"
        return nm

    def _link(row):
        return f"<{row['url']}|{slack_escape(_plain_name(row))}>"

    def _interleave(lines, jp_texts):
        kos = translate_ja_to_ko_batch(jp_texts)
        out = []
        for i, ln in enumerate(lines):
            out.append(ln)
            if kos and i < len(kos) and kos[i]:
                out.append(kos[i])
        return out

    # ---------- TOP 10 ----------
    prev_index = None
    if df_prev is not None and len(df_prev):
        prev_index = df_prev.copy()
        prev_index["__key__"] = prev_index.apply(
            lambda x: (str(x.get("product_code")).strip() if (pd.notnull(x.get("product_code")) and str(x.get("product_code")).strip()) else str(x.get("url")).strip()),
            axis=1
        )
        prev_index.set_index("__key__", inplace=True)

    jp_rows, lines = [], []
    top10 = df_today.dropna(subset=["rank"]).sort_values("rank").head(10)
    for _, r in top10.iterrows():
        jp_rows.append(_plain_name(r))
        marker = ""
        if prev_index is not None:
            key = (str(r.get("product_code")).strip() if (pd.notnull(r.get("product_code")) and str(r.get("product_code")).strip()) else str(r.get("url")).strip())
            if key in prev_index.index and pd.notnull(prev_index.loc[key, "rank"]):
                pr, cr = int(prev_index.loc[key, "rank"]), int(r["rank"])
                d = pr - cr
                marker = f"(↑{d}) " if d > 0 else (f"(↓{abs(d)}) " if d < 0 else "")
            else:
                marker = "(New) "
        tail = f" (↓{int(r['discount_percent'])}%)" if pd.notnull(r.get("discount_percent")) else ""
        try:
            price_str = f"￥{int(r.get('price')):,}"
        except Exception:
            price_str = "￥0"
        lines.append(f"{int(r['rank'])}. {marker}{_link(r)} — {price_str}{tail}")
    S["top10"] = _interleave(lines, jp_rows)

    if prev_index is None:
        return S

    # ---------- 급하락 (Top200 기준, OUT 포함) ----------
    cur_index = df_today.copy()
    cur_index["__key__"] = cur_index.apply(
        lambda x: (str(x.get("product_code")).strip() if (pd.notnull(x.get("product_code")) and str(x.get("product_code")).strip()) else str(x.get("url")).strip()),
        axis=1
    )
    cur_index.set_index("__key__", inplace=True)

    t200 = cur_index[(cur_index["rank"].notna()) & (cur_index["rank"] <= MAX_RANK)]
    p200 = prev_index[(prev_index["rank"].notna()) & (prev_index["rank"] <= MAX_RANK)]

    common = set(t200.index) & set(p200.index)
    out_keys = set(p200.index) - set(t200.index)

    movers = []
    for k in common:
        pr, cr = int(p200.loc[k, "rank"]), int(t200.loc[k, "rank"])
        drop = cr - pr
        if drop > 0:
            row = t200.loc[k]
            movers.append((drop, cr, pr, f"- {_link(row)} {pr}위 → {cr}위 (↓{drop})", _plain_name(row)))

    movers.sort(key=lambda x: (-x[0], x[1], x[2], x[4]))

    chosen_lines, chosen_jp = [], []
    for _, _, _, txt, jpn in movers:
        if len(chosen_lines) >= 5:
            break
        chosen_lines.append(txt)
        chosen_jp.append(jpn)

    if len(chosen_lines) < 5:
        outs_sorted = sorted(list(out_keys), key=lambda k: int(p200.loc[k, "rank"]))
        for k in outs_sorted:
            if len(chosen_lines) >= 5:
                break
            row = p200.loc[k]
            txt = f"- {_link(row)} {int(row['rank'])}위 → OUT"
            chosen_lines.append(txt)
            chosen_jp.append(_plain_name(row))

    S["falling"] = _interleave(chosen_lines, chosen_jp)

    # ---------- 인&아웃 개수 ----------
    today_keys = set(t200.index)
    prev_keys  = set(p200.index)
    S["inout_count"] = len(today_keys ^ prev_keys) // 2
    return S

def build_slack_message(date_str: str, S: Dict[str, List[str]]) -> str:
    lines: List[str] = []
    lines.append(f"*Qoo10 Japan 뷰티 랭킹 {MAX_RANK} — {date_str}*")
    lines.append("")
    lines.append("*TOP 10*")
    lines.extend(S.get("top10") or ["- 데이터 없음"])
    lines.append("")
    lines.append("*📉 급하락*")
    lines.extend(S.get("falling") or ["- 해당 없음"])
    lines.append("")
    lines.append("*🔄 랭크 인&아웃*")
    lines.append(f"{S.get('inout_count', 0)}개의 제품이 인&아웃 되었습니다.")
    return "\n".join(lines)

# ---------- main ----------
def main():
    date_str = today_kst_str()
    ymd_yesterday = yesterday_kst_str()
    file_today = build_filename(date_str)
    file_yesterday = build_filename(ymd_yesterday)

    print("수집 시작:", MOBILE_URLS[0])
    items = fetch_products()
    print("수집 완료:", len(items))
    if len(items) < 10:
        raise RuntimeError("제품 카드가 너무 적게 수집되었습니다. 셀렉터/렌더링 점검 필요")

    df_today = to_dataframe(items, date_str)
    os.makedirs("data", exist_ok=True)
    df_today.to_csv(os.path.join("data", file_today), index=False, encoding="utf-8-sig")
    print("로컬 저장:", file_today)

    # Google Drive
    df_prev = None
    folder = normalize_folder_id(os.getenv("GDRIVE_FOLDER_ID",""))
    if folder:
        try:
            svc = build_drive_service()
            drive_upload_csv(svc, folder, file_today, df_today)
            print("Google Drive 업로드 완료:", file_today)
            df_prev = drive_download_csv(svc, folder, file_yesterday)
            print("전일 CSV", "미발견" if df_prev is None else "성공")
        except Exception as e:
            print("Google Drive 처리 오류:", e)
            traceback.print_exc()
    else:
        print("[경고] GDRIVE_FOLDER_ID 미설정 → 드라이브 업로드/전일 비교 생략")

    S = build_sections(df_today, df_prev)
    msg = build_slack_message(date_str, S)
    slack_post(msg)
    print("Slack 전송 완료")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print("[오류 발생]", e)
        traceback.print_exc()
        try:
            slack_post(f"*큐텐 재팬 뷰티 랭킹 자동화 실패*\n```\n{e}\n```")
        except: pass
        raise
