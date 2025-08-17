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
  * 수집 상한: QOO10_MAX_RANK (기본 200)
"""

import os, re, io, math, pytz, traceback
import urllib.parse, base64, json
import datetime as dt
from dataclasses import dataclass
from typing import List, Dict, Optional, Tuple

import requests
import pandas as pd
from bs4 import BeautifulSoup

# ---------- const ----------
KST = pytz.timezone("Asia/Seoul")
MOBILE_URLS = [
    "https://www.qoo10.jp/gmkt.inc/Mobile/Bestsellers/Default.aspx?group_code=2",
    "https://www.qoo10.jp/gmkt.inc/Mobile/Bestsellers/Default.aspx?__ar=Y&group_code=2",
    "https://www.qoo10.jp/gmkt.inc/mobile/bestsellers/default.aspx?group_code=2",
]
DESKTOP_URL = "https://www.qoo10.jp/gmkt.inc/Bestsellers/?g=2"
MAX_RANK = int(os.getenv("QOO10_MAX_RANK", "200"))

# ---------- time/utils ----------
def today_kst_str(): return dt.datetime.now(KST).strftime("%Y-%m-%d")
def yesterday_kst_str(): return (dt.datetime.now(KST) - dt.timedelta(days=1)).strftime("%Y-%m-%d")
def build_filename(d): return f"큐텐재팬_뷰티_랭킹_{d}.csv"
def clean_text(s): return re.sub(r"\s+", " ", (s or "")).strip()
def slack_escape(s): return (s or "").replace("&","&amp;").replace("<","&lt;").replace(">","&gt;")

# ---------- name cleaning ----------
BRACKETS_PAT = re.compile(r"(\[.*?\]|【.*?】|（.*?）|\(.*?\))")
JP_CHAR_RE = re.compile(r"[一-龯ぁ-ゔァ-ヴー々〆ヵヶｦ-ﾟ]")

def strip_brackets_for_slack(s: str) -> str:
    return clean_text(BRACKETS_PAT.sub("", s or ""))

def remove_official_token(s: str) -> str:
    # '公式' 제거 + 여백 정리
    return clean_text(re.sub(r"\b公式\b", "", s or ""))

# ---------- price/discount ----------
YEN_AMOUNT_RE = re.compile(r"(?:¥|)(\d{1,3}(?:,\d{3})+|\d+)\s*円")
PCT_RE = re.compile(r"(\d+)\s*% ?OFF", re.I)

def parse_jpy_amounts(text: str) -> List[int]:
    return [int(m.group(1).replace(",", "")) for m in YEN_AMOUNT_RE.finditer(text or "")]

def compute_prices(block_text: str) -> Tuple[Optional[int], Optional[int], Optional[int]]:
    amounts = parse_jpy_amounts(block_text); sale = orig = None
    if amounts:
        sale = min(amounts)
        if len(amounts) >= 2:
            orig = max(amounts)
            if orig == sale: orig = None
    pct = None
    m = PCT_RE.search(block_text)
    if m:
        pct = int(m.group(1))
    elif orig and sale:
        pct = max(0, int((1 - sale / orig) * 100))
    return sale, orig, pct

# ---------- url/code ----------
ITEM_QUERY_RE = re.compile(r"(?:[?&](?:goods?_?code|goodsno)=(\d+))", re.I)
ITEM_PATH_RE = re.compile(r"/(?:Item|item|goods)/(?:.*?/)?(\d+)(?:[/?#]|$)")

def normalize_href(href: str) -> str:
    if href.startswith("//"): return "https:" + href
    if href.startswith("/"):  return "https://www.qoo10.jp" + href
    return href

def extract_goods_code(url: str, block_text=""):
    if not url: return ""
    m = ITEM_QUERY_RE.search(url)
    if m: return m.group(1)
    m2 = ITEM_PATH_RE.search(url)
    if m2: return m2.group(1)
    m3 = re.search(r"商品番号\s*[:：]\s*(\d+)", block_text or "")
    return m3.group(1) if m3 else ""

# ---------- brand ----------
def bs_pick_brand(container) -> str:
    """컨테이너 내에서 상품 링크가 아닌 첫 a를 브랜드로 추정. '公式'류 제거."""
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
    product_code: str

# ---------- fetch/parse (모바일 우선, 실패 시 데스크톱) ----------
def fetch_mobile() -> List[Product]:
    headers = {
        "User-Agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123 Safari/537.36",
        "Accept-Language":"ja,en-US;q=0.9,en;q=0.8,ko;q=0.7",
        "Cache-Control":"no-cache","Pragma":"no-cache",
    }
    for url in MOBILE_URLS:
        try:
            r = requests.get(url, headers=headers, timeout=20); r.raise_for_status()
            html = r.text
            soup = BeautifulSoup(html, "lxml")
            lis = soup.select("li")
            items: List[Product] = []
            seen = set()
            for li in lis:
                a = li.select_one("a[href*='Goods.aspx'], a[href*='/Item/'], a[href*='/item/'], a[href*='/goods']")
                if not a: continue
                href = normalize_href(a.get("href",""))
                name = clean_text(a.get_text(" ", strip=True))
                if not name: continue
                block = clean_text(li.get_text(" ", strip=True))
                code = extract_goods_code(href, block)
                if not code and ("goods.aspx" not in href.lower()):
                    continue
                brand = bs_pick_brand(li)
                price, orig, pct = compute_prices(block)
                key = code or href
                if key in seen: continue
                seen.add(key)
                items.append(Product(len(items)+1, remove_official_token(brand), remove_official_token(name), price, orig, pct, href, code))
                if len(items) >= MAX_RANK: break
            if len(items) >= 10: return items
        except Exception as e:
            print("[모바일 실패]", url, e)
    return []

def fetch_desktop() -> List[Product]:
    from playwright.sync_api import sync_playwright
    with sync_playwright() as p:
        browser = p.chromium.launch(
            headless=True,
            args=["--no-sandbox","--disable-dev-shm-usage","--disable-blink-features=AutomationControlled"],
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
                const name = (a.textContent || '').replace(/\s+/g,' ').trim();
                const li = a.closest('li') || a.closest('div');
                if (!href || !name || !li) continue;

                // 브랜드: 상품 링크가 아닌 첫 a
                let brand = '';
                const anchors = Array.from(li.querySelectorAll('a'));
                for (const b of anchors) {
                  const h = (b.getAttribute('href') || '').toLowerCase();
                  if (h.includes('goods.aspx') || h.includes('/item/')) continue;
                  const t = (b.textContent || '').replace(/\s+/g,' ').trim();
                  if (t.length >= 1 && t.length <= 40) { brand = t; break; }
                }
                const block = (li.innerText || '').replace(/\s+/g,' ').trim();
                const key = href + '|' + name;
                if (seen.has(key)) continue;
                seen.add(key);
                rows.push({href, name, brand, block});
                if (rows.length >= 600) break;
              }
              return rows;
            }
        """)
        context.close(); browser.close()

    items: List[Product] = []; seen=set()
    for row in data:
        href = normalize_href(row.get("href",""))
        name = clean_text(row.get("name",""))
        brand = clean_text(row.get("brand",""))
        block = clean_text(row.get("block",""))
        code = extract_goods_code(href, block)
        price, orig, pct = compute_prices(block)
        key = code or href
        if key in seen: continue
        seen.add(key)
        items.append(Product(len(items)+1, remove_official_token(brand), remove_official_token(name), price, orig, pct, href, code))
        if len(items) >= MAX_RANK: break
    return items

def fetch_products() -> List[Product]:
    items = fetch_mobile()
    if len(items) >= 10: return items
    print("[Playwright 폴백]")
    return fetch_desktop()

# ---------- drive ----------
def build_drive_service():
    try:
        from googleapiclient.discovery import build
        from google.oauth2.service_account import Credentials
        raw = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON","").strip()
        if not raw: return None
        info = json.loads(raw if raw.lstrip().startswith("{") else base64.b64decode(raw).decode("utf-8"))
        creds = Credentials.from_service_account_info(info, scopes=["https://www.googleapis.com/auth/drive"])
        svc = build("drive","v3",credentials=creds, cache_discovery=False)
        try:
            who = svc.about().get(fields="user(displayName,emailAddress)").execute().get("user",{})
            print(f"[Drive] whoami: {who.get('displayName')} <{who.get('emailAddress')}>")
        except Exception as e:
            print("[Drive] whoami 실패:", e)
        return svc
    except Exception as e:
        print("[Drive] 서비스 계정 생성 실패:", e)
        try:
            from googleapiclient.discovery import build
            from google.oauth2.credentials import Credentials
            cid, sec, rt = os.getenv("GOOGLE_CLIENT_ID"), os.getenv("GOOGLE_CLIENT_SECRET"), os.getenv("GOOGLE_REFRESH_TOKEN")
            if not (cid and sec and rt): return None
            creds = Credentials(None, refresh_token=rt, token_uri="https://oauth2.googleapis.com/token",
                                client_id=cid, client_secret=sec)
            svc = build("drive","v3",credentials=creds, cache_discovery=False)
            try:
                who = svc.about().get(fields="user(displayName,emailAddress)").execute().get("user",{})
                print(f"[Drive] whoami: {who.get('displayName')} <{who.get('emailAddress')}>")
            except Exception as e:
                print("[Drive] whoami 실패:", e)
            return svc
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
    fh.seek(0)
    return pd.read_csv(fh)

# ---------- translate ----------
def translate_ja_to_ko_batch(lines: List[str]) -> List[str]:
    if not (os.getenv("SLACK_TRANSLATE_JA2KO","0").lower() in ("1","true","yes")):
        return ["" for _ in lines]
    try:
        # googletrans (googleapis)
        from googletrans import Translator
        tr = Translator(service_urls=['translate.googleapis.com'])
        segs = []
        # 길이 제한 회피를 위한 토막 처리
        for t in lines:
            t = t or ""
            if not t:
                segs.append([("raw", ""), None]); continue
            # 가격/기호 제거된 원문만 번역
            tt = t
            segs.append([("raw", None)],)  # placeholder
        res = tr.translate(lines, src="ja", dest="ko")
        out = []
        for r in (res if isinstance(res, list) else [res]):
            out.append(r.text)
        return out
    except Exception:
        # 실패 시 빈값
        return ["" for _ in lines]

# ===== compare/message ----------
def to_dataframe(products: List[Product], date_str: str) -> pd.DataFrame:
    return pd.DataFrame([{
        "date": date_str,
        "rank": p.rank,
        "brand": p.brand,            # '公式' 제거 반영
        "product_name": p.title,     # '公式' 제거 반영
        "price": p.price,
        "orig_price": p.orig_price,
        "discount_percent": p.discount_percent,
        "url": p.url,
        "product_code": p.product_code,
    } for p in products])

def build_sections(df_today: pd.DataFrame, df_prev: Optional[pd.DataFrame]) -> Dict[str, List[str]]:
    """
    슬랙 포맷 전용 섹션 빌드 (크롤링/브랜드/드라이브 로직에는 영향 없음)
      - TOP10: (↑n)/(↓n)/(New) 마커 + 번역 줄
      - 급상승/뉴랭커: 제거
      - 급하락: Top30 교집합 하락 + OUT 포함, 최대 5개만 노출, 각 항목 번역 줄 포함
      - 인&아웃: 개수만
    """
    S = {"top10": [], "falling": [], "inout_count": 0}

    def plain_name(row):
        nm = strip_brackets_for_slack(clean_text(row.get("product_name","")))
        br = clean_text(row.get("brand",""))
        if br and not nm.lower().startswith(br.lower()):
            nm = f"{br} {nm}"
        return nm

    def link(row):
        return f"<{row['url']}|{slack_escape(plain_name(row))}>"

    def interleave(lines, jp_texts):
        kos = translate_ja_to_ko_batch(jp_texts)
        out = []
        for i, ln in enumerate(lines):
            out.append(ln)
            if kos and i < len(kos) and kos[i]:
                out.append(kos[i])
        return out

    # ---- TOP10
    prev_all = None
    if df_prev is not None and len(df_prev):
        prev_all = df_prev.copy()
        prev_all["key"] = prev_all.apply(lambda x: x["product_code"] if (pd.notnull(x.get("product_code")) and str(x.get("product_code")).strip()) else x["url"], axis=1)
        prev_all.set_index("key", inplace=True)

    jp, lines = [], []
    top10 = df_today.dropna(subset=["rank"]).sort_values("rank").head(10)
    for _, r in top10.iterrows():
        jp.append(plain_name(r))
        marker = ""
        if prev_all is not None:
            k = r.get("product_code") if (pd.notnull(r.get("product_code")) and str(r.get("product_code")).strip()) else r.get("url")
            if k in prev_all.index and pd.notnull(prev_all.loc[k, "rank"]):
                pr, cr = int(prev_all.loc[k, "rank"]), int(r["rank"])
                d = pr - cr
                if d > 0: marker = f"(↑{d}) "
                elif d < 0: marker = f"(↓{abs(d)}) "
            else:
                marker = "(New) "
        tail = f" (↓{int(r['discount_percent'])}%)" if pd.notnull(r.get('discount_percent')) else ""
        # 가격은 원본의 jpy 포맷 함수가 있으면 그걸 쓰고, 없으면 간단 포맷
        def fmt_currency_jpy(v):
            try: return f"₩{int(v):,}"
            except: return "₩0"
        lines.append(f"{int(r['rank'])}. {marker}{link(r)} — {fmt_currency_jpy(r.get('price'))}{tail}")
    S["top10"] = interleave(lines, jp)

    if prev_all is None:
        return S

    # ---- 급하락 (Top30 교집합 하락 + OUT 포함, 최대 5개)
    df_t = df_today.copy()
    df_t["key"] = df_t.apply(lambda x: x["product_code"] if (pd.notnull(x.get("product_code")) and str(x.get("product_code")).strip()) else x["url"], axis=1)
    df_t.set_index("key", inplace=True)
    t30 = df_t[(df_t["rank"].notna()) & (df_t["rank"] <= 30)]
    p30 = prev_all[(prev_all["rank"].notna()) & (prev_all["rank"] <= 30)]
    common = set(t30.index) & set(p30.index)
    out_keys = set(p30.index) - set(t30.index)

    movers = []
    for k in common:
        pr, cr = int(p30.loc[k, "rank"]), int(t30.loc[k, "rank"])
        drop = cr - pr
        if drop > 0:
            row = t30.loc[k]
            movers.append((drop, cr, pr, f"- {link(row)} {pr}위 → {cr}위 (↓{drop})", plain_name(row)))
    # 하락폭 내림차순 → 오늘 순위 → 전일 순위 → 제품명
    movers.sort(key=lambda x: (-x[0], x[1], x[2], x[4]))

    chosen_lines, chosen_jp = [], []
    for _,_,_,txt,jpn in movers:
        if len(chosen_lines) >= 5: break
        chosen_lines.append(txt); chosen_jp.append(jpn)

    # OUT 보충
    if len(chosen_lines) < 5:
        outs_sorted = sorted(list(out_keys), key=lambda k: int(p30.loc[k, "rank"]))
        for k in outs_sorted:
            if len(chosen_lines) >= 5: break
            row = p30.loc[k]
            txt = f"- {link(row)} {int(row['rank'])}위 → OUT"
            chosen_lines.append(txt); chosen_jp.append(plain_name(row))

    S["falling"] = interleave(chosen_lines, chosen_jp)
    S["inout_count"] = len(set(t30.index) - set(p30.index)) + len(out_keys)
    return S

def build_slack_message(date_str: str, S: Dict[str, List[str]]) -> str:
    lines: List[str] = []
    lines.append(f"*큐텐 재팬 뷰티 랭킹 — {date_str}*")
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
    today_file = build_filename(date_str)
    yest_file = build_filename(yesterday_kst_str())

    items = fetch_products()
    if len(items) < 10:
        print("[경고] 품목 10개 미만, 폴백 실패")
        raise RuntimeError("수집 품목 부족")

    df_today = to_dataframe(items, date_str)

    # 구글드라이브 업로드 & 전일 파일 로드
    df_prev = None
    try:
        svc = build_drive_service()
        folder_id = os.getenv("GDRIVE_FOLDER_ID","").strip()
        if svc and folder_id:
            drive_upload_csv(svc, folder_id, today_file, df_today)
            df_prev = drive_download_csv(svc, folder_id, yest_file)
    except Exception as e:
        print("[Drive 경고]", e)

    # 로컬 전일 대체
    if df_prev is None:
        try:
            if os.path.exists(yest_file):
                df_prev = pd.read_csv(yest_file)
        except Exception:
            df_prev = None

    # Slack 메시지
    S = build_sections(df_today, df_prev)
    msg = build_slack_message(date_str, S)
    slack_post(msg)
    print("Slack 전송 완료")

# ---------- Slack ----------
def fmt_currency_jpy(v):
    try: return f"₩{int(v):,}"
    except: return "₩0"

def slack_post(text: str):
    url = os.getenv("SLACK_WEBHOOK_URL")
    if not url:
        print(text); return
    try:
        requests.post(url, json={"text": text, "unfurl_links": False, "unfurl_media": False}, timeout=20)
    except Exception as e:
        print("[Slack 전송 실패]", e)
        print(text)

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print("[오류 발생]", e); traceback.print_exc()
        try:
            slack_post(f"*큐텐 재팬 뷰티 랭킹 자동화 실패*\n```\n{e}\n```")
        except: pass
        raise
