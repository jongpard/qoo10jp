# -*- coding: utf-8 -*-
"""
큐텐 재팬 뷰티(https://www.qoo10.jp/gmkt.inc/Bestsellers/?g=2) 랭킹 자동화
- 1차 소스: 모바일 베스트셀러 (정적 HTML)
  https://www.qoo10.jp/gmkt.inc/Mobile/Bestsellers/Default.aspx?group_code=2
  (백업: ?__ar=Y 파라미터 변형도 시도)
- 실패 시: 데스크톱 페이지를 Playwright로 폴백
- 파일명: 큐텐재팬_뷰티_랭킹_YYYY-MM-DD.csv (KST)
- Top30 비교(전일 CSV) 기준으로 Slack 메시지 생성 (TOP10 / 급상승 / 뉴랭커 / 급하락(+OUT) / 인&아웃 개수)

환경변수:
  SLACK_WEBHOOK_URL
  GOOGLE_CLIENT_ID, GOOGLE_CLIENT_SECRET, GOOGLE_REFRESH_TOKEN
  GDRIVE_FOLDER_ID
"""

import os, re, io, math, json, pytz, traceback
import datetime as dt
from dataclasses import dataclass
from typing import List, Dict, Optional, Tuple

import requests
import pandas as pd
from bs4 import BeautifulSoup

KST = pytz.timezone("Asia/Seoul")
MOBILE_URLS = [
    "https://www.qoo10.jp/gmkt.inc/Mobile/Bestsellers/Default.aspx?group_code=2",
    "https://www.qoo10.jp/gmkt.inc/Mobile/Bestsellers/Default.aspx?__ar=Y&group_code=2",
    "https://www.qoo10.jp/gmkt.inc/mobile/bestsellers/default.aspx?group_code=2",
]
DESKTOP_URL = "https://www.qoo10.jp/gmkt.inc/Bestsellers/?g=2"

# ---------- 시간/문자 유틸 ----------
def now_kst(): return dt.datetime.now(KST)
def today_kst_str(): return now_kst().strftime("%Y-%m-%d")
def yesterday_kst_str(): return (now_kst() - dt.timedelta(days=1)).strftime("%Y-%m-%d")
def build_filename(d): return f"큐텐재팬_뷰티_랭킹_{d}.csv"
def clean_text(s): return re.sub(r"\s+", " ", (s or "")).strip()
def to_float(s):
    if s is None: return None
    try: return float(str(s).replace(",", ""))
    except: return None

# ---------- 통화/표기 ----------
YEN_NUM_RE = re.compile(r"(?<!\d)(\d{1,3}(?:,\d{3})+|\d+)(?![\d.])")
PCT_RE = re.compile(r"(\d+)\s*% ?OFF", re.I)

def parse_jpy_to_int(x: str) -> Optional[int]:
    """텍스트에서 엔 금액 추출 → int"""
    if not x: return None
    t = x.replace(",", "").replace("円", "").strip()
    m = re.match(r"^\d+$", t)
    return int(t) if m else None

def fmt_currency_jpy(v) -> str:
    try:
        return f"¥{int(round(float(v))):,}"
    except:
        return "¥0"

def discount_floor(orig: Optional[float], sale: Optional[float], pct_txt: Optional[str]) -> Optional[int]:
    if pct_txt:
        m = re.search(r"\d+", pct_txt)
        if m: return int(m.group(0))
    if orig and sale and orig > 0:
        return max(0, int(math.floor((1 - sale / orig) * 100)))
    return None

def slack_escape(s): return s.replace("&","&amp;").replace("<","&lt;").replace(">","&gt;")

# ---------- 모델 ----------
@dataclass
class Product:
    rank: Optional[int]
    brand: str        # 없으면 ""
    title: str        # 상품명
    price: Optional[float]       # 판매가
    orig_price: Optional[float]  # 정가(없으면 None)
    discount_percent: Optional[int]
    url: str

def make_display_name(brand: str, product: str, include_brand: bool=True) -> str:
    product = clean_text(product); brand = clean_text(brand)
    if not include_brand or not brand: return product
    if re.match(rf"^\[?\s*{re.escape(brand)}\b", product, flags=re.I): return product
    return f"{brand} {product}"

# ---------- 파싱 로직 (모바일 정적) ----------
def parse_mobile_html(html: str) -> List[Product]:
    """
    모바일 베스트셀러 페이지는 정적 HTML로 내려와 파싱이 쉬움.
    - 각 아이템은 a[href*='Goods.aspx']를 포함
    - 텍스트 블록 내에 'xx%OFF orig sale' 패턴이 자주 등장
    """
    soup = BeautifulSoup(html, "lxml")
    anchors = soup.select("a[href*='Goods.aspx']")
    items: List[Product] = []
    seen = set()
    for idx, a in enumerate(anchors, start=1):
        href = a.get("href", "")
        if not href: continue
        # 중복 제거(같은 상품이 상/하 이미지로 2번 노출되는 경우)
        key = (href, clean_text(a.get_text(" ", strip=True)))
        if key in seen: continue
        seen.add(key)

        # 상위 컨테이너 찾기
        container = a.find_parent("li") or a.find_parent("div")
        block_text = clean_text(container.get_text(" ", strip=True)) if container else clean_text(a.get_text(" ", strip=True))
        name = clean_text(a.get_text(" ", strip=True))
        brand = ""  # 큐텐은 브랜드 필드가 분리되지 않은 케이스 多

        # 가격/할인 파싱
        pct = None; sale = None; orig = None
        m_pct = PCT_RE.search(block_text)
        if m_pct:
            pct = int(m_pct.group(1))
            tail = block_text[m_pct.end():]  # %OFF 뒤쪽에서 가격 1~2개 뽑기
            nums = [parse_jpy_to_int(m) for m in YEN_NUM_RE.findall(tail)]
            nums = [n for n in nums if n is not None]
            if len(nums) >= 2:
                orig, sale = nums[0], nums[1]
            elif len(nums) == 1:
                sale = nums[0]
        else:
            # %OFF가 없으면 블록 전체에서 숫자 후보 추출
            nums = [parse_jpy_to_int(m) for m in YEN_NUM_RE.findall(block_text)]
            nums = [n for n in nums if n is not None]
            # 후기개수/평점 숫자도 섞여있을 수 있으니, 뒤쪽 큰 값 1~2개만 채택
            if nums:
                # 큰 값 상위 2개
                pick = sorted(nums)[-2:]
                if len(pick) == 2:
                    orig, sale = max(pick), min(pick)
                else:
                    sale = pick[-1]
        # 할인 재계산(백업)
        pct = discount_floor(orig, sale, str(pct) if pct is not None else None)

        # 절대 URL
        if href.startswith("//"): href = "https:" + href
        elif href.startswith("/"): href = "https://www.qoo10.jp" + href

        items.append(Product(rank=idx, brand=brand, title=name, price=sale, orig_price=orig, discount_percent=pct, url=href))
    # 상위 60개까지만 (뷰티 탭 첫 화면 기준)
    return items[:60]

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
                return items
        except Exception as e:
            last_err = e
    if last_err: print("[HTTP 모바일 오류]", last_err)
    return []

# ---------- Playwright (데스크톱 폴백) ----------
def fetch_by_playwright() -> List[Product]:
    from playwright.sync_api import sync_playwright
    import time, pathlib

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

        # 스크롤 폴링
        start = dt.datetime.now().timestamp()
        found = 0
        while dt.datetime.now().timestamp() - start < 35:
            try: page.mouse.wheel(0, 1200)
            except: pass
            # a[href*='Goods.aspx'] 수
            try:
                found = page.eval_on_selector_all("a[href*='Goods.aspx']", "els => els.length")
            except:
                found = 0
            if found >= 30: break
            page.wait_for_timeout(600)

        # JS에서 필요한 텍스트만 수집 (name, href, containerText)
        data = page.evaluate("""
            () => {
              const as = Array.from(document.querySelectorAll("a[href*='Goods.aspx']"));
              const uniq = new Map();
              for (const a of as) {
                const href = a.getAttribute('href') || '';
                const name = (a.textContent || '').replace(/\\s+/g,' ').trim();
                if (!href || !name) continue;
                const li = a.closest('li') || a.closest('div');
                const block = (li ? li.innerText : a.innerText || '').replace(/\\s+/g,' ').trim();
                const key = href + '|' + name;
                if (!uniq.has(key)) uniq.set(key, {href, name, block});
              }
              return Array.from(uniq.values()).slice(0, 80);
            }
        """)
        context.close(); browser.close()

    items: List[Product] = []
    for i, row in enumerate(data, start=1):
        href = row.get("href",""); name = clean_text(row.get("name",""))
        block_text = clean_text(row.get("block",""))
        if href.startswith("//"): href = "https:" + href
        elif href.startswith("/"): href = "https://www.qoo10.jp" + href

        # 가격/할인
        pct = None; sale = None; orig = None
        m_pct = PCT_RE.search(block_text)
        if m_pct:
            pct = int(m_pct.group(1))
            tail = block_text[m_pct.end():]
            nums = [parse_jpy_to_int(m) for m in YEN_NUM_RE.findall(tail)]
            nums = [n for n in nums if n is not None]
            if len(nums) >= 2:
                orig, sale = nums[0], nums[1]
            elif len(nums) == 1:
                sale = nums[0]
        else:
            nums = [parse_jpy_to_int(m) for m in YEN_NUM_RE.findall(block_text)]
            nums = [n for n in nums if n is not None]
            if nums:
                pick = sorted(nums)[-2:]
                if len(pick) == 2:
                    orig, sale = max(pick), min(pick)
                else:
                    sale = pick[-1]
        pct = discount_floor(orig, sale, str(pct) if pct is not None else None)

        items.append(Product(rank=i, brand="", title=name, price=sale, orig_price=orig, discount_percent=pct, url=href))
    return items[:60]

def fetch_products() -> List[Product]:
    items = fetch_by_http_mobile()
    if len(items) >= 10:
        return items
    print("[Playwright 폴백 진입]")
    return fetch_by_playwright()

# ---------- Google Drive (국내/글로벌과 동일) ----------
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
        raise RuntimeError("OAuth 자격정보가 없습니다. GOOGLE_CLIENT_ID/SECRET/REFRESH_TOKEN 확인")
    creds = Credentials(
        None,
        refresh_token=rtk,
        token_uri="https://oauth2.googleapis.com/token",
        client_id=cid,
        client_secret=csec,
    )
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

# ---------- Slack ----------
def slack_post(text: str):
    url = os.getenv("SLACK_WEBHOOK_URL")
    if not url:
        print("[경고] SLACK_WEBHOOK_URL 미설정 → 콘솔 출력\n", text); return
    r = requests.post(url, json={"text": text}, timeout=20)
    if r.status_code >= 300:
        print("[Slack 실패]", r.status_code, r.text)

# ---------- DF/비교/메시지 ----------
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
    } for p in products])

def line_move(name_link: str, prev_rank: Optional[int], curr_rank: Optional[int]) -> Tuple[str, int]:
    if prev_rank is None and curr_rank is not None: return f"- {name_link} NEW → {curr_rank}위", 99999
    if curr_rank is None and prev_rank is not None: return f"- {name_link} {prev_rank}위 → OUT", 99999
    if prev_rank is None or curr_rank is None:    return f"- {name_link}", 0
    delta = prev_rank - curr_rank
    if   delta > 0: return f"- {name_link} {prev_rank}위 → {curr_rank}위 (↑{delta})", delta
    elif delta < 0: return f"- {name_link} {prev_rank}위 → {curr_rank}위 (↓{abs(delta)})", abs(delta)
    else:           return f"- {name_link} {prev_rank}위 → {curr_rank}위 (변동없음)", 0

def build_sections(df_today: pd.DataFrame, df_prev: Optional[pd.DataFrame]) -> Dict[str, List[str]]:
    S = {"top10": [], "rising": [], "newcomers": [], "falling": [], "outs": [], "inout_count": 0}

    # TOP 10 (브랜드 포함—큐텐은 브랜드 분리가 안될 수 있어 product_name 그대로 노출)
    top10 = df_today.dropna(subset=["rank"]).sort_values("rank").head(10)
    for _, r in top10.iterrows():
        disp = make_display_name(r.get("brand",""), r["product_name"], include_brand=True)
        name_link = f"<{r['url']}|{slack_escape(disp)}>"
        price_txt = fmt_currency_jpy(r["price"])
        dc = r.get("discount_percent")
        tail = f" (↓{int(dc)}%)" if pd.notnull(dc) else ""
        S["top10"].append(f"{int(r['rank'])}. {name_link} — {price_txt}{tail}")

    if df_prev is None or not len(df_prev):
        return S

    df_t = df_today.copy(); df_t["key"] = df_t["url"]; df_t.set_index("key", inplace=True)
    df_p = df_prev.copy(); df_p["key"] = df_p["url"]; df_p.set_index("key", inplace=True)

    t30 = df_t[(df_t["rank"].notna()) & (df_t["rank"] <= 30)].copy()
    p30 = df_p[(df_p["rank"].notna()) & (df_p["rank"] <= 30)].copy()
    common = set(t30.index) & set(p30.index)
    new    = set(t30.index) - set(p30.index)
    out    = set(p30.index) - set(t30.index)

    def full_name_link(row):
        disp = make_display_name(row.get("brand",""), row.get("product_name",""), include_brand=True)
        return f"<{row['url']}|{slack_escape(disp)}>"

    # 🔥 급상승 (상위 3)
    rising = []
    for k in common:
        prev_rank = int(p30.loc[k,"rank"]); curr_rank = int(t30.loc[k,"rank"])
        imp = prev_rank - curr_rank
        if imp > 0:
            line,_ = line_move(full_name_link(t30.loc[k]), prev_rank, curr_rank)
            rising.append((imp, curr_rank, prev_rank, slack_escape(t30.loc[k].get("product_name","")), line))
    rising.sort(key=lambda x: (-x[0], x[1], x[2], x[3]))
    S["rising"] = [e[-1] for e in rising[:3]]

    # 🆕 뉴랭커 (≤3)
    newcomers = []
    for k in new:
        curr_rank = int(t30.loc[k,"rank"])
        newcomers.append((curr_rank, f"- {full_name_link(t30.loc[k])} NEW → {curr_rank}위"))
    newcomers.sort(key=lambda x: x[0])
    S["newcomers"] = [line for _, line in newcomers[:3]]

    # 📉 급하락 (상위 5)
    falling = []
    for k in common:
        prev_rank = int(p30.loc[k,"rank"]); curr_rank = int(t30.loc[k,"rank"])
        drop = curr_rank - prev_rank
        if drop > 0:
            line,_ = line_move(full_name_link(t30.loc[k]), prev_rank, curr_rank)
            falling.append((drop, curr_rank, prev_rank, slack_escape(t30.loc[k].get("product_name","")), line))
    falling.sort(key=lambda x: (-x[0], x[1], x[2], x[3]))
    S["falling"] = [e[-1] for e in falling[:5]]

    # OUT (전일 Top30 → 금일 Top30 밖)
    for k in sorted(list(out)):
        prev_rank = int(p30.loc[k,"rank"])
        line,_ = line_move(full_name_link(p30.loc[k]), prev_rank, None)
        S["outs"].append(line)

    S["inout_count"] = len(new) + len(out)
    return S

def build_slack_message(date_str: str, S: Dict[str, List[str]]) -> str:
    lines: List[str] = []
    lines.append(f"*큐텐 재팬 뷰티 랭킹 — {date_str}*")
    lines.append("")
    lines.append("*TOP 10*");          lines.extend(S.get("top10") or ["- 데이터 없음"]); lines.append("")
    lines.append("*🔥 급상승*");       lines.extend(S.get("rising") or ["- 해당 없음"]); lines.append("")
    lines.append("*🆕 뉴랭커*");       lines.extend(S.get("newcomers") or ["- 해당 없음"]); lines.append("")
    lines.append("*📉 급하락*");       lines.extend(S.get("falling") or ["- 해당 없음"])
    lines.extend(S.get("outs") or [])
    lines.append(""); lines.append("*🔄 랭크 인&아웃*")
    lines.append(f"{S.get('inout_count', 0)}개의 제품이 인&아웃 되었습니다.")
    return "\n".join(lines)

# ---------- 메인 ----------
def main():
    date_str = today_kst_str()
    ymd_yesterday = yesterday_kst_str()
    file_today = build_filename(date_str)
    file_yesterday = build_filename(ymd_yesterday)

    print("수집 시작:", MOBILE_URLS[0])
    items = fetch_by_http_mobile()
    if len(items) < 10:
        print("[Playwright 폴백 진입]")
        items = fetch_by_playwright()
    print("수집 완료:", len(items))
    if len(items) < 10:
        raise RuntimeError("제품 카드가 너무 적게 수집되었습니다. 셀렉터/렌더링 점검 필요")

    df_today = to_dataframe(items, date_str)
    os.makedirs("data", exist_ok=True)
    df_today.to_csv(os.path.join("data", file_today), index=False, encoding="utf-8-sig")
    print("로컬 저장:", file_today)

    # Google Drive 업로드 + 전일 CSV 로드
    df_prev = None
    folder = normalize_folder_id(os.getenv("GDRIVE_FOLDER_ID",""))
    if folder:
        try:
            svc = build_drive_service()
            drive_upload_csv(svc, folder, file_today, df_today)
            print("Google Drive 업로드 완료:", file_today)
            df_prev = drive_download_csv(svc, folder, file_yesterday)
            print("전일 CSV", "성공" if df_prev is not None else "미발견")
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
        print("[오류 발생]", e); traceback.print_exc()
        try:
            slack_post(f"*큐텐 재팬 뷰티 랭킹 자동화 실패*\n```\n{e}\n```")
        except: pass
        raise
