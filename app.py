# -*- coding: utf-8 -*-
"""
Qoo10 JP Beauty Bestsellers (g=2)
- 모바일 정적 HTML 우선, 부족 시 Playwright 폴백(데스크톱)
- CSV 파일명: 큐텐재팬_뷰티_랭킹_YYYY-MM-DD.csv (KST)
- 비교 키: product_code 우선, 없으면 URL

Slack 포맷(간소화):
  1) TOP 10
     "순위.(변동) {브랜드 제품명} — ₩가격 (↓할인%)"
      - 변동: (↑6)/(↓12)/(New)  *변동없음은 표기 생략*
      - 바로 아래 줄에 한국어 번역(옵션: SLACK_TRANSLATE_JA2KO=1)
  2) 📉 급하락: Top30 기준 하락폭>0 상위 5개, "전일위 → 당일위 (↓폭)" + 번역 한 줄
  3) OUT: 전일 Top30 → 당일 Top30 밖, 최대 10개, 번역 없음
  4) 🔄 랭크 인&아웃: 인(차트인)+아웃(랭크아웃) 개수만 표기

환경변수:
- SLACK_WEBHOOK_URL (필수)
- SLACK_TRANSLATE_JA2KO=1  (번역 활성화)
- QOO10_MAX_RANK (기본 200)
- QOO10_MAX_OUT  (기본 10)
"""

import os, re, io, math, pytz, traceback
import datetime as dt
from dataclasses import dataclass
from typing import List, Dict, Optional, Tuple

import requests
import pandas as pd
from bs4 import BeautifulSoup

# ---------- Config ----------
KST = pytz.timezone("Asia/Seoul")
MOBILE_URLS = [
    "https://www.qoo10.jp/gmkt.inc/Mobile/Bestsellers/Default.aspx?group_code=2",
    "https://www.qoo10.jp/gmkt.inc/Mobile/Bestsellers/Default.aspx?__ar=Y&group_code=2",
    "https://www.qoo10.jp/gmkt.inc/mobile/bestsellers/default.aspx?group_code=2",
]
DESKTOP_URL = "https://www.qoo10.jp/gmkt.inc/Bestsellers/?g=2"
MAX_RANK = int(os.getenv("QOO10_MAX_RANK", "200"))
MAX_FALLING = 5
MAX_OUT = int(os.getenv("QOO10_MAX_OUT", "10"))

# ---------- time/utils ----------
def now_kst(): return dt.datetime.now(KST)
def today_kst_str(): return now_kst().strftime("%Y-%m-%d")
def yesterday_kst_str(): return (now_kst() - dt.timedelta(days=1)).strftime("%Y-%m-%d")
def build_filename(d): return f"큐텐재팬_뷰티_랭킹_{d}.csv"
def clean_text(s): return re.sub(r"\s+", " ", (s or "")).strip()
def slack_escape(s): return (s or "").replace("&","&amp;").replace("<","&lt;").replace(">","&gt;")

# ---------- 괄호 제거(슬랙 표시 전용) ----------
BRACKETS_PAT = re.compile(r"(\[.*?\]|【.*?】|（.*?）|\(.*?\))")
def strip_brackets_for_slack(s: str) -> str:
    if not s: return ""
    return clean_text(BRACKETS_PAT.sub("", s))

# ---------- price/discount ----------
YEN_AMOUNT_RE = re.compile(r"(?:¥|)(\d{1,3}(?:,\d{3})+|\d+)\s*円")
PCT_RE = re.compile(r"(\d+)\s*% ?OFF", re.I)

def parse_jpy_amounts(text: str) -> List[int]:
    return [int(m.group(1).replace(",", "")) for m in YEN_AMOUNT_RE.finditer(text or "")]

def compute_prices(block_text: str) -> Tuple[Optional[int], Optional[int], Optional[int]]:
    amounts = parse_jpy_amounts(block_text)
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

# ---------- model ----------
@dataclass
class Product:
    rank: Optional[int]
    brand: str
    title: str
    price: Optional[int]
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

        # URL normalize
        if href.startswith("//"): href = "https:" + href
        elif href.startswith("/"): href = "https://www.qoo10.jp" + href

        code = extract_goods_code(href, block_text)
        key = code or href
        if key in seen: continue
        seen.add(key)

        name = clean_text(a.get_text(" ", strip=True))
        # 브랜드는 명확치 않아 저장은 title에만, 슬랙표시는 제목만 사용
        sale, _, pct = compute_prices(block_text)

        items.append(Product(
            rank=len(items)+1, brand="", title=name,
            price=sale, discount_percent=pct,
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
              const rows = []; const seen = new Set();
              for (const a of as) {
                const href = a.getAttribute('href') || '';
                const name = (a.textContent || '').replace(/\\s+/g,' ').trim();
                const li = a.closest('li') || a.closest('div');
                if (!href || !name || !li) continue;
                const block = (li.innerText || '').replace(/\\s+/g,' ').trim();
                const key = href + '|' + name;
                if (seen.has(key)) continue; seen.add(key);
                rows.push({href, name, block});
              }
              return rows.slice(0, 500);
            }
        """)
        context.close(); browser.close()

    items: List[Product] = []
    seen = set()
    for row in data:
        href = row.get("href","")
        name = clean_text(row.get("name",""))
        block_text = clean_text(row.get("block",""))

        if href.startswith("//"): href = "https:" + href
        elif href.startswith("/"): href = "https://www.qoo10.jp" + href

        code = extract_goods_code(href, block_text)
        key = code or href
        if key in seen: continue
        seen.add(key)

        sale, _, pct = compute_prices(block_text)

        items.append(Product(
            rank=len(items)+1, brand="", title=name,
            price=sale, discount_percent=pct,
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

# ---------- Slack ----------
def fmt_currency_krw_like(v) -> str:
    try: return f"₩{int(round(float(v))):,}"
    except: return "₩0"

def slack_post(text: str):
    url = os.getenv("SLACK_WEBHOOK_URL")
    if not url:
        print("[경고] SLACK_WEBHOOK_URL 미설정 → 콘솔 출력\n", text); return
    payload = {"text": text, "unfurl_links": False, "unfurl_media": False}
    r = requests.post(url, json=payload, timeout=20)
    if r.status_code >= 300:
        print("[Slack 실패]", r.status_code, r.text)

# ---------- Translate ----------
def translate_ja_to_ko_batch(lines: List[str]) -> List[str]:
    flag = os.getenv("SLACK_TRANSLATE_JA2KO", "0").lower() in ("1","true","yes")
    texts = [(l or "").strip() for l in lines]
    if not flag or not texts:
        return ["" for _ in texts]
    # 간단 번역 파이프라인 (googletrans → deep_translator 폴백)
    try:
        from googletrans import Translator
        tr = Translator(service_urls=['translate.googleapis.com'])
        res = tr.translate(texts, src="ja", dest="ko")
        return [r.text for r in (res if isinstance(res, list) else [res])]
    except Exception:
        try:
            from deep_translator import GoogleTranslator as DT
            gt = DT(source='ja', target='ko')
            return [gt.translate(t) if t else "" for t in texts]
        except Exception:
            return ["" for _ in texts]

# ---------- DataFrame ----------
def to_dataframe(products: List[Product], date_str: str) -> pd.DataFrame:
    """CSV에는 원문(괄호 포함) 저장. 반드시 rank 포함."""
    return pd.DataFrame([{
        "date": date_str,
        "rank": p.rank,                         # ✅ rank 포함 (이전 오류 원인)
        "brand": clean_text(p.brand),           # 현재는 빈 문자열일 수 있음
        "product_name": clean_text(p.title),    # 괄호 포함 원문
        "price": p.price,
        "discount_percent": p.discount_percent,
        "url": p.url,
        "product_code": p.product_code,
    } for p in products])

# ---------- Sections ----------
def build_sections(df_today: pd.DataFrame, df_prev: Optional[pd.DataFrame]) -> Dict[str, List[str]]:
    S = {"top10": [], "falling": [], "outs": [], "inout_count": 0}

    # 슬랙 표시용 이름(여기에서만 괄호 제거 + 브랜드 결합)
    def plain_name(row):
        nm = strip_brackets_for_slack(clean_text(row.get("product_name","")))
        br = clean_text(row.get("brand",""))
        if br and not nm.lower().startswith(br.lower()):
            nm = f"{br} {nm}"
        return nm.strip() or "상품"

    def link_name(row):
        return f"<{row['url']}|{slack_escape(plain_name(row))}>"

    # prev 매핑
    def keyify(df):
        df = df.copy()
        df["key"] = df.apply(lambda x: x["product_code"] if (pd.notnull(x.get("product_code")) and str(x.get("product_code")).strip()) else x["url"], axis=1)
        df.set_index("key", inplace=True)
        return df

    prev_all = keyify(df_prev) if (df_prev is not None and len(df_prev)) else None

    # ---------- TOP 10 (변동 마커 + 번역)
    jp_for_tr, top10_lines = [], []
    for _, r in df_today.dropna(subset=["rank"]).sort_values("rank").head(10).iterrows():
        nm = plain_name(r)
        jp_for_tr.append(nm)

        marker = ""
        if prev_all is not None:
            key = r["product_code"] if (pd.notnull(r.get("product_code")) and str(r.get("product_code")).strip()) else r["url"]
            if key in prev_all.index and pd.notnull(prev_all.loc[key, "rank"]):
                pr, cr = int(prev_all.loc[key, "rank"]), int(r["rank"])
                delta = pr - cr
                if   delta > 0: marker = f"(↑{delta}) "
                elif delta < 0: marker = f"(↓{abs(delta)}) "
                else: marker = ""
            else:
                marker = "(New) "

        tail = f" (↓{int(r['discount_percent'])}%)" if pd.notnull(r.get("discount_percent")) else ""
        top10_lines.append(f"{int(r['rank'])}. {marker}{link_name(r)} — {fmt_currency_krw_like(r['price'])}{tail}")

    kos = translate_ja_to_ko_batch(jp_for_tr)
    S["top10"] = [f"{a}\n{b}" if b else a for a, b in zip(top10_lines, kos)]

    # 전일 없으면 급하락/OUT 계산 불가
    if prev_all is None:
        return S

    # ---------- 급하락 (Top30 교집합, 하락폭>0, 정렬 후 상위 5개, 번역 포함)
    df_t = keyify(df_today)
    t30 = df_t[(df_t["rank"].notna()) & (df_t["rank"] <= 30)].copy()
    p30 = prev_all[(prev_all["rank"].notna()) & (prev_all["rank"] <= 30)].copy()
    common = set(t30.index) & set(p30.index)
    out = set(p30.index) - set(t30.index)

    falling_pack = []
    for k in common:
        pr, cr = int(p30.loc[k,"rank"]), int(t30.loc[k,"rank"])
        drop = cr - pr
        if drop > 0:
            falling_pack.append((drop, cr, pr, t30.loc[k]))
    # 하락폭 내림차순 → 오늘 순위 → 전일 순위 → 제품명
    falling_pack.sort(key=lambda x: (-x[0], x[1], x[2], clean_text(str(x[3].get("product_name","")))))

    falling_lines, falling_jp = [], []
    for drop, cr, pr, row in falling_pack[:MAX_FALLING]:
        falling_lines.append(f"- {link_name(row)} {pr}위 → {cr}위 (↓{drop})")
        falling_jp.append(plain_name(row))
    kos_fall = translate_ja_to_ko_batch(falling_jp)
    S["falling"] = [f"{a}\n{b}" if b else a for a, b in zip(falling_lines, kos_fall)]

    # ---------- OUT (전일 Top30 → 당일 Top30 밖, 번역 없음, 전일 순위 오름차순, 최대 MAX_OUT)
    outs_pack = []
    for k in sorted(list(out)):
        pr = int(p30.loc[k,"rank"])
        outs_pack.append((pr, f"- {link_name(p30.loc[k])} {pr}위 → OUT"))
    outs_pack.sort(key=lambda x: x[0])
    S["outs"] = [e[1] for e in outs_pack[:MAX_OUT]]

    # ---------- 인&아웃 카운트 (Top30 기준)
    new_in = set(t30.index) - set(p30.index)
    S["inout_count"] = len(new_in) + len(out)
    return S

# ---------- Message ----------
def build_slack_message(date_str: str, S: Dict[str, List[str]]) -> str:
    lines: List[str] = []
    lines.append(f"*🛒 큐텐 재팬 뷰티 랭킹 — {date_str}*")
    lines.append("")
    lines.append("*TOP 10*")
    lines.extend(S.get("top10") or ["- 데이터 없음"])
    lines.append("")
    lines.append("*📉 급하락*")
    lines.extend(S.get("falling") or ["- 해당 없음"])
    if S.get("outs"): lines.extend(S["outs"])
    lines.append("")
    lines.append("*🔄 랭크 인&아웃*")
    lines.append(f"{S.get('inout_count', 0)}개의 제품이 인&아웃 되었습니다.")
    return "\n".join(lines)

# ---------- main ----------
def main():
    date_str = today_kst_str()
    y_file = build_filename(yesterday_kst_str())
    t_file = build_filename(date_str)

    print("[INFO] 수집 시작")
    items = fetch_products()
    if len(items) < 10:
        print("[Playwright 폴백 진입]")
        items = fetch_by_playwright()
    if len(items) < 10:
        raise RuntimeError("제품 카드 수가 너무 적습니다. 셀렉터/렌더링 점검 필요")

    df_today = to_dataframe(items, date_str)
    os.makedirs("data", exist_ok=True)
    df_today.to_csv(os.path.join("data", t_file), index=False, encoding="utf-8-sig")
    print("[INFO] CSV 저장:", t_file)

    df_prev = None
    try:
        prev_path = os.path.join("data", y_file)
        if os.path.exists(prev_path):
            df_prev = pd.read_csv(prev_path)
            print("[INFO] 전일 CSV 로드 성공:", y_file)
        else:
            print("[INFO] 전일 CSV 없음:", y_file)
    except Exception as e:
        print("[WARN] 전일 CSV 로드 실패:", e)

    S = build_sections(df_today, df_prev)
    msg = build_slack_message(date_str, S)
    slack_post(msg)
    print("[INFO] Slack 전송 완료")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print("[오류]", e); traceback.print_exc()
        try:
            slack_post(f"*큐텐 재팬 뷰티 랭킹 자동화 실패*\n```\n{e}\n```")
        except: pass
        raise
