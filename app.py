# -*- coding: utf-8 -*-
"""
Qoo10 JP Beauty Bestsellers (g=2)

- 모바일 정적 HTML 우선, 부족 시 Playwright 폴백
- CSV: 큐텐재팬_뷰티_랭킹_YYYY-MM-DD.csv (KST)
- 비교 키: product_code 우선, 없으면 URL(정규화)

Slack 포맷:
 TOP 10 (변동 마커 + 번역, '브랜드 제품명 — ₩가격 (↓할인%)')
 📉 급하락 (5개만, OUT 포함, 번역)
 🔄 랭크 인&아웃 (개수만)

디버그 스냅샷:
 data/debug/qoo10_mobile_*.html, qoo10_desktop_*.html, qoo10_desktop_li_###.html, extract_log.csv
"""

import os, re, io, math, pytz, traceback, urllib.parse, base64, json
import datetime as dt
from dataclasses import dataclass, asdict
from typing import List, Dict, Optional, Tuple

import requests
import pandas as pd
from bs4 import BeautifulSoup

# -------------------- Config --------------------
KST = pytz.timezone("Asia/Seoul")
MOBILE_URLS = [
    "https://www.qoo10.jp/gmkt.inc/Mobile/Bestsellers/Default.aspx?group_code=2",
    "https://www.qoo10.jp/gmkt.inc/Mobile/Bestsellers/Default.aspx?__ar=Y&group_code=2",
    "https://www.qoo10.jp/gmkt.inc/mobile/bestsellers/default.aspx?group_code=2",
]
DESKTOP_URL = "https://www.qoo10.jp/gmkt.inc/Bestsellers/?g=2"
MAX_RANK = int(os.getenv("QOO10_MAX_RANK", "200"))
MAX_FALLING = 5
DEBUG_DIR = os.path.join("data", "debug")
os.makedirs(DEBUG_DIR, exist_ok=True)

def now_tag(): return dt.datetime.now(KST).strftime("%Y%m%d_%H%M%S")
def save_text(path, text): open(path, "w", encoding="utf-8").write(text or "")
def snapshot(name: str, text: str):
    path = os.path.join(DEBUG_DIR, f"{name}_{now_tag()}.html")
    save_text(path, text); print("[DEBUG] saved:", path)

# -------------------- Utils --------------------
def today_kst_str(): return dt.datetime.now(KST).strftime("%Y-%m-%d")
def yesterday_kst_str(): return (dt.datetime.now(KST) - dt.timedelta(days=1)).strftime("%Y-%m-%d")
def build_filename(d): return f"큐텐재팬_뷰티_랭킹_{d}.csv"
def clean_text(s): return re.sub(r"\s+", " ", (s or "")).strip()
def slack_escape(s): return (s or "").replace("&","&amp;").replace("<","&lt;").replace(">","&gt;")

BRACKETS_PAT = re.compile(r"(\[.*?\]|【.*?】|（.*?）|\(.*?\))")
NOISE_TOKENS_RE = re.compile(r"(TooltipBtn|クーポン発行|クーポン|ショップ券|送料無料|即日|OFF|％|%|レビュー|ポイント|GIFT付|公式|セット|選べる|個|本|枚|ml|g)\s*", re.I)
JP_CHAR_RE = re.compile(r"[一-龯ぁ-ゔァ-ヴー々〆ヵヶｦ-ﾟ]")
ROMAN_HEAD_RE = re.compile(r"^([A-Za-z0-9&.\-+/]+)")
KATAKANA_HEAD_RE = re.compile(r"^([ァ-ヴーｦ-ﾟ]+)")

def strip_brackets_for_slack(s: str) -> str:
    s = clean_text(BRACKETS_PAT.sub("", s or ""))
    return clean_text(NOISE_TOKENS_RE.sub(" ", s))

def score_title(s: str) -> int:
    if not s: return -1
    low = s.lower()
    if any(b in low for b in ("wish","shop","ショップ","store","公式","qoo10","ストア")): return -1
    s = strip_brackets_for_slack(s)
    j = len(JP_CHAR_RE.findall(s))
    return j * 3 + len(s)

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
    if m: pct = int(m.group(1))
    elif orig and sale:
        pct = max(0, int(math.floor((1 - sale / orig) * 100)))
    return sale, orig, pct

# -------------------- Models --------------------
@dataclass
class Product:
    rank: Optional[int]; brand: str; title: str
    price: Optional[int]; discount_percent: Optional[int]
    url: str; product_code: str = ""

# -------------------- Helpers --------------------
def extract_goods_code(url: str, block_text=""):
    if not url: return ""
    m = re.search(r"(?:[?&](?:goods?_?code|goodsno)=(\d+))", url, re.I)
    if m: return m.group(1)
    m2 = re.search(r"/(?:Item|item|goods)/(?:.*?/)?(\d+)(?:[/?#]|$)", url)
    if m2: return m2.group(1)
    m3 = re.search(r"商品番号\s*[:：]\s*(\d+)", block_text or "")
    return m3.group(1) if m3 else ""

def normalize_href(href: str) -> str:
    if href.startswith("//"): return "https:" + href
    if href.startswith("/"):  return "https://www.qoo10.jp" + href
    return href

def _brand_ok(b: str) -> bool:
    low = b.lower()
    if any(x in low for x in ["wish","shop","ショップ","store","公式","qoo10","ストア","楽天","amazon"]): return False
    if any(tok in b for tok in ["%","OFF","円"]): return False
    return True

# ---- 브랜드: 상점명 우선 ----
def extract_brand_from_li(li: BeautifulSoup, title: str) -> str:
    # (모바일) <span class="mShop">公式 O&ME</span>
    mshop = li.select_one(".mShop")
    if mshop:
        txt = clean_text(mshop.get_text(" ", strip=True)).replace("公式", "").strip()
        if txt and _brand_ok(txt): return txt

    # (배지) '公式' 텍스트 근처의 a
    for badge in li.find_all(string=re.compile(r"^\s*公式\s*$")):
        par = badge.parent if badge and getattr(badge, "parent", None) else None
        if not par: continue
        for sib in par.find_all_next(["a","span"], limit=6):
            if sib.name == "a":
                t = clean_text(sib.get_text(" ", strip=True))
                if t and _brand_ok(t): return t

    # (링크/클래스) 상점 링크
    for a in li.select("a[href*='/shop/'], a[href*='Shop'], a[class*='shop'], a[class*='seller'], a[class*='store']"):
        t = clean_text(a.get_text(" ", strip=True))
        if t and _brand_ok(t): return t

    # (영역) 상점명 영역
    for sel in [".seller", ".shop", ".store", ".brand", ".brand-name", ".brandName", ".name__brand", ".goods_brand", ".prd_brand"]:
        for el in li.select(sel):
            t = clean_text(el.get_text(" ", strip=True))
            if t and _brand_ok(t): return t

    # (폴백) 제목 선두 토큰
    t0 = clean_text(title or "")
    for m in (ROMAN_HEAD_RE.match(t0), KATAKANA_HEAD_RE.match(t0)):
        if m and _brand_ok(m.group(1)): return m.group(1)
    return ""

def choose_product_title(li: BeautifulSoup, a: BeautifulSoup) -> str:
    cands = []
    tit = a.get("title");  cands.append(tit or "")
    cands.append(a.get_text(" ", strip=True))
    for img in li.select("img[alt]"): cands.append(img.get("alt", ""))
    for sel in [".tit", ".title", ".name", ".prd_tit", ".prd_name", ".tb-tit", ".goods_name"]:
        for el in li.select(sel): cands.append(el.get_text(" ", strip=True))
    block = clean_text(li.get_text(" ", strip=True))
    for seg in re.split(r"[|•/▶▷›»·・\-–—]+", block): cands.append(seg)
    best, best_sc = "", -1
    for s in cands:
        s = clean_text(s); sc = score_title(s)
        if sc > best_sc: best_sc, best = sc, s
    return best or clean_text(a.get_text(" ", strip=True))

def is_ad_or_noise(name: str, url: str, code: str) -> bool:
    if not name: return True
    low = name.lower()
    if any(b in low for b in ["wish","shop","ショップ","store","公式","qoo10","ストア","楽天","amazon"]): return True
    if not code and re.search(r"/(ad|adclick|event|shop)/", url.lower()): return True
    return False

# -------------------- Parse --------------------
def _find_ranking_list(soup: BeautifulSoup):
    candidates = []
    for ul in soup.select("ul,ol"):
        cnt = len(ul.select("a[href*='Goods.aspx'], a[href*='/Item/'], a[href*='/item/'], a[href*='/goods']"))
        if cnt >= 10: candidates.append((cnt, ul))
    if not candidates: return None
    candidates.sort(key=lambda x: -x[0])
    return candidates[0][1]

def parse_mobile_html(html: str) -> List[Product]:
    soup = BeautifulSoup(html, "lxml")
    root = _find_ranking_list(soup) or soup
    items: List[Product] = []; seen=set()
    logs = []

    lis = root.select(":scope > li") or root.select("li")
    for li in lis:
        a = li.select_one("a[href*='Goods.aspx'], a[href*='/Item/'], a[href*='/item/'], a[href*='/goods']")
        if not a: continue
        href = normalize_href(a.get("href",""))
        block = clean_text(li.get_text(" ", strip=True))
        code = extract_goods_code(href, block)
        name = choose_product_title(li, a)
        if is_ad_or_noise(name, href, code): continue
        brand = extract_brand_from_li(li, name)

        key = code or href
        if key in seen: continue
        seen.add(key)
        sale, _, pct = compute_prices(block)
        prod = Product(len(items)+1, brand, name, sale, pct, href, code)
        items.append(prod)
        logs.append({**asdict(prod), "src":"mobile"})
        if len(items) >= MAX_RANK: break

    if logs:
        pd.DataFrame(logs).to_csv(os.path.join(DEBUG_DIR, "extract_log.csv"), index=False, encoding="utf-8-sig")
    return items

# -------------------- Fetch --------------------
def fetch_by_http_mobile()->List[Product]:
    headers = {
        "User-Agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123 Safari/537.36",
        "Accept-Language":"ja,en-US;q=0.9,en;q=0.8,ko;q=0.7",
        "Cache-Control":"no-cache","Pragma":"no-cache",
    }
    for url in MOBILE_URLS:
        try:
            r = requests.get(url, headers=headers, timeout=20); r.raise_for_status()
            snapshot("qoo10_mobile", r.text)  # 모바일 HTML 저장
            items = parse_mobile_html(r.text)
            if len(items) >= 10:
                print(f"[HTTP 모바일] {url} → {len(items)}개")
                return items
        except Exception as e:
            print("[HTTP 모바일 실패]", url, e)
    return []

def fetch_by_playwright() -> List[Product]:
    from playwright.sync_api import sync_playwright
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True, args=["--no-sandbox","--disable-dev-shm-usage","--disable-blink-features=AutomationControlled"])
        context = browser.new_context(
            viewport={"width":1366,"height":900}, locale="ja-JP", timezone_id="Asia/Tokyo",
            user_agent=("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123 Safari/537.36"),
            extra_http_headers={"Accept-Language":"ja,en-US;q=0.9,en;q=0.8,ko;q=0.7"},
        )
        context.add_init_script("Object.defineProperty(navigator,'webdriver',{get:()=>undefined});")
        page = context.new_page()
        page.goto(DESKTOP_URL, wait_until="domcontentloaded", timeout=60_000)
        try: page.wait_for_load_state("networkidle", timeout=25_000)
        except: pass

        html = page.content()
        snapshot("qoo10_desktop", html)

        # li 20개 outerHTML 저장
        li_htmls = page.evaluate("""
            () => {
              function findRoot() {
                const lists = Array.from(document.querySelectorAll('ul,ol'));
                let best=null, bestCnt=0;
                for (const el of lists) {
                  const cnt = el.querySelectorAll("a[href*='Goods.aspx'], a[href*='/Item/'], a[href*='/item/'], a[href*='/goods']").length;
                  if (cnt >= 10 && cnt > bestCnt) { best = el; bestCnt = cnt; }
                }
                return best || document;
              }
              const root = findRoot();
              const lis = (root.querySelectorAll(':scope > li').length ? root.querySelectorAll(':scope > li') : root.querySelectorAll('li'));
              return Array.from(lis).slice(0, 20).map(li => li.outerHTML);
            }
        """)
        for i, outer in enumerate(li_htmls, start=1):
            save_text(os.path.join(DEBUG_DIR, f"qoo10_desktop_li_{i:03}.html"), outer)

        rows = page.evaluate("""
            () => {
              function findRoot() {
                const lists = Array.from(document.querySelectorAll('ul,ol'));
                let best=null, bestCnt=0;
                for (const el of lists) {
                  const cnt = el.querySelectorAll("a[href*='Goods.aspx'], a[href*='/Item/'], a[href*='/item/'], a[href*='/goods']").length;
                  if (cnt >= 10 && cnt > bestCnt) { best = el; bestCnt = cnt; }
                }
                return best || document;
              }
              const root = findRoot();
              const list = root.querySelectorAll(':scope > li').length ? root.querySelectorAll(':scope > li') : root.querySelectorAll('li');
              const out = []; const seen = new Set(); let rank = 1;
              for (const li of list) {
                const a = li.querySelector("a[href*='Goods.aspx'], a[href*='/Item/'], a[href*='/item/'], a[href*='/goods']");
                if (!a) continue;
                const href = a.getAttribute('href') || '';
                const name = (a.textContent || '').replace(/\\s+/g,' ').trim();
                const block = (li.innerText || '').replace(/\\s+/g,' ').trim();
                const key = href + '|' + name;
                if (seen.has(key)) continue; seen.add(key);
                out.push({href, name, block, html: li.outerHTML, rank: rank++});
                if (out.length >= 500) break;
              }
              return out;
            }
        """)
        context.close(); browser.close()

    items: List[Product] = []; seen=set(); logs=[]
    for row in rows:
        href = normalize_href(row.get("href",""))
        block = clean_text(row.get("block",""))
        name_raw = clean_text(row.get("name",""))
        name = strip_brackets_for_slack(name_raw)
        code = extract_goods_code(href, block)

        brand = ""
        try:
            sub = BeautifulSoup(row.get("html",""), "lxml")
            brand = extract_brand_from_li(sub, name)
        except Exception:
            pass
        if not brand:
            for rgx in (ROMAN_HEAD_RE, KATAKANA_HEAD_RE):
                m = rgx.match(name)
                if m and _brand_ok(m.group(1)): brand = m.group(1); break

        if score_title(name) < 0:
            segs = re.split(r"[|•/▶▷›»·・\\-–—]+", block)
            best, best_sc = "", -1
            for s in segs:
                sc = score_title(s)
                if sc > best_sc: best_sc, best = sc, clean_text(s)
            if best:
                name = best
                if not brand:
                    for rgx in (ROMAN_HEAD_RE, KATAKANA_HEAD_RE):
                        m = rgx.match(name)
                        if m and _brand_ok(m.group(1)): brand = m.group(1); break

        if is_ad_or_noise(name, href, code): continue
        key = code or href
        if key in seen: continue
        seen.add(key)
        sale, _, pct = compute_prices(block)
        prod = Product(len(items)+1, brand, name, sale, pct, href, code)
        items.append(prod); logs.append({**asdict(prod), "src":"desktop"})
        if len(items) >= MAX_RANK: break

    if logs:
        p = os.path.join(DEBUG_DIR, "extract_log.csv")
        if os.path.exists(p):
            old = pd.read_csv(p); logs = pd.concat([old, pd.DataFrame(logs)], ignore_index=True)
        else:
            logs = pd.DataFrame(logs)
        logs.to_csv(p, index=False, encoding="utf-8-sig")

    print(f"[Playwright] {len(items)}개")
    return items

def fetch_products():
    items = fetch_by_http_mobile()
    if len(items) >= 10: return items
    print("[Playwright 폴백]"); return fetch_by_playwright()

# -------------------- Slack --------------------
def fmt_currency(v):
    try: return f"₩{int(v):,}"
    except: return "₩0"

def slack_post(text):
    url = os.getenv("SLACK_WEBHOOK_URL")
    if not url: print(text); return
    try:
        requests.post(url, json={"text": text, "unfurl_links": False, "unfurl_media": False}, timeout=20)
    except Exception as e:
        print("[Slack 전송 실패]", e); print(text)

# -------------------- Translate --------------------
def translate_ja_to_ko_batch(lines: List[str])->List[str]:
    if not (os.getenv("SLACK_TRANSLATE_JA2KO","0").lower() in ("1","true","yes")): return ["" for _ in lines]
    try:
        from googletrans import Translator
        tr = Translator(service_urls=['translate.googleapis.com'])
        res = tr.translate(lines, src="ja", dest="ko")
        return [r.text for r in (res if isinstance(res, list) else [res])]
    except Exception:
        try:
            from deep_translator import GoogleTranslator as DT
            gt = DT(source='ja', target='ko'); return [gt.translate(t) if t else "" for t in lines]
        except Exception: return [""]*len(lines)

# -------------------- DataFrame --------------------
def to_dataframe(products: List[Product], date_str: str)->pd.DataFrame:
    return pd.DataFrame([{
        "date": date_str, "rank": p.rank, "brand": p.brand, "product_name": p.title,
        "price": p.price, "discount_percent": p.discount_percent, "url": p.url, "product_code": p.product_code
    } for p in products])

# -------------------- Key normalize --------------------
def _norm_product_code(v)->str:
    if pd.isna(v): return ""
    try:
        f = float(str(v))
        if f.is_integer(): return str(int(f))
    except: pass
    return str(v).strip()

def _norm_url(u:str)->str:
    if not u: return ""
    u = u.strip()
    if u.startswith("//"): u = "https:" + u
    if u.startswith("/"):  u = "https://www.qoo10.jp" + u
    try:
        pr = urllib.parse.urlparse(u); q = urllib.parse.parse_qsl(pr.query)
        BAD = {"utm_source","utm_medium","utm_campaign","utm_term","utm_content","cid","g"}
        q = [(k,v) for k,v in q if k not in BAD]
        return urllib.parse.urlunparse(pr._replace(query=urllib.parse.urlencode(q))).lower()
    except:
        return u.lower()

def keyify(df):
    if df is None or not len(df): return None
    df = df.copy()
    df["product_code"] = df["product_code"].apply(_norm_product_code)
    df["url"] = df["url"].apply(_norm_url)
    df["key"] = df.apply(lambda x: x["product_code"] if x["product_code"] else x["url"], axis=1)
    df.set_index("key", inplace=True)
    return df

# -------------------- Sections --------------------
def build_sections(df_today:pd.DataFrame, df_prev:Optional[pd.DataFrame])->Dict[str,List[str]]:
    S={"top10":[], "falling":[], "outs":[], "inout_count":0}

    def plain_name(r):
        nm = strip_brackets_for_slack(clean_text(r.get("product_name","")))
        br = clean_text(r.get("brand",""))
        if br and not nm.lower().startswith(br.lower()): nm = f"{br} {nm}"
        return nm

    def full_link(r): return f"<{r['url']}|{slack_escape(plain_name(r))}>"

    def interleave_ko(lines: List[str], jp_texts: List[str]) -> List[str]:
        kos = translate_ja_to_ko_batch(jp_texts); out=[]
        for i,ln in enumerate(lines):
            out.append(ln)
            if kos and i < len(kos) and kos[i]: out.append(kos[i])
        return out

    prev_all = keyify(df_prev) if (df_prev is not None and len(df_prev)) else None
    jp, lines = [], []
    for _, r in df_today.dropna(subset=["rank"]).sort_values("rank").head(10).iterrows():
        jp.append(plain_name(r)); marker = ""
        if prev_all is not None:
            k = (_norm_product_code(r["product_code"]) or _norm_url(r["url"]))
            if k in prev_all.index and pd.notnull(prev_all.loc[k,"rank"]):
                pr, cr = int(prev_all.loc[k,"rank"]), int(r["rank"]); d = pr - cr
                marker = f"(↑{d}) " if d>0 else (f"(↓{abs(d)}) " if d<0 else "")
            else: marker = "(New) "
        tail = f" (↓{int(r['discount_percent'])}%)" if pd.notnull(r["discount_percent"]) else ""
        lines.append(f"{int(r['rank'])}. {marker}{full_link(r)} — {fmt_currency(r['price'])}{tail}")
    S["top10"] = interleave_ko(lines, jp)

    if prev_all is None: return S

    df_t = keyify(df_today); t30 = df_t[df_t["rank"]<=30]; p30 = prev_all[prev_all["rank"]<=30]
    common = set(t30.index)&set(p30.index); out = set(p30.index)-set(t30.index)

    movers=[]
    for k in common:
        pr, cr = int(p30.loc[k,"rank"]), int(t30.loc[k,"rank"])
        drop = cr - pr
        if drop > 0:
            row = t30.loc[k]
            movers.append((drop, cr, pr, f"- {full_link(row)} {pr}위 → {cr}위 (↓{drop})", plain_name(row)))
    movers.sort(key=lambda x:(-x[0], x[1], x[2], x[4]))
    chosen_lines, chosen_jp = [], []
    for _,_,_,txt,jpn in movers:
        if len(chosen_lines) >= MAX_FALLING: break
        chosen_lines.append(txt); chosen_jp.append(jpn)
    if len(chosen_lines) < MAX_FALLING:
        outs_sorted = sorted(list(out), key=lambda k: int(p30.loc[k,"rank"]))
        for k in outs_sorted:
            if len(chosen_lines) >= MAX_FALLING: break
            row = p30.loc[k]; txt = f"- {full_link(row)} {int(row['rank'])}위 → OUT"
            chosen_lines.append(txt); chosen_jp.append(plain_name(row))
    S["falling"] = interleave_ko(chosen_lines, chosen_jp)
    S["outs"] = []
    S["inout_count"] = len(set(t30.index)-set(p30.index)) + len(out)
    return S

# -------------------- Slack message --------------------
def build_slack_message(date,S):
    lines=[f"*🛒 큐텐 재팬 뷰티 랭킹 — {date}*","","*TOP 10*"]
    lines+=S["top10"]; lines+=["","*📉 급하락*"]; lines+=S["falling"] or ["- 해당 없음"]
    lines+=["","*🔄 랭크 인&아웃*", f"{S['inout_count']}개의 제품이 인&아웃 되었습니다."]
    return "\n".join(lines)

# -------------------- Google Drive --------------------
def _drive_service_service_account():
    try:
        from googleapiclient.discovery import build
        from google.oauth2.service_account import Credentials
        raw=os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON","").strip()
        if not raw: return None
        info=json.loads(base64.b64decode(raw).decode("utf-8")) if not raw.lstrip().startswith("{") else json.loads(raw)
        creds=Credentials.from_service_account_info(info,scopes=["https://www.googleapis.com/auth/drive"])
        svc=build("drive","v3",credentials=creds,cache_discovery=False)
        who=svc.about().get(fields="user(displayName,emailAddress)").execute().get("user",{})
        print(f"[Drive-SA] 로그인: {who.get('displayName')} <{who.get('emailAddress')}>"); return svc
    except Exception as e:
        print("[Drive-SA 실패]", e); return None

def _drive_service_oauth():
    try:
        from googleapiclient.discovery import build
        from google.oauth2.credentials import Credentials
        cid,sec,rt=os.getenv("GOOGLE_CLIENT_ID"),os.getenv("GOOGLE_CLIENT_SECRET"),os.getenv("GOOGLE_REFRESH_TOKEN")
        if not (cid and sec and rt): return None
        creds=Credentials(None,refresh_token=rt,token_uri="https://oauth2.googleapis.com/token",client_id=cid,client_secret=sec)
        svc=build("drive","v3",credentials=creds,cache_discovery=False)
        who=svc.about().get(fields="user(displayName,emailAddress)").execute().get("user",{})
        print(f"[Drive-OAuth] 로그인: {who.get('displayName')} <{who.get('emailAddress')}>"); return svc
    except Exception as e:
        print("[Drive-OAuth 실패]", e); return None

def build_drive_service():
    return _drive_service_service_account() or _drive_service_oauth()

def drive_upload_csv(svc,folder_id,name,df):
    from googleapiclient.http import MediaIoBaseUpload
    buf=io.BytesIO(); df.to_csv(buf,index=False,encoding="utf-8-sig"); buf.seek(0)
    media=MediaIoBaseUpload(buf,mimetype="text/csv",resumable=False)
    q=f"name='{name}' and '{folder_id}' in parents and trashed=false"
    res=svc.files().list(q=q,fields="files(id)",supportsAllDrives=True).execute()
    if res.get("files"):
        fid=res["files"][0]["id"]
        svc.files().update(fileId=fid,media_body=media,supportsAllDrives=True).execute()
        print("[Drive] 업데이트:", name); return fid
    meta={"name":name,"parents":[folder_id],"mimeType":"text/csv"}
    fid=svc.files().create(body=meta,media_body=media,fields="id",supportsAllDrives=True).execute()["id"]
    print("[Drive] 업로드:", name); return fid

def drive_download_csv(svc,folder_id,pattern_name):
    from googleapiclient.http import MediaIoBaseDownload
    base=pattern_name.replace(".csv","")
    q=f"name contains '{base}' and '{folder_id}' in parents and trashed=false"
    res=svc.files().list(q=q,fields="files(id,name,modifiedTime)",orderBy="modifiedTime desc",
                         supportsAllDrives=True,includeItemsFromAllDrives=True).execute()
    files=res.get("files",[])
    if not files: print("[Drive] 전일 파일 미발견:", base); return None
    fid=files[0]["id"]; req=svc.files().get_media(fileId=fid,supportsAllDrives=True)
    fh=io.BytesIO(); dl=MediaIoBaseDownload(fh,req); done=False
    while not done: _,done=dl.next_chunk()
    fh.seek(0); print("[Drive] 다운로드:", files[0]["name"])
    return pd.read_csv(fh)

# -------------------- Main --------------------
def main():
    date=today_kst_str(); today_file=build_filename(date); yest_file=build_filename(yesterday_kst_str())
    print("[INFO] 수집 시작")
    items=fetch_products()
    if len(items)<10:
        print("[Playwright 폴백]"); items=fetch_by_playwright()
    if len(items)<10: raise RuntimeError("제품 카드 수가 너무 적습니다.")

    df_today=to_dataframe(items,date)
    os.makedirs("data",exist_ok=True)
    df_today.to_csv(os.path.join("data",today_file),index=False,encoding="utf-8-sig")
    print("[INFO] CSV 저장:", today_file)

    df_prev=None
    try:
        svc=build_drive_service(); folder=os.getenv("GDRIVE_FOLDER_ID","").strip()
        if svc and folder:
            drive_upload_csv(svc,folder,today_file,df_today)
            df_prev=drive_download_csv(svc,folder,yest_file)
        if df_prev is None:
            local_prev=os.path.join("data",yest_file)
            if os.path.exists(local_prev):
                df_prev=pd.read_csv(local_prev); print("[INFO] 로컬 전일 CSV 사용:", yest_file)
    except Exception as e:
        print("[WARN] 전일 로딩 실패]:", e)

    S=build_sections(df_today,df_prev)
    msg=build_slack_message(date,S)
    slack_post(msg)
    print("[INFO] Slack 전송 완료")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print("[오류]", e); traceback.print_exc()
        try: slack_post(f"*큐텐 재팬 뷰티 랭킹 자동화 실패*\n```\n{e}\n```")
        except: pass
        raise
