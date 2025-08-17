# -*- coding: utf-8 -*-
"""
Qoo10 JP Beauty Bestsellers (g=2)
- 모바일 정적 → 실패 시 Playwright 폴백
- CSV: 큐텐재팬_뷰티_랭킹_YYYY-MM-DD.csv
- 비교 키: product_code (없으면 URL)

Slack 포맷:
  TOP 10: "순위.(변동) 상품명 — ₩가격 (↓할인%)"
          (변동 없음: 표시 X, New: (New))
          한 줄 아래 한국어 번역
  📉 급하락: 최대 5개 "prev위 → curr위 (↓폭)" + 한국어 번역
  OUT: 전일 Top30 → 오늘 OUT (번역 없음, 최대 10개)
  🔄 랭크 인&아웃: 개수만
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
]
DESKTOP_URL = "https://www.qoo10.jp/gmkt.inc/Bestsellers/?g=2"
MAX_RANK = int(os.getenv("QOO10_MAX_RANK", "200"))
MAX_FALLING = 5
MAX_OUT = 10

# ---------- time/utils ----------
def now_kst(): return dt.datetime.now(KST)
def today_kst_str(): return now_kst().strftime("%Y-%m-%d")
def yesterday_kst_str(): return (now_kst() - dt.timedelta(days=1)).strftime("%Y-%m-%d")
def build_filename(d): return f"큐텐재팬_뷰티_랭킹_{d}.csv"
def clean_text(s): return re.sub(r"\s+", " ", (s or "")).strip()
def slack_escape(s): return (s or "").replace("&","&amp;").replace("<","&lt;").replace(">","&gt;")

# ---------- 괄호 제거 ----------
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
def extract_goods_code(url: str) -> str:
    if not url: return ""
    m = GOODS_CODE_RE.search(url)
    if m: return m.group(1)
    m2 = ITEM_PATH_RE.search(url)
    return m2.group(1) if m2 else ""

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

# ---------- parse ----------
def parse_mobile_html(html: str) -> List[Product]:
    soup = BeautifulSoup(html, "lxml")
    anchors = soup.select("a[href*='Goods.aspx'], a[href*='/Item/']")
    items: List[Product] = []
    seen = set()
    for a in anchors:
        href = a.get("href", "")
        if not href: continue
        if href.startswith("//"): href = "https:" + href
        elif href.startswith("/"): href = "https://www.qoo10.jp" + href
        code = extract_goods_code(href)
        if code in seen: continue
        seen.add(code)
        name = clean_text(a.get_text(" ", strip=True))
        block_text = clean_text(a.find_parent("li").get_text(" ", strip=True)) if a.find_parent("li") else name
        sale, orig, pct = compute_prices(block_text)
        items.append(Product(rank=len(items)+1, brand="", title=name,
                             price=sale, discount_percent=pct,
                             url=href, product_code=code))
        if len(items) >= MAX_RANK: break
    return items

# ---------- fetch ----------
def fetch_products() -> List[Product]:
    try:
        r = requests.get(MOBILE_URLS[0], headers={"User-Agent":"Mozilla/5.0"}, timeout=20)
        r.raise_for_status()
        return parse_mobile_html(r.text)
    except:
        return []

# ---------- Slack translate ----------
def slack_post(text: str):
    url = os.getenv("SLACK_WEBHOOK_URL")
    if not url: 
        print(text); return
    requests.post(url, json={"text":text,"unfurl_links":False,"unfurl_media":False}, timeout=20)

def translate_ja_to_ko_batch(lines: List[str]) -> List[str]:
    try:
        from googletrans import Translator
        tr = Translator(service_urls=['translate.googleapis.com'])
        res = tr.translate(lines, src="ja", dest="ko")
        return [r.text for r in (res if isinstance(res, list) else [res])]
    except:
        return ["" for _ in lines]

# ---------- build df ----------
def to_dataframe(products: List[Product], date_str: str) -> pd.DataFrame:
    return pd.DataFrame([{
        "date": date_str,
        "rank": p.rank,
        "product_name": p.title,
        "price": p.price,
        "discount_percent": p.discount_percent,
        "url": p.url,
        "product_code": p.product_code,
    } for p in products])

# ---------- build sections ----------
def build_sections(df_today: pd.DataFrame, df_prev: Optional[pd.DataFrame]) -> Dict[str,List[str]]:
    S = {"top10": [], "falling": [], "outs": [], "inout_count": 0}

    def plain_name(row):
        return strip_brackets_for_slack(clean_text(row.get("product_name","")))

    def link_name(row):
        return f"<{row['url']}|{slack_escape(plain_name(row))}>"

    # prev mapping
    def keyify(df):
        df = df.copy()
        df["key"] = df["product_code"].fillna(df["url"])
        df.set_index("key", inplace=True)
        return df
    prev_all = keyify(df_prev) if df_prev is not None else None

    # ---------- TOP10
    jp_for_tr, top10_lines = [], []
    for _, r in df_today.sort_values("rank").head(10).iterrows():
        nm = plain_name(r)
        jp_for_tr.append(nm)
        marker = ""
        if prev_all is not None:
            key = r["product_code"] or r["url"]
            if key in prev_all.index and pd.notnull(prev_all.loc[key,"rank"]):
                pr, cr = int(prev_all.loc[key,"rank"]), int(r["rank"])
                delta = pr - cr
                if delta > 0: marker = f"(↑{delta}) "
                elif delta < 0: marker = f"(↓{abs(delta)}) "
            else:
                marker = "(New) "
        tail = f" (↓{int(r['discount_percent'])}%)" if pd.notnull(r["discount_percent"]) else ""
        top10_lines.append(f"{int(r['rank'])}. {marker}{link_name(r)} — ₩{r['price']:,}{tail}")
    kos = translate_ja_to_ko_batch(jp_for_tr)
    S["top10"] = [f"{line}\n{ko}" if ko else line for line,ko in zip(top10_lines,kos)]

    if prev_all is None: return S

    # ---------- 급하락 (5개 제한)
    df_t = keyify(df_today)
    t30 = df_t[df_t["rank"]<=30]
    p30 = prev_all[prev_all["rank"]<=30]
    common = set(t30.index) & set(p30.index)
    out = set(p30.index) - set(t30.index)

    falling_pack=[]
    for k in common:
        pr, cr = int(p30.loc[k,"rank"]), int(t30.loc[k,"rank"])
        drop = cr-pr
        if drop>0:
            falling_pack.append((drop,cr,pr,t30.loc[k]))
    falling_pack.sort(key=lambda x:(-x[0],x[1],x[2]))
    falling_lines=[]
    jp_for_tr=[]
    for drop,cr,pr,row in falling_pack[:MAX_FALLING]:
        line=f"- {link_name(row)} {pr}위 → {cr}위 (↓{drop})"
        falling_lines.append(line)
        jp_for_tr.append(plain_name(row))
    kos=translate_ja_to_ko_batch(jp_for_tr)
    S["falling"]=[f"{l}\n{ko}" if ko else l for l,ko in zip(falling_lines,kos)]

    # ---------- OUT
    outs=[]
    for k in sorted(list(out))[:MAX_OUT]:
        pr=int(p30.loc[k,"rank"])
        outs.append(f"- {link_name(p30.loc[k])} {pr}위 → OUT")
    S["outs"]=outs

    S["inout_count"]=len(set(t30.index)-set(p30.index))+len(out)
    return S

# ---------- Slack message ----------
def build_slack_message(date_str: str, S: Dict[str,List[str]]) -> str:
    lines=[]
    lines.append(f"*🛒 큐텐 재팬 뷰티 랭킹 — {date_str}*")
    lines.append("")
    lines.append("*TOP 10*")
    lines.extend(S["top10"])
    lines.append("")
    lines.append("*📉 급하락*")
    lines.extend(S["falling"] or ["- 해당 없음"])
    if S["outs"]: lines.extend(S["outs"])
    lines.append("")
    lines.append("*🔄 랭크 인&아웃*")
    lines.append(f"{S['inout_count']}개의 제품이 인&아웃 되었습니다.")
    return "\n".join(lines)

# ---------- main ----------
def main():
    date_str=today_kst_str()
    y_file=build_filename(yesterday_kst_str())
    t_file=build_filename(date_str)

    items=fetch_products()
    df_today=to_dataframe(items,date_str)
    os.makedirs("data",exist_ok=True)
    df_today.to_csv(os.path.join("data",t_file),index=False,encoding="utf-8-sig")

    df_prev=None
    if os.path.exists(os.path.join("data",y_file)):
        df_prev=pd.read_csv(os.path.join("data",y_file))

    S=build_sections(df_today,df_prev)
    msg=build_slack_message(date_str,S)
    slack_post(msg)

if __name__=="__main__":
    main()
