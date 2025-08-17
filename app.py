# -*- coding: utf-8 -*-
"""
Qoo10 JP Beauty Bestsellers (g=2)

- 모바일 정적 HTML 우선, 부족 시 Playwright 폴백
- CSV: 큐텐재팬_뷰티_랭킹_YYYY-MM-DD.csv (KST)
- 비교 키: product_code 우선, 없으면 URL (정규화해서 매칭)

Slack 포맷:
 TOP10 (변동 마커 + 번역)
 📉 급하락 (5개만, 번역)
 OUT (최대 10개, 번역 없음)
 🔄 랭크 인&아웃 (개수만)

Drive:
 - GOOGLE_SERVICE_ACCOUNT_JSON 있으면 서비스계정 로그인
 - 아니면 GOOGLE_CLIENT_ID/SECRET/REFRESH_TOKEN OAuth 로그인
"""

import os, re, io, math, pytz, traceback, urllib.parse
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
]
DESKTOP_URL = "https://www.qoo10.jp/gmkt.inc/Bestsellers/?g=2"
MAX_RANK = int(os.getenv("QOO10_MAX_RANK", "200"))
MAX_FALLING = 5
MAX_OUT = int(os.getenv("QOO10_MAX_OUT", "10"))

# ---------- Time ----------
def today_kst_str(): return dt.datetime.now(KST).strftime("%Y-%m-%d")
def yesterday_kst_str(): return (dt.datetime.now(KST) - dt.timedelta(days=1)).strftime("%Y-%m-%d")
def build_filename(d): return f"큐텐재팬_뷰티_랭킹_{d}.csv"
def clean_text(s): return re.sub(r"\s+", " ", (s or "")).strip()
def slack_escape(s): return (s or "").replace("&","&amp;").replace("<","&lt;").replace(">","&gt;")

# ---------- 이름/괄호 ----------
BRACKETS_PAT = re.compile(r"(\[.*?\]|【.*?】|（.*?）|\(.*?\))")
def strip_brackets_for_slack(s: str) -> str:
    return clean_text(BRACKETS_PAT.sub("", s or ""))

# ---------- 가격/할인 ----------
YEN_AMOUNT_RE = re.compile(r"(?:¥|)(\d{1,3}(?:,\d{3})+|\d+)\s*円")
PCT_RE = re.compile(r"(\d+)\s*% ?OFF", re.I)
def parse_jpy_amounts(text: str) -> List[int]:
    return [int(m.group(1).replace(",", "")) for m in YEN_AMOUNT_RE.finditer(text or "")]
def compute_prices(block_text: str) -> Tuple[Optional[int], Optional[int], Optional[int]]:
    amounts = parse_jpy_amounts(block_text); sale=orig=None
    if amounts: 
        sale=min(amounts); 
        if len(amounts)>=2: orig=max(amounts); 
        if orig==sale: orig=None
    pct=None
    m=PCT_RE.search(block_text)
    if m: pct=int(m.group(1))
    elif orig and sale: pct=max(0,int(math.floor((1-sale/orig)*100)))
    return sale, orig, pct

# ---------- Product ----------
@dataclass
class Product:
    rank: Optional[int]; brand: str; title: str
    price: Optional[int]; discount_percent: Optional[int]
    url: str; product_code: str=""

# ---------- Parse ----------
def extract_goods_code(url: str, block_text=""):
    if not url: return ""
    m=re.search(r"(?:[?&](?:goods?_?code|goodsno)=(\d+))",url)
    if m: return m.group(1)
    m2=re.search(r"/(?:Item|item)/(?:.*?/)?(\d+)(?:[/?#]|$)",url)
    if m2: return m2.group(1)
    m3=re.search(r"商品番号\s*[:：]\s*(\d+)",block_text)
    return m3.group(1) if m3 else ""

def parse_mobile_html(html: str) -> List[Product]:
    soup=BeautifulSoup(html,"lxml")
    anchors=soup.select("a[href*='Goods.aspx'],a[href*='/Item/']")
    items=[]; seen=set()
    for a in anchors:
        href=a.get("href",""); 
        if not href: continue
        cont=a.find_parent("li") or a.find_parent("div")
        block=clean_text(cont.get_text(" ",strip=True)) if cont else ""
        if href.startswith("//"): href="https:"+href
        elif href.startswith("/"): href="https://www.qoo10.jp"+href
        code=extract_goods_code(href,block); key=code or href
        if key in seen: continue; seen.add(key)
        name=clean_text(a.get_text(" ",strip=True))
        sale,_,pct=compute_prices(block)
        items.append(Product(len(items)+1,"",name,sale,pct,href,code))
        if len(items)>=MAX_RANK: break
    return items

def fetch_by_http_mobile()->List[Product]:
    headers={"User-Agent":"Mozilla/5.0","Accept-Language":"ja,en;q=0.8,ko;q=0.7"}
    for url in MOBILE_URLS:
        try:
            r=requests.get(url,headers=headers,timeout=20); r.raise_for_status()
            items=parse_mobile_html(r.text)
            if len(items)>=10: return items
        except: pass
    return []

def fetch_by_playwright()->List[Product]:
    from playwright.sync_api import sync_playwright
    with sync_playwright() as p:
        br=p.chromium.launch(headless=True,args=["--no-sandbox"])
        ctx=br.new_context(locale="ja-JP",timezone_id="Asia/Tokyo")
        page=ctx.new_page(); page.goto(DESKTOP_URL,timeout=60_000)
        page.wait_for_timeout(5000)
        html=page.content(); ctx.close(); br.close()
    return parse_mobile_html(html)

def fetch_products():
    items=fetch_by_http_mobile()
    if len(items)>=10: return items
    return fetch_by_playwright()

# ---------- Slack ----------
def fmt_currency(v): 
    try: return f"₩{int(v):,}"
    except: return "₩0"
def slack_post(text):
    url=os.getenv("SLACK_WEBHOOK_URL")
    if not url: print(text); return
    requests.post(url,json={"text":text,"unfurl_links":False})

# ---------- 번역 ----------
def translate_ja_to_ko_batch(lines: List[str])->List[str]:
    if not (os.getenv("SLACK_TRANSLATE_JA2KO","0") in ("1","true")):
        return ["" for _ in lines]
    try:
        from googletrans import Translator
        tr=Translator(service_urls=['translate.googleapis.com'])
        res=tr.translate(lines,src="ja",dest="ko")
        return [r.text for r in (res if isinstance(res,list) else [res])]
    except: return ["" for _ in lines]

# ---------- DataFrame ----------
def to_dataframe(products: List[Product], date_str: str)->pd.DataFrame:
    return pd.DataFrame([{
        "date":date_str,"rank":p.rank,"brand":p.brand,"product_name":p.title,
        "price":p.price,"discount_percent":p.discount_percent,"url":p.url,"product_code":p.product_code
    } for p in products])

# ---------- Key Normalize ----------
def _norm_product_code(v)->str:
    if pd.isna(v): return ""
    try:
        f=float(str(v)); 
        if f.is_integer(): return str(int(f))
    except: pass
    return str(v).strip()
def _norm_url(u:str)->str:
    if not u: return ""
    u=u.strip(); 
    if u.startswith("//"): u="https:"+u
    if u.startswith("/"): u="https://www.qoo10.jp"+u
    try:
        pr=urllib.parse.urlparse(u); q=urllib.parse.parse_qsl(pr.query)
        BAD={"utm_source","utm_medium","utm_campaign","utm_term","utm_content","cid","g"}
        q=[(k,v) for k,v in q if k not in BAD]
        return urllib.parse.urlunparse(pr._replace(query=urllib.parse.urlencode(q))).lower()
    except: return u.lower()
def keyify(df):
    if df is None or not len(df): return None
    df=df.copy()
    df["product_code"]=df["product_code"].apply(_norm_product_code)
    df["url"]=df["url"].apply(_norm_url)
    df["key"]=df.apply(lambda x:x["product_code"] if x["product_code"] else x["url"],axis=1)
    df.set_index("key",inplace=True)
    return df

# ---------- Sections ----------
def build_sections(df_today:pd.DataFrame,df_prev:Optional[pd.DataFrame])->Dict[str,List[str]]:
    S={"top10":[],"falling":[],"outs":[],"inout_count":0}
    def plain_name(r): return strip_brackets_for_slack(clean_text(r.get("product_name","")))
    def link_name(r): return f"<{r['url']}|{slack_escape(plain_name(r))}>"
    prev_all=keyify(df_prev) if (df_prev is not None and len(df_prev)) else None

    # TOP10
    jp,tr_lines=[],[]
    for _,r in df_today.sort_values("rank").head(10).iterrows():
        nm=plain_name(r); jp.append(nm); marker=""
        if prev_all is not None:
            k=r["product_code"] or r["url"]
            if k in prev_all.index:
                pr=int(prev_all.loc[k,"rank"]); cr=int(r["rank"]); d=pr-cr
                if d>0: marker=f"(↑{d}) "
                elif d<0: marker=f"(↓{abs(d)}) "
            else: marker="(New) "
        tail=f" (↓{int(r['discount_percent'])}%)" if pd.notnull(r["discount_percent"]) else ""
        tr_lines.append(f"{int(r['rank'])}. {marker}{link_name(r)} — {fmt_currency(r['price'])}{tail}")
    kos=translate_ja_to_ko_batch(jp)
    S["top10"]=[f"{a}\n{b}" if b else a for a,b in zip(tr_lines,kos)]

    if prev_all is None: return S

    # Falling
    df_t=keyify(df_today); t30=df_t[df_t["rank"]<=30]; p30=prev_all[prev_all["rank"]<=30]
    common=set(t30.index)&set(p30.index); out=set(p30.index)-set(t30.index)
    pack=[]
    for k in common:
        pr,cr=int(p30.loc[k,"rank"]),int(t30.loc[k,"rank"])
        drop=cr-pr
        if drop>0: pack.append((drop,cr,pr,t30.loc[k]))
    pack.sort(key=lambda x:(-x[0],x[1],x[2]))
    lines,jp2=[],[]
    for d,cr,pr,row in pack[:MAX_FALLING]:
        lines.append(f"- {link_name(row)} {pr}위 → {cr}위 (↓{d})"); jp2.append(plain_name(row))
    kos2=translate_ja_to_ko_batch(jp2)
    S["falling"]=[f"{a}\n{b}" if b else a for a,b in zip(lines,kos2)]

    # OUT
    outs=[(int(p30.loc[k,"rank"]),f"- {link_name(p30.loc[k])} {int(p30.loc[k,'rank'])}위 → OUT") for k in out]
    outs.sort(key=lambda x:x[0]); S["outs"]=[x[1] for x in outs[:MAX_OUT]]
    S["inout_count"]=len(set(t30.index)-set(p30.index))+len(out)
    return S

# ---------- Slack Message ----------
def build_slack_message(date,S):
    lines=[f"*🛒 큐텐 재팬 뷰티 랭킹 — {date}*","","*TOP 10*"]; lines+=S["top10"]; lines+=["","*📉 급하락*"]
    lines+=S["falling"] or ["- 해당 없음"]; 
    if S["outs"]: lines+=S["outs"]
    lines+=["","*🔄 랭크 인&아웃*",f"{S['inout_count']}개의 제품이 인&아웃 되었습니다."]
    return "\n".join(lines)

# ---------- Drive ----------
def build_drive_service():
    try:
        from googleapiclient.discovery import build
        from google.oauth2.service_account import Credentials
        import json,base64
        raw=os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON","")
        if raw:
            info=json.loads(base64.b64decode(raw).decode()) if not raw.lstrip().startswith("{") else json.loads(raw)
            creds=Credentials.from_service_account_info(info,scopes=["https://www.googleapis.com/auth/drive"])
            svc=build("drive","v3",credentials=creds,cache_discovery=False)
            who=svc.about().get(fields="user").execute()["user"]
            print("[Drive-SA]",who); return svc
    except Exception as e: print("[Drive-SA 실패]",e)
    try:
        from googleapiclient.discovery import build
        from google.oauth2.credentials import Credentials
        cid,sec,rt=os.getenv("GOOGLE_CLIENT_ID"),os.getenv("GOOGLE_CLIENT_SECRET"),os.getenv("GOOGLE_REFRESH_TOKEN")
        if cid and sec and rt:
            creds=Credentials(None,refresh_token=rt,token_uri="https://oauth2.googleapis.com/token",client_id=cid,client_secret=sec)
            svc=build("drive","v3",credentials=creds,cache_discovery=False)
            who=svc.about().get(fields="user").execute()["user"]
            print("[Drive-OAuth]",who); return svc
    except Exception as e: print("[Drive-OAuth 실패]",e)
    print("[Drive] 자격증명 없음"); return None

def drive_upload_csv(svc,folder_id,name,df):
    from googleapiclient.http import MediaIoBaseUpload
    buf=io.BytesIO(); df.to_csv(buf,index=False,encoding="utf-8-sig"); buf.seek(0)
    media=MediaIoBaseUpload(buf,mimetype="text/csv",resumable=False)
    q=f"name='{name}' and '{folder_id}' in parents and trashed=false"
    res=svc.files().list(q=q,fields="files(id)",supportsAllDrives=True).execute()
    if res.get("files"):
        fid=res["files"][0]["id"]; svc.files().update(fileId=fid,media_body=media,supportsAllDrives=True).execute()
        print("[Drive] 업데이트:",name); return fid
    meta={"name":name,"parents":[folder_id]}
    fid=svc.files().create(body=meta,media_body=media,fields="id",supportsAllDrives=True).execute()["id"]
    print("[Drive] 업로드:",name); return fid

def drive_download_csv(svc,folder_id,pattern):
    from googleapiclient.http import MediaIoBaseDownload
    base=pattern.replace(".csv","")
    q=f"name contains '{base}' and '{folder_id}' in parents and trashed=false"
    res=svc.files().list(q=q,fields="files(id,name,modifiedTime)",orderBy="modifiedTime desc",supportsAllDrives=True).execute()
    files=res.get("files",[])
    if not files: return None
    fid=files[0]["id"]; req=svc.files().get_media(fileId=fid,supportsAllDrives=True)
    fh=io.BytesIO(); dl=MediaIoBaseDownload(fh,req); done=False
    while not done: _,done=dl.next_chunk()
    fh.seek(0); return pd.read_csv(fh)

# ---------- Main ----------
def main():
    date=today_kst_str(); today_file=build_filename(date); yest_file=build_filename(yesterday_kst_str())
    items=fetch_products(); df_today=to_dataframe(items,date)
    os.makedirs("data",exist_ok=True); df_today.to_csv(os.path.join("data",today_file),index=False,encoding="utf-8-sig")
    df_prev=None
    try:
        # 구글드라이브 우선
        svc=build_drive_service(); fid=None
        folder=os.getenv("GDRIVE_FOLDER_ID")
        if svc and folder: fid=drive_upload_csv(svc,folder,today_file,df_today); df_prev=drive_download_csv(svc,folder,yest_file)
        # 로컬 대체
        if df_prev is None and os.path.exists(os.path.join("data",yest_file)):
            df_prev=pd.read_csv(os.path.join("data",yest_file))
    except Exception as e: print("[Prev 로드 실패]",e)

    S=build_sections(df_today,df_prev); msg=build_slack_message(date,S)
    slack_post(msg); print("[완료]")

if __name__=="__
