# ===== QOO10: 키 추출/정규화/CSV/비교/슬랙 =====
import re, csv, json, time, os, datetime as dt
from pathlib import Path
from urllib.parse import urlparse, parse_qs
import requests

KST = dt.timezone(dt.timedelta(hours=9))
TODAY = dt.datetime.now(KST).date()
DATA_DIR = Path("data"); DATA_DIR.mkdir(parents=True, exist_ok=True)

# ---- ENV
SLACK_WEBHOOK_URL = os.getenv("SLACK_WEBHOOK_URL", "")
GDRIVE_FOLDER_ID   = os.getenv("GDRIVE_FOLDER_ID", "")
GOOGLE_CLIENT_ID   = os.getenv("GOOGLE_CLIENT_ID", "")
GOOGLE_CLIENT_SECRET = os.getenv("GOOGLE_CLIENT_SECRET", "")
GOOGLE_REFRESH_TOKEN = os.getenv("GOOGLE_REFRESH_TOKEN", "")
SLACK_TRANSLATE_JA2KO = os.getenv("SLACK_TRANSLATE_JA2KO", "0")  # "1"이면 번역

# ---------- Slack ----------
def slack_post(text: str):
    if not SLACK_WEBHOOK_URL: 
        print("[Slack] 미설정")
        return
    try:
        requests.post(SLACK_WEBHOOK_URL, json={"text": text}, timeout=20).raise_for_status()
        print("[Slack] 전송 OK")
    except Exception as e:
        print("[Slack] 실패:", e)

# ---------- Google Drive (refresh_token) ----------
def google_oauth_token():
    url = "https://oauth2.googleapis.com/token"
    data = {
        "client_id": GOOGLE_CLIENT_ID,
        "client_secret": GOOGLE_CLIENT_SECRET,
        "refresh_token": GOOGLE_REFRESH_TOKEN,
        "grant_type": "refresh_token",
    }
    r = requests.post(url, data=data, timeout=30); r.raise_for_status()
    return r.json()["access_token"]

def drive_upload_file(path: Path) -> str:
    if not GDRIVE_FOLDER_ID: 
        print("[Drive] 미설정")
        return ""
    token = google_oauth_token()
    meta = {"name": path.name, "parents": [GDRIVE_FOLDER_ID]}
    files = {
        "metadata": ("metadata", json.dumps(meta), "application/json; charset=UTF-8"),
        "file": (path.name, open(path, "rb"), "text/csv"),
    }
    url = "https://www.googleapis.com/upload/drive/v3/files?uploadType=multipart"
    r = requests.post(url, headers={"Authorization": f"Bearer {token}"}, files=files, timeout=60)
    r.raise_for_status()
    fid = r.json().get("id", "")
    print("[Drive] 업로드:", fid)
    return fid

def drive_latest_prev(prefix: str) -> Path | None:
    """드라이브에서 prefix로 시작하고 오늘 이전 날짜가 포함된 최신 CSV를 내려받음."""
    if not GDRIVE_FOLDER_ID: 
        return None
    token = google_oauth_token()
    q = f"'{GDRIVE_FOLDER_ID}' in parents and name contains '{prefix}' and trashed=false"
    url = "https://www.googleapis.com/drive/v3/files"
    params = {"q": q, "fields": "files(id,name,createdTime)", "orderBy":"createdTime desc", "pageSize":100}
    r = requests.get(url, headers={"Authorization": f"Bearer {token}"}, params=params, timeout=30)
    r.raise_for_status()
    for f in r.json().get("files", []):
        m = re.search(r"(\d{4}-\d{2}-\d{2})", f["name"])
        if not m: 
            continue
        d = dt.date.fromisoformat(m.group(1))
        if d < TODAY:
            # download
            out = DATA_DIR / "prev.csv"
            dl = requests.get(f"https://www.googleapis.com/drive/v3/files/{f['id']}?alt=media",
                              headers={"Authorization": f"Bearer {token}"}, timeout=60)
            dl.raise_for_status()
            out.write_bytes(dl.content)
            print("[Drive] 전일 CSV 받음:", out)
            return out
    return None

# ---------- Qoo10: 상품코드 추출/정규화 ----------
PID_RE_LIST = [
    re.compile(r"/(\d{8,12})(?:\?|$)"),           # .../1091763751?...
    re.compile(r"[?&](?:goodsNo|goodsno)=(\d+)"), # ?goodsNo=...
]

def extract_qoo10_pid(url: str) -> str:
    for rgx in PID_RE_LIST:
        m = rgx.search(url)
        if m: return m.group(1)
    return ""

NAME_CLEAN_RE = re.compile(r"(?:^公式\b|\bNEW\b|\[[^\]]*\]|\([^)]+\)|\s{2,})")

def normalize_qoo10_text(s: str) -> str:
    # '公式' 제거, 대괄호/괄호 블럭 제거, 다중 공백 정리
    s = NAME_CLEAN_RE.sub(" ", s).strip()
    return s

def make_key(name: str, url: str) -> str:
    pid = extract_qoo10_pid(url)
    return f"PID:{pid}" if pid else f"NM:{normalize_qoo10_text(name)}"

# ---------- CSV ----------
def save_qoo10_csv(items: list[dict], prefix: str) -> Path:
    """
    items: [{rank, brand, name, price, url, product_code(옵션)}]
    """
    path = DATA_DIR / f"{prefix}_{TODAY.isoformat()}.csv"
    with path.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["date","rank","brand","product_name","price","url","product_code"])
        for it in items:
            pid = it.get("product_code") or extract_qoo10_pid(it["url"])
            w.writerow([TODAY.isoformat(), it["rank"], it.get("brand",""),
                        it["name"], it.get("price",0), it["url"], pid])
    print("[CSV] 저장:", path)
    return path

def load_rows(path: Path) -> list[dict]:
    rows=[]
    if not path or not path.exists(): 
        return rows
    with path.open("r", encoding="utf-8") as f:
        r = csv.DictReader(f)
        for row in r:
            row["rank"]=int(row["rank"]); row["price"]=int(row.get("price","0") or "0")
            rows.append(row)
    return rows

# ---------- 비교(급상승/하락/뉴랭커/아웃) ----------
def analyze(today_rows, prev_rows, top_n_for_new=30, limit_rise=3, limit_fall=5, limit_new=3):
    def key_of(row):
        pid = row.get("product_code") or extract_qoo10_pid(row["url"])
        return f"PID:{pid}" if pid else f"NM:{normalize_qoo10_text(row['product_name'])}"

    t_map = {key_of(r): r for r in today_rows}
    p_map = {key_of(r): r for r in prev_rows}
    common = list(t_map.keys() & p_map.keys())

    rises=[]; falls=[]
    for k in common:
        t = t_map[k]["rank"]; p = p_map[k]["rank"]
        diff = p - t
        if diff>0:  rises.append((diff, t_map[k], p_map[k]))
        elif diff<0: falls.append((-diff, t_map[k], p_map[k]))

    rises.sort(key=lambda x:(-x[0], x[1]["rank"]))
    falls.sort(key=lambda x:(-x[0], x[1]["rank"]))

    # new / out
    new_rankers=[]
    for k,r in t_map.items():
        if r["rank"]<=top_n_for_new and k not in p_map:
            new_rankers.append(r)
        elif r["rank"]<=top_n_for_new and k in p_map and p_map[k]["rank"]>top_n_for_new:
            new_rankers.append(r)
    new_rankers.sort(key=lambda r:r["rank"])

    rank_outs=[]
    for k,r in p_map.items():
        if r["rank"]<=top_n_for_new and (k not in t_map or t_map[k]["rank"]>top_n_for_new):
            rank_outs.append(r)

    return rises[:limit_rise], new_rankers[:limit_new], falls[:limit_fall], len(new_rankers)+len(rank_outs)

# ---------- (선택) 일본어→한국어 번역 ----------
def maybe_translate(text: str) -> str:
    if SLACK_TRANSLATE_JA2KO != "1": 
        return text
    try:
        # 간단한 무료 API 회피용: 구글 번역 웹엔진 라이브러리 사용이 막히는 경우가 많아
        # 여기선 슬랙 메시지 길이 줄이기에 집중하고 번역 실패는 조용히 무시
        from googletrans import Translator  # requirements에 googletrans==4.0.0-rc1
        tr = Translator()
        # 영어/숫자만인 줄은 번역 안 함
        def needs_ja(s): return bool(re.search(r"[\u3040-\u30ff\u3400-\u9fff]", s))
        lines=[]
        for ln in text.splitlines():
            if needs_ja(ln):
                lines.append(tr.translate(ln, src="ja", dest="ko").text)
            else:
                lines.append(ln)
        return "\n".join(lines)
    except Exception:
        return text

# ---------- Slack 메시지 ----------
def build_slack_qoo10(today_rows, rises, new_rankers, falls, inout_cnt, title_date):
    top10 = [r for r in today_rows if r["rank"]<=10]
    def line(r):
        brand = normalize_qoo10_text(r.get("brand","")).replace("公式","").strip()
        name  = normalize_qoo10_text(r["product_name"])
        head  = f"{brand} " if brand else ""
        price = f" — ¥{r['price']:,}" if r.get("price") else ""
        return f"{r['rank']}. <{r['url']}|{head}{name}>{price}"

    msg=[]
    msg.append(f"*큐텐 재팬 뷰티 랭킹 — {title_date}*")
    msg.append("")
    msg.append("*TOP 10*")
    for r in top10:
        msg.append(line(r))

    # sections
    if rises:
        msg.append("\n🔥 *급상승*")
        for diff,t,p in rises:
            msg.append(f"- {normalize_qoo10_text(t['product_name'])} {p['rank']}위 → {t['rank']}위 (↑{diff})")
    else:
        msg.append("\n🔥 *급상승*\n- 해당 없음")

    if new_rankers:
        msg.append("\n🆕 *뉴랭커*")
        for r in new_rankers:
            msg.append(f"- {normalize_qoo10_text(r['product_name'])} NEW → {r['rank']}위")
    else:
        msg.append("\n🆕 *뉴랭커*\n- 해당 없음")

    if falls:
        msg.append("\n📉 *급하락*")
        for diff,t,p in falls:
            msg.append(f"- {normalize_qoo10_text(t['product_name'])} {p['rank']}위 → {t['rank']}위 (↓{diff})")
    else:
        msg.append("\n📉 *급하락*\n- 해당 없음")

    msg.append("\n🔗 *랭크 인&아웃*")
    msg.append(f"{inout_cnt}개의 제품이 인&아웃 되었습니다.")
    return "\n".join(msg)

# ---------- 메인에서 이렇게 사용하세요 ----------
def qoo10_pipeline(items: list[dict]):
    """
    items 예시 스키마(크롤러가 채워줌):
      {'rank':1,'brand':'公式 デイジーク','name':'[8/15~19] 全品999円…', 'price':999,
       'url':'https://www.qoo10.jp/item/.../1091763751?...'}
    """
    # 1) PID/정규화 키 보강
    for it in items:
        it["product_code"] = extract_qoo10_pid(it["url"])
        it["name"] = normalize_qoo10_text(it["name"])
        if "brand" in it:
            it["brand"] = normalize_qoo10_text(it["brand"]).replace("公式","").strip()

    # 2) CSV 저장/업로드
    csv_path = save_qoo10_csv(
        [{"rank":it["rank"], "brand":it.get("brand",""), "name":it["name"],
          "price":it.get("price",0), "url":it["url"], "product_code":it.get("product_code","")}
         for it in items],
        prefix="큐텐재팬_뷰티_랭킹"
    )
    drive_upload_file(csv_path)

    # 3) 전일 CSV 로드
    prev_path = drive_latest_prev("큐텐재팬_뷰티_랭킹_")
    today_rows = load_rows(csv_path)
    prev_rows = load_rows(prev_path) if prev_path else []

    # 4) 비교
    rises, new_rankers, falls, inout_cnt = analyze(
        today_rows, prev_rows,
        top_n_for_new=30, limit_rise=3, limit_fall=5, limit_new=3
    )

    # 5) 슬랙
    msg = build_slack_qoo10(today_rows, rises, new_rankers, falls, inout_cnt, TODAY.isoformat())
    msg = maybe_translate(msg)  # JA→KO(옵션)
    slack_post(msg)
# ===== END QOO10 BLOCK =====
