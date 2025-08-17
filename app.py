def fetch_qoo10() -> List[Dict]:
    """
    Qoo10 JP 모바일 베스트셀러(뷰티: group_code=2) 수집.
    - 1차: 네트워크 응답(JSON) 스니핑으로 수집
    - 2차: 모바일 DOM 파싱
    - 3차: 데스크톱 UA로 재시도 DOM 파싱
    반환: [{rank,name,price,url,code}]
    """
    items: List[Dict] = []

    def parse_from_json_payload(payload) -> List[Dict]:
        results = []
        # JSON 스키마가 수시로 바뀌므로, 폭넓게 키 후보를 탐색합니다.
        def pick(d, *keys):
            for k in keys:
                if k in d and d[k]:
                    return d[k]
            return None

        # payload가 dict/ list 모두 가능
        stack = [payload]
        found = []
        while stack:
            cur = stack.pop()
            if isinstance(cur, dict):
                # 상품 리스트에 자주 쓰이는 키 후보
                if any(k in cur for k in ("GoodsName", "GoodsNm", "ItemName", "goodname")) and any(
                    k in cur for k in ("SellPrice", "Price", "sellprice", "price")
                ) and any(k in cur for k in ("GoodsNo", "GoodsCode", "GoodNo", "no", "id")):
                    found.append(cur)
                for v in cur.values():
                    stack.append(v)
            elif isinstance(cur, list):
                stack.extend(cur)

        rank = 1
        for d in found:
            name = pick(d, "GoodsName", "GoodsNm", "ItemName", "goodname")
            price = pick(d, "SellPrice", "Price", "sellprice", "price")
            url = pick(d, "ItemUrl", "GoodsUrl", "Url", "url", "LinkUrl")
            code = pick(d, "GoodsNo", "GoodsCode", "GoodNo", "no", "id")

            # URL이 없으면 PartialUrl + 코드로 만들 시도
            if (not url) and code:
                # 모바일 item 패턴으로 보완
                url = f"https://www.qoo10.jp/item/{code}"

            try:
                name = clean_name(str(name or "").strip())
                price = int(str(price).replace(",", "").replace("¥", "").strip()) if price else None
            except:
                price = None

            if name and price and url:
                results.append({
                    "rank": rank,
                    "name": name,
                    "price": price,
                    "url": url,
                    "code": extract_code_from_url(url) or (str(code) if code else None)
                })
                rank += 1
                if rank > TOP_LIMIT_SAVE:
                    break

        return results

    def parse_from_dom(html: str) -> List[Dict]:
        soup = BeautifulSoup(html, "lxml")

        # 상품 상세로 연결되는 앵커
        anchors = soup.select('a[href*="/item/"]')
        seen = set()
        cards = []
        for a in anchors:
            href = a.get("href", "")
            if "/item/" not in href:
                continue
            if href in seen:
                continue
            seen.add(href)
            cards.append(a)

        name_selectors = [
            ".sbj", ".tit", ".prd_tit", ".name", "p", "div.title", "span.title"
        ]
        price_selectors = [
            ".prc", ".price", ".org", ".dq_price", ".won", "em", "strong", "b", "span"
        ]

        def nearest_text(el, selectors: List[str]) -> str:
            cur = el
            for _ in range(3):
                parent = cur.parent if cur else None
                if not parent:
                    break
                for sel in selectors:
                    cand = parent.select_one(sel)
                    if cand and cand.get_text(strip=True):
                        return cand.get_text(" ", strip=True)
                cur = parent
            return el.get_text(" ", strip=True)

        out = []
        rank = 1
        for a in cards:
            url = a.get("href", "")
            code = extract_code_from_url(url)
            raw_name = nearest_text(a, name_selectors)
            name = clean_name(raw_name)
            raw_price = nearest_text(a, price_selectors)
            price = extract_price(raw_price)

            if not name or not price or not code:
                continue
            out.append({
                "rank": rank,
                "name": name,
                "price": price,
                "url": url,
                "code": code
            })
            rank += 1
            if rank > TOP_LIMIT_SAVE:
                break
        return out

    # -------------------------
    # Playwright run
    # -------------------------
    with sync_playwright() as p:
        # 1) 모바일 UA로 시도 + 네트워크 JSON 스니핑
        browser = p.chromium.launch(headless=True, args=["--disable-dev-shm-usage"])
        ua_mobile = "Mozilla/5.0 (iPhone; CPU iPhone OS 15_2 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/15.2 Mobile/15E148 Safari/604.1"
        context = browser.new_context(
            user_agent=ua_mobile,
            locale="ja-JP",
            viewport={"width": 390, "height": 844},
        )
        page = context.new_page()

        captured_json = []

        def on_response(res):
            try:
                ctype = (res.headers or {}).get("content-type", "")
                url = res.url
                # 베스트셀러 API 추정 응답 감지
                if ("Best" in url or "Bestseller" in url) and ("json" in ctype or url.lower().endswith(".json")):
                    data = res.json()
                    captured_json.append(data)
            except:
                pass

        page.on("response", on_response)

        print("수집 시작:", QOO10_URL)
        page.goto(QOO10_URL, wait_until="networkidle", timeout=90_000)

        # 쿠키/동의 팝업 처리
        for sel in ["#onetrust-accept-btn-handler",
                    "button#onetrust-accept-btn-handler",
                    "button:has-text('同意')",
                    "button:has-text('同意する')",
                    "button[aria-label='close']"]:
            try:
                loc = page.locator(sel).first
                if loc.is_visible():
                    loc.click()
                    page.wait_for_timeout(300)
            except:
                pass

        # 스크롤(느린 통신 대비)
        for _ in range(40):
            page.evaluate("window.scrollBy(0, 1200)")
            page.wait_for_timeout(120)

        # 1차: 네트워크 JSON에서 파싱
        for payload in captured_json:
            parsed = parse_from_json_payload(payload)
            if parsed:
                items.extend(parsed)
        # 중복/정렬 정리
        if items:
            # 같은 코드 중복 제거
            uniq = {}
            for r in items:
                c = r["code"]
                if c not in uniq:
                    uniq[c] = r
            items = list(uniq.values())
            items.sort(key=lambda x: x["rank"])
        # 2차: DOM 보강
        if len(items) < 50:
            html = page.content()
            dom_items = parse_from_dom(html)
            if dom_items:
                # code 기준 병합
                by_code = {it["code"]: it for it in items}
                for d in dom_items:
                    by_code.setdefault(d["code"], d)
                items = list(by_code.values())
                items.sort(key=lambda x: x["rank"])

        context.close()

        # 3차: 여전히 부족하면 데스크톱 UA 재시도
        if len(items) < 50:
            context2 = browser.new_context(
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36",
                locale="ja-JP",
                viewport={"width": 1366, "height": 900},
            )
            page2 = context2.new_page()
            page2.goto(QOO10_URL, wait_until="networkidle", timeout=90_000)
            for _ in range(40):
                page2.evaluate("window.scrollBy(0, 1400)")
                page2.wait_for_timeout(120)
            html2 = page2.content()
            dom_items2 = parse_from_dom(html2)
            if dom_items2:
                by_code = {it["code"]: it for it in items}
                for d in dom_items2:
                    by_code.setdefault(d["code"], d)
                items = list(by_code.values())
                items.sort(key=lambda x: x["rank"])
            context2.close()

        browser.close()

    # 최종 rank 보정
    items = sorted(items, key=lambda x: x["rank"])[:TOP_LIMIT_SAVE]
    for i, r in enumerate(items, 1):
        r["rank"] = i

    if not items:
        raise RuntimeError("Qoo10 수집 결과 0건. 셀렉터/렌더링 점검 필요")

    return items
