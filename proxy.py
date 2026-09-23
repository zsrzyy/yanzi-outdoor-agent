# -*- coding: utf-8 -*-
"""
燕子户外 · 本地智能体代理（多线程版）
能力：
  1. /chat   转发 DeepSeek 问答（多线程，并发请求不再卡死）
  2. /weather 和风天气实时 + 3 天预报
  3. /train  12306 车次时刻表（按车次号，如 G101）
  4. /tickets 站站火车/高铁余票（如 徐州 -> 济南，自动覆盖城市全部车站）
  5. /cheapest 最便宜出行方案（如 从成都到大理最便宜的方案，含票价与车次信息）
  6. /deepseek/chat/completions  DeepSeek 透传（服务端注入 Key，支持流式 SSE）
用法：保持窗口运行；静默启动用同目录「静默启动.vbs」。
"""
import json
import os
import re
import sys
import gzip
import ssl
import time
import threading
import datetime
import urllib.request
import urllib.error
import urllib.parse
import http.cookiejar
from concurrent.futures import ThreadPoolExecutor
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
CONFIG = os.path.join(BASE_DIR, "config.txt")
API_URL = "https://api.deepseek.com/v1/responses"
MODEL = "deepseek-chat"
# 监听地址：默认 127.0.0.1（本机 Nginx 直连）；若 Nginx 跑在 Docker 容器里
# （如 1Panel 的 OpenResty），需设 YANZI_HOST=0.0.0.0 让容器经 172.17.0.1 访问，
# 并确保云防火墙不放行 8899（否则代理直接暴露公网）。
HOST = os.environ.get("YANZI_HOST", "127.0.0.1")
PORT = int(os.environ.get("YANZI_PORT", "8899"))

# 和风天气（Key 从 qweather_key.txt 读取，不入 git；Host 为账号专属 API 域名）
QWEATHER_HOST = "https://mh78m47ufw.re.qweatherapi.com"
QWEATHER_KEY_FILE = os.path.join(BASE_DIR, "qweather_key.txt")
QWEATHER_KEY = None
try:
    with open(QWEATHER_KEY_FILE, "r", encoding="utf-8") as _f:
        QWEATHER_KEY = _f.read().strip() or None
except Exception:
    pass
XZ_LOC = "101190801"  # 徐州

# 聚合数据（兜底数据源）：12306 直连被风控/无数据时，用聚合数据火车时刻表接口兜底
JUHE_URL = "https://apis.juhe.cn/fapigw/train/query"
JUHE_KEY_FILE = os.path.join(BASE_DIR, "juhe_key.txt")

# 本机 Python 缺 CA 证书，外呼统一跳过证书校验
SSL_CTX = ssl._create_unverified_context()
UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36")

# 全局 opener（带 cookie 管理，12306 需要会话 cookie）
_cj = http.cookiejar.CookieJar()
OPENER = urllib.request.build_opener(
    urllib.request.HTTPSHandler(context=SSL_CTX),
    urllib.request.HTTPCookieProcessor(_cj),
)


def http_get(url, referer=None, xhr=False, timeout=20):
    headers = {
        "User-Agent": UA,
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "zh-CN,zh;q=0.9",
    }
    if referer:
        headers["Referer"] = referer
    if xhr:
        headers["X-Requested-With"] = "XMLHttpRequest"
    req = urllib.request.Request(url, headers=headers)
    with OPENER.open(req, timeout=timeout) as r:
        data = r.read()
        if data[:2] == b"\x1f\x8b":
            data = gzip.decompress(data)
        return data.decode("utf-8-sig", "ignore")


# 大批量换乘查询时每个线程用独立 opener（各自 cookie + init），避免共享会话互相挤占
_tls = threading.local()

# 全局限速：所有 12306 请求发起间隔不小于 0.12 秒，避免触发风控
_rl_lock = threading.Lock()
_rl_last = [0.0]


def _rate_gate():
    with _rl_lock:
        now = time.time()
        wait = 0.25 - (now - _rl_last[0])
        if wait > 0:
            time.sleep(wait)
        _rl_last[0] = time.time()


# 简单 TTL 缓存：余票 20 分钟、票价 30 分钟（同一问题反复问不再打 12306）
_TICKET_CACHE = {}
_PRICE_CACHE = {}
_cache_lock = threading.Lock()


def _cache_get(cache, key, ttl):
    with _cache_lock:
        item = cache.get(key)
        if item and time.time() - item[0] < ttl:
            return item[1]
    return None


def _cache_set(cache, key, val):
    with _cache_lock:
        cache[key] = (time.time(), val)


def _tls_opener():
    op = getattr(_tls, "op", None)
    if op is None:
        cj = http.cookiejar.CookieJar()
        op = urllib.request.build_opener(
            urllib.request.HTTPSHandler(context=SSL_CTX),
            urllib.request.HTTPCookieProcessor(cj),
        )
        headers = {"User-Agent": UA}
        _rate_gate()
        with op.open(urllib.request.Request(
            "https://kyfw.12306.cn/otn/leftTicket/init?linktypeid=dc",
            headers=headers), timeout=20) as r:
            r.read()
        _tls.op = op
    return op


def _http_tls(url, referer=None, xhr=False, timeout=20):
    headers = {
        "User-Agent": UA,
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "zh-CN,zh;q=0.9",
    }
    if referer:
        headers["Referer"] = referer
    if xhr:
        headers["X-Requested-With"] = "XMLHttpRequest"
    op = _tls_opener()
    _rate_gate()
    with op.open(urllib.request.Request(url, headers=headers), timeout=timeout) as r:
        data = r.read()
        if data[:2] == b"\x1f\x8b":
            data = gzip.decompress(data)
        return data.decode("utf-8-sig", "ignore")


def load_key():
    with open(CONFIG, "r", encoding="utf-8") as f:
        return f.read().strip()


def load_juhe_key():
    """读取聚合数据 key（juhe_key.txt，git 忽略）；未配置返回 None。"""
    try:
        with open(JUHE_KEY_FILE, "r", encoding="utf-8") as f:
            k = f.read().strip()
        return k or None
    except Exception:
        return None


def _mins(s):
    p = (s or "").split(":")
    return int(p[0]) * 60 + int(p[1]) if len(p) == 2 else 0


# ===================== 12306 火车票 =====================

_STATION_MAP = None  # 站名 -> telecode
_CITY_MAP = None     # 城市名 -> [(站名, code)]


def _norm_city(name):
    return (name or "").strip().rstrip("市县区").strip()


def _city_candidates(station_name):
    """徐州东 -> ['徐州']；济南西 -> ['济南']；石家庄 -> ['石家庄']"""
    suffixes = ["东", "西", "南", "北", "新", "老", "站"]
    cands = [station_name]
    for sfx in suffixes:
        if station_name.endswith(sfx) and len(station_name) > 2:
            cands.append(station_name[:-1])
    out = []
    for c in cands:
        c = _norm_city(c)
        if c and c not in out:
            out.append(c)
    return out


def load_stations():
    """加载 12306 站名码表：@缩写|站名|telecode|拼音|首字母|序号@..."""
    global _STATION_MAP, _CITY_MAP
    if _STATION_MAP is not None:
        return
    raw = http_get("https://kyfw.12306.cn/otn/resources/js/framework/station_name.js",
                   referer="https://www.12306.cn/")
    body = raw.strip().rstrip(";").split("=", 1)[-1].strip("'\"\n ")
    name2code = {}
    for chunk in body.split("@"):
        p = chunk.split("|")
        if len(p) >= 3 and p[1]:
            name2code[p[1]] = p[2]
    city = {}
    for name, code in name2code.items():
        for cn in _city_candidates(name):
            city.setdefault(cn, []).append((name, code))
    _STATION_MAP = name2code
    _CITY_MAP = city


def resolve_station(text):
    """把用户输入的城市/车站名解析为 [(站名, telecode)]，精确车站优先，其次城市全部车站"""
    load_stations()
    t = _norm_city(text)
    if t in _STATION_MAP:
        return [(t, _STATION_MAP[t])]
    if t in _CITY_MAP:
        stations = list(_CITY_MAP[t])
        stations.sort(key=lambda x: (0 if _norm_city(x[0]) == t else 1, x[0]))
        return stations[:4]
    hits = [(n, c) for n, c in _STATION_MAP.items() if t in n]
    return hits[:4]


def query_train_code(code, date_str):
    """按车次号查时刻表"""
    code = code.strip().upper()
    load_stations()
    ymd = date_str.replace("-", "")
    search_url = ("https://search.12306.cn/search/v1/train/search?keyword=%s&date=%s"
                  % (urllib.parse.quote(code), ymd))
    raw = http_get(search_url, referer="https://www.12306.cn/")
    data = json.loads(raw).get("data") or []
    if not data:
        return {"ok": False, "error": "没查到车次 %s，确认一下车次号和日期（只能查今天起 15 天内）。" % code}
    item = next((x for x in data if x.get("station_train_code", "").upper() == code), data[0])
    from_name, to_name = item["from_station"], item["to_station"]
    fc = _STATION_MAP.get(from_name) or resolve_station(from_name)[0][1]
    tc = _STATION_MAP.get(to_name) or resolve_station(to_name)[0][1]
    tt_url = ("https://www.12306.cn/index/otn/czxx/queryByTrainNo?train_no=%s"
              "&from_station_telecode=%s&to_station_telecode=%s&depart_date=%s"
              % (item["train_no"], fc, tc, date_str))
    raw2 = http_get(tt_url, referer="https://www.12306.cn/")
    d2 = json.loads(raw2)
    stops_raw = (d2.get("data") or {}).get("data") or []
    if not stops_raw:
        return {"ok": False, "error": "12306 暂未返回 %s 在 %s 的时刻表，换个日期试试。" % (code, date_str)}
    stops = []
    for s in stops_raw:
        stops.append({
            "no": s.get("station_no", ""),
            "name": s.get("station_name", ""),
            "arrive": s.get("arrive_time", ""),
            "start": s.get("start_time", ""),
            "stop": s.get("stopover_time", "") or "----",
        })
    return {
        "ok": True, "code": item["station_train_code"], "date": date_str,
        "from": from_name, "to": to_name, "train_no": item["train_no"],
        "total": len(stops), "stops": stops,
    }


def _seat_cells(code, rows):
    """根据车次类型提取余票"""
    cells = rows.split("|")

    def v(i):
        x = cells[i] if i < len(cells) else ""
        return x if x else "--"
    if code[:1] in ("G", "D", "C"):
        return [
            {"label": "二等座", "value": v(30)},
            {"label": "一等座", "value": v(31)},
            {"label": "商务/特等", "value": v(32)},
            {"label": "无座", "value": v(26)},
        ]
    return [
        {"label": "硬座", "value": v(29)},
        {"label": "硬卧", "value": v(28)},
        {"label": "软卧", "value": v(23)},
        {"label": "无座", "value": v(26)},
    ]


def query_tickets(from_text, to_text, date_str):
    """站站余票：城市自动展开为全部车站，两两查询后合并去重"""
    from_stations = resolve_station(from_text)
    to_stations = resolve_station(to_text)
    if not from_stations:
        return {"ok": False, "error": "没认出出发地「%s」，请用车站所在城市名，如 徐州" % from_text}
    if not to_stations:
        return {"ok": False, "error": "没认出到达地「%s」，请用车站所在城市名，如 济南" % to_text}

    # 各线程独立会话，_left_query_once 内部会先 init 并在被风控时退避重试
    referer = "https://kyfw.12306.cn/otn/leftTicket/init?linktypeid=dc"

    def one_pair(pair):
        (fn, fc), (tn, tc) = pair
        return _left_query_once(fc, tc, date_str, referer)

    pairs = [(a, b) for a in from_stations for b in to_stations]
    merged, seen, name_map = [], set(), {}
    got_any = False  # 是否至少有一个站对拿到了正常载荷（区分限流与真无车）
    with ThreadPoolExecutor(max_workers=6) as ex:
        for data in ex.map(one_pair, pairs):
            if data is None:
                continue  # 该站对被风控拦截，未查成
            got_any = True
            name_map.update(data.get("map", {}))
            for row in data.get("result", []):
                f = row.split("|")
                if len(f) < 33:
                    continue
                key = f[2] + f[6] + f[7] + f[8]
                if key in seen:
                    continue
                seen.add(key)
                code = f[3]
                merged.append({
                    "code": code,
                    "fromStation": name_map.get(f[6], f[6]),
                    "toStation": name_map.get(f[7], f[7]),
                    "depart": f[8],
                    "arrive": f[9],
                    "duration": f[10],
                    "canBuy": f[11] == "Y",
                    "seats": _seat_cells(code, row),
                    "trainNo": f[2],
                    "fromNo": f[16],
                    "toNo": f[17],
                    "seatTypes": f[35] if len(f) > 35 else "",
                })
    if not merged:
        if not got_any:
            return {"ok": False,
                    "error": "12306 正在限流（风控拦截），这次没查成。这不是没有车，请稍等 30 秒后再问一次。"}
        return {"ok": False, "error": "%s %s 到 %s 没查到车次，可能是调度过了或日期超出预售期（15 天）。"
                % (date_str, from_text, to_text)}

    def tmin(s):
        p = s.split(":")
        return int(p[0]) * 60 + int(p[1]) if len(p) == 2 else 0
    merged.sort(key=lambda x: tmin(x["depart"]))
    return {
        "ok": True, "date": date_str,
        "fromQuery": from_text, "toQuery": to_text,
        "fromStations": [n for n, _ in from_stations],
        "toStations": [n for n, _ in to_stations],
        "count": len(merged), "trains": merged,
        "source": "12306",
    }


# 聚合数据免费版限流：每秒最多 1 次调用。全局限速，避免自己撞上限流。
_JUHE_LOCK = threading.Lock()
_JUHE_LAST = [0.0]
_JUHE_MIN_INTERVAL = 1.1


def _juhe_gate():
    with _JUHE_LOCK:
        wait = _JUHE_MIN_INTERVAL - (time.time() - _JUHE_LAST[0])
        if wait > 0:
            time.sleep(wait)
        _JUHE_LAST[0] = time.time()


def _juhe_query(from_text, to_text, date_str):
    """聚合数据站到站时刻表（兜底数据源）。返回结构与 query_tickets 对齐，方便前端复用。"""
    key = load_juhe_key()
    if not key:
        return {"ok": False, "error": "聚合数据兜底未配置（缺少 juhe_key.txt）"}
    url = (JUHE_URL + "?key=" + urllib.parse.quote(key)
           + "&search_type=1"
           + "&departure_station=" + urllib.parse.quote(from_text)
           + "&arrival_station=" + urllib.parse.quote(to_text)
           + "&date=" + date_str)
    # 聚合数据免费版限流：每秒最多 1 次。主动节流 + 撞限流自动重试一次。
    data = None
    for attempt in range(2):
        _juhe_gate()
        try:
            data = json.loads(http_get(url))
        except Exception as e:
            if attempt == 1:
                return {"ok": False, "error": "聚合数据接口调用失败：%s" % e}
            time.sleep(1.5)
            continue
        reason = str(data.get("reason") or "")
        if "频率" in reason or "限制" in reason:
            if attempt == 0:
                time.sleep(1.5)
                continue
            return {"ok": False, "error": "聚合数据调用频率受限（免费版每秒 1 次），请稍后重试"}
        break
    if data is None:
        return {"ok": False, "error": "聚合数据无响应"}
    if data.get("reason") != "success" and data.get("error_code", 0) != 0:
        return {"ok": False, "error": "聚合数据接口返回异常：%s" % (data.get("reason") or data.get("error_code"))}
    rows = data.get("result") or []
    if not rows:
        return {"ok": False, "error": "%s %s 到 %s 聚合数据也没查到车次" % (date_str, from_text, to_text)}
    trains = []
    for r in rows:
        seats = []
        for p in (r.get("prices") or []):
            num = p.get("num") or ""
            val = "有" if num == "有" else ("--" if num in ("无", "") else str(num))
            seats.append({
                "label": p.get("seat_name") or "",
                "value": val,
                "price": p.get("price"),
            })
        trains.append({
            "code": r.get("train_no"),
            "fromStation": r.get("departure_station"),
            "toStation": r.get("arrival_station"),
            "depart": r.get("departure_time"),
            "arrive": r.get("arrival_time"),
            "duration": r.get("duration"),
            "canBuy": r.get("enable_booking") == "Y",
            "seats": seats,
            "trainNo": r.get("train_no"),
        })
    trains.sort(key=lambda x: _mins(x["depart"]))
    return {
        "ok": True, "date": date_str,
        "fromQuery": from_text, "toQuery": to_text,
        "fromStations": [from_text], "toStations": [to_text],
        "count": len(trains), "trains": trains,
        "source": "juhe",
    }


def _juhe_cheapest(fr, to, date_str):
    """聚合数据兜底的最便宜方案：只有直达比价（聚合无中转能力），结构对齐 query_cheapest。"""
    jr = _juhe_query(fr, to, date_str)
    if not jr.get("ok"):
        return {"ok": False, "error": jr.get("error") or "聚合数据查询失败"}
    priced = []
    for t in jr.get("trains") or []:
        opts = []
        for s in (t.get("seats") or []):
            if s.get("price") is None:
                continue
            avail = s.get("value") or "--"
            opts.append({
                "label": s.get("label") or "",
                "avail": avail,
                "price": s["price"],
                "wuzuo": "无座" in (s.get("label") or ""),
                "bookable": avail != "--",
            })
        if not opts:
            continue
        # 有票的优先，其次按价格升序；无座排在等价席别之后
        opts.sort(key=lambda o: (0 if o["bookable"] else 1, o["price"], 1 if o["wuzuo"] else 0))
        priced.append((t, opts))
    if not priced:
        return {"ok": False, "error": "聚合数据未返回带票价的车次"}
    priced.sort(key=lambda x: x[1][0]["price"])

    def slim(t, o):
        return {
            "code": t["code"], "fromStation": t["fromStation"], "toStation": t["toStation"],
            "depart": t["depart"], "arrive": t["arrive"], "duration": t["duration"],
            "seatLabel": o["label"], "avail": o["avail"], "price": o["price"],
        }

    t0, opts0 = priced[0]
    best_opts = [{"label": o["label"], "avail": o["avail"], "price": o["price"], "wuzuo": o["wuzuo"]}
                 for o in opts0]
    best = {
        "kind": "direct",
        "code": t0["code"], "fromStation": t0["fromStation"], "toStation": t0["toStation"],
        "depart": t0["depart"], "arrive": t0["arrive"], "duration": t0["duration"],
        "best": best_opts[0], "options": best_opts,
    }
    return {
        "ok": True, "date": date_str, "fromQuery": fr, "toQuery": to,
        "directCount": len(priced), "transferCount": 0,
        "best": best,
        "direct": {"count": len(priced), "cheapest": best,
                   "top": [slim(t, o[0]) for t, o in priced[1:6]]},
        "transfers": [],
        "source": "juhe",
        "note": "12306 直连本次被拦截，以上为「聚合数据」查询结果（仅直达 · 不支持中转比价）",
    }


# ===================== 票价 / 最便宜方案 =====================

# 席别余票顺序（_seat_cells）对应的 12306 票价编码（一个席别可能对应多个编码，取第一个有值的）
PRICE_KEYS_HIGH = [["O"], ["M"], ["P", "A9", "9"], ["WZ"]]       # 二等座 / 一等座 / 商务特等 / 无座
PRICE_KEYS_NORMAL = [["A1", "1"], ["A3", "3"], ["A4", "4"], ["WZ"]]  # 硬座 / 硬卧 / 软卧 / 无座


def extract_cities(text):
    """从自然语言里识别「X 到 Y」的城市/车站名，返回 (出发, 到达)"""
    load_stations()
    names = sorted(set(list(_STATION_MAP) + list(_CITY_MAP)), key=len, reverse=True)
    for m in re.finditer(r"[到去往至回]", text):
        i = m.start()
        before = text[max(0, i - 8):i]
        after = text[i + 1:i + 9]
        fr = next((n for n in names if before.endswith(n)), None)
        to = next((n for n in names if after.startswith(n)), None)
        if fr and to and fr != to:
            return fr, to
    return None, None


def query_price(train, date_str, getter=None):
    """查单趟车各席别票价，返回 {席别: 价格数字}，失败返回 {}"""
    getter = getter or http_get
    if not train.get("seatTypes"):
        return {}
    ck = (train["trainNo"], train["fromNo"], train["toNo"], train["seatTypes"], date_str)
    cached = _cache_get(_PRICE_CACHE, ck, 1800)
    if cached is not None:
        return cached
    url = ("https://kyfw.12306.cn/otn/leftTicket/queryTicketPrice?train_no=%s"
           "&from_station_no=%s&to_station_no=%s&seat_types=%s&train_date=%s"
           % (train["trainNo"], train["fromNo"], train["toNo"], train["seatTypes"], date_str))
    try:
        data = json.loads(getter(
            url, referer="https://kyfw.12306.cn/otn/leftTicket/init?linktypeid=dc", xhr=True)
        ).get("data") or {}
    except Exception:
        return {}
    prices = {}
    for k, v in data.items():
        if k in ("OT", "train_no"):
            continue
        mm = re.search(r"(\d+(?:\.\d+)?)", str(v))
        if mm:
            prices[k] = float(mm.group(1))
    if prices:
        _cache_set(_PRICE_CACHE, ck, prices)
    return prices


def _seat_bookable(value):
    return value == "有" or (value.isdigit() and int(value) > 0)


def _train_options(train, date_str, getter=None):
    """给一趟车查票价，筛出当前可售席别，返回选项列表（按价格升序）"""
    prices = query_price(train, date_str, getter=getter)
    keys = PRICE_KEYS_HIGH if train["code"][:1] in ("G", "D", "C") else PRICE_KEYS_NORMAL
    options = []
    for seat, candidates in zip(train["seats"], keys):
        price = None
        for pk in candidates:
            if pk in prices:
                price = prices[pk]
                break
        if price is None:
            continue
        if _seat_bookable(seat["value"]):
            options.append({
                "label": seat["label"], "avail": seat["value"],
                "price": price, "wuzuo": seat["label"] == "无座",
            })
    options.sort(key=lambda o: (o["price"], 1 if o["wuzuo"] else 0))
    return options


# ===================== 中转换乘规划（DIY，一次换乘） =====================

# 全国主要铁路枢纽 + 大理方向门户站，作为候选换乘点
HUB_CITIES = [
    "北京", "上海", "广州", "武汉", "郑州", "西安", "重庆", "昆明", "贵阳", "南宁",
    "长沙", "南昌", "南京", "杭州", "济南", "徐州", "洛阳", "襄阳", "怀化", "鹰潭",
    "安康", "宝鸡", "汉中", "广元", "西昌", "攀枝花", "曲靖", "楚雄", "兰州", "广通北",
]


def _station_city(station_name):
    """昆明南 -> 昆明；广州南 -> 广州"""
    return _city_candidates(station_name)[0]


def _left_query_once(fc, tc, date_str, referer):
    """查询余票。返回 dict（正常，result 可能为空=真无车）或 None（多次重试仍被风控拦截）。"""
    key = (fc, tc, date_str)
    cached = _cache_get(_TICKET_CACHE, key, 1200)
    if cached is not None:
        return cached
    url = ("https://kyfw.12306.cn/otn/leftTicket/query?leftTicketDTO.train_date=%s"
           "&leftTicketDTO.from_station=%s&leftTicketDTO.to_station=%s&purpose_codes=ADULT"
           % (date_str, fc, tc))
    delays = (1, 2, 4, 8, 12)
    for attempt in range(len(delays)):
        try:
            raw = _http_tls(url, referer=referer, xhr=True)
            if raw.lstrip().startswith("<"):
                raise ValueError("blocked by anti-bot page")
            payload = json.loads(raw)
            data = payload.get("data")
            if isinstance(data, dict) and "map" in data:
                _cache_set(_TICKET_CACHE, key, data)
                return data  # 正常载荷（无车时 result 为空表，但 map 一定在）
        except Exception:
            pass
        # 被风控拦到：丢弃被污染的会话，重新 init，再退避
        if hasattr(_tls, "op"):
            del _tls.op
        time.sleep(delays[attempt])
    return None  # 明确区分：这是"风控没查成"，不是"真没车"


def _parse_ticket_row(row, mp):
    f = row.split("|")
    if len(f) < 36:
        return None
    code = f[3]
    return {
        "code": code,
        "fromStation": mp.get(f[6], f[6]),
        "toStation": mp.get(f[7], f[7]),
        "depart": f[8], "arrive": f[9], "duration": f[10],
        "trainNo": f[2], "fromNo": f[16], "toNo": f[17],
        "seatTypes": f[35], "seats": _seat_cells(code, row),
    }


def query_transfers(fr, to, date_str, referer):
    """经主要枢纽规划一次换乘方案：两段余票 -> 时刻拼接 -> 逐段查票价，按总价升序"""
    from_stations = resolve_station(fr)
    to_stations = resolve_station(to)
    if not from_stations or not to_stations:
        return []
    fc = from_stations[0][1]
    tc = to_stations[0][1]
    fr_city = _norm_city(from_stations[0][0])
    to_city = _norm_city(to_stations[0][0])

    hubs = []
    for h in HUB_CITIES:
        rs = resolve_station(h)
        if not rs:
            continue
        hname, hcode = rs[0]
        if _norm_city(hname) in (fr_city, to_city) or h in (fr, to):
            continue
        hubs.append((h, hname, hcode))

    # 第一段：出发城市 -> 各枢纽
    leg1_rows = []

    def leg1(hub):
        h, hname, hcode = hub
        data = _left_query_once(fc, hcode, date_str, referer)
        out = []
        if data is None:
            return out  # 被风控拦截，跳过该枢纽
        for row in data.get("result", []):
            t = _parse_ticket_row(row, data.get("map", {}))
            if t:
                t["hub"] = h
                out.append(t)
        return out

    with ThreadPoolExecutor(max_workers=3) as ex:
        for rs in ex.map(leg1, hubs):
            leg1_rows.extend(rs)

    reach = {}
    for t in leg1_rows:
        reach.setdefault(t["hub"], []).append(t)
    if not reach:
        return []

    # 第二段：各可达枢纽 -> 到达城市，当天和次日各查一次
    d0 = datetime.datetime.strptime(date_str, "%Y-%m-%d").date()
    d1 = d0 + datetime.timedelta(days=1)
    last_day = datetime.date.today() + datetime.timedelta(days=15)
    leg_dates = [date_str]
    if d1 <= last_day:
        leg_dates.append(d1.strftime("%Y-%m-%d"))
    hub_code = dict((h, (hname, hcode)) for h, hname, hcode in hubs)

    leg2_jobs = [(h, ds) for h in reach for ds in leg_dates]
    leg2_rows = []

    def leg2(job):
        h, ds = job
        hname, hcode = hub_code[h]
        data = _left_query_once(hcode, tc, ds, referer)
        out = []
        if data is None:
            return out  # 被风控拦截，跳过
        for row in data.get("result", []):
            t = _parse_ticket_row(row, data.get("map", {}))
            if t:
                t["hub"] = h
                t["date"] = ds
                t["dayOffset"] = 0 if ds == date_str else 1
                out.append(t)
        return out

    with ThreadPoolExecutor(max_workers=3) as ex:
        for rs in ex.map(leg2, leg2_jobs):
            leg2_rows.extend(rs)

    byhub = {}
    for t in leg2_rows:
        byhub.setdefault(t["hub"], []).append(t)

    def mins(x):
        p = x.split(":")
        return int(p[0]) * 60 + int(p[1]) if len(p) == 2 else -1

    def dur_mins(t):
        p = (t.get("duration") or "").split(":")
        return int(p[0]) * 60 + int(p[1]) if len(p) == 2 else 0

    def arrive_day(t):
        """根据发车时刻与历时推算到达跨几天：0=当天 1=次日"""
        dep, dm = mins(t["depart"]), dur_mins(t)
        if dep < 0:
            return 0
        return (dep + dm) // 1440

    # 时刻拼接：同站换乘至少等 25 分钟；同城不同站（昆明站/昆明南）至少 90 分钟
    combos = []
    for hub, trains1 in reach.items():
        hub_combos = []
        seen_a = set()
        for a in sorted(trains1, key=lambda x: arrive_day(x) * 1440 + mins(x["arrive"])):
            ta = mins(a["arrive"])
            a_abs = arrive_day(a) * 1440 + ta
            for b in byhub.get(hub, []):
                tb = mins(b["depart"])
                if ta < 0 or tb < 0:
                    continue
                wait = b["dayOffset"] * 1440 + tb - a_abs  # 第一段可能次日才到
                same_station = a["toStation"] == b["fromStation"]
                if same_station:
                    if wait < 25 or wait > 480:
                        continue
                else:
                    if _station_city(a["toStation"]) != _station_city(b["fromStation"]):
                        continue
                    if wait < 90 or wait > 600:
                        continue
                if a["trainNo"] in seen_a:
                    continue  # 每个第一趟车次只保留最早可行接续
                seen_a.add(a["trainNo"])
                hub_combos.append({
                    "hub": hub, "wait": wait, "cross": 0 if same_station else 1,
                    "a": a, "b": b,
                    "aDay": arrive_day(a),
                    "bDay": b["dayOffset"] + arrive_day(b),
                })
                if len(hub_combos) >= 3:
                    break
            if len(hub_combos) >= 3:
                break
        combos.extend(hub_combos)
    if not combos:
        return []

    # 逐段查票价（同车次同区间只查一次），去重后并发拉取
    price_cache = {}
    distinct_legs, seen_leg = [], set()
    for c in combos:
        for leg in (c["a"], c["b"]):
            key = (leg["trainNo"], leg["fromNo"], leg["toNo"], leg.get("date", date_str))
            if key not in seen_leg:
                seen_leg.add(key)
                distinct_legs.append((key, leg))

    def fetch_price(item):
        key, leg = item
        return key, _train_options(leg, leg.get("date", date_str), getter=_http_tls)

    with ThreadPoolExecutor(max_workers=4) as ex:
        for key, opts in ex.map(fetch_price, distinct_legs):
            price_cache[key] = opts

    def leg_price(leg):
        key = (leg["trainNo"], leg["fromNo"], leg["toNo"], leg.get("date", date_str))
        opts = price_cache.get(key) or []
        return opts[0] if opts else None

    plans = []
    for c in combos:
        oa = leg_price(c["a"])
        ob = leg_price(c["b"])
        if not oa or not ob:
            continue

        def leg_view(t, opt, arr_day):
            return {
                "code": t["code"], "fromStation": t["fromStation"], "toStation": t["toStation"],
                "depart": t["depart"], "arrive": t["arrive"], "duration": t["duration"],
                "date": t.get("date", date_str), "arrDay": arr_day,
                "seatLabel": opt["label"], "avail": opt["avail"], "price": opt["price"],
            }

        plans.append({
            "kind": "transfer", "hubCity": c["hub"], "waitMin": c["wait"],
            "crossCity": c["cross"], "total": round(oa["price"] + ob["price"], 1),
            "legs": [leg_view(c["a"], oa, c["aDay"]), leg_view(c["b"], ob, c["bDay"])],
        })

    plans.sort(key=lambda p: p["total"])
    return plans[:6]


def query_cheapest(raw_text, date_str):
    """识别城市 -> 直达比价 + 一次中转换乘比价 -> 返回最便宜方案与备选"""
    fr, to = extract_cities(raw_text)
    if not fr:
        return {"ok": False,
                "error": "没认出出发地和目的地，试试这样问：从成都到大理最便宜的方案"}

    # 12306 要求先 init 拿 JSESSIONID（预热线程会话）
    referer = "https://kyfw.12306.cn/otn/leftTicket/init?linktypeid=dc"
    try:
        _tls_opener()
    except Exception:
        pass

    # ---- 直达比价 ----
    direct_payload = None
    risk_blocked = False  # 12306 是否明确风控拦截（拦截时中转必然也失败，直接兜底更快）
    data = query_tickets(fr, to, date_str)
    if data.get("ok"):
        with ThreadPoolExecutor(max_workers=4) as ex:
            trains = list(ex.map(
                lambda t: (t.update(options=_train_options(t, date_str, getter=_http_tls)) or t),
                data["trains"]))
        priced = [t for t in trains if t.get("options")]
        if priced:
            priced.sort(key=lambda t: (t["options"][0]["price"],
                                       1 if t["options"][0]["wuzuo"] else 0, t["duration"]))
            cheapest = priced[0]

            def slim(t):
                o = t["options"][0]
                return {
                    "code": t["code"], "fromStation": t["fromStation"], "toStation": t["toStation"],
                    "depart": t["depart"], "arrive": t["arrive"], "duration": t["duration"],
                    "seatLabel": o["label"], "avail": o["avail"], "price": o["price"],
                }

            direct_payload = {
                "count": len(priced),
                "cheapest": {
                    "code": cheapest["code"], "fromStation": cheapest["fromStation"],
                    "toStation": cheapest["toStation"], "depart": cheapest["depart"],
                    "arrive": cheapest["arrive"], "duration": cheapest["duration"],
                    "best": cheapest["options"][0], "options": cheapest["options"],
                },
                "top": [slim(t) for t in priced[1:6]],
            }
    elif "限流" in (data.get("error") or ""):
        risk_blocked = True  # 被风控：中转查询也会被拦，跳过以免白等

    # ---- 中转比价（12306 明确风控时跳过，直接进兜底）----
    transfers = []
    if not risk_blocked:
        try:
            transfers = query_transfers(fr, to, date_str, referer)
        except Exception:
            transfers = []

    if not direct_payload and not transfers:
        # 12306 直连完全失败（风控/无数据）→ 聚合数据兜底（仅直达，聚合无中转比价能力）
        juhe_res = _juhe_cheapest(fr, to, date_str)
        if juhe_res.get("ok"):
            return juhe_res
        return {"ok": False,
                "error": "%s 到 %s 在 %s 没有查到可购票的直达或中转方案"
                         "（12306 未返回数据，聚合数据兜底也未成功：%s）"
                         % (fr, to, date_str, juhe_res.get("error") or "未知原因")}

    # ---- 汇总最便宜 ----
    candidates = []
    if direct_payload:
        c = direct_payload["cheapest"]
        candidates.append(("direct", c["best"]["price"]))
    if transfers:
        candidates.append(("transfer", transfers[0]["total"]))
    best_kind, best_price = sorted(candidates, key=lambda x: x[1])[0]
    if best_kind == "direct":
        c = direct_payload["cheapest"]
        best = {"kind": "direct"}
        for k in ("code", "fromStation", "toStation", "depart", "arrive",
                  "duration", "best", "options"):
            best[k] = c[k]
    else:
        best = dict(transfers[0])

    return {
        "ok": True, "date": date_str, "fromQuery": fr, "toQuery": to,
        "directCount": direct_payload["count"] if direct_payload else 0,
        "transferCount": len(transfers),
        "best": best,
        "direct": direct_payload,
        "transfers": transfers,
    }


def valid_date(date_str):
    try:
        d = datetime.datetime.strptime(date_str, "%Y-%m-%d").date()
    except Exception:
        return None, "日期格式应为 YYYY-MM-DD"
    today = datetime.date.today()
    if d < today:
        return None, "查不了过去的车次，请选今天或以后的日期"
    if d > today + datetime.timedelta(days=15):
        return None, "12306 预售期为 15 天，再远的车次还没放票"
    return d, None


# ===================== HTTP 服务 =====================

class Handler(BaseHTTPRequestHandler):
    def _send(self, obj, code=200):
        out = json.dumps(obj, ensure_ascii=False).encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")
        self.send_header("Cache-Control", "no-store")
        self.send_header("Content-Length", str(len(out)))
        self.end_headers()
        self.wfile.write(out)

    def _send_raw(self, raw, code=200, ctype="application/json"):
        """透传上游原始响应（文本/字节），用于 /deepseek 转发，保持与 DeepSeek 一致的错误结构。"""
        out = raw.encode("utf-8") if isinstance(raw, str) else raw
        self.send_response(code)
        self.send_header("Content-Type", ctype + "; charset=utf-8")
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")
        self.send_header("Cache-Control", "no-store")
        self.send_header("Content-Length", str(len(out)))
        self.end_headers()
        self.wfile.write(out)

    def _err_json(self, msg, code):
        self._send_raw(json.dumps({"error": {"message": msg}}, ensure_ascii=False), code)

    def do_OPTIONS(self):
        self._send({"ok": True})

    def do_GET(self):
        parsed = urllib.parse.urlparse(self.path)
        qs = urllib.parse.parse_qs(parsed.query)
        try:
            if parsed.path == "/health":
                # 健康检查：返回版本与能力清单，前端据此判断代理是否完整可用
                self._send({
                    "ok": True,
                    "name": "yanzi-outdoor-proxy",
                    "version": "2.0.0",
                    "routes": ["/health", "/weather", "/train", "/tickets", "/cheapest", "/deepseek/chat/completions"],
                    "time": time.strftime("%Y-%m-%d %H:%M:%S"),
                })
            elif parsed.path == "/weather":
                self._send(self.api_weather())
            elif parsed.path == "/train":
                date = qs.get("date", [datetime.date.today().strftime("%Y-%m-%d")])[0]
                code = qs.get("code", [""])[0]
                if not code:
                    self._send({"ok": False, "error": "缺少 code 参数，例如 /train?code=G101"}, 400)
                    return
                _, err = valid_date(date)
                if err:
                    self._send({"ok": False, "error": err}, 400)
                    return
                self._send(query_train_code(code, date))
            elif parsed.path == "/tickets":
                date = qs.get("date", [datetime.date.today().strftime("%Y-%m-%d")])[0]
                fr = qs.get("from", [""])[0]
                to = qs.get("to", [""])[0]
                if not fr or not to:
                    self._send({"ok": False, "error": "缺少 from / to 参数"}, 400)
                    return
                _, err = valid_date(date)
                if err:
                    self._send({"ok": False, "error": err}, 400)
                    return
                res = query_tickets(fr, to, date)
                # 12306 直连失败（限流/无数据/异常）→ 聚合数据兜底，保证"查车次"永远有结果
                if not res.get("ok"):
                    juhe = _juhe_query(fr, to, date)
                    if juhe.get("ok"):
                        juhe["note"] = "12306 直连本次未取到数据，已自动切换「聚合数据」查询"
                        res = juhe
                    else:
                        res["fallbackError"] = juhe.get("error")
                self._send(res)
            elif parsed.path == "/cheapest":
                date = qs.get("date", [datetime.date.today().strftime("%Y-%m-%d")])[0]
                raw = qs.get("q", [""])[0]
                if not raw:
                    self._send({"ok": False, "error": "缺少 q 参数，例如 /cheapest?q=从成都到大理最便宜的方案"}, 400)
                    return
                _, err = valid_date(date)
                if err:
                    self._send({"ok": False, "error": err}, 400)
                    return
                self._send(query_cheapest(raw, date))
            else:
                self._send({"ok": False, "error": "not found"}, 404)
        except urllib.error.HTTPError as e:
            self._send({"ok": False, "error": "上游接口错误 %s" % e.code}, 502)
        except Exception as e:
            self._send({"ok": False, "error": "服务异常: %s" % e}, 502)

    def _resolve_loc(self, loc):
        """把地点名或经纬度解析为 location 对象。loc 支持：
           - 地名：五台山 / 徐州 / 济南
           - 经纬度：经度,纬度 如 117.28,34.20
        返回 (loc_id, loc_name, loc_adm) 或抛 ValueError。"""
        loc = (loc or "").strip()
        if not loc:
            return XZ_LOC, "徐州", "江苏省徐州市"
        # 经纬度格式：两个数字用逗号分隔（经度,纬度）
        m = re.match(r"^\s*(-?\d+(?:\.\d+)?)\s*[,，]\s*(-?\d+(?:\.\d+)?)\s*$", loc)
        if m:
            lon, lat = m.group(1), m.group(2)
            try:
                rev = json.loads(http_get(
                    QWEATHER_HOST + "/geo/v2/city/lookup?location="
                    + urllib.parse.quote(lon + "," + lat) + "&key=" + QWEATHER_KEY + "&number=1"))
            except urllib.error.HTTPError as e:
                raise ValueError("定位查询失败（HTTP %s）" % e.code)
            hits = rev.get("location") or []
            if not hits:
                raise ValueError("当前位置附近没有可用的天气数据，请手动输入城市或景区名")
            hit = hits[0]
            return str(hit["id"]), hit.get("name", "当前位置"), (hit.get("adm1", "") or "") + (hit.get("adm2", "") or "")
        try:
            lookup = json.loads(http_get(
                QWEATHER_HOST + "/geo/v2/city/lookup?location="
                + urllib.parse.quote(loc) + "&key=" + QWEATHER_KEY + "&number=1"))
        except urllib.error.HTTPError:
            raise ValueError("没认出地点「%s」，请用城市或景区名，如 五台山 / 济南" % loc)
        hits = lookup.get("location") or []
        if not hits:
            raise ValueError("没认出地点「%s」，请用城市或景区名，如 五台山 / 济南" % loc)
        hit = hits[0]
        return str(hit["id"]), hit.get("name", loc), (hit.get("adm1", "") or "") + (hit.get("adm2", "") or "")

    def api_weather(self):
        if not QWEATHER_KEY:
            return {"ok": False, "error": "天气服务未配置：请在 proxy.py 同目录创建 qweather_key.txt 并填入和风天气 Key。"}
        qs = urllib.parse.parse_qs(urllib.parse.urlparse(self.path).query)
        loc = (qs.get("loc", [""])[0] or "").strip()
        try:
            loc_id, loc_name, loc_adm = self._resolve_loc(loc)
        except ValueError as e:
            return {"ok": False, "error": str(e)}
        now = json.loads(http_get(
            QWEATHER_HOST + "/v7/weather/now?location=" + loc_id + "&key=" + QWEATHER_KEY))
        d7 = json.loads(http_get(
            QWEATHER_HOST + "/v7/weather/7d?location=" + loc_id + "&key=" + QWEATHER_KEY))
        # 逐小时预报（未来 24h）
        hourly = []
        try:
            h24 = json.loads(http_get(
                QWEATHER_HOST + "/v7/weather/24h?location=" + loc_id + "&key=" + QWEATHER_KEY))
            hourly = h24.get("hourly", [])
        except Exception:
            hourly = []
        # 日出日落（未来几天）
        sun = []
        try:
            astro = json.loads(http_get(
                QWEATHER_HOST + "/v7/astronomy/sun?location=" + loc_id + "&key=" + QWEATHER_KEY
                + "&date=" + datetime.date.today().strftime("%Y%m%d")))
            sun = astro.get("sunrise", "") or ""
            sunset = astro.get("sunset", "")
            if sunset:
                sun = [sun, sunset] if isinstance(sun, str) else sun
        except Exception:
            sun = []
        return {
            "ok": True,
            "location": {"id": loc_id, "name": loc_name, "adm": loc_adm},
            "query": loc or "徐州",
            "updateTime": now.get("updateTime", ""),
            "now": now.get("now", {}),
            "daily": d7.get("daily", []),
            "hourly": hourly,
            "sun": sun,
        }

    def _proxy_deepseek(self):
        """POST /deepseek/chat/completions 透传：
        从 config.txt 读取 DeepSeek Key，在服务端拼接 Authorization，把前端请求体原样转发到
        https://api.deepseek.com/chat/completions，并原样返回。支持 stream:true 的 SSE 逐块透传
        （逐行 readline + flush，不整包缓存），前端全程不持有密钥。"""
        length = int(self.headers.get("Content-Length", 0) or 0)
        raw = self.rfile.read(length)
        try:
            body = json.loads(raw.decode("utf-8"))
        except Exception:
            self._err_json("请求体不是合法 JSON", 400)
            return
        try:
            key = load_key()
        except Exception:
            self._err_json("未找到 config.txt，请把完整 DeepSeek Key 填进去。", 500)
            return

        stream = bool(body.get("stream"))
        payload = json.dumps(body, ensure_ascii=False).encode("utf-8")
        req = urllib.request.Request(
            "https://api.deepseek.com/chat/completions",
            data=payload,
            headers={
                "Authorization": "Bearer " + key,
                "Content-Type": "application/json",
                "Accept": "text/event-stream" if stream else "application/json",
            },
            method="POST",
        )
        try:
            upstream = urllib.request.urlopen(req, timeout=300, context=SSL_CTX)
        except urllib.error.HTTPError as e:
            detail = e.read().decode("utf-8", "ignore")
            self._send_raw(detail, e.code, "application/json")
            return
        except Exception as e:
            self._err_json("上游连接失败: %s" % e, 502)
            return

        status = upstream.getcode()
        if stream:
            # 流式 SSE：逐行透传，关闭 Nginx 缓冲，靠连接关闭界定 body 长度
            self.protocol_version = "HTTP/1.1"
            self.send_response(status)
            self.send_header("Content-Type", "text/event-stream; charset=utf-8")
            self.send_header("Cache-Control", "no-cache")
            self.send_header("X-Accel-Buffering", "no")
            self.send_header("Access-Control-Allow-Origin", "*")
            self.send_header("Connection", "close")
            self.end_headers()
            self.close_connection = True
            try:
                while True:
                    line = upstream.readline()
                    if not line:
                        break
                    self.wfile.write(line)
                    self.wfile.flush()
            except Exception:
                pass
            finally:
                upstream.close()
        else:
            data = upstream.read()
            upstream.close()
            self._send_raw(data.decode("utf-8", "ignore"), status, "application/json")

    def do_POST(self):
        parsed = urllib.parse.urlparse(self.path)
        if parsed.path == "/deepseek/chat/completions":
            self._proxy_deepseek()
            return
        if parsed.path != "/chat":
            self._send({"ok": False, "error": "not found"}, 404)
            return
        length = int(self.headers.get("Content-Length", 0))
        try:
            body = json.loads(self.rfile.read(length).decode("utf-8"))
            messages = body.get("messages", [])
            if not messages:
                raise ValueError("empty messages")
        except Exception as e:
            self._send({"ok": False, "error": "请求格式错误: %s" % e}, 400)
            return
        try:
            key = load_key()
        except Exception:
            self._send({"ok": False, "error": "未找到 config.txt，请把完整 DeepSeek Key 填进去。"}, 500)
            return
        payload = json.dumps({
            "model": MODEL,
            "input": messages,
            "tools": [{"type": "web_search"}],
        }).encode("utf-8")
        req = urllib.request.Request(
            API_URL, data=payload,
            headers={"Authorization": "Bearer " + key, "Content-Type": "application/json"},
            method="POST",
        )
        try:
            with urllib.request.urlopen(req, timeout=120, context=SSL_CTX) as resp:
                data = json.loads(resp.read().decode("utf-8"))
            answer = ""
            if "output" in data:
                for item in data["output"]:
                    for c in item.get("content", []):
                        if "text" in c:
                            answer += c["text"]
            elif "choices" in data:
                answer = data["choices"][0]["message"]["content"]
            if not answer:
                self._send({"ok": False, "error": "DeepSeek 返回为空，请重试。"}, 502)
            else:
                self._send({"ok": True, "answer": answer})
        except urllib.error.HTTPError as e:
            detail = e.read().decode("utf-8", "ignore")
            try:
                detail = json.loads(detail).get("error", {}).get("message", detail)
            except Exception:
                pass
            self._send({"ok": False, "error": "DeepSeek 返回错误 (%s): %s" % (e.code, detail)}, 502)
        except Exception as e:
            self._send({"ok": False, "error": "网络请求失败: %s" % e}, 502)

    def log_message(self, fmt, *args):
        sys.stdout.write("[proxy %s] %s\n" % (time.strftime("%H:%M:%S"), fmt % args))


if __name__ == "__main__":
    # 端口自愈：优先用 YANZI_PORT，被占用则依次尝试 8899/8901/8902/8903
    ports = []
    if os.environ.get("YANZI_PORT"):
        ports.append(int(os.environ["YANZI_PORT"]))
    for p in (8899, 8901, 8902, 8903):
        if p not in ports:
            ports.append(p)

    server = None
    bound_port = None
    for p in ports:
        try:
            server = ThreadingHTTPServer((HOST, p), Handler)
            bound_port = p
            break
        except OSError as e:
            print("[跳过] 端口 %s 不可用（%s），尝试下一个..." % (p, e))
            server = None
    if server is None:
        print("错误：8899/8901/8902/8903 全部被占用，无法启动。请先关闭旧代理窗口。")
        sys.exit(1)

    print("=" * 48)
    print("燕子户外本地代理已启动：http://%s:%s" % (HOST, bound_port))
    print("能力：/health 健康检查 · /weather 天气 · /train 车次时刻")
    print("      /tickets 站站余票 · /cheapest 最便宜方案 · /deepseek 对话透传")
    print("前端会自动发现本端口，无需手动配置。请保持本窗口打开。")
    print("=" * 48)
    server.serve_forever()
