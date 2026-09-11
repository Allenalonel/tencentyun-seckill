"""腾讯云秒杀脚本.

流程: 加载 cookies.json 建会话 -> 开抢前自检会话 -> 对时等待 ->
提前 LEAD_MS 开打 -> 每轮 3 地域并发下单, 循环直到成功或超时.

每期开抢前只需核对顶部 CONFIG 区 (F12 抓包抄 Request Payload),
活动 URL 由 get_cookies.py 扫码时自动同步到 config.json.
"""

import contextlib
import copy
import json
import os
import random
import re
import sys
import time
import webbrowser
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta

import requests

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
COOKIES_PATH = os.path.join(BASE_DIR, "cookies.json")
AUTO_CONFIG_PATH = os.path.join(BASE_DIR, "config.json")
LOG_DIR = os.path.join(BASE_DIR, "logs")


class Tee:
    """双写: 控制台 + 落盘(每次写即 open/close, 崩溃也不丢日志)."""

    def __init__(self, stream, log_path):
        self.stream = stream
        self.log_path = log_path

    def write(self, data):
        self.stream.write(data)
        with open(self.log_path, "a", encoding="utf-8") as f:
            f.write(data)
        self.stream.flush()

    def flush(self):
        self.stream.flush()

    def isatty(self):
        return self.stream.isatty() if hasattr(self.stream, "isatty") else False


def setup_logging():
    """__main__ 首行调用: 之后所有 print 同步落盘到 logs/snap_*.log, 返回日志路径."""
    os.makedirs(LOG_DIR, exist_ok=True)
    log_path = os.path.join(LOG_DIR, f"snap_{time.strftime('%Y%m%d_%H%M%S')}.log")
    with contextlib.suppress(Exception):
        for stream in (sys.stdout, sys.stderr):
            reconfig = getattr(stream, "reconfigure", None)
            if callable(reconfig):
                reconfig(encoding="utf-8")  # GBK 控制台防 emoji 炸
    sys.stdout = Tee(sys.stdout, log_path)
    sys.stderr = Tee(sys.stderr, log_path)
    return log_path


# ================= CONFIG: 每期开抢前 F12 核对本区 =================
CONFIG = {
    # --- 人工确认: 秒杀时间(北京时间)、地域优先级 ---
    "SECKILL_TIME_STR": "2026-09-11 11:14:00",
    "REGION_PRIORITY": [1, 4, 8],
    "REGION_NAMES": {1: "华北", 4: "华东", 8: "华南"},
    # --- 开打策略 ---
    "LEAD_MS": 1500,  # 提前量: 早打靠重试吸收, 绝不晚打
    "RETRY_WINDOW_S": 30,  # 开闸后重试窗口(秒), 成功立即停
    "BACKOFF_408_S": 1.5,  # 触发限流(408)后的退避秒数
    "BUY_TIMEOUT_S": 3.0,  # 下单单请求超时: 拥塞时快速失败, 多打几轮
    "ROUND_GAP_S": 0.35,  # 轮间基础间隔(秒): 首轮直接打, 轮间喘息降限流
    "ROUND_JITTER_S": 0.25,  # 轮间抖动上限: 实际间隔 = 基础 + uniform(0, 抖动)
    "VERBOSE": False,  # True 则打印每次下单完整返回体 (排查用, 默认只看摘要)
    "WAIT_POLL_S": 1.0,  # 远未到点时的轮询间隔
    "NEAR_POLL_S": 0.2,  # 最后 5 秒的轮询间隔
    "SYNC_AHEAD_S": 60,  # 提前多少秒做一次性钟差校准
    "SYNC_SAMPLES": 5,  # 校准采样数 (取中位数抗抖动)
    # --- F12 抓包抄 Request Payload (每期必核对) ---
    # 2026-09-09 Playwright 实测 featured-202607/warmup-202606: 会场 164461404341040, 9-10场商品 1897632168296710(查/买同ID)
    "ACTIVITY_ID": 164461404341040,
    # 注意: 查库存与下单用的 act_id 历史上不一致, 本期实测一致, 开抢前仍须抓包核对二者
    "CHECK_ACT_ID": 1897632168296710,
    "BUY_ACT_ID": 1897632168296710,
    "SKU": "bundle_budget_mc_lg4_01",
    "BUSINESS": {"id": 24448, "from": "lightningDeals"},
    "GOODS_PARAM": {  # 下单 goods_param 模板, regionId 下单时填入
        "BlueprintId": "LINUX_UNIX",
        "area": 1,
        "ddocUnionConnect": 0,
        "goodsNum": 1,
        "imageId": "lhbp-eqora508",
        "scenario": "0",
        "timeSpanUnit": "12m",
        "zone": "",
        "type": "bundle_budget_mc_lg4_01",
    },
    # --- 手动兜底项 ---
    # x-csrf-token 由 skey 自动计算, 下面手填值仅在 cookies.json 无 skey 时兜底
    # (已验证 qcmainCSRFToken 明文不可用, 不要直接填它)
    "X_CSRF_TOKEN_MANUAL": "90658945",
    # 活动页 URL: 优先用 get_cookies.py 扫码时自动同步的, 下面是兜底
    "ACT_URL_FALLBACK": (
        "https://cloud.tencent.com/act/pro/featured-202607"
        "?fromSource=gwzcw.10216579.10216579.10216579&utm_medium=cpc"
        "&utm_id=gwzcw.10216579.10216579.10216579"
        "&msclkid=9d471e943d2d142808a4771f328779e6&page=warmup-202606"
        "&s_source=https%3A%2F%2Fcloud.tencent.com%2Fact%2Fpro%2Fdouble12-2025"
    ),
    "SERVER_TIME_URL": "https://cloud.tencent.com/act/pro/double12-2025",
}


def load_auto_config():
    """读取扫码时自动同步的配置 (config.json), 不存在则返回空."""
    try:
        with open(AUTO_CONFIG_PATH, encoding="utf-8") as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return {}


def get_act_url():
    """活动页 URL: 自动同步优先, 手填兜底."""
    return load_auto_config().get("act_url") or CONFIG["ACT_URL_FALLBACK"]


def compute_g_tk(skey):
    """腾讯经典 g_tk 算法: x-csrf-token 由 skey hash 得出.

    已验证 cookies.json 里的 qcmainCSRFToken 明文不可用
    (三个域的值调 check-available 全返回 CSRF-ERROR), 必须用 skey 现算.
    """
    h = 5381
    for ch in skey:
        h += (h << 5) + ord(ch)
    return str(h & 0x7FFFFFFF)


def build_session():
    """加载 cookies.json 建会话, 返回 (session, csrf_token, token_source)."""
    if not os.path.exists(COOKIES_PATH):
        raise FileNotFoundError("找不到 cookies.json, 请先运行 get_cookies.py 扫码登录")
    with open(COOKIES_PATH, encoding="utf-8") as f:
        cookies = json.load(f)
    session = requests.Session()
    skey = None
    for cookie in cookies:
        session.cookies.set(
            cookie.get("name", ""),
            cookie.get("value", ""),
            domain=cookie.get("domain", ""),
            path=cookie.get("path", "/"),
        )
        # skey 多域同值, 优先 .cloud.tencent.com 的
        if (
            cookie.get("name") == "skey"
            and cookie.get("value")
            and (skey is None or cookie.get("domain") == ".cloud.tencent.com")
        ):
            skey = cookie["value"]
    if skey:
        return session, compute_g_tk(skey), "skey 自动计算(g_tk)"
    return session, str(CONFIG["X_CSRF_TOKEN_MANUAL"]), "CONFIG 手填值(兜底)"


def make_headers(csrf_token):
    act_url = get_act_url()
    return {
        "x-csrf-token": csrf_token,
        "Content-Type": "application/json",
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36"
        ),
        "referer": act_url,
    }


CLOCK_SKEW_MS = 0  # 本地钟差 (server - local): sync_clock() 一次性测准, 之后零网络


def now_ms():
    """当前毫秒时间戳 (本地钟 + 已校准钟差)."""
    return int(time.time() * 1000) + CLOCK_SKEW_MS


def sync_clock():
    """提前一次性校准钟差, 取 SYNC_SAMPLES 个样本的中位数.

    Date 头只有秒精度: 取整截断误差用 +500ms 对中, RTT 取半计入.
    失败不抛异常, 沿用上次值 (首次失败即 skew=0).
    """
    global CLOCK_SKEW_MS
    skews = []
    last_err = None
    samples = CONFIG["SYNC_SAMPLES"]
    for _ in range(samples):
        try:
            t0 = int(time.time() * 1000)
            resp = requests.head(CONFIG["SERVER_TIME_URL"], timeout=5)
            t1 = int(time.time() * 1000)
            server_time = resp.headers.get("Date")
            if not server_time:
                continue
            dt = datetime.strptime(server_time, "%a, %d %b %Y %H:%M:%S GMT")
            date_ms = int((dt + timedelta(hours=8)).timestamp() * 1000)
            server_at_t1 = date_ms + 500 + (t1 - t0) // 2
            skews.append(server_at_t1 - t1)
        except Exception as e:
            last_err = e
            continue
    if skews:
        skews.sort()
        CLOCK_SKEW_MS = skews[len(skews) // 2]
        print(f"🕒 钟差已校准: {CLOCK_SKEW_MS}ms ({len(skews)}/{samples}样本中位数)")
    else:
        print(f"⚠️ 钟差校准失败 ({last_err}), 用本地钟 (skew=0)")
    return CLOCK_SKEW_MS


def fetch_goods_regions(session, headers):
    """拉取本商品 regionMap 全量地域ID (查库存时与页面发一致).

    页面对其它商品发的是全量列表 (如 [1,4,8,16,33]), 只发子集后面若被
    严格校验会 400. 本商品 regionMap 在活动页 __NEXT_DATA__ 里.
    失败回退 REGION_PRIORITY. 每进程只在开抢前自检时调一次.
    """
    fallback = list(CONFIG["REGION_PRIORITY"])
    try:
        resp = session.get(get_act_url(), headers=headers, timeout=15)
        html = resp.text
    except Exception as e:
        print(f"⚠️ 地域列表拉取失败, 用优先级列表兜底: {e}")
        return fallback
    act_id = str(CONFIG["CHECK_ACT_ID"])
    # 只认销售条目起点 ("act_id":X 紧跟 "goods_name"), regionMap 取到下一条目为止,
    # 避免串入相邻商品 (如隔壁流量包的中国香港 region 5)
    entries = list(re.finditer(r'"act_id":(\d+),"goods_name":"', html))
    found = []
    for i, entry in enumerate(entries):
        if entry.group(1) != act_id:
            continue
        end = entries[i + 1].start() if i + 1 < len(entries) else entry.start() + 20000
        window = html[entry.start() : end]
        matches = re.finditer(
            r'\{"value":(\d+),"label":"[^"]*","regionEn":"[^"]*"\}', window
        )
        for m in matches:
            rid = int(m.group(1))
            if rid not in found:
                found.append(rid)
    if not found:
        print("⚠️ 未解析到商品地域表, 用优先级列表兜底")
        return fallback
    ordered = [r for r in CONFIG["REGION_PRIORITY"] if r in found]
    ordered += [r for r in found if r not in ordered]
    print(f"🗺️ 查库存地域 (页面全量): {ordered}")
    return ordered


def check_available(session, headers):
    """查库存, 返回 (接口正常, 按优先级排序的有货地域ID列表).

    地域自动发现: 优先 REGION_PRIORITY 顺序, 其余有货地域自动追加,
    本期新增地域无需改代码.
    """
    check_data = {
        "activity_id": CONFIG["ACTIVITY_ID"],
        "goods": [
            {
                "act_id": CONFIG["CHECK_ACT_ID"],
                "region_id": fetch_goods_regions(session, headers),
            }
        ],
        "preview": 0,
    }
    try:
        resp = session.post(
            "https://act-api.cloud.tencent.com/dianshi/check-available",
            json=check_data,
            headers=headers,
            timeout=10,
        )
        resp.raise_for_status()
        result = resp.json()
    except Exception as e:
        print(f"❌ 库存检查接口调用失败: {e}")
        return False, []
    if result.get("code") != 0 or result.get("msg") != "ok":
        print(f"❌ 库存检查接口返回异常: {json.dumps(result, ensure_ascii=False)}")
        return False, []
    goods_data = result.get("data", [{}])[0]
    quota = goods_data.get("quota", {})
    sku = CONFIG["SKU"]
    in_stock = [
        rid
        for rid in CONFIG["REGION_PRIORITY"]
        if quota.get(str(rid), {}).get(sku, {}).get("available", 0) > 0
    ]
    # 自动发现: 优先级列表之外的有货地域也纳入
    for key, val in quota.items():
        try:
            rid = int(key)
        except (TypeError, ValueError):
            continue
        if (
            rid not in CONFIG["REGION_PRIORITY"]
            and (val or {}).get(sku, {}).get("available", 0) > 0
        ):
            print(f"🔍 发现优先级列表外的新地域有货: region_id={rid}")
            in_stock.append(rid)
    return True, in_stock


def face_from_config():
    """登录脚本写入的人脸状态兜底 (24 小时内有效). 查不到返回 None."""
    record = load_auto_config().get("facecheck") or {}
    if record.get("ever_pass") is not True:
        return None
    try:
        checked = datetime.strptime(record.get("checked", ""), "%Y-%m-%d %H:%M:%S")
    except ValueError:
        return None
    if datetime.now() - checked < timedelta(hours=24):
        print(f"ℹ️ 沿用登录时的人脸状态 (已通过, {record['checked']} 记录)")
        return True
    return None


def check_face(session, headers):
    """查人脸核身状态, 返回 True(已通过)/False(需核身)/None(查不到).

    抢购窗口内 do-goods 才报 1100202, 等开打才知道就晚了;
    user/get-facecheck-info 的 ever_pass 可提前预警.
    实时查询失败时, 兜底读登录脚本写入 config.json 的状态.
    """
    try:
        resp = session.post(
            "https://act-api.cloud.tencent.com/user/get-facecheck-info",
            json={},
            headers=headers,
            timeout=10,
        )
        result = resp.json()
    except Exception as e:
        print(f"⚠️ 人脸核身状态查询失败: {e}")
        return face_from_config()
    if result.get("code") != 0:
        print(f"⚠️ 人脸核身状态查询异常: {result}")
        return face_from_config()
    return bool((result.get("data") or {}).get("ever_pass"))


def region_label(rid):
    name = CONFIG["REGION_NAMES"].get(rid, "未知地域")
    return f"{name}(region_id={rid})"


def buy_now(session, headers, region_id):
    """对单个地域下单, 返回 (region_id, 结果dict/None)."""
    goods_param = copy.deepcopy(CONFIG["GOODS_PARAM"])
    goods_param["regionId"] = region_id
    do_data = {
        "activity_id": CONFIG["ACTIVITY_ID"],
        "agent_channel": {
            "fromChannel": "",
            "fromSales": "",
            "isAgentClient": False,
            "fromUrl": get_act_url(),
        },
        "business": dict(CONFIG["BUSINESS"]),
        "goods": [
            {
                "act_id": CONFIG["BUY_ACT_ID"],
                "type": CONFIG["SKU"],
                "goods_param": goods_param,
            }
        ],
        "preview": 0,
    }
    try:
        resp = session.post(
            "https://act-api.cloud.tencent.com/dianshi/do-goods",
            json=do_data,
            headers=headers,
            timeout=CONFIG["BUY_TIMEOUT_S"],
        )
        if CONFIG["VERBOSE"]:
            print(f"🎯 下单接口返回 [{region_label(region_id)}]: {resp.text}")
        return region_id, resp.json()
    except Exception as e:
        if CONFIG["VERBOSE"]:
            print(f"❌ 下单接口调用失败 [{region_label(region_id)}]: {e}")
        return region_id, None


# do-goods 返回码分类 (2026-09-10 两场实战沉淀: 1100202/408/1100205)
FATAL_CODES = {1100202}  # 确定性失败 (如人脸核身): 重试无意义, 直接停, 省配额
AUTH_CODES = {"CSRF-ERROR", "NOT-LOGINED"}  # 会话失效: 重试无意义, 直接停
RATE_CODES = {408}  # 触发限流: 退避后继续
UNPAID_CODES = {1101411}  # 有未支付订单: 重试无意义, 停手弹订单页 (2026-09-11 实战)


def result_code(result):
    """取 do-goods 返回码 (兼容 int/str/网络异常None)."""
    if isinstance(result, dict):
        return result.get("code")
    return None


CODE_TAGS = {
    0: "成功",
    1100202: "需人脸",
    408: "限流",
    1100205: "繁忙",
    1101411: "有未付单",
    None: "异常",
}


def code_tag(code):
    """返回码短标签 (未知码直接显示原值)."""
    return CODE_TAGS.get(code, str(code))


def hist_line(hist):
    """计数 dict -> '限流×3, 繁忙×1' (按次数降序)."""
    return ", ".join(
        f"{code_tag(code)}×{n}"
        for code, n in sorted(hist.items(), key=lambda kv: (-kv[1], str(kv[0])))
    )


def extract_order_url(result):
    """从 1101411 这类返回的 msg 里抠订单链接 (bigDealId 优先)."""
    msg = result.get("msg", "") if isinstance(result, dict) else ""
    if not isinstance(msg, str) or not msg:
        return None
    m = re.search(r"bigDealId=(\d+)", msg)
    if m:
        return f"https://console.cloud.tencent.com/expense/deal/info?bigDealId={m.group(1)}"
    m = re.search(r'href="(//console\.cloud\.tencent\.com[^"]+)"', msg)
    if m:
        return "https:" + m.group(1)
    return None


def handle_unpaid(order_url):
    """有未支付订单: 停手 + 弹订单页 (占着单重试全是 1101411)."""
    print("🛑 检测到未支付订单, 继续重试无意义, 收工")
    if order_url and order_url.startswith("http"):
        print(f"🧾 订单页: {order_url}")
        try:
            webbrowser.open(order_url)
            print("🌐 已弹出浏览器订单页, 付款或取消后才能继续抢")
        except Exception as e:
            print(f"⚠️ 自动弹订单页失败 ({e}), 请手动打开上面链接")
    else:
        print(
            "ℹ️ 请去控制台订单中心处理未支付订单: "
            "https://console.cloud.tencent.com/expense/order"
        )


def buy_round(session, headers, region_ids, round_no):
    """一轮并发下单, 只打印一行摘要.

    返回 (outcome, detail, hist): success=抢到; fatal=确定性失败(含原因);
    rate_limited=被限流; retry=继续下一轮; unpaid=有未付单(detail 为订单链接).
    hist 为本轮 code->次数.
    """
    if not region_ids:
        return "retry", "空地域列表", {}, None
    hist = Counter()
    hits = []  # (code, result)
    with ThreadPoolExecutor(max_workers=len(region_ids)) as executor:
        futures = {
            executor.submit(buy_now, session, headers, rid): rid for rid in region_ids
        }
        for future in as_completed(futures):
            rid, result = future.result()
            code = result_code(result)
            if code == 0:
                print(f"—— 第 {round_no} 轮 —— 🎉 抢购成功! {region_label(rid)}")
                return "success", region_label(rid), dict(hist), result
            hist[code] += 1
            hits.append((code, result))
    print(f"—— 第 {round_no} 轮 —— {hist_line(hist)}")
    for code, result in hits:
        if code in UNPAID_CODES:
            return "unpaid", extract_order_url(result), dict(hist), None
    for code, result in hits:
        if code in FATAL_CODES or code in AUTH_CODES:
            msg = result.get("msg", "") if isinstance(result, dict) else result
            return "fatal", f"code={code} {msg}", dict(hist), None
    if any(code in RATE_CODES for code, _ in hits):
        return "rate_limited", "触发限流", dict(hist), None
    return "retry", "继续", dict(hist), None


# 成功返回体结构未知: 宽匹配订单号/付款链接 (key 大小写不敏感)
PAY_URL_KEYS = frozenset(
    [
        "pay_url",
        "payurl",
        "cashier_url",
        "cashierurl",
        "pay_link",
        "paylink",
        "redirect_url",
        "redirecturl",
        "pay_gate_url",
    ]
)
ORDER_ID_KEYS = frozenset(
    [
        "order_id",
        "orderid",
        "order_no",
        "orderno",
        "deal_id",
        "dealid",
        "bill_id",
        "billid",
        "transaction_id",
        "transactionid",
    ]
)
ORDER_KEYS = PAY_URL_KEYS | ORDER_ID_KEYS


def find_keys(obj, found):
    """递归搜集订单/付款相关 key, found: 小写key -> [value...]."""
    if isinstance(obj, dict):
        items = list(obj.items())
    elif isinstance(obj, list):
        items = list(enumerate(obj))
    else:
        return found
    for k, v in items:
        if isinstance(k, str) and k.lower() in ORDER_KEYS:
            found.setdefault(k.lower(), []).append(v)
        find_keys(v, found)
    return found


def _collect_pay_url(info, key, v):
    """命中付款 key 且值为 http 链接则收录 (守卫子句, 无嵌套)."""
    if key not in PAY_URL_KEYS or not isinstance(v, str):
        return
    if not v.startswith("http") or v in info["pay_urls"]:
        return
    info["pay_urls"].append(v)


def _collect_order_id(info, key, v):
    """命中订单 key 且值有效则收录 (守卫子句, 无嵌套)."""
    if key not in ORDER_ID_KEYS or v in (None, ""):
        return
    if str(v) in [str(x) for x in info["order_ids"]]:
        return
    info["order_ids"].append(v)


def extract_order_info(result):
    """从成功返回体提取订单号/付款链接."""
    info = {"order_ids": [], "pay_urls": []}
    for key, vals in find_keys(result, {}).items():
        for v in vals:
            _collect_pay_url(info, key, v)
            _collect_order_id(info, key, v)
    return info


def handle_success(payload):
    """抢到: 完整返回打屏(进日志)、提取订单/付款信息、能弹付款页就弹."""
    print("🎉 抢购成功! 完整返回:")
    if isinstance(payload, dict):
        print(json.dumps(payload, ensure_ascii=False, indent=2))
    else:
        print(payload)
        return
    info = extract_order_info(payload)
    if info["order_ids"]:
        print(f"🧾 订单号: {', '.join(str(x) for x in info['order_ids'])}")
    if info["pay_urls"]:
        print(f"💳 付款链接: {info['pay_urls'][0]}")
        try:
            webbrowser.open(info["pay_urls"][0])
            print("🌐 已弹出浏览器付款页, 请尽快完成支付 (超时订单会释放)")
        except Exception as e:
            print(f"⚠️ 自动弹付款页失败 ({e}), 请手动打开上面链接付款")
    else:
        print(
            "ℹ️ 未解析到直接付款链接, 请去控制台订单中心付款: "
            "https://console.cloud.tencent.com/expense/order"
        )


def wait_until(target_ms):
    """sleep 等到目标时间, 只用本地钟+钟差, 零网络.

    远端大步睡, 临近 5 秒加密; 睡眠步长钳住不越过目标点.
    """
    while True:
        remain = target_ms - now_ms()
        if remain <= 0:
            return
        if remain > 5000:
            time.sleep(min(CONFIG["WAIT_POLL_S"], remain / 2000.0))
        else:
            print(f"⏳ 临近开抢, 剩余 {remain}ms")
            time.sleep(min(CONFIG["NEAR_POLL_S"], remain / 2000.0))


if __name__ == "__main__":
    log_path = setup_logging()
    print("🚀 启动腾讯云抢购脚本...")
    print(f"📝 日志落盘: {log_path}")
    session, csrf_token, token_source = build_session()
    print(f"🔑 x-csrf-token 来源: {token_source}")
    headers = make_headers(csrf_token)

    # 开抢前人脸核身预检: 窗口内才报 1100202 就晚了, 这里提前预警
    face = check_face(session, headers)
    if face is True:
        print("✅ 人脸核身已通过 (ever_pass)")
    elif face is False:
        print(
            "⛔ 账号需人脸核身校验, 开抢必报 1100202! 请先去 "
            "https://cloud.tencent.com/act/pro/face_authentication "
            "完成验证 (微信扫一扫+人脸识别), 仍将按时开打"
        )

    # 开抢前会话自检: 接口通且已登录才继续 (只验连通性, 不验有无库存)
    ok, pre_stock = check_available(session, headers)
    if not ok:
        print(
            "⚠️ 会话自检未通过 (接口异常): 请检查 cookies.json / csrf 是否过期, "
            "仍将按时开打"
        )
    elif pre_stock:
        print(f"ℹ️ 提前查到有货地域: {[region_label(r) for r in pre_stock]}")

    seckill_ms = (
        int(time.mktime(time.strptime(CONFIG["SECKILL_TIME_STR"], "%Y-%m-%d %H:%M:%S")))
        * 1000
    )
    print(
        f"🎯 秒杀时间(本地解析): {CONFIG['SECKILL_TIME_STR']} "
        f"({seckill_ms}), 提前 {CONFIG['LEAD_MS']}ms 开打"
    )

    # 对时: 远未到点先睡, 提前 SYNC_AHEAD_S 一次性校准, 之后零网络等开打
    sync_point = seckill_ms - CONFIG["LEAD_MS"] - CONFIG["SYNC_AHEAD_S"] * 1000
    if now_ms() < sync_point:
        print("💤 距离开抢还远, 休眠至校准点...")
        wait_until(sync_point)
    sync_clock()
    wait_until(seckill_ms - CONFIG["LEAD_MS"])
    print("🔥 开打!")
    deadline_ms = seckill_ms + CONFIG["RETRY_WINDOW_S"] * 1000
    round_no = 0
    totals = Counter()
    while True:
        round_no += 1
        outcome, detail, hist, payload = buy_round(
            session, headers, CONFIG["REGION_PRIORITY"], round_no
        )
        totals.update(hist)
        if outcome == "success":
            handle_success(payload)
            break
        if outcome == "unpaid":
            handle_unpaid(detail)
            break
        if outcome == "fatal":
            print(f"🛑 {detail}, 收工 (重试无意义)")
            break
        if outcome == "rate_limited":
            backoff = CONFIG["BACKOFF_408_S"]
            print(f"🐢 {detail}, 退避 {backoff}s 后继续")
            time.sleep(backoff)
        # 轮间喘息 + 抖动: 避免固定频率 hammer, 首轮不受影响 (直接开打)
        gap = CONFIG["ROUND_GAP_S"] + random.uniform(0, CONFIG["ROUND_JITTER_S"])
        time.sleep(gap)
        if now_ms() > deadline_ms:
            print(f"🛑 超过重试窗口 ({CONFIG['RETRY_WINDOW_S']}s), 收工")
            break
    if totals:
        print(f"📊 本场统计 (共{round_no}轮): {hist_line(totals)}")
    print("✅ 脚本结束")
