"""扫码登录腾讯云, 保存 cookies.json, 并自动同步活动 URL 到 config.json.

登录成功后不再写死活动期号: 只要地址栏跳到任意 act/pro/* 活动页即停,
把最终 URL 写入 config.json, 供 snap_up_server.py 自动读取
(referer / fromUrl / 时间校准都用它).

登录后顺带确认人脸核身状态 (user/get-facecheck-info ever_pass):
没通过则在同一浏览器打开验证页, 等用户做完再退出,
状态写入 config.json["facecheck"] 供抢购脚本读取.
"""

import contextlib
import json
import os
import time

import requests
from playwright.sync_api import sync_playwright

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
COOKIES_PATH = os.path.join(BASE_DIR, "cookies.json")
CONFIG_PATH = os.path.join(BASE_DIR, "config.json")

# 登录入口(稳定); 登录后跳到哪一期活动页则自动捕获, 无需手写
LOGIN_URL = (
    "https://cloud.tencent.com/login"
    "?s_url=https%3A%2F%2Fcloud.tencent.com%2Fact%2Fpro%2Fdouble12-2025"
    "%3FfromSource%3Dgwzcw.10216579.10216579.10216579%26utm_medium%3Dcpc"
    "%26utm_id%3Dgwzcw.10216579.10216579.10216579"
    "%26msclkid%3D9d471e943d2d142808a4771f328779e6"
)

FACE_URL = "https://cloud.tencent.com/act/pro/face_authentication"
FACE_POLL_S = 15
FACE_TIMEOUT_S = 600  # 验证页最多等 10 分钟


def compute_g_tk(skey):
    """腾讯经典 g_tk 算法: x-csrf-token 由 skey hash 得出, 与 snap_up_server.py 同逻辑."""
    h = 5381
    for ch in skey:
        h += (h << 5) + ord(ch)
    return str(h & 0x7FFFFFFF)


def query_face_pass(cookies):
    """查人脸核身状态, 返回 True/False/None(查不到). 永不抛异常."""
    try:
        session = requests.Session()
        skey = None
        for cookie in cookies:
            session.cookies.set(
                cookie.get("name", ""),
                cookie.get("value", ""),
                domain=cookie.get("domain", ""),
                path=cookie.get("path", "/"),
            )
            if (
                cookie.get("name") == "skey"
                and cookie.get("value")
                and (skey is None or cookie.get("domain") == ".cloud.tencent.com")
            ):
                skey = cookie["value"]
        if not skey:
            return None
        headers = {
            "x-csrf-token": compute_g_tk(skey),
            "Content-Type": "application/json",
            "User-Agent": "Mozilla/5.0",
            "referer": FACE_URL,
        }
        resp = session.post(
            "https://act-api.cloud.tencent.com/user/get-facecheck-info",
            json={},
            headers=headers,
            timeout=10,
        )
        result = resp.json()
        if result.get("code") != 0:
            return None
        return bool((result.get("data") or {}).get("ever_pass"))
    except Exception as e:
        print(f"⚠️ 人脸状态查询失败: {e}")
        return None


def ensure_face_verified(page, cookies):
    """登录后人脸核身确认, 返回 (ever_pass, checked_at).

    没通过则在同一浏览器打开验证页等用户做完 (微信扫一扫+人脸识别),
    轮询通过或超时后返回. 永不抛异常, 不影响已保存的登录态.
    """
    checked_at = time.strftime("%Y-%m-%d %H:%M:%S")
    passed = query_face_pass(cookies)
    if passed is True:
        print("✅ 人脸核身已通过, 无需额外操作")
        return True, checked_at
    if passed is not False:
        print("⚠️ 未能确认人脸状态, 请开抢前手动确认是否需核身")
        return None, checked_at
    print("⛔ 账号需人脸核身校验, 否则抢购必报 1100202!")
    print("👉 已在当前浏览器打开验证页, 请按页完成验证 (微信扫一扫+人脸识别)")
    try:
        page.goto(FACE_URL)
    except Exception as e:
        print(f"⚠️ 验证页打开失败, 请手动访问: {FACE_URL} ({e})")
    deadline = time.time() + FACE_TIMEOUT_S
    while time.time() < deadline:
        time.sleep(FACE_POLL_S)
        if query_face_pass(cookies) is True:
            print("✅ 人脸核身已通过!")
            return True, time.strftime("%Y-%m-%d %H:%M:%S")
        print("⌛ 等待人脸核身完成...")
    print(f"⚠️ 超时未检测到通过, 请开抢前手动确认: {FACE_URL}")
    return False, checked_at


def auto_rush_buy():
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False)
        context = browser.new_context()
        page = context.new_page()
        page.goto(LOGIN_URL)

        print("请扫描二维码登录...")
        # 不写死期号: 跳到任意活动页即视为登录成功
        page.wait_for_url("**/act/pro/**", timeout=0)
        page.wait_for_timeout(2000)

        act_url = page.url
        print(f"登录成功! 当前活动页: {act_url}")

        cookies = context.cookies()
        with open(COOKIES_PATH, "w", encoding="utf-8") as f:
            json.dump(cookies, f, ensure_ascii=False, indent=2)
        print(f"已保存 {len(cookies)} 个 Cookie 到 cookies.json")

        # 登录后人脸核身确认: 状态写入 config.json 供抢购脚本读取
        face_pass, face_checked = ensure_face_verified(page, cookies)

        # 自动同步活动 URL, 主脚本优先读取它
        auto_config = {}
        if os.path.exists(CONFIG_PATH):
            try:
                with open(CONFIG_PATH, encoding="utf-8") as f:
                    auto_config = json.load(f)
            except json.JSONDecodeError:
                auto_config = {}
        auto_config["act_url"] = act_url
        auto_config["updated"] = time.strftime("%Y-%m-%d %H:%M:%S")
        auto_config["facecheck"] = {"ever_pass": face_pass, "checked": face_checked}
        with open(CONFIG_PATH, "w", encoding="utf-8") as f:
            json.dump(auto_config, f, ensure_ascii=False, indent=2)
        print("活动 URL 已同步到 config.json")

        with contextlib.suppress(Exception):
            browser.close()  # 验证时用户可能已手动关窗
        print("浏览器已关闭, 可以运行 snap_up_server.py 了")


if __name__ == "__main__":
    auto_rush_buy()
