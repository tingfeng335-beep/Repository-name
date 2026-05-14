# -*- coding: utf-8 -*-
"""
v3.8 机器人诊断脚本
用法：python test_bots.py
作用：
  1. 验证两个 bot token 是否有效（getMe）
  2. 看每个 bot 收到过哪些用户的 /start（getUpdates）
  3. 直接给每个 chat_id 发一条测试消息
"""
import requests, json

BOTS = {
    "默认机器人 (1h/4h/系统通知)":
        "8597069493:AAEmXzUJ3Yv42NGd2EsP3M93aatLjqzPWFI",
    "第一机器人 (15m)":
        "8536181331:AAF2KZXP8gn9dkH_lubHu3EFFB3uniE3Mjc",
}
CHAT_IDS = {
    "你自己":           "7470996017",
    "朋友 @Nick_Tuz":   "6587035253",
}

NO_PROXY = {"http": None, "https": None}

def call(token, method, params=None):
    url = f"https://api.telegram.org/bot{token}/{method}"
    try:
        r = requests.get(url, params=params, timeout=10, proxies=NO_PROXY)
        return r.json()
    except Exception as e:
        return {"ok": False, "error": f"{type(e).__name__}: {e}"}

print("=" * 70)
for bot_name, token in BOTS.items():
    print(f"\n>>> {bot_name}")
    print(f"    token: {token[:15]}...{token[-6:]}")

    # 1. getMe
    r = call(token, "getMe")
    if not r.get("ok"):
        print(f"    [X] token 无效或网络不通: {r.get('description', r.get('error'))}")
        continue
    info = r["result"]
    print(f"    [OK] 机器人名称: {info.get('first_name')} (@{info.get('username')})")

    # 2. getUpdates - 看谁发过 /start
    r = call(token, "getUpdates")
    if r.get("ok"):
        users = set()
        for u in r.get("result", []):
            msg = u.get("message") or u.get("edited_message") or {}
            chat = msg.get("chat", {})
            if chat.get("id"):
                users.add((str(chat["id"]),
                           chat.get("first_name", ""),
                           chat.get("username", "")))
        if users:
            print(f"    [INFO] 已和这些用户对话过（说明他们发过 /start 或之前互动过）:")
            for uid, fname, uname in users:
                print(f"           - chat_id={uid}  {fname}  @{uname}")
        else:
            print(f"    [WARN] getUpdates 为空：可能没人发过 /start，"
                  f"或者机器人之前用过 webhook（返回历史会被清）")

    # 3. 实测推送
    for who, cid in CHAT_IDS.items():
        r = call(token, "sendMessage", {
            "chat_id": cid,
            "text": f"[测试] {bot_name} -> {who}",
        })
        if r.get("ok"):
            print(f"    [OK] 推送到 {who} ({cid}) 成功")
        else:
            desc = r.get("description", r.get("error", "?"))
            print(f"    [FAIL] 推送到 {who} ({cid}) 失败: {desc}")

print("\n" + "=" * 70)
print("诊断完毕。如果某个机器人对某人 [FAIL] 提示")
print("'bot can't initiate conversation with a user' —— 让那个人在 TG 里")
print("打开机器人对话，发一条 /start，再运行此脚本。")
