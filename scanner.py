# -*- coding: utf-8 -*-
"""
Quantum Flow 背离侦察系统 v3.8
- 按 Binance USDT 永续合约 24h 成交额降序取前 TOP_N 个扫描
- 严格排除交割合约、USDC 计价合约
- 并发扫描（线程池），单周期扫描提速 3~5 倍
- 信号持久化去重 + 3 天自动清理过期记录
- 信号带强度分 (★ 1~5) + 结构化日志 signals.log
- 丢弃未收盘 K 线，防止信号闪烁
- 主循环顶层异常保护，7x24 不挂
- 失败合约自动退避（三振出局，冷藏 1 小时）
- 分周期独立 FRESH_BARS 配置
- v3.2 新增: 信号按强度分级推送（高星级单独推 + 低星级合并摘要）
- v3.4 修复: (1) sent_signals.json 原子写入，防损坏 (2) 启动首轮静默，避免爆推
- v3.5 新增: (1) 4h 扫描提速 60→20 分钟 (2) 分周期色带 4h红/1h蓝/15m绿，醒目区分
- v3.6 新增: (1) TOP_N 230→190 (2) 心跳消息(每6h) (3) 崩溃通知 (4) 每日日报(UTC 0点)
- v3.7 修复: 幽灵背离 — 拉 K 线数 200→500 + 枢轴扫描起点 +20→+50，避免在指标未预热区间找假枢轴
- v3.8 新增: 多机器人分流推送 — 15m 信号走"第一个机器人"，1h/4h/系统通知走默认机器人
"""

import ccxt
import pandas as pd
import numpy as np
import requests
import time
import os
import sys
import json
import atexit
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone

# ==============================================================================
# 1. 【核心配置区】
# ==============================================================================

TG_TOKEN           = os.getenv("TG_TOKEN",           "8597069493:AAEmXzUJ3Yv42NGd2EsP3M93aatLjqzPWFI")
# v3.8 多机器人分流：第一个机器人专门收 15m 信号
# 用法：在 BotFather 拿到 token 填这里；接收人需先在 TG 给该机器人发 /start，否则推不过去
TG_TOKEN_15M       = os.getenv("TG_TOKEN_15M",       "8536181331:AAGPI90prRZLsZ5IP-fumytpviwVfWCC3z4")

# 多用户推送：把需要接收的 chat_id 写进列表里即可；脚本会逐个推送
TG_CHAT_IDS        = [
    "7470996017",    # 你自己
    "6587035253",    # 癫皇比特币 @Nick_Tuz
]

# v3.8 按周期路由到不同机器人；未列出的周期 / 系统消息（启动/心跳/日报/崩溃）走 TG_TOKEN
TG_TOKEN_BY_TF = {
    "15m": TG_TOKEN_15M,
    "1h":  TG_TOKEN,
    "4h":  TG_TOKEN,
}
BINANCE_API_KEY    = os.getenv("BINANCE_API_KEY",    "LmoOgqAkSmcpilfJcdEW4genLy76swigcTnIMPVR7gvqwR55aY4lxjdJbigzeKY8")
BINANCE_SECRET_KEY = os.getenv("BINANCE_SECRET_KEY", "aErEwJ3gOvBr6mS92zOaJ9mhbBqeors2WXp7nFESyZaJQ2e38giKwWr0NMAXofJI")

# --- 扫描任务表 ---
SCAN_TASKS = [
    {"timeframe": "15m", "interval_minutes": 3},
    {"timeframe": "1h",  "interval_minutes": 8},
    {"timeframe": "4h",  "interval_minutes": 20},
]

# --- 合约池设置：按 24h 成交额降序取前 TOP_N 个 ---
TOP_N               = 190
MARKETS_CACHE_TTL   = 3600      # 合约列表缓存 1 小时

# --- 并发设置 ---
SCAN_WORKERS        = 20        # 并发线程数（提速：10→20，仍远低于 Binance 2400/分钟限制）

# --- 枢轴与背离参数 ---
F_PL = 5;   F_PR = 3;   F_MB = 8;   F_MD = 3.0
M_PL = 10;  M_PR = 6;   M_MB = 20;  M_MD = 3.0
HOLD_BARS  = 8    # 双背离共振：两类确认时间差窗口

# --- 分周期新鲜度：允许 0~N 根前的信号推送 ---
# 15m/1h 给 3 根（约 45 分钟 / 3 小时），4h 给 2 根（约 8 小时），避免过时
FRESH_BARS_MAP = {
    "15m": 3,
    "1h":  3,
    "4h":  2,
}
FRESH_BARS_DEFAULT = 3

# --- 分周期消息样式：emoji + 标签 + 色带 ---
# 色带作用：在 TG 消息流中一眼分辨不同周期
#   15m = 绿色（短线）🟢
#   1h  = 蓝色（中线）🔵
#   4h  = 红色（主力大指标，最醒目）🔴
TF_STYLE_MAP = {
    "15m": {"emoji": "⚡",  "tag": "短线", "band": "🟢"},
    "1h":  {"emoji": "🎯",  "tag": "中线", "band": "🔵"},
    "4h":  {"emoji": "💎",  "tag": "主力", "band": "🔴"},
}
TF_STYLE_DEFAULT = {"emoji": "📊", "tag": "", "band": "⚪"}

# --- 信号推送分级：>= MSG_TIER_STARS 的单独推；< 的合并成摘要 ---
# 建议：4 = 只有 4~5 星单独推，1~3 星合并；5 = 只有 5 星单独推；0 = 所有单独推（旧行为）
MSG_TIER_STARS      = 4

# --- 失败合约退避 ---
FAIL_STRIKE_LIMIT = 3             # 连续失败多少次进入冷藏
COOLDOWN_SECONDS  = 60 * 60       # 冷藏时长（秒）= 1 小时

# --- 持久化文件 ---
SENT_SIGNALS_FILE  = "sent_signals.json"
SIGNALS_LOG_FILE   = "signals.log"

# --- 去重记录保留时长（毫秒） ---
SIGNAL_RETENTION_MS = 3 * 24 * 3600 * 1000    # 3 天

# --- v3.6 监控：心跳 / 日报 ---
HEARTBEAT_INTERVAL_HOURS = 6     # 心跳间隔：每 6 小时发一条"还活着"
DAILY_REPORT_HOUR_UTC    = 0     # 日报推送时刻（UTC 0 点）

# ==============================================================================
# 2. 【通讯模块】
# ==============================================================================

# 禁用系统代理，避免 Windows 代理设置导致 requests 卡死
_NO_PROXY = {"http": None, "https": None}

_tg_lock = threading.Lock()   # 并发环境下串行化 TG 发送，避免触发 429


def send_tg(message, retries=3, timeframe=None):
    """发送战报至 Telegram，失败指数退避重试（线程安全）
    对 TG_CHAT_IDS 里的每个用户逐一推送，任一用户发送成功即视为 True

    v3.8: 按 timeframe 路由到不同机器人；timeframe=None 走默认 TG_TOKEN
    （启动通知 / 心跳 / 日报 / 崩溃通知都不传 timeframe，走默认机器人）
    """
    token = TG_TOKEN_BY_TF.get(timeframe, TG_TOKEN) if timeframe else TG_TOKEN
    url = f"https://api.telegram.org/bot{token}/sendMessage"

    with _tg_lock:
        any_ok = False
        for chat_id in TG_CHAT_IDS:
            payload = {"chat_id": chat_id, "text": message, "parse_mode": "HTML"}
            sent = False
            for attempt in range(retries):
                try:
                    resp = requests.post(
                        url, json=payload,
                        timeout=(3, 5),
                        proxies=_NO_PROXY
                    )
                    result = resp.json()
                    if result.get("ok"):
                        sent = True
                        break
                    desc = result.get("description", "")
                    if "Too Many Requests" in desc and attempt < retries - 1:
                        retry_after = result.get("parameters", {}).get("retry_after", 2 ** attempt)
                        time.sleep(retry_after)
                        continue
                    print(f"  [WARN] TG({chat_id}) 返回错误: {desc}")
                    break
                except (requests.exceptions.ConnectTimeout,
                        requests.exceptions.ReadTimeout,
                        requests.exceptions.ConnectionError) as e:
                    if attempt < retries - 1:
                        time.sleep(2 ** attempt)
                        continue
                    print(f"  [WARN] TG({chat_id}) 最终失败: {type(e).__name__}: {e}")
                    break
                except Exception as e:
                    print(f"  [WARN] TG({chat_id}) 发送异常: {type(e).__name__}: {e}")
                    break
            any_ok = any_ok or sent
        return any_ok


# ==============================================================================
# 3. 【去重缓存：持久化 + 自动清理过期】
# ==============================================================================

_sent_lock = threading.Lock()


def _load_sent_signals():
    """加载并清理过期记录（> SIGNAL_RETENTION_MS）"""
    try:
        with open(SENT_SIGNALS_FILE, "r", encoding="utf-8") as f:
            raw = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return {}

    cutoff = int(time.time() * 1000) - SIGNAL_RETENTION_MS
    clean = {}
    dropped = 0
    for k, v in raw.items():
        if isinstance(v, (int, float)) and v > cutoff:
            clean[tuple(k.split("|", 2))] = v
        else:
            dropped += 1
    if dropped:
        print(f"  [INFO] 去重缓存清理: 丢弃 {dropped} 条过期记录, 保留 {len(clean)} 条")
    return clean


def _save_sent_signals():
    # v3.4 原子写入：先写临时文件，再替换，防止写到一半崩溃/断电导致 JSON 损坏
    try:
        with _sent_lock:
            serializable = {"|".join(k): v for k, v in sent_signals.items()}
        tmp_file = SENT_SIGNALS_FILE + ".tmp"
        with open(tmp_file, "w", encoding="utf-8") as f:
            json.dump(serializable, f, ensure_ascii=False, indent=2)
        os.replace(tmp_file, SENT_SIGNALS_FILE)
    except Exception as e:
        print(f"  [WARN] 去重缓存保存失败: {e}")


sent_signals = _load_sent_signals()
atexit.register(_save_sent_signals)


def should_send(symbol, tf, sig_type, bar_time_ms):
    """若该信号（同一背离确认 K 线）已发送过，返回 False；否则登记并返回 True"""
    key = (symbol, tf, sig_type)
    with _sent_lock:
        if sent_signals.get(key) == bar_time_ms:
            return False
        sent_signals[key] = bar_time_ms
    return True


# ==============================================================================
# 4. 【失败合约退避】
# ==============================================================================

_strike_lock = threading.Lock()
# key: (symbol, timeframe) -> {"fails": 连续失败次数, "cold_until": 解冻时间戳}
_symbol_strikes = {}


def is_cold(symbol, tf):
    """检查合约是否处于冷藏期"""
    with _strike_lock:
        rec = _symbol_strikes.get((symbol, tf))
        if not rec:
            return False
        return rec.get("cold_until", 0) > time.time()


def record_success(symbol, tf):
    """成功后清除连续失败计数"""
    with _strike_lock:
        _symbol_strikes.pop((symbol, tf), None)


def record_failure(symbol, tf):
    """记录一次失败；达到阈值则冷藏"""
    with _strike_lock:
        rec = _symbol_strikes.get((symbol, tf), {"fails": 0, "cold_until": 0})
        rec["fails"] += 1
        if rec["fails"] >= FAIL_STRIKE_LIMIT:
            rec["cold_until"] = time.time() + COOLDOWN_SECONDS
            rec["fails"] = 0
            _symbol_strikes[(symbol, tf)] = rec
            return True   # 触发冷藏
        _symbol_strikes[(symbol, tf)] = rec
        return False


# ==============================================================================
# 5. 【结构化日志】
# ==============================================================================

_log_lock = threading.Lock()


# v3.6 监控统计：进程内累计计数（用于心跳消息、崩溃通知）
_stats_lock = threading.Lock()
_run_stats = {
    "start_time": time.time(),
    "scan_count": 0,          # 累计扫描轮数（所有周期加起来）
    "signal_count": 0,        # 累计新增信号数（单推 + 摘要里的）
    "last_scan_time": 0,      # 最近一次扫描完成时间
}


def _bump_stats(scans=0, signals=0):
    """扫描完后更新统计"""
    with _stats_lock:
        _run_stats["scan_count"] += scans
        _run_stats["signal_count"] += signals
        if scans:
            _run_stats["last_scan_time"] = time.time()


def _snapshot_stats():
    """拍快照，避免持锁读"""
    with _stats_lock:
        return dict(_run_stats)


def log_signal(row):
    """把信号写入 CSV 格式日志，方便后续统计胜率"""
    header = "time_utc,symbol,timeframe,sig_type,strength_stars,strength_ratio,price,fusion,flow,age_bars,bar_utc\n"
    line = (
        f"{datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')},"
        f"{row['symbol']},{row['tf']},{row['sig_type']},"
        f"{row['stars']},{row['ratio']:.2f},"
        f"{row['price']:.6f},{row['fusion']:.2f},{row['flow']:.2f},"
        f"{row['age']},{row['bar_utc']}\n"
    )
    try:
        with _log_lock:
            exists = os.path.exists(SIGNALS_LOG_FILE)
            with open(SIGNALS_LOG_FILE, "a", encoding="utf-8") as f:
                if not exists:
                    f.write(header)
                f.write(line)
    except Exception as e:
        print(f"  [WARN] 日志写入失败: {e}")


# ==============================================================================
# 6. 【核心指标算法】
# ==============================================================================

def calculate_quantum_flow(df):
    """完全复刻 Pine Script 的 Quantum Flow Cipher 计算"""
    df = df.copy()
    df['close_prev'] = df['close'].shift(1)
    df = df.dropna().reset_index(drop=True)

    # --- Delta Volume ---
    df['deltaVol'] = np.where(
        df['close'] > df['close_prev'],  df['volume'],
        np.where(df['close'] < df['close_prev'], -df['volume'], 0)
    )

    # --- CVD 动量 ---
    df['cvd_cum']      = df['deltaVol'].cumsum()
    df['cvd_cum_diff'] = df['cvd_cum'] - df['cvd_cum'].shift(3)
    df['cvd_mom']      = df['cvd_cum_diff'].ewm(span=10, adjust=False).mean()

    # --- MACD ---
    df['ema12']   = df['close'].ewm(span=12, adjust=False).mean()
    df['ema26']   = df['close'].ewm(span=26, adjust=False).mean()
    df['macd']    = df['ema12'] - df['ema26']
    df['macd_lr'] = df['macd'].ewm(span=3, adjust=False).mean()

    # --- 归一化 ---
    df['macd_std']  = df['macd_lr'].rolling(50).std().replace(0, 1e-6)
    df['macd_norm'] = df['macd_lr'] / df['macd_std']
    df['cvd_std']   = df['cvd_mom'].rolling(50).std().replace(0, 1e-6)
    df['cvd_norm']  = df['cvd_mom'] / df['cvd_std']

    # --- ATR 波动率加权 ---
    df['tr'] = np.maximum(
        df['high'] - df['low'],
        np.maximum(
            np.abs(df['high'] - df['close'].shift(1)),
            np.abs(df['low']  - df['close'].shift(1))
        )
    )
    df['atr']       = df['tr'].rolling(14).mean()
    df['atr_sma20'] = df['atr'].rolling(20).mean().replace(0, 1e-6)
    df['vr']        = np.clip(df['atr'] / df['atr_sma20'], 0.7, 1.5)
    df['mw']        = (1 / df['vr']) / (1 / df['vr'] + df['vr'])

    # --- Quantum Fusion (Tanh 压缩，范围 -75 ~ +75) ---
    df['fusion_raw'] = (
        df['macd_norm'] * df['mw'] + df['cvd_norm'] * (1 - df['mw'])
    ).ewm(span=3, adjust=False).mean()
    df['fusion_c'] = np.tanh(df['fusion_raw'] / 3.0) * 3.0
    df['fusion_d'] = df['fusion_c'] * 25.0

    # --- Money Flow 柱 ---
    hlc3    = (df['high'] + df['low'] + df['close']) / 3
    mf_m    = hlc3.rolling(5).mean()
    diff    = hlc3 - mf_m
    sma_abs = diff.abs().rolling(5).mean().replace(0, 1e-6)
    df['mf'] = (diff / (0.015 * sma_abs)).rolling(60).mean()

    return df


def detect_pivots(series_values, left, right):
    """
    检测枢轴高低点 —— 严格对齐 TradingView 的 ta.pivothigh/pivotlow。
    Pine 官方实现：两侧都用严格 > / 严格 <（中心点必须严格大于/小于所有邻居）。
    """
    n = len(series_values)
    ph_vals = [np.nan] * n
    pl_vals = [np.nan] * n

    for i in range(left, n - right):
        v = series_values[i]
        is_high = (all(v > series_values[i - j] for j in range(1, left + 1)) and
                   all(v > series_values[i + j] for j in range(1, right + 1)))
        if is_high:
            ph_vals[i] = v

        is_low = (all(v < series_values[i - j] for j in range(1, left + 1)) and
                  all(v < series_values[i + j] for j in range(1, right + 1)))
        if is_low:
            pl_vals[i] = v

    return ph_vals, pl_vals


def _strength_stars(ratio):
    """
    根据"实际幅度 / 阈值"的比率映射成 1~5 星
    1.0x -> ★
    1.5x -> ★★
    2.0x -> ★★★
    3.0x -> ★★★★
    >=5x -> ★★★★★
    """
    if ratio >= 5.0:
        return 5
    if ratio >= 3.0:
        return 4
    if ratio >= 2.0:
        return 3
    if ratio >= 1.5:
        return 2
    return 1


def _stars_str(n):
    return "★" * n + "☆" * (5 - n)


def detect_divergence_signals(df, symbol, timeframe):
    """
    背离检测 + 双背离共振。
    返回 [(sig_type, msg, pivot_bar_time_ms, log_row), ...]

    FRESH_BARS 按周期查表（FRESH_BARS_MAP），默认 FRESH_BARS_DEFAULT。
    """
    fresh_bars = FRESH_BARS_MAP.get(timeframe, FRESH_BARS_DEFAULT)

    out = []
    n = len(df)
    if n < 200:   # v3.7 样本数检查 100→200：保证指标已充分预热
        return out

    fusion_arr = df['fusion_d'].values
    mf_arr     = df['mf'].values
    high_arr   = df['high'].values
    low_arr    = df['low'].values
    time_arr   = df['time'].values

    f_ph_vals, f_pl_vals = detect_pivots(fusion_arr, F_PL, F_PR)
    m_ph_vals, m_pl_vals = detect_pivots(mf_arr, M_PL, M_PR)

    fh_v = fh_p = fh_b = np.nan
    fl_v = fl_p = fl_b = np.nan
    mh_v = mh_p = mh_b = np.nan
    ml_v = ml_p = ml_b = np.nan

    # 每种背离记录最近一次"确认索引 + 背离幅度"
    last = {
        "F_BULL": {"idx": -1, "amp": 0.0},
        "F_BEAR": {"idx": -1, "amp": 0.0},
        "M_BULL": {"idx": -1, "amp": 0.0},
        "M_BEAR": {"idx": -1, "amp": 0.0},
    }

    # v3.7 枢轴扫描起点 +20→+50：跳过 fusion_d/mf 指标不稳定区间
    # rolling(50).std 需要 50 根才出值，rolling(60).mean 需要 60 根
    # 从第 60 根开始扫才能保证枢轴基于稳定的指标数值
    start = max(F_PL, F_PR, M_PL, M_PR) + 50

    for i in range(start, n):
        # ── Quantum 顶背离 ──
        if not np.isnan(f_ph_vals[i]):
            pb = i
            pv = f_ph_vals[i]
            pp = high_arr[pb]
            if not np.isnan(fh_v) and not np.isnan(fh_b):
                amp = fh_v - pv
                if pp >= fh_p and amp >= F_MD and (pb - fh_b) >= F_MB:
                    last["F_BEAR"] = {"idx": i, "amp": amp}
            fh_v, fh_p, fh_b = pv, pp, pb

        # ── Quantum 底背离 ──
        if not np.isnan(f_pl_vals[i]):
            pb = i
            pv = f_pl_vals[i]
            pp = low_arr[pb]
            if not np.isnan(fl_v) and not np.isnan(fl_b):
                amp = pv - fl_v
                if pp <= fl_p and amp >= F_MD and (pb - fl_b) >= F_MB:
                    last["F_BULL"] = {"idx": i, "amp": amp}
            fl_v, fl_p, fl_b = pv, pp, pb

        # ── Flow 顶背离 ──
        if not np.isnan(m_ph_vals[i]):
            pb = i
            pv = m_ph_vals[i]
            pp = high_arr[pb]
            # 对齐 Pine ta.highest/ta.lowest(high, m_pl*3)：取最近 M_PL*3 根 K（含当前）
            lookback = M_PL * 3
            start_k  = max(0, i - lookback + 1)
            hh = high_arr[start_k:i + 1].max()
            ll = low_arr[start_k:i + 1].min()
            if pp >= ll + (hh - ll) * 0.5:
                if not np.isnan(mh_v) and not np.isnan(mh_b):
                    amp = mh_v - pv
                    if pp >= mh_p and amp >= M_MD and (pb - mh_b) >= M_MB:
                        last["M_BEAR"] = {"idx": i, "amp": amp}
            mh_v, mh_p, mh_b = pv, pp, pb

        # ── Flow 底背离 ──
        if not np.isnan(m_pl_vals[i]):
            pb = i
            pv = m_pl_vals[i]
            pp = low_arr[pb]
            # 对齐 Pine ta.highest/ta.lowest(high, m_pl*3)：取最近 M_PL*3 根 K（含当前）
            lookback = M_PL * 3
            start_k  = max(0, i - lookback + 1)
            hh = high_arr[start_k:i + 1].max()
            ll = low_arr[start_k:i + 1].min()
            if pp <= ll + (hh - ll) * 0.5:
                if not np.isnan(ml_v) and not np.isnan(ml_b):
                    amp = pv - ml_v
                    if pp <= ml_p and amp >= M_MD and (pb - ml_b) >= M_MB:
                        last["M_BULL"] = {"idx": i, "amp": amp}
            ml_v, ml_p, ml_b = pv, pp, pb

    # --- 只保留"最近 fresh_bars 根内确认"的信号 ---
    last_idx = n - 1

    def fresh(key):
        idx = last[key]["idx"]
        return idx >= 0 and (last_idx - idx) <= fresh_bars

    def co_fresh(a, b):
        ia, ib = last[a]["idx"], last[b]["idx"]
        return (fresh(a) and fresh(b) and abs(ia - ib) <= HOLD_BARS)

    f_bull_sig = fresh("F_BULL")
    f_bear_sig = fresh("F_BEAR")
    m_bull_sig = fresh("M_BULL")
    m_bear_sig = fresh("M_BEAR")

    # 双背离共振已禁用（用户要求）
    dbl_bull = False
    dbl_bear = False

    curr_f   = float(df['fusion_d'].iloc[-1])
    curr_mf  = float(df['mf'].iloc[-1])
    curr_p   = float(df['close'].iloc[-1])

    def _age(idx):
        return last_idx - idx

    def _pivot_time_ms(idx):
        return int(time_arr[idx])

    def _bar_str(idx):
        return datetime.fromtimestamp(_pivot_time_ms(idx) / 1000, tz=timezone.utc) \
                       .strftime('%Y-%m-%d %H:%M UTC')

    def _build(sig_type, title_prefix, title_cn, idx, ratio):
        stars = _strength_stars(ratio)
        age   = _age(idx)
        bar_t = _bar_str(idx)
        tf_style = TF_STYLE_MAP.get(timeframe, TF_STYLE_DEFAULT)
        tf_emoji = tf_style["emoji"]
        tf_tag   = tf_style["tag"]
        tf_band  = tf_style.get("band", "⚪")
        detail_line = {
            "DBL_BULL": f"Quantum + Flow 同时底背离",
            "DBL_BEAR": f"Quantum + Flow 同时顶背离",
            "F_BULL":   f"Fusion: {curr_f:.1f} | 价格新低，Fusion 未新低",
            "F_BEAR":   f"Fusion: {curr_f:.1f} | 价格新高，Fusion 未新高",
            "M_BULL":   f"Flow: {curr_mf:.2f} | 价格新低，Flow 未新低",
            "M_BEAR":   f"Flow: {curr_mf:.2f} | 价格新高，Flow 未新高",
        }[sig_type]

        tf_label = f"{tf_emoji} {tf_tag}·{timeframe}" if tf_tag else f"{tf_emoji} {timeframe}"

        # v3.5 色带装饰：4h=红 / 1h=蓝 / 15m=绿，消息首尾各 10 个方块，醒目
        band_line = tf_band * 10

        msg = (
            f"{band_line}\n"
            f"{title_prefix} <b>[{tf_label}] {symbol} {title_cn}</b>\n"
            f"强度: {_stars_str(stars)}  ({ratio:.1f}x 阈值)\n"
            f"{detail_line}\n"
            f"新鲜度: {age} 根前 | 价: {curr_p:.6f}\n"
            f"确认K线: {bar_t}\n"
            f"{band_line}"
        )
        log_row = {
            "symbol": symbol, "tf": timeframe, "sig_type": sig_type,
            "stars": stars, "ratio": ratio, "price": curr_p,
            "fusion": curr_f, "flow": curr_mf, "age": age, "bar_utc": bar_t,
        }
        return (sig_type, msg, _pivot_time_ms(idx), log_row)

    if f_bull_sig and not dbl_bull:
        idx = last["F_BULL"]["idx"]
        ratio = last["F_BULL"]["amp"] / F_MD
        out.append(_build("F_BULL", "[UP]", "Quantum 底背离", idx, ratio))
    if f_bear_sig and not dbl_bear:
        idx = last["F_BEAR"]["idx"]
        ratio = last["F_BEAR"]["amp"] / F_MD
        out.append(_build("F_BEAR", "[DN]", "Quantum 顶背离", idx, ratio))
    if m_bull_sig and not dbl_bull:
        idx = last["M_BULL"]["idx"]
        ratio = last["M_BULL"]["amp"] / M_MD
        out.append(_build("M_BULL", "[UP]", "Flow 底背离", idx, ratio))
    if m_bear_sig and not dbl_bear:
        idx = last["M_BEAR"]["idx"]
        ratio = last["M_BEAR"]["amp"] / M_MD
        out.append(_build("M_BEAR", "[DN]", "Flow 顶背离", idx, ratio))

    return out


# ==============================================================================
# 7. 【合约池】
# ==============================================================================

_markets_cache = {"time": 0, "symbols": []}


def get_top_symbols(exchange, top_n=TOP_N, ttl=MARKETS_CACHE_TTL):
    """获取 24h 成交额 Top N 的 Binance USDT 永续合约（带缓存）"""
    if _markets_cache["symbols"] and (time.time() - _markets_cache["time"] < ttl):
        return _markets_cache["symbols"]

    markets = exchange.fetch_markets()
    eligible = [
        m for m in markets
        if m.get('swap', False)
        and m.get('active', False)
        and m.get('quote') == 'USDT'
        and not m.get('expiry')
        and 'USDC' not in m['symbol']
    ]

    tickers = exchange.fetch_tickers([m['symbol'] for m in eligible])

    def _quote_vol(sym):
        t = tickers.get(sym, {}) or {}
        qv = t.get('quoteVolume')
        if qv is None:
            info = t.get('info') or {}
            try:
                qv = float(info.get('quoteVolume', 0) or 0)
            except (TypeError, ValueError):
                qv = 0
        return qv or 0

    ranked = sorted(eligible, key=lambda m: _quote_vol(m['symbol']), reverse=True)
    symbols = [m['symbol'] for m in ranked[:top_n]]

    _markets_cache["time"] = time.time()
    _markets_cache["symbols"] = symbols

    print(f"  [INFO] 合约池刷新: 取成交额 Top {len(symbols)} / 候选 {len(eligible)}")
    return symbols


# ==============================================================================
# 8. 【信号分级推送】
# ==============================================================================

_SIG_LABEL = {
    "F_BULL":   "Quantum底",
    "F_BEAR":   "Quantum顶",
    "M_BULL":   "Flow底",
    "M_BEAR":   "Flow顶",
}


def _is_high_tier(sig_type, stars):
    """判断是否走单独推送通道（星级 >= MSG_TIER_STARS）"""
    return stars >= MSG_TIER_STARS


def _build_summary(timeframe, low_tier_rows):
    """把一批低星级信号合并成一条摘要消息"""
    if not low_tier_rows:
        return None
    # 按星级降序、方向分组
    low_tier_rows = sorted(low_tier_rows, key=lambda r: (-r['stars'], r['sig_type']))
    bulls = [r for r in low_tier_rows if r['sig_type'].endswith('BULL')]
    bears = [r for r in low_tier_rows if r['sig_type'].endswith('BEAR')]

    tf_style = TF_STYLE_MAP.get(timeframe, TF_STYLE_DEFAULT)
    tf_emoji = tf_style["emoji"]
    tf_tag   = tf_style["tag"]
    tf_band  = tf_style.get("band", "⚪")
    tf_label = f"{tf_emoji} {tf_tag}·{timeframe}" if tf_tag else f"{tf_emoji} {timeframe}"

    # v3.5 色带装饰：摘要消息首尾加色带
    band_line = tf_band * 10

    lines = [
        band_line,
        f"<b>📊 [{tf_label}] 摘要 | 本轮新增 {len(low_tier_rows)} 个低星级信号</b>",
    ]

    def _fmt_group(title, rows):
        if not rows:
            return []
        out = [f"\n<b>{title}</b> ({len(rows)})"]
        for r in rows[:15]:   # 每方向最多 15 条
            label = _SIG_LABEL.get(r['sig_type'], r['sig_type'])
            out.append(
                f"  {_stars_str(r['stars'])} {r['symbol']} "
                f"[{label}] {r['ratio']:.1f}x | 价 {r['price']:.4f} | {r['age']}根前"
            )
        if len(rows) > 15:
            out.append(f"  ... 还有 {len(rows) - 15} 条（查看 signals.log）")
        return out

    lines += _fmt_group("🔺 看涨", bulls)
    lines += _fmt_group("🔻 看跌", bears)
    lines.append(band_line)
    return "\n".join(lines)


# ==============================================================================
# 9. 【侦察引擎：并发 + 失败退避 + 分级推送】
# ==============================================================================

def _scan_one(exchange, symbol, timeframe):
    """
    扫描单个合约，返回 (symbol, signals_to_send) 或抛异常
    signals_to_send: [(sig_type, msg, bar_time, log_row), ...]
    已做 should_send 过滤
    """
    # v3.7 拉 500 根 K（原 200）：让 rolling(50).std / rolling(60).mean / ewm 充分预热
    # 修幽灵背离：避免在 fusion_d/mf 还不稳定的前 60 根里找到"假枢轴"
    bars = exchange.fetch_ohlcv(symbol, timeframe=timeframe, limit=500)
    if len(bars) < 200:
        return symbol, []

    df = pd.DataFrame(bars, columns=['time', 'open', 'high', 'low', 'close', 'volume'])
    df = df.iloc[:-1].reset_index(drop=True)    # 丢弃未收盘 K 线

    df = calculate_quantum_flow(df)
    raw = detect_divergence_signals(df, symbol, timeframe)

    to_send = []
    for sig_type, msg, bar_time, log_row in raw:
        if should_send(symbol, timeframe, sig_type, bar_time):
            to_send.append((sig_type, msg, bar_time, log_row))
    return symbol, to_send


def scan_markets(exchange, timeframe):
    """并发扫描 Top N 合约（带失败退避 + 分级推送）"""
    t0 = time.time()
    try:
        all_symbols = get_top_symbols(exchange)

        # 过滤出未冷藏的合约
        symbols = [s for s in all_symbols if not is_cold(s, timeframe)]
        skipped_cold = len(all_symbols) - len(symbols)

        total = len(symbols)
        fresh_bars = FRESH_BARS_MAP.get(timeframe, FRESH_BARS_DEFAULT)
        extra = f" | 冷藏跳过:{skipped_cold}" if skipped_cold else ""
        print(f"\n[{datetime.now().strftime('%H:%M:%S')}] [SCAN] 周期:{timeframe} | "
              f"合约:{total} | 并发:{SCAN_WORKERS} | 新鲜度≤{fresh_bars}{extra}")

        high_tier_queue = []    # [(msg, log_row), ...] 单独推送
        low_tier_queue  = []    # [log_row, ...] 合并摘要
        done_count   = 0
        cold_count   = 0
        tg_logs      = []

        with ThreadPoolExecutor(max_workers=SCAN_WORKERS) as pool:
            future_map = {pool.submit(_scan_one, exchange, s, timeframe): s for s in symbols}

            for fut in as_completed(future_map):
                sym = future_map[fut]
                done_count += 1
                try:
                    _, signals = fut.result()
                    record_success(sym, timeframe)
                    for sig_type, msg, _bt, log_row in signals:
                        # 无论分级如何，都写入日志
                        log_signal(log_row)
                        if _is_high_tier(sig_type, log_row['stars']):
                            high_tier_queue.append((msg, log_row))
                        else:
                            low_tier_queue.append(log_row)
                except Exception as e:
                    triggered = record_failure(sym, timeframe)
                    if triggered:
                        cold_count += 1
                        tg_logs.append(f"  [COLD] {sym}: 连续失败，冷藏 1 小时 ({type(e).__name__})")
                    else:
                        tg_logs.append(f"  [SKIP] {sym}: {type(e).__name__}")

                # 进度条
                alert_preview = len(high_tier_queue) + len(low_tier_queue)
                pct = done_count / total * 100 if total > 0 else 100
                sys.stdout.write(
                    f"\r[{timeframe}] {pct:5.1f}% | {done_count:>3}/{total} | 警报:{alert_preview}   "
                )
                sys.stdout.flush()

        sys.stdout.write("\n")

        # --- 推送阶段 ---
        # 1. 高星级 / 双背离：单独推
        sent_high = 0
        for msg, log_row in high_tier_queue:
            ok = send_tg(msg, timeframe=timeframe)
            sent_high += 1 if ok else 0
            tag = 'OK' if ok else 'FAIL'
            tg_logs.append(
                f"  [{tag}] {log_row['symbol']} {log_row['sig_type']} {_stars_str(log_row['stars'])}"
            )

        # 2. 低星级：合并摘要推
        summary = _build_summary(timeframe, low_tier_queue)
        sent_summary = False
        if summary:
            sent_summary = send_tg(summary, timeframe=timeframe)
            tag = 'OK' if sent_summary else 'FAIL'
            tg_logs.append(f"  [{tag}] [摘要] {timeframe} 合并 {len(low_tier_queue)} 条低星级")

        elapsed = time.time() - t0
        total_alerts = len(high_tier_queue) + len(low_tier_queue)
        summary_line = (
            f"耗时 {elapsed:.1f}s | 新增信号 {total_alerts} 个 "
            f"(单推 {len(high_tier_queue)} / 摘要 {len(low_tier_queue)})"
        )
        if cold_count:
            summary_line += f" | 新冷藏 {cold_count} 个"
        print(f"[{timeframe}] 巡逻完毕 | {summary_line}")
        for line in tg_logs[-30:]:
            print(line)

        _save_sent_signals()

        # v3.6 更新累计统计（供心跳/日报使用）
        _bump_stats(scans=1, signals=total_alerts)

    except Exception as e:
        print(f"\n[ERROR] 扫描引擎异常: {type(e).__name__}: {e}")


# ==============================================================================
# 10. 【监控：心跳 / 崩溃通知 / 每日日报】(v3.6 新增)
# ==============================================================================

def _parse_log_row_from_line(line):
    """解析 signals.log 的一行 CSV，返回 dict 或 None"""
    parts = line.rstrip("\n").split(",")
    if len(parts) < 11:
        return None
    try:
        return {
            "time_utc":  parts[0],
            "symbol":    parts[1],
            "tf":        parts[2],
            "sig_type":  parts[3],
            "stars":     int(parts[4]),
            "ratio":     float(parts[5]),
            "price":     float(parts[6]),
        }
    except (ValueError, IndexError):
        return None


def _read_signals_last_hours(hours):
    """读取 signals.log 最近 N 小时的信号记录"""
    if not os.path.exists(SIGNALS_LOG_FILE):
        return []

    cutoff = datetime.now(timezone.utc).timestamp() - hours * 3600
    rows = []
    try:
        with open(SIGNALS_LOG_FILE, "r", encoding="utf-8") as f:
            next(f, None)    # 跳过 header
            for line in f:
                row = _parse_log_row_from_line(line)
                if not row:
                    continue
                try:
                    t = datetime.strptime(row["time_utc"], "%Y-%m-%d %H:%M:%S") \
                        .replace(tzinfo=timezone.utc).timestamp()
                    if t >= cutoff:
                        rows.append(row)
                except ValueError:
                    continue
    except Exception as e:
        print(f"  [WARN] 读取 signals.log 失败: {e}")
    return rows


def send_heartbeat():
    """心跳消息：汇报脚本还活着 + 最近统计"""
    stats = _snapshot_stats()
    uptime_hours = (time.time() - stats["start_time"]) / 3600
    last_scan_ago = (time.time() - stats["last_scan_time"]) / 60 if stats["last_scan_time"] else -1

    msg = (
        f"💓 <b>心跳</b> | 脚本运行中\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"⏱ 运行时长: {uptime_hours:.1f} 小时\n"
        f"🔍 累计扫描: {stats['scan_count']} 轮\n"
        f"📊 累计信号: {stats['signal_count']} 条\n"
        f"🕐 上次扫描: {last_scan_ago:.1f} 分钟前\n"
        f"🕐 当前时间: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}"
    )
    ok = send_tg(msg)
    print(f"[HEARTBEAT] {'OK' if ok else 'FAIL'} | 运行 {uptime_hours:.1f}h / 扫描 {stats['scan_count']} / 信号 {stats['signal_count']}")


def send_daily_report():
    """每日日报：汇总过去 24 小时信号统计"""
    rows = _read_signals_last_hours(24)

    if not rows:
        msg = (
            f"📅 <b>每日日报</b> (过去 24 小时)\n"
            f"━━━━━━━━━━━━━━━━━━━━\n"
            f"暂无信号记录"
        )
        ok = send_tg(msg)
        print(f"[DAILY] {'OK' if ok else 'FAIL'} | 0 条信号")
        return

    # 按周期统计
    tf_count = {"15m": 0, "1h": 0, "4h": 0}
    type_count = {"F_BULL": 0, "F_BEAR": 0, "M_BULL": 0, "M_BEAR": 0}
    for r in rows:
        if r["tf"] in tf_count:
            tf_count[r["tf"]] += 1
        if r["sig_type"] in type_count:
            type_count[r["sig_type"]] += 1

    # 最强 5 个信号（按 ratio 降序）
    top5 = sorted(rows, key=lambda r: -r["ratio"])[:5]

    lines = [
        f"📅 <b>每日日报</b> (过去 24 小时)",
        f"━━━━━━━━━━━━━━━━━━━━",
        f"📊 <b>总信号数</b>: {len(rows)} 条",
        f"",
        f"<b>按周期</b>:",
        f"  🟢 15m: {tf_count['15m']} 条",
        f"  🔵  1h: {tf_count['1h']} 条",
        f"  🔴  4h: {tf_count['4h']} 条",
        f"",
        f"<b>按类型</b>:",
        f"  🔺 Quantum底: {type_count['F_BULL']}  |  🔻 Quantum顶: {type_count['F_BEAR']}",
        f"  🔺    Flow底: {type_count['M_BULL']}  |  🔻    Flow顶: {type_count['M_BEAR']}",
        f"",
        f"<b>🔥 最强 5 个信号</b>:",
    ]
    for r in top5:
        stars_str = "★" * r["stars"] + "☆" * (5 - r["stars"])
        lines.append(
            f"  {stars_str} {r['symbol']} [{r['tf']} {r['sig_type']}] "
            f"{r['ratio']:.1f}x @ {r['price']:.4f}"
        )
    lines.append(f"━━━━━━━━━━━━━━━━━━━━")
    lines.append(f"📅 {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}")

    msg = "\n".join(lines)
    ok = send_tg(msg)
    print(f"[DAILY] {'OK' if ok else 'FAIL'} | {len(rows)} 条信号")


def send_crash_notice(exc_type, exc_value):
    """程序崩溃时发通知（简短，避免发不出去）"""
    try:
        msg = (
            f"🚨 <b>脚本崩溃</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━\n"
            f"时间: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}\n"
            f"异常类型: <code>{exc_type}</code>\n"
            f"错误信息: <code>{str(exc_value)[:300]}</code>\n"
            f"请尽快检查服务器！"
        )
        send_tg(msg)
    except Exception as e:
        print(f"  [WARN] 崩溃通知发送失败: {e}")


# ==============================================================================
# 11. 【主循环】
# ==============================================================================

def main():
    print("[INIT] 初始化交易所连接...")
    exchange = ccxt.binance({
        'apiKey':  BINANCE_API_KEY,
        'secret':  BINANCE_SECRET_KEY,
        'options': {'defaultType': 'future'},
        'enableRateLimit': True
    })
    print("[OK] 交易所初始化完成")

    print("[INIT] 发送启动宣告至 Telegram...")
    fresh_desc = " / ".join(f"{tf}:{FRESH_BARS_MAP.get(tf, FRESH_BARS_DEFAULT)}根"
                            for tf in (t["timeframe"] for t in SCAN_TASKS))
    interval_desc = " / ".join(f"{t['timeframe']}:{t['interval_minutes']}分"
                               for t in SCAN_TASKS)
    send_tg(
        f"<b>Quantum Flow 背离侦察系统 v3.8 启动</b>\n"
        f"时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
        f"监控周期: 15m / 1h / 4h\n"
        f"扫描频率: {interval_desc}\n"
        f"合约池: Binance USDT 永续 Top {TOP_N} (按 24h 成交额)\n"
        f"并发扫描: {SCAN_WORKERS} 线程\n"
        f"新鲜度: {fresh_desc}\n"
        f"失败退避: 连续 {FAIL_STRIKE_LIMIT} 次 → 冷藏 {COOLDOWN_SECONDS // 60} 分钟\n"
        f"推送分级: ≥ {MSG_TIER_STARS}★ 单独推 / &lt; {MSG_TIER_STARS}★ 合并摘要\n"
        f"机器人路由: 15m → 第一机器人 / 1h·4h·系统通知 → 默认机器人\n"
        f"监控: 💓 心跳每{HEARTBEAT_INTERVAL_HOURS}h / 📅 日报UTC 0点 / 🚨 崩溃自动通知\n"
        f"信号: Quantum/Flow 背离 (带强度星级)\n"
        f"状态: 全市场扫描中..."
    )

    # v3.4 启动首轮静默：用当前时间初始化，首次扫描会按正常节奏（等 interval_minutes 分钟后）触发
    # 避免启动瞬间 3 个周期同时扫描、爆推一堆旧信号
    _now_init = time.time()
    last_run_times = {task['timeframe']: _now_init for task in SCAN_TASKS}

    # v3.6 监控调度：
    # - 心跳：每 HEARTBEAT_INTERVAL_HOURS 小时一次，从启动时间开始算
    # - 日报：每天 UTC DAILY_REPORT_HOUR_UTC 点（0 点）一次
    last_heartbeat_time = _now_init                    # 启动后 HEARTBEAT_INTERVAL_HOURS 小时才发第一条
    last_daily_date     = datetime.now(timezone.utc).date()   # 启动当天不发日报，避免重启刷屏

    print("\n" + "=" * 60)
    print("  Quantum Flow 背离侦察系统 v3.8 部署成功")
    print("=" * 60 + "\n")

    while True:
        try:
            now = time.time()
            for task in SCAN_TASKS:
                tf           = task['timeframe']
                interval_sec = task['interval_minutes'] * 60
                if now - last_run_times[tf] >= interval_sec:
                    scan_markets(exchange, tf)
                    last_run_times[tf] = time.time()
                    time.sleep(2)

            # v3.6 心跳消息（每 6 小时）
            if now - last_heartbeat_time >= HEARTBEAT_INTERVAL_HOURS * 3600:
                send_heartbeat()
                last_heartbeat_time = now

            # v3.6 每日日报（UTC 0 点第一次进入循环时发）
            now_utc = datetime.now(timezone.utc)
            if now_utc.hour == DAILY_REPORT_HOUR_UTC and now_utc.date() != last_daily_date:
                send_daily_report()
                last_daily_date = now_utc.date()

            sys.stdout.write(f"\r[{datetime.now().strftime('%H:%M:%S')}] 待机监视中...   ")
            sys.stdout.flush()
            time.sleep(10)

        except KeyboardInterrupt:
            print("\n[EXIT] 收到中断信号，保存去重缓存后退出...")
            _save_sent_signals()
            break
        except Exception as e:
            print(f"\n[ERROR] 主循环异常: {type(e).__name__}: {e}")
            time.sleep(60)


if __name__ == "__main__":
    # v3.6 顶层崩溃捕获：main() 里的异常已经会被内部 try 吃掉
    # 这里捕获的是初始化阶段的致命错误（如 ccxt 初始化失败、import 失败等）
    try:
        main()
    except Exception as _e:
        print(f"\n[FATAL] 顶层崩溃: {type(_e).__name__}: {_e}")
        send_crash_notice(type(_e).__name__, _e)
        raise
