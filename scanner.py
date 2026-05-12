# -*- coding: utf-8 -*-
"""
Quantum Flow 背离侦察系统
- 每个时间周期按 Binance USDT 永续合约 24h 成交额降序取前 N 个扫描
- 严格排除交割合约、USDC 计价合约
- 信号带去重（持久化），避免刷屏
- 丢弃未收盘 K 线，防止信号闪烁
- 主循环有顶层异常保护，7x24 不挂
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
from datetime import datetime, timezone

# ==============================================================================
# 1. 【核心配置区】
# ==============================================================================

TG_TOKEN           = os.getenv("TG_TOKEN",           "8597069493:AAEmXzUJ3Yv42NGd2EsP3M93aatLjqzPWFI")
TG_CHAT_ID         = os.getenv("TG_CHAT_ID",         "7470996017")
BINANCE_API_KEY    = os.getenv("BINANCE_API_KEY",    "LmoOgqAkSmcpilfJcdEW4genLy76swigcTnIMPVR7gvqwR55aY4lxjdJbigzeKY8")
BINANCE_SECRET_KEY = os.getenv("BINANCE_SECRET_KEY", "aErEwJ3gOvBr6mS92zOaJ9mhbBqeors2WXp7nFESyZaJQ2e38giKwWr0NMAXofJI")

# --- 扫描任务表 ---
SCAN_TASKS = [
    {"timeframe": "15m", "interval_minutes": 5},
    {"timeframe": "1h",  "interval_minutes": 15},
    {"timeframe": "4h",  "interval_minutes": 60},
]

# --- 合约池设置：按 24h 成交额降序取前 TOP_N 个 ---
TOP_N               = 180
MARKETS_CACHE_TTL   = 3600      # 合约列表缓存 1 小时

# --- 枢轴与背离参数 ---
F_PL = 5;   F_PR = 3;   F_MB = 8;   F_MD = 3.0
M_PL = 10;  M_PR = 6;   M_MB = 20;  M_MD = 3.0
HOLD_BARS = 8

# --- 去重持久化文件 ---
SENT_SIGNALS_FILE = "sent_signals.json"


# ==============================================================================
# 2. 【通讯模块】
# ==============================================================================

# 禁用系统代理，避免 Windows 代理设置导致 requests 卡死
_NO_PROXY = {"http": None, "https": None}


def send_tg(message, retries=3):
    """发送战报至 Telegram，失败指数退避重试"""
    url = f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage"
    payload = {"chat_id": TG_CHAT_ID, "text": message, "parse_mode": "HTML"}

    for attempt in range(retries):
        try:
            resp = requests.post(
                url, json=payload,
                timeout=(3, 5),
                proxies=_NO_PROXY
            )
            result = resp.json()
            if result.get("ok"):
                return True
            # Telegram 返回错误
            desc = result.get("description", "")
            if "Too Many Requests" in desc and attempt < retries - 1:
                retry_after = result.get("parameters", {}).get("retry_after", 2 ** attempt)
                time.sleep(retry_after)
                continue
            print(f"  [WARN] TG 返回错误: {desc}")
            return False
        except (requests.exceptions.ConnectTimeout,
                requests.exceptions.ReadTimeout,
                requests.exceptions.ConnectionError) as e:
            if attempt < retries - 1:
                time.sleep(2 ** attempt)   # 1s, 2s, 4s
                continue
            print(f"  [WARN] TG 最终失败: {type(e).__name__}: {e}")
            return False
        except Exception as e:
            print(f"  [WARN] TG 发送异常: {type(e).__name__}: {e}")
            return False
    return False


# ==============================================================================
# 3. 【去重缓存：持久化】
# ==============================================================================

def _load_sent_signals():
    try:
        with open(SENT_SIGNALS_FILE, "r", encoding="utf-8") as f:
            raw = json.load(f)
        # 反序列化：字符串 key "symbol|tf|type" -> tuple
        return {tuple(k.split("|", 2)): v for k, v in raw.items()}
    except (FileNotFoundError, json.JSONDecodeError):
        return {}


def _save_sent_signals():
    try:
        serializable = {"|".join(k): v for k, v in sent_signals.items()}
        with open(SENT_SIGNALS_FILE, "w", encoding="utf-8") as f:
            json.dump(serializable, f, ensure_ascii=False, indent=2)
    except Exception as e:
        print(f"  [WARN] 去重缓存保存失败: {e}")


sent_signals = _load_sent_signals()
atexit.register(_save_sent_signals)


def should_send(symbol, tf, sig_type, bar_time_ms):
    """若该信号（同 K 线）已发送过，返回 False；否则登记并返回 True"""
    key = (symbol, tf, sig_type)
    if sent_signals.get(key) == bar_time_ms:
        return False
    sent_signals[key] = bar_time_ms
    return True


# ==============================================================================
# 4. 【核心指标算法】
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
    检测枢轴高低点（左严格 > / 右 >=；左严格 < / 右 <=），
    贴近 TradingView ta.pivothigh/pivotlow 的常见实现，平台期不重复出枢轴。
    """
    n = len(series_values)
    ph_vals = [np.nan] * n
    pl_vals = [np.nan] * n

    for i in range(left, n - right):
        v = series_values[i]
        is_high = (all(v >  series_values[i - j] for j in range(1, left + 1)) and
                   all(v >= series_values[i + j] for j in range(1, right + 1)))
        if is_high:
            ph_vals[i] = v

        is_low = (all(v <  series_values[i - j] for j in range(1, left + 1)) and
                  all(v <= series_values[i + j] for j in range(1, right + 1)))
        if is_low:
            pl_vals[i] = v

    return ph_vals, pl_vals


def detect_divergence_signals(df, symbol, timeframe):
    """背离检测 + 双背离共振，返回 (msg, sig_type) 列表"""
    out = []
    n = len(df)
    if n < 100:
        return out

    fusion_arr = df['fusion_d'].values
    mf_arr     = df['mf'].values
    high_arr   = df['high'].values
    low_arr    = df['low'].values

    f_ph_vals, f_pl_vals = detect_pivots(fusion_arr, F_PL, F_PR)
    m_ph_vals, m_pl_vals = detect_pivots(mf_arr, M_PL, M_PR)

    fh_v = fh_p = fh_b = np.nan
    fl_v = fl_p = fl_b = np.nan
    mh_v = mh_p = mh_b = np.nan
    ml_v = ml_p = ml_b = np.nan

    f_bull = f_bear = m_bull = m_bear = False
    f_bl = f_bh = m_bl = m_bh = 0
    f_bull_sig = f_bear_sig = m_bull_sig = m_bear_sig = False

    start = max(F_PL, F_PR, M_PL, M_PR) + 20

    for i in range(start, n):
        # ── Quantum 顶背离 ──
        if not np.isnan(f_ph_vals[i]):
            pb = i - F_PR
            pv = f_ph_vals[i]
            pp = high_arr[pb] if pb >= 0 else high_arr[i]
            if not np.isnan(fh_v) and not np.isnan(fh_b):
                if pp >= fh_p and (fh_v - pv) >= F_MD and (pb - fh_b) >= F_MB:
                    f_bear = True
            fh_v, fh_p, fh_b = pv, pp, pb

        # ── Quantum 底背离 ──
        if not np.isnan(f_pl_vals[i]):
            pb = i - F_PR
            pv = f_pl_vals[i]
            pp = low_arr[pb] if pb >= 0 else low_arr[i]
            if not np.isnan(fl_v) and not np.isnan(fl_b):
                if pp <= fl_p and (pv - fl_v) >= F_MD and (pb - fl_b) >= F_MB:
                    f_bull = True
            fl_v, fl_p, fl_b = pv, pp, pb

        # ── Flow 顶背离 ──
        if not np.isnan(m_ph_vals[i]):
            pb = i - M_PR
            pv = m_ph_vals[i]
            pp = high_arr[pb] if pb >= 0 else high_arr[i]
            lookback = M_PL * 3
            hh = high_arr[max(0, i - lookback):i + 1].max()
            ll = low_arr[max(0, i - lookback):i + 1].min()
            if pp >= ll + (hh - ll) * 0.5:
                if not np.isnan(mh_v) and not np.isnan(mh_b):
                    if pp >= mh_p and (mh_v - pv) >= M_MD and (pb - mh_b) >= M_MB:
                        m_bear = True
            mh_v, mh_p, mh_b = pv, pp, pb

        # ── Flow 底背离 ──
        if not np.isnan(m_pl_vals[i]):
            pb = i - M_PR
            pv = m_pl_vals[i]
            pp = low_arr[pb] if pb >= 0 else low_arr[i]
            lookback = M_PL * 3
            hh = high_arr[max(0, i - lookback):i + 1].max()
            ll = low_arr[max(0, i - lookback):i + 1].min()
            if pp <= ll + (hh - ll) * 0.5:
                if not np.isnan(ml_v) and not np.isnan(ml_b):
                    if pp <= ml_p and (pv - ml_v) >= M_MD and (pb - ml_b) >= M_MB:
                        m_bull = True
            ml_v, ml_p, ml_b = pv, pp, pb

        # ── 信号保持窗口 ──
        f_bh = HOLD_BARS if f_bear else max(0, f_bh - 1)
        f_bl = HOLD_BARS if f_bull else max(0, f_bl - 1)
        m_bh = HOLD_BARS if m_bear else max(0, m_bh - 1)
        m_bl = HOLD_BARS if m_bull else max(0, m_bl - 1)

        if i == n - 1:
            f_bull_sig = f_bl > 0
            f_bear_sig = f_bh > 0
            m_bull_sig = m_bl > 0
            m_bear_sig = m_bh > 0

        f_bear = f_bull = m_bear = m_bull = False

    curr_f    = df['fusion_d'].iloc[-1]
    curr_mf   = df['mf'].iloc[-1]
    curr_p    = df['close'].iloc[-1]
    bar_time  = int(df['time'].iloc[-1])
    bar_str   = datetime.fromtimestamp(bar_time / 1000, tz=timezone.utc).strftime('%Y-%m-%d %H:%M UTC')

    dbl_bull = (f_bl > 0 and m_bl > 0)
    dbl_bear = (f_bh > 0 and m_bh > 0)

    if dbl_bull:
        out.append(("DBL_BULL",
            f"[UP x2] <b>{symbol} [{timeframe}] 双底背离共振</b>\n"
            f"Quantum + Flow 同时底背离\n"
            f"Fusion: {curr_f:.1f} | Flow: {curr_mf:.2f} | 价: {curr_p:.4f}\n"
            f"K线: {bar_str}"
        ))
    if dbl_bear:
        out.append(("DBL_BEAR",
            f"[DN x2] <b>{symbol} [{timeframe}] 双顶背离共振</b>\n"
            f"Quantum + Flow 同时顶背离\n"
            f"Fusion: {curr_f:.1f} | Flow: {curr_mf:.2f} | 价: {curr_p:.4f}\n"
            f"K线: {bar_str}"
        ))
    if f_bull_sig and not dbl_bull:
        out.append(("F_BULL",
            f"[UP] <b>{symbol} [{timeframe}] Quantum 底背离</b>\n"
            f"Fusion: {curr_f:.1f} | 价格新低，指标未新低 | 价: {curr_p:.4f}\n"
            f"K线: {bar_str}"
        ))
    if f_bear_sig and not dbl_bear:
        out.append(("F_BEAR",
            f"[DN] <b>{symbol} [{timeframe}] Quantum 顶背离</b>\n"
            f"Fusion: {curr_f:.1f} | 价格新高，指标未新高 | 价: {curr_p:.4f}\n"
            f"K线: {bar_str}"
        ))
    if m_bull_sig and not dbl_bull:
        out.append(("M_BULL",
            f"[UP] <b>{symbol} [{timeframe}] Flow 底背离</b>\n"
            f"Flow: {curr_mf:.2f} | 价格新低，柱状线未新低 | 价: {curr_p:.4f}\n"
            f"K线: {bar_str}"
        ))
    if m_bear_sig and not dbl_bear:
        out.append(("M_BEAR",
            f"[DN] <b>{symbol} [{timeframe}] Flow 顶背离</b>\n"
            f"Flow: {curr_mf:.2f} | 价格新高，柱状线未新高 | 价: {curr_p:.4f}\n"
            f"K线: {bar_str}"
        ))

    # 附带 K 线时间戳，供去重登记
    return [(sig_type, msg, bar_time) for (sig_type, msg) in out]


# ==============================================================================
# 5. 【合约池：永续 + USDT + 24h 成交额 Top N】
# ==============================================================================

_markets_cache = {"time": 0, "symbols": []}


def get_top_symbols(exchange, top_n=TOP_N, ttl=MARKETS_CACHE_TTL):
    """获取 24h 成交额 Top N 的 Binance USDT 永续合约（带缓存）"""
    if _markets_cache["symbols"] and (time.time() - _markets_cache["time"] < ttl):
        return _markets_cache["symbols"]

    markets = exchange.fetch_markets()
    eligible = [
        m for m in markets
        if m.get('swap', False)               # 永续合约
        and m.get('active', False)
        and m.get('quote') == 'USDT'          # USDT 计价
        and not m.get('expiry')               # 排除交割合约
        and 'USDC' not in m['symbol']         # 排除 USDC 相关
    ]

    # 用 fetch_tickers 批量拿 24h 成交额
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
# 6. 【侦察引擎】
# ==============================================================================

def scan_markets(exchange, timeframe):
    """对 Top N 合约按指定周期扫描背离信号"""
    try:
        symbols = get_top_symbols(exchange)
        total = len(symbols)
        print(f"\n[{datetime.now().strftime('%H:%M:%S')}] [SCAN] 周期:{timeframe} | 合约数:{total}")

        alert_count = 0
        tg_logs = []   # 暂存发送结果，最后统一打印，避免打乱进度条

        for index, symbol in enumerate(symbols):
            try:
                percent = (index + 1) / total * 100
                sys.stdout.write(
                    f"\r[{timeframe}] {percent:5.1f}% | {index + 1:>3}/{total}"
                    f" | {symbol:<20} | 警报:{alert_count}    "
                )
                sys.stdout.flush()

                bars = exchange.fetch_ohlcv(symbol, timeframe=timeframe, limit=200)
                if len(bars) < 101:   # 丢掉最后一根未收盘 K 线后仍需 >=100 根
                    continue

                df = pd.DataFrame(bars, columns=['time', 'open', 'high', 'low', 'close', 'volume'])
                df = df.iloc[:-1].reset_index(drop=True)   # 丢弃未收盘 K 线

                df = calculate_quantum_flow(df)
                signals = detect_divergence_signals(df, symbol, timeframe)

                for sig_type, msg, bar_time in signals:
                    if not should_send(symbol, timeframe, sig_type, bar_time):
                        continue
                    ok = send_tg(msg)
                    alert_count += 1
                    tg_logs.append(f"  [{'OK' if ok else 'FAIL'}] {symbol} {sig_type}")

            except Exception as e:
                tg_logs.append(f"  [SKIP] {symbol}: {type(e).__name__}")
                continue

        # 换行，扫描结束
        sys.stdout.write("\n")
        print(f"[{timeframe}] 巡逻完毕 | 新增信号 {alert_count} 个")
        for line in tg_logs[-20:]:   # 只打印最近 20 条，避免刷屏
            print(line)

        # 每个周期扫完持久化一次去重缓存，防止异常中断丢记录
        _save_sent_signals()

    except Exception as e:
        print(f"\n[ERROR] 扫描引擎异常: {type(e).__name__}: {e}")


# ==============================================================================
# 7. 【主循环】
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
    send_tg(
        f"<b>Quantum Flow 背离侦察系统启动</b>\n"
        f"时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
        f"监控周期: 15m / 1h / 4h\n"
        f"合约池: Binance USDT 永续 Top {TOP_N} (按 24h 成交额)\n"
        f"信号: Quantum/Flow/双背离共振\n"
        f"状态: 全市场扫描中..."
    )

    last_run_times = {task['timeframe']: 0 for task in SCAN_TASKS}

    print("\n" + "=" * 60)
    print("  Quantum Flow 背离侦察系统部署成功")
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

            sys.stdout.write(f"\r[{datetime.now().strftime('%H:%M:%S')}] 待机监视中...   ")
            sys.stdout.flush()
            time.sleep(10)

        except KeyboardInterrupt:
            print("\n[EXIT] 收到中断信号，保存去重缓存后退出...")
            _save_sent_signals()
            break
        except Exception as e:
            print(f"\n[ERROR] 主循环异常: {type(e).__name__}: {e}")
            time.sleep(60)   # 等一分钟重试


if __name__ == "__main__":
    main()
