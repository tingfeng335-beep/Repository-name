# 项目上下文备忘（给下一个 AI 看的）

> 这份文档让新会话能快速接手这个项目。新对话开始时把这份文件发给 AI，或者让 AI 读 `PROJECT_CONTEXT.md`。

---

## 一、项目是什么

**Quantum Flow 背离侦察系统** —— Python 脚本 + Telegram 推送机器人。

- 扫描 Binance USDT 永续合约 Top 230（按 24h 成交额）
- 三个周期：15m / 1h / 4h
- 检测两种背离：Quantum Fusion 背离 + Money Flow 背离
- 信号出现时自动推送到 Telegram
- 参考 Pine Script 原版：文件 `Pine_Script.pine` 或在 TV 上的 "Quantum Flow Cipher" 指标

**核心文件**：`scanner.py`（唯一入口）

---

## 二、当前版本

**v3.3-fix2**（commit `add3179`）

**分支**：`optimize/scanner-v1`

下载命令：
```powershell
Invoke-WebRequest -Uri "https://raw.githubusercontent.com/tingfeng335-beep/Repository-name/optimize/scanner-v1/scanner.py" -OutFile "C:\Users\Administrator\scanner.py"
```

---

## 三、核心配置（scanner.py 第 30~90 行）

```python
TG_CHAT_IDS = [
    "7470996017",    # 主账号
    "6587035253",    # 朋友 @Nick_Tuz
]

SCAN_TASKS = [
    {"timeframe": "15m", "interval_minutes": 3},
    {"timeframe": "1h",  "interval_minutes": 8},
    {"timeframe": "4h",  "interval_minutes": 60},
]

TOP_N         = 230    # Binance Top N 合约
SCAN_WORKERS  = 20     # 并发线程

F_PL = 5; F_PR = 3; F_MB = 8;  F_MD = 3.0   # Quantum 枢轴/间距/幅度阈值
M_PL = 10; M_PR = 6; M_MB = 20; M_MD = 3.0  # Flow 枢轴/间距/幅度阈值

FRESH_BARS_MAP = {"15m": 3, "1h": 3, "4h": 2}   # 分周期新鲜度
MSG_TIER_STARS = 4       # >= 4★ 单独推，< 4★ 合并摘要
```

---

## 四、核心指标逻辑（**不能改**）

`calculate_quantum_flow()` 函数 —— 严格复刻 Pine Script，任何人都不要动：
- Delta Volume / CVD 动量 / MACD / 归一化 / ATR 加权 / Fusion 融合 / Money Flow

`detect_pivots()` —— 严格对齐 Pine `ta.pivothigh/pivotlow`：**两侧都用严格 `>` / `<`**。

---

## 五、已修复的 Bug 历史

1. **v1 → v3**：从串行 → 20 并发，加去重、失败退避、分级推送、多用户
2. **v3.2**：分周期 emoji 标签（⚡短线 / 🎯中线 / 💎主力）
3. **v3.3-fix1**：枢轴检测从 `>=` 改为严格 `>`（对齐 Pine）；Flow lookback 修 off-by-one
4. **v3.3-fix2**：`pb = i - F_PR` → `pb = i`（4 处）。因为 `detect_pivots` 已经把枢轴标记在真实位置，不需要再偏移

---

## 六、已知问题 / 待验证

### 待用户验证
**"幽灵背离"问题**（v3.3-fix2 刚修完，需要用户观察）
- 现象：TG 推送了某币的背离，但 TV 上 Quantum Flow Cipher 指标没画背离线
- 根因（推测）：`pb` 偏移 Bug，已在 v3.3-fix2 修复
- 验证方式：用户收到推送后去 TV 对比（必须**时区切 UTC**）

### 暂搁置
1. **微信推送**：用户研究过企业微信、PushPlus、Server酱，最终决定暂不接入
2. **TV 指标版本**：用户 TV 上用的是公开的 Quantum Flow Cipher（Pine 源码已在对话中提供，如需要可再让用户提供）

---

## 七、用户的使用习惯与偏好

- **不懂代码**：每次改完需要给 commit 编号 + 下载命令
- **希望核心指标不动**：反复确认过 `calculate_quantum_flow` 没改
- **关注推送时效**：信号太旧的会不满意
- **关注信号数量**：信号太少会觉得"不准"
- **沟通方式**：中文，简洁直接，不要长篇解释

---

## 八、文件结构

```
Repository-name/
├── scanner.py              # 主程序（唯一入口）
├── sent_signals.json       # 去重缓存（自动生成，启动时加载）
├── signals.log             # 信号历史 CSV（自动生成）
├── TODO.md                 # 之前的 TODO 清单（可能过期）
├── README.md
└── PROJECT_CONTEXT.md      # 本文件
```

---

## 九、常用命令

### 运行
```powershell
python scanner.py
```

### 重置去重（改完代码后）
```powershell
del sent_signals.json
```

### 下载最新版
```powershell
Invoke-WebRequest -Uri "https://raw.githubusercontent.com/tingfeng335-beep/Repository-name/optimize/scanner-v1/scanner.py" -OutFile "C:\Users\Administrator\scanner.py"
```

### 检查本地版本
```powershell
Get-Content C:\Users\Administrator\scanner.py | Select-String "pb = i"
```

---

## 十、给下一个 AI 的话

1. **读完这份文档再开始**
2. **核心指标算法 `calculate_quantum_flow` 永远不要动**
3. **改代码时带上 commit 编号**（v3.4, v3.5…）和下载命令
4. **用户 TV 时区是 UTC**，推送消息里的时间也是 UTC
5. 用户可能会让你对比 Pine Script 原版 —— 源码在历史对话里，或者让用户重新提供
