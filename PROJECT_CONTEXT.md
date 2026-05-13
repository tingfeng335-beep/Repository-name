# 项目上下文备忘（给下一个 AI 看的）

> 这份文档让新会话能快速接手这个项目。新对话开始时把这份文件发给 AI，或者让 AI 读 `PROJECT_CONTEXT.md`。
> **每次改代码都要更新这份文档的"五、更新日志"。**

---

## 一、项目是什么

**Quantum Flow 背离侦察系统** —— Python 脚本 + Telegram 推送机器人。

- 扫描 Binance USDT 永续合约 Top 230（按 24h 成交额）
- 三个周期：15m / 1h / 4h
- 检测两种背离：Quantum Fusion 背离 + Money Flow 背离
- 信号出现时自动推送到 Telegram
- 参考 Pine Script 原版：TV 上的 "Quantum Flow Cipher" 指标

**核心文件**：`scanner.py`（唯一入口）

---

## 二、当前版本

**v3.5**（commit `cad1557`）

**分支**：`optimize/scanner-v1`（所有 PR 合并到这里）

下载命令：
```powershell
Invoke-WebRequest -Uri "https://raw.githubusercontent.com/tingfeng335-beep/Repository-name/optimize/scanner-v1/scanner.py" -OutFile "C:\Users\Administrator\scanner.py"
```

---

## 三、核心配置（scanner.py 顶部）

```python
TG_CHAT_IDS = [
    "7470996017",    # 主账号
    "6587035253",    # 朋友 @Nick_Tuz
]

SCAN_TASKS = [
    {"timeframe": "15m", "interval_minutes": 3},
    {"timeframe": "1h",  "interval_minutes": 8},
    {"timeframe": "4h",  "interval_minutes": 20},   # v3.5: 60→20
]

TOP_N         = 230    # Binance Top N 合约
SCAN_WORKERS  = 20     # 并发线程

F_PL = 5; F_PR = 3; F_MB = 8;  F_MD = 3.0   # Quantum 枢轴/间距/幅度阈值
M_PL = 10; M_PR = 6; M_MB = 20; M_MD = 3.0  # Flow 枢轴/间距/幅度阈值

FRESH_BARS_MAP = {"15m": 3, "1h": 3, "4h": 2}   # 分周期新鲜度
MSG_TIER_STARS = 4       # >= 4★ 单独推，< 4★ 合并摘要

# v3.5: 分周期色带（醒目区分）
TF_STYLE_MAP = {
    "15m": {"emoji": "⚡",  "tag": "短线", "band": "🟢"},   # 绿
    "1h":  {"emoji": "🎯",  "tag": "中线", "band": "🔵"},   # 蓝
    "4h":  {"emoji": "💎",  "tag": "主力", "band": "🔴"},   # 红（最醒目）
}
```

---

## 四、核心指标逻辑（**永远不能改**）

`calculate_quantum_flow()` 函数 —— 严格复刻 Pine Script，任何人都不要动：
- Delta Volume / CVD 动量 / MACD / 归一化 / ATR 加权 / Fusion 融合 / Money Flow

`detect_pivots()` —— 严格对齐 Pine `ta.pivothigh/pivotlow`：**两侧都用严格 `>` / `<`**。

---

## 五、更新日志（最新的在上）

> 📋 **每次改代码都必须在这里加一条记录**。格式：版本号、commit、分支/PR、改了什么、为什么、风险。

### v3.5（2026-05-13，commit `cad1557`，PR #2）
**改动**：
1. **4h 扫描频率**：60 分钟 → **20 分钟**（4h 信号最坏延迟从 1 小时变 20 分钟）
2. **分周期色带装饰**：每条推送消息首尾加 10 个方块色带
   - 15m = 🟢 绿色（短线）
   - 1h  = 🔵 蓝色（中线）
   - 4h  = 🔴 红色（主力大指标，最醒目）
3. 摘要消息也加色带

**为什么**：用户想加快 4h 捕获速度、并要求 TG 推送区分颜色（4h 红/1h 蓝/15m 绿）。

**API 影响**：4h 改到 20 分钟后，额外 API 调用 +11.5 次/分钟；总峰值远低于 Binance 2400/分钟限制。

**核心指标**：✅ 完全没动。

**风险**：0（只改配置 + 字符串拼接）。

---

### v3.4（2026-05-13，commit `e02ad88`，PR #1）
**改动**：
1. **`sent_signals.json` 原子写入**：先写 `.tmp` 再 `os.replace` 替换，防止写到一半崩溃/断电导致 JSON 损坏
2. **启动首轮静默**：`last_run_times` 初始化用 `time.time()` 而不是 `0`，避免启动瞬间 3 个周期同时扫描、爆推几十条旧信号

**为什么**：基础健壮性修复。密钥下移到 env 的改动被用户否决（API 是临时测试 key，不用纠结）。

**核心指标**：✅ 完全没动。

**风险**：0。

---

### v3.3-fix2（commit `add3179`）
**改动**：`pb = i - F_PR` → `pb = i`（4 处）。因为 `detect_pivots` 已经把枢轴标记在真实位置，不需要再偏移。

**为什么**：修"幽灵背离"问题。

---

### v3.3-fix1（commit 略）
**改动**：枢轴检测从 `>=` 改为严格 `>`（对齐 Pine）；Flow lookback 修 off-by-one。

---

### v3.2
**改动**：分周期 emoji 标签（⚡短线 / 🎯中线 / 💎主力）；信号按强度分级推送（≥ 4★ 单独推，< 4★ 合并摘要）。

---

### v1 → v3
从串行 → 20 并发；加去重、失败退避、多用户推送。

---

## 六、已知问题 / 待验证

### 待用户验证
**"幽灵背离"问题**（v3.3-fix2 修完，需要用户观察）
- 现象：TG 推送了某币的背离，但 TV 上 Quantum Flow Cipher 指标没画背离线
- 根因（推测）：`pb` 偏移 Bug，已在 v3.3-fix2 修复
- 验证方式：用户收到推送后去 TV 对比（必须**时区切 UTC**）

### 暂搁置
1. **微信推送**：用户研究过企业微信、PushPlus、Server酱，最终决定暂不接入
2. **TV 指标版本**：用户 TV 上用的是公开的 Quantum Flow Cipher
3. **信号追踪/胜率回测**：用户目前不需要
4. **密钥下移到 .env**：用户说是临时测试 key，不用管

---

## 七、用户的使用习惯与偏好

- **不懂代码**：每次改完需要给 commit 编号 + 下载命令
- **希望核心指标不动**：反复确认过 `calculate_quantum_flow` 没改
- **讨厌大改动**：改动越少越好，看不懂的改动会被否决
- **交易风格**：C 型（信号辅助决策，自己看盘），不做自动交易
- **止损风格**：固定百分比（不要 ATR 动态）
- **推送数量**：保持现状"刚好"，不要硬砍
- **关注推送时效**：信号太旧会不满意
- **沟通方式**：中文，简洁直接，不要长篇解释

---

## 八、文件结构

```
Repository-name/
├── scanner.py              # 主程序（唯一入口，~850 行）
├── sent_signals.json       # 去重缓存（自动生成，启动时加载）
├── sent_signals.json.tmp   # 原子写入的临时文件（v3.4 新增）
├── signals.log             # 信号历史 CSV（自动生成）
├── TODO.md                 # 早期 TODO 清单（可能过期）
├── README.md
└── PROJECT_CONTEXT.md      # 本文件
```

---

## 九、常用命令

### 运行
```powershell
python scanner.py
```

### 重置去重（改完代码后推荐先删）
```powershell
del sent_signals.json
```

### 下载最新版
```powershell
Invoke-WebRequest -Uri "https://raw.githubusercontent.com/tingfeng335-beep/Repository-name/optimize/scanner-v1/scanner.py" -OutFile "C:\Users\Administrator\scanner.py"
```

### 检查本地版本
```powershell
Get-Content C:\Users\Administrator\scanner.py | Select-String "v3\."
```

---

## 十、Git 工作流（必须遵守）

1. **永远不要直接提交到 `optimize/scanner-v1`**（用户的主分支）
2. **每次改动开新分支**：
   - 修 Bug：`fix/vX.Y-描述`（如 `fix/v3.4-safety`）
   - 加功能：`feat/vX.Y-描述`（如 `feat/v3.5-color-bands`）
3. **开 PR 让用户审阅**，合并到 `optimize/scanner-v1`
4. **commit message 带版本号**：`fix(v3.4): ...` / `feat(v3.5): ...`
5. **每次改完必须更新本文档的"五、更新日志"**

---

## 十一、给下一个 AI 的话

1. **读完这份文档再开始**（尤其"五、更新日志"和"七、用户习惯"）
2. **核心指标算法 `calculate_quantum_flow` 永远不要动**
3. **改代码时带上 commit 编号**（vX.Y）和 PowerShell 下载命令
4. **改完必须更新"五、更新日志"**（这是硬规矩）
5. **改动越少越好**，一次只做一件事，多个改动分多个 PR
6. **用户 TV 时区是 UTC**，推送消息里的时间也是 UTC
7. **不要自己发挥做"锦上添花"**（如趋势过滤、回测、自动交易）—— 用户不要
