# Axiom Quant — 工程憲章 (Engineering Constitution)

> 本文件是 `quant_etl` 專案的最高工程規範。
> 所有協作者（人與 AI）在修改任何程式碼前，必須先完整讀取並遵守本憲章。
>
> **版本**：v1.0  
> **專案路徑**：`C:\Users\USER\Desktop\我的AI工作區\quant_etl`  
> **核心定位**：這不是 demo，不是玩具專案。這是涉及真實資產、真實風險、真實決策責任的量化系統。

---

## 一、最高執行原則

1. **先求不炸，再求漂亮**
2. **先找 root cause，再改 code**
3. **先做 impact analysis，再動手**
4. **先修 source of truth，不要只修表面 UI**
5. **先驗證，再交付**
6. **禁止 vibe coding 式憑感覺亂補、亂改、亂重構**

---

## 二、防禦性編程鐵律

### 2.1 禁止 `print()`

- 專案內**全面禁止**使用 `print()`
- 所有輸出必須使用 `logging`
- log 必須能清楚指出：
  - 模組與函式名稱
  - 錯誤原因
  - fallback 是否生效
  - 是否影響輸出可信度

### 2.2 所有外部 I/O 必須做防禦處理

以下操作**全部**必須有 `try/except` 與清楚 logging，**絕對不允許 silent crash**：

- API requests（FRED、yfinance、LINE Messaging API 等）
- DB read / write（PostgreSQL）
- External data loading
- LINE Messaging API sending

### 2.3 缺值優先 forward fill

- 尤其低頻資料（PMI / CPI / ISM 等月資料）
- 不可因資料當天沒更新就直接顯示 N/A
- 唯一例外：整個資料庫根本沒有任何有效值

### 2.4 防止極端值爆炸

- 所有除法 / z-score / 比例計算**必須**設分母底線
- 標準做法：`max(std, 0.01)` 或 `rolling_std.clip(lower=0.01)`
- 對應常數：`MIN_ZSCORE_DENOMINATOR = 0.01`（定義於 `etl/cleaner.py`）
- 禁止因分母過小產生不合理的極端值（如 z = ±20 這類爆炸數字）

---

## 三、架構與維護鐵律

### 3.1 禁止邏輯沾黏

以下邏輯**必須分層**，每層職責單一：

| 層級 | 職責 | 對應位置 |
|------|------|---------|
| ETL / 資料抓取 | 從外部取得原始資料 | `etl/` |
| 補值 / 對齊 / 清洗 | forward fill、staleness 檢查 | `etl/cleaner.py` |
| 指標計算 | rolling stats、z-score | `indicators/` |
| 狀態判斷 | Scenario A/B/C、risk light | `engine/` |
| Summary 文案生成 | level / emoji / title / message | `report/daily_report.py` |
| UI / Flex payload | LINE Flex Message 組裝 | `report/line_flex.py` |
| LINE sender | 發送 LINE 訊息 | `report/send_line.py` |
| 監控 daemon | Tripwire / 警報評估 | `monitor/` |

### 3.2 禁止 magic numbers

所有 threshold、window、timeout、cooldown、URL、DB config 等，**必須**集中管理：

- 分析層常數 → `etl/cleaner.py`（如 `MIN_ZSCORE_DENOMINATOR`、`MAX_MONTHLY_STALENESS_DAYS`）
- Tripwire 閾值 → `monitor/tripwire.py` 頂部常數區
- 其餘配置 → `etl/config.py` 或 `.env`

禁止將數字硬埋在 `if/else` 條件中。

### 3.3 單一來源原則（Single Source of Truth）

同一套商業規則**只能定義一次**。特別保護的 SSOT：

- **Risk Summary（red / yellow / green 燈號）**
  - 唯一定義：`report/daily_report.py` → `_zscore_risk_signal_v2()` 回傳 `RiskSummary`
  - 包含：`level`、`icon`、`title`、`message`
  - UI 與文案**必須**吃這一個 SSOT，禁止各自重判

- **Z-Score 分級門檻**

  | 等級 | 條件 | 常數來源 |
  |------|------|---------|
  | red | `abs(z) >= 2.0` | `monitor/tripwire.py` `RED_THRESHOLD_*` |
  | yellow | `abs(z) >= 1.0` | `monitor/tripwire.py` `YELLOW_THRESHOLD_*` |
  | green | `abs(z) < 1.0` | — |

- **Scenario 定義**：`engine/regime.py`
- **Baseline 配置**（VOO 70%、tactical 上限等）：`report/daily_report.py` 頂部常數區

---

## 四、反 Vibe Coding 紀律

### 4.1 禁止未盤點影響面的直接修改

修改前**必須**先確認：

1. 哪些檔案相關？
2. 哪些函式呼叫這段邏輯？
3. 哪些 consumer 依賴這個輸出？
4. 哪裡才是 source of truth？

### 4.2 禁止未授權的大型重構

以下操作**禁止**在未明確授權下執行：

- rename 函式 / 模組
- 更改 DB schema
- 更改 LINE Flex payload key
- 更改 public interface
- 重整整條資料流

**唯一例外**：你能明確證明「不改就修不好」，且已清楚說明最小必要改動。

### 4.3 禁止需求腦補

- 若需求未明確指定 → 保守沿用現有行為
- 禁止自行發明新商業規則並直接寫入系統
- 不確定時：先列出假設，不要偽裝成既定需求

### 4.4 禁止只修畫面不修根因

若問題源自資料、計算、補值、狀態判斷或 payload source of truth，
**必須**優先修根因，不可只在顯示層遮醜。

---

## 五、修改前強制分析清單

在真正修改前，**必須**能回答以下問題（無法回答 → 先停止修改、先回報分析）：

```
1. 本次要修的 root cause 是什麼？
2. 本次修改涉及哪些檔案？
3. 哪裡是 source of truth？
4. 哪些地方只是 consumer？
5. 這次修改屬於：
   □ 局部 bug fix
   □ 共用邏輯修正
   □ 規則層改動
   □ 架構層改動
6. 有哪些回歸風險？
7. 是否可能造成 UI / engine / payload 不同步？
```

---

## 六、實作要求

### 6.1 最小侵入修改

- 優先沿用現有檔案結構
- 優先沿用現有命名風格
- 優先做最小必要修補
- 不為了「更漂亮」做無關重構

### 6.2 保持相容性

- 不可隨意改 LINE Flex payload key（`line_flex.py` `_parse()` 下游依賴）
- 不可隨意改 public interface（`build_report()`、`build_line_flex_payload()` 等）
- 不可破壞既有 consumer
- 若必須改，必須明確說明相容性影響

### 6.3 缺資料時不可崩潰

- 對 `None` / `NaN` / missing columns / empty DataFrame 做合理保護
- 保持既有 fallback 風格；若不足則補強並加 log

### 6.4 新邏輯可讀、可追蹤、可維護

- 小而清楚的 helper 優先（如 `compute_zscore`、`latest_valid_value`）
- 避免散落重複判斷
- 命名直觀，反映業務語義

---

## 七、修改完成後強制驗證

任何非 trivial 修改，至少補以下四類測試：

| 類型 | 說明 |
|------|------|
| 正常情境 | 標準輸入 → 預期輸出 |
| 缺值情境 | `None` / `NaN` / 空 DataFrame |
| 極端值情境 | z-score 爆炸、std ≈ 0、超大數值 |
| 回歸測試 | 確認既有行為未被破壞 |

每個測試交代：測試名稱、輸入、預期輸出、實際結果。

---

## 八、回報格式（強制）

完成任何非 trivial 修改後，**必須**依以下格式回報：

```
1. 本次修改目標
2. 變更檔案清單
3. Root cause 分析
4. 修正策略
5. 關鍵邏輯說明
6. 相容性影響
7. 測試與驗證結果
8. 自我審查（必答）：
   □ 空資料是否安全？
   □ API / DB / 外部 I/O 失敗是否安全？
   □ 極端值是否安全？
   □ logging 是否足夠定位？
   □ 是否有新增 magic number？
   □ 是否有邏輯沾黏？
   □ 是否有未授權重構？
```

---

## 九、執行態度

你不是來表演寫程式能力的。  
你是來**維護一套真實世界量化系統的穩定性、正確性、可追責性與可維護性**。

> 請先搜尋實際相關檔案與呼叫鏈，再開始修改。  
> 若 source of truth、依賴鏈或影響面尚未確認，先停止修改並回報分析，不要直接動手。

---

## 附錄：專案結構快覽

```
quant_etl/
├── CLAUDE.md              ← 本憲章（工程規範最高文件）
├── SETUP.md               ← 環境建置指南
├── requirements.txt
├── schema_main.sql        ← DB schema（不可任意修改）
│
├── etl/                   ← 資料抓取 + 清洗（ETL 層）
│   ├── cleaner.py         ← 核心清洗工具（MIN_ZSCORE_DENOMINATOR 等常數在此）
│   ├── config.py          ← FRED series、資產設定
│   ├── db.py              ← DB schema + upsert
│   └── fetch_macro.py     ← FRED 資料下載
│
├── indicators/            ← 指標計算層
│   ├── base.py
│   ├── loader.py
│   ├── zscore.py          ← Rolling z-score（ZSCORE_TARGETS、ZSCORE_DISPLAY_ORDER）
│   ├── spread.py
│   └── vix_stats.py
│
├── engine/                ← 狀態判斷層（Scenario / Regime / Signal）
│   ├── regime.py
│   ├── regime_matrix.py   ← Growth × Inflation 矩陣
│   ├── signals.py
│   └── snapshot.py        ← Snapshot dataclass + SnapshotLoader
│
├── monitor/               ← 即時監控 daemon
│   ├── tripwire.py        ← Z-score 警報邏輯（RED/YELLOW/GREEN 閾值常數在此）
│   ├── state_manager.py
│   └── tripwire_line.py   ← Tripwire LINE 訊息
│
├── report/                ← 報告生成 + 傳送層
│   ├── daily_report.py    ← 日報 Markdown（RiskSummary SSOT 在此）
│   ├── line_flex.py       ← LINE Flex Message 組裝
│   ├── send_line.py       ← LINE push sender
│   └── webhook.py
│
├── backtest/              ← 回測層
│   └── strategy.py        ← Positions dataclass
│
├── tests/                 ← 測試
│   └── test_bugfixes.py
│
├── validation/            ← 資料驗證
├── state/                 ← Tripwire 狀態持久化
├── logs/                  ← 執行日誌
└── output/                ← 自動產出的報告檔
```

---

## 附錄：重要常數速查

| 常數 | 值 | 位置 |
|------|----|------|
| `MIN_ZSCORE_DENOMINATOR` | `0.01` | `etl/cleaner.py` |
| `MAX_MONTHLY_STALENESS_DAYS` | `45` | `etl/cleaner.py` |
| `MAX_DAILY_STALENESS_DAYS` | `5` | `etl/cleaner.py` |
| `MAX_INDICATOR_STALENESS_DAYS` | `7` | `etl/cleaner.py` |
| `ZSCORE_WINDOW` | `252` | `indicators/zscore.py`、`monitor/tripwire.py` |
| `RED_THRESHOLD_VIX` | `2.0` | `monitor/tripwire.py` |
| `RED_THRESHOLD_HY` | `2.0` | `monitor/tripwire.py` |
| `YELLOW_THRESHOLD_VIX` | `1.0` | `monitor/tripwire.py` |
| `YELLOW_THRESHOLD_HY` | `1.0` | `monitor/tripwire.py` |
| `SCOUTING_MULT` | `0.50` | `report/daily_report.py` |
| `CORE_WEIGHT` | `0.70` | `report/daily_report.py` |
