# 台股強勢突破追蹤系統

自動化台股突破股雷達，結合公開資料 + yfinance 即時串流 + Firebase 即時同步 + 簡易網頁儀表板。

## 主要功能

- 每日 13:35 盤後使用 TWSE / TPEx 公開 API + yfinance 批次計算近一年 K 線，篩選「接近前高」的強勢突破候選股。
- 盤中自動啟動 yfinance WebSocket 即時推播報價到 Firebase (RTDB)。
- 網頁儀表板 (index.html) 直接從 Firebase 即時顯示大盤與個股狀態。
- 完全不依賴付費 Fugle 即時 API（目前版本使用公開資料源）。

## 快速開始

### 1. 安裝依賴

```bash
python3 -m pip install -r requirements.txt
```

### 2. 設定環境變數

```bash
cp .env.example .env
# 編輯 .env 填入你的 Gemini API Key（可選，AI 公司簡介用）
# 以及 FIREBASE_RTDB_URL
```

下載 Firebase Admin SDK 金鑰：
- 去 Firebase Console → 專案設定 → 服務帳戶 → 產生新的私密金鑰
- 儲存為 `serviceAccountKey.json` 放在專案根目錄

### 3. 執行

```bash
# 強制執行一次盤後批次（產生 breakout_daily_list.csv 並同步 Firestore）
python3 main.py --mode daily

# 常駐模式（自動在交易時間啟動 WebSocket 即時推播）
python3 main.py --mode auto
```

### 4. 查看儀表板

```bash
python3 -m http.server 8000
# 瀏覽器開啟 http://localhost:8000/index.html
```

儀表板會即時從你的 Firebase 讀取資料（公開讀取，無需登入）。

## 專案結構

```
.
├── main.py                 # 主要引擎（YFinance 版）
├── requirements.txt
├── index.html              # Vue + ECharts 即時儀表板
├── .env.example
├── README.md
├── breakout_daily_list.csv # 最新突破清單（預設 gitignore，可手動 force add）
├── data_cache/             # 快取（已 gitignore）
├── debug/                  # 除錯輸出（已 gitignore）
└── serviceAccountKey.json  # Firebase 金鑰（絕對不要 commit！）
```

## Git 設定建議

本專案已設定 `.gitignore`，會排除：
- 敏感金鑰 (`.env`, `serviceAccountKey.json`)
- 大型產生資料 (`breakout_daily_list.csv`, `data_cache/`, `debug/`)

如果你想把某一次的突破清單版本控制起來：

```bash
git add -f breakout_daily_list.csv
git commit -m "chore: snapshot of signals on 2026-06-03"
```

## 注意事項

- yfinance 偶爾會出現 "possibly delisted" 訊息（已盡量抑制），是正常現象，代表該股票目前無交易資料。
- Firebase 寫入需要正確的 `serviceAccountKey.json`。
- 公開 API 抓取有頻率限制，批次下載已做 chunk 處理 + sleep。
- 本系統目前設計為「每日產生一次清單 + 盤中即時報價」，適合當作選股雷達使用。

## 貢獻 / 後續擴充想法

- 加入更多技術指標過濾
- 整合更多資料源（例如公開的法人買賣超）
- 加上 Telegram / Line 通知
- 使用 GitHub Actions 定時跑 daily 並 commit 結果

---

**目前狀態**：已移除舊的 Fugle 依賴版本，改用公開資料源，穩定性較高。
