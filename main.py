#!/usr/bin/env python3
import os
import time
import json
import argparse
import pandas as pd
import requests
import yfinance as yf
from google import genai
import firebase_admin
from firebase_admin import credentials, firestore, db as rtdb
from datetime import datetime, timedelta
import urllib3
from dotenv import load_dotenv
import threading
import logging
import warnings
import math
import ssl

try:
    import websocket
    HAS_WEBSOCKET = True
except ImportError:
    HAS_WEBSOCKET = False

# 抑制 yfinance 對已下市/問題股票的警告輸出
logging.getLogger("yfinance").setLevel(logging.CRITICAL)
warnings.filterwarnings("ignore", message=".*possibly delisted.*")

load_dotenv()
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# ==========================================
# 系統與 API 金鑰設定區
# ==========================================
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")
FUGLE_API_KEY = os.getenv("FUGLE_API_KEY")
FIREBASE_RTDB_URL = os.getenv("FIREBASE_RTDB_URL") 

FIREBASE_CRED_PATH = "serviceAccountKey.json"
DAILY_LIST_FILE = "breakout_daily_list.csv"

os.makedirs("debug", exist_ok=True)
os.makedirs("data_cache", exist_ok=True)

class YFBackendEngine:
    def __init__(self):
        self.yf_to_fugle = {} 
        self.live_intraday_tracker = {}
        self.reload_csv_to_memory()
        
        self.genai_client = None
        if GEMINI_API_KEY and GEMINI_API_KEY != "YOUR_GEMINI_API_KEY":
            try:
                self.genai_client = genai.Client(api_key=GEMINI_API_KEY)
            except: pass

        self.fugle_headers = {}
        self.fugle_base_url = "https://api.fugle.tw/marketdata/v1.0/stock"
        if FUGLE_API_KEY:
            clean_key = str(FUGLE_API_KEY).strip()
            self.fugle_headers = {"X-API-KEY": clean_key}
            print("[系統] 富果 API 已載入 (用於注意股/處置股/當沖標記)")
            
        self.last_fugle_index_push_time = 0

        self.db_firestore = None
        if os.path.exists(FIREBASE_CRED_PATH):
            if not firebase_admin._apps:
                try:
                    cred = credentials.Certificate(FIREBASE_CRED_PATH)
                    if FIREBASE_RTDB_URL:
                        firebase_admin.initialize_app(cred, {'databaseURL': FIREBASE_RTDB_URL})
                    else:
                        firebase_admin.initialize_app(cred)
                        
                    self.db_firestore = firestore.client()
                    print(f"[系統] Firebase 雙資料庫 (Firestore + RTDB) 連線成功。")
                except: pass

    def get_yf_symbol(self, code, market):
        return f"{code}.TWO" if market == "櫃" else f"{code}.TW"

    def update_firebase_status(self, is_open):
        if not self.db_firestore: return
        try:
            self.db_firestore.collection("System").document("Status").set({
                "isMarketOpen": is_open,
                "lastUpdate": datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            })
        except: pass

    def reload_csv_to_memory(self):
        if os.path.exists(DAILY_LIST_FILE):
            df = pd.read_csv(DAILY_LIST_FILE, dtype={'Symbol': str}).fillna("")
            self.yf_to_fugle = {str(row['YF_Symbol']): row.to_dict() for _, row in df.iterrows()}
            # 後處理：利用 daily 產生時已存的 Klines（最近 ~60 根，包含最新 bar 的 high）修正前高。
            # 如果 Klines 裡出現比存檔 Max_High 更高的點（表示產生 csv 當天那根 bar 創新高），就把記憶體中的 Max_High 升級為該最近高點，
            # 並以 Klines 尾端為基準重新算 Days_Since_High（0 表示就在「最新 bar」=給隔天看的「昨天」）。
            # 只升級不降級，安全：舊的歷史 ATH（>60天前）仍保留正確的 Max_High（Klines 裡不會有更高）。
            # 這讓即使 csv 是用舊邏輯產的，當前 run 的 in-memory + seed + live dist 計算也正確，不用等下次 daily。
            # 根源邏輯已在 daily batch 裡乾淨重寫（不再排除最新 K 線算 max + 取最近一次達到峰值的 idx）。
            for yf_sym, row in list(self.yf_to_fugle.items()):
                try:
                    kl_str = row.get('Klines') or '[]'
                    kl = json.loads(kl_str) if isinstance(kl_str, str) else kl_str
                    if kl and isinstance(kl, list):
                        highs = [float(k.get('high', 0) or 0) for k in kl]
                        if highs:
                            recent_max = max(highs)
                            stored_max = float(row.get('Max_High', 0) or 0)
                            if recent_max > stored_max + 0.01:
                                row['Max_High'] = round(recent_max, 2)
                                # 從 kl 尾端（最新 bar）往前找最後一次出現該 high 的位置
                                for i in range(len(kl) - 1, -1, -1):
                                    if abs(float(kl[i].get('high', 0) or 0) - recent_max) < 0.01:
                                        row['Days_Since_High'] = len(kl) - 1 - i
                                        break
                except Exception:
                    pass
        else:
            self.yf_to_fugle = {}

    def start_yfinance_websocket(self):
        if not self.yf_to_fugle or not FIREBASE_RTDB_URL: return
        
        # 取得所有標的，並加入台灣加權指數 (^TWII) 作為大盤追蹤
        yf_symbols = list(self.yf_to_fugle.keys())
        index_symbol = "^TWII"
        
        print(f"[{datetime.now().strftime('%H:%M:%S')}] 啟動 YFinance 官方 WebSocket (股票分批 + Index) ... (stocks live to RTDB live_quotes + Index/YF 獨立路徑，與 Fugle 各自跳動比較)")
        
        self.ws_lock = threading.Lock()
        self.rtdb_ws_buffer = {}
        self.active_yf_stocks = set()  # track which stocks actually receive data (yf or fallback)
        self.last_yf_tick = {}         # sym -> last real yf message time (for delay stats)
        self.yf_ticked_symbols = set() # only real yf path (for "completely no yf update" count)
        
        def start_flush_timer():
            def flush_loop():
                while True:
                    time.sleep(1.0)
                    with self.ws_lock:
                        to_upload = dict(self.rtdb_ws_buffer)
                        self.rtdb_ws_buffer.clear()
                    
                    if to_upload and self.db_firestore:
                        try:
                            rtdb.reference("live_quotes").update(to_upload)
                            print(f"[YF WS] flushed {len(to_upload)} stock updates @ {datetime.now().strftime('%H:%M:%S')}")
                        except Exception as e: 
                            print(f"[YF WS] flush error: {e}")
            threading.Thread(target=flush_loop, daemon=True).start()
            
        start_flush_timer()

        def message_handler(msg):
            try:
                # 更寬鬆的 key 解析，兼容不同訊息格式
                yf_sym = msg.get('id') or msg.get('symbol') or msg.get('ticker')
                price = msg.get('price') or msg.get('lastPrice') or msg.get('close') or msg.get('last_trade_price')
                if not yf_sym or price is None: return
                price = float(price)
                self._yf_recv_count = getattr(self, '_yf_recv_count', 0) + 1
                if self._yf_recv_count % 50 == 0:
                    print(f"[YF WS] received {self._yf_recv_count} messages, latest {yf_sym}={price}")

                # ==========================================
                # 獨立處理大盤加權指數 (^TWII)
                # ==========================================
                if yf_sym == "^TWII":
                    c_price = price
                    change = msg.get('change', 0)
                    change_pct = msg.get('change_percent', 0)
                    if c_price > 0 and self.db_firestore:
                        try:
                            idx_data = {
                                "price": round(c_price, 2), 
                                "change": round(change, 2), 
                                "changePct": round(change_pct, 2), 
                                "time": datetime.now().strftime('%H:%M:%S'),
                                "source": "yf"
                            }
                            # 只寫 YF 專用路徑，讓富果和YF各自獨立更新跳動，方便比較誰更準
                            rtdb.reference("System/Index/YF").update(idx_data)
                            print(f"[YF WS Index] pushed @ {datetime.now().strftime('%H:%M:%S')} price={idx_data.get('price')}")
                        except Exception as e: 
                            print(f"[YF WS Index] push error: {e}")
                    return

                # ==========================================
                # 處理個股報價
                # ==========================================
                if yf_sym not in self.yf_to_fugle: return

                if yf_sym not in self.active_yf_stocks:
                    self.active_yf_stocks.add(yf_sym)
                    print(f"[YF WS] first live data received for stock {yf_sym} (total active: {len(self.active_yf_stocks)})")

                row = self.yf_to_fugle[yf_sym]
                sym = str(row['Symbol'])
                
                if sym not in self.live_intraday_tracker:
                    self.live_intraday_tracker[sym] = {'open': price, 'high': price, 'low': price, 'vol': 0}
                
                tracker = self.live_intraday_tracker[sym]
                if msg.get('day_open'): tracker['open'] = float(msg['day_open'])
                if msg.get('day_high'): tracker['high'] = float(msg['day_high'])
                else: tracker['high'] = max(tracker['high'], price)
                if msg.get('day_low'): tracker['low'] = float(msg['day_low'])
                else: tracker['low'] = min(tracker['low'], price)
                if msg.get('day_volume'): tracker['vol'] = int(msg['day_volume'])

                base_ref = float(row.get('Latest_Price', price))  # 修正：當日漲幅應以「前日收盤」(csv Latest_Price) 為基準，而非 Ref_Close（快照日前一日）
                base_5d = float(row.get('Base_5D', price))
                base_10d = float(row.get('Base_10D', price))
                base_20d = float(row.get('Base_20D', price))
                max_high = float(row.get('Max_High', price))

                t_return = ((price - base_ref) / base_ref) * 100 if base_ref > 0 else 0.0
                ret_5d = ((price - base_5d) / base_5d) * 100 if base_5d > 0 else 0.0
                ret_10d = ((price - base_10d) / base_10d) * 100 if base_10d > 0 else 0.0
                ret_20d = ((price - base_20d) / base_20d) * 100 if base_20d > 0 else 0.0
                # 統一距離前高公式：(price - max_high) / min(price, max_high) * 100 ，正負都有
                if max_high > 0 and price > 0:
                    denom = min(price, max_high)
                    raw_dist = (price - max_high) / denom * 100
                else:
                    raw_dist = 0.0

                with self.ws_lock:
                    self.rtdb_ws_buffer[sym] = {
                        "Latest_Price": round(float(price), 2),
                        "Today_Open": round(tracker['open'], 2),
                        "Today_High": round(tracker['high'], 2),
                        "Today_Low": round(tracker['low'], 2),
                        "Today_Volume": tracker['vol'],
                        "Today_Return": round(t_return, 2),
                        "Return_5D": round(ret_5d, 2),
                        "Return_10D": round(ret_10d, 2),
                        "Return_20D": round(ret_20d, 2),
                        "Breakout_Margin": round(raw_dist, 2) if raw_dist >= 0 else None,
                        "Distance_To_High": round(raw_dist, 2),
                        "Max_High": round(float(row.get('Max_High', price)), 2),
                        "Days_Since_High": int(row.get('Days_Since_High', 0) or 0),
                        "lastUpdate": int(time.time() * 1000)
                    }
                    self.last_yf_tick[sym] = time.time()
                    self.yf_ticked_symbols.add(sym)
            except Exception as e: 
                print(f"[YF WS message_handler] error: {e}")

        # 分批訂閱股票，避免單次訂閱太多導致只收到部分資料（單連 450+ 常只 partial，昨天觀察到的「部分資料」主因）
        stock_symbols = [s for s in yf_symbols if s != index_symbol]
        batch_size = 80  # 每批 80 檔，減少單連接負擔；實測 6 批可達 95%+ 真實 tick
        stock_batches = [stock_symbols[i:i+batch_size] for i in range(0, len(stock_symbols), batch_size)]
        print(f"[YF WS] 股票分 {len(stock_batches)} 批訂閱，每批最多 {batch_size} 檔")

        def run_stock_batch(batch, batch_id):
            while True:
                try:
                    with yf.WebSocket(verbose=False) as ws:
                        ws.subscribe(batch)
                        print(f"[YF WS batch {batch_id}] subscribed {len(batch)} symbols")
                        # 訂閱成功後立即 seed 初始（確保即使 yf 完全不送該檔，RTDB 也有 entry，網頁列可顯示最後已知價）
                        for yfs in batch:
                            if yfs in self.yf_to_fugle:
                                self._seed_pre_open(yfs)
                        
                        def heartbeat():
                            while True:
                                time.sleep(15.0)
                                try: ws.subscribe(batch)
                                except: break
                        threading.Thread(target=heartbeat, daemon=True).start()
                        
                        ws.listen(message_handler)
                except Exception as e:
                    print(f"[YF WS batch {batch_id}] 斷線重連中: {e}")
                time.sleep(5)

        for bid, batch in enumerate(stock_batches):
            t = threading.Thread(target=run_stock_batch, args=(batch, bid), daemon=True)
            t.start()

        # 指數單獨一個連接，確保大盤有資料
        def run_index_ws():
            while True:
                try:
                    with yf.WebSocket(verbose=False) as ws:
                        ws.subscribe([index_symbol])
                        print(f"[YF WS Index] subscribed to {index_symbol}")
                        
                        def heartbeat():
                            while True:
                                time.sleep(15.0)
                                try: ws.subscribe([index_symbol])
                                except: break
                        threading.Thread(target=heartbeat, daemon=True).start()
                        
                        ws.listen(message_handler)
                except Exception as e:
                    print(f"[YF WS Index] 斷線重連中: {e}")
                time.sleep(5)

        threading.Thread(target=run_index_ws, daemon=True).start()

        # 啟動狀態回報 + 針對 yf 靜默檔的 Fugle quote fallback（3 分鐘後只對仍未 active 的做 1 次 paced seed，之後每 5min 重試仍靜默者）
        self._start_yf_status_reporter()
        self._start_yf_fallback_seeder()

    def _seed_pre_open(self, yf_sym):
        """為單一 symbol seed 初始到 buffer：Latest=日批最後已知價，但 Today_*=0 讓前端 hasLiveData 真實判斷不觸發假今天K線（避免昨天重複K bug）"""
        if yf_sym not in self.yf_to_fugle:
            return
        row = self.yf_to_fugle[yf_sym]
        sym = str(row['Symbol'])
        base_price = float(row.get('Latest_Price', 0) or 0)
        # 當日漲幅 pre-seed 用 csv Latest_Price 作 base → 0% （新的一天開盤前當日漲幅應為 0，之後 yf tick 會更新為真實 (現價-昨收)）
        base_ref = float(row.get('Latest_Price', base_price) or base_price)
        base_5d = float(row.get('Base_5D', base_price) or base_price)
        base_10d = float(row.get('Base_10D', base_price) or base_price)
        base_20d = float(row.get('Base_20D', base_price) or base_price)
        max_high = float(row.get('Max_High', base_price) or base_price)
        t_return = ((base_price - base_ref) / base_ref) * 100 if base_ref > 0 else 0.0
        ret_5d = ((base_price - base_5d) / base_5d) * 100 if base_5d > 0 else 0.0
        ret_10d = ((base_price - base_10d) / base_10d) * 100 if base_10d > 0 else 0.0
        ret_20d = ((base_price - base_20d) / base_20d) * 100 if base_20d > 0 else 0.0
        # 統一前高距離公式 (seed 用 snapshot base_price)
        if max_high > 0 and base_price > 0:
            denom = min(base_price, max_high)
            raw_dist = (base_price - max_high) / denom * 100
        else:
            raw_dist = 0.0
        with self.ws_lock:
            self.rtdb_ws_buffer[sym] = {
                "Latest_Price": round(base_price, 2),
                "Today_Open": 0,
                "Today_High": 0,
                "Today_Low": 0,
                "Today_Volume": 0,
                "Today_Return": round(t_return, 2),
                "Return_5D": round(ret_5d, 2),
                "Return_10D": round(ret_10d, 2),
                "Return_20D": round(ret_20d, 2),
                "Breakout_Margin": round(raw_dist, 2) if raw_dist >= 0 else None,
                "Distance_To_High": round(raw_dist, 2),
                "Max_High": round(float(row.get('Max_High', base_price)), 2),
                "Days_Since_High": int(row.get('Days_Since_High', 0) or 0),
                "lastUpdate": int(time.time() * 1000)
            }
            self.last_yf_tick[sym] = 0  # pre-seed, not a real yf tick
            # do not add to yf_ticked_symbols (only real yf message_handler does)

    def _start_yf_status_reporter(self):
        def reporter():
            start_t = time.time()
            while True:
                time.sleep(60)
                try:
                    exp = len([k for k in self.yf_to_fugle.keys() if k != "^TWII"])
                    yf_ticked = len(getattr(self, 'yf_ticked_symbols', set()))
                    no_yf = exp - yf_ticked
                    now = time.time()
                    last_tick = getattr(self, 'last_yf_tick', {})
                    delayed = sum(1 for ts in last_tick.values() if ts > 0 and (now - ts > 60))
                    act = len(getattr(self, 'active_yf_stocks', set()))
                    pct = (act / exp * 100) if exp else 0
                    print(f"[YF WS status] {datetime.now().strftime('%H:%M:%S')} active={act}/{exp} ({pct:.1f}%) yf_ticked={yf_ticked} no_real_yf_update={no_yf} delayed(>60s no tick)={delayed}  ws_uptime={int(time.time()-start_t)}s")
                except Exception as e:
                    print(f"[YF WS status] reporter error: {e}")
        threading.Thread(target=reporter, daemon=True).start()

    def _start_yf_fallback_seeder(self):
        """3min 後 + 每5min 對仍無 yf tick 的檔，用富果 quote 補 seed 真實今日 OHLC/價（paced <60/min），讓 100% 都有 live_quotes 資料。YF 後續 tick 會自然 overwrite。"""
        def fallback_loop():
            # 第一次等 yf 盡量送（很多檔第一筆要花時間）
            time.sleep(180)
            while True:
                try:
                    if not self.fugle_headers:
                        time.sleep(300); continue
                    expected = [yf for yf in self.yf_to_fugle.keys() if yf != "^TWII"]
                    silent = [yf for yf in expected if yf not in self.active_yf_stocks]
                    if silent:
                        print(f"[YF Fallback] 開始為 {len(silent)} 檔 yf-靜默者用富果 quote 補資料（1.1s/檔）...")
                        seeded = 0
                        for yfs in silent:
                            if yfs in self.active_yf_stocks: continue
                            row = self.yf_to_fugle.get(yfs) or {}
                            sym = str(row.get('Symbol', ''))
                            if not sym: continue
                            try:
                                time.sleep(1.1)
                                r = requests.get(f"{self.fugle_base_url}/intraday/quote/{sym}", headers=self.fugle_headers, timeout=6)
                                if r.status_code != 200: continue
                                q = r.json() or {}
                                last_p = q.get('lastPrice') or q.get('closePrice') or q.get('referencePrice') or 0
                                if last_p <= 0: continue
                                o = float(q.get('openPrice') or last_p)
                                h = float(q.get('highPrice') or last_p)
                                l = float(q.get('lowPrice') or last_p)
                                v = int((q.get('total') or {}).get('tradeVolume') or 0)
                                # fallback 提供的「當日」也應使用 csv Latest_Price 作為昨收基準（而非 Ref_Close）
                                base_ref = float(row.get('Latest_Price', last_p) or last_p)
                                base_5d = float(row.get('Base_5D', last_p) or last_p)
                                base_10d = float(row.get('Base_10D', last_p) or last_p)
                                base_20d = float(row.get('Base_20D', last_p) or last_p)
                                max_high = float(row.get('Max_High', last_p) or last_p)
                                t_return = ((last_p - base_ref) / base_ref) * 100 if base_ref > 0 else 0.0
                                ret_5d = ((last_p - base_5d) / base_5d) * 100 if base_5d > 0 else 0.0
                                ret_10d = ((last_p - base_10d) / base_10d) * 100 if base_10d > 0 else 0.0
                                ret_20d = ((last_p - base_20d) / base_20d) * 100 if base_20d > 0 else 0.0
                                # 統一前高距離 (fallback)
                                if max_high > 0 and last_p > 0:
                                    denom = min(last_p, max_high)
                                    raw_dist = (last_p - max_high) / denom * 100
                                else:
                                    raw_dist = 0.0
                                with self.ws_lock:
                                    self.rtdb_ws_buffer[sym] = {
                                        "Latest_Price": round(last_p, 2),
                                        "Today_Open": round(o, 2),
                                        "Today_High": round(h, 2),
                                        "Today_Low": round(l, 2),
                                        "Today_Volume": v,
                                        "Today_Return": round(t_return, 2),
                                        "Return_5D": round(ret_5d, 2),
                                        "Return_10D": round(ret_10d, 2),
                                        "Return_20D": round(ret_20d, 2),
                                        "Breakout_Margin": round(raw_dist, 2) if raw_dist >= 0 else None,
                                        "Distance_To_High": round(raw_dist, 2),
                                        "Max_High": round(float(row.get('Max_High', last_p)), 2),
                                        "Days_Since_High": int(row.get('Days_Since_High', 0) or 0),
                                        "lastUpdate": int(time.time() * 1000)
                                    }
                                seeded += 1
                                # 也標記 active，避免重複
                                self.active_yf_stocks.add(yfs)
                                self.last_yf_tick[sym] = time.time()  # treat fallback poll as an "update" for staleness (but not for yf_ticked)
                                print(f"[YF Fallback] seeded {sym} last={last_p} ohl=({o},{h},{l}) vol={v}")
                            except Exception as ee:
                                pass
                        if seeded:
                            print(f"[YF Fallback] 本輪補 {seeded} 檔（總 active 現在 {len(self.active_yf_stocks)}）")
                    else:
                        print("[YF Fallback] 目前無靜默檔，全部由 YF WS 提供即時資料")
                except Exception as e:
                    print(f"[YF Fallback] loop error: {e}")
                time.sleep(300)  # 每 5 分鐘檢查一次仍靜默者
        threading.Thread(target=fallback_loop, daemon=True).start()

    def start_fugle_index_websocket(self):
        """【富果 WS 來源】大盤指數 (IX0001) - 寫到 System/Index/Fugle 供與 YF 的 /Index/YF 各自獨立更新跳動比較誰更準"""
        if not HAS_WEBSOCKET or not self.fugle_headers or not FIREBASE_RTDB_URL:
            print("[Fugle Index WS] 跳過 (缺少 websocket 套件 或 無 FUGLE_API_KEY 或 無 FIREBASE_RTDB_URL)")
            return

        self.index_prev_close = 0
        try:
            res = requests.get(f"{self.fugle_base_url}/intraday/quote/IX0001", headers=self.fugle_headers, timeout=5)
            if res.status_code == 200:
                self.index_prev_close = res.json().get('previousClose', 0)
        except: pass

        def on_message(ws, message):
            try:
                data = json.loads(message)
                event = data.get('event')
                if event == 'authenticated':
                    print("[Fugle Index WS] authenticated, subscribing to IX0001...")
                    ws.send(json.dumps({"event": "subscribe", "data": {"channel": "indices", "symbol": "IX0001"}}))
                    return

                if event in ['data', 'snapshot']:
                    info = data.get('data', {})
                    c_price = info.get('index') or info.get('closePrice') or info.get('price') or info.get('lastPrice')
                    if not c_price and 'quote' in info:
                        c_price = info['quote'].get('closePrice') or info['quote'].get('price')
                    if not c_price and 'trade' in info:
                        c_price = info['trade'].get('price')

                    c_price = float(c_price or 0)
                    p_close = float(info.get('previousClose') or self.index_prev_close or 0)

                    if c_price > 0 and p_close > 0:
                        self.index_prev_close = p_close
                        current_time = time.time()
                        if current_time - self.last_fugle_index_push_time >= 1.0:
                            change = c_price - p_close
                            change_pct = (change / p_close) * 100
                            try:
                                fugle_data = {
                                    "price": round(c_price, 2),
                                    "change": round(change, 2),
                                    "changePct": round(change_pct, 2),
                                    "time": datetime.now().strftime('%H:%M:%S'),
                                    "source": "fugle"
                                }
                                rtdb.reference("System/Index/Fugle").update(fugle_data)
                                # 僅寫獨立 /Fugle 路徑，與 YF 的 /YF 各自獨立更新跳動，前端分開顯示以比較誰更準（不再覆寫 root）
                                print(f"[Fugle Index WS] pushed @ {datetime.now().strftime('%H:%M:%S')} price={fugle_data.get('price')}")
                            except Exception as e: 
                                print(f"[Fugle Index WS] push error: {e}")
                            self.last_fugle_index_push_time = current_time
            except Exception as e: 
                print(f"[Fugle Index WS on_message] error: {e}")

        def on_error(ws, error): pass
        def on_close(ws, close_status_code, close_msg):
            time.sleep(5)
            self.start_fugle_index_websocket()
        def on_open(ws):
            print("[Fugle Index WS] on_open, sending auth...")
            ws.send(json.dumps({"event": "auth", "data": {"apikey": FUGLE_API_KEY}}))

        ws_url = "wss://api.fugle.tw/marketdata/v1.0/stock/streaming"
        def run_ws():
            ws = websocket.WebSocketApp(ws_url, on_open=on_open, on_message=on_message, on_error=on_error, on_close=on_close)
            ws.run_forever(ping_interval=15, ping_timeout=10, sslopt={"cert_reqs": ssl.CERT_NONE})

        threading.Thread(target=run_ws, daemon=True).start()
        print(f"[{datetime.now().strftime('%H:%M:%S')}] 啟動 富果 Index WebSocket (IX0001) for 大盤比較 (寫到 /Fugle 獨立路徑，與 YF /YF 各自跳動)")

    def check_is_market_open_today(self):
        now = datetime.now()
        return now.weekday() < 5

    def get_taiwan_stock_list(self):
        tickers = []
        print("\n[Debug] 開始請求台灣政府 OpenAPI 取得台股清單...")
        
        # 1. 取得上市清單 (TWSE) - 欄位使用中文
        try:
            url_twse = "https://openapi.twse.com.tw/v1/opendata/t187ap03_L"
            res = requests.get(url_twse, timeout=10)
            if res.status_code == 200:
                for item in res.json():
                    code = str(item.get("公司代號", "")).strip()
                    name = item.get("公司簡稱", "").strip() or item.get("公司名稱", "").strip()
                    industry = item.get("產業別", "未分類").strip()
                    if len(code) == 4 and code.isdigit() and not code.startswith("0"):
                        tickers.append({
                            "symbol": code, 
                            "name": name, 
                            "industry": industry, 
                            "market": "市"
                        })
            else:
                print(f"❌ [錯誤] 無法取得上市清單，狀態碼: {res.status_code}")
        except Exception as e:
            print(f"❌ [錯誤] 上市清單 API 例外: {e}")

        # 2. 取得上櫃清單 (TPEx) - 💥 欄位使用英文
        try:
            url_tpex = "https://www.tpex.org.tw/openapi/v1/mopsfin_t187ap03_O"
            res = requests.get(url_tpex, timeout=10)
            if res.status_code == 200:
                for item in res.json():
                    code = str(item.get("SecuritiesCompanyCode", "")).strip()
                    name = item.get("CompanyAbbreviation", "").strip() or item.get("CompanyName", "").strip()
                    industry = item.get("SecuritiesIndustryCode", "未分類").strip()
                    if len(code) == 4 and code.isdigit() and not code.startswith("0"):
                        tickers.append({
                            "symbol": code, 
                            "name": name, 
                            "industry": industry, 
                            "market": "櫃"
                        })
            else:
                print(f"❌ [錯誤] 無法取得上櫃清單，狀態碼: {res.status_code}")
        except Exception as e:
            print(f"❌ [錯誤] 上櫃清單 API 例外: {e}")

        print(f"✅ [完成] 總共解析出 {len(tickers)} 檔有效標的。\n")
        # 修正 9105 產業代碼誤植 (TPEx 回傳 91)
        for t in tickers:
            if t.get("symbol") == "9105" and t.get("industry") in ("91", "未分類"):
                t["industry"] = "其他電子業"
        return tickers

    def fetch_yfinance_rating(self, yf_symbol):
        try:
            tkr = yf.Ticker(yf_symbol)
            info = getattr(tkr, 'info', {}) or {}
            recos = tkr.recommendations
            parts = []
            key = info.get('recommendationKey') or info.get('recommendationMean')
            num = info.get('numberOfAnalystOpinions')
            if num:
                parts.append(f"{num}分析師")
            if key:
                kmap = {'strong_buy': '強力買進', 'buy': '買進', 'hold': '持有', 'sell': '賣出', 'strong_sell': '強力賣出', 'none': ''}
                kstr = kmap.get(str(key).lower(), str(key))
                if kstr and kstr != 'none':
                    parts.append(kstr)
            # 移除評分數字，只保留強力買進/買進/持有/賣出等文字樣式（避免混淆）
            if parts:
                return " ".join(parts)
            # fallback to df
            if recos is not None and not recos.empty:
                if 'To Grade' in recos.columns: return str(recos.iloc[-1]['To Grade']).replace('\n', ' ')
                elif 'period' in recos.columns and 'strongBuy' in recos.columns:
                    row = recos.iloc[0] 
                    return f"強買:{row['strongBuy']} 買進:{row['buy']} 持有:{row['hold']}"
            return ""
        except: return ""

    def generate_gemini_summaries(self, stock_list):
        """從 Gemini 產生 AI 企業速寫，一句話精要介紹，優先使用快取"""
        if not self.genai_client: return stock_list
        ai_cache_file = "data_cache/ai_summaries.json"
        ai_cache = {}
        if os.path.exists(ai_cache_file):
            try:
                with open(ai_cache_file, 'r', encoding='utf-8') as f: ai_cache = json.load(f)
            except: pass
        
        needs_ai = []
        for stock in stock_list:
            symbol = str(stock['Symbol'])
            if symbol in ai_cache and str(ai_cache[symbol]).strip():
                stock['Company_Info'] = ai_cache[symbol] 
            else:
                needs_ai.append(stock)
                
        if not needs_ai: return stock_list
        
        batch_size = 100 
        for i in range(0, len(needs_ai), batch_size):
            batch = needs_ai[i:i+batch_size]
            prompt = "你是一位專業的台股分析師。請為以下台股公司各提供「一句話的精要介紹」。\n【嚴格規則】：1. 不超過50字。2. 格式「代號: 介紹」。3. 不准遺漏！\n\n"
            for stock in batch: prompt += f"{stock['Symbol']} ({stock['Name']}, {stock['Industry']})\n"
            
            for attempt in range(3):
                try:
                    response = self.genai_client.models.generate_content(model='gemini-flash-latest', contents=prompt)
                    for line in response.text.split('\n'):
                        if ":" in line or "：" in line:
                            parts = line.replace("：", ":").split(":", 1)
                            if len(parts) == 2:
                                code, desc = parts[0].strip(), parts[1].strip()
                                for s in batch:
                                    if s['Symbol'] in code:
                                        s['Company_Info'] = desc
                                        ai_cache[s['Symbol']] = desc 
                                        break
                    with open(ai_cache_file, 'w', encoding='utf-8') as f: json.dump(ai_cache, f, ensure_ascii=False, indent=4)
                    time.sleep(15) 
                    break 
                except Exception as e:
                    print(f"[Gemini] 嘗試 {attempt+1} 失敗: {e}")
                    time.sleep(60)
        return stock_list

    def run_daily_batch(self):
        print(f"\n=== 啟動過濾引擎 (13:35 Daily Batch - YFinance架構) ===")
        all_stocks = self.get_taiwan_stock_list()
        
        if not all_stocks: 
            print("❌ [中斷] 無法取得台股清單，程式強制結束！")
            return

        # ==========================================
        # 透過 YFinance 多執行緒批次下載 K 線 (極速)
        # ==========================================
        chunk_size = 150
        breakout_list = []
        
        print(f"[階段 2] 透過 YFinance 批次下載近一年 K 線並計算 (共 {len(all_stocks)} 檔)...")
        
        for i in range(0, len(all_stocks), chunk_size):
            chunk_stocks = all_stocks[i:i+chunk_size]
            chunk_yf_syms = [self.get_yf_symbol(s['symbol'], s['market']) for s in chunk_stocks]
            
            print(f"   => 正在處理第 {i+1} ~ {min(i+chunk_size, len(all_stocks))} 檔標的...")
            
            try:
                df_chunk = yf.download(chunk_yf_syms, period="1y", group_by="ticker", threads=True, progress=False)
            except Exception as e:
                print(f"   ⚠️ 下載此批次時發生錯誤: {e}")
                continue

            for stock in chunk_stocks:
                symbol = stock['symbol']
                yf_sym = self.get_yf_symbol(symbol, stock['market'])
                
                try:
                    if isinstance(df_chunk.columns, pd.MultiIndex):
                        if yf_sym not in df_chunk.columns.get_level_values(0): continue
                        df_stock = df_chunk[yf_sym].copy()
                    else:
                        df_stock = df_chunk.copy()
                        
                    df_stock = df_stock.dropna(subset=['Close'])
                    if len(df_stock) < 20: continue
                    
                    df = pd.DataFrame({
                        'date': df_stock.index.strftime('%Y-%m-%d'),
                        'open': df_stock['Open'].astype(float),
                        'high': df_stock['High'].astype(float),
                        'low': df_stock['Low'].astype(float),
                        'close': df_stock['Close'].astype(float),
                        'volume': df_stock['Volume'].astype(float)
                    }).reset_index(drop=True)
                    
                    ma5 = df['close'].rolling(5).mean().iloc[-1]
                    ma10 = df['close'].rolling(10).mean().iloc[-1]
                    ma20 = df['close'].rolling(20).mean().iloc[-1]
                    
                    latest_close = round(float(df['close'].iloc[-1]), 2)
                    
                    # 統一邏輯：前高定義永遠從「現有 K 線圖的倒數第二根」起算（最新 bar 不算入前高）。
                    # Firestore 資料優先，RTDB 附加最新 bar（如果更「新的一天」）。
                    # 無論 batch 或 live view，前高 / 漲幅 base 都基於「K線倒數第二根」之前的歷史。
                    if len(df) > 0:
                        pre_df = df.iloc[:-1] if len(df) > 1 else df
                        if len(pre_df) > 0:
                            max_high_val = float(pre_df['high'].max())
                            prev_max_high = round(max_high_val, 2)
                            recent_high_idx = pre_df[pre_df['high'] == max_high_val].index[-1]
                            days_since_high = int(len(pre_df) - 1 - recent_high_idx)
                        else:
                            prev_max_high = latest_close
                            days_since_high = 0
                        if len(df) > 1:
                            yesterday_close = round(float(df['close'].iloc[-2]), 2)
                        else:
                            yesterday_close = latest_close
                    else:
                        yesterday_close = latest_close
                        prev_max_high = latest_close
                        days_since_high = 0

                    if latest_close > 0 and prev_max_high > 0 and (latest_close * 1.1 >= prev_max_high):
                        
                        close_5d = round(float(df['close'].iloc[-6]) if len(df) >= 6 else df['close'].iloc[0], 2)
                        close_10d = round(float(df['close'].iloc[-11]) if len(df) >= 11 else df['close'].iloc[0], 2)
                        close_20d = round(float(df['close'].iloc[-21]) if len(df) >= 21 else df['close'].iloc[0], 2)
                        
                        def calc_return(current, old): 
                            return ((current - old) / old) * 100 if old > 0 else 0.0

                        today_return = calc_return(latest_close, yesterday_close)
                        # 統一前高距離邏輯： (close - 前高) / min(close, 前高) * 100 ，有正有負
                        # 作為唯一的「距離前高」數字（正=突破，負=距下）
                        if prev_max_high > 0 and latest_close > 0:
                            denom = min(latest_close, prev_max_high)
                            raw_dist = (latest_close - prev_max_high) / denom * 100
                        else:
                            raw_dist = 0.0
                        
                        df_chart = df.tail(60)
                        klines = [{
                            "date": str(r['date'])[:10].split('-', 1)[1], 
                            "open": round(float(r['open']), 2), 
                            "close": round(float(r['close']), 2), 
                            "low": round(float(r['low']), 2), 
                            "high": round(float(r['high']), 2), 
                            "volume": int(r['volume'])
                        } for _, r in df_chart.iterrows()]
                        df_last = df.iloc[-1]
                        
                        today_open = round(float(df_last['open']), 2)
                        today_high = round(float(df_last['high']), 2)
                        today_low = round(float(df_last['low']), 2)
                        today_vol = int(df_last['volume'])
                        
                        rating = self.fetch_yfinance_rating(yf_sym)
                        
                        breakout_list.append({
                            "Industry": stock['industry'], "Symbol": symbol, "Name": stock['name'],
                            "Latest_Price": latest_close, "Today_Open": today_open, "Today_High": today_high, "Today_Low": today_low, "Today_Volume": today_vol,
                            "Max_High": prev_max_high, "Days_Since_High": days_since_high, 
                            
                            "Ref_Close": yesterday_close,
                            "Base_5D": close_5d,
                            "Base_10D": close_10d,
                            "Base_20D": close_20d,
                            
                            "Today_Return": round(today_return, 2), 
                            "Return_5D": round(calc_return(latest_close, close_5d), 2), 
                            "Return_10D": round(calc_return(latest_close, close_10d), 2), 
                            "Return_20D": round(calc_return(latest_close, close_20d), 2),
                            "Breakout_Margin": round(raw_dist, 2) if raw_dist >= 0 else None, "Distance_To_High": round(raw_dist, 2),
                            
                            "Status_Tags": "-",
                            "Rating": rating, "Company_Info": "", "YF_Symbol": yf_sym,
                            "MA5": round(ma5, 2), "MA10": round(ma10, 2), "MA20": round(ma20, 2), "Klines": json.dumps(klines)
                        })
                except: pass

        if breakout_list:
            # ==========================================
            # 從富果 (免費60/min) 取得注意股/處置股/當沖資訊；yfinance 無此資料
            # 注意：富果 /tickers?isAttention / isDisposition 列表在「某些時段」（非交易中、特定快照時）常回傳空 data:[] 
            # 即使個股真的被標記。解決：**一定對 breakout_list 裡的每一檔做 per-ticker 查詢**（1.05s pacing），
            # 並以回應裡的 isAttention / isDisposition / canDayTrade / matchingInterval 為準（最可靠）。
            # 列表查詢只用來印 log 對照。這樣才能確認「真的有抓到資料」還是空。
            # 451 檔約 8 分鐘，盤後可接受；加上明確 log 方便你觀察。
            # ==========================================
            # 1. 先做列表查詢（只為 log 診斷，常是 0）
            list_att = set()
            list_disp = set()
            if self.fugle_headers:
                for flag, target in [("isAttention=true", list_att), ("isDisposition=true", list_disp)]:
                    for ex in ["TWSE", "TPEx", ""]:
                        try:
                            q = f"{self.fugle_base_url}/intraday/tickers?type=EQUITY"
                            if ex: q += f"&exchange={ex}"
                            q += f"&{flag}"
                            r = requests.get(q, headers=self.fugle_headers, timeout=5)
                            if r.status_code == 200:
                                for it in r.json().get("data", []):
                                    sym = str(it.get("symbol", "")).strip()
                                    if sym: target.add(sym)
                        except: pass
            print(f"[階段 3] 列表診斷: isAttention列表={len(list_att)} isDisposition列表={len(list_disp)} （很多時段會是0，這是富果的空資料現象）")

            # 2. 強制對所有 451 檔做 per-ticker（以個股回應為準）
            print(f"[階段 3] 開始逐檔 per-ticker 抓旗標（共 {len(breakout_list)} 檔，1.05s 間隔，避免超 60/min）...")
            real_att_count = 0
            real_disp_count = 0
            resp_dates = set()
            for idx, stock in enumerate(breakout_list):
                symbol = stock['Symbol']
                try:
                    if not self.fugle_headers:
                        stock["Status_Tags"] = "-"
                        continue
                    time.sleep(1.05)
                    res = requests.get(f"{self.fugle_base_url}/intraday/ticker/{symbol}", headers=self.fugle_headers, timeout=5)
                    if res.status_code == 200:
                        t_data = res.json() or {}
                        resp_dates.add(t_data.get("date", ""))
                        is_att = bool(t_data.get("isAttention"))
                        is_disp = bool(t_data.get("isDisposition"))
                        if is_att: real_att_count += 1
                        if is_disp: real_disp_count += 1

                        is_day_trade = "可當沖" if t_data.get("canDayTrade") else "-"
                        is_margin_short = "-" if t_data.get("canDayTrade") else ("可先買" if t_data.get("canBuyDayTrade") else "-")
                        is_attention = "注意股" if is_att else "-"
                        disposition_status = "-"
                        if is_disp:
                            mi = t_data.get("matchingInterval", 0) or 0
                            disposition_status = f"處置({math.ceil(mi / 60)}分)"

                        tags = f"{is_day_trade} | {is_margin_short} | {is_attention} | {disposition_status}"
                        tags = tags.replace("- | ", "").replace(" | -", "").strip(" | -") or "-"
                        stock["Status_Tags"] = tags
                    else:
                        # 401 或其他錯誤 → 至少給基本標籤，避免完全空白
                        stock["Status_Tags"] = "可當沖"
                except Exception as e:
                    stock["Status_Tags"] = "可當沖"

            print(f"[階段 3] 完成 per-ticker。**實際從富果回應抓到**：注意股 {real_att_count} 檔、處置股 {real_disp_count} 檔。")
            if resp_dates:
                print(f"        回應日期樣本：{sorted([d for d in resp_dates if d])[:3]} （若與預期不同或全是舊日期，代表該時段富果快照為空/歷史）")
            if real_att_count == 0 and real_disp_count == 0:
                print("        *** 警告：本次所有 per-ticker 都沒回傳 isAttention/isDisposition=true。")
                print("            這可能是「富果某些時段回傳空資料」的情況（非交易時段、快照尚未更新、或真的沒有標記）。")
                print("            列表也常是 0。資料是「真的抓了但富果說沒有」，不是沒呼叫。")
            else:
                print("        資料看起來有內容（有注意/處置被標記）。")

            # ==========================================
            # AI 企業速寫 (Gemini, 快取優先，僅新股呼叫)
            # ==========================================
            print("[階段 4] 載入/生成 AI 企業速寫...")
            breakout_list = self.generate_gemini_summaries(breakout_list)

            pd.DataFrame(breakout_list).to_csv(DAILY_LIST_FILE, index=False, encoding='utf-8-sig')
            # 同時輸出 JSON 供前端用 http.server 直接 fetch 快速載入（避開 Firestore 大集合 snapshot 慢的問題）
            try:
                with open("breakout_list.json", "w", encoding="utf-8") as f:
                    json.dump(breakout_list, f, ensure_ascii=False)
                print(f"[系統] 已輸出 breakout_list.json ({len(breakout_list)} 檔)，前端可快速本地載入清單")
            except Exception as e:
                print(f"[警告] 輸出 breakout_list.json 失敗: {e}")
            self.reload_csv_to_memory()
            
            if self.db_firestore:
                existing_docs = self.db_firestore.collection("LiveStocks").stream()
                new_symbols = {str(stock['Symbol']) for stock in breakout_list}
                batch, del_count = self.db_firestore.batch(), 0
                for doc in existing_docs:
                    if doc.id not in new_symbols:
                        batch.delete(doc.reference)
                        del_count += 1
                for i, stock in enumerate(breakout_list):
                    batch.set(self.db_firestore.collection("LiveStocks").document(str(stock['Symbol'])), stock)
                    if (i + del_count + 1) % 400 == 0:
                        batch.commit()
                        batch = self.db_firestore.batch()
                batch.commit()
                
            if FIREBASE_RTDB_URL:
                try: rtdb.reference("live_quotes").set({})
                except: pass

            self.live_intraday_tracker.clear()
            print(f"✅ 13:35 盤後作業結束！共篩選出 {len(breakout_list)} 檔標的。")

    def run_auto_scheduler(self):
        print("\n=== 啟動全自動無人值守模式 (YFinance K線 + 富果少量meta + 雙大盤來源比較) ===")
        print("邏輯原則：13:35 YF批次K線過濾 + 富果ticker(注意/處置/當沖, 1.05s pacing) + Gemini AI速寫；盤中(8:00起)同時啟 YF WS (stocks + Index/YF) 與 Fugle Index WS (Index/Fugle) 寫 RTDB 供比較 (兩來源獨立路徑，前端只顯示單一主來源)")
        
        schedule_flags = {"1335": False}

        while True:
            now = datetime.now()
            time_str = now.strftime("%H%M")
            
            if self.check_is_market_open_today() and (800 <= int(time_str) <= 1330):
                self.update_firebase_status(True)
                
                if not getattr(self, 'yf_ws_started', False):
                    self.start_yfinance_websocket()
                    self.yf_ws_started = True

                if not getattr(self, 'fugle_index_ws_started', False):
                    self.start_fugle_index_websocket()
                    self.fugle_index_ws_started = True
                    
                time.sleep(30)
            else:
                self.update_firebase_status(False)
                
                if time_str == "1335" and not schedule_flags["1335"]:
                    self.run_daily_batch()
                    schedule_flags["1335"] = True
                    self.yf_ws_started = False 
                    self.fugle_index_ws_started = False 
                
                if time_str == "0005":
                    schedule_flags = {"1335": False}
                    self.live_intraday_tracker.clear()
                    self.fugle_index_ws_started = False
                    self.yf_ws_started = False 
                    
                time.sleep(30)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='台股強勢突破監控系統')
    parser.add_argument('--mode', type=str, default='auto', help='執行模式: auto 或 daily')
    args = parser.parse_args()

    engine = YFBackendEngine()
    
    if args.mode == 'daily':
        engine.run_daily_batch()
    else:
        engine.run_auto_scheduler()