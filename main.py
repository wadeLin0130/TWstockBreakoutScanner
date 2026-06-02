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

# 抑制 yfinance 對已下市/問題股票的警告輸出
logging.getLogger("yfinance").setLevel(logging.CRITICAL)
warnings.filterwarnings("ignore", message=".*possibly delisted.*")

load_dotenv()
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# ==========================================
# 系統與 API 金鑰設定區
# ==========================================
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")
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
        else:
            self.yf_to_fugle = {}

    def start_yfinance_websocket(self):
        if not self.yf_to_fugle or not FIREBASE_RTDB_URL: return
        
        # 取得所有標的，並加入台灣加權指數 (^TWII) 作為大盤追蹤
        yf_symbols = list(self.yf_to_fugle.keys())
        ws_symbols = yf_symbols + ["^TWII"]
        
        print(f"[{datetime.now().strftime('%H:%M:%S')}] 啟動 YFinance 官方 WebSocket ({len(ws_symbols)} 檔)...")
        
        self.ws_lock = threading.Lock()
        self.rtdb_ws_buffer = {}
        
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
                        except: pass
            threading.Thread(target=flush_loop, daemon=True).start()
            
        start_flush_timer()

        def message_handler(msg):
            try:
                yf_sym = msg.get('id')
                price = msg.get('price')
                if not yf_sym or not price: return

                # ==========================================
                # 獨立處理大盤加權指數 (^TWII)
                # ==========================================
                if yf_sym == "^TWII":
                    c_price = float(price)
                    change = msg.get('change', 0)
                    change_pct = msg.get('change_percent', 0)
                    if c_price > 0 and self.db_firestore:
                        try:
                            rtdb.reference("System/Index").update({
                                "price": round(c_price, 2), 
                                "change": round(change, 2), 
                                "changePct": round(change_pct, 2), 
                                "time": datetime.now().strftime('%H:%M:%S')
                            })
                        except: pass
                    return

                # ==========================================
                # 處理個股報價
                # ==========================================
                if yf_sym not in self.yf_to_fugle: return

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

                base_ref = float(row.get('Ref_Close', price))
                base_5d = float(row.get('Base_5D', price))
                base_10d = float(row.get('Base_10D', price))
                base_20d = float(row.get('Base_20D', price))
                max_high = float(row.get('Max_High', price))

                t_return = ((price - base_ref) / base_ref) * 100 if base_ref > 0 else 0.0
                ret_5d = ((price - base_5d) / base_5d) * 100 if base_5d > 0 else 0.0
                ret_10d = ((price - base_10d) / base_10d) * 100 if base_10d > 0 else 0.0
                ret_20d = ((price - base_20d) / base_20d) * 100 if base_20d > 0 else 0.0
                raw_dist = ((price - max_high) / max_high) * 100 if max_high > 0 else 0.0

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
                        "Breakout_Margin": round(raw_dist, 2) if raw_dist >= 0 else "",
                        "Distance_To_High": round(abs(raw_dist), 2) if raw_dist < 0 else ""
                    }
            except: pass

        def run_yf_ws():
            while True:
                try:
                    with yf.WebSocket(verbose=False) as ws:
                        ws.subscribe(ws_symbols)
                        
                        def heartbeat():
                            while True:
                                time.sleep(15.0)
                                try: ws.subscribe(ws_symbols)
                                except: break
                        threading.Thread(target=heartbeat, daemon=True).start()
                        
                        ws.listen(message_handler)
                except Exception as e:
                    print(f"[YF WebSocket] 斷線重連中: {e}")
                time.sleep(5)

        threading.Thread(target=run_yf_ws, daemon=True).start()

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
        return tickers

    def fetch_yfinance_rating(self, yf_symbol):
        try:
            tkr = yf.Ticker(yf_symbol)
            recos = tkr.recommendations
            if recos is not None and not recos.empty:
                if 'To Grade' in recos.columns: return str(recos.iloc[-1]['To Grade']).replace('\n', ' ')
                elif 'period' in recos.columns and 'strongBuy' in recos.columns:
                    row = recos.iloc[0] 
                    return f"強買:{row['strongBuy']} 買進:{row['buy']} 持有:{row['hold']}"
            return ""
        except: return ""

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
                    
                    if len(df) > 1:
                        yesterday_close = round(float(df['close'].iloc[-2]), 2)
                        # 前高：除了最新 K 線外，歷史最高
                        df_history = df.iloc[:-1] 
                        prev_max_high = round(float(df_history['high'].max()), 2)
                        days_since_high = int(len(df) - 1 - df_history['high'].idxmax())
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
                        raw_dist = calc_return(latest_close, prev_max_high)
                        
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
                            "Breakout_Margin": round(raw_dist, 2) if raw_dist >= 0 else "", "Distance_To_High": round(abs(raw_dist), 2) if raw_dist < 0 else "",
                            
                            "Status_Tags": "-",
                            "Rating": rating, "Company_Info": "", "YF_Symbol": yf_sym,
                            "MA5": round(ma5, 2), "MA10": round(ma10, 2), "MA20": round(ma20, 2), "Klines": json.dumps(klines)
                        })
                except: pass

        if breakout_list:
            pd.DataFrame(breakout_list).to_csv(DAILY_LIST_FILE, index=False, encoding='utf-8-sig')
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
        print("\n=== 啟動全自動無人值守模式 (完全脫離富果版) ===")
        print("邏輯原則：13:35 透過 YFinance 抓取 K 線過濾；09:00 盤中利用 yf.WebSocket 推播大盤與個股")
        
        schedule_flags = {"1335": False}

        while True:
            now = datetime.now()
            time_str = now.strftime("%H%M")
            
            if self.check_is_market_open_today() and (900 <= int(time_str) <= 1330):
                self.update_firebase_status(True)
                
                if not getattr(self, 'yf_ws_started', False):
                    self.start_yfinance_websocket()
                    self.yf_ws_started = True
                    
                time.sleep(30)
            else:
                self.update_firebase_status(False)
                
                if time_str == "1335" and not schedule_flags["1335"]:
                    self.run_daily_batch()
                    schedule_flags["1335"] = True
                    self.yf_ws_started = False 
                
                if time_str == "0005":
                    schedule_flags = {"1335": False}
                    self.live_intraday_tracker.clear() 
                    
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