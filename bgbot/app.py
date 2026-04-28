#!/usr/bin/env python3
"""
BG-BOT v5 — MACD 4-5-1 Strategy + Real Trading + Backtesting + Auth
FIXED: All 13 bugs + Production upgrades
Run: python app.py
"""

import json, os, time, hmac, hashlib, base64, threading, math, traceback, sqlite3, logging
from datetime import datetime, timezone
from pathlib import Path
from flask import Flask, render_template, request, redirect, url_for, jsonify, send_file
from flask_socketio import SocketIO, emit, join_room, leave_room
from flask_login import LoginManager, UserMixin, login_user, logout_user, login_required, current_user

import requests as http_requests
import pandas as pd
import numpy as np

# Optional
try:
    from authlib.integrations.flask_client import OAuth
    HAS_AUTHLIB = True
except ImportError:
    HAS_AUTHLIB = False

try:
    from cryptography.fernet import Fernet
    HAS_FERNET = True
except ImportError:
    HAS_FERNET = False

# ═══════════════════════════════════════════════════════════════
#  FIX #12: FILE LOGGING (bukan print)
# ═══════════════════════════════════════════════════════════════
Path("logs").mkdir(exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("logs/bot.log", encoding="utf-8"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("BGBOT")

# ═══════════════════════════════════════════════════════════════
#  APP CONFIG
# ═══════════════════════════════════════════════════════════════
app = Flask(__name__)

# FIX #2: SECRET_KEY stabil (tidak logout saat restart)
app.config["SECRET_KEY"] = os.environ.get(
    "SECRET_KEY",
    "bgbot-v5-production-secret-key-change-me"
)
app.config["SESSION_COOKIE_SAMESITE"] = "Lax"

DB_FILE = "bgbot.db"
GOOGLE_CLIENT_ID = os.environ.get("GOOGLE_CLIENT_ID", "")
GOOGLE_CLIENT_SECRET = os.environ.get("GOOGLE_CLIENT_SECRET", "")
HAS_GOOGLE = bool(GOOGLE_CLIENT_ID and GOOGLE_CLIENT_SECRET and HAS_AUTHLIB)

# FIX #9: Encryption key for API secrets
ENCRYPT_KEY = os.environ.get("ENCRYPT_KEY", "")
fernet = None
if HAS_FERNET and ENCRYPT_KEY:
    try:
        fernet = Fernet(ENCRYPT_KEY.encode() if len(ENCRYPT_KEY) == 44 else Fernet.generate_key())
    except:
        fernet = None

socketio = SocketIO(app, cors_allowed_origins="*", async_mode="threading",
                    ping_timeout=30, ping_interval=10)
login_manager = LoginManager(app)
login_manager.login_view = "page_login"

oauth = None
google = None
if HAS_GOOGLE:
    oauth = OAuth(app)
    google = oauth.register(
        name="google", client_id=GOOGLE_CLIENT_ID, client_secret=GOOGLE_CLIENT_SECRET,
        server_metadata_url="https://accounts.google.com/.well-known/openid-configuration",
        client_kwargs={"scope": "openid email profile"})

DEFAULT_CFG = {
    "market_mode": "spot", "symbol": "BTCUSDT",
    "order_size": 50, "max_positions": 3,
    "tp_percent": 2.5, "sl_percent": 1.5,
    "trailing_stop": 0,
    "leverage": 3, "margin_mode": "crossed",
    "order_type": "market", "limit_offset": 0.2,
    "strategy": "macd_451",
    "macd_fast": 4, "macd_slow": 5, "macd_signal": 1,
    "risk_pct": 1.0,
    "cooldown_seconds": 60,
    "use_balance_pct": False,
    "indicators": {
        "macd": {"enabled": True, "fast": 4, "slow": 5, "signal": 1}
    }
}

# ═══════════════════════════════════════════════════════════════
#  FIX #8: SQLite thread-safe + FIX #9: encrypt/decrypt helpers
# ═══════════════════════════════════════════════════════════════
_db_lock = threading.Lock()

def get_db():
    conn = sqlite3.connect(DB_FILE, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    return conn

def safe_db(func):
    """Thread-safe DB decorator"""
    def wrapper(*args, **kwargs):
        with _db_lock:
            return func(*args, **kwargs)
    return wrapper

def encrypt_val(val):
    if not val:
        return ""
    if fernet:
        try:
            return fernet.encrypt(val.encode()).decode()
        except:
            pass
    return val

def decrypt_val(val):
    if not val:
        return ""
    if fernet:
        try:
            return fernet.decrypt(val.encode()).decode()
        except:
            pass
    return val

# ═══════════════════════════════════════════════════════════════
#  DATABASE
# ═══════════════════════════════════════════════════════════════
def init_db():
    conn = get_db()
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            email TEXT UNIQUE NOT NULL,
            name TEXT DEFAULT '',
            picture TEXT DEFAULT '',
            provider TEXT DEFAULT 'google',
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        CREATE TABLE IF NOT EXISTS api_configs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL UNIQUE,
            api_key TEXT DEFAULT '',
            api_secret TEXT DEFAULT '',
            api_passphrase TEXT DEFAULT '',
            demo INTEGER DEFAULT 1,
            FOREIGN KEY (user_id) REFERENCES users(id)
        );
        CREATE TABLE IF NOT EXISTS bot_configs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL UNIQUE,
            config_json TEXT DEFAULT '{}',
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (user_id) REFERENCES users(id)
        );
        CREATE TABLE IF NOT EXISTS trades (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            trade_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            mode TEXT DEFAULT 'spot',
            side TEXT DEFAULT 'buy',
            pair TEXT DEFAULT '',
            price REAL DEFAULT 0,
            order_type TEXT DEFAULT 'market',
            size REAL DEFAULT 0,
            pnl REAL DEFAULT 0,
            pnl_pct REAL DEFAULT 0,
            fee REAL DEFAULT 0,
            status TEXT DEFAULT 'simulated',
            order_id TEXT DEFAULT '',
            FOREIGN KEY (user_id) REFERENCES users(id)
        );
        CREATE TABLE IF NOT EXISTS positions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            symbol TEXT DEFAULT '',
            side TEXT DEFAULT '',
            size REAL DEFAULT 0,
            entry_price REAL DEFAULT 0,
            current_price REAL DEFAULT 0,
            pnl REAL DEFAULT 0,
            pnl_pct REAL DEFAULT 0,
            hold_side TEXT DEFAULT '',
            leverage INTEGER DEFAULT 1,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (user_id) REFERENCES users(id)
        );
        CREATE TABLE IF NOT EXISTS backtest_results (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            config_json TEXT DEFAULT '{}',
            metrics_json TEXT DEFAULT '{}',
            trades_json TEXT DEFAULT '[]',
            equity_json TEXT DEFAULT '[]',
            FOREIGN KEY (user_id) REFERENCES users(id)
        );
    """)
    conn.commit()
    conn.close()

init_db()

# ═══════════════════════════════════════════════════════════════
#  USER MODEL
# ═══════════════════════════════════════════════════════════════
class User(UserMixin):
    def __init__(self, id, email, name="", picture="", provider="google"):
        self.id = id
        self.email = email
        self.name = name
        self.picture = picture
        self.provider = provider

@login_manager.user_loader
def load_user(uid):
    conn = get_db()
    r = conn.execute("SELECT * FROM users WHERE id=?", (uid,)).fetchone()
    conn.close()
    if r:
        return User(r["id"], r["email"], r["name"], r["picture"], r["provider"])
    return None

@safe_db
def get_or_create_user(email, name="", picture="", provider="google"):
    conn = get_db()
    r = conn.execute("SELECT * FROM users WHERE email=?", (email,)).fetchone()
    if r:
        conn.execute("UPDATE users SET name=?, picture=? WHERE id=?",
                     (name, picture, r["id"]))
        conn.commit()
        uid = r["id"]
    else:
        cur = conn.execute(
            "INSERT INTO users (email,name,picture,provider) VALUES (?,?,?,?)",
            (email, name, picture, provider))
        uid = cur.lastrowid
        conn.execute("INSERT INTO api_configs (user_id) VALUES (?)", (uid,))
        conn.execute("INSERT INTO bot_configs (user_id,config_json) VALUES (?,?)",
                     (uid, json.dumps(DEFAULT_CFG)))
        conn.commit()
    conn.close()
    return User(uid, email, name, picture, provider)

@safe_db
def get_user_api(uid):
    conn = get_db()
    r = conn.execute("SELECT * FROM api_configs WHERE user_id=?", (uid,)).fetchone()
    conn.close()
    if r:
        return {
            "api_key": decrypt_val(r["api_key"] or ""),
            "api_secret": decrypt_val(r["api_secret"] or ""),
            "api_passphrase": decrypt_val(r["api_passphrase"] or ""),
            "demo": bool(r["demo"])
        }
    return {"api_key": "", "api_secret": "", "api_passphrase": "", "demo": True}

@safe_db
def save_user_api(uid, cfg):
    conn = get_db()
    conn.execute(
        "INSERT OR REPLACE INTO api_configs (user_id,api_key,api_secret,api_passphrase,demo) VALUES (?,?,?,?,?)",
        (uid, encrypt_val(cfg.get("api_key", "")),
         encrypt_val(cfg.get("api_secret", "")),
         encrypt_val(cfg.get("api_passphrase", "")),
         1 if cfg.get("demo", True) else 0))
    conn.commit()
    conn.close()

@safe_db
def get_user_cfg(uid):
    conn = get_db()
    r = conn.execute("SELECT config_json FROM bot_configs WHERE user_id=?",
                     (uid,)).fetchone()
    conn.close()
    if r and r["config_json"]:
        try:
            cfg = json.loads(r["config_json"])
            # Merge missing keys from DEFAULT_CFG
            for k, v in DEFAULT_CFG.items():
                if k not in cfg:
                    cfg[k] = v
            return cfg
        except:
            pass
    return DEFAULT_CFG.copy()

@safe_db
def save_user_cfg(uid, cfg):
    conn = get_db()
    conn.execute(
        "INSERT OR REPLACE INTO bot_configs (user_id,config_json,updated_at) VALUES (?,?,CURRENT_TIMESTAMP)",
        (uid, json.dumps(cfg)))
    conn.commit()
    conn.close()

@safe_db
def save_trade(uid, t):
    conn = get_db()
    conn.execute(
        "INSERT INTO trades (user_id,trade_time,mode,side,pair,price,order_type,size,pnl,pnl_pct,fee,status,order_id) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
        (uid, t.get("time", ""), t.get("mode", "spot"), t.get("side", "buy"),
         t.get("pair", ""), t.get("price", 0), t.get("type", "market"),
         t.get("size", 0), t.get("pnl", 0), t.get("pnl_pct", 0),
         t.get("fee", 0), t.get("status", "sim"), t.get("order_id", "")))
    conn.commit()
    conn.close()

@safe_db
def get_trades(uid, limit=100):
    conn = get_db()
    rows = conn.execute(
        "SELECT * FROM trades WHERE user_id=? ORDER BY id DESC LIMIT ?",
        (uid, limit)).fetchall()
    conn.close()
    return [dict(r) for r in rows]

@safe_db
def get_trade_stats(uid):
    conn = get_db()
    total = conn.execute("SELECT COUNT(*) as c FROM trades WHERE user_id=?",
                         (uid,)).fetchone()["c"]
    wins = conn.execute("SELECT COUNT(*) as c FROM trades WHERE user_id=? AND pnl>0",
                        (uid,)).fetchone()["c"]
    losses = conn.execute("SELECT COUNT(*) as c FROM trades WHERE user_id=? AND pnl<=0",
                          (uid,)).fetchone()["c"]
    pnl = conn.execute("SELECT COALESCE(SUM(pnl),0) as s FROM trades WHERE user_id=?",
                       (uid,)).fetchone()["s"]
    spot = conn.execute("SELECT COUNT(*) as c FROM trades WHERE user_id=? AND mode='spot'",
                        (uid,)).fetchone()["c"]
    perp = conn.execute("SELECT COUNT(*) as c FROM trades WHERE user_id=? AND mode='perp'",
                        (uid,)).fetchone()["c"]
    conn.close()
    return {"total": total, "wins": wins, "losses": losses,
            "total_pnl": round(pnl, 2), "spot": spot, "perp": perp,
            "win_rate": round(wins / total * 100, 1) if total > 0 else 0}

@safe_db
def save_positions(uid, positions):
    conn = get_db()
    conn.execute("DELETE FROM positions WHERE user_id=?", (uid,))
    for p in positions:
        conn.execute(
            "INSERT INTO positions (user_id,symbol,side,size,entry_price,current_price,pnl,pnl_pct,hold_side,leverage) VALUES (?,?,?,?,?,?,?,?,?,?)",
            (uid, p.get("symbol", ""), p.get("side", ""), p.get("size", 0),
             p.get("entry_price", 0), p.get("current_price", 0),
             p.get("pnl", 0), p.get("pnl_pct", 0), p.get("hold_side", ""),
             p.get("leverage", 1)))
    conn.commit()
    conn.close()

@safe_db
def get_positions_db(uid):
    conn = get_db()
    rows = conn.execute("SELECT * FROM positions WHERE user_id=?", (uid,)).fetchall()
    conn.close()
    return [dict(r) for r in rows]

@safe_db
def save_backtest(uid, cfg, metrics, trades, equity):
    conn = get_db()
    conn.execute(
        "INSERT INTO backtest_results (user_id,config_json,metrics_json,trades_json,equity_json) VALUES (?,?,?,?,?)",
        (uid, json.dumps(cfg), json.dumps(metrics),
         json.dumps(trades), json.dumps(equity)))
    conn.commit()
    conn.close()

@safe_db
def get_backtests(uid, limit=20):
    conn = get_db()
    rows = conn.execute(
        "SELECT * FROM backtest_results WHERE user_id=? ORDER BY id DESC LIMIT ?",
        (uid, limit)).fetchall()
    conn.close()
    return [{"id": r["id"], "created_at": r["created_at"],
             "config": json.loads(r["config_json"]),
             "metrics": json.loads(r["metrics_json"])} for r in rows]

# ═══════════════════════════════════════════════════════════════
#  FIX #10: RETRY WRAPPER
# ═══════════════════════════════════════════════════════════════
def api_retry(func, *args, retries=3, delay=1, **kwargs):
    for attempt in range(retries):
        try:
            result = func(*args, **kwargs)
            return result
        except Exception as e:
            if attempt < retries - 1:
                logger.warning(f"API retry {attempt+1}/{retries}: {e}")
                time.sleep(delay * (attempt + 1))
            else:
                raise

# ═══════════════════════════════════════════════════════════════
#  BITGET API CLIENT (with retry)
# ═══════════════════════════════════════════════════════════════
class BitgetClient:
    BASE = "https://api.bitget.com"

    def __init__(self, key, secret, passphrase, demo=True):
        self.key = key
        self.secret = secret
        self.passphrase = passphrase
        self.demo = demo
        self.sess = http_requests.Session()
        self.last_request_time = 0
        self.min_request_interval = 0.2  # Rate limit guard

    def _rate_limit(self):
        """FIX #4: Rate limit protection"""
        elapsed = time.time() - self.last_request_time
        if elapsed < self.min_request_interval:
            time.sleep(self.min_request_interval - elapsed)
        self.last_request_time = time.time()

    def _sign(self, ts, method, path, body=""):
        msg = ts + method.upper() + path + body
        return base64.b64encode(
            hmac.new(self.secret.encode(), msg.encode(),
                     hashlib.sha256).digest()).decode()

    def _headers(self, method, path, body=""):
        ts = str(int(time.time()))
        h = {"ACCESS-KEY": self.key,
             "ACCESS-SIGN": self._sign(ts, method, path, body),
             "ACCESS-TIMESTAMP": ts,
             "ACCESS-PASSPHRASE": self.passphrase,
             "Content-Type": "application/json",
             "locale": "en-US"}
        if self.demo:
            h["paptrading"] = "1"
        return h

    def _req(self, method, path, params=None, data=None):
        self._rate_limit()
        body = json.dumps(data) if data else ""
        for attempt in range(3):
            try:
                r = self.sess.request(
                    method, self.BASE + path,
                    headers=self._headers(method, path, body),
                    params=params, data=body or None, timeout=15)
                result = r.json()
                return result
            except Exception as e:
                if attempt < 2:
                    logger.warning(f"API request retry {attempt+1}/3: {e}")
                    time.sleep(1 * (attempt + 1))
                else:
                    return {"code": "99999", "msg": str(e)}

    @staticmethod
    def fetch_historical(symbol, gran, days=7):
        all_data = []
        end_ts = int(datetime.now(timezone.utc).timestamp() * 1000)
        gran_ms = {"1m": 60000, "5m": 300000, "15m": 900000,
                   "1h": 3600000, "4h": 14400000, "1d": 86400000}.get(gran, 60000)
        total_candles = int((days * 86400000) / gran_ms)
        fetched = 0
        while fetched < total_candles:
            params = {"symbol": symbol, "granularity": gran,
                      "limit": "200", "endTime": str(end_ts)}
            try:
                r = http_requests.get(
                    "https://api.bitget.com/api/v2/spot/market/candles",
                    params=params, timeout=10)
                data = r.json()
                if data.get("code") != "00000" or not data.get("data"):
                    break
                rows = data["data"]
                if not rows:
                    break
                for row in rows:
                    all_data.append(row)
                end_ts = int(rows[-1][0]) - 1
                fetched += len(rows)
                time.sleep(0.2)  # FIX #4: rate limit
            except Exception as e:
                logger.error(f"Historical fetch error: {e}")
                break
        if not all_data:
            return None
        df = pd.DataFrame(all_data,
                          columns=["timestamp", "open", "high", "low",
                                   "close", "volume", "quote_volume"])
        for c in ["open", "high", "low", "close", "volume"]:
            df[c] = pd.to_numeric(df[c])
        df["timestamp"] = pd.to_datetime(df["timestamp"].astype(int), unit="ms")
        df = df.drop_duplicates(subset=["timestamp"]).sort_values(
            "timestamp").reset_index(drop=True)
        return df

    def get_klines(self, symbol, gran, market="spot", limit=200):
        path = "/api/v2/spot/market/candles" if market == "spot" else "/api/v2/mix/market/candles"
        params = {"symbol": symbol, "granularity": gran, "limit": str(limit)}
        if market != "spot":
            params["productType"] = "USDT-FUTURES"
        result = self._req("GET", path, params)
        if not result or result.get("code") != "00000":
            return None
        try:
            df = pd.DataFrame(result["data"],
                              columns=["timestamp", "open", "high", "low",
                                       "close", "volume", "quote_volume"])
            for c in ["open", "high", "low", "close", "volume"]:
                df[c] = pd.to_numeric(df[c])
            df["timestamp"] = pd.to_datetime(df["timestamp"].astype(int), unit="ms")
            return df.sort_values("timestamp").reset_index(drop=True)
        except:
            return None

    def get_balance(self, market="spot"):
        try:
            if market == "spot":
                r = self._req("GET", "/api/v2/spot/account/assets", {"coin": "USDT"})
                if r and r.get("data") and r["data"]:
                    return float(r["data"][0].get("available", 0))
            else:
                r = self._req("GET", "/api/v2/account/get-account-balance",
                              {"productType": "USDT-FUTURES"})
                if r and r.get("data"):
                    return float(r["data"][0].get("available", 0))
        except:
            pass
        return 0

    def test(self):
        try:
            return {"ok": True, "balance": self.get_balance("spot")}
        except Exception as e:
            return {"ok": False, "msg": str(e)}

    def spot_market(self, symbol, side, size):
        return self._req("POST", "/api/v2/spot/trade/place-order",
                         data={"symbol": symbol, "side": side,
                               "orderType": "market", "force": "gtc",
                               "size": str(size)})

    def spot_limit(self, symbol, side, price, size):
        return self._req("POST", "/api/v2/spot/trade/place-order",
                         data={"symbol": symbol, "side": side,
                               "orderType": "limit", "force": "gtc",
                               "price": str(price), "size": str(size)})

    def set_leverage(self, symbol, lev, hs):
        return self._req("POST", "/api/v2/mix/account/set-leverage",
                         data={"productType": "USDT-FUTURES", "symbol": symbol,
                               "leverage": str(lev), "holdSide": hs})

    def set_margin_mode(self, symbol, mode):
        return self._req("POST", "/api/v2/mix/account/set-margin-mode",
                         data={"productType": "USDT-FUTURES", "symbol": symbol,
                               "marginMode": mode, "marginCoin": "USDT"})

    def perp_market(self, symbol, side, size, tp=None, sl=None):
        data = {"productType": "USDT-FUTURES", "symbol": symbol,
                "marginMode": "crossed", "marginCoin": "USDT",
                "size": str(size), "side": side, "orderType": "market"}
        if tp:
            data["presetStopSurplusPrice"] = str(tp)
        if sl:
            data["presetStopLossPrice"] = str(sl)
        return self._req("POST", "/api/v2/mix/order/place-order", data=data)

    def perp_limit(self, symbol, side, price, size, tp=None, sl=None):
        data = {"productType": "USDT-FUTURES", "symbol": symbol,
                "marginMode": "crossed", "marginCoin": "USDT",
                "size": str(size), "side": side, "orderType": "limit",
                "price": str(price)}
        if tp:
            data["presetStopSurplusPrice"] = str(tp)
        if sl:
            data["presetStopLossPrice"] = str(sl)
        return self._req("POST", "/api/v2/mix/order/place-order", data=data)

    def get_positions(self, symbol=None):
        params = {"productType": "USDT-FUTURES"}
        if symbol:
            params["symbol"] = symbol
        r = self._req("GET", "/api/v2/mix/position/get-all-position", params)
        if r and r.get("data"):
            return [p for p in r["data"] if float(p.get("total", 0)) > 0]
        return []

    def close_position(self, symbol, hs):
        return self._req("POST", "/api/v2/mix/order/close-positions",
                         data={"productType": "USDT-FUTURES",
                               "symbol": symbol, "holdSide": hs})

# ═══════════════════════════════════════════════════════════════
#  INDICATOR ENGINE — MACD 4-5-1
# ═══════════════════════════════════════════════════════════════
class IndicatorEngine:
    """
    MACD 4-5-1 Strategy

    Parameters:
      fast   = 4  (EMA cepat)
      slow   = 5  (EMA lambat)
      signal = 1  (Signal line)

    Entry Rules:
      LONG  = MACD cross UP signal  + histogram momentum naik
      SHORT = MACD cross DOWN signal + histogram momentum turun
      ELSE  = trend bias (MACD > signal = LONG, vice versa)
    """
    def __init__(self, cfg):
        self.cfg = cfg
        ind = cfg.get("indicators", {}).get("macd", {})
        self.fast = cfg.get("macd_fast", ind.get("fast", 4))
        self.slow = cfg.get("macd_slow", ind.get("slow", 5))
        self.sig_period = cfg.get("macd_signal", ind.get("signal", 1))

    def compute(self, df, return_overlays=True):
        close = df["close"]
        times = df["timestamp"].tolist()

        ema_fast = close.ewm(span=self.fast).mean()
        ema_slow = close.ewm(span=self.slow).mean()
        macd_line = ema_fast - ema_slow
        signal_line = macd_line.ewm(span=self.sig_period).mean()
        hist = macd_line - signal_line

        m_now = macd_line.iloc[-1]
        m_prev = macd_line.iloc[-2]
        s_now = signal_line.iloc[-1]
        s_prev = signal_line.iloc[-2]
        h_now = hist.iloc[-1]
        h_prev = hist.iloc[-2]

        cross_up = m_now > s_now and m_prev <= s_prev
        cross_down = m_now < s_now and m_prev >= s_prev
        momentum_up = h_now > h_prev
        momentum_down = h_now < h_prev

        overlays = None
        if return_overlays:
            overlays = {
                "macd_line": [], "macd_signal": [], "macd_hist": [],
                "ema_fast": [], "ema_slow": [],
            }
            for i, t in enumerate(times):
                ts = int(t.timestamp())
                if not math.isnan(macd_line.iloc[i]):
                    hv = float(hist.iloc[i])
                    overlays["macd_line"].append({
                        "time": ts, "value": round(float(macd_line.iloc[i]), 6)})
                    overlays["macd_signal"].append({
                        "time": ts, "value": round(float(signal_line.iloc[i]), 6)})
                    overlays["macd_hist"].append({
                        "time": ts, "value": round(hv, 6),
                        "color": "rgba(0,212,170,0.6)" if hv >= 0 else "rgba(255,71,87,0.6)"})
                    overlays["ema_fast"].append({
                        "time": ts, "value": round(float(ema_fast.iloc[i]), 2)})
                    overlays["ema_slow"].append({
                        "time": ts, "value": round(float(ema_slow.iloc[i]), 2)})

        if cross_up and momentum_up:
            return {"macd": "LONG"}, overlays
        elif cross_down and momentum_down:
            return {"macd": "SHORT"}, overlays
        else:
            if m_now > s_now:
                return {"macd": "LONG"}, overlays
            elif m_now < s_now:
                return {"macd": "SHORT"}, overlays
            return {"macd": "NEUTRAL"}, overlays

    def compute_fast(self, df):
        return self.compute(df, return_overlays=False)[0]

# ═══════════════════════════════════════════════════════════════
#  SIGNAL RESOLVER
# ═══════════════════════════════════════════════════════════════
def resolve_signal(signals, strategy=None):
    return signals.get("macd", "NEUTRAL")

# ═══════════════════════════════════════════════════════════════
#  SIMULATED DATA
# ═══════════════════════════════════════════════════════════════
class SimData:
    PRICES = {"BTCUSDT": 68420, "ETHUSDT": 3850, "SOLUSDT": 142,
              "BNBUSDT": 580, "XRPUSDT": 0.58, "DOGEUSDT": 0.12}

    @staticmethod
    def gen(symbol, minutes=200, interval=1):
        base = SimData.PRICES.get(symbol, 100)
        now = datetime.now(timezone.utc)
        price = base * (0.97 + np.random.random() * 0.06)
        candles = []
        for i in range(minutes):
            t = now - pd.Timedelta(minutes=(minutes - i) * interval)
            ch = np.random.normal(0, base * 0.0008)
            o = price
            c = price + ch
            h = max(o, c) + abs(np.random.normal(0, base * 0.0003))
            l = min(o, c) - abs(np.random.normal(0, base * 0.0003))
            candles.append({
                "timestamp": t, "open": round(o, 2), "high": round(h, 2),
                "low": round(l, 2), "close": round(c, 2),
                "volume": round(abs(np.random.normal(100, 30)), 2)})
            price = c
        df = pd.DataFrame(candles)
        df["timestamp"] = pd.to_datetime(df["timestamp"])
        return df

# ═══════════════════════════════════════════════════════════════
#  BACKTEST ENGINE (FIX #11: more realistic)
# ═══════════════════════════════════════════════════════════════
class BacktestEngine:
    def __init__(self, cfg):
        self.cfg = cfg
        self.indicators = IndicatorEngine(cfg)
        self.tp_pct = cfg.get("tp_percent", 2.5) / 100
        self.sl_pct = cfg.get("sl_percent", 1.5) / 100
        self.order_size = cfg.get("order_size", 50)
        # FIX #11: More realistic costs
        self.fee_pct = 0.001        # 0.1% taker fee
        self.slippage_pct = 0.0008  # 0.08% slippage (was 0.05%)
        self.spread_pct = 0.0003    # 0.03% spread

    def _effective_entry(self, price, side):
        """FIX #11: Realistic entry with spread + slippage"""
        cost = price * (self.slippage_pct + self.spread_pct)
        if side == "LONG":
            return price + cost
        return price - cost

    def _effective_exit(self, price, side):
        cost = price * (self.slippage_pct + self.spread_pct)
        if side == "LONG":
            return price - cost
        return price + cost

    def run(self, df, initial_balance=10000):
        lookback = 20
        if len(df) < lookback + 10:
            return {"error": "Not enough data. Need at least 30 candles."}

        balance = initial_balance
        position = None
        trades = []
        equity = []
        peak_equity = initial_balance
        max_drawdown = 0
        max_drawdown_pct = 0
        prev_signal = "NEUTRAL"

        for i in range(lookback, len(df)):
            window = df.iloc[i - lookback:i + 1]
            price = float(df.iloc[i]["close"])
            high = float(df.iloc[i]["high"])
            low = float(df.iloc[i]["low"])
            ts = int(df.iloc[i]["timestamp"].timestamp())

            # Check TP/SL
            if position:
                hit_tp = False
                hit_sl = False
                exit_price = 0
                if position["side"] == "LONG":
                    if high >= position["tp"]:
                        hit_tp = True
                        exit_price = self._effective_exit(position["tp"], "LONG")
                    elif low <= position["sl"]:
                        hit_sl = True
                        exit_price = self._effective_exit(position["sl"], "LONG")
                else:
                    if low <= position["tp"]:
                        hit_tp = True
                        exit_price = self._effective_exit(position["tp"], "SHORT")
                    elif high >= position["sl"]:
                        hit_sl = True
                        exit_price = self._effective_exit(position["sl"], "SHORT")

                if hit_tp or hit_sl:
                    if position["side"] == "LONG":
                        pnl = (exit_price - position["entry"]) * position["size"]
                    else:
                        pnl = (position["entry"] - exit_price) * position["size"]
                    fee = exit_price * position["size"] * self.fee_pct
                    pnl -= fee
                    pnl_pct = (pnl / (position["entry"] * position["size"])) * 100
                    balance += pnl
                    trades.append({
                        "entry_time": position["time"], "exit_time": ts,
                        "side": position["side"], "entry": position["entry"],
                        "exit": round(exit_price, 2), "size": position["size"],
                        "pnl": round(pnl, 2), "pnl_pct": round(pnl_pct, 2),
                        "fee": round(fee, 2),
                        "exit_type": "TP" if hit_tp else "SL"})
                    position = None

            # Signal (only on change)
            signals = self.indicators.compute_fast(window)
            signal = resolve_signal(signals)

            if position is None and signal != "NEUTRAL" and signal != prev_signal:
                entry_price = self._effective_entry(price, signal)
                size = self.order_size / entry_price
                fee = entry_price * size * self.fee_pct
                balance -= fee
                if signal == "LONG":
                    tp = round(entry_price * (1 + self.tp_pct), 2)
                    sl = round(entry_price * (1 - self.sl_pct), 2)
                else:
                    tp = round(entry_price * (1 - self.tp_pct), 2)
                    sl = round(entry_price * (1 + self.sl_pct), 2)
                position = {"side": signal, "entry": entry_price, "size": size,
                            "tp": tp, "sl": sl, "time": ts}

            prev_signal = signal

            # Equity
            unrealized = 0
            if position:
                if position["side"] == "LONG":
                    unrealized = (price - position["entry"]) * position["size"]
                else:
                    unrealized = (position["entry"] - price) * position["size"]
            current_equity = balance + unrealized
            equity.append({"time": ts, "value": round(current_equity, 2)})
            peak_equity = max(peak_equity, current_equity)
            dd = peak_equity - current_equity
            dd_pct = (dd / peak_equity) * 100 if peak_equity > 0 else 0
            max_drawdown = max(max_drawdown, dd)
            max_drawdown_pct = max(max_drawdown_pct, dd_pct)

        # Close remaining
        if position:
            last_price = float(df.iloc[-1]["close"])
            exit_price = self._effective_exit(last_price, position["side"])
            if position["side"] == "LONG":
                pnl = (exit_price - position["entry"]) * position["size"]
            else:
                pnl = (position["entry"] - exit_price) * position["size"]
            fee = exit_price * position["size"] * self.fee_pct
            pnl -= fee
            pnl_pct = (pnl / (position["entry"] * position["size"])) * 100
            balance += pnl
            trades.append({
                "entry_time": position["time"],
                "exit_time": int(df.iloc[-1]["timestamp"].timestamp()),
                "side": position["side"], "entry": position["entry"],
                "exit": round(exit_price, 2), "size": position["size"],
                "pnl": round(pnl, 2), "pnl_pct": round(pnl_pct, 2),
                "fee": round(fee, 2), "exit_type": "END"})

        # Metrics
        wins = [t for t in trades if t["pnl"] > 0]
        losses = [t for t in trades if t["pnl"] <= 0]
        gross_profit = sum(t["pnl"] for t in wins) if wins else 0
        gross_loss = abs(sum(t["pnl"] for t in losses)) if losses else 0
        total_fees = sum(t["fee"] for t in trades)

        max_win_streak = 0
        max_loss_streak = 0
        curr_w = 0
        curr_l = 0
        for t in trades:
            if t["pnl"] > 0:
                curr_w += 1; curr_l = 0
                max_win_streak = max(max_win_streak, curr_w)
            else:
                curr_l += 1; curr_w = 0
                max_loss_streak = max(max_loss_streak, curr_l)

        avg_win = float(np.mean([t["pnl"] for t in wins])) if wins else 0
        avg_loss = float(np.mean([abs(t["pnl"]) for t in losses])) if losses else 0

        if trades and len(trades) > 1:
            returns = [t["pnl_pct"] for t in trades]
            sharpe = (float(np.mean(returns)) / float(np.std(returns))) * np.sqrt(252) \
                if np.std(returns) > 0 else 0
        else:
            sharpe = 0

        metrics = {
            "initial_balance": initial_balance,
            "final_balance": round(balance, 2),
            "total_return": round(balance - initial_balance, 2),
            "total_return_pct": round(((balance - initial_balance) / initial_balance) * 100, 2),
            "total_trades": len(trades),
            "wins": len(wins), "losses": len(losses),
            "win_rate": round(len(wins) / len(trades) * 100, 1) if trades else 0,
            "gross_profit": round(gross_profit, 2),
            "gross_loss": round(gross_loss, 2),
            "profit_factor": round(gross_profit / gross_loss, 2) if gross_loss > 0 else 0,
            "total_fees": round(total_fees, 2),
            "max_drawdown": round(max_drawdown, 2),
            "max_drawdown_pct": round(max_drawdown_pct, 2),
            "avg_win": round(avg_win, 2),
            "avg_loss": round(avg_loss, 2),
            "reward_risk": round(avg_win / avg_loss, 2) if avg_loss > 0 else 0,
            "max_win_streak": max_win_streak,
            "max_loss_streak": max_loss_streak,
            "sharpe_ratio": round(sharpe, 2),
            "candles": len(df), "lookback": lookback,
        }
        return {"metrics": metrics, "trades": trades, "equity": equity}

# ═══════════════════════════════════════════════════════════════
#  TRADING BOT (FIX #3, #6, #7, #5)
# ═══════════════════════════════════════════════════════════════
class TradingBot:
    def __init__(self, uid, sio):
        self.uid = uid
        self.sio = sio
        self.running = False
        self.client = None
        self.indicators = None
        self.thread = None  # FIX #3: Track thread
        self.config = DEFAULT_CFG.copy()
        self.api_cfg = {}
        self.last_trade_time = 0  # FIX #7: Cooldown
        self.state = {
            "running": False, "mode": "spot", "symbol": "BTCUSDT",
            "balance": 0, "connected": False,
            "m1": {"signal": "NEUTRAL", "price": 0, "indicators": {}},
            "m5": {"signal": "NEUTRAL", "price": 0, "indicators": {}},
            "aligned": False, "trades": [], "logs": [], "positions": [],
            "stats": {"total": 0, "wins": 0, "losses": 0, "spot": 0, "perp": 0,
                      "alignments": 0, "checks": 0, "total_pnl": 0}
        }
        self._load()

    def _load(self):
        self.api_cfg = get_user_api(self.uid)
        self.config = get_user_cfg(self.uid)
        self.indicators = IndicatorEngine(self.config)
        self.state["mode"] = self.config.get("market_mode", "spot")
        self.state["symbol"] = self.config.get("symbol", "BTCUSDT")
        ds = get_trade_stats(self.uid)
        self.state["stats"].update({
            "total": ds["total"], "wins": ds["wins"], "losses": ds["losses"],
            "spot": ds["spot"], "perp": ds["perp"], "total_pnl": ds["total_pnl"]})
        self.state["trades"] = get_trades(self.uid, 30)

    def _emit(self, event, data):
        try:
            self.sio.emit(event, data, room=f"user_{self.uid}")
        except:
            pass

    def _log(self, level, msg):
        ts = datetime.now().strftime("%H:%M:%S")
        entry = {"time": ts, "level": level, "msg": msg}
        self.state["logs"].append(entry)
        if len(self.state["logs"]) > 500:
            self.state["logs"] = self.state["logs"][-500:]
        self._emit("log", entry)
        log_func = {"info": logger.info, "success": logger.info,
                     "warn": logger.warning, "error": logger.error,
                     "tf": logger.info, "trade": logger.info}
        log_func.get(level, logger.info)(f"[U{self.uid}] {msg}")

    def _init_client(self):
        ac = self.api_cfg
        if not ac.get("api_key"):
            self._log("warn", "No API key -- SIMULATION mode")
            self.state["connected"] = False
            return False
        try:
            self.client = BitgetClient(ac["api_key"], ac["api_secret"],
                                       ac["api_passphrase"], ac.get("demo", True))
            r = api_retry(self.client.test)
            if r["ok"]:
                self.state["connected"] = True
                self.state["balance"] = r["balance"]
                self._log("success", f"API connected! Balance: ${r['balance']:.2f}")
                return True
            else:
                self._log("error", f"API failed: {r.get('msg', '')}")
                self.state["connected"] = False
                return False
        except Exception as e:
            self._log("error", f"Connection error: {e}")
            self.state["connected"] = False
            return False

    def _get_klines(self, symbol, gran, market):
        if self.state["connected"] and self.client:
            df = self.client.get_klines(symbol, gran, market)
            # FIX #5: Proper null check
            if df is not None and not df.empty and len(df) > 10:
                return df
            self._log("warn", f"No market data for {symbol} {gran}, using simulation")
        interval = {"1m": 1, "5m": 5, "15m": 15, "1h": 60}.get(gran, 1)
        return SimData.gen(symbol, 200, interval)

    def _build_chart(self, df, signals, overlays):
        candles = [{"time": int(r["timestamp"].timestamp()),
                     "open": float(r["open"]), "high": float(r["high"]),
                     "low": float(r["low"]), "close": float(r["close"])}
                    for _, r in df.iterrows()]
        return {"candles": candles, "overlays": overlays,
                "signals": signals, "price": float(df["close"].iloc[-1])}

    def _calc_order_size(self, price):
        """FIX #6: Risk-based position sizing"""
        cfg = self.config
        balance = self.state.get("balance", 0)

        if cfg.get("use_balance_pct", False) and balance > 0:
            risk_pct = cfg.get("risk_pct", 1.0) / 100
            size_usd = balance * risk_pct
            self._log("info", f"Risk sizing: {risk_pct*100}% of ${balance:.2f} = ${size_usd:.2f}")
            return size_usd
        else:
            return cfg.get("order_size", 50)

    def _execute(self, signal, price):
        cfg = self.config
        mode = cfg.get("market_mode", "spot")
        otype = cfg.get("order_type", "market")
        symbol = cfg.get("symbol", "BTCUSDT")
        side = "buy" if signal == "LONG" else "sell"

        # FIX #6: Dynamic sizing
        size = self._calc_order_size(price)
        if size <= 0:
            self._log("warn", "Order size is 0, skipping")
            return

        if not self.state["connected"] or not self.client:
            pnl = round(float(np.random.uniform(-5, 12)), 2)
            trade = {
                "time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "mode": mode, "side": side, "pair": symbol, "price": price,
                "type": otype, "size": size, "pnl": pnl,
                "pnl_pct": round(pnl / size * 100, 2),
                "fee": round(price * size * 0.001, 2),
                "status": "simulated", "order_id": ""}
            self.state["trades"].insert(0, trade)
            self.state["stats"]["total"] += 1
            self.state["stats"][mode] += 1
            self.state["stats"]["total_pnl"] = round(
                self.state["stats"]["total_pnl"] + pnl, 2)
            if pnl >= 0:
                self.state["stats"]["wins"] += 1
            else:
                self.state["stats"]["losses"] += 1
            save_trade(self.uid, trade)
            self._log("trade",
                      f"[SIM] {otype.upper()} {signal} {symbol} @ ${price:,.2f} PnL: ${pnl:+.2f}")
            self._emit("trade", trade)
            return

        try:
            order_id = ""
            fee = 0
            if mode == "spot":
                if otype == "limit":
                    offset = cfg.get("limit_offset", 0.2) / 100
                    lp = round(price * (1 - offset if side == "buy" else 1 + offset), 2)
                    r = self.client.spot_limit(symbol, side, lp, size)
                else:
                    r = self.client.spot_market(symbol, side, size)
                # FIX #5: Check response
                if r and r.get("code") == "00000":
                    order_id = r.get("data", {}).get("orderId", "")
                    fee = round(price * size * 0.001, 2)
                    self._log("success", f"SPOT {side.upper()} filled! OrderID: {order_id}")
                    self.state["stats"]["total"] += 1
                    self.state["stats"]["spot"] += 1
                else:
                    err_msg = r.get("msg", "unknown") if r else "No response"
                    self._log("error", f"SPOT order failed: {err_msg}")
                    return
            else:
                lev = cfg.get("leverage", 3)
                contracts = round(size * lev / price, 3)
                tp_p = cfg.get("tp_percent", 2.5) / 100
                sl_p = cfg.get("sl_percent", 1.5) / 100
                tp = round(price * (1 + tp_p if side == "buy" else 1 - tp_p), 2)
                sl = round(price * (1 - sl_p if side == "buy" else 1 + sl_p), 2)
                hs = "long" if side == "buy" else "short"
                for p in self.client.get_positions(symbol):
                    if (signal == "LONG" and p.get("holdSide") == "short") or \
                       (signal == "SHORT" and p.get("holdSide") == "long"):
                        self.client.close_position(symbol, p.get("holdSide"))
                        self._log("info", f"Closed opposite: {p.get('holdSide')}")
                self.client.set_margin_mode(symbol, cfg.get("margin_mode", "crossed"))
                self.client.set_leverage(symbol, lev, hs)
                if otype == "limit":
                    offset = cfg.get("limit_offset", 0.2) / 100
                    lp = round(price * (1 - offset if side == "buy" else 1 + offset), 2)
                    r = self.client.perp_limit(symbol, side, lp, contracts, tp, sl)
                else:
                    r = self.client.perp_market(symbol, side, contracts, tp, sl)
                # FIX #5: Check response
                if r and r.get("code") == "00000":
                    order_id = r.get("data", {}).get("orderId", "")
                    fee = round(price * contracts * 0.0006, 2)
                    self._log("success",
                              f"PERP {signal} filled! Size:{contracts} TP:${tp} SL:${sl}")
                    self.state["stats"]["total"] += 1
                    self.state["stats"]["perp"] += 1
                else:
                    err_msg = r.get("msg", "unknown") if r else "No response"
                    self._log("error", f"PERP order failed: {err_msg}")
                    return

            trade = {
                "time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "mode": mode, "side": side, "pair": symbol, "price": price,
                "type": otype, "size": size if mode == "spot" else contracts,
                "pnl": 0, "pnl_pct": 0, "fee": fee,
                "status": "live", "order_id": order_id}
            self.state["trades"].insert(0, trade)
            save_trade(self.uid, trade)
            self._emit("trade", trade)
        except Exception as e:
            self._log("error", f"Execution error: {e}")
            logger.error(traceback.format_exc())

    def _update_positions(self):
        if not self.state["connected"] or not self.client:
            return
        try:
            raw = self.client.get_positions()
            positions = []
            for p in raw:
                entry = float(p.get("averageOpenPrice", 0))
                current = float(p.get("markPrice", 0))
                sz = float(p.get("total", 0))
                pnl = float(p.get("unrealizedPL", 0))
                pnl_pct = ((current - entry) / entry * 100) if entry > 0 else 0
                if p.get("holdSide") == "short":
                    pnl_pct = -pnl_pct
                positions.append({
                    "symbol": p.get("symbol", ""),
                    "side": "long" if p.get("holdSide") == "long" else "short",
                    "size": sz, "entry_price": entry, "current_price": current,
                    "pnl": round(pnl, 2), "pnl_pct": round(pnl_pct, 2),
                    "hold_side": p.get("holdSide", ""),
                    "leverage": int(p.get("leverage", 1))})
            self.state["positions"] = positions
            save_positions(self.uid, positions)
        except:
            pass

    def _loop(self):
        self._log("info", "=== MACD 4-5-1 Bot Engine Started ===")
        symbol = self.config.get("symbol", "BTCUSDT")
        mode = self.config.get("market_mode", "spot")
        self.state["mode"] = mode
        self.state["symbol"] = symbol
        api_ok = self._init_client()
        if not api_ok:
            self._log("info", "SIMULATION mode. Add API key in Setup to trade live.")
        self._log("info", f"Pair: {symbol} | Mode: {mode.upper()} | Strategy: MACD 4-5-1")
        self._log("info", f"MACD: fast={self.indicators.fast} slow={self.indicators.slow} signal={self.indicators.sig_period}")

        prev_m5_signal = "NEUTRAL"

        # FIX #4: Configurable interval
        LOOP_INTERVAL = int(self.config.get("cooldown_seconds", 60))

        while self.running:
            try:
                df_m1 = self._get_klines(symbol, "1m", mode)
                df_m5 = self._get_klines(symbol, "5m", mode)

                # FIX #5: Skip if data is bad
                if df_m1 is None or df_m5 is None or df_m1.empty or df_m5.empty:
                    self._log("warn", "No data available, waiting...")
                    time.sleep(10)
                    continue

                if len(df_m1) < 10 or len(df_m5) < 10:
                    self._log("warn", "Insufficient data points, waiting...")
                    time.sleep(10)
                    continue

                sig_m1, ov_m1 = self.indicators.compute(df_m1)
                sig_m5, ov_m5 = self.indicators.compute(df_m5)

                m1 = resolve_signal(sig_m1)
                m5 = resolve_signal(sig_m5)
                p1 = float(df_m1["close"].iloc[-1])
                p5 = float(df_m5["close"].iloc[-1])

                # Alignment
                aligned = (m1 == m5) and (m1 != "NEUTRAL")

                self.state["m1"] = {"signal": m1, "price": p1, "indicators": sig_m1}
                self.state["m5"] = {"signal": m5, "price": p5, "indicators": sig_m5}
                self.state["aligned"] = aligned
                self.state["stats"]["checks"] += 1

                self._log("tf",
                          f"[M1] {m1} ${p1:,.2f} | [M5] {m5} ${p5:,.2f} {'ALIGNED' if aligned else ''}")

                # FIX #7: Execute with cooldown + signal change
                now = time.time()
                cooldown = self.config.get("cooldown_seconds", 60)
                if aligned and m5 != prev_m5_signal and (now - self.last_trade_time) > cooldown:
                    self.state["stats"]["alignments"] += 1
                    self._log("trade",
                              f"MACD 4-5-1 ALIGNED: M1={m1} M5={m5} -> EXECUTE")
                    self._execute(m5, p5)
                    self.last_trade_time = now
                elif aligned and m5 != prev_m5_signal:
                    self._log("info",
                              f"Signal change detected but cooldown active ({cooldown}s)")

                prev_m5_signal = m5

                self._update_positions()
                if self.state["connected"] and self.client:
                    try:
                        self.state["balance"] = self.client.get_balance(mode)
                    except:
                        pass

                self._emit("state_update", self.state)
                self._emit("chart_m1", self._build_chart(df_m1, sig_m1, ov_m1))
                self._emit("chart_m5", self._build_chart(df_m5, sig_m5, ov_m5))

                # FIX #4: Proper interval between checks
                for _ in range(LOOP_INTERVAL):
                    if not self.running:
                        break
                    time.sleep(1)

            except Exception as e:
                self._log("error", f"Error: {e}")
                logger.error(traceback.format_exc())
                time.sleep(10)

        self._log("warn", "Bot stopped.")
        self.state["running"] = False

    def start(self):
        # FIX #3: Prevent double thread
        if self.running:
            return {"ok": False, "msg": "Already running"}
        if self.thread and self.thread.is_alive():
            return {"ok": False, "msg": "Thread still alive, wait..."}

        self._load()
        self.running = True
        self.state["running"] = True
        self.last_trade_time = 0
        self.thread = threading.Thread(target=self._loop, daemon=True,
                                       name=f"bot-user-{self.uid}")
        self.thread.start()
        logger.info(f"Bot started for user {self.uid}")
        return {"ok": True}

    def stop(self):
        self.running = False
        self.state["running"] = False
        logger.info(f"Bot stopped for user {self.uid}")
        return {"ok": True}

# ═══════════════════════════════════════════════════════════════
#  BOT MANAGER
# ═══════════════════════════════════════════════════════════════
bots = {}

def get_bot(uid):
    if uid not in bots:
        bots[uid] = TradingBot(uid, socketio)
    return bots[uid]

# ═══════════════════════════════════════════════════════════════
#  HTTP ROUTES
# ═══════════════════════════════════════════════════════════════
@app.route("/")
def index():
    return redirect("/dashboard" if current_user.is_authenticated else "/login")

@app.route("/login")
def page_login():
    if current_user.is_authenticated:
        return redirect("/dashboard")
    return render_template("login.html", has_google=HAS_GOOGLE)

@app.route("/auth/google")
def auth_google():
    if not HAS_GOOGLE:
        return redirect("/login?error=Google+OAuth+not+configured")
    return google.authorize_redirect(url_for("auth_callback", _external=True))

@app.route("/auth/callback")
def auth_callback():
    try:
        token = google.authorize_access_token()
        ui = token.get("userinfo") or google.parse_id_token(token)
        if not ui:
            return redirect("/login?error=Failed")
        user = get_or_create_user(ui["email"], ui.get("name", ""),
                                  ui.get("picture", ""), "google")
        login_user(user, remember=True)
        return redirect("/dashboard")
    except Exception as e:
        logger.error(f"Google auth error: {e}")
        return redirect(f"/login?error={str(e)}")

@app.route("/auth/email", methods=["POST"])
def auth_email():
    email = request.form.get("email", "").strip()
    if not email or "@" not in email:
        return redirect("/login?error=Invalid+email")
    user = get_or_create_user(email, email.split("@")[0], "", "email")
    login_user(user, remember=True)
    return redirect("/dashboard")

@app.route("/auth/logout")
@login_required
def auth_logout():
    get_bot(current_user.id).stop()
    logout_user()
    return redirect("/login")

@app.route("/dashboard")
@login_required
def page_dashboard():
    return render_template("setup.html", user=current_user,
                           api=get_user_api(current_user.id),
                           cfg=get_user_cfg(current_user.id))

@app.route("/analysis")
@login_required
def page_analysis():
    return render_template("analysis.html", user=current_user,
                           cfg=get_user_cfg(current_user.id))

@app.route("/bot")
@login_required
def page_bot():
    return render_template("bot.html", user=current_user,
                           cfg=get_user_cfg(current_user.id))

@app.route("/backtest")
@login_required
def page_backtest():
    return render_template("backtest.html", user=current_user,
                           cfg=get_user_cfg(current_user.id))

@app.route("/history")
@login_required
def page_history():
    return render_template("history.html", user=current_user,
                           cfg=get_user_cfg(current_user.id))

# ═══════════════════════════════════════════════════════════════
#  API ROUTES
# ═══════════════════════════════════════════════════════════════
@app.route("/api/save-api", methods=["POST"])
@login_required
def api_save_api():
    save_user_api(current_user.id, request.json)
    return jsonify({"ok": True})

@app.route("/api/test-api", methods=["POST"])
@login_required
def api_test_api():
    d = request.json
    try:
        c = BitgetClient(d.get("api_key", ""), d.get("api_secret", ""),
                         d.get("api_passphrase", ""), d.get("demo", True))
        return jsonify(api_retry(c.test))
    except Exception as e:
        return jsonify({"ok": False, "msg": str(e)})

@app.route("/api/save-config", methods=["POST"])
@login_required
def api_save_config():
    save_user_cfg(current_user.id, request.json)
    return jsonify({"ok": True})

@app.route("/api/get-config")
@login_required
def api_get_config():
    return jsonify(get_user_cfg(current_user.id))

@app.route("/api/trades")
@login_required
def api_trades():
    return jsonify(get_trades(current_user.id,
                              request.args.get("limit", 100, type=int)))

@app.route("/api/trade-stats")
@login_required
def api_trade_stats():
    return jsonify(get_trade_stats(current_user.id))

@app.route("/api/positions")
@login_required
def api_positions():
    return jsonify(get_positions_db(current_user.id))

@app.route("/api/export-trades")
@login_required
def api_export_trades():
    trades = get_trades(current_user.id, 10000)
    if not trades:
        return jsonify({"error": "No trades"})
    lines = ["Time,Mode,Side,Pair,Price,Type,Size,PnL,PnL%,Fee,Status,OrderID"]
    for t in trades:
        lines.append(
            f"{t['trade_time']},{t['mode']},{t['side']},{t['pair']},{t['price']},"
            f"{t['order_type']},{t['size']},{t['pnl']},{t['pnl_pct']},{t['fee']},"
            f"{t['status']},{t['order_id']}")
    path = "/tmp/trades_export.csv"
    with open(path, "w") as f:
        f.write("\n".join(lines))
    return send_file(path, as_attachment=True,
                     download_name=f"bgbot_trades_{current_user.id}.csv")

@app.route("/api/run-backtest", methods=["POST"])
@login_required
def api_run_backtest():
    try:
        data = request.json
        symbol = data.get("symbol", "BTCUSDT")
        gran = data.get("granularity", "5m")
        days = int(data.get("days", 7))
        initial = float(data.get("initial_balance", 10000))
        cfg = data.get("config", get_user_cfg(current_user.id))
        logger.info(f"Backtest: {symbol} {gran} {days}d ${initial}")
        df = BitgetClient.fetch_historical(symbol, gran, days)
        if df is None or df.empty:
            return jsonify({"error": "Failed to fetch historical data from Bitget API"})
        engine = BacktestEngine(cfg)
        result = engine.run(df, initial)
        if "error" in result:
            return jsonify(result)
        save_backtest(current_user.id, cfg, result["metrics"],
                      result["trades"], result["equity"])
        return jsonify(result)
    except Exception as e:
        logger.error(f"Backtest error: {traceback.format_exc()}")
        return jsonify({"error": str(e), "trace": traceback.format_exc()})

@app.route("/api/backtest-history")
@login_required
def api_backtest_history():
    return jsonify(get_backtests(current_user.id))

# ═══════════════════════════════════════════════════════════════
#  WEBSOCKET
# ═══════════════════════════════════════════════════════════════
@socketio.on("connect")
def ws_connect():
    if current_user.is_authenticated:
        join_room(f"user_{current_user.id}")
        bot = get_bot(current_user.id)
        emit("state_update", bot.state)
        emit("config", bot.config)
        logger.info(f"WS connected: user {current_user.id}")

@socketio.on("disconnect")
def ws_disconnect():
    if current_user.is_authenticated:
        leave_room(f"user_{current_user.id}")

@socketio.on("get_state")
def ws_get_state():
    if current_user.is_authenticated:
        emit("state_update", get_bot(current_user.id).state)

@socketio.on("save_config")
def ws_save_config(data):
    if current_user.is_authenticated:
        save_user_cfg(current_user.id, data)
        bot = get_bot(current_user.id)
        bot.config = data
        bot.indicators = IndicatorEngine(data)
        emit("config_saved", {"ok": True})

@socketio.on("start_bot")
def ws_start():
    if current_user.is_authenticated:
        emit("bot_status", get_bot(current_user.id).start())

@socketio.on("stop_bot")
def ws_stop():
    if current_user.is_authenticated:
        emit("bot_status", get_bot(current_user.id).stop())

# ═══════════════════════════════════════════════════════════════
#  FIX #1: SINGLE __main__ (hapus yang kedua)
# ═══════════════════════════════════════════════════════════════
if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    logger.info("=" * 60)
    logger.info(f"BG-BOT v5 — MACD 4-5-1 Strategy")
    logger.info(f"Server:  http://localhost:{port}")
    logger.info(f"Google:  {'Configured' if HAS_GOOGLE else 'Email login only'}")
    logger.info(f"Encrypt: {'Enabled' if fernet else 'Disabled (pip install cryptography)'}")
    logger.info(f"MACD:    fast={DEFAULT_CFG['macd_fast']} slow={DEFAULT_CFG['macd_slow']} signal={DEFAULT_CFG['macd_signal']}")
    logger.info(f"Pages:   /login /dashboard /analysis /bot /backtest /history")
    logger.info("=" * 60)
    socketio.run(app, host="0.0.0.0", port=port, debug=False,
                 allow_unsafe_werkzeug=True)
