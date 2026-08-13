import html as html_mod
import json
import os
import re
import threading
import time
import xml.etree.ElementTree as ET
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, timedelta

import requests
from flask import Flask, jsonify, render_template, request

from nse_client import NseClient

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DOWNLOAD_FOLDER = os.path.join(BASE_DIR, ".nse_cache")
HIST_CACHE_DIR = os.path.join(DOWNLOAD_FOLDER, "hist_cache")
os.makedirs(DOWNLOAD_FOLDER, exist_ok=True)
os.makedirs(HIST_CACHE_DIR, exist_ok=True)

app = Flask(__name__)


@app.errorhandler(400)
@app.errorhandler(404)
@app.errorhandler(405)
@app.errorhandler(500)
@app.errorhandler(Exception)
def _handle_error(e):
    if request.path.startswith("/api/"):
        code = getattr(e, "code", 500)
        try:
            code = int(code)
        except (TypeError, ValueError):
            code = 500
        return jsonify({"ok": False, "error": str(e) or "Server error"}), code
    return e

_tls = threading.local()
SCAN_WORKERS = 6


def _client():
    if not hasattr(_tls, "client"):
        _tls.client = NseClient()
    return _tls.client


def _hist_rows(symbol, from_date, to_date):
    cache_file = os.path.join(HIST_CACHE_DIR, symbol + ".json")
    try:
        if os.path.exists(cache_file):
            with open(cache_file, "r", encoding="utf-8") as f:
                entry = json.load(f)
            rows = entry.get("rows") or []
            c_from = date.fromisoformat(entry.get("from", ""))
            c_to = date.fromisoformat(entry.get("to", ""))
            if c_from <= from_date and c_to >= to_date:
                return rows
    except Exception:
        pass
    rows = _client().fetch_equity_historical_data(symbol, from_date, to_date)
    try:
        tmp = cache_file + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump({"from": from_date.isoformat(), "to": to_date.isoformat(), "rows": rows}, f)
        os.replace(tmp, cache_file)
    except Exception:
        pass
    return rows


TOKEN_CACHE_FILE = os.path.join(DOWNLOAD_FOLDER, "tokens.json")
_token_cache = {}
_token_cache_lock = threading.Lock()
_intraday_cache = {}
_intraday_cache_lock = threading.Lock()
INTRADAY_TTL = 300


def _load_token_cache():
    global _token_cache
    try:
        if os.path.exists(TOKEN_CACHE_FILE):
            with open(TOKEN_CACHE_FILE, "r", encoding="utf-8") as f:
                _token_cache = json.load(f)
    except Exception:
        _token_cache = {}


def _save_token_cache():
    try:
        tmp = TOKEN_CACHE_FILE + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(_token_cache, f)
        os.replace(tmp, TOKEN_CACHE_FILE)
    except Exception:
        pass


_load_token_cache()


def _charting_token(symbol, client=None):
    client = client or _client()
    with _token_cache_lock:
        cached = _token_cache.get(symbol)
    if cached:
        return cached
    token = client.get_charting_token(symbol)
    if token:
        with _token_cache_lock:
            _token_cache[symbol] = token
            _save_token_cache()
    return token


def _intraday_rows(symbol, fetch_fn):
    now = time.time()
    key = (symbol, fetch_fn.__name__)
    with _intraday_cache_lock:
        hit = _intraday_cache.get(key)
        if hit and now - hit[0] < INTRADAY_TTL:
            return hit[1]
    rows = fetch_fn(symbol, _charting_token(symbol))
    with _intraday_cache_lock:
        _intraday_cache[key] = (now, rows)
    return rows


def _intraday_rows_5(symbol):
    return _intraday_rows(symbol, _client().fetch_intraday_5min)


def _intraday_rows_1(symbol):
    return _intraday_rows(symbol, _client().fetch_intraday_1min)

# (canonical index name shown on the website, name used by the constituents API)
SECTOR_INDICES = [
    ("NIFTY 50", "NIFTY 50"),
    ("NIFTY MIDCAP 100", "NIFTY MIDCAP 100"),
    ("NIFTY SMALLCAP 250", "NIFTY SMALLCAP 250"),
    ("NIFTY BANK", "NIFTY BANK"),
    ("NIFTY FINANCIAL SERVICES", "NIFTY FIN SERVICE"),
    ("NIFTY PRIVATE BANK", "NIFTY PVT BANK"),
    ("NIFTY PSU BANK", "NIFTY PSU BANK"),
    ("NIFTY IT", "NIFTY IT"),
    ("NIFTY PHARMA", "NIFTY PHARMA"),
    ("NIFTY HEALTHCARE", "NIFTY HEALTHCARE"),
    ("NIFTY AUTO", "NIFTY AUTO"),
    ("NIFTY FMCG", "NIFTY FMCG"),
    ("NIFTY METAL", "NIFTY METAL"),
    ("NIFTY MEDIA", "NIFTY MEDIA"),
    ("NIFTY REALTY", "NIFTY REALTY"),
    ("NIFTY ENERGY", "NIFTY ENERGY"),
    ("NIFTY OIL & GAS", "NIFTY OIL AND GAS"),
    ("NIFTY INFRA", "NIFTY INFRA"),
    ("NIFTY CONSUMER DURABLES", "NIFTY CONSUMER DURABLES"),
    ("NIFTY INDIA CONSUMPTION", "NIFTY CONSUMPTION"),
    ("NIFTY COMMODITIES", "NIFTY COMMODITIES"),
]
CANONICAL_TO_API = {c: a for c, a in SECTOR_INDICES}

NEWS_FEEDS = {
    "india": "https://news.google.com/rss/search?q=nifty+sensex+stock+market&hl=en-IN&gl=IN&ceid=IN:en",
    "goldsilver": "https://news.google.com/rss/search?q=gold+silver+prices&hl=en-US&gl=US&ceid=US:en",
    "crypto": "https://news.google.com/rss/search?q=bitcoin+cryptocurrency&hl=en-US&gl=US&ceid=US:en",
}
_NEWS_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/125.0 Safari/537.36"
}
FX_CALENDAR_URL = "https://nfs.faireconomy.media/ff_calendar_thisweek.json"
FLASH_FEED = "https://news.google.com/rss/search?q=nifty+OR+sensex+OR+shares+OR+%22stock+market%22&hl=en-IN&gl=IN&ceid=IN:en"

# Minimum distance (in %) from the 100 EMA required to qualify, so borderline
# whipsaw cases near the EMA are not flagged as signals.
EMA_TOLERANCE_PCT = 0.5

_cache = {}
_cache_lock = threading.Lock()
_last_market_status = ""


def _mk_status(raw):
    if isinstance(raw, dict):
        return (raw.get("marketStatus") or raw.get("marketStatusMessage") or "").strip()
    return (raw or "").strip()


def _fallback_market_status():
    try:
        from datetime import datetime, time as dtime
        from zoneinfo import ZoneInfo
        now = datetime.now(ZoneInfo("Asia/Kolkata"))
        if now.weekday() < 5:
            t = now.time()
            if dtime(9, 15) <= t <= dtime(15, 30):
                return "Market Open"
    except Exception:
        pass
    return ""


def _cached(key, ttl, fn):
    now = time.time()
    with _cache_lock:
        hit = _cache.get(key)
        if hit and now - hit[0] < ttl:
            return hit[1]
    data = fn()
    with _cache_lock:
        _cache[key] = (now, data)
    return data


@app.after_request
def _no_cache(resp):
    if resp.mimetype == "application/json":
        resp.headers["Cache-Control"] = "no-store"
    return resp


def _ema(values, period):
    # TradingView-style EMA: seeded with the first value, alpha = 2/(period+1)
    k = 2.0 / (period + 1)
    out = []
    prev = None
    for i, v in enumerate(values):
        if prev is None:
            prev = v
            out.append(prev)
        else:
            prev = v * k + prev * (1 - k)
            out.append(prev)
    return out


def _macd_indicators(closes):
    n = len(closes)
    if n < 110:
        return None
    ema12 = _ema(closes, 12)
    ema26 = _ema(closes, 26)
    ema100 = _ema(closes, 100)
    macd = [ema12[i] - ema26[i] for i in range(n)]
    sig_list = _ema(macd, 9)
    hist = [macd[i] - sig_list[i] for i in range(n)]
    return macd, hist, ema100


def _slice_mean(values, lo, hi):
    lo = max(lo, 0)
    if hi <= lo:
        return 0.0
    seg = values[lo:hi]
    return sum(seg) / len(seg) if seg else 0.0


def _rsi(closes, period=14):
    n = len(closes)
    if n <= period:
        return 50.0
    gains = [0.0] * n
    losses = [0.0] * n
    for i in range(1, n):
        d = closes[i] - closes[i - 1]
        if d > 0:
            gains[i] = d
        else:
            losses[i] = -d
    avg_gain = sum(gains[1:period + 1]) / period
    avg_loss = sum(losses[1:period + 1]) / period
    for i in range(period + 1, n):
        avg_gain = (avg_gain * (period - 1) + gains[i]) / period
        avg_loss = (avg_loss * (period - 1) + losses[i]) / period
    return 100.0 if avg_loss == 0 else 100 - 100 / (1 + avg_gain / avg_loss)


def _rsi_series(closes, period=14):
    n = len(closes)
    out = [50.0] * n
    if n <= period:
        return out
    gains = [0.0] * n
    losses = [0.0] * n
    for i in range(1, n):
        d = closes[i] - closes[i - 1]
        if d > 0:
            gains[i] = d
        else:
            losses[i] = -d
    avg_gain = sum(gains[1:period + 1]) / period
    avg_loss = sum(losses[1:period + 1]) / period
    out[period] = 100.0 if avg_loss == 0 else 100 - 100 / (1 + avg_gain / avg_loss)
    for i in range(period + 1, n):
        avg_gain = (avg_gain * (period - 1) + gains[i]) / period
        avg_loss = (avg_loss * (period - 1) + losses[i]) / period
        out[i] = 100.0 if avg_loss == 0 else 100 - 100 / (1 + avg_gain / avg_loss)
    return out


def _breakout_scan_one(client, symbol, from_date, to_date):
    try:
        rows = _hist_rows(symbol, from_date, to_date)
        O, H, L, C, V = [], [], [], [], []
        for r in rows:
            o = r.get("chOpeningPrice")
            h = r.get("chTradeHighPrice")
            lo = r.get("chTradeLowPrice")
            cl = r.get("chClosingPrice")
            if o is None or h is None or lo is None or cl is None:
                continue
            O.append(float(o))
            H.append(float(h))
            L.append(float(lo))
            C.append(float(cl))
            V.append(float(r.get("chTotTradedQty") or 0))
        n = len(C)
        if n < 230:
            return symbol, None
        BB, MAX_W = 25, 18.0
        price = C[-1]
        if price < 50:
            return symbol, None
        base_high = max(H[n - 1 - BB:n - 1])
        base_low = min(L[n - 1 - BB:n - 1])
        base_prev = max(H[n - 2 - BB:n - 2])
        base_range_pct = 100 * (base_high - base_low) / max(base_low, 0.01)
        avg_body10 = _slice_mean([abs(C[i] - O[i]) for i in range(n)], n - 11, n - 1)
        avg_range_base = _slice_mean([H[i] - L[i] for i in range(n)], n - 1 - BB, n - 1)
        accum_base = base_range_pct <= MAX_W and avg_body10 <= avg_range_base * 0.75
        base_avg_vol = _slice_mean(V, n - 1 - BB, n - 1)
        recent_avg_vol = _slice_mean(V, n - 11, n - 1)
        dry_up = recent_avg_vol <= base_avg_vol * 1.00
        ema50s = _ema(C, 50)
        ema200s = _ema(C, 200)
        ema50, ema200 = ema50s[-1], ema200s[-1]
        trend_ok = (
            price > ema50
            and ema50 > ema200
            and ema50s[-1] > ema50s[-6]
            and ema200s[-1] > ema200s[-11]
        )
        pivot = base_high
        bp = pivot * (1 + 0.10 / 100)
        bp_prev = base_prev * (1 + 0.10 / 100)
        breakout = price > bp and C[-2] <= bp_prev
        avg_vol20 = _slice_mean(V, n - 21, n - 1)
        vol_ratio = V[-1] / max(avg_vol20, 1)
        vol_ok = vol_ratio >= 1.30
        close_pos = (price - L[-1]) / max(H[-1] - L[-1], 0.01)
        strong_close = close_pos >= 0.65
        rsi = _rsi(C, 14)
        signal = trend_ok and accum_base and dry_up and breakout and vol_ok and strong_close and rsi >= 52
        entry = max(O[-1], bp)
        stop = base_low * (1 - 0.50 / 100)
        risk = max(entry - stop, 0.01)
        row = {
            "symbol": symbol,
            "price": round(price, 2),
            "changePct": round((price - C[-2]) / C[-2] * 100, 2) if C[-2] else 0,
            "entry": round(entry, 2),
            "stop": round(stop, 2),
            "target1": round(entry + risk, 2),
            "target2": round(entry + 2 * risk, 2),
            "target3": round(entry + 3 * risk, 2),
            "volRatio": round(vol_ratio, 2),
            "rsi": round(rsi, 1),
            "baseHigh": round(base_high, 2),
            "baseLow": round(base_low, 2),
            "baseWidthPct": round(base_range_pct, 2),
            "riskPct": round(100 * risk / entry, 2) if entry else 0,
            "closePos": round(close_pos * 100, 1),
        }
        row["signal"] = signal
        return symbol, row
    except Exception:
        raise


_FAILED = object()


def _parallel_scan(symbols, worker, retries=2):
    results = {}
    todo = list(symbols)
    for attempt in range(retries + 1):
        if not todo:
            break
        max_workers = SCAN_WORKERS if attempt == 0 else 3
        retry_me = []
        with ThreadPoolExecutor(max_workers=max_workers) as ex:
            fut_map = {ex.submit(worker, s): s for s in todo}
            for fut in as_completed(fut_map):
                s = fut_map[fut]
                _sym, row = fut.result()
                if row is _FAILED:
                    retry_me.append(s)
                elif row:
                    results[_sym] = row
        todo = list(dict.fromkeys(retry_me))
        if todo:
            time.sleep(2)
    return results, len(todo)


def _breakout_scan_symbols(symbols):
    to_date = date.today()
    from_date = to_date - timedelta(days=700)

    def worker(sym):
        try:
            return _breakout_scan_one(_client(), sym, from_date, to_date)
        except Exception:
            return sym, _FAILED

    results, _failed = _parallel_scan(symbols, worker)
    signals = sorted((r for r in results.values() if r["signal"]), key=lambda r: -r["volRatio"])
    return signals, len(symbols), len(results)


def _breakout_scan_sector(canonical):
    return _breakout_scan_symbols(list(_sector_live_map(canonical).keys()))


def _breakout_scan_all():
    return _breakout_scan_symbols(list(_all_sector_live_map().keys()))


# Darvas box scanner settings.
DARVAS_LOOKBACK = 20        # new-high lookback in trading days
DARVAS_MIN_PULLBACK = 3     # consecutive non-new-high days that complete a box
DARVAS_MAX_WIDTH_PCT = 25.0 # widest acceptable box
DARVAS_VOL_MULT = 1.5       # breakout volume vs the prior 20-day average
DARVAS_MAX_AGE = 8          # fresh breakout / box age in trading days
DARVAS_MIN_PRICE = 50.0


def _darvas_scan_one(client, symbol, from_date, to_date):
    try:
        rows = _hist_rows(symbol, from_date, to_date)
        O, H, L, C, V = [], [], [], [], []
        for r in rows:
            o = r.get("chOpeningPrice")
            h = r.get("chTradeHighPrice")
            lo = r.get("chTradeLowPrice")
            cl = r.get("chClosingPrice")
            if o is None or h is None or lo is None or cl is None:
                continue
            O.append(float(o))
            H.append(float(h))
            L.append(float(lo))
            C.append(float(cl))
            V.append(float(r.get("chTotTradedQty") or 0))
        n = len(C)
        if n < 230:
            return symbol, None
        price = C[-1]
        if price < DARVAS_MIN_PRICE:
            return symbol, None

        # Bullish boxes: a new 20-day high followed by a 3-day pullback.
        box_top = box_bottom = None
        box_idx = last_high_idx = None
        pullback = 0
        for i in range(DARVAS_LOOKBACK, n):
            if H[i] > max(H[i - DARVAS_LOOKBACK:i]):
                last_high_idx = i
                pullback = 0
            elif last_high_idx is not None:
                pullback += 1
                if pullback == DARVAS_MIN_PULLBACK:
                    box_top = H[last_high_idx]
                    box_bottom = min(L[i - DARVAS_MIN_PULLBACK + 1:i + 1])
                    box_idx = i
                    last_high_idx = None

        # Bearish boxes: a new 20-day low followed by a 3-day bounce.
        brk_top = brk_bottom = None
        brk_idx = last_low_idx = None
        pullback = 0
        for i in range(DARVAS_LOOKBACK, n):
            if L[i] < min(L[i - DARVAS_LOOKBACK:i]):
                last_low_idx = i
                pullback = 0
            elif last_low_idx is not None:
                pullback += 1
                if pullback == DARVAS_MIN_PULLBACK:
                    brk_bottom = L[last_low_idx]
                    brk_top = max(H[i - DARVAS_MIN_PULLBACK + 1:i + 1])
                    brk_idx = i
                    last_low_idx = None

        change_pct = round((price - C[-2]) / C[-2] * 100, 2) if C[-2] else 0
        ema200 = _ema(C, 200)[-1]
        rsi = _rsi(C, 14)
        high252 = max(H[-252:])
        dist52 = 100 * (high252 - price) / max(high252, 0.01)

        row = {
            "symbol": symbol,
            "price": round(price, 2),
            "changePct": change_pct,
            "boxTop": None,
            "boxBottom": None,
            "boxWidthPct": 0,
            "breakoutAge": None,
            "volRatio": None,
            "rsi": round(rsi, 1),
            "ema200": round(ema200, 2),
            "distHigh52Pct": round(dist52, 2),
            "entry": None,
            "stop": None,
            "target1": None,
            "target2": None,
            "target3": None,
            "riskPct": 0,
            "signal": None,
        }

        if box_idx is not None and (box_top and box_bottom):
            breakout_j = None
            for j in range(box_idx + 1, n):
                if C[j] > box_top:
                    breakout_j = j
                    break
            width_pct = 100 * (box_top - box_bottom) / max(box_bottom, 0.01)
            risk = max(box_top - box_bottom, 0.01)
            box_age = n - 1 - box_idx
            if breakout_j is not None:
                vol_ratio = V[breakout_j] / max(_slice_mean(V, breakout_j - 20, breakout_j), 1)
                age = n - 1 - breakout_j
                row["volRatio"] = round(vol_ratio, 2)
                row["breakoutAge"] = age
                if (
                    age <= DARVAS_MAX_AGE
                    and price >= box_top
                    and vol_ratio >= DARVAS_VOL_MULT
                    and width_pct <= DARVAS_MAX_WIDTH_PCT
                    and price > ema200
                ):
                    row.update({
                        "boxTop": round(box_top, 2),
                        "boxBottom": round(box_bottom, 2),
                        "boxWidthPct": round(width_pct, 2),
                        "entry": round(box_top, 2),
                        "stop": round(box_bottom, 2),
                        "target1": round(box_top + risk, 2),
                        "target2": round(box_top + 2 * risk, 2),
                        "target3": round(box_top + 3 * risk, 2),
                        "riskPct": round(100 * risk / box_top, 2) if box_top else 0,
                        "signal": "BREAKOUT",
                    })
            elif (
                box_age <= DARVAS_MAX_AGE
                and box_bottom <= price <= box_top
                and width_pct <= DARVAS_MAX_WIDTH_PCT
                and price > ema200
            ):
                row.update({
                    "boxTop": round(box_top, 2),
                    "boxBottom": round(box_bottom, 2),
                    "boxWidthPct": round(width_pct, 2),
                    "entry": round(box_top, 2),
                    "stop": round(box_bottom, 2),
                    "target1": round(box_top + risk, 2),
                    "target2": round(box_top + 2 * risk, 2),
                    "target3": round(box_top + 3 * risk, 2),
                    "riskPct": round(100 * risk / box_top, 2) if box_top else 0,
                    "signal": "INBOX",
                })

        if brk_idx is not None and (brk_top and brk_bottom):
            breakdown_j = None
            for j in range(brk_idx + 1, n):
                if C[j] < brk_bottom:
                    breakdown_j = j
                    break
            width_pct = 100 * (brk_top - brk_bottom) / max(brk_bottom, 0.01)
            risk = max(brk_top - brk_bottom, 0.01)
            if breakdown_j is not None:
                vol_ratio = V[breakdown_j] / max(_slice_mean(V, breakdown_j - 20, breakdown_j), 1)
                age = n - 1 - breakdown_j
                row["volRatio"] = round(vol_ratio, 2)
                row["breakoutAge"] = age
                if (
                    age <= DARVAS_MAX_AGE
                    and price <= brk_bottom
                    and width_pct <= DARVAS_MAX_WIDTH_PCT
                    and price < ema200
                ):
                    row.update({
                        "boxTop": round(brk_top, 2),
                        "boxBottom": round(brk_bottom, 2),
                        "boxWidthPct": round(width_pct, 2),
                        "entry": round(brk_bottom, 2),
                        "stop": round(brk_top, 2),
                        "target1": round(brk_bottom - risk, 2),
                        "target2": round(brk_bottom - 2 * risk, 2),
                        "target3": round(brk_bottom - 3 * risk, 2),
                        "riskPct": round(100 * risk / brk_bottom, 2) if brk_bottom else 0,
                        "signal": "BREAKDOWN",
                    })

        return symbol, row
    except Exception:
        raise


def _darvas_scan_symbols(symbols):
    to_date = date.today()
    from_date = to_date - timedelta(days=700)

    def worker(sym):
        try:
            return _darvas_scan_one(_client(), sym, from_date, to_date)
        except Exception:
            return sym, _FAILED

    results, _failed = _parallel_scan(symbols, worker)
    breakout = sorted(
        (r for r in results.values() if r and r["signal"] == "BREAKOUT"),
        key=lambda r: (r["breakoutAge"] or 0, -(r["volRatio"] or 0)),
    )
    inbox = sorted(
        (r for r in results.values() if r and r["signal"] == "INBOX"),
        key=lambda r: -r["boxWidthPct"],
    )
    breakdown = sorted(
        (r for r in results.values() if r and r["signal"] == "BREAKDOWN"),
        key=lambda r: (r["breakoutAge"] or 0, -(r["volRatio"] or 0)),
    )
    return breakout, inbox, breakdown, len(symbols), len(results)


def _darvas_scan_sector(canonical):
    return _darvas_scan_symbols(list(_sector_live_map(canonical).keys()))


def _darvas_scan_all():
    return _darvas_scan_symbols(list(_all_sector_live_map().keys()))


def _scan_one(client, symbol, from_date, to_date, live):
    try:
        rows = _hist_rows(symbol, from_date, to_date)
        closes = [float(r["chClosingPrice"]) for r in rows if r.get("chClosingPrice") is not None]
        closes = closes[-300:]
        if len(closes) < 110:
            return symbol, None
        macd, hist, ema100 = _macd_indicators(closes)
        ema = ema100[-1]
        if live and live.get("lastPrice"):
            price = float(live["lastPrice"])
            change_pct = live.get("pChange")
            if change_pct is None:
                prev = closes[-2]
                change_pct = round((price - prev) / prev * 100, 2) if prev else 0
        else:
            price = closes[-1]
            prev = closes[-2]
            change_pct = round((price - prev) / prev * 100, 2) if prev else 0
        row = {
            "symbol": symbol,
            "price": round(price, 2),
            "changePct": round(change_pct, 2),
            "macdPrev": round(macd[-2], 4),
            "macdLast": round(macd[-1], 4),
            "histPrev": round(hist[-2], 4) if hist[-2] is not None else None,
            "histLast": round(hist[-1], 4),
            "ema100": round(ema, 2),
            "distPct": round((price - ema) / ema * 100, 2) if ema else 0,
            "rising": bool(hist[-1] > hist[-2]) if hist[-2] is not None else False,
        }
        row["bullish"] = (
            hist[-2] is not None
            and hist[-2] <= 0 < hist[-1]
            and macd[-1] > macd[-2]
            and price > ema * (1 + EMA_TOLERANCE_PCT / 100.0)
        )
        row["bearish"] = (
            hist[-2] is not None
            and hist[-2] >= 0 > hist[-1]
            and macd[-1] < macd[-2]
            and price < ema * (1 - EMA_TOLERANCE_PCT / 100.0)
        )
        return symbol, row
    except Exception:
        raise


def _sector_live_map(canonical):
    api_name = CANONICAL_TO_API.get(canonical)
    if api_name is None:
        raise ValueError("Unknown sector: %s" % canonical)
    payload = _client().listEquityStocksByIndex(api_name)
    return {
        s.get("symbol"): {
            "lastPrice": s.get("lastPrice"),
            "pChange": s.get("pChange"),
        }
        for s in payload.get("data", [])
        if s.get("series") == "EQ" and s.get("symbol")
    }


def _all_sector_live_map():
    payload = _client().listIndices()
    by_name = {d.get("index"): d for d in payload.get("data", [])}
    canonicals = [c for c, _api in SECTOR_INDICES if by_name.get(c) is not None]

    def fetch(c):
        try:
            return _sector_live_map(c)
        except Exception:
            return {}

    live_map = {}
    with ThreadPoolExecutor(max_workers=8) as ex:
        for lm in ex.map(fetch, canonicals):
            for sym, info in lm.items():
                live_map.setdefault(sym, info)
    return live_map


def _scan_symbols(live_map):
    symbols = list(live_map.keys())
    to_date = date.today()
    from_date = to_date - timedelta(days=450)

    def worker(sym):
        try:
            return _scan_one(_client(), sym, from_date, to_date, live_map.get(sym))
        except Exception:
            return sym, _FAILED

    results, _failed = _parallel_scan(symbols, worker)
    bullish = sorted((r for r in results.values() if r["bullish"]), key=lambda r: -r["changePct"])
    bearish = sorted((r for r in results.values() if r["bearish"]), key=lambda r: r["changePct"])
    return bullish, bearish, len(symbols), len(results)


def _scan_sector(canonical):
    return _scan_symbols(_sector_live_map(canonical))


def _scan_all():
    return _scan_symbols(_all_sector_live_map())


def _rsi_scan_one(client, symbol, from_date, to_date, live):
    try:
        rows = _hist_rows(symbol, from_date, to_date)
        closes = [float(r["chClosingPrice"]) for r in rows if r.get("chClosingPrice") is not None]
        closes = closes[-300:]
        if len(closes) < 200:
            return symbol, None
        ema200 = _ema(closes, 200)[-1]
        closes60 = closes[-60:]
        rsi_series = _rsi_series(closes60, 14)
        rsi_now = rsi_series[-1]
        rsi_prev = rsi_series[-2]
        price = closes[-1]
        if live and live.get("lastPrice"):
            price = float(live["lastPrice"])
        prev = closes[-2]
        change_pct = round((price - prev) / prev * 100, 2) if prev else 0
        row = {
            "symbol": symbol,
            "price": round(price, 2),
            "changePct": change_pct,
            "rsi": round(rsi_now, 1),
            "rsiPrev": round(rsi_prev, 1),
            "ema200": round(ema200, 2),
            "distPct": round((price - ema200) / ema200 * 100, 2) if ema200 else 0,
        }
        row["buy"] = bool(60 < rsi_now < 70 and price > ema200)
        row["sell"] = bool(30 < rsi_now < 45 and rsi_now < rsi_prev and price < ema200)
        return symbol, row
    except Exception:
        raise


def _rsi_scan_symbols(live_map):
    symbols = list(live_map.keys())
    to_date = date.today()
    from_date = to_date - timedelta(days=420)

    def worker(sym):
        try:
            return _rsi_scan_one(_client(), sym, from_date, to_date, live_map.get(sym))
        except Exception:
            return sym, _FAILED

    results, _failed = _parallel_scan(symbols, worker)
    buy = sorted((r for r in results.values() if r["buy"]), key=lambda r: -r["changePct"])
    sell = sorted((r for r in results.values() if r["sell"]), key=lambda r: -r["changePct"])
    return buy, sell, len(symbols), len(results)


def _rsi_scan_sector(canonical):
    return _rsi_scan_symbols(_sector_live_map(canonical))


def _rsi_scan_all():
    return _rsi_scan_symbols(_all_sector_live_map())


# Minimum number of 5-minute bars required for a meaningful EMA 200 (~3.5 sessions).
EMA5_MIN_BARS = 300
# Minimum number of 1-minute bars required for a meaningful EMA 200 (~4 sessions).
EMA1_MIN_BARS = 1500


def _ema_between_one(client, symbol, live, rows_fn, min_bars):
    try:
        rows = rows_fn(symbol)
        closes = [float(r["close"]) for r in rows if r.get("close") is not None]
        closes = closes[-2000:]
        n = len(closes)
        if n < min_bars:
            return symbol, None
        ema100s = _ema(closes, 100)
        ema200s = _ema(closes, 200)
        ema100, ema200 = ema100s[-1], ema200s[-1]
        lo, hi = min(ema100, ema200), max(ema100, ema200)
        price = closes[-1]
        prev = closes[-2]
        if live and live.get("lastPrice"):
            price = float(live["lastPrice"])
        change_pct = live.get("pChange") if live else None
        if change_pct is None:
            change_pct = round((price - prev) / prev * 100, 2) if prev else 0
        else:
            change_pct = round(float(change_pct), 2)
        band_width_pct = (hi - lo) / lo * 100 if lo else 0
        pos_pct = (price - lo) / (hi - lo) * 100 if hi > lo else 50
        row = {
            "symbol": symbol,
            "price": round(price, 2),
            "changePct": change_pct,
            "ema100": round(ema100, 2),
            "ema200": round(ema200, 2),
            "bandWidthPct": round(band_width_pct, 2),
            "posPct": round(pos_pct, 1),
            "uptrend": bool(ema100 > ema200),
            "bars": n,
            "signal": bool(lo < price < hi),
        }
        return symbol, row
    except Exception:
        raise


def _ema_between_scan_symbols(live_map, rows_fn, min_bars):
    symbols = list(live_map.keys())

    def worker(sym):
        try:
            return _ema_between_one(_client(), sym, live_map.get(sym), rows_fn, min_bars)
        except Exception:
            return sym, _FAILED

    results, _failed = _parallel_scan(symbols, worker)
    signals = [r for r in results.values() if r["signal"]]
    uptrend = sorted((r for r in signals if r["uptrend"]), key=lambda r: -r["changePct"])
    downtrend = sorted((r for r in signals if not r["uptrend"]), key=lambda r: -r["changePct"])
    return uptrend, downtrend, len(symbols), len(results)


def _ema_between_scan_sector(canonical, rows_fn, min_bars):
    return _ema_between_scan_symbols(_sector_live_map(canonical), rows_fn, min_bars)


def _ema_between_scan_all(rows_fn, min_bars):
    return _ema_between_scan_symbols(_all_sector_live_map(), rows_fn, min_bars)


# Minimum number of 5-minute bars required for a meaningful EMA 100 cross.
EMA_CROSS5_MIN_BARS = 150


def _ema_cross5_one(client, symbol, live, rows_fn):
    try:
        rows = rows_fn(symbol)
        closes = [float(r["close"]) for r in rows if r.get("close") is not None]
        closes = closes[-2000:]
        n = len(closes)
        if n < EMA_CROSS5_MIN_BARS:
            return symbol, None
        ema10s = _ema(closes, 10)
        ema100s = _ema(closes, 100)
        p10, p100 = ema10s[-2], ema100s[-2]
        c10, c100 = ema10s[-1], ema100s[-1]
        if p10 <= p100 and c10 > c100:
            cross = "BULLISH"
        elif p10 >= p100 and c10 < c100:
            cross = "BEARISH"
        else:
            return symbol, None
        price = closes[-1]
        prev = closes[-2]
        if live and live.get("lastPrice"):
            price = float(live["lastPrice"])
        change_pct = live.get("pChange") if live else None
        if change_pct is None:
            change_pct = round((price - prev) / prev * 100, 2) if prev else 0
        else:
            change_pct = round(float(change_pct), 2)
        gap_pct = (c10 - c100) / c100 * 100 if c100 else 0
        row = {
            "symbol": symbol,
            "price": round(price, 2),
            "changePct": change_pct,
            "ema10": round(c10, 2),
            "ema100": round(c100, 2),
            "gapPct": round(gap_pct, 2),
            "cross": cross,
            "bars": n,
        }
        return symbol, row
    except Exception:
        raise


def _ema_cross5_scan_symbols(live_map, rows_fn):
    symbols = list(live_map.keys())

    def worker(sym):
        try:
            return _ema_cross5_one(_client(), sym, live_map.get(sym), rows_fn)
        except Exception:
            return sym, _FAILED

    results, _failed = _parallel_scan(symbols, worker)
    signals = [r for r in results.values() if r]
    bullish = sorted((r for r in signals if r["cross"] == "BULLISH"), key=lambda r: -r["changePct"])
    bearish = sorted((r for r in signals if r["cross"] == "BEARISH"), key=lambda r: -r["changePct"])
    return bullish, bearish, len(symbols), len(results)


def _ema_cross5_scan_sector(canonical, rows_fn):
    return _ema_cross5_scan_symbols(_sector_live_map(canonical), rows_fn)


def _ema_cross5_scan_all(rows_fn):
    return _ema_cross5_scan_symbols(_all_sector_live_map(), rows_fn)


# Minimum number of 1-minute bars required for a meaningful EMA 200.
EMA_SQZ_MIN_BARS = 300
# Max % spread between the EMAs for a squeeze.
EMA_SQZ_PCT = 0.5
# Bars back used to measure the fast-EMA slope.
EMA_SQZ_SLOPE_BARS = 3


def _ema_sqz_one(client, symbol, live, rows_fn):
    try:
        rows = rows_fn(symbol)
        closes = [float(r["close"]) for r in rows if r.get("close") is not None]
        closes = closes[-2000:]
        n = len(closes)
        if n < EMA_SQZ_MIN_BARS or n < EMA_SQZ_SLOPE_BARS + 2:
            return symbol, None
        ema20s = _ema(closes, 20)
        ema50s = _ema(closes, 50)
        ema100s = _ema(closes, 100)
        ema200s = _ema(closes, 200)
        c20, c50, c100, c200 = ema20s[-1], ema50s[-1], ema100s[-1], ema200s[-1]
        if not (c20 > 0 and c50 > 0 and c100 > 0 and c200 > 0):
            return symbol, None
        spread_pct = (max(c20, c50, c100, c200) - min(c20, c50, c100, c200)) / min(c20, c50, c100, c200) * 100
        if spread_pct > EMA_SQZ_PCT:
            return symbol, None
        slope_up = ema20s[-1] > ema20s[-1 - EMA_SQZ_SLOPE_BARS]
        slope_down = ema20s[-1] < ema20s[-1 - EMA_SQZ_SLOPE_BARS]
        if not slope_up and not slope_down:
            return symbol, None
        signal = "BUY" if slope_up else "SELL"
        price = closes[-1]
        prev = closes[-2]
        if live and live.get("lastPrice"):
            price = float(live["lastPrice"])
        change_pct = live.get("pChange") if live else None
        if change_pct is None:
            change_pct = round((price - prev) / prev * 100, 2) if prev else 0
        else:
            change_pct = round(float(change_pct), 2)
        row = {
            "symbol": symbol,
            "price": round(price, 2),
            "changePct": change_pct,
            "ema20": round(c20, 2),
            "ema50": round(c50, 2),
            "ema100": round(c100, 2),
            "ema200": round(c200, 2),
            "spreadPct": round(spread_pct, 2),
            "slope": "UP" if slope_up else "DOWN",
            "signal": signal,
            "bars": n,
        }
        return symbol, row
    except Exception:
        raise


def _ema_sqz_scan_symbols(live_map, rows_fn):
    symbols = list(live_map.keys())

    def worker(sym):
        try:
            return _ema_sqz_one(_client(), sym, live_map.get(sym), rows_fn)
        except Exception:
            return sym, _FAILED

    results, _failed = _parallel_scan(symbols, worker)
    signals = [r for r in results.values() if r]
    buy = sorted((r for r in signals if r["signal"] == "BUY"), key=lambda r: -r["changePct"])
    sell = sorted((r for r in signals if r["signal"] == "SELL"), key=lambda r: r["changePct"])
    return buy, sell, len(symbols), len(results)


def _ema_sqz_scan_sector(canonical, rows_fn):
    return _ema_sqz_scan_symbols(_sector_live_map(canonical), rows_fn)


def _ema_sqz_scan_all(rows_fn):
    return _ema_sqz_scan_symbols(_all_sector_live_map(), rows_fn)


def _all_indices():
    payload = _client().listIndices()
    by_name = {d.get("index"): d for d in payload.get("data", [])}
    sectors = []
    for canonical, _api in SECTOR_INDICES:
        d = by_name.get(canonical)
        if d is None:
            continue
        sectors.append({
            "name": canonical,
            "last": d.get("last"),
            "change": d.get("variation"),
            "pChange": d.get("percentChange"),
            "open": d.get("open"),
            "high": d.get("high"),
            "low": d.get("low"),
        })
    sectors.sort(key=lambda s: (s["pChange"] is None, -(s["pChange"] or 0)))
    return sectors, payload.get("timestamp"), payload.get("marketStatus")


def _sector_stocks(canonical):
    api_name = CANONICAL_TO_API.get(canonical)
    if api_name is None:
        raise ValueError("Unknown sector: %s" % canonical)
    payload = _client().listEquityStocksByIndex(api_name)
    stocks = []
    for s in payload.get("data", []):
        if s.get("series") != "EQ":
            continue
        stocks.append({
            "symbol": s.get("symbol"),
            "price": s.get("lastPrice"),
            "change": s.get("change"),
            "pChange": s.get("pChange"),
            "open": s.get("open"),
            "high": s.get("dayHigh"),
            "low": s.get("dayLow"),
            "prevClose": s.get("previousClose"),
            "volume": s.get("totalTradedVolume"),
            "turnover": s.get("totalTradedValue"),
        })
    gainers = sorted(stocks, key=lambda x: (x["pChange"] is None, -(x["pChange"] or 0)))
    losers = sorted(stocks, key=lambda x: (x["pChange"] is None, x["pChange"] or 0))
    return stocks, gainers[:8], losers[:8], _mk_status(payload.get("marketStatus")), payload.get("timestamp")


def _strip_html(raw):
    return re.sub(r"<[^>]+>", "", html_mod.unescape(raw or "")).strip()


def _fetch_news(feed):
    resp = requests.get(feed, headers=_NEWS_HEADERS, timeout=15)
    resp.raise_for_status()
    root = ET.fromstring(resp.content)
    items = []
    for item in root.iter("item"):
        src_el = item.find("source")
        items.append({
            "title": html_mod.unescape(item.findtext("title") or "").strip(),
            "link": (item.findtext("link") or "").strip(),
            "source": html_mod.unescape(src_el.text or "").strip() if src_el is not None else "",
            "published": (item.findtext("pubDate") or "").strip(),
            "summary": _strip_html(item.findtext("description")),
        })
    return [i for i in items if i["title"] and i["link"]][:20]


def _fetch_fx_calendar():
    resp = requests.get(FX_CALENDAR_URL, headers=_NEWS_HEADERS, timeout=15)
    resp.raise_for_status()
    events = []
    for e in resp.json():
        events.append({
            "title": (e.get("title") or "").strip(),
            "country": (e.get("country") or "").strip(),
            "date": (e.get("date") or "").strip(),
            "impact": (e.get("impact") or "").strip(),
            "forecast": (e.get("forecast") or "").strip(),
            "previous": (e.get("previous") or "").strip(),
        })
    events.sort(key=lambda x: x["date"])
    return events


@app.route("/")
def index():
    return render_template("index.html")


@app.route("/api/sectors")
def api_sectors():
    global _last_market_status
    try:
        sectors, ts, ms = _cached("sectors", 10, _all_indices)
        st = _mk_status(ms) or _fallback_market_status()
        if st:
            _last_market_status = st
        return jsonify({
            "ok": True,
            "marketStatus": _last_market_status,
            "timestamp": ts,
            "sectors": sectors,
        })
    except Exception as e:
        return jsonify({"ok": False, "error": "NSE error: %s" % e}), 502


@app.route("/api/sector")
def api_sector():
    global _last_market_status
    name = request.args.get("name", "").strip()
    if name not in CANONICAL_TO_API:
        return jsonify({"ok": False, "error": "Unknown sector"}), 400
    try:
        stocks, gainers, losers, ms, ts = _cached(
            "sector:" + name, 15, lambda: _sector_stocks(name)
        )
        if ms:
            _last_market_status = ms
        return jsonify({
            "ok": True,
            "sector": name,
            "marketStatus": ms or _fallback_market_status(),
            "timestamp": ts,
            "stocks": stocks,
            "gainers": gainers,
            "losers": losers,
        })
    except Exception as e:
        return jsonify({"ok": False, "error": "NSE error: %s" % e}), 502


@app.route("/api/history")
def api_history():
    symbol = request.args.get("symbol", "").strip().upper()
    days_raw = request.args.get("days", "90")
    try:
        days = min(max(int(days_raw), 5), 365)
    except ValueError:
        days = 90
    if not symbol:
        return jsonify({"ok": False, "error": "Missing symbol"}), 400
    try:
        to_date = date.today()
        from_date = to_date - timedelta(days=days)
        rows = _cached(
            "hist:" + symbol + ":" + str(days),
            300,
            lambda: _client().fetch_equity_historical_data(
                symbol, from_date, to_date
            ),
        )
        candles = [
            {
                "date": r.get("mtimestamp"),
                "open": r.get("chOpeningPrice"),
                "high": r.get("chTradeHighPrice"),
                "low": r.get("chTradeLowPrice"),
                "close": r.get("chClosingPrice"),
                "volume": r.get("chTotTradedQty"),
            }
            for r in rows
        ]
        return jsonify({"ok": True, "symbol": symbol, "candles": candles})
    except Exception as e:
        return jsonify({"ok": False, "error": "NSE error: %s" % e}), 502


@app.route("/api/news")
def api_news():
    category = request.args.get("category", "india").strip().lower()
    if category not in NEWS_FEEDS:
        return jsonify({"ok": False, "error": "Unknown news category"}), 400
    try:
        items = _cached("news:" + category, 60, lambda: _fetch_news(NEWS_FEEDS[category]))
        return jsonify({"ok": True, "category": category, "items": items})
    except Exception as e:
        return jsonify({"ok": False, "error": "News error: %s" % e}), 502


@app.route("/api/calendar")
def api_calendar():
    try:
        events = _cached("fx:calendar", 600, _fetch_fx_calendar)
        return jsonify({"ok": True, "events": events})
    except Exception as e:
        return jsonify({"ok": False, "error": "Calendar error: %s" % e}), 502


@app.route("/api/flash")
def api_flash():
    try:
        items = _cached("flash", 30, lambda: _fetch_news(FLASH_FEED))
        return jsonify({"ok": True, "items": items})
    except Exception as e:
        return jsonify({"ok": False, "error": "News error: %s" % e}), 502


@app.route("/api/scan")
def api_scan():
    name = request.args.get("name", "").strip()
    if name != "ALL" and name not in CANONICAL_TO_API:
        return jsonify({"ok": False, "error": "Unknown sector"}), 400
    try:
        start = time.time()
        if name == "ALL":
            bullish, bearish, total, scanned = _cached(
                "scan:ALL", 900, _scan_all
            )
        else:
            bullish, bearish, total, scanned = _cached(
                "scan:" + name, 900, lambda: _scan_sector(name)
            )
        return jsonify({
            "ok": True,
            "sector": name,
            "bullish": bullish,
            "bearish": bearish,
            "total": total,
            "scanned": scanned,
            "elapsed": round(time.time() - start, 1),
        })
    except Exception as e:
        return jsonify({"ok": False, "error": "Scan error: %s" % e}), 502


@app.route("/api/breakout")
def api_breakout():
    name = request.args.get("name", "").strip()
    if name != "ALL" and name not in CANONICAL_TO_API:
        return jsonify({"ok": False, "error": "Unknown sector"}), 400
    try:
        start = time.time()
        if name == "ALL":
            signals, total, scanned = _cached(
                "breakout:ALL", 900, _breakout_scan_all
            )
        else:
            signals, total, scanned = _cached(
                "breakout:" + name, 900, lambda: _breakout_scan_sector(name)
            )
        return jsonify({
            "ok": True,
            "sector": name,
            "signals": signals,
            "total": total,
            "scanned": scanned,
            "elapsed": round(time.time() - start, 1),
        })
    except Exception as e:
        return jsonify({"ok": False, "error": "Scan error: %s" % e}), 502


@app.route("/api/darvas")
def api_darvas():
    name = request.args.get("name", "").strip()
    if name != "ALL" and name not in CANONICAL_TO_API:
        return jsonify({"ok": False, "error": "Unknown sector"}), 400
    try:
        start = time.time()
        if name == "ALL":
            breakout, inbox, breakdown, total, scanned = _cached(
                "darvas:ALL", 900, _darvas_scan_all
            )
        else:
            breakout, inbox, breakdown, total, scanned = _cached(
                "darvas:" + name, 900, lambda: _darvas_scan_sector(name)
            )
        return jsonify({
            "ok": True,
            "sector": name,
            "breakout": breakout,
            "breakdown": breakdown,
            "inbox": inbox,
            "total": total,
            "scanned": scanned,
            "elapsed": round(time.time() - start, 1),
        })
    except Exception as e:
        return jsonify({"ok": False, "error": "Scan error: %s" % e}), 502


@app.route("/api/rsiscan")
def api_rsiscan():
    name = request.args.get("name", "").strip()
    if name != "ALL" and name not in CANONICAL_TO_API:
        return jsonify({"ok": False, "error": "Unknown sector"}), 400
    try:
        start = time.time()
        if name == "ALL":
            buy, sell, total, scanned = _cached(
                "rsi:ALL", 900, _rsi_scan_all
            )
        else:
            buy, sell, total, scanned = _cached(
                "rsi:" + name, 900, lambda: _rsi_scan_sector(name)
            )
        return jsonify({
            "ok": True,
            "sector": name,
            "buy": buy,
            "sell": sell,
            "total": total,
            "scanned": scanned,
            "elapsed": round(time.time() - start, 1),
        })
    except Exception as e:
        return jsonify({"ok": False, "error": "Scan error: %s" % e}), 502


@app.route("/api/ema5scan")
def api_ema5scan():
    name = request.args.get("name", "").strip()
    if name != "ALL" and name not in CANONICAL_TO_API:
        return jsonify({"ok": False, "error": "Unknown sector"}), 400
    try:
        start = time.time()
        if name == "ALL":
            uptrend, downtrend, total, scanned = _cached(
                "ema5:ALL", 300, lambda: _ema_between_scan_all(_intraday_rows_5, EMA5_MIN_BARS)
            )
        else:
            uptrend, downtrend, total, scanned = _cached(
                "ema5:" + name, 300, lambda: _ema_between_scan_sector(name, _intraday_rows_5, EMA5_MIN_BARS)
            )
        return jsonify({
            "ok": True,
            "sector": name,
            "uptrend": uptrend,
            "downtrend": downtrend,
            "total": total,
            "scanned": scanned,
            "elapsed": round(time.time() - start, 1),
        })
    except Exception as e:
        return jsonify({"ok": False, "error": "Scan error: %s" % e}), 502


@app.route("/api/ema1scan")
def api_ema1scan():
    name = request.args.get("name", "").strip()
    if name != "ALL" and name not in CANONICAL_TO_API:
        return jsonify({"ok": False, "error": "Unknown sector"}), 400
    try:
        start = time.time()
        if name == "ALL":
            uptrend, downtrend, total, scanned = _cached(
                "ema1:ALL", 300, lambda: _ema_between_scan_all(_intraday_rows_1, EMA1_MIN_BARS)
            )
        else:
            uptrend, downtrend, total, scanned = _cached(
                "ema1:" + name, 300, lambda: _ema_between_scan_sector(name, _intraday_rows_1, EMA1_MIN_BARS)
            )
        return jsonify({
            "ok": True,
            "sector": name,
            "uptrend": uptrend,
            "downtrend": downtrend,
            "total": total,
            "scanned": scanned,
            "elapsed": round(time.time() - start, 1),
        })
    except Exception as e:
        return jsonify({"ok": False, "error": "Scan error: %s" % e}), 502


@app.route("/api/emacross5scan")
def api_emacross5scan():
    name = request.args.get("name", "").strip()
    if name != "ALL" and name not in CANONICAL_TO_API:
        return jsonify({"ok": False, "error": "Unknown sector"}), 400
    try:
        start = time.time()
        if name == "ALL":
            bullish, bearish, total, scanned = _cached(
                "emacross5:ALL", 300, lambda: _ema_cross5_scan_all(_intraday_rows_5)
            )
        else:
            bullish, bearish, total, scanned = _cached(
                "emacross5:" + name, 300, lambda: _ema_cross5_scan_sector(name, _intraday_rows_5)
            )
        return jsonify({
            "ok": True,
            "sector": name,
            "bullish": bullish,
            "bearish": bearish,
            "total": total,
            "scanned": scanned,
            "elapsed": round(time.time() - start, 1),
        })
    except Exception as e:
        return jsonify({"ok": False, "error": "Scan error: %s" % e}), 502


@app.route("/api/emasqz1scan")
def api_emasqz1scan():
    name = request.args.get("name", "").strip()
    if name != "ALL" and name not in CANONICAL_TO_API:
        return jsonify({"ok": False, "error": "Unknown sector"}), 400
    try:
        start = time.time()
        if name == "ALL":
            buy, sell, total, scanned = _cached(
                "emasqz1:ALL", 300, lambda: _ema_sqz_scan_all(_intraday_rows_1)
            )
        else:
            buy, sell, total, scanned = _cached(
                "emasqz1:" + name, 300, lambda: _ema_sqz_scan_sector(name, _intraday_rows_1)
            )
        return jsonify({
            "ok": True,
            "sector": name,
            "buy": buy,
            "sell": sell,
            "total": total,
            "scanned": scanned,
            "elapsed": round(time.time() - start, 1),
        })
    except Exception as e:
        return jsonify({"ok": False, "error": "Scan error: %s" % e}), 502


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5052))
    app.run(host="0.0.0.0", port=port, debug=False, threaded=True)
