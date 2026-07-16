"""
Solana Wallet Convergence Alert Bot  — SpiderWalletBot
=======================================================
- 50 wallets (static fallback list, no discovery)
- Helius enhanced webhook for SWAP events (one webhook, PUT to update)
- Signature deduplicationc: TTLCache(maxsize=10000, ttl=3600) — eliminates ~95% duplicate alerts
- Price source: DexScreener only (free, no key, price + mcap + 5m data)
- Price cache: 5-min TTL with stampede protection
- Symbol resolution: Helius field → Jupiter token list → DexScreener
- Wallet ranking: weighted score (win_rate * log(trades) * roi_factor) — elite wallets > weak wallets
- Buy alert: weighted score ≥ threshold within 2-min window + mcap gate (<500k)
- Confidence score: wallet quality + convergence speed + liquidity + rug risk → Final Grade A-F
- Rug check: LP burned, mint authority, freeze authority via RugCheck.xyz API
- AI signal grading: multi-factor → Final Grade A+ / A / B / C / D
- Personal trade assistant: entry price, TP1 (30%), TP2 (60%), stop loss (15%)
- Exit alert: ONE alert when the same wallets that triggered buy start selling (no progressive tracking)
- MIN_HOLD_TIME: sell alert blocked 120s after buy alert fires
- Adaptive threshold: 4 (quiet) / 5 (normal) / 6 (active)
- Daily report: top wallets by wins at midnight
- ThreadPoolExecutor (8 workers) — webhook returns 200 immediately, processes async
- Trojan on Solana link (@solana_trojanbot + ref)
- Visual card with PIL, safe HTML captions (text fallback if PIL fails)
"""

import os, time, logging, math, requests, threading, io, importlib, pytz
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from flask import Flask, request, jsonify
from telegram import Bot, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.error import TelegramError
from cachetools import TTLCache, LRUCache
from PIL import Image, ImageDraw, ImageFont

try:
    _apscheduler_mod = importlib.import_module("apscheduler.schedulers.background")
    BackgroundScheduler = _apscheduler_mod.BackgroundScheduler
except ImportError:
    BackgroundScheduler = None

import wallet_intelligence as wi

# ═══════════════════════════════════════════════════════════════════════════════
#  CONFIG
# ═══════════════════════════════════════════════════════════════════════════════

# ═══════════════════════════════════════════════════════════════════════════════
#  CONFIG  (all secrets/environment-specific values come from env vars — see
#  .env.example for the full list. Nothing sensitive is hardcoded here.)
# ═══════════════════════════════════════════════════════════════════════════════

def _require_env(name: str) -> str:
    value = os.environ.get(name, "").strip()
    if not value:
        raise SystemExit(
            f"❌ Missing required environment variable: {name}\n"
            f"   Set it in your Render service's Environment tab (or a local .env "
            f"file) — see .env.example for the full list of variables this bot needs."
        )
    return value


TELEGRAM_TOKEN  = _require_env("TELEGRAM_TOKEN")
CHAT_ID         = _require_env("CHAT_ID")
HELIUS_API_KEY  = _require_env("HELIUS_API_KEY")
WEBHOOK_SECRET  = _require_env("WEBHOOK_SECRET")
TROJAN_REF_CODE = os.environ.get("TROJAN_REF_CODE", "").strip()   # optional — affiliate link only

# ── Public URL this service is reachable at (Helius needs this to POST webhooks) ──
# Priority: explicit APP_URL  >  Render's auto-injected RENDER_EXTERNAL_URL
#         > NGROK_SUBDOMAIN (local dev tunnel)  >  fail with a clear error.
NGROK_SUBDOMAIN = os.environ.get("NGROK_SUBDOMAIN", "").strip()
_render_url     = os.environ.get("RENDER_EXTERNAL_URL", "").strip()

if os.environ.get("APP_URL", "").strip():
    APP_URL = os.environ["APP_URL"].strip().rstrip("/")
elif _render_url:
    APP_URL = _render_url.rstrip("/")
elif NGROK_SUBDOMAIN:
    APP_URL = f"https://{NGROK_SUBDOMAIN}.ngrok-free.app"
else:
    raise SystemExit(
        "❌ Could not determine a public APP_URL.\n"
        "   On Render this should be set automatically via RENDER_EXTERNAL_URL — "
        "if you're seeing this on Render, set APP_URL manually to your service's "
        "onrender.com URL. For local dev, set NGROK_SUBDOMAIN or APP_URL."
    )

# Alert tuning
THRESHOLD        = 5      # base wallet count threshold (adaptive adjusts ±1)
WINDOW           = 180    # seconds — buy convergence window (2 minutes)
ALERT_COOLDOWN   = 600    # seconds — suppress repeat buy alerts per token
SELL_WINDOW      = 86400    # seconds — sell convergence window
MIN_HOLD_TIME    = 300    # seconds — minimum time after buy alert before sell alert fires
FAST_DUMP_MIN_SELLERS = 3  # if this many original buy wallets sell together, alert bypasses MIN_HOLD_TIME
MIN_MCAP         = 50_000  # Ignore ultra-low-cap tokens to reduce scam and noise
MAX_MCAP         = 5_000_000  # USD — skip tokens already above this market cap
REFRESH_HOURS    = 720    # 30 days — one webhook registration per month

# Wallet ranking weights
MIN_WALLET_SCORE = 0.6    # wallets below this score are ignored in weighted sum
WEIGHTED_TRIGGER = 5.5    # total weighted score needed to fire alert
MIN_LIQUIDITY = 50_000     # minimum liquidity needed to fire alert
MIN_BUY_SOL = 1.0         # minimum buy amount in SOL
MIN_ELITE_WALLETS = 1         # minimum number of elite wallets to fire alert
MIN_AI_GRADE = "B+"          # minimum AI grade to fire alert (A+ > A > B > C > D)

# Wrapped SOL mint — excluded from token transfer detection
WSOL_MINT = "So11111111111111111111111111111111111111112"

FALLBACK_WALLETS = [
    "Bgokg3jutarxEMWQVospwUucSQfpG6Jw27jRbMxcvU2q",
    "28YSwogXw2JdKLJ8AgK2nWy6k39jpB7hqrhe1AV18QpD",
    "498SWfPJisr26J4oCiZccyzReFrByNE7jsHwbm3caNma",
    "CyaE1VxvBrahnPWkqm5VsdCvyS2QmNht2UFrKJHga54o",
    "97fVD4SLcrcTr16kdgTS9Gq5kJFaP3N2HXAEm1PJRKqv",
    "3KvsoNxgn64nsuHKPBHQJsguef3DgEkP2izE49k6CSAZ",
    "Bi4rd5FH5bYEN8scZ7wevxNZyNmKHdaBcvewdPFxYdLt",
    "HxjwdF326ZunmUwC1iXhfgL3ku78YsksN6n7Rfxzwr6b",
    "2QfBNK2WDwSLoUQRb1zAnp3KM12N9hQ8q6ApwUMnWW2T",
    "gtagyESa99t49VmUqnnfsuowYnigSNKuYXdXWyXWNdd",
    "JD6rVaerbyz6wjQ433nrw6bFTgFrp46MiYmi8EtUAfsG",
    "5gn3uxhsZ7TtLDZwxKXPJuUTB9dEMgnb3oFJ6rKDjoX4",
    "Gghj6515zeefxS2Dv7vwSSGyWqtASJFojuLwVMFsc6FN",
    "2EYVKHYQKC7goT3eB3iCYPp8gKsPVj6QMyUzy1oQay6a",
    "AQ3MK4mf4i4r3G9rkbAvfoxGP6eZ7yscuiy5Syyuq27U",
    "A7FMMgue4aZmPLLoutVtbC7gJcyqkHybUieiaDg9aaVE",
    "GNoYNXQ66dnTqcR39nKi2QJSizjxvHHAy9GSbNszQuuq",
    "roUteHjDohtkatXTb79PJ99bbxkTipgo3GJ4EJZ1YpB",
    "GxDC9e7SP9mzhDo4re5HbpLa2RW7gB9DtmThx4i4pXSq",
    "DsCJ5siuJTPQtQa3A9N69azGZaWtUPzi9VPp2G9Jfpx9",
    "UUAhspPgUdGuXUnokmxERH1VvNGNh1ouN3mfcbfV8yd",
    "eLLnsiBsWvERB34kgiJ4wPhdRzqM17gMG4cHouMqaHz",
    "8pY1AukbuPgUE3EetyLa59rFLMimJGT94ZzbMEZcQF4w",
    "77eg8ZALn2CuEs2ErACpBsuzcG5nRoDkK2eyrGQNfxD2",
    "3xBmQQijfUghKXmnvjUKFEwrobxV2gmz2mYvFPoLoG4C",
    "Ft6fZtTtL5EJANevdXwcSvD4Lum8QMTMepF9zjvFPh2X",
    "CQwT1byuHgjKnL6vzmuNaAywKfDBVxDmFVgsQDBWxcWt",
    "yUwUyoufLrCmjcgURefVzvAfpcaZ2so6be4uDziT9aH",
    "8NQ32SyFKD1d5kenq4oM8Da6C6J9TQSMW1uAgFRveEQr",
    "pau23UpU2BFwF4JZrLxAnf4ZqgnD3xLnz6ESu7vPsao",
    "EDBvw6czdnJMWP1ZXRTrAArE4ha1FoND2E34cDKHtV3J",
    "54qjvmfmUkcfsQm6aJURegHPcvB2QjY8z2w6ZkFx2cjc",
    "EciGzv86MdB6zLHkoLPLiAyU37BQmpJyY7yQb7SPG9zS",
    "6qudAN2kV8mtCcYJxb5QQ6Vr15itdHHdeVbYm99NKMhy",
    "4iaJQWCdr9iBqh2DUDVhaf5DeLi1mZBZLHanvbTLGFbv",
]
# ═══════════════════════════════════════════════════════════════════════════════
#  INTERNALS
# ═══════════════════════════════════════════════════════════════════════════════

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

app       = Flask(__name__)
bot       = Bot(token=TELEGRAM_TOKEN)
scheduler = (
    BackgroundScheduler(timezone=pytz.timezone("Africa/Lagos"))
    if BackgroundScheduler else None
)
_tx_executor = ThreadPoolExecutor(max_workers=8, thread_name_prefix="tx_worker")

# Activity caches — always access under activity_lock
recent_activity = TTLCache(maxsize=1000, ttl=WINDOW * 2)
alerted_tokens  = TTLCache(maxsize=1000, ttl=ALERT_COOLDOWN)
sell_activity   = TTLCache(maxsize=1000, ttl=SELL_WINDOW * 2)
sell_alerted    = TTLCache(maxsize=1000, ttl=ALERT_COOLDOWN)
activity_lock   = threading.Lock()

# ── 1. SIGNATURE DEDUPLICATION ────────────────────────────────────────────────
# TTL=3600s — eliminates ~95% of duplicate alerts from Helius retries
# Larger maxsize and longer TTL than previous version for better coverage
processed_signatures = TTLCache(maxsize=10000, ttl=3600)
sig_lock             = threading.Lock()

# Buy alert records — {mint: {"wallets": frozenset, "ts": int}}
buy_alert_wallets = TTLCache(maxsize=1000, ttl=86400)
buy_alert_lock    = threading.Lock()

# Position tracking
wallet_positions = TTLCache(maxsize=5000, ttl=86400)
position_lock    = threading.Lock()

# Adaptive threshold — track alert frequency over last hour
_alert_times      = []
_alert_times_lock = threading.Lock()

# Daily win tracking — {wallet: {"wins": int, "last_reset": date_str}}
wallet_wins      = {}
wallet_wins_lock = threading.Lock()

watched_wallets    = []
wallet_lock        = threading.Lock()
current_webhook_id = ""

HELIUS_BASE          = "https://api.helius.xyz/v0"
_card_template_cache = LRUCache(maxsize=4)

# Jupiter token list — loaded once at startup
_jupiter_token_map  = {}
_jupiter_map_lock   = threading.Lock()
_jupiter_map_loaded = False

# Price cache + stampede protection
price_cache       = TTLCache(maxsize=5000, ttl=300)
price_cache_lock  = threading.Lock()
_price_fetching   = set()
_price_fetch_event = {}

# Rug check cache — TTL 10 min (rug status rarely changes in seconds)
rug_cache      = TTLCache(maxsize=2000, ttl=600)
rug_cache_lock = threading.Lock()


# ═══════════════════════════════════════════════════════════════════════════════
#  2. WALLET RANKING SYSTEM
# ═══════════════════════════════════════════════════════════════════════════════

# Persistent wallet stats — updated every time a buy/sell cycle completes
# {wallet: {"wins": int, "losses": int, "total_roi": float, "trades": int, "hold_times": [int]}}
wallet_stats      = {}
wallet_stats_lock = threading.Lock()


def _get_wallet_score(wallet: str) -> float:
    """
    Wallet score, 0–2.5 scale (compatible with the legacy weighted-vote
    thresholds). This now comes from the Wallet Confidence Score system —
    a combined score across ROI, hit rate, early-entry timing, leader
    influence, and outcome consistency (see wallet_intelligence.py) —
    rather than the old win_rate*volume*roi formula alone.

    New wallets with no trade history yet still get a real (non-1.0-flat)
    score, since early-entry and leader signal can be known before a
    wallet's first recorded exit.
    """
    try:
        return wi.get_wallet_confidence_score(wallet)
    except Exception as e:
        logger.debug("Confidence score unavailable for %s, using legacy fallback: %s", wallet, e)
        return _get_legacy_wallet_score(wallet)


def _get_legacy_wallet_score(wallet: str) -> float:
    """
    Original formula, kept as a fallback if the intelligence DB is ever
    unreachable. Formula:
      base    = win_rate (0–1)
      volume  = log1p(trades) normalised to 0–1 across all wallets
      roi_fac = 1 + (avg_roi / 100)  [capped at 2.5]
      score   = base * volume_factor * roi_factor
    """
    with wallet_stats_lock:
        stats = wallet_stats.get(wallet)
    if not stats or stats["trades"] < 3:
        return 1.0   # insufficient data — neutral

    wins     = stats["wins"]
    trades   = stats["trades"]
    win_rate = wins / trades
    avg_roi  = stats["total_roi"] / trades if trades else 0
    roi_fac  = min(1 + (avg_roi / 100), 2.5)
    vol_fac  = min(math.log1p(trades) / math.log1p(50), 1.0)   # normalise at 50 trades
    return round(win_rate * vol_fac * roi_fac, 3)


def _get_wallet_quality_score(wallets: set) -> float:
    """
    Returns 0–100 quality score for the SET of buying wallets.
    Elite wallets contribute more. Used in AI signal grade.
    """
    if not wallets:
        return 50.0
    scores = [_get_wallet_score(w) for w in wallets]
    avg    = sum(scores) / len(scores)
    # Normalise: score of 2.5 = 100, score 0 = 0
    return min(round((avg / 2.5) * 100, 1), 100.0)


def _get_weighted_vote(wallets: set) -> float:
    """Sum of wallet scores — used instead of raw count for alert triggering.
    Wallets scoring below MIN_WALLET_SCORE are excluded as unreliable.
    """
    return sum(
        score for w in wallets
        if (score := _get_wallet_score(w)) >= MIN_WALLET_SCORE
    )


def _record_trade_outcome(wallet: str, win: bool, roi_pct: float, hold_seconds: int):
    """Update wallet stats after a confirmed exit."""
    with wallet_stats_lock:
        s = wallet_stats.get(wallet, {"wins": 0, "losses": 0, "total_roi": 0.0,
                                       "trades": 0, "hold_times": []})
        s["trades"]    += 1
        s["total_roi"] += roi_pct
        s["hold_times"].append(hold_seconds)
        if win:
            s["wins"] += 1
        else:
            s["losses"] += 1
        # Keep hold_times bounded
        if len(s["hold_times"]) > 200:
            s["hold_times"] = s["hold_times"][-200:]
        wallet_stats[wallet] = s


def _get_wallet_stats_display(wallet: str) -> str:
    """Short stats string for daily report."""
    with wallet_stats_lock:
        s = wallet_stats.get(wallet)
    if not s or s["trades"] < 1:
        return "no data"
    wr  = round((s["wins"] / s["trades"]) * 100)
    roi = round(s["total_roi"] / s["trades"], 1)
    avg_hold = int(sum(s["hold_times"]) / len(s["hold_times"])) if s["hold_times"] else 0
    mins = avg_hold // 60
    return f"{s['wins']}W/{s['losses']}L  WR:{wr}%  ROI:{roi:+.1f}%  Hold:{mins}m"


def _record_win(wallet: str):
    from datetime import date
    today = str(date.today())
    with wallet_wins_lock:
        rec = wallet_wins.get(wallet, {"wins": 0, "last_reset": today})
        if rec["last_reset"] != today:
            rec = {"wins": 0, "last_reset": today}
        rec["wins"] += 1
        wallet_wins[wallet] = rec


# ═══════════════════════════════════════════════════════════════════════════════
#  ADAPTIVE THRESHOLD
# ═══════════════════════════════════════════════════════════════════════════════

def _get_adaptive_threshold() -> int:
    now = time.time()

    with _alert_times_lock:
        recent = [t for t in _alert_times if now - t <= 3600]
        _alert_times.clear()
        _alert_times.extend(recent)

    count = len(recent)

    if count < 6:
        return 5      # quiet market

    if count > 15:
        return 6      # very busy market

    return 5          # normal market


# ═══════════════════════════════════════════════════════════════════════════════
#  SYMBOL RESOLUTION
# ═══════════════════════════════════════════════════════════════════════════════

def _load_jupiter_token_map():
    global _jupiter_token_map, _jupiter_map_loaded
    with _jupiter_map_lock:
        if _jupiter_map_loaded:
            return
        try:
            r = requests.get("https://token.jup.ag/all", timeout=15)
            r.raise_for_status()
            tokens = r.json()
            _jupiter_token_map = {
                t["address"]: t["symbol"]
                for t in tokens if t.get("address") and t.get("symbol")
            }
            logger.info("🗂 Jupiter token list loaded — %d tokens", len(_jupiter_token_map))
        except Exception as e:
            logger.warning("Jupiter token list failed: %s", e)
        _jupiter_map_loaded = True


def _resolve_symbol(mint: str, helius_symbol) -> str:
    if helius_symbol and str(helius_symbol).strip():
        return str(helius_symbol).strip()
    with _jupiter_map_lock:
        if mint in _jupiter_token_map:
            return _jupiter_token_map[mint]
    try:
        r = requests.get(
            f"https://api.dexscreener.com/latest/dex/tokens/{mint}", timeout=5)
        r.raise_for_status()
        pairs = r.json().get("pairs", [])
        if pairs:
            best = max(pairs, key=lambda x: float(x.get("liquidity", {}).get("usd") or 0))
            sym  = best.get("baseToken", {}).get("symbol", "")
            if sym:
                with _jupiter_map_lock:
                    _jupiter_token_map[mint] = sym
                return sym
    except Exception as e:
        logger.debug("DexScreener symbol lookup failed %s: %s", mint, e)
    return mint[:8] + "…"


# ═══════════════════════════════════════════════════════════════════════════════
#  3. RUG CHECK
# ═══════════════════════════════════════════════════════════════════════════════

def _check_rug_risk(mint: str) -> dict:
    """
    Fetches rug safety data from RugCheck.xyz (free, no key required).
    Returns dict with risk level, flags, and a 0-100 safety score.
    """
    with rug_cache_lock:
        if mint in rug_cache:
            return rug_cache[mint]

    result = {
        "risk_level": "Unknown",
        "risk_emoji": "⚪",
        "safety_score": 50,
        "lp_burned": None,
        "mint_disabled": None,
        "freeze_disabled": None,
        "flags": [],
    }

    try:
        r = requests.get(
            f"https://api.rugcheck.xyz/v1/tokens/{mint}/report/summary",
            timeout=8,
        )
        r.raise_for_status()

        data = r.json()

        logger.info("RugCheck response for %s: %s", mint, data)
        logger.info("RugCheck raw response: %s", data)
        logger.info("Available RugCheck keys: %s", list(data.keys()))

        score = int(data.get("score_normalised", 50))

        risks = data.get("risks", [])
        flags = [
            risk.get("name", "")
            for risk in risks
            if risk.get("level") in ("warn", "danger")
        ]

        lp_locked_pct = float(data.get("lpLockedPct", 0))

        if score <= 1 and not risks:
            logger.warning(
                "RugCheck returned empty risks with score=%s for %s. Using neutral score.",
                score,
                mint,
            )
            score = 50

        lp_burned = data.get("lpBurned")
        mint_disabled = data.get("mintDisabled")
        freeze_disabled = data.get("freezeDisabled")

        safety = score

        if lp_locked_pct >= 80:
            safety += 20
        elif lp_locked_pct >= 50:
            safety += 10
        elif lp_locked_pct >= 20:
            safety += 5

        if lp_burned is True:
            safety += 10

        if mint_disabled is True:
            safety += 5

        if freeze_disabled is True:
            safety += 5

        if len(flags) >= 4:
            safety -= 20
        elif len(flags) >= 2:
            safety -= 10

        safety = max(0, min(100, safety))

        if safety >= 75:
            risk_level, risk_emoji = "Low", "🟢"
        elif safety >= 50:
            risk_level, risk_emoji = "Medium", "🟡"
        else:
            risk_level, risk_emoji = "High", "🔴"

        result.update({
            "risk_level": risk_level,
            "risk_emoji": risk_emoji,
            "safety_score": safety,
            "lp_burned": lp_burned,
            "mint_disabled": mint_disabled,
            "freeze_disabled": freeze_disabled,
            "flags": flags[:5],
        })

        logger.info(
            "Rug check %s: %s (score=%d, lpLocked=%.2f%%, risks=%d)",
            mint,
            risk_level,
            safety,
            lp_locked_pct,
            len(flags),
        )

    except Exception as e:
        logger.debug("RugCheck failed for %s: %s", mint, e)

    # Always cache and return a dictionary
    with rug_cache_lock:
        rug_cache[mint] = result

    return result

# ═══════════════════════════════════════════════════════════════════════════════
#  PRICE FETCHER — DexScreener only
# ═══════════════════════════════════════════════════════════════════════════════

def _fmt_price(price: float) -> str:
    if price <= 0:          return "N/A"
    if price < 0.000001:    return f"${price:.10f}"
    if price < 0.01:        return f"${price:.8f}"
    if price < 1:           return f"${price:.6f}"
    return f"${price:,.4f}"

def _fmt_mcap(mcap: float) -> str:
    if mcap <= 0:           return "N/A"
    if mcap >= 1_000_000:   return f"${mcap/1_000_000:.2f}M"
    if mcap >= 1_000:       return f"${mcap/1_000:.1f}K"
    return f"${mcap:.0f}"


def _fetch_dexscreener_price(mint: str, result: dict) -> bool:
    try:
        r = requests.get(
            f"https://api.dexscreener.com/latest/dex/tokens/{mint}", timeout=6)
        r.raise_for_status()
        pairs = r.json().get("pairs", [])
        if not pairs:
            return False
        p = max(pairs, key=lambda x: float(x.get("liquidity", {}).get("usd") or 0))

        price     = float(p.get("priceUsd", 0) or 0)
        mcap      = float(p.get("marketCap") or p.get("fdv") or 0)
        liq       = float(p.get("liquidity", {}).get("usd") or 0)
        sym_hint  = p.get("baseToken", {}).get("symbol", "")

        if sym_hint:
            result["_symbol_hint"] = sym_hint

        if price > 0 and not result.get("price"):
            result["price"]     = price
            result["price_str"] = _fmt_price(price)
            result["source"]    = "DexScreener"

        if mcap > 0:
            result["market_cap"] = mcap
            result["mcap_str"]   = _fmt_mcap(mcap)

        result["liquidity_usd"] = liq

        pc = p.get("priceChange")
        if isinstance(pc, dict):
            try:    result["price_change_5m"] = float(pc.get("m5") or 0) or None
            except Exception: result["price_change_5m"] = None

        vol = p.get("volume")
        if isinstance(vol, dict):
            try:    result["volume_5m"] = float(vol.get("m5") or 0) or None
            except Exception: result["volume_5m"] = None

        return True
    except Exception as e:
        logger.debug("DexScreener price failed: %s", e)
    return False


def _get_token_price(mint: str, bypass_cache: bool = False) -> dict:
    if not bypass_cache:
        with price_cache_lock:
            if mint in price_cache:
                return price_cache[mint]

    with price_cache_lock:
        if mint in _price_fetching:
            event = _price_fetch_event.get(mint)
        else:
            _price_fetching.add(mint)
            event = threading.Event()
            _price_fetch_event[mint] = event
            event = None

    if event is not None:
        event.wait(timeout=8)
        with price_cache_lock:
            if mint in price_cache:
                return price_cache[mint]

    result = {
        "price": None, "price_str": "N/A",
        "market_cap": None, "mcap_str": "N/A",
        "price_change_5m": None, "volume_5m": None,
        "liquidity_usd": 0, "source": "N/A",
    }

    try:
        _fetch_dexscreener_price(mint, result)
        if result.get("_symbol_hint"):
            with _jupiter_map_lock:
                _jupiter_token_map[mint] = result.pop("_symbol_hint")
        logger.info("Price %s: %s (bypass=%s)", mint, result["price_str"], bypass_cache)
    finally:
        with price_cache_lock:
            _price_fetching.discard(mint)
            ev = _price_fetch_event.pop(mint, None)
            if not bypass_cache:
                price_cache[mint] = result
        if ev:
            ev.set()

    return result


# ═══════════════════════════════════════════════════════════════════════════════
#  HELIUS WEBHOOK MANAGEMENT
# ═══════════════════════════════════════════════════════════════════════════════

def _webhook_payload(wallets: list) -> dict:
    return {
        "webhookURL":       f"{APP_URL.rstrip('/')}/helius-webhook",
        "transactionTypes": ["SWAP"],
        "accountAddresses": wallets,
        "webhookType":      "enhanced",
        "authHeader":       WEBHOOK_SECRET,
    }


def _upsert_webhook(wallets: list, existing_id: str = "") -> str:
    if existing_id:
        try:
            resp = requests.put(
                f"{HELIUS_BASE}/webhooks/{existing_id}?api-key={HELIUS_API_KEY}",
                json=_webhook_payload(wallets), timeout=10)
            resp.raise_for_status()
            logger.info("✅ Webhook updated — ID: %s (%d wallets)", existing_id, len(wallets))
            return existing_id
        except Exception as e:
            logger.warning("Webhook PUT failed (%s) — creating new", e)

    resp = requests.post(
        f"{HELIUS_BASE}/webhooks?api-key={HELIUS_API_KEY}",
        json=_webhook_payload(wallets), timeout=10)
    resp.raise_for_status()
    wid = resp.json().get("webhookID", "")
    logger.info("✅ Webhook created — ID: %s (%d wallets)", wid, len(wallets))
    return wid


def refresh_wallets():
    global watched_wallets, current_webhook_id
    if not FALLBACK_WALLETS:
        logger.error("FALLBACK_WALLETS is empty")
        return

    # Dynamic roster (seeded from FALLBACK_WALLETS, then mutated over time by
    # the promotion/demotion and discovery-engine cycles) takes precedence.
    # Falls back to the static list if the intelligence DB has nothing yet
    # or is unreachable.
    try:
        wi.seed_active_roster(FALLBACK_WALLETS)
        roster = wi.get_active_roster()
        new_wallets = roster if roster else list(FALLBACK_WALLETS)
    except Exception as e:
        logger.warning("Dynamic roster unavailable (%s) — using static fallback list", e)
        new_wallets = list(FALLBACK_WALLETS)

    logger.info("📋 Using %d wallets (dynamic roster)", len(new_wallets))

    new_id, webhook_ok = "", False
    try:
        new_id     = _upsert_webhook(new_wallets, existing_id=current_webhook_id)
        webhook_ok = True
    except Exception as e:
        logger.error("Webhook upsert failed: %s", e)
        try:
            resp  = requests.get(f"{HELIUS_BASE}/webhooks?api-key={HELIUS_API_KEY}", timeout=10)
            hooks = resp.json()
            if hooks:
                new_id = hooks[0].get("webhookID", "")
                logger.info("♻️ Recovered webhook: %s", new_id)
        except Exception as e2:
            logger.warning("Could not recover webhook: %s", e2)

    with wallet_lock:
        watched_wallets = new_wallets
        if new_id:
            current_webhook_id = new_id

    status_str = "✅ Webhook Registered" if webhook_ok else "⚠️ Webhook registration failed"
    try:
        stats = wi.get_dashboard_stats(new_wallets)
    except Exception as e:
        logger.warning("Dashboard stats unavailable: %s", e)
        stats = None

    if stats:
        stats_block = (
            f"👛 Watching: <b>{stats['watching_total']} wallets</b>\n\n"
            f"🏆 Elite: <b>{stats['tier_elite']}</b>\n"
            f"💪 Strong: <b>{stats['tier_strong']}</b>\n"
            f"📊 Standard: <b>{stats['tier_standard']}</b>\n"
            + (f"🩹 Probation: <b>{stats['tier_probation']}</b>\n" if stats['tier_probation'] else "")
            + (f"🪦 Retired: <b>{stats['tier_retired']}</b>\n" if stats['tier_retired'] else "")
            + "\n"
            f"🎯 <b>Confidence DB:</b>\n"
            f"Wallets scored: <b>{stats['wallets_scored']:,}</b>\n\n"
            f"🚀 <b>Early Entry DB:</b>\n"
            f"Tokens tracked: <b>{stats['tokens_tracked_early']:,}</b>\n\n"
            f"👑 <b>Leader Graph:</b>\n"
            f"Leader edges: <b>{stats['leader_edges']:,}</b>\n\n"
            f"📈 <b>Lifecycle DB:</b>\n"
            f"Tokens monitored: <b>{stats['tokens_monitored']:,}</b>\n"
            f"5× winners: <b>{stats['hit_5x']:,}</b>\n"
            f"10× winners: <b>{stats['hit_10x']:,}</b>\n\n"
            f"🔍 <b>Discovery Queue:</b>\n"
            f"Candidates: <b>{stats['discovery_candidates']:,}</b>\n"
            f"Pending promotions: <b>{stats['pending_promotions']:,}</b>\n\n"
            f"{status_str}"
        )
    else:
        stats_block = (
            f"👛 Watching <b>{len(new_wallets)} wallets</b>\n\n"
            f"{status_str}"
        )

    try:
        bot.send_message(
            chat_id=CHAT_ID,
            text=f"🕷 <b>SpiderWalletBot Started</b>\n\n{stats_block}",
            parse_mode="HTML",
        )
    except TelegramError as e:
        logger.error("Startup message failed: %s", e)


def _run_lifecycle_updater_job():
    """Background pass: refresh prices for active tokens, update ATH/ROI/2x-5x-10x."""
    try:
        wi.background_lifecycle_updater(_get_token_price, alert_on_milestone=_alert_on_milestone)
    except Exception as e:
        logger.error("Lifecycle updater job failed: %s", e)


def _alert_on_milestone(mint: str, symbol: str, crossed: list, lifecycle_row: dict):
    """
    Fired when a tracked token crosses one or more of 2x / 5x / 10x off its
    first recorded price. `crossed` is the full list of multiples newly
    crossed since the last check — usually just one, but can be more than
    one if the price jumped past several thresholds between polls (e.g. a
    huge pump caught in a single 15-min check). We send exactly ONE message
    per call, naming every threshold that was crossed, instead of one
    message per multiple — sending three separate alerts that each show the
    same current ROI number is what caused the duplicate-looking alerts.
    """
    try:
        roi = lifecycle_row.get("roi_pct", 0)
        if len(crossed) == 1:
            title = f"🚀 <b>{crossed[0]}x MILESTONE</b>"
        else:
            title = f"🚀 <b>{crossed[0]}x → {crossed[-1]}x MILESTONE</b>  <i>(crossed {', '.join(f'{m}x' for m in crossed)} at once)</i>"
        bot.send_message(
            chat_id=CHAT_ID,
            text=(
                f"{title}\n\n"
                f"🪙 Token : <b>{_esc(symbol or mint[:8]+'…')}</b>\n"
                f"📈 ROI since first seen : <b>{roi:+.1f}%</b>\n"
                f"📋 CA: <code>{_esc(mint)}</code>"
            ),
            parse_mode="HTML",
        )
    except TelegramError as e:
        logger.debug("Milestone alert failed: %s", e)


def _run_risk_monitor_job():
    """
    Faster-cadence pass over currently open trades: drawdown from ATH,
    stop-loss breach, and rug-pull signals (liquidity removal / price crash).
    """
    try:
        wi.run_risk_monitor(
            _get_token_price,
            on_drawdown=_alert_on_drawdown,
            on_stop_loss=_alert_on_stop_loss,
            on_rug=_alert_on_rug,
        )
    except Exception as e:
        logger.error("Risk monitor job failed: %s", e)


def _alert_on_drawdown(mint: str, symbol: str, drawdown_pct: float, thresholds: list, lifecycle_row: dict):
    """Fired when price falls 20%/30%/50% below the token's ATH."""
    try:
        highest = max(thresholds)
        ath = lifecycle_row.get("ath_price") or 0
        last = lifecycle_row.get("last_price") or 0
        bot.send_message(
            chat_id=CHAT_ID,
            text=(
                f"📉 <b>-{highest}% DRAWDOWN</b>\n\n"
                f"🪙 Token : <b>{_esc(symbol or mint[:8]+'…')}</b>\n"
                f"📊 Down <b>{drawdown_pct:.1f}%</b> from ATH ({_fmt_price(ath)} → {_fmt_price(last)})\n"
                f"📋 CA: <code>{_esc(mint)}</code>"
            ),
            parse_mode="HTML",
        )
    except TelegramError as e:
        logger.debug("Drawdown alert failed: %s", e)


def _alert_on_stop_loss(mint: str, symbol: str, price: float, stop_loss_price: float, lifecycle_row: dict):
    """Fired once when price falls to/below the suggested stop-loss level."""
    try:
        bot.send_message(
            chat_id=CHAT_ID,
            text=(
                f"🛑 <b>STOP-LOSS BREACHED</b>\n\n"
                f"🪙 Token : <b>{_esc(symbol or mint[:8]+'…')}</b>\n"
                f"💰 Price : <b>{_fmt_price(price)}</b>  (stop was {_fmt_price(stop_loss_price)})\n"
                f"⚠️ Suggested action: exit remaining position\n"
                f"📋 CA: <code>{_esc(mint)}</code>"
            ),
            parse_mode="HTML",
        )
    except TelegramError as e:
        logger.debug("Stop-loss alert failed: %s", e)


def _alert_on_rug(mint: str, symbol: str, signals: list, lifecycle_row: dict):
    """Fired once when liquidity is pulled and/or price crashes rapidly."""
    try:
        reasons = []
        if "liquidity_removed" in signals:
            reasons.append("💧 Liquidity pulled")
        if "price_crash" in signals:
            reasons.append("📉 Rapid price crash (5m)")
        bot.send_message(
            chat_id=CHAT_ID,
            text=(
                f"🚨 <b>POSSIBLE RUG PULL</b>\n\n"
                f"🪙 Token : <b>{_esc(symbol or mint[:8]+'…')}</b>\n"
                + "\n".join(f"{r}" for r in reasons) + "\n\n"
                f"⚠️ Exit immediately if still holding\n"
                f"📋 CA: <code>{_esc(mint)}</code>"
            ),
            parse_mode="HTML",
        )
    except TelegramError as e:
        logger.debug("Rug alert failed: %s", e)
    finally:
        # A confirmed rug means there's nothing left worth polling — mark the
        # lifecycle row dead so it drops out of both the lifecycle updater
        # and the risk monitor's active/open-trade queries, instead of
        # burning price-fetch calls on it for the rest of its 72h window.
        try:
            wi.mark_token_dead(mint)
        except Exception as e:
            logger.debug("mark_token_dead failed for %s: %s", mint, e)


def _alert_smart_money_exit(mint: str, symbol: str, wallet: str, sell_count: int, total_buyers: int):
    """
    Fired the FIRST time any original tracked buyer starts selling a token —
    a lightweight early warning, distinct from (and earlier than) the full
    dump/sell-signal alert which only fires once enough wallets have exited
    together to clear the convergence threshold.
    """
    try:
        bot.send_message(
            chat_id=CHAT_ID,
            text=(
                f"💸 <b>SMART MONEY EXIT</b>\n\n"
                f"🪙 Token : <b>{_esc(symbol or mint[:8]+'…')}</b>\n"
                f"👛 A tracked wallet just started selling\n"
                f"📊 {sell_count}/{total_buyers} original buyers have now sold\n"
                f"<i>Early signal — full sell alert fires once more wallets exit together.</i>\n"
                f"📋 CA: <code>{_esc(mint)}</code>"
            ),
            parse_mode="HTML",
        )
    except TelegramError as e:
        logger.debug("Smart-money exit alert failed: %s", e)


def _run_promotion_demotion_job():
    """Recomputes confidence + tier for every watched wallet, logs any tier moves."""
    try:
        with wallet_lock:
            wallets = list(watched_wallets)
        result = wi.run_promotion_demotion_cycle(wallets)
        if result["promoted"] or result["demoted"] or result["retired"]:
            bot.send_message(
                chat_id=CHAT_ID,
                text=(
                    "🔄 <b>Wallet Tier Update</b>\n\n"
                    f"⬆️ Promoted : <b>{len(result['promoted'])}</b>\n"
                    f"⬇️ Demoted  : <b>{len(result['demoted'])}</b>\n"
                    f"🪦 Retired  : <b>{len(result['retired'])}</b>\n\n"
                    f"<i>Retired wallets will be rotated out by the discovery engine.</i>"
                ),
                parse_mode="HTML",
            )
    except Exception as e:
        logger.error("Promotion/demotion job failed: %s", e)


def _run_discovery_job():
    """Finds new candidate wallets from winning tokens, graduates good ones, backfills roster."""
    try:
        with wallet_lock:
            wallets = list(watched_wallets)
        result = wi.run_discovery_cycle(wallets)
        if result["roster_changed"]:
            logger.info("Discovery cycle changed the roster — triggering webhook refresh")
            refresh_wallets()
            bot.send_message(
                chat_id=CHAT_ID,
                text=(
                    "🔍 <b>Wallet Discovery Update</b>\n\n"
                    f"🪦 Retired  : <b>{len(result['retired'])}</b>\n"
                    f"✅ Added    : <b>{len(result['added'])}</b>\n"
                    f"👀 Watching : <b>{wi.get_discovery_stats()['watching']}</b> candidates"
                ),
                parse_mode="HTML",
            )
    except Exception as e:
        logger.error("Discovery job failed: %s", e)


def _start_refresh_scheduler():
    if scheduler:
        scheduler.add_job(refresh_wallets, "interval",
                          hours=REFRESH_HOURS, id="wallet_refresh")
        scheduler.add_job(_run_lifecycle_updater_job, "interval",
                          seconds=wi.LIFECYCLE_REFRESH_SECS, id="lifecycle_updater")
        scheduler.add_job(_run_risk_monitor_job, "interval",
                          seconds=wi.RISK_MONITOR_INTERVAL_SECS, id="risk_monitor")
        scheduler.add_job(_run_promotion_demotion_job, "interval",
                          hours=6, id="promotion_demotion")
        scheduler.add_job(_run_discovery_job, "interval",
                          hours=12, id="wallet_discovery")
        try:
            from apscheduler.triggers.cron import CronTrigger
            import pytz

            scheduler.add_job(
                send_daily_report,
                CronTrigger(
                    hour=0,
                    minute=0,
                    timezone=pytz.timezone("Africa/Lagos")
                ),
                id="daily_report"
            )
        except Exception:
            pass
        scheduler.start()
        logger.info(
            "⏰ Scheduler started — refresh %dh, lifecycle %ds, risk-monitor %ds, "
            "tiers 6h, discovery 12h, midnight report",
            REFRESH_HOURS, wi.LIFECYCLE_REFRESH_SECS, wi.RISK_MONITOR_INTERVAL_SECS)
        return

    def _loop():
        last_lifecycle = last_risk = last_tiers = last_discovery = time.time()
        while True:
            time.sleep(30)
            now = time.time()
            if now - last_lifecycle >= wi.LIFECYCLE_REFRESH_SECS:
                _run_lifecycle_updater_job()
                last_lifecycle = now
            if now - last_risk >= wi.RISK_MONITOR_INTERVAL_SECS:
                _run_risk_monitor_job()
                last_risk = now
            if now - last_tiers >= 6 * 3600:
                _run_promotion_demotion_job()
                last_tiers = now
            if now - last_discovery >= 12 * 3600:
                _run_discovery_job()
                last_discovery = now

    def _refresh_loop():
        while True:
            time.sleep(REFRESH_HOURS * 3600)
            try:
                refresh_wallets()
            except Exception as e:
                logger.error("Scheduled refresh failed: %s", e)

    threading.Thread(target=_loop, daemon=True).start()
    threading.Thread(target=_refresh_loop, daemon=True).start()
    logger.info("⏰ Fallback thread-loop scheduler started (no APScheduler)")


# ═══════════════════════════════════════════════════════════════════════════════
#  VISUAL CARD GENERATOR
# ═══════════════════════════════════════════════════════════════════════════════

W, H = 1200, 630

_FONT_CANDIDATES = [
    "C:/Windows/Fonts/segoeuib.ttf",
    "C:/Windows/Fonts/arialbd.ttf",
    "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf",
    "/usr/share/fonts/truetype/liberation/LiberationSans-Bold.ttf",
    "/usr/share/fonts/truetype/ubuntu/Ubuntu-B.ttf",
    "/usr/share/fonts/truetype/noto/NotoSans-Bold.ttf",
    "/usr/share/fonts/truetype/freefont/FreeSansBold.ttf",
]
_resolved_font_path = None


def ensure_font():
    global _resolved_font_path
    for path in _FONT_CANDIDATES:
        if Path(path).exists():
            _resolved_font_path = path
            logger.info("🔤 Font: %s", path)
            return
    logger.warning("No system font found — PIL default used")


def _load_font(size: int):
    if _resolved_font_path:
        try:
            return ImageFont.truetype(_resolved_font_path, size)
        except Exception:
            pass
    return ImageFont.load_default()


def _build_template(is_sell: bool) -> Image.Image:
    key = "sell" if is_sell else "buy"
    if key in _card_template_cache:
        return _card_template_cache[key].copy()
    img  = Image.new("RGB", (W, H), color=(10, 10, 18))
    draw = ImageDraw.Draw(img)
    for x in range(W // 2):
        shade = int(15 + 10 * (x / (W // 2)))
        draw.line([(x, 0), (x, H)], fill=(shade, shade, shade + 8))
    accent = (210, 50, 50) if is_sell else (50, 210, 110)
    draw.rectangle([0, 0, 10, H],      fill=accent)
    draw.rectangle([0, 0, W-1, H-1],   outline=(40, 40, 65), width=2)
    _card_template_cache[key] = img.copy()
    return img


def _make_card(symbol, price_str, mcap_str, wallet_count, price_change,
               grade="", is_sell=False):
    img   = _build_template(is_sell)
    draw  = ImageDraw.Draw(img)
    pc    = (230, 80, 80)  if is_sell else (80, 235, 130)
    rx    = W // 2 + 45

    f100  = _load_font(100)
    f52   = _load_font(52)
    f38   = _load_font(38)
    f30   = _load_font(30)
    f24   = _load_font(24)

    badge = "🔴 SELL SIGNAL" if is_sell else "🚨 BUY SIGNAL"
    draw.rounded_rectangle([rx, 35, W-35, 105], radius=14,
                           fill=(180,40,40) if is_sell else (35,160,80))
    draw.text((rx+18, 48),  badge,               font=f30,  fill=(255,255,255))
    draw.text((rx, 120),    f"{symbol}/SOL",      font=f52,  fill=(190,190,215))
    draw.text((rx, 185),    price_str,            font=f100, fill=pc)
    draw.text((rx, 310),    f"Mkt Cap: {mcap_str}", font=f38, fill=(150,150,175))

    if price_change is not None:
        arrow = "▼" if price_change < 0 else "▲"
        col   = (230,80,80) if price_change < 0 else (80,220,120)
        draw.text((rx, 360), f"{arrow} {price_change:+.2f}% (5m)", font=f38, fill=col)

    action = "selling" if is_sell else "buying"
    draw.text((rx, 415), f"{'🔴' if is_sell else '👛'} {wallet_count} wallets {action}",
              font=f30, fill=(150,150,175))

    if grade:
        draw.text((rx, 455), f"Grade: {grade}", font=f52,
                  fill=(255,215,0))   # gold

    draw.text((rx, 535), "Helius  •  DexScreener  •  RugCheck",
              font=f24, fill=(65,65,90))

    buf = io.BytesIO()
    img.save(buf, format="PNG", optimize=True)
    buf.seek(0)
    return buf


# ═══════════════════════════════════════════════════════════════════════════════
#  4. AI SIGNAL GRADING  +  5. PERSONAL TRADE ASSISTANT
# ═══════════════════════════════════════════════════════════════════════════════

def _grade_signal(wallets: set, price_data: dict, buy_times: list,
                  rug: dict, adaptive_thresh: int) -> dict:
    """
    Multi-factor AI grade. Four dimensions → weighted average → letter grade.

    Wallet Quality  (30%) — based on wallet ranking scores
    Conv. Speed     (25%) — how fast wallets converged
    Liquidity       (25%) — DexScreener liquidity USD
    Rug Safety      (20%) — RugCheck safety score
    """
    # ── Wallet Quality (0-100) ─────────────────────────────────────────────────
    wallet_quality = _get_wallet_quality_score(wallets)
    # Bonus for exceeding threshold
    excess_ratio   = len(wallets) / max(adaptive_thresh, 1)
    wallet_quality = min(wallet_quality * excess_ratio, 100)

    # ── Convergence Speed (0-100) ──────────────────────────────────────────────
    if len(buy_times) >= 2:
        span = max(buy_times) - min(buy_times)
        if span <= 20:    speed_score, speed_label = 98, "⚡ Lightning (<20s)"
        elif span <= 45:  speed_score, speed_label = 88, "🚀 Very Fast (<45s)"
        elif span <= 90:  speed_score, speed_label = 72, "🔥 Fast (<90s)"
        else:             speed_score, speed_label = 50, "🐢 Moderate"
    else:
        speed_score, speed_label = 70, "🔥 Fast"

    # ── Liquidity (0-100) ─────────────────────────────────────────────────────
    liq = price_data.get("liquidity_usd") or 0
    if liq >= 500_000:  liq_score = 95
    elif liq >= 200_000: liq_score = 82
    elif liq >= 100_000: liq_score = 68
    elif liq >= 50_000:  liq_score = 52
    elif liq >= 20_000:  liq_score = 38
    elif liq > 0:        liq_score = 22
    else:                liq_score = 30   # unknown — neutral

    # ── Rug Safety (0-100) ────────────────────────────────────────────────────
    rug_score = rug.get("safety_score", 50)

    # ── Weighted final score ──────────────────────────────────────────────────
    final = (
        wallet_quality * 0.30 +
        speed_score    * 0.25 +
        liq_score      * 0.25 +
        rug_score      * 0.20
    )
    final = round(final, 1)

    # Letter grade
    if   final >= 93: grade = "A+"
    elif final >= 87: grade = "A"
    elif final >= 80: grade = "A-"
    elif final >= 73: grade = "B+"
    elif final >= 67: grade = "B"
    elif final >= 60: grade = "B-"
    elif final >= 53: grade = "C+"
    elif final >= 47: grade = "C"
    else:             grade = "D"

    return {
        "final_score":     final,
        "grade":           grade,
        "wallet_quality":  round(wallet_quality, 1),
        "speed_score":     speed_score,
        "speed_label":     speed_label,
        "liq_score":       liq_score,
        "rug_score":       rug_score,
    }

def _trade_assistant(price_data: dict, rug: dict) -> dict:
    """
    Personal trade assistant — suggests entry, TP1, TP2, stop loss.
    Uses live price from DexScreener.

    TP targets calibrated to meme coin typical moves:
      TP1 = +30%  (take half position)
      TP2 = +60%  (take remaining)
      SL  = -15%  (protect capital)

    If rug risk is High, tightens SL to -10% and reduces TP2 to +40%.
    """
    price = price_data.get("price") or 0
    if price <= 0:
        return {"available": False}

    high_risk = rug.get("risk_level") == "High"

    tp1_pct = 30
    tp2_pct = 40 if high_risk else 60
    sl_pct  = 10 if high_risk else 15

    entry = price
    tp1   = price * (1 + tp1_pct / 100)
    tp2   = price * (1 + tp2_pct / 100)
    sl    = price * (1 - sl_pct  / 100)

    return {
        "available":  True,
        "entry":      _fmt_price(entry),
        "tp1":        _fmt_price(tp1),
        "tp1_pct":    tp1_pct,
        "tp2":        _fmt_price(tp2),
        "tp2_pct":    tp2_pct,
        "sl":         _fmt_price(sl),
        "sl_pct":     sl_pct,
        "risk_adj":   high_risk,
    }


# ═══════════════════════════════════════════════════════════════════════════════
#  TELEGRAM ALERTS
# ═══════════════════════════════════════════════════════════════════════════════

CAPTION_PHOTO_LIMIT = 1024   # Telegram hard limit for photo captions
CAPTION_TEXT_LIMIT  = 4096   # Telegram hard limit for text messages


def _truncate_caption(caption: str, limit: int) -> str:
    """Truncate caption to Telegram limit, preserving the CA line at the bottom."""
    if len(caption) <= limit:
        return caption
    ca_marker = "\n\n📋 CA:"
    ca_idx    = caption.rfind(ca_marker)
    ca_tail   = caption[ca_idx:] if ca_idx != -1 else ""
    budget    = limit - len(ca_tail) - 4
    return caption[:budget] + "…" + ca_tail

def _esc(text: str) -> str:
    return str(text).replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


def _trojan_link(mint: str) -> str:
    return f"https://t.me/solana_trojanbot?start={TROJAN_REF_CODE}"


def _jupiter_link(mint: str) -> str:
    return f"https://jup.ag/swap/SOL-{mint}"


def _build_caption(mint: str, symbol: str, wallets: set, price_data: dict,
                   is_sell: bool, grade_data: dict = None, trade: dict = None,
                   rug: dict = None, sell_count: int = 0, sell_total: int = 0) -> str:

    sym_safe  = _esc(symbol)
    mint_safe = _esc(mint)
    pc  = price_data.get("price_change_5m")
    vol = price_data.get("volume_5m")

    pc_line  = f"📉 5m Change : <b>{pc:+.2f}%</b>\n" if pc is not None else ""
    vol_line = f"💹 5m Volume : <b>${vol:,.0f}</b>\n"  if vol            else ""

    preview = "\n".join(
        f"  <code>{w[:4]}…{w[-4:]}</code> (score: {_get_wallet_score(w):.2f})"
        for w in list(wallets)[:5]
    )
    if len(wallets) > 5:
        preview += f"\n  <i>…and {len(wallets)-5} more</i>"

    # ── Buy alert ─────────────────────────────────────────────────────────────
    if not is_sell:
        # AI Signal Grade block
        grade_block = ""
        if grade_data:
            g     = grade_data
            score = g["final_score"]
            grd   = g["grade"]
            grade_block = (
                f"\n━━━━━━━━━━━━━━━━━━━━\n"
                f"🤖 <b>AI Signal Grade</b>\n\n"
                f"🏆 Final Grade       : <b>{grd}  ({score}/100)</b>\n"
                f"👛 Wallet Quality    : <b>{g['wallet_quality']}/100</b>\n"
                f"⚡ Conv. Speed       : <b>{g['speed_score']}/100</b>  {_esc(g['speed_label'])}\n"
                f"💧 Liquidity         : <b>{g['liq_score']}/100</b>\n"
                f"🛡 Rug Safety        : <b>{g['rug_score']}/100</b>\n"
                f"━━━━━━━━━━━━━━━━━━━━\n"
            )

        # Rug risk block
        rug_block = ""
        if rug:
            rug_block = f"\n🛡 <b>Rug Risk</b> : {_fmt_rug_flags(rug)}\n"

        # Trade assistant block
        trade_block = ""
        if trade and trade.get("available"):
            risk_note = "  ⚠️ <i>Tightened — High rug risk</i>\n" if trade["risk_adj"] else ""
            trade_block = (
                f"\n━━━━━━━━━━━━━━━━━━━━\n"
                f"📐 <b>Trade Assistant</b>\n\n"
                f"  🟢 Entry      : <b>{_esc(trade['entry'])}</b>\n"
                f"  🎯 TP1 (+{trade['tp1_pct']}%) : <b>{_esc(trade['tp1'])}</b>  <i>(take 50%)</i>\n"
                f"  🎯 TP2 (+{trade['tp2_pct']}%) : <b>{_esc(trade['tp2'])}</b>  <i>(take rest)</i>\n"
                f"  🛑 Stop (-{trade['sl_pct']}%) : <b>{_esc(trade['sl'])}</b>\n"
                f"{risk_note}"
                f"━━━━━━━━━━━━━━━━━━━━\n"
            )

        return (
            f"🚨 <b>BUY SIGNAL — Wallet Convergence</b>\n\n"
            f"🪙 Token   : <b>{sym_safe}</b>\n"
            f"💰 Price   : <b>{_esc(price_data['price_str'])}</b>\n"
            f"📊 Mkt Cap : <b>{_esc(price_data['mcap_str'])}</b>\n"
            f"{pc_line}{vol_line}"
            f"👛 Wallets : <b>{len(wallets)}/{len(FALLBACK_WALLETS)}</b> buying\n\n"
            f"<b>Buying Wallets:</b>\n{preview}"
            f"{grade_block}"
            f"{rug_block}"
            f"{trade_block}"
            f"\n👇 <b>To buy on Trojan:</b>\n"
            f"1. Tap <b>Open Trojan</b> below\n"
            f"2. Tap CA → copy → paste into Trojan → Buy\n\n"
            f"📋 CA: <code>{mint_safe}</code>"
        )

    # ── Sell / Exit alert — one clean alert when same wallets exit ───────────
    else:
        sell_total_safe = max(sell_total, 1)
        pct = round((sell_count / sell_total_safe) * 100)
        return (
            f"🔴 <b>EXIT ALERT — Same Wallets Selling</b>\n\n"
            f"🪙 Token   : <b>{sym_safe}</b>\n"
            f"💰 Price   : <b>{_esc(price_data['price_str'])}</b>\n"
            f"📊 Mkt Cap : <b>{_esc(price_data['mcap_str'])}</b>\n"
            f"{pc_line}{vol_line}\n"
            f"🚨 <b>{sell_count}/{sell_total} original buy wallets now selling ({pct}%)</b>\n\n"
            f"<b>Selling Wallets:</b>\n{preview}\n\n"
            f"👇 <b>To sell on Trojan:</b>\n"
            f"1. Tap <b>Open Trojan</b> below\n"
            f"2. Tap CA → copy → paste into Trojan → Sell\n\n"
            f"📋 CA: <code>{mint_safe}</code>"
        )


def _send_alert(mint: str, symbol: str, wallets: set, price_data: dict,
                is_sell: bool = False, grade_data: dict = None,
                trade: dict = None, rug: dict = None,
                sell_count: int = 0, sell_total: int = 0):

    grade_str     = grade_data["grade"] if grade_data else ""
    caption_full  = _build_caption(mint, symbol, wallets, price_data, is_sell,
                                    grade_data=grade_data, trade=trade, rug=rug,
                                    sell_count=sell_count, sell_total=sell_total)
    caption_photo = _truncate_caption(caption_full, CAPTION_PHOTO_LIMIT)
    caption_text  = _truncate_caption(caption_full, CAPTION_TEXT_LIMIT)

    tlabel   = "🔴 Open Trojan (Sell)" if is_sell else "🏹 Open Trojan (Buy)"
    keyboard = InlineKeyboardMarkup([
        [
            InlineKeyboardButton(tlabel,            url=_trojan_link(mint)),
            InlineKeyboardButton("🪐 Jupiter",      url=_jupiter_link(mint)),
        ],
        [
            InlineKeyboardButton("📈 Solscan",      url=f"https://solscan.io/token/{mint}"),
            InlineKeyboardButton("🔍 DexScreener",  url=f"https://dexscreener.com/solana/{mint}"),
        ],
    ])

    try:
        card = _make_card(symbol, price_data["price_str"], price_data["mcap_str"],
                          len(wallets), price_data.get("price_change_5m"),
                          grade=grade_str, is_sell=is_sell)
        bot.send_photo(chat_id=CHAT_ID, photo=card, caption=caption_photo,
                       parse_mode="HTML", reply_markup=keyboard)
        logger.info("%s alert sent — %s @ %s (%d wallets) grade=%s",
                    "SELL" if is_sell else "BUY", symbol,
                    price_data["price_str"], len(wallets), grade_str or "N/A")
        return
    except Exception as e:
        logger.warning("send_photo failed (%s) — falling back to text", e)

    try:
        bot.send_message(chat_id=CHAT_ID, text=caption_text,
                         parse_mode="HTML", reply_markup=keyboard)
        logger.info("%s text alert sent — %s (%d wallets)",
                    "SELL" if is_sell else "BUY", symbol, len(wallets))
    except TelegramError as e:
        logger.error("Telegram alert completely failed: %s", e)


# ═══════════════════════════════════════════════════════════════════════════════
#  TRANSACTION PROCESSOR
# ═══════════════════════════════════════════════════════════════════════════════

def _safe_process_tx(tx: dict, ts: int):
    """Wrapper so ThreadPoolExecutor catches and logs all exceptions."""
    try:
        _process_tx(tx, ts)
    except Exception as e:
        logger.error("_process_tx error: %s", e, exc_info=True)


def _process_tx(tx: dict, ts: int):
    if tx.get("type") != "SWAP":
        return

    # ── 1. Signature deduplication (TTL=3600s) ────────────────────────────────
    sig = tx.get("signature", "")
    if sig:
        with sig_lock:
            if sig in processed_signatures:
                logger.debug("Duplicate tx skipped: %s", sig[:12])
                return
            processed_signatures[sig] = True

    wallet = tx.get("feePayer", "").strip()
    if not wallet:
        return

    transfers = tx.get("tokenTransfers", [])

    # ── BUY detection ─────────────────────────────────────────────────────────
    bought = []

    for transfer in transfers:
        if transfer.get("toUserAccount") != wallet:
            continue

        mint = transfer.get("mint", "").strip()
        if not mint:
            continue

        # Ignore common non-meme assets
        if mint in IGNORED_MINTS:
            continue

        try:
            amount = float(transfer.get("tokenAmount") or 0)
        except (TypeError, ValueError):
            amount = 0

        if amount <= 0:
            continue

        bought.append(transfer)

    for transfer in bought:
        mint = transfer.get("mint", "").strip()
        symbol = _resolve_symbol(mint, transfer.get("tokenSymbol"))

        with position_lock:
            wallet_positions.setdefault(wallet, {})[mint] = ts
 
    # ── Intelligence hooks ─────────────────────────────────────
try:
    wi.record_token_launch(mint, ts, symbol)
    wi.record_buy_sequence(mint, wallet, ts)
    wi.compute_early_entry_score(wallet, mint, ts)
except Exception as e:
    logger.debug(
        "Intelligence buy hooks failed for %s/%s: %s",
        wallet,
        mint,
        e,
    )

adaptive_thresh = _get_adaptive_threshold()

with activity_lock:
    try:
        existing = recent_activity[mint]
    except Exception:
        existing = []

    entries = [(w, t) for (w, t) in existing if ts - t <= WINDOW]
    entries.append((wallet, ts))
    recent_activity[mint] = entries

    unique = {w for (w, _) in entries}
    buy_times = [t for (_, t) in entries]
    already_alerted = mint in alerted_tokens
    
    # ── 2. Weighted vote — elite wallets count more ───────────────────
            weighted_score = _get_weighted_vote(unique)
            enough_wallets = len(unique) >= adaptive_thresh
            enough_weight = weighted_score >= WEIGHTED_TRIGGER

            should_buy_alert = (
                (enough_wallets or enough_weight)
                and not already_alerted
            )

            logger.info(
                "BUY CHECK | %s | wallets=%d threshold=%d weight=%.2f enough_wallets=%s enough_weight=%s already_alerted=%s should=%s",
                symbol,
                len(unique),
                adaptive_thresh,
                weighted_score,
                enough_wallets,
                enough_weight,
                already_alerted,
                should_buy_alert,
            )

            if should_buy_alert:
                alerted_tokens[mint] = ts

        if should_buy_alert:
            price_data = _get_token_price(mint)

            # Market-cap gate
            mcap = price_data.get("market_cap") or 0
            if mcap > MAX_MCAP:
                logger.info(
                    "Skipping %s — mcap $%s > MAX_MCAP",
                    symbol,
                    f"{mcap:,.0f}",
                )
                with activity_lock:
                    alerted_tokens.pop(mint, None)
                continue

            # Liquidity gate
            liquidity = price_data.get("liquidity_usd") or 0
            if liquidity < MIN_LIQUIDITY:
                logger.info(
                    "Skipping %s — liquidity $%s < MIN_LIQUIDITY",
                    symbol,
                    f"{liquidity:,.0f}",
                )
                with activity_lock:
                    alerted_tokens.pop(mint, None)
                continue

            rug = _check_rug_risk(mint)

            if rug is None:
                logger.error("Rug check returned None for %s", mint)
                rug = {
                    "risk_level": "Unknown",
                    "risk_emoji": "🟡",
                    "safety_score": 50,
                    "lp_burned": None,
                    "mint_disabled": None,
                    "freeze_disabled": None,
                    "flags": [],
                }

            grade_data = _grade_signal(
                unique,
                price_data,
                buy_times,
                rug,
                adaptive_thresh,
            )

            # AI grade filter
            grade_order = {
                "D": 0,
                "C": 1,
                "C+": 2,
                "B-": 3,
                "B": 4,
                "B+": 5,
                "A-": 6,
                "A": 7,
                "A+": 8,
            }

            if grade_order.get(
                grade_data["grade"], 0
            ) < grade_order.get(MIN_AI_GRADE, 0):
                logger.info(
                    "Skipping %s — AI grade %s below %s",
                    symbol,
                    grade_data["grade"],
                    MIN_AI_GRADE,
                )
                with activity_lock:
                    alerted_tokens.pop(mint, None)
                continue

            trade = _trade_assistant(price_data, rug)

            try:
                wi.upsert_token_lifecycle(
                    mint,
                    symbol,
                    price_data.get("price") or 0,
                    price_data.get("market_cap") or 0,
                    ts,
                    liquidity=price_data.get("liquidity_usd"),
                    price_change_5m=price_data.get("price_change_5m"),
                )

                wi.compute_leader_follower_edges(mint)

                stop_price = None
                if trade.get("available"):
                    stop_price = (
                        price_data.get("price", 0)
                        * (1 - trade["sl_pct"] / 100)
                    )

                wi.set_trade_plan(
                    mint,
                    stop_loss_price=stop_price,
                    initial_liquidity=price_data.get("liquidity_usd"),
                )

            except Exception as e:
                logger.debug(
                    "Intelligence alert hooks failed for %s: %s",
                    mint,
                    e,
                )

            _record_alert_time()

            _send_alert(
                mint,
                symbol,
                unique,
                price_data,
                is_sell=False,
                grade_data=grade_data,
                trade=trade,
                rug=rug,
            )

            with buy_alert_lock:
                buy_alert_wallets[mint] = {
                    "wallets": frozenset(unique),
                    "ts": ts,
                    "entry_price": price_data.get("price") or 0,
                }

    # ── SELL detection ────────────────────────────────────────────────────────
    # Exit alert fires ONCE when the SAME wallets that triggered the buy alert
    # start selling the same token. No progressive tracking — one clean alert.
    sold = [t for t in transfers
            if t.get("fromUserAccount") == wallet and t.get("mint", "") != WSOL_MINT]

    for transfer in sold:
        mint = transfer.get("mint", "").strip()
        if not mint:
            continue

        symbol = _resolve_symbol(mint, transfer.get("tokenSymbol"))

        with position_lock:
            try:    del wallet_positions[wallet][mint]
            except Exception: pass

        # Only proceed if we sent a buy alert for this token
        with buy_alert_lock:
            buy_record = buy_alert_wallets.get(mint)

        if not buy_record:
            continue

        original_buyers = buy_record["wallets"]
        buy_alert_time  = buy_record["ts"]
        entry_price     = buy_record.get("entry_price", 0)
        time_since_buy  = ts - buy_alert_time

        # Check if this wallet was one of the original buyers
        if wallet not in original_buyers:
            continue

        # ── Smart-money exit: lightweight early warning the FIRST time any
        # original tracked buyer sells, independent of whether enough
        # wallets exit together to clear the full dump-alert threshold below.
        try:
            if wi.mark_smart_exit_alerted(mint):
                with activity_lock:
                    try:    running_sells = sell_activity[mint]
                    except Exception: running_sells = []
                sellers_so_far = {w for (w, t) in running_sells if ts - t <= SELL_WINDOW} | {wallet}
                _alert_smart_money_exit(
                    mint, symbol, wallet,
                    len(sellers_so_far & original_buyers), len(original_buyers))
        except Exception as e:
            logger.debug("Smart-money exit hook failed for %s: %s", mint, e)

        # Accumulate which original buyers are now selling
        with activity_lock:
            already_sell_alerted = mint in sell_alerted
            if not already_sell_alerted:
                try:    existing_sells = sell_activity[mint]
                except Exception: existing_sells = []
                s_entries = [(w, t) for (w, t) in existing_sells if ts - t <= SELL_WINDOW]
                s_entries.append((wallet, ts))
                sell_activity[mint] = s_entries
                sellers = {w for (w, _) in s_entries} & original_buyers
                sell_count    = len(sellers)
                sell_total    = len(original_buyers)
                overlap_ratio = sell_count / sell_total if sell_total else 0

                within_hold_window = time_since_buy < MIN_HOLD_TIME
                fast_dump = sell_count >= FAST_DUMP_MIN_SELLERS

                if within_hold_window and not fast_dump:
                    # Not enough sellers yet to override the hold-time gate — keep waiting
                    logger.debug(
                        "Sell suppressed for %s — only %ds since buy (min %ds), %d/%d sellers",
                        symbol, time_since_buy, MIN_HOLD_TIME, sell_count, sell_total)
                    should_sell = False
                else:
                    # Fire ONE sell alert when same-wallet threshold is met, OR
                    # when FAST_DUMP_MIN_SELLERS+ original buyers dump together
                    # (this overrides MIN_HOLD_TIME since a fast multi-wallet exit
                    # is itself the strongest possible signal)
                    should_sell = (
                        sell_count >= _get_adaptive_threshold() or
                        overlap_ratio >= 0.5 or
                        fast_dump
                    )
                if should_sell:
                    sell_alerted[mint] = ts
            else:
                should_sell = False
                sellers     = set()
                sell_count  = 0
                sell_total  = len(original_buyers)
                entry_price = buy_record.get("entry_price", 0)

        if not should_sell:
            continue

        price_data    = _get_token_price(mint, bypass_cache=True)
        current_price = price_data.get("price") or 0
        roi_pct       = ((current_price - entry_price) / entry_price * 100
                         if entry_price and current_price else 0)
        hold_secs     = time_since_buy
        win           = roi_pct > 0

        # Record outcome for wallet ranking
        for w in original_buyers:
            _record_trade_outcome(w, win=win, roi_pct=roi_pct, hold_seconds=hold_secs)
            if win:
                _record_win(w)
            try:
                wi.record_trade_outcome_extended(w, win=win, roi_pct=roi_pct)
            except Exception as e:
                logger.debug("Confidence-score update failed for %s: %s", w, e)

        # Refresh this token's lifecycle row with the exit price too, so ATH/ROI
        # tracking doesn't go stale between now and the next background pass.
        try:
            wi.upsert_token_lifecycle(
                mint, symbol, current_price, price_data.get("market_cap") or 0, ts,
                liquidity=price_data.get("liquidity_usd"),
                price_change_5m=price_data.get("price_change_5m"))
        except Exception as e:
            logger.debug("Lifecycle update on sell failed for %s: %s", mint, e)

        _send_alert(mint, symbol, sellers, price_data,
                    is_sell=True, sell_count=sell_count, sell_total=sell_total)

        # Position is fully exited — stop the risk monitor polling this mint
        # for drawdown/stop-loss/rug signals until it's bought again.
        try:
            wi.close_trade(mint)
        except Exception as e:
            logger.debug("close_trade failed for %s: %s", mint, e)

        with buy_alert_lock:
            try:    del buy_alert_wallets[mint]
            except Exception: pass


# ═══════════════════════════════════════════════════════════════════════════════
#  DAILY REPORT
# ═══════════════════════════════════════════════════════════════════════════════

def send_daily_report():
    from datetime import date
    today = str(date.today())
    with wallet_wins_lock:
        todays = {
            w: rec["wins"]
            for w, rec in wallet_wins.items()
            if rec.get("last_reset") == today and rec.get("wins", 0) > 0
        }

    if not todays:
        try:
            bot.send_message(chat_id=CHAT_ID,
                             text="📋 <b>Daily Report</b>\n\nNo wins recorded today.",
                             parse_mode="HTML")
        except TelegramError: pass
        return

    ranked  = sorted(todays.items(), key=lambda x: x[1], reverse=True)
    medals  = ["🥇", "🥈", "🥉"]
    lines   = ["📋 <b>Top Wallets Today</b>\n"]

    for i, (wallet, wins) in enumerate(ranked[:10]):
        medal  = medals[i] if i < 3 else f"{i+1}."
        short  = f"{wallet[:4]}…{wallet[-4:]}"
        score  = _get_wallet_score(wallet)
        stats  = _get_wallet_stats_display(wallet)
        plural = "win" if wins == 1 else "wins"
        lines.append(
            f"{medal} <code>{short}</code> — <b>{wins} {plural}</b>\n"
            f"   Score: <b>{score:.2f}</b> | {_esc(stats)}"
        )

    lines.append(f"\n<i>{today} | {len(FALLBACK_WALLETS)} wallets tracked</i>")

    try:
        bot.send_message(chat_id=CHAT_ID, text="\n".join(lines), parse_mode="HTML")
        logger.info("Daily report sent — %d wallets with wins", len(ranked))
    except TelegramError as e:
        logger.error("Daily report failed: %s", e)


# ═══════════════════════════════════════════════════════════════════════════════
#  FLASK ROUTES
# ═══════════════════════════════════════════════════════════════════════════════

@app.route("/helius-webhook", methods=["POST"])
def helius_webhook():
    if request.headers.get("Authorization") != WEBHOOK_SECRET:
        return jsonify({"status": "unauthorized"}), 401
    payload = request.get_json(silent=True)
    if not payload:
        return jsonify({"status": "bad_request"}), 400
    ts = int(time.time())
    txs = payload if isinstance(payload, list) else [payload]
    for tx in txs:
        _tx_executor.submit(_safe_process_tx, tx, ts)
    return jsonify({"status": "ok"}), 200


@app.route("/status", methods=["GET"])
def status():
    with wallet_lock:
        count = len(watched_wallets)
    with wallet_stats_lock:
        ranked = sorted(
            [(w, _get_wallet_score(w)) for w in wallet_stats],
            key=lambda x: x[1], reverse=True
        )[:5]
    return jsonify({
        "status":            "ok",
        "watching_wallets":  count,
        "webhook_id":        current_webhook_id,
        "app_url":           APP_URL,
        "wallet_count":      len(FALLBACK_WALLETS),
        "jupiter_symbols":   len(_jupiter_token_map),
        "top_wallets":       [{"wallet": w[:8]+"…", "score": s} for w, s in ranked],
    }), 200


@app.route("/wallets", methods=["GET"])
def list_wallets():
    with wallet_lock:
        wl = list(watched_wallets)
    return jsonify({"count": len(wl), "wallets": wl}), 200


@app.route("/refresh", methods=["POST"])
def force_refresh():
    threading.Thread(target=refresh_wallets, daemon=True).start()
    return jsonify({"status": "refresh started"}), 200


@app.route("/report", methods=["GET"])
def daily_report_route():
    threading.Thread(target=send_daily_report, daemon=True).start()
    return jsonify({"status": "report sending"}), 200


@app.route("/wallet-stats", methods=["GET"])
def wallet_stats_route():
    """Show all wallet scores and stats. Snapshot first to avoid lock contention."""
    with wallet_stats_lock:
        snapshot = dict(wallet_stats)   # release lock before calling _get_wallet_score
    data = {
        w: {
            "score":   _get_wallet_score(w),   # acquires wallet_stats_lock internally — safe now
            "wins":    s["wins"],
            "losses":  s["losses"],
            "trades":  s["trades"],
            "avg_roi": round(s["total_roi"] / s["trades"], 2) if s["trades"] else 0,
        }
        for w, s in snapshot.items()
    }
    return jsonify({"wallet_stats": data}), 200


@app.route("/confidence", methods=["GET"])
def confidence_route():
    """Wallet confidence scores + tiers for every watched wallet."""
    with wallet_lock:
        wallets = list(watched_wallets)
    data = {}
    for w in wallets:
        try:
            data[w] = {
                "confidence": wi.get_wallet_confidence_raw(w),
                "tier":       wi.get_wallet_tier(w),
                "early_entry_score": wi.get_wallet_early_entry_score(w),
                "leader_score":      wi.get_wallet_leader_score(w),
            }
        except Exception as e:
            data[w] = {"error": str(e)}
    return jsonify({"wallets": data}), 200


@app.route("/tiers", methods=["GET"])
def tiers_route():
    """Wallets grouped by current tier."""
    with wallet_lock:
        wallets = list(watched_wallets)
    grouped = wi.get_wallets_by_tier(wallets)
    return jsonify({"tiers": grouped}), 200


@app.route("/tier-history/<wallet>", methods=["GET"])
def tier_history_route(wallet):
    """Promotion/demotion history for a single wallet, most recent first."""
    limit = request.args.get("limit", default=20, type=int)
    return jsonify({"wallet": wallet, "history": wi.get_tier_history(wallet, limit=limit)}), 200


@app.route("/leaders", methods=["GET"])
def leaders_route():
    """Top leader wallets by influence score."""
    return jsonify({"leaders": wi.get_top_leaders(15)}), 200


@app.route("/lifecycle/<mint>", methods=["GET"])
def lifecycle_route(mint):
    data = wi.get_token_lifecycle(mint)
    if not data:
        return jsonify({"status": "not_found"}), 404
    return jsonify({"lifecycle": data}), 200


@app.route("/lifecycle-summary", methods=["GET"])
def lifecycle_summary_route():
    return jsonify(wi.get_lifecycle_summary_stats()), 200


@app.route("/open-trades", methods=["GET"])
def open_trades_route():
    """Mints currently being risk-monitored (drawdown/stop-loss/rug) with their lifecycle state."""
    mints = wi.get_open_trade_mints()
    data = {m: wi.get_token_lifecycle(m) for m in mints}
    return jsonify({"open_trades": data, "count": len(mints)}), 200


@app.route("/dashboard", methods=["GET"])
def dashboard_route():
    """Same rollup stats shown in the startup message, on demand."""
    with wallet_lock:
        wallets = list(watched_wallets)
    try:
        return jsonify(wi.get_dashboard_stats(wallets)), 200
    except Exception as e:
        return jsonify({"status": "error", "detail": str(e)}), 500


@app.route("/discovery-stats", methods=["GET"])
def discovery_stats_route():
    return jsonify(wi.get_discovery_stats()), 200


@app.route("/run-tier-cycle", methods=["POST"])
def run_tier_cycle_route():
    threading.Thread(target=_run_promotion_demotion_job, daemon=True).start()
    return jsonify({"status": "tier cycle started"}), 200


@app.route("/run-discovery-cycle", methods=["POST"])
def run_discovery_cycle_route():
    threading.Thread(target=_run_discovery_job, daemon=True).start()
    return jsonify({"status": "discovery cycle started"}), 200


# ═══════════════════════════════════════════════════════════════════════════════
#  STARTUP
# ═══════════════════════════════════════════════════════════════════════════════
#
# IMPORTANT: this runs unconditionally at module *import* time, not just under
# `if __name__ == "__main__"`. That's deliberate — on Render (and most other
# PaaS deploys) the app is served by gunicorn, which imports this module and
# looks up the `app` object rather than executing the file as a script. If
# webhook registration / the scheduler only lived behind the __main__ guard,
# none of it would ever run under gunicorn. A `_bootstrapped` flag keeps this
# idempotent in case the module ever gets imported more than once.

_bootstrapped = False


def _bootstrap():
    global _bootstrapped
    if _bootstrapped:
        return
    _bootstrapped = True

    ensure_font()
    _build_template(False)
    _build_template(True)
    logger.info("🖼 Card templates cached")

    threading.Thread(target=_load_jupiter_token_map, daemon=True).start()

    logger.info("🚀 Starting SpiderWalletBot...")
    refresh_wallets()
    _start_refresh_scheduler()


_bootstrap()

if __name__ == "__main__":
    # Local/dev entry point only. On Render, gunicorn serves `app` directly
    # (see Procfile / render.yaml) and this block never executes — bootstrap
    # already ran above at import time.
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=False)
