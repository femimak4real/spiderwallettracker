"""
wallet_intelligence.py — SpiderWalletBot Intelligence Layer
=============================================================
Adds six subsystems on top of the existing wallet-convergence bot:

  1. Early Entry Score        — how early a wallet gets into a token relative to launch
  2. Token Lifecycle Database — persistent per-token history, ATH, 2x/5x/10x, ROI
  3. Leader Wallet Detection  — who buys first and who follows them
  4. Wallet Confidence Score  — combined score replacing the old win-rate-only formula
  5. Promotion / Demotion     — dynamic tiers, automatic retirement of weak wallets
  6. Wallet Discovery Engine  — finds new candidate wallets and rotates them in

Everything here is additive: import this module from the main bot file and
call the hook functions at the marked integration points. All state is
persisted to a local SQLite database so scores/tiers survive restarts.

Thread-safety: a single sqlite3 connection is shared across threads, guarded
by one global RLock (_DB_LOCK). This is simple and correct for the traffic
volumes involved here (webhook events at human-wallet-trading speed, not HFT).
"""

import os
import time
import math
import sqlite3
import logging
import threading
import statistics
from collections import defaultdict

logger = logging.getLogger("wallet_intelligence")

# ═══════════════════════════════════════════════════════════════════════════════
#  CONFIG
# ═══════════════════════════════════════════════════════════════════════════════

DB_PATH = os.environ.get("WALLET_INTEL_DB", os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "wallet_intel.db"))

# ── Early entry windows (seconds since launch → score 0-100) ──────────────────
EARLY_ENTRY_BUCKETS = [
    (60,     100),   # first minute
    (300,     90),   # < 5 min
    (900,     75),   # < 15 min
    (3600,    55),   # < 1 hr
    (21600,   30),   # < 6 hr
    (86400,   12),   # < 24 hr
]
EARLY_ENTRY_FLOOR = 3   # anything older than the last bucket

# Bonus for being among the very first buyers of a token (by order, not time)
ENTRY_RANK_BONUS = {1: 15, 2: 10, 3: 6, 4: 3, 5: 1}

# ── Token lifecycle ─────────────────────────────────────────────────────────
LIFECYCLE_ACTIVE_HOURS   = 72     # keep refreshing prices for this long after launch
LIFECYCLE_REFRESH_SECS   = 900    # background updater cadence
MULTIPLIER_TARGETS       = (2.0, 5.0, 10.0)

# ── Risk monitoring: drawdown / stop-loss / rug-pull ────────────────────────
# Faster cadence than the lifecycle updater — drawdowns and rug pulls can
# happen in minutes, so open trades get checked more often than the general
# ATH/ROI refresh.
RISK_MONITOR_INTERVAL_SECS = 180    # 3 min
DRAWDOWN_THRESHOLDS_PCT    = [20, 30, 50]   # % below ATH — fires once per threshold per trade cycle
RUG_LIQUIDITY_DROP_PCT     = 60     # liquidity falling below this % of its initial value is a red flag...
RUG_LIQUIDITY_FLOOR_USD    = 10_000 # ...but only counts as a rug signal below this absolute floor too
RUG_PRICE_CRASH_PCT        = 50     # 5-minute price change <= -50% is treated as a crash signal

# ── Leader detection ────────────────────────────────────────────────────────
LEADER_LAG_WINDOW        = 90     # seconds — a "follow" must happen within this of the leader's buy
MIN_COOCCURRENCES        = 4      # minimum shared-token occurrences before a leader/follower edge counts

# ── Confidence score weights (must sum to 1.0) ─────────────────────────────
CONF_WEIGHTS = {
    "roi":          0.30,
    "hit_rate":     0.20,
    "early_entry":  0.20,
    "leader":       0.15,
    "consistency":  0.15,
}
MIN_TRADES_FOR_CONFIDENCE = 3

# ── Tiers ────────────────────────────────────────────────────────────────────
TIERS = ["RETIRED", "PROBATION", "STANDARD", "STRONG", "ELITE"]
TIER_THRESHOLDS = {
    "ELITE":     80,
    "STRONG":    62,
    "STANDARD":  40,
    "PROBATION": 20,
    # below PROBATION threshold with enough trades → RETIRED
}
MIN_TRADES_FOR_TIER_CHANGE = 5
RETIREMENT_MIN_TRADES      = 8
RETIREMENT_MAX_SCORE       = 18

# ── Discovery engine ─────────────────────────────────────────────────────────
DISCOVERY_MIN_OBSERVATIONS = 3     # candidate needs this many observed trades before eligible
DISCOVERY_MIN_WINS         = 2
DISCOVERY_MIN_SCORE        = 45    # confidence-equivalent score needed to graduate
DISCOVERY_MAX_ACTIVE_WATCH = 200   # cap on candidates tracked simultaneously
TARGET_ACTIVE_WALLET_COUNT = 50    # keep the live watched-wallet roster around this size

_DB_LOCK = threading.RLock()
_conn = None


# ═══════════════════════════════════════════════════════════════════════════════
#  DB BOOTSTRAP
# ═══════════════════════════════════════════════════════════════════════════════

def _get_conn():
    global _conn
    if _conn is not None:
        return _conn
    with _DB_LOCK:
        if _conn is None:                       # re-check inside the lock
            conn = sqlite3.connect(DB_PATH, check_same_thread=False)
            conn.execute("PRAGMA journal_mode=WAL")
            _init_schema(conn)
            _migrate_schema(conn)
            _conn = conn
    return _conn


def _init_schema(conn):
    conn.executescript("""
    CREATE TABLE IF NOT EXISTS token_launches (
        mint            TEXT PRIMARY KEY,
        launch_ts       INTEGER NOT NULL,
        first_symbol    TEXT
    );

    CREATE TABLE IF NOT EXISTS early_entries (
        mint            TEXT NOT NULL,
        wallet          TEXT NOT NULL,
        buy_ts          INTEGER NOT NULL,
        entry_rank      INTEGER NOT NULL,
        seconds_since_launch INTEGER NOT NULL,
        score           REAL NOT NULL,
        PRIMARY KEY (mint, wallet)
    );

    CREATE TABLE IF NOT EXISTS wallet_early_entry_avg (
        wallet          TEXT PRIMARY KEY,
        avg_score       REAL NOT NULL,
        sample_count    INTEGER NOT NULL,
        last_updated    INTEGER NOT NULL
    );

    CREATE TABLE IF NOT EXISTS token_lifecycle (
        mint                TEXT PRIMARY KEY,
        symbol              TEXT,
        first_price         REAL,
        first_mcap          REAL,
        first_ts            INTEGER,
        ath_price           REAL,
        ath_mcap            REAL,
        ath_ts              INTEGER,
        last_price          REAL,
        last_mcap           REAL,
        last_updated        INTEGER,
        hit_2x              INTEGER DEFAULT 0,
        hit_5x              INTEGER DEFAULT 0,
        hit_10x             INTEGER DEFAULT 0,
        hit_2x_ts           INTEGER,
        hit_5x_ts           INTEGER,
        hit_10x_ts          INTEGER,
        roi_pct             REAL DEFAULT 0,
        status              TEXT DEFAULT 'active',
        -- risk-monitoring fields (drawdown / stop-loss / rug / smart-exit)
        initial_liquidity   REAL,
        last_liquidity      REAL,
        stop_loss_price     REAL,
        dd_alerted_pct      INTEGER DEFAULT 0,
        stop_loss_alerted   INTEGER DEFAULT 0,
        rug_alerted         INTEGER DEFAULT 0,
        smart_exit_alerted  INTEGER DEFAULT 0
    );

    CREATE TABLE IF NOT EXISTS buy_sequences (
        mint            TEXT NOT NULL,
        wallet          TEXT NOT NULL,
        buy_ts          INTEGER NOT NULL,
        seq_position    INTEGER NOT NULL,
        PRIMARY KEY (mint, wallet)
    );

    CREATE TABLE IF NOT EXISTS leader_edges (
        leader          TEXT NOT NULL,
        follower        TEXT NOT NULL,
        cooccurrences   INTEGER DEFAULT 0,
        led_count       INTEGER DEFAULT 0,
        PRIMARY KEY (leader, follower)
    );

    CREATE TABLE IF NOT EXISTS leader_scores (
        wallet          TEXT PRIMARY KEY,
        influence_score REAL DEFAULT 0,
        distinct_followers INTEGER DEFAULT 0,
        times_led       INTEGER DEFAULT 0,
        last_updated    INTEGER
    );

    CREATE TABLE IF NOT EXISTS wallet_metrics (
        wallet          TEXT PRIMARY KEY,
        trades          INTEGER DEFAULT 0,
        wins            INTEGER DEFAULT 0,
        losses          INTEGER DEFAULT 0,
        total_roi       REAL DEFAULT 0,
        roi_samples     TEXT DEFAULT ''   -- comma-separated recent ROI values, bounded
    );

    CREATE TABLE IF NOT EXISTS wallet_confidence (
        wallet          TEXT PRIMARY KEY,
        confidence      REAL DEFAULT 50,
        roi_score       REAL DEFAULT 0,
        hit_rate_score  REAL DEFAULT 0,
        early_entry_score REAL DEFAULT 0,
        leader_score    REAL DEFAULT 0,
        consistency_score REAL DEFAULT 0,
        tier            TEXT DEFAULT 'STANDARD',
        last_updated    INTEGER
    );

    CREATE TABLE IF NOT EXISTS tier_history (
        id              INTEGER PRIMARY KEY AUTOINCREMENT,
        wallet          TEXT NOT NULL,
        old_tier        TEXT,
        new_tier        TEXT,
        reason          TEXT,
        ts              INTEGER
    );

    CREATE TABLE IF NOT EXISTS active_roster (
        wallet          TEXT PRIMARY KEY,
        added_ts        INTEGER,
        source          TEXT DEFAULT 'seed',    -- 'seed' | 'discovery'
        status          TEXT DEFAULT 'active'   -- 'active' | 'retired'
    );

    CREATE TABLE IF NOT EXISTS discovery_candidates (
        wallet          TEXT PRIMARY KEY,
        first_seen_ts   INTEGER,
        observations    INTEGER DEFAULT 0,
        wins            INTEGER DEFAULT 0,
        total_roi       REAL DEFAULT 0,
        early_entry_hits INTEGER DEFAULT 0,
        status          TEXT DEFAULT 'watching'  -- watching | graduated | rejected
    );

    -- Persisted mirror of the bot's legacy in-RAM wallet_stats dict (win/loss/
    -- roi/hold-time tracking used by the pre-confidence-score formula and the
    -- daily report). Kept schema-compatible with that dict so load/save is a
    -- straight round-trip with no translation logic on the bot side.
    CREATE TABLE IF NOT EXISTS legacy_wallet_stats (
        wallet          TEXT PRIMARY KEY,
        wins            INTEGER DEFAULT 0,
        losses          INTEGER DEFAULT 0,
        total_roi       REAL DEFAULT 0,
        trades          INTEGER DEFAULT 0,
        hold_times      TEXT DEFAULT ''   -- comma-separated seconds, bounded to 200
    );

    -- Persisted mirror of the bot's legacy in-RAM wallet_wins dict (today's
    -- win count per wallet, used for the daily leaderboard).
    CREATE TABLE IF NOT EXISTS daily_wallet_wins (
        wallet          TEXT PRIMARY KEY,
        wins            INTEGER DEFAULT 0,
        last_reset      TEXT
    );
    """)
    conn.commit()


def _migrate_schema(conn):
    """
    Additive column migrations for DBs created by older versions of this
    module. Each ALTER is wrapped individually — SQLite has no
    'ADD COLUMN IF NOT EXISTS', so we just ignore the "duplicate column"
    error when a column is already there.
    """
    migrations = [
        ("active_roster",   "status",             "TEXT DEFAULT 'active'"),
        ("token_lifecycle", "initial_liquidity",  "REAL"),
        ("token_lifecycle", "last_liquidity",     "REAL"),
        ("token_lifecycle", "stop_loss_price",    "REAL"),
        ("token_lifecycle", "dd_alerted_pct",     "INTEGER DEFAULT 0"),
        ("token_lifecycle", "stop_loss_alerted",  "INTEGER DEFAULT 0"),
        ("token_lifecycle", "rug_alerted",        "INTEGER DEFAULT 0"),
        ("token_lifecycle", "smart_exit_alerted", "INTEGER DEFAULT 0"),
    ]
    for table, column, coltype in migrations:
        try:
            conn.execute(f"ALTER TABLE {table} ADD COLUMN {column} {coltype}")
            conn.commit()
        except sqlite3.OperationalError:
            pass   # column already exists — fine

    # Backfill: any pre-existing active_roster rows from before the status
    # column existed default to NULL, not 'active' — normalize them once.
    try:
        conn.execute("UPDATE active_roster SET status='active' WHERE status IS NULL")
        conn.commit()
    except sqlite3.OperationalError:
        pass


# ═══════════════════════════════════════════════════════════════════════════════
#  1. EARLY ENTRY SCORE
# ═══════════════════════════════════════════════════════════════════════════════

def record_token_launch(mint: str, ts: int, symbol: str = ""):
    """
    Registers the first time this bot has ever observed activity on a mint as
    its 'launch' reference point. Idempotent — only the first call sticks.
    Downstream code treats this timestamp as t=0 for early-entry scoring.
    """
    conn = _get_conn()
    with _DB_LOCK:
        conn.execute(
            "INSERT OR IGNORE INTO token_launches (mint, launch_ts, first_symbol) VALUES (?,?,?)",
            (mint, ts, symbol))
        conn.commit()


def get_token_launch_ts(mint: str):
    conn = _get_conn()
    with _DB_LOCK:
        row = conn.execute(
            "SELECT launch_ts FROM token_launches WHERE mint=?", (mint,)).fetchone()
    return row[0] if row else None


def _entry_rank_for(mint: str) -> int:
    """How many wallets have already bought this mint (per our early_entries table)."""
    conn = _get_conn()
    with _DB_LOCK:
        row = conn.execute(
            "SELECT COUNT(*) FROM early_entries WHERE mint=?", (mint,)).fetchone()
    return (row[0] if row else 0) + 1


def _score_from_elapsed(elapsed_secs: int) -> float:
    for window_secs, score in EARLY_ENTRY_BUCKETS:
        if elapsed_secs <= window_secs:
            return float(score)
    return float(EARLY_ENTRY_FLOOR)


def compute_early_entry_score(wallet: str, mint: str, buy_ts: int) -> float:
    """
    Scores 0-100 how early `wallet` entered `mint` relative to the token's
    launch timestamp (first time the bot ever saw it), plus a bonus for
    being one of the first N buyers by strict order.

    Call this once per (wallet, mint) — first buy only. Idempotent: repeat
    calls for the same pair are ignored (early entry is a one-time property
    of a wallet's relationship to a token).
    """
    conn = _get_conn()
    with _DB_LOCK:
        existing = conn.execute(
            "SELECT score FROM early_entries WHERE mint=? AND wallet=?",
            (mint, wallet)).fetchone()
        if existing:
            return existing[0]

    launch_ts = get_token_launch_ts(mint)
    if launch_ts is None:
        # Shouldn't normally happen — record_token_launch should run first —
        # but fall back to treating this buy as the launch itself.
        record_token_launch(mint, buy_ts)
        launch_ts = buy_ts

    elapsed = max(buy_ts - launch_ts, 0)
    base_score = _score_from_elapsed(elapsed)

    rank = _entry_rank_for(mint)
    bonus = ENTRY_RANK_BONUS.get(rank, 0)
    final_score = min(base_score + bonus, 100.0)

    conn = _get_conn()
    with _DB_LOCK:
        conn.execute(
            "INSERT OR IGNORE INTO early_entries "
            "(mint, wallet, buy_ts, entry_rank, seconds_since_launch, score) "
            "VALUES (?,?,?,?,?,?)",
            (mint, wallet, buy_ts, rank, elapsed, final_score))
        conn.commit()

    _update_wallet_early_entry_avg(wallet)
    return final_score


def _update_wallet_early_entry_avg(wallet: str):
    conn = _get_conn()
    with _DB_LOCK:
        rows = conn.execute(
            "SELECT score FROM early_entries WHERE wallet=?", (wallet,)).fetchall()
        if not rows:
            return
        scores = [r[0] for r in rows]
        avg = sum(scores) / len(scores)
        conn.execute(
            "INSERT INTO wallet_early_entry_avg (wallet, avg_score, sample_count, last_updated) "
            "VALUES (?,?,?,?) "
            "ON CONFLICT(wallet) DO UPDATE SET avg_score=excluded.avg_score, "
            "sample_count=excluded.sample_count, last_updated=excluded.last_updated",
            (wallet, avg, len(scores), int(time.time())))
        conn.commit()


def get_wallet_early_entry_score(wallet: str) -> float:
    """0-100. Returns 50 (neutral) if we have no data yet."""
    conn = _get_conn()
    with _DB_LOCK:
        row = conn.execute(
            "SELECT avg_score FROM wallet_early_entry_avg WHERE wallet=?", (wallet,)).fetchone()
    return row[0] if row else 50.0


def get_token_entry_rank(mint: str, wallet: str):
    """Returns the buy-order rank of `wallet` for `mint`, or None if unknown."""
    conn = _get_conn()
    with _DB_LOCK:
        row = conn.execute(
            "SELECT entry_rank FROM early_entries WHERE mint=? AND wallet=?",
            (mint, wallet)).fetchone()
    return row[0] if row else None


# ═══════════════════════════════════════════════════════════════════════════════
#  2. TOKEN LIFECYCLE DATABASE
# ═══════════════════════════════════════════════════════════════════════════════

def upsert_token_lifecycle(mint: str, symbol: str, price: float, mcap: float, ts: int,
                           liquidity: float = None, price_change_5m: float = None) -> dict:
    """
    Records/updates a token's price history: first-seen price, ATH, and
    2x/5x/10x milestones (measured off the first recorded price, which is
    treated as our best proxy for 'entry price' at discovery time).

    Also runs the risk-monitoring checks (drawdown from ATH, stop-loss
    breach, rug-pull signals from liquidity/price-crash) when `liquidity`
    and/or `price_change_5m` are supplied — both are optional so existing
    callers that only track price/mcap keep working unchanged.

    Returns the current lifecycle row as a dict, with any newly-triggered
    events surfaced under:
      _newly_crossed     — list[int]  multiplier milestones just crossed (2/5/10)
      _new_drawdowns     — list[int]  drawdown %% thresholds just crossed (20/30/50)
      _stop_loss_breached — bool      True the moment price first drops to/below the stop
      _rug_signals       — list[str] reasons just triggered ("liquidity_removed", "price_crash")
    """
    if not price or price <= 0:
        return {}

    conn = _get_conn()
    with _DB_LOCK:
        row = conn.execute("SELECT * FROM token_lifecycle WHERE mint=?", (mint,)).fetchone()
        cols = [d[0] for d in conn.execute("SELECT * FROM token_lifecycle LIMIT 0").description]

        if row is None:
            conn.execute(
                "INSERT INTO token_lifecycle "
                "(mint, symbol, first_price, first_mcap, first_ts, ath_price, ath_mcap, "
                " ath_ts, last_price, last_mcap, last_updated, roi_pct, status, last_liquidity) "
                "VALUES (?,?,?,?,?,?,?,?,?,?,?,0,'active',?)",
                (mint, symbol, price, mcap, ts, price, mcap, ts, price, mcap, ts, liquidity))
            conn.commit()
            row = conn.execute("SELECT * FROM token_lifecycle WHERE mint=?", (mint,)).fetchone()

        data = dict(zip(cols, row))
        first_price = data["first_price"] or price
        prev_ath    = data["ath_price"] or price

        newly_crossed  = []
        new_drawdowns  = []
        stop_breached  = False
        rug_signals    = []

        new_ath = price > prev_ath
        if new_ath:
            ath_price, ath_ts, ath_mcap = price, ts, mcap
        else:
            ath_price, ath_ts, ath_mcap = prev_ath, data["ath_ts"], data["ath_mcap"]

        roi_pct      = ((price - first_price) / first_price * 100) if first_price else 0
        ath_multiple = (ath_price / first_price) if first_price else 0

        # ── 2x / 5x / 10x milestones ────────────────────────────────────────
        hit_2x, hit_5x, hit_10x = data["hit_2x"], data["hit_5x"], data["hit_10x"]
        hit_2x_ts, hit_5x_ts, hit_10x_ts = data["hit_2x_ts"], data["hit_5x_ts"], data["hit_10x_ts"]

        if ath_multiple >= 2.0 and not hit_2x:
            hit_2x, hit_2x_ts = 1, ts
            newly_crossed.append(2)
        if ath_multiple >= 5.0 and not hit_5x:
            hit_5x, hit_5x_ts = 1, ts
            newly_crossed.append(5)
        if ath_multiple >= 10.0 and not hit_10x:
            hit_10x, hit_10x_ts = 1, ts
            newly_crossed.append(10)

        # ── Drawdown from ATH ────────────────────────────────────────────────
        # A fresh ATH means "distance from the peak" is reset — any drawdown
        # alerts fired against the OLD peak no longer describe the current
        # situation, so clear the alerted-threshold marker.
        dd_alerted_pct = 0 if new_ath else (data["dd_alerted_pct"] or 0)
        dd_pct = ((ath_price - price) / ath_price * 100) if ath_price else 0
        if dd_pct > 0:
            for threshold in DRAWDOWN_THRESHOLDS_PCT:
                if dd_pct >= threshold and threshold > dd_alerted_pct:
                    new_drawdowns.append(threshold)
            if new_drawdowns:
                dd_alerted_pct = max(new_drawdowns)

        # ── Stop-loss breach ─────────────────────────────────────────────────
        stop_loss_price   = data["stop_loss_price"]
        stop_loss_alerted = data["stop_loss_alerted"] or 0
        if stop_loss_price and not stop_loss_alerted and price <= stop_loss_price:
            stop_loss_alerted = 1
            stop_breached = True

        # ── Rug-pull signals: liquidity removal + rapid price crash ─────────
        rug_alerted       = data["rug_alerted"] or 0
        initial_liquidity = data["initial_liquidity"]
        last_liquidity     = data["last_liquidity"]
        if liquidity is not None:
            last_liquidity = liquidity
        if not rug_alerted:
            if (initial_liquidity and liquidity is not None and initial_liquidity > 0):
                liq_ratio = liquidity / initial_liquidity
                if (liq_ratio <= (1 - RUG_LIQUIDITY_DROP_PCT / 100)
                        and liquidity < RUG_LIQUIDITY_FLOOR_USD):
                    rug_signals.append("liquidity_removed")
            if price_change_5m is not None and price_change_5m <= -RUG_PRICE_CRASH_PCT:
                rug_signals.append("price_crash")
            if rug_signals:
                rug_alerted = 1

        conn.execute(
            "UPDATE token_lifecycle SET symbol=?, ath_price=?, ath_mcap=?, ath_ts=?, "
            "last_price=?, last_mcap=?, last_updated=?, hit_2x=?, hit_5x=?, hit_10x=?, "
            "hit_2x_ts=?, hit_5x_ts=?, hit_10x_ts=?, roi_pct=?, dd_alerted_pct=?, "
            "stop_loss_alerted=?, rug_alerted=?, last_liquidity=? WHERE mint=?",
            (symbol or data["symbol"], ath_price, ath_mcap, ath_ts, price, mcap, ts,
             hit_2x, hit_5x, hit_10x, hit_2x_ts, hit_5x_ts, hit_10x_ts, roi_pct,
             dd_alerted_pct, stop_loss_alerted, rug_alerted, last_liquidity, mint))
        conn.commit()

        data.update({
            "ath_price": ath_price, "ath_mcap": ath_mcap, "ath_ts": ath_ts,
            "last_price": price, "last_mcap": mcap, "last_updated": ts,
            "hit_2x": hit_2x, "hit_5x": hit_5x, "hit_10x": hit_10x,
            "roi_pct": roi_pct, "dd_alerted_pct": dd_alerted_pct,
            "stop_loss_alerted": stop_loss_alerted, "rug_alerted": rug_alerted,
            "last_liquidity": last_liquidity,
            "_newly_crossed": newly_crossed,
            "_new_drawdowns": new_drawdowns,
            "_drawdown_pct": round(dd_pct, 1),
            "_stop_loss_breached": stop_breached,
            "_rug_signals": rug_signals,
        })
        return data


def set_trade_plan(mint: str, stop_loss_price: float = None, initial_liquidity: float = None):
    """
    Call once when a fresh buy alert fires for a mint — records the
    suggested stop-loss price and the liquidity baseline, and resets all
    risk-alert flags so this new trade cycle gets fresh drawdown/stop-loss/
    rug/smart-exit alerts rather than staying suppressed by a previous
    cycle's history on the same mint.
    """
    conn = _get_conn()
    with _DB_LOCK:
        row = conn.execute("SELECT mint FROM token_lifecycle WHERE mint=?", (mint,)).fetchone()
        if row is None:
            # Lifecycle row doesn't exist yet (shouldn't normally happen —
            # the buy-alert hook upserts lifecycle first — but guard anyway).
            conn.execute(
                "INSERT INTO token_lifecycle (mint, status) VALUES (?, 'active')", (mint,))
        conn.execute(
            "UPDATE token_lifecycle SET stop_loss_price=?, initial_liquidity=?, "
            "last_liquidity=COALESCE(?, last_liquidity), dd_alerted_pct=0, "
            "stop_loss_alerted=0, rug_alerted=0, smart_exit_alerted=0 WHERE mint=?",
            (stop_loss_price, initial_liquidity, initial_liquidity, mint))
        conn.commit()


def mark_smart_exit_alerted(mint: str) -> bool:
    """
    Idempotent check-and-set for the smart-money-exit alert: returns True
    the first time it's called for this trade cycle (meaning the caller
    should send the alert), and False on every subsequent call until
    set_trade_plan() resets it for a new cycle on the same mint.
    """
    conn = _get_conn()
    with _DB_LOCK:
        row = conn.execute(
            "SELECT smart_exit_alerted FROM token_lifecycle WHERE mint=?", (mint,)).fetchone()
        if row is None:
            conn.execute(
                "INSERT INTO token_lifecycle (mint, status, smart_exit_alerted) "
                "VALUES (?, 'active', 1)", (mint,))
            conn.commit()
            return True
        if row[0]:
            return False
        conn.execute("UPDATE token_lifecycle SET smart_exit_alerted=1 WHERE mint=?", (mint,))
        conn.commit()
        return True


def close_trade(mint: str):
    """
    Call when a position is fully exited (the full sell/dump alert has
    fired). Clears the stop-loss price so this mint drops out of
    get_open_trade_mints() and the risk monitor stops polling it — there's
    nothing left to protect once the position is closed. Drawdown/rug/
    smart-exit flags are left as-is (historical record); set_trade_plan()
    will reset them again if this mint gets bought back into a new cycle.
    """
    conn = _get_conn()
    with _DB_LOCK:
        conn.execute(
            "UPDATE token_lifecycle SET stop_loss_price=NULL WHERE mint=?", (mint,))
        conn.commit()


def get_token_lifecycle(mint: str) -> dict:
    conn = _get_conn()
    with _DB_LOCK:
        row = conn.execute("SELECT * FROM token_lifecycle WHERE mint=?", (mint,)).fetchone()
        if not row:
            return {}
        cols = [d[0] for d in conn.execute("SELECT * FROM token_lifecycle LIMIT 0").description]
    return dict(zip(cols, row))


def compute_roi(mint: str) -> float:
    """Current ROI % off the first recorded price for this token."""
    data = get_token_lifecycle(mint)
    return data.get("roi_pct", 0.0)


def mark_token_dead(mint: str):
    conn = _get_conn()
    with _DB_LOCK:
        conn.execute("UPDATE token_lifecycle SET status='dead' WHERE mint=?", (mint,))
        conn.commit()


def get_active_lifecycle_mints(max_age_hours: int = LIFECYCLE_ACTIVE_HOURS) -> list:
    """Mints still within their active tracking window (worth refreshing)."""
    cutoff = int(time.time()) - max_age_hours * 3600
    conn = _get_conn()
    with _DB_LOCK:
        rows = conn.execute(
            "SELECT mint FROM token_lifecycle WHERE first_ts >= ? AND status='active'",
            (cutoff,)).fetchall()
    return [r[0] for r in rows]


def get_open_trade_mints() -> list:
    """Mints that currently have a stop-loss plan set — i.e. open trades."""
    conn = _get_conn()
    with _DB_LOCK:
        rows = conn.execute(
            "SELECT mint FROM token_lifecycle WHERE stop_loss_price IS NOT NULL "
            "AND status='active'").fetchall()
    return [r[0] for r in rows]


def background_lifecycle_updater(price_fetcher, alert_on_milestone=None):
    """
    One pass of the background updater: refresh prices for all tokens still
    inside their active window, update ATH / ROI / multiplier hits, and
    optionally fire a callback when a token crosses 2x/5x/10x.

    `price_fetcher(mint) -> dict` should return the same shape as the main
    bot's `_get_token_price`, i.e. a dict with "price", "market_cap".
    `alert_on_milestone(mint, symbol, crossed_multiples, lifecycle_row)` is
    optional and receives the FULL list of multiples crossed since the last
    check (usually just one, but can be more than one if the price jumped
    past several thresholds between polls) — fire ONE alert per call, not
    one per multiple, so a single big pump doesn't produce several messages
    that all show the same current ROI number.

    Designed to be called on a schedule (e.g. every LIFECYCLE_REFRESH_SECS)
    from APScheduler or a plain sleep-loop thread in the host bot.
    """
    mints = get_active_lifecycle_mints()
    if not mints:
        return
    logger.info("Lifecycle updater: refreshing %d active tokens", len(mints))
    for mint in mints:
        try:
            price_data = price_fetcher(mint, bypass_cache=True)
            price = price_data.get("price") or 0
            mcap  = price_data.get("market_cap") or 0
            if price <= 0:
                continue
            lifecycle = get_token_lifecycle(mint)
            symbol = lifecycle.get("symbol") or ""
            updated = upsert_token_lifecycle(mint, symbol, price, mcap, int(time.time()))
            crossed = updated.get("_newly_crossed") or []
            if crossed and alert_on_milestone:
                try:
                    alert_on_milestone(mint, symbol, crossed, updated)
                except Exception as e:
                    logger.warning("milestone callback failed: %s", e)
        except Exception as e:
            logger.debug("Lifecycle refresh failed for %s: %s", mint, e)

    # Age out tokens past the active window so we stop polling them
    cutoff = int(time.time()) - LIFECYCLE_ACTIVE_HOURS * 3600
    conn = _get_conn()
    with _DB_LOCK:
        conn.execute(
            "UPDATE token_lifecycle SET status='dormant' "
            "WHERE first_ts < ? AND status='active'", (cutoff,))
        conn.commit()


def run_risk_monitor(price_fetcher, on_drawdown=None, on_stop_loss=None, on_rug=None):
    """
    Faster-cadence pass over currently OPEN trades (tokens with a stop-loss
    plan set) checking for drawdown from ATH, stop-loss breach, and rug-pull
    signals (liquidity removal / rapid price crash). Meant to run more often
    than the general lifecycle updater — see RISK_MONITOR_INTERVAL_SECS.

    `price_fetcher(mint, bypass_cache=True) -> dict` — same shape as
    `_get_token_price`, expected to also populate "liquidity_usd" and
    "price_change_5m" when available (both optional; checks that need a
    missing field are simply skipped for that token this pass).

    Callbacks, all optional:
      on_drawdown(mint, symbol, drawdown_pct, thresholds_crossed, lifecycle_row)
      on_stop_loss(mint, symbol, price, stop_loss_price, lifecycle_row)
      on_rug(mint, symbol, signals, lifecycle_row)
    """
    mints = get_open_trade_mints()
    if not mints:
        return
    logger.info("Risk monitor: checking %d open trades", len(mints))
    for mint in mints:
        try:
            price_data = price_fetcher(mint, bypass_cache=True)
            price = price_data.get("price") or 0
            if price <= 0:
                continue
            lifecycle = get_token_lifecycle(mint)
            symbol = lifecycle.get("symbol") or ""
            updated = upsert_token_lifecycle(
                mint, symbol, price, price_data.get("market_cap") or 0, int(time.time()),
                liquidity=price_data.get("liquidity_usd"),
                price_change_5m=price_data.get("price_change_5m"))

            if updated.get("_new_drawdowns") and on_drawdown:
                try:
                    on_drawdown(mint, symbol, updated["_drawdown_pct"],
                               updated["_new_drawdowns"], updated)
                except Exception as e:
                    logger.warning("drawdown callback failed: %s", e)

            if updated.get("_stop_loss_breached") and on_stop_loss:
                try:
                    on_stop_loss(mint, symbol, price, updated.get("stop_loss_price"), updated)
                except Exception as e:
                    logger.warning("stop-loss callback failed: %s", e)

            if updated.get("_rug_signals") and on_rug:
                try:
                    on_rug(mint, symbol, updated["_rug_signals"], updated)
                except Exception as e:
                    logger.warning("rug callback failed: %s", e)
        except Exception as e:
            logger.debug("Risk check failed for %s: %s", mint, e)


def get_lifecycle_summary_stats() -> dict:
    """Rollup stats across all tracked tokens — used for reporting."""
    conn = _get_conn()
    with _DB_LOCK:
        total   = conn.execute("SELECT COUNT(*) FROM token_lifecycle").fetchone()[0]
        hit2    = conn.execute("SELECT COUNT(*) FROM token_lifecycle WHERE hit_2x=1").fetchone()[0]
        hit5    = conn.execute("SELECT COUNT(*) FROM token_lifecycle WHERE hit_5x=1").fetchone()[0]
        hit10   = conn.execute("SELECT COUNT(*) FROM token_lifecycle WHERE hit_10x=1").fetchone()[0]
        avg_roi = conn.execute("SELECT AVG(roi_pct) FROM token_lifecycle").fetchone()[0] or 0
    return {
        "tokens_tracked": total, "hit_2x": hit2, "hit_5x": hit5,
        "hit_10x": hit10, "avg_roi_pct": round(avg_roi, 1),
    }


# ═══════════════════════════════════════════════════════════════════════════════
#  3. LEADER WALLET DETECTION
# ═══════════════════════════════════════════════════════════════════════════════

def record_buy_sequence(mint: str, wallet: str, buy_ts: int) -> int:
    """
    Records this wallet's buy-order position for this mint. Returns the
    sequence position (1 = first buyer we ever saw for this token).
    Idempotent per (mint, wallet).
    """
    conn = _get_conn()
    with _DB_LOCK:
        existing = conn.execute(
            "SELECT seq_position FROM buy_sequences WHERE mint=? AND wallet=?",
            (mint, wallet)).fetchone()
        if existing:
            return existing[0]
        pos = conn.execute(
            "SELECT COUNT(*) FROM buy_sequences WHERE mint=?", (mint,)).fetchone()[0] + 1
        conn.execute(
            "INSERT INTO buy_sequences (mint, wallet, buy_ts, seq_position) VALUES (?,?,?,?)",
            (mint, wallet, buy_ts, pos))
        conn.commit()
    return pos


def compute_leader_follower_edges(mint: str):
    """
    Once a token's buy activity window has enough participants, derive
    leader→follower edges: for every pair (A, B) where A bought before B
    within LEADER_LAG_WINDOW seconds, credit A as having "led" B for this
    token. Call this after a buy-alert fires for a mint (we already have
    the full wallet set + timing at that point).
    """
    conn = _get_conn()
    with _DB_LOCK:
        rows = conn.execute(
            "SELECT wallet, buy_ts FROM buy_sequences WHERE mint=? ORDER BY buy_ts ASC",
            (mint,)).fetchall()

    if len(rows) < 2:
        return

    edges_seen_this_token = set()
    conn = _get_conn()
    with _DB_LOCK:
        for i, (leader, leader_ts) in enumerate(rows):
            for follower, follower_ts in rows[i + 1:]:
                lag = follower_ts - leader_ts
                if lag < 0:
                    continue
                if lag > LEADER_LAG_WINDOW:
                    break  # rows sorted by ts — no later follower will be closer
                if leader == follower:
                    continue
                key = (leader, follower)
                if key in edges_seen_this_token:
                    continue
                edges_seen_this_token.add(key)
                conn.execute(
                    "INSERT INTO leader_edges (leader, follower, cooccurrences, led_count) "
                    "VALUES (?,?,1,1) "
                    "ON CONFLICT(leader, follower) DO UPDATE SET "
                    "cooccurrences=cooccurrences+1, led_count=led_count+1",
                    (leader, follower))
        conn.commit()

    # Refresh influence scores for every wallet touched in this token's sequence
    touched = {w for (w, _) in rows}
    for wallet in touched:
        _recompute_leader_score(wallet)


def _recompute_leader_score(wallet: str):
    """
    Influence score (0-100): rewards wallets that reliably lead OTHER wallets
    into tokens, weighted by how many distinct followers they have (a wallet
    that always leads the same one follower is less valuable than one that
    leads many different wallets).
    """
    conn = _get_conn()
    with _DB_LOCK:
        rows = conn.execute(
            "SELECT follower, cooccurrences, led_count FROM leader_edges WHERE leader=?",
            (wallet,)).fetchall()

    if not rows:
        return

    qualifying = [r for r in rows if r[1] >= MIN_COOCCURRENCES]
    distinct_followers = len(qualifying)
    if not qualifying:
        times_led = sum(r[2] for r in rows)
        influence = 0.0
    else:
        lead_ratios = [r[2] / r[1] for r in qualifying]  # led_count / cooccurrences per follower
        avg_ratio   = sum(lead_ratios) / len(lead_ratios)
        breadth_fac = min(distinct_followers / 5.0, 1.0)   # saturate at 5 reliable followers
        influence   = round(avg_ratio * 70 + breadth_fac * 30, 1)
        times_led   = sum(r[2] for r in rows)

    conn = _get_conn()
    with _DB_LOCK:
        conn.execute(
            "INSERT INTO leader_scores (wallet, influence_score, distinct_followers, times_led, last_updated) "
            "VALUES (?,?,?,?,?) "
            "ON CONFLICT(wallet) DO UPDATE SET influence_score=excluded.influence_score, "
            "distinct_followers=excluded.distinct_followers, times_led=excluded.times_led, "
            "last_updated=excluded.last_updated",
            (wallet, influence, distinct_followers, times_led, int(time.time())))
        conn.commit()


def get_wallet_leader_score(wallet: str) -> float:
    """0-100. Returns 0 (no evidence of leadership) if unknown."""
    conn = _get_conn()
    with _DB_LOCK:
        row = conn.execute(
            "SELECT influence_score FROM leader_scores WHERE wallet=?", (wallet,)).fetchone()
    return row[0] if row else 0.0


def get_top_leaders(limit: int = 10) -> list:
    conn = _get_conn()
    with _DB_LOCK:
        rows = conn.execute(
            "SELECT wallet, influence_score, distinct_followers, times_led "
            "FROM leader_scores ORDER BY influence_score DESC LIMIT ?",
            (limit,)).fetchall()
    return [{"wallet": w, "influence_score": s, "followers": f, "times_led": t}
            for (w, s, f, t) in rows]


# ═══════════════════════════════════════════════════════════════════════════════
#  4. WALLET CONFIDENCE SCORE
# ═══════════════════════════════════════════════════════════════════════════════

def record_trade_outcome_extended(wallet: str, win: bool, roi_pct: float):
    """
    Feeds the confidence-score inputs. This runs ALONGSIDE the existing
    `_record_trade_outcome` in the main bot (which still powers the legacy
    win/loss counters used in the daily report) — this one keeps a bounded
    ROI sample list for consistency scoring.
    """
    conn = _get_conn()
    with _DB_LOCK:
        row = conn.execute(
            "SELECT trades, wins, losses, total_roi, roi_samples FROM wallet_metrics WHERE wallet=?",
            (wallet,)).fetchone()
        if row:
            trades, wins, losses, total_roi, samples_str = row
        else:
            trades, wins, losses, total_roi, samples_str = 0, 0, 0, 0.0, ""

        trades += 1
        total_roi += roi_pct
        if win:
            wins += 1
        else:
            losses += 1

        samples = [float(x) for x in samples_str.split(",") if x]
        samples.append(roi_pct)
        samples = samples[-100:]   # bounded — last 100 trades
        samples_str = ",".join(f"{x:.2f}" for x in samples)

        conn.execute(
            "INSERT INTO wallet_metrics (wallet, trades, wins, losses, total_roi, roi_samples) "
            "VALUES (?,?,?,?,?,?) "
            "ON CONFLICT(wallet) DO UPDATE SET trades=excluded.trades, wins=excluded.wins, "
            "losses=excluded.losses, total_roi=excluded.total_roi, roi_samples=excluded.roi_samples",
            (wallet, trades, wins, losses, total_roi, samples_str))
        conn.commit()

    recompute_wallet_confidence(wallet)


def _consistency_score(samples: list) -> float:
    """
    0-100. High when ROI outcomes are stable and positive, low when they're
    wildly erratic (huge wins mixed with huge losses = unreliable signal
    even if the average looks good).
    """
    if len(samples) < 3:
        return 50.0
    mean = sum(samples) / len(samples)
    stdev = statistics.pstdev(samples)
    if mean <= 0:
        return max(30 - stdev / 10, 0)
    # Coefficient of variation — lower is more consistent
    cv = stdev / (abs(mean) + 1e-9)
    score = max(100 - cv * 25, 0)
    return round(min(score, 100), 1)


def recompute_wallet_confidence(wallet: str) -> dict:
    """
    Combines ROI, hit rate, early entry, leader score, and consistency into
    a single 0-100 confidence score plus a discrete tier. This is the
    replacement for the old win_rate*log(trades)*roi_factor formula.
    """
    conn = _get_conn()
    with _DB_LOCK:
        row = conn.execute(
            "SELECT trades, wins, losses, total_roi, roi_samples FROM wallet_metrics WHERE wallet=?",
            (wallet,)).fetchone()

    if not row or row[0] < MIN_TRADES_FOR_CONFIDENCE:
        trades = row[0] if row else 0
        # Not enough data yet — neutral confidence, but still folds in
        # early-entry/leader signal since those don't require trade outcomes.
        early_score  = get_wallet_early_entry_score(wallet)
        leader_score = get_wallet_leader_score(wallet)
        confidence = round(50 * 0.65 + early_score * 0.20 + leader_score * 0.15, 1)
        _store_confidence(wallet, confidence, 0, 0, early_score, leader_score, 50,
                          trades_known=trades)
        return {"confidence": confidence, "trades": trades, "tier": _tier_for_score(confidence, trades)}

    trades, wins, losses, total_roi, samples_str = row
    win_rate  = wins / trades
    avg_roi   = total_roi / trades
    samples   = [float(x) for x in samples_str.split(",") if x]

    # ROI score: map avg ROI% to 0-100 (0% ROI -> 50, +100% ROI -> ~90, -50% -> ~15)
    roi_score = max(0, min(50 + avg_roi * 0.4, 100))

    # Hit rate score: win_rate directly scaled, with a volume dampener so a
    # 100% win rate on 1 trade doesn't outrank a 70% win rate on 40 trades.
    vol_fac = min(math.log1p(trades) / math.log1p(30), 1.0)
    hit_rate_score = round(win_rate * 100 * (0.5 + 0.5 * vol_fac), 1)

    early_score  = get_wallet_early_entry_score(wallet)
    leader_score = get_wallet_leader_score(wallet)
    consistency  = _consistency_score(samples)

    confidence = (
        roi_score      * CONF_WEIGHTS["roi"] +
        hit_rate_score * CONF_WEIGHTS["hit_rate"] +
        early_score    * CONF_WEIGHTS["early_entry"] +
        leader_score   * CONF_WEIGHTS["leader"] +
        consistency    * CONF_WEIGHTS["consistency"]
    )
    confidence = round(confidence, 1)

    _store_confidence(wallet, confidence, roi_score, hit_rate_score, early_score,
                      leader_score, consistency, trades_known=trades)

    return {
        "confidence": confidence, "trades": trades, "roi_score": roi_score,
        "hit_rate_score": hit_rate_score, "early_entry_score": early_score,
        "leader_score": leader_score, "consistency_score": consistency,
        "tier": _tier_for_score(confidence, trades),
    }


def _store_confidence(wallet, confidence, roi_score, hit_rate_score, early_score,
                      leader_score, consistency, trades_known):
    tier = _tier_for_score(confidence, trades_known)
    conn = _get_conn()
    with _DB_LOCK:
        prev = conn.execute(
            "SELECT tier FROM wallet_confidence WHERE wallet=?", (wallet,)).fetchone()
        conn.execute(
            "INSERT INTO wallet_confidence (wallet, confidence, roi_score, hit_rate_score, "
            "early_entry_score, leader_score, consistency_score, tier, last_updated) "
            "VALUES (?,?,?,?,?,?,?,?,?) "
            "ON CONFLICT(wallet) DO UPDATE SET confidence=excluded.confidence, "
            "roi_score=excluded.roi_score, hit_rate_score=excluded.hit_rate_score, "
            "early_entry_score=excluded.early_entry_score, leader_score=excluded.leader_score, "
            "consistency_score=excluded.consistency_score, tier=excluded.tier, "
            "last_updated=excluded.last_updated",
            (wallet, confidence, roi_score, hit_rate_score, early_score, leader_score,
             consistency, tier, int(time.time())))
        conn.commit()
    if prev and prev[0] != tier:
        _log_tier_change(wallet, prev[0], tier, "confidence recompute")


def _tier_for_score(score: float, trades: int) -> str:
    if trades >= RETIREMENT_MIN_TRADES and score <= RETIREMENT_MAX_SCORE:
        return "RETIRED"
    if trades < MIN_TRADES_FOR_TIER_CHANGE:
        return "STANDARD"   # not enough history to move out of the default tier
    if score >= TIER_THRESHOLDS["ELITE"]:
        return "ELITE"
    if score >= TIER_THRESHOLDS["STRONG"]:
        return "STRONG"
    if score >= TIER_THRESHOLDS["STANDARD"]:
        return "STANDARD"
    if score >= TIER_THRESHOLDS["PROBATION"]:
        return "PROBATION"
    return "RETIRED"


def get_wallet_confidence_score(wallet: str) -> float:
    """
    Returns the confidence score rescaled to roughly the same 0-2.5 range
    the legacy `_get_wallet_score` used, so it's a drop-in replacement
    anywhere the old score fed into weighted-vote thresholds. (0-100 raw
    confidence is available via get_wallet_confidence_raw for display.)
    """
    raw = get_wallet_confidence_raw(wallet)
    return round((raw / 100) * 2.5, 3)


def get_wallet_confidence_raw(wallet: str) -> float:
    """0-100 confidence score. Computes on the fly if not yet cached."""
    conn = _get_conn()
    with _DB_LOCK:
        row = conn.execute(
            "SELECT confidence FROM wallet_confidence WHERE wallet=?", (wallet,)).fetchone()
    if row:
        return row[0]
    return recompute_wallet_confidence(wallet).get("confidence", 50.0)


def get_wallet_tier(wallet: str) -> str:
    conn = _get_conn()
    with _DB_LOCK:
        row = conn.execute(
            "SELECT tier FROM wallet_confidence WHERE wallet=?", (wallet,)).fetchone()
    return row[0] if row else "STANDARD"


# ═══════════════════════════════════════════════════════════════════════════════
#  5. AUTOMATIC WALLET PROMOTION / DEMOTION
# ═══════════════════════════════════════════════════════════════════════════════

def _log_tier_change(wallet: str, old_tier: str, new_tier: str, reason: str):
    conn = _get_conn()
    with _DB_LOCK:
        conn.execute(
            "INSERT INTO tier_history (wallet, old_tier, new_tier, reason, ts) VALUES (?,?,?,?,?)",
            (wallet, old_tier, new_tier, reason, int(time.time())))
        conn.commit()
    logger.info("Tier change: %s… %s -> %s (%s)", wallet[:6], old_tier, new_tier, reason)


def get_tier_history(wallet: str, limit: int = 20) -> list:
    conn = _get_conn()
    with _DB_LOCK:
        rows = conn.execute(
            "SELECT old_tier, new_tier, reason, ts FROM tier_history "
            "WHERE wallet=? ORDER BY ts DESC LIMIT ?", (wallet, limit)).fetchall()
    return [{"old_tier": o, "new_tier": n, "reason": r, "ts": t} for (o, n, r, t) in rows]


def run_promotion_demotion_cycle(active_wallets: list) -> dict:
    """
    Recomputes confidence + tier for every currently-watched wallet, and
    returns a summary of what changed. Wallets that land on RETIRED are
    flagged for removal by the discovery engine (see retire_and_replace).

    `active_wallets` should be the bot's current watched-wallet list.
    """
    promoted, demoted, retired = [], [], []

    for wallet in active_wallets:
        before_tier = get_wallet_tier(wallet)
        result = recompute_wallet_confidence(wallet)
        after_tier = result["tier"]

        if after_tier == before_tier:
            continue

        before_rank = TIERS.index(before_tier) if before_tier in TIERS else 2
        after_rank  = TIERS.index(after_tier) if after_tier in TIERS else 2

        if after_tier == "RETIRED":
            retired.append(wallet)
            _log_tier_change(wallet, before_tier, after_tier, "confidence below retirement floor")
        elif after_rank > before_rank:
            promoted.append(wallet)
            _log_tier_change(wallet, before_tier, after_tier, "confidence improved")
        elif after_rank < before_rank:
            demoted.append(wallet)
            _log_tier_change(wallet, before_tier, after_tier, "confidence declined")

    logger.info("Promotion/demotion cycle: %d promoted, %d demoted, %d retired",
               len(promoted), len(demoted), len(retired))
    return {"promoted": promoted, "demoted": demoted, "retired": retired}


def get_wallets_by_tier(active_wallets: list) -> dict:
    grouped = defaultdict(list)
    for wallet in active_wallets:
        grouped[get_wallet_tier(wallet)].append(wallet)
    return dict(grouped)


def get_retired_wallets(active_wallets: list) -> list:
    return [w for w in active_wallets if get_wallet_tier(w) == "RETIRED"]


# ═══════════════════════════════════════════════════════════════════════════════
#  6. WALLET DISCOVERY ENGINE
# ═══════════════════════════════════════════════════════════════════════════════

def seed_active_roster(wallets: list):
    """
    Ensures every wallet in the given static list has a row in the roster.
    Safe to call on every refresh_wallets() cycle, not just at first startup:
    `INSERT OR IGNORE` only adds wallets that have NEVER been seen before —
    a wallet that was previously retired (status='retired', row still
    present) is deliberately left untouched here, so re-running this doesn't
    undo retirement decisions made by the promotion/demotion or discovery
    systems. Only genuinely new entries in `wallets` get added.
    """
    conn = _get_conn()
    now = int(time.time())
    with _DB_LOCK:
        for w in wallets:
            conn.execute(
                "INSERT OR IGNORE INTO active_roster (wallet, added_ts, source, status) "
                "VALUES (?,?,'seed','active')",
                (w, now))
        conn.commit()


def get_active_roster() -> list:
    conn = _get_conn()
    with _DB_LOCK:
        rows = conn.execute(
            "SELECT wallet FROM active_roster WHERE status='active'").fetchall()
    return [r[0] for r in rows]


def remove_from_roster(wallet: str):
    """
    Soft-retires a wallet — marks it 'retired' rather than deleting the row.
    This is what makes seed_active_roster() safe to re-run: if we hard-deleted
    here, the next refresh_wallets() -> seed_active_roster() call would just
    re-insert the wallet from the static FALLBACK_WALLETS list and silently
    undo the retirement.
    """
    conn = _get_conn()
    with _DB_LOCK:
        conn.execute(
            "UPDATE active_roster SET status='retired' WHERE wallet=?", (wallet,))
        conn.commit()


def add_to_roster(wallet: str, source: str = "discovery"):
    """Adds a wallet, or re-activates it if it was previously retired."""
    conn = _get_conn()
    with _DB_LOCK:
        conn.execute(
            "INSERT INTO active_roster (wallet, added_ts, source, status) "
            "VALUES (?,?,?,'active') "
            "ON CONFLICT(wallet) DO UPDATE SET status='active', source=excluded.source, "
            "added_ts=excluded.added_ts",
            (wallet, int(time.time()), source))
        conn.commit()


def observe_candidate_wallet(wallet: str, mint: str, win: bool = None, roi_pct: float = 0,
                             was_early: bool = False):
    """
    Records an observation of a NON-watched wallet's activity, so it can be
    evaluated as a future roster candidate. This should be fed from any
    wallet activity the bot happens to see that ISN'T already in the active
    roster — e.g. wallets seen buying tokens that later go on to hit a big
    multiplier in the token lifecycle DB (retroactive discovery), or wallets
    surfaced from a "top holders of a winning token" lookup.
    """
    conn = _get_conn()
    with _DB_LOCK:
        row = conn.execute(
            "SELECT observations, wins, total_roi, early_entry_hits, status "
            "FROM discovery_candidates WHERE wallet=?", (wallet,)).fetchone()

        if row is None:
            count = conn.execute("SELECT COUNT(*) FROM discovery_candidates").fetchone()[0]
            if count >= DISCOVERY_MAX_ACTIVE_WATCH:
                return  # candidate pool full — skip until some graduate/get rejected
            observations, wins, total_roi, early_hits, status = 0, 0, 0.0, 0, "watching"
        else:
            observations, wins, total_roi, early_hits, status = row
            if status != "watching":
                return

        observations += 1
        if win:
            wins += 1
        total_roi += roi_pct
        if was_early:
            early_hits += 1

        conn.execute(
            "INSERT INTO discovery_candidates "
            "(wallet, first_seen_ts, observations, wins, total_roi, early_entry_hits, status) "
            "VALUES (?,?,?,?,?,?,'watching') "
            "ON CONFLICT(wallet) DO UPDATE SET observations=excluded.observations, "
            "wins=excluded.wins, total_roi=excluded.total_roi, "
            "early_entry_hits=excluded.early_entry_hits",
            (wallet, int(time.time()), observations, wins, total_roi, early_hits))
        conn.commit()


def find_early_buyers_of_winner(mint: str, get_top_holders_fn=None) -> list:
    """
    Given a token that hit a big multiplier (per the lifecycle DB), returns
    wallets worth evaluating as discovery candidates: our own recorded
    early buyers of that token (from buy_sequences) plus, optionally,
    externally-supplied top holders (e.g. from a Helius/Solscan lookup
    passed in as `get_top_holders_fn(mint) -> list[str]`, since that API
    call is network/provider-specific and lives in the host bot).
    """
    conn = _get_conn()
    with _DB_LOCK:
        rows = conn.execute(
            "SELECT wallet FROM buy_sequences WHERE mint=? ORDER BY seq_position ASC LIMIT 15",
            (mint,)).fetchall()
    candidates = [r[0] for r in rows]

    if get_top_holders_fn:
        try:
            extra = get_top_holders_fn(mint) or []
            for w in extra:
                if w not in candidates:
                    candidates.append(w)
        except Exception as e:
            logger.debug("get_top_holders_fn failed for %s: %s", mint, e)

    return candidates


def evaluate_candidates_for_graduation() -> list:
    """
    Checks every 'watching' candidate against graduation criteria. Returns
    the list of wallets that just graduated (should be added to the live
    roster). Candidates that have accumulated enough observations but
    perform poorly are marked 'rejected' so they stop consuming pool slots.
    """
    conn = _get_conn()
    with _DB_LOCK:
        rows = conn.execute(
            "SELECT wallet, observations, wins, total_roi, early_entry_hits "
            "FROM discovery_candidates WHERE status='watching'").fetchall()

    graduated = []
    for wallet, obs, wins, total_roi, early_hits in rows:
        if obs < DISCOVERY_MIN_OBSERVATIONS:
            continue

        avg_roi   = total_roi / obs if obs else 0
        win_rate  = wins / obs if obs else 0
        early_fac = early_hits / obs if obs else 0

        # Lightweight scoring mirroring confidence-score inputs, since these
        # wallets don't have full trade histories yet.
        pseudo_score = (
            max(0, min(50 + avg_roi * 0.4, 100)) * 0.35 +
            (win_rate * 100) * 0.30 +
            (early_fac * 100) * 0.35
        )

        conn = _get_conn()
        if wins >= DISCOVERY_MIN_WINS and pseudo_score >= DISCOVERY_MIN_SCORE:
            with _DB_LOCK:
                conn.execute(
                    "UPDATE discovery_candidates SET status='graduated' WHERE wallet=?", (wallet,))
                conn.commit()
            graduated.append(wallet)
            logger.info("Candidate graduated: %s… (score %.1f, %d obs)",
                       wallet[:6], pseudo_score, obs)
        elif obs >= DISCOVERY_MIN_OBSERVATIONS * 3 and pseudo_score < DISCOVERY_MIN_SCORE * 0.5:
            with _DB_LOCK:
                conn.execute(
                    "UPDATE discovery_candidates SET status='rejected' WHERE wallet=?", (wallet,))
                conn.commit()

    return graduated


def run_discovery_cycle(active_wallets: list, get_top_holders_fn=None) -> dict:
    """
    Full discovery pass, meant to run on a schedule (e.g. every few hours):

      1. Pull tokens that hit 5x+ from the lifecycle DB.
      2. Surface their early buyers as discovery candidates (if not already
         on the active roster).
      3. Evaluate all watching candidates for graduation.
      4. Retire underperforming roster wallets and backfill with graduates,
         keeping the roster near TARGET_ACTIVE_WALLET_COUNT.

    Returns a summary dict the host bot can use to decide whether to call
    refresh_wallets() (i.e. push a new webhook registration).
    """
    conn = _get_conn()
    with _DB_LOCK:
        winners = conn.execute(
            "SELECT mint FROM token_lifecycle WHERE hit_5x=1 "
            "AND last_updated > ?", (int(time.time()) - 30 * 86400,)).fetchall()

    roster_set = set(active_wallets)
    new_candidates_seen = 0
    for (mint,) in winners:
        for wallet in find_early_buyers_of_winner(mint, get_top_holders_fn):
            if wallet in roster_set:
                continue
            was_early = (get_token_entry_rank(mint, wallet) or 99) <= 5
            observe_candidate_wallet(wallet, mint, win=True,
                                     roi_pct=compute_roi(mint), was_early=was_early)
            new_candidates_seen += 1

    graduated = evaluate_candidates_for_graduation()

    retired = get_retired_wallets(active_wallets)

    # Backfill: promote graduates into empty retired slots, up to the target size
    added, removed = [], []
    slots_open = max(TARGET_ACTIVE_WALLET_COUNT - (len(active_wallets) - len(retired)), 0)
    intake = graduated[:max(slots_open, len(retired))]

    for wallet in retired:
        remove_from_roster(wallet)
        removed.append(wallet)

    for wallet in intake:
        add_to_roster(wallet, source="discovery")
        added.append(wallet)

    logger.info(
        "Discovery cycle: %d winners scanned, %d new candidate obs, %d graduated, "
        "%d retired, %d added",
        len(winners), new_candidates_seen, len(graduated), len(removed), len(added))

    return {
        "winners_scanned": len(winners),
        "candidates_observed": new_candidates_seen,
        "graduated": graduated,
        "retired": removed,
        "added": added,
        "roster_changed": bool(added or removed),
    }


def get_discovery_stats() -> dict:
    conn = _get_conn()
    with _DB_LOCK:
        watching   = conn.execute(
            "SELECT COUNT(*) FROM discovery_candidates WHERE status='watching'").fetchone()[0]
        graduated  = conn.execute(
            "SELECT COUNT(*) FROM discovery_candidates WHERE status='graduated'").fetchone()[0]
        rejected   = conn.execute(
            "SELECT COUNT(*) FROM discovery_candidates WHERE status='rejected'").fetchone()[0]
    return {"watching": watching, "graduated": graduated, "rejected": rejected}


# ═══════════════════════════════════════════════════════════════════════════════
#  LEGACY STATE PERSISTENCE (wallet_stats / wallet_wins)
# ═══════════════════════════════════════════════════════════════════════════════
# The bot keeps fast in-RAM dicts (wallet_stats, wallet_wins) for its hot read
# paths. These functions are the persisted mirror: called on every write so
# nothing is lost on restart, and called once at startup to repopulate the
# RAM dicts from disk. The bot owns the dict shape; these just round-trip it.

def save_wallet_stat(wallet: str, stats: dict):
    """
    Persists one wallet's legacy stats dict, shape:
    {"wins": int, "losses": int, "total_roi": float, "trades": int,
     "hold_times": list[int]}
    """
    hold_times_str = ",".join(str(int(x)) for x in (stats.get("hold_times") or [])[-200:])
    conn = _get_conn()
    with _DB_LOCK:
        conn.execute(
            "INSERT INTO legacy_wallet_stats (wallet, wins, losses, total_roi, trades, hold_times) "
            "VALUES (?,?,?,?,?,?) "
            "ON CONFLICT(wallet) DO UPDATE SET wins=excluded.wins, losses=excluded.losses, "
            "total_roi=excluded.total_roi, trades=excluded.trades, hold_times=excluded.hold_times",
            (wallet, stats.get("wins", 0), stats.get("losses", 0),
             stats.get("total_roi", 0.0), stats.get("trades", 0), hold_times_str))
        conn.commit()


def load_all_wallet_stats() -> dict:
    """Returns {wallet: stats_dict} for every wallet with saved legacy stats — call once at startup."""
    conn = _get_conn()
    with _DB_LOCK:
        rows = conn.execute(
            "SELECT wallet, wins, losses, total_roi, trades, hold_times FROM legacy_wallet_stats"
        ).fetchall()
    out = {}
    for wallet, wins, losses, total_roi, trades, hold_times_str in rows:
        hold_times = [int(x) for x in hold_times_str.split(",") if x] if hold_times_str else []
        out[wallet] = {
            "wins": wins, "losses": losses, "total_roi": total_roi,
            "trades": trades, "hold_times": hold_times,
        }
    return out


def save_daily_win(wallet: str, wins: int, last_reset: str):
    """Persists one wallet's today's-win-count record, shape: {"wins": int, "last_reset": "YYYY-MM-DD"}."""
    conn = _get_conn()
    with _DB_LOCK:
        conn.execute(
            "INSERT INTO daily_wallet_wins (wallet, wins, last_reset) VALUES (?,?,?) "
            "ON CONFLICT(wallet) DO UPDATE SET wins=excluded.wins, last_reset=excluded.last_reset",
            (wallet, wins, last_reset))
        conn.commit()


def load_all_daily_wins() -> dict:
    """Returns {wallet: {"wins": int, "last_reset": str}} for every wallet — call once at startup."""
    conn = _get_conn()
    with _DB_LOCK:
        rows = conn.execute("SELECT wallet, wins, last_reset FROM daily_wallet_wins").fetchall()
    return {wallet: {"wins": wins, "last_reset": last_reset} for (wallet, wins, last_reset) in rows}


def get_pending_promotions_count() -> int:
    """
    Candidates that already cleared graduation criteria but haven't been
    rotated onto the live roster yet (discovery only backfills as many
    slots as there are retirees/target headroom per cycle, so a graduate
    can sit queued for a cycle or two).
    """
    conn = _get_conn()
    with _DB_LOCK:
        graduated_wallets = {r[0] for r in conn.execute(
            "SELECT wallet FROM discovery_candidates WHERE status='graduated'").fetchall()}
        roster = {r[0] for r in conn.execute("SELECT wallet FROM active_roster").fetchall()}
    return len(graduated_wallets - roster)


def get_dashboard_stats(active_wallets: list) -> dict:
    """
    One-stop rollup across all six subsystems, meant for the startup
    message / a status dashboard. `active_wallets` should be the bot's
    current watched-wallet list (drives the tier breakdown and the
    'wallets scored' count, since those are scoped to the live roster).
    """
    conn = _get_conn()

    tiers = get_wallets_by_tier(active_wallets)
    elite     = len(tiers.get("ELITE", []))
    strong    = len(tiers.get("STRONG", []))
    standard  = len(tiers.get("STANDARD", []))
    probation = len(tiers.get("PROBATION", []))
    retired   = len(tiers.get("RETIRED", []))

    with _DB_LOCK:
        placeholders = ",".join("?" * len(active_wallets)) if active_wallets else ""
        if active_wallets:
            wallets_scored = conn.execute(
                f"SELECT COUNT(*) FROM wallet_confidence WHERE wallet IN ({placeholders})",
                active_wallets).fetchone()[0]
        else:
            wallets_scored = 0

        tokens_tracked_early = conn.execute(
            "SELECT COUNT(*) FROM token_launches").fetchone()[0]
        leader_edges = conn.execute(
            "SELECT COUNT(*) FROM leader_edges").fetchone()[0]

    lifecycle  = get_lifecycle_summary_stats()
    discovery  = get_discovery_stats()
    pending    = get_pending_promotions_count()

    return {
        "watching_total":   len(active_wallets),
        "tier_elite":       elite,
        "tier_strong":      strong,
        "tier_standard":    standard,
        "tier_probation":   probation,
        "tier_retired":     retired,
        "wallets_scored":   wallets_scored,
        "tokens_tracked_early": tokens_tracked_early,
        "leader_edges":     leader_edges,
        "tokens_monitored": lifecycle["tokens_tracked"],
        "hit_5x":           lifecycle["hit_5x"],
        "hit_10x":          lifecycle["hit_10x"],
        "discovery_candidates": discovery["watching"],
        "pending_promotions":   pending,
    }
