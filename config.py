"""
config.py — Shared constants for the entire project.
"""

# ── Ports ──────────────────────────────────────────────────────────────────
BALANCER_PORT = 8000
SERVER_PORTS  = [8001, 8002, 8003, 8004]

# ── Algorithm switching thresholds ────────────────────────────────────────
IMBALANCE_THRESH  = 3   # RR → LC  when max_conns - min_conns > this
REBALANCE_THRESH  = 1   # LC → RR  when max_conns - min_conns ≤ this

# ── Slow server (srv-8001 deliberately laggy to trigger imbalance) ─────────
SLOW_SERVER_PORT  = 8001
SLOW_MIN_DELAY    = 1.5   # seconds
SLOW_MAX_DELAY    = 3.0
FAST_MIN_DELAY    = 0.05
FAST_MAX_DELAY    = 0.4

# ── Failure simulation (realistic flakiness) ──────────────────────────────
# Probability [0-1] that any backend request returns HTTP 500
FAILURE_RATE      = 0.05   # 5 % of requests fail

# ── Health checker ────────────────────────────────────────────────────────
HEALTH_INTERVAL   = 3.0    # seconds between health polls
HEALTH_TIMEOUT    = 2.0    # seconds before marking a backend DOWN
HEALTH_RECOVER    = 2      # consecutive successes needed to mark UP again

# ── Balancer retry policy ─────────────────────────────────────────────────
MAX_RETRIES       = 2      # how many *other* backends to try on 5xx / timeout
REQUEST_TIMEOUT   = 10.0   # total timeout per upstream attempt (seconds)

# ── Connection pool ────────────────────────────────────────────────────────
# aiohttp TCPConnector limit per backend host
CONN_POOL_SIZE    = 50

# ── Client traffic shaping ─────────────────────────────────────────────────
# Base inter-request delay (seconds) in steady mode  →  ~20 req/s
STEADY_RATE_DELAY = 0.05
# Burst: every BURST_EVERY requests, fire BURST_SIZE requests with no delay
BURST_EVERY       = 40     # requests between bursts
BURST_SIZE        = 15     # concurrent requests in one burst