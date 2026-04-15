"""
balancer.py — Adaptive load balancer (aiohttp) on port 8000.

What's real-world here:
  • Active health checks every HEALTH_INTERVAL seconds — unhealthy backends
    are removed from rotation automatically and re-added on recovery.
  • Retry on 5xx or timeout: tries up to MAX_RETRIES other backends before
    returning 502 to the client.
  • Dedicated aiohttp TCPConnector with a per-host connection pool
    (CONN_POOL_SIZE) — mirrors what nginx / envoy do.
  • Request-level timeout (REQUEST_TIMEOUT) separate from the health-check
    timeout (HEALTH_TIMEOUT).

Endpoints:
  GET /           → proxies request to a backend, returns its response
  GET /stats      → JSON snapshot for the dashboard
  GET /health     → balancer's own health (always 200 while running)
  GET /dashboard  → serves the HTML dashboard
"""

import asyncio
import json
import os
import sys
import time
from collections import deque
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path

sys.path.insert(0, os.path.dirname(__file__))

import aiohttp
from aiohttp import web

from config import (
    BALANCER_PORT,
    CONN_POOL_SIZE,
    HEALTH_INTERVAL, HEALTH_RECOVER, HEALTH_TIMEOUT,
    IMBALANCE_THRESH,
    MAX_RETRIES,
    REBALANCE_THRESH,
    REQUEST_TIMEOUT,
    SERVER_PORTS,
    SLOW_SERVER_PORT,
)


# ──────────────────────────────────────────────────────────────────────────────
# Data model
# ──────────────────────────────────────────────────────────────────────────────

class Algorithm(str, Enum):
    ROUND_ROBIN = "Round Robin"
    LEAST_CONN  = "Least Connections"


@dataclass
class Backend:
    port: int
    active_conns:    int  = 0
    total_handled:   int  = 0
    total_errors:    int  = 0
    total_retried:   int  = 0      # times this backend triggered a retry

    # Health state
    healthy:         bool = True
    consec_successes: int = 0      # for recovery hysteresis

    @property
    def url(self) -> str:
        return f"http://127.0.0.1:{self.port}"

    @property
    def name(self) -> str:
        return f"srv-{self.port}"

    def to_dict(self) -> dict:
        return {
            "name":            self.name,
            "port":            self.port,
            "active_conns":    self.active_conns,
            "total_handled":   self.total_handled,
            "total_errors":    self.total_errors,
            "total_retried":   self.total_retried,
            "healthy":         self.healthy,
            "is_slow":         self.port == SLOW_SERVER_PORT,
        }


@dataclass
class LoadBalancer:
    backends: list[Backend]
    threshold:            int = IMBALANCE_THRESH
    rebalance_threshold:  int = REBALANCE_THRESH

    algorithm:       Algorithm = Algorithm.ROUND_ROBIN
    rr_index:        int       = 0
    total_requests:  int       = 0
    total_retries:   int       = 0
    algo_switches:   int       = 0
    start_time:      float     = field(default_factory=time.time)

    events: deque = field(default_factory=lambda: deque(maxlen=60))

    # ── Healthy backend list ──────────────────────────────────────────────

    @property
    def healthy_backends(self) -> list[Backend]:
        h = [b for b in self.backends if b.healthy]
        return h if h else self.backends   # never return empty — fallback to all

    # ── Algorithm switching ───────────────────────────────────────────────

    def _imbalance(self) -> int:
        conns = [b.active_conns for b in self.healthy_backends]
        return max(conns) - min(conns)

    def _maybe_switch(self) -> None:
        imb = self._imbalance()
        if self.algorithm == Algorithm.ROUND_ROBIN and imb > self.threshold:
            self.algorithm = Algorithm.LEAST_CONN
            self.algo_switches += 1
            self._log("SWITCH",
                f"imbalance={imb} > threshold={self.threshold} → switched to Least Connections")
        elif self.algorithm == Algorithm.LEAST_CONN and imb <= self.rebalance_threshold:
            self.algorithm = Algorithm.ROUND_ROBIN
            self.algo_switches += 1
            self._log("SWITCH",
                f"imbalance={imb} ≤ rebalance={self.rebalance_threshold} → switched to Round Robin")

    # ── Server selection ──────────────────────────────────────────────────

    def pick_backend(self, exclude: set[int] | None = None) -> Backend | None:
        """
        Pick the next backend, optionally excluding a set of ports
        (used by the retry loop so we don't retry the same broken server).
        """
        self._maybe_switch()
        pool = [b for b in self.healthy_backends if b.port not in (exclude or set())]
        if not pool:
            # Last resort: any backend not in exclude
            pool = [b for b in self.backends if b.port not in (exclude or set())]
        if not pool:
            return None

        if self.algorithm == Algorithm.ROUND_ROBIN:
            # Maintain global round-robin index across all healthy backends
            b = pool[self.rr_index % len(pool)]
            self.rr_index += 1
        else:
            b = min(pool, key=lambda b: b.active_conns)
        return b

    # ── Request lifecycle ─────────────────────────────────────────────────

    def on_start(self, backend: Backend, req_id: int) -> None:
        backend.active_conns += 1
        self.total_requests  += 1
        conns = [b.active_conns for b in self.backends]
        self._log("REQ",
            f"#{req_id:05d} → {backend.name}  [{self.algorithm.value}]  "
            f"active={conns}  imbalance={self._imbalance()}")

    def on_end(self, backend: Backend, success: bool, retried: bool = False) -> None:
        backend.active_conns = max(0, backend.active_conns - 1)
        if success:
            backend.total_handled += 1
        else:
            backend.total_errors  += 1
        if retried:
            backend.total_retried += 1
            self.total_retries    += 1

    # ── Health state ──────────────────────────────────────────────────────

    def mark_down(self, backend: Backend) -> None:
        if backend.healthy:
            backend.healthy = False
            backend.consec_successes = 0
            self._log("DOWN", f"{backend.name} marked UNHEALTHY — removed from rotation")

    def mark_up(self, backend: Backend) -> None:
        if not backend.healthy:
            backend.healthy = True
            self._log("UP", f"{backend.name} recovered — added back to rotation")

    def record_health_success(self, backend: Backend) -> None:
        backend.consec_successes += 1
        if not backend.healthy and backend.consec_successes >= HEALTH_RECOVER:
            self.mark_up(backend)

    def record_health_failure(self, backend: Backend) -> None:
        backend.consec_successes = 0
        self.mark_down(backend)

    # ── Snapshot for /stats ───────────────────────────────────────────────

    def stats(self) -> dict:
        return {
            "algorithm":           self.algorithm.value,
            "algo_switches":       self.algo_switches,
            "total_requests":      self.total_requests,
            "total_retries":       self.total_retries,
            "imbalance":           self._imbalance(),
            "threshold":           self.threshold,
            "rebalance_threshold": self.rebalance_threshold,
            "elapsed":             round(time.time() - self.start_time, 1),
            "backends":            [b.to_dict() for b in self.backends],
            "events":              list(self.events),
        }

    # ── Internal ──────────────────────────────────────────────────────────

    def _log(self, kind: str, msg: str) -> None:
        self.events.appendleft({
            "ts":   round(time.time(), 3),
            "kind": kind,
            "msg":  msg,
        })


# ──────────────────────────────────────────────────────────────────────────────
# Health checker background task
# ──────────────────────────────────────────────────────────────────────────────

async def health_checker(app: web.Application) -> None:
    """
    Polls every backend's /health every HEALTH_INTERVAL seconds.
    Updates LoadBalancer state so unhealthy backends leave rotation.
    """
    lb:      LoadBalancer      = app["lb"]
    session: aiohttp.ClientSession = app["session"]

    while True:
        await asyncio.sleep(HEALTH_INTERVAL)
        for backend in lb.backends:
            try:
                async with session.get(
                    f"{backend.url}/health",
                    timeout=aiohttp.ClientTimeout(total=HEALTH_TIMEOUT),
                ) as resp:
                    if resp.status == 200:
                        lb.record_health_success(backend)
                    else:
                        lb.record_health_failure(backend)
            except Exception:
                lb.record_health_failure(backend)


# ──────────────────────────────────────────────────────────────────────────────
# aiohttp request handlers
# ──────────────────────────────────────────────────────────────────────────────

async def handle_proxy(request: web.Request) -> web.Response:
    """
    Real-world proxy with retry logic:
      1. Pick a backend.
      2. Forward the request.
      3. On 5xx or connection error → decrement counter, log retry, pick another backend.
      4. After MAX_RETRIES exhausted → return 502.
    """
    lb:      LoadBalancer      = request.app["lb"]
    session: aiohttp.ClientSession = request.app["session"]

    tried:   set[int] = set()
    req_id   = lb.total_requests   # snapshot before increment (first pick increments)
    attempt  = 0
    last_err = "no backends available"

    while attempt <= MAX_RETRIES:
        backend = lb.pick_backend(exclude=tried)
        if backend is None:
            break

        is_retry = attempt > 0
        lb.on_start(backend, req_id)
        tried.add(backend.port)

        try:
            async with session.get(
                f"{backend.url}/work",
                timeout=aiohttp.ClientTimeout(total=REQUEST_TIMEOUT),
            ) as resp:
                data = await resp.json()

                if resp.status >= 500:
                    # Backend returned 5xx — counts as error, maybe retry
                    lb.on_end(backend, success=False, retried=is_retry)
                    last_err = f"{backend.name} returned HTTP {resp.status}"
                    if is_retry:
                        lb._log("RETRY",
                            f"attempt {attempt}: {last_err} → trying another backend")
                    attempt += 1
                    continue

                lb.on_end(backend, success=True, retried=is_retry)
                return web.json_response({
                    "routed_to": backend.name,
                    "algorithm": lb.algorithm.value,
                    "attempt":   attempt + 1,
                    **data,
                })

        except asyncio.TimeoutError:
            lb.on_end(backend, success=False, retried=is_retry)
            last_err = f"{backend.name} timed out after {REQUEST_TIMEOUT}s"
            lb._log("RETRY", f"attempt {attempt}: {last_err}")
            attempt += 1

        except Exception as exc:
            lb.on_end(backend, success=False, retried=is_retry)
            last_err = f"{backend.name} connection error: {exc}"
            lb._log("RETRY", f"attempt {attempt}: {last_err}")
            attempt += 1

    # All retries exhausted
    lb._log("ERR", f"req#{req_id:05d} failed after {attempt} attempt(s): {last_err}")
    return web.json_response(
        {"error": last_err, "attempts": attempt},
        status=502,
    )


async def handle_stats(request: web.Request) -> web.Response:
    lb: LoadBalancer = request.app["lb"]
    resp = web.json_response(lb.stats())
    resp.headers["Access-Control-Allow-Origin"] = "*"
    return resp


async def handle_health(request: web.Request) -> web.Response:
    """Balancer's own health endpoint — useful if this sits behind another proxy."""
    return web.json_response({"status": "ok", "port": BALANCER_PORT})


async def handle_dashboard(request: web.Request) -> web.Response:
    html_path = Path(__file__).parent / "dashboard.html"
    return web.FileResponse(html_path)


# ──────────────────────────────────────────────────────────────────────────────
# App lifecycle
# ──────────────────────────────────────────────────────────────────────────────

async def on_startup(app: web.Application) -> None:
    # Dedicated connection pool — mirrors nginx upstream keepalive
    connector = aiohttp.TCPConnector(
        limit_per_host=CONN_POOL_SIZE,
        limit=CONN_POOL_SIZE * len(SERVER_PORTS),
        keepalive_timeout=30,
        enable_cleanup_closed=True,
    )
    app["session"] = aiohttp.ClientSession(connector=connector)

    # Start health checker as a background task
    app["health_task"] = asyncio.create_task(health_checker(app))

    print(f"Load balancer ready → http://127.0.0.1:{BALANCER_PORT}")
    print(f"Dashboard          → http://127.0.0.1:{BALANCER_PORT}/dashboard")
    print(f"Stats API          → http://127.0.0.1:{BALANCER_PORT}/stats")
    print(f"Health check runs every {HEALTH_INTERVAL}s | retries={MAX_RETRIES} | pool={CONN_POOL_SIZE}/host\n")


async def on_cleanup(app: web.Application) -> None:
    app["health_task"].cancel()
    try:
        await app["health_task"]
    except asyncio.CancelledError:
        pass
    await app["session"].close()


def build_app() -> web.Application:
    backends = [Backend(port=p) for p in SERVER_PORTS]
    lb       = LoadBalancer(backends=backends)

    app = web.Application()
    app["lb"] = lb

    app.router.add_get("/",          handle_proxy)
    app.router.add_get("/stats",     handle_stats)
    app.router.add_get("/health",    handle_health)
    app.router.add_get("/dashboard", handle_dashboard)

    app.on_startup.append(on_startup)
    app.on_cleanup.append(on_cleanup)
    return app


if __name__ == "__main__":
    web.run_app(build_app(), host="127.0.0.1", port=BALANCER_PORT, print=lambda _: None)