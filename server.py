"""
server.py — Spins up NUM_SERVERS real FastAPI/uvicorn servers on consecutive ports.

Run:  python server.py

Each server responds to:
  GET /work    — simulated work with random delay; 5 % chance of HTTP 500
  GET /health  — always 200 (used by balancer health checker)

srv-8001 is deliberately slow to trigger load imbalance and algorithm switching.
"""

import asyncio
import os
import random
import sys

sys.path.insert(0, os.path.dirname(__file__))

import uvicorn
from fastapi import FastAPI
from fastapi.responses import JSONResponse

from config import (
    FAILURE_RATE,
    FAST_MAX_DELAY, FAST_MIN_DELAY,
    SERVER_PORTS,
    SLOW_MAX_DELAY, SLOW_MIN_DELAY, SLOW_SERVER_PORT,
)

# ── Per-port counters (live inside each process / coroutine space) ─────────

_counters: dict[int, dict] = {}


def _get_counter(port: int) -> dict:
    if port not in _counters:
        _counters[port] = {"requests": 0, "errors": 0}
    return _counters[port]


# ── App factory ────────────────────────────────────────────────────────────

def make_app(port: int) -> FastAPI:
    app = FastAPI(title=f"backend-{port}")

    @app.get("/work")
    async def work():
        ctr = _get_counter(port)
        ctr["requests"] += 1

        # Simulate real-world flakiness: random 5xx
        if random.random() < FAILURE_RATE:
            ctr["errors"] += 1
            return JSONResponse(
                status_code=500,
                content={
                    "server": port,
                    "error":  "internal server error (simulated)",
                    "status": "error",
                },
            )

        # Normal path — variable latency
        if port == SLOW_SERVER_PORT:
            delay = random.uniform(SLOW_MIN_DELAY, SLOW_MAX_DELAY)
        else:
            delay = random.uniform(FAST_MIN_DELAY, FAST_MAX_DELAY)

        await asyncio.sleep(delay)
        return {
            "server":   port,
            "delay":    round(delay, 3),
            "status":   "ok",
            "req_count": ctr["requests"],
        }

    @app.get("/health")
    async def health():
        """Used by the load balancer's health checker — always fast."""
        ctr = _get_counter(port)
        return {
            "server":   port,
            "status":   "healthy",
            "requests": ctr["requests"],
            "errors":   ctr["errors"],
        }

    return app


# ── Main: run all servers concurrently ────────────────────────────────────

async def main() -> None:
    configs = [
        uvicorn.Config(
            make_app(port),
            host="127.0.0.1",
            port=port,
            log_level="warning",
            # Real-world: limit worker concurrency via loop settings
            loop="asyncio",
        )
        for port in SERVER_PORTS
    ]
    servers = [uvicorn.Server(cfg) for cfg in configs]

    slow_tag = f"  ← SLOW ({SLOW_MIN_DELAY}–{SLOW_MAX_DELAY}s, {FAILURE_RATE*100:.0f}% fail)"
    fast_tag = f"  ({FAST_MIN_DELAY}–{FAST_MAX_DELAY}s, {FAILURE_RATE*100:.0f}% fail)"

    print("Starting backend servers:")
    for port in SERVER_PORTS:
        tag = slow_tag if port == SLOW_SERVER_PORT else fast_tag
        print(f"  srv-{port}{tag}")
    print()

    await asyncio.gather(*[srv.serve() for srv in servers])


if __name__ == "__main__":
    asyncio.run(main())