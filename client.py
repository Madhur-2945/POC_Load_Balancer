"""
client.py — Fires concurrent HTTP requests at the load balancer on port 8000.

Traffic shaping modes:
  steady   — constant ~20 req/s (original behaviour)
  burst    — steady baseline + periodic spikes (Poisson-like bursts)
  ramp     — linearly increases concurrency over time (stress test)

Usage:
    python client.py                           # burst mode, 200 total, 12 workers
    python client.py --mode steady             # original steady stream
    python client.py --mode ramp --total 0     # ramp forever until Ctrl-C
    python client.py --total 0 --concurrency 20
"""

import argparse
import asyncio
import sys
import os
import time
import random

sys.path.insert(0, os.path.dirname(__file__))

import aiohttp

from config import (
    BALANCER_PORT,
    BURST_EVERY,
    BURST_SIZE,
    STEADY_RATE_DELAY,
)

BALANCER_URL = f"http://127.0.0.1:{BALANCER_PORT}"


# ──────────────────────────────────────────────────────────────────────────────
# Worker
# ──────────────────────────────────────────────────────────────────────────────

async def worker(
    session: aiohttp.ClientSession,
    worker_id: int,
    queue: asyncio.Queue,
    results: list,
) -> None:
    while True:
        item = await queue.get()
        if item is None:          # poison pill
            queue.task_done()
            break

        req_id, is_burst = item
        t0 = time.perf_counter()
        burst_tag = " [BURST]" if is_burst else ""

        try:
            async with session.get(
                BALANCER_URL,
                timeout=aiohttp.ClientTimeout(total=15),
            ) as resp:
                data    = await resp.json()
                elapsed = time.perf_counter() - t0

                if resp.status == 502:
                    results.append({"ok": False, "error": "502 from balancer", "elapsed": elapsed})
                    print(f"  w{worker_id:02d}  req#{req_id:04d}{burst_tag}  "
                          f"✗ 502 (all retries exhausted)  {elapsed:.3f}s")
                else:
                    attempt = data.get("attempt", 1)
                    retry_tag = f"  retry×{attempt-1}" if attempt > 1 else ""
                    results.append({
                        "ok": True,
                        "server": data.get("routed_to"),
                        "elapsed": elapsed,
                        "retried": attempt > 1,
                    })
                    print(f"  w{worker_id:02d}  req#{req_id:04d}{burst_tag}  "
                          f"✓ [{data.get('routed_to')}]  {elapsed:.3f}s"
                          f"  algo={data.get('algorithm')}{retry_tag}")

        except Exception as exc:
            elapsed = time.perf_counter() - t0
            results.append({"ok": False, "error": str(exc), "elapsed": elapsed})
            print(f"  w{worker_id:02d}  req#{req_id:04d}{burst_tag}  ✗ ERROR  {elapsed:.3f}s  {exc}")

        queue.task_done()


# ──────────────────────────────────────────────────────────────────────────────
# Traffic generators
# ──────────────────────────────────────────────────────────────────────────────

async def steady_producer(queue: asyncio.Queue, total: int) -> int:
    """Constant rate — one request every STEADY_RATE_DELAY seconds."""
    req_id = 0
    try:
        while total == 0 or req_id < total:
            await queue.put((req_id, False))
            req_id += 1
            await asyncio.sleep(STEADY_RATE_DELAY)
    except asyncio.CancelledError:
        pass
    return req_id


async def burst_producer(queue: asyncio.Queue, total: int) -> int:
    """
    Steady baseline with periodic bursts — mimics real traffic (ad campaigns,
    cron jobs, viral spikes).

    Every BURST_EVERY requests we fire BURST_SIZE requests simultaneously
    with no inter-request delay.
    """
    req_id = 0
    try:
        while total == 0 or req_id < total:
            # Burst trigger
            if req_id > 0 and req_id % BURST_EVERY == 0:
                n = min(BURST_SIZE, total - req_id) if total else BURST_SIZE
                print(f"\n  ⚡ BURST  {n} requests fired simultaneously  (req#{req_id})\n")
                for _ in range(n):
                    await queue.put((req_id, True))
                    req_id += 1
                # No sleep after burst — that's the point
            else:
                await queue.put((req_id, False))
                req_id += 1
                # Poisson-like jitter: sleep drawn from exponential distribution
                await asyncio.sleep(random.expovariate(1 / STEADY_RATE_DELAY))
    except asyncio.CancelledError:
        pass
    return req_id


async def ramp_producer(queue: asyncio.Queue, total: int) -> int:
    """
    Starts slow and accelerates — simulates traffic ramp-up / stress test.
    Rate doubles every 30 seconds (up to 10× initial).
    """
    req_id   = 0
    t_start  = time.monotonic()
    try:
        while total == 0 or req_id < total:
            elapsed  = time.monotonic() - t_start
            # Doubles every 30s, capped at 10× base
            factor   = min(2 ** (elapsed / 30), 10)
            delay    = STEADY_RATE_DELAY / factor
            await queue.put((req_id, False))
            req_id += 1
            await asyncio.sleep(delay)
    except asyncio.CancelledError:
        pass
    return req_id


PRODUCERS = {
    "steady": steady_producer,
    "burst":  burst_producer,
    "ramp":   ramp_producer,
}


# ──────────────────────────────────────────────────────────────────────────────
# Runner
# ──────────────────────────────────────────────────────────────────────────────

async def run(total: int, concurrency: int, mode: str) -> None:
    queue:   asyncio.Queue = asyncio.Queue(maxsize=concurrency * 4)
    results: list          = []

    label = "∞" if total == 0 else str(total)
    print(f"Mode={mode}  |  total={label}  |  concurrency={concurrency}")
    print(f"Sending to {BALANCER_URL}\n")

    connector = aiohttp.TCPConnector(limit=concurrency + 8)
    async with aiohttp.ClientSession(connector=connector) as session:
        workers = [
            asyncio.create_task(worker(session, i, queue, results))
            for i in range(concurrency)
        ]

        producer_fn = PRODUCERS[mode]
        producer    = asyncio.create_task(producer_fn(queue, total))

        try:
            await producer
        except KeyboardInterrupt:
            producer.cancel()

        # Send poison pills to stop workers
        for _ in range(concurrency):
            await queue.put(None)

        await asyncio.gather(*workers)

    # ── Summary ──────────────────────────────────────────────────────────────
    ok      = [r for r in results if r["ok"]]
    errors  = [r for r in results if not r["ok"]]
    retried = [r for r in ok if r.get("retried")]

    if not results:
        return

    per_server: dict[str, int] = {}
    for r in ok:
        srv = r.get("server", "?")
        per_server[srv] = per_server.get(srv, 0) + 1

    avg_latency = sum(r["elapsed"] for r in ok) / len(ok) if ok else 0
    p99_latency = sorted(r["elapsed"] for r in ok)[int(len(ok) * 0.99)] if ok else 0

    print("\n" + "─" * 60)
    print(f"  Mode         : {mode}")
    print(f"  Total sent   : {len(results)}")
    print(f"  Succeeded    : {len(ok)}")
    print(f"  Errors/502s  : {len(errors)}")
    print(f"  Retried      : {len(retried)}  (succeeded after retry)")
    print(f"  Avg latency  : {avg_latency:.3f}s")
    print(f"  p99 latency  : {p99_latency:.3f}s")
    print(f"  Per server   :")
    for srv, count in sorted(per_server.items()):
        pct = count / len(ok) * 100 if ok else 0
        print(f"    {srv}: {count}  ({pct:.1f}%)")
    print("─" * 60)


# ──────────────────────────────────────────────────────────────────────────────
# Entry point
# ──────────────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(description="Load balancer test client")
    parser.add_argument("--total",       type=int, default=200,
                        help="Total requests (0 = run forever)")
    parser.add_argument("--concurrency", type=int, default=12,
                        help="Concurrent workers")
    parser.add_argument("--mode",        choices=list(PRODUCERS), default="burst",
                        help="Traffic shaping: steady | burst | ramp")
    args = parser.parse_args()

    try:
        asyncio.run(run(args.total, args.concurrency, args.mode))
    except KeyboardInterrupt:
        print("\nStopped.")


if __name__ == "__main__":
    main()