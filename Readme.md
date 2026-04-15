# Adaptive Load Balancer

Switches automatically between **Round Robin** and **Least Connections**
based on real-time connection imbalance across four actual HTTP servers.

## Architecture

```
client.py  ──▶  balancer.py :8000  ──▶  server.py :8001  (slow ⚠)
                                   ──▶  server.py :8002
                                   ──▶  server.py :8003
                                   ──▶  server.py :8004

dashboard.html  ◀── GET /stats (polls every 800ms)
```

## Install

```bash
pip install fastapi uvicorn aiohttp
```

## Run (3 terminals)

**Terminal 1 — backends**
```bash
python server.py
```

**Terminal 2 — load balancer + dashboard**
```bash
python balancer.py
# Open http://localhost:8000/dashboard
```

**Terminal 3 — client**
```bash
python client.py                        # 150 requests, 12 concurrent
python client.py --total 300 --concurrency 20
python client.py --total 0              # run forever
```

## How the switching works

| Condition | Action |
|-----------|--------|
| `max_active - min_active > IMBALANCE_THRESH (3)` | Switch RR → Least Connections |
| `max_active - min_active ≤ REBALANCE_THRESH (1)` | Switch back LC → Round Robin |

srv-8001 holds connections for 1.5–3s (others 0.1–0.5s), which quickly
creates the imbalance that triggers the switch. Once LC drains the slow
server and balance is restored, it flips back to RR.

## Files

| File | Responsibility |
|------|---------------|
| `config.py`     | All constants — ports, thresholds, delays |
| `server.py`     | 4 FastAPI backends (one intentionally slow) |
| `balancer.py`   | aiohttp proxy + RR/LC logic + /stats API |
| `client.py`     | Concurrent request generator |
| `dashboard.html`| Live browser dashboard (served by balancer) |