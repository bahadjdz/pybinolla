
# pybinolla

<p align="center">
  <img src="logo.png" alt="pybinolla logo" width="160" />
</p>

<p align="center">
  <img src="https://img.shields.io/badge/Python-3.10+-3776AB?style=for-the-badge&logo=python&logoColor=white" alt="Python 3.10+" />
  <img src="https://img.shields.io/badge/Binolla-API-00B0FF?style=for-the-badge" alt="Binolla API" />
  <img src="https://img.shields.io/badge/AsyncIO-Event--Driven-4CAF50?style=for-the-badge" alt="AsyncIO" />
  <img src="https://img.shields.io/badge/License-MIT-yellow?style=for-the-badge" alt="MIT License" />
  <img src="https://img.shields.io/badge/WebSocket-Socket.IO%20v4-blueviolet?style=for-the-badge" alt="Socket.IO v4" />
</p>

<p align="center">
  <b>The Python library for the <a href="https://binolla.com">Binolla</a> binary options platform.</b><br/>
  Async · Event-driven · WebSocket · Demo Trading · Candle Data · Real-time Quotes
</p>

> **pybinolla** is an unofficial, open-source Python library that gives you full programmatic access to the Binolla trading platform.  
> Built on top of native `asyncio`, Socket.IO v4, and a Cloudflare-aware HTTP session — giving you low-latency, production-ready access to market data and demo trading.

---

## ⚡ Features Comparison: Community vs Pro

| Feature | 🆓 Community (Free) | 💎 Pro Edition |
| :--- | :---: | :---: |
| **Platform Login & Cloudflare Bypass** | ✅ Full Support | ✅ Full Support |
| **Session Persistence (`binolla_session.json`)** | ✅ Yes | ✅ Yes |
| **Real-time Price & Tick Streaming** | ✅ Yes (~3 ticks/s) | ✅ Yes (Ultra-low latency) |
| **Historical Candle Data (`history/region`)** | ✅ M1, M5, M15, M30, H1 | ✅ All Periods + Tick-level |
| **Demo Account Trading (`AccountType.DEMO`)** | ✅ Unlimited | ✅ Unlimited |
| **Balance Queries** | ✅ Demo + Live snapshots | ✅ Demo + Live snapshots |
| **Real Account Live Trading (`AccountType.REAL`)** | ❌ Pro Only | 🔥 **Full Real Account Trading** |
| **Blitz / Turbo Expirations (5s / 15s / 30s)** | ❌ Pro Only | 🔥 **5s, 10s, 15s, 30s Blitz Modes** |
| **Payout % Live Inspector** | ❌ Pro Only | 🔥 **Automated Payout Filter (>85%)** |
| **Multi-Account Copy Trading** | ❌ Pro Only | 🔥 **Copy Trading Engine** |
| **Risk Management & Martingale Helpers** | ❌ Pro Only | 🔥 **Automated Martingale / Stop-Loss** |
| **Anti-Disconnection Health Watchdog** | Basic | 🔥 **Zero-Downtime Auto-Reconnect** |

> 💡 **Interested in the Pro version?**  
> Contact on Telegram: **[@YourTelegramUsername](https://t.me/YourTelegramUsername)** or open an inquiry on GitHub Discussions.

---

## 📦 Installation

```bash
# Clone the repository
git clone https://github.com/bahadjdz/pybinolla.git
cd pybinolla

# Install dependencies
pip install -r requirements.txt

# Or install as a package (editable mode)
pip install -e .
```

---

## 🚀 Quick Start

### 1. Check Balances & Fetch Candles

```python
import asyncio
from pybinolla import Binolla, Period, AccountType

EMAIL = "your_email@example.com"
PASSWORD = "your_password"

async def main():
    client = Binolla(email=EMAIL, password=PASSWORD)

    # 1. Connect (Demo mode)
    ok, reason = await client.connect(account=AccountType.DEMO)
    if not ok:
        print("Login failed:", reason)
        return

    # 2. Query Balance
    bal = await client.get_balance()
    print(f"Demo Balance: ${bal.demo_balance:.2f} | Live Balance: ${bal.live_balance:.2f}")

    # 3. Fetch 100 historical 1-minute candles for EUR/USD (OTC)
    candles = await client.get_candles("EURUSD_otc", count=100, period=Period.M1)
    print(f"Retrieved {len(candles)} candles.")
    if candles:
        latest = candles[-1]
        print(f"Latest Close: {latest.close} at {latest.datetime_utc}")

    await client.close()

asyncio.run(main())
```

### 2. Place a Demo Trade & Track Result

```python
import asyncio
from pybinolla import Binolla, Direction, Period

async def main():
    client = Binolla(email="your_email@example.com", password="your_password")
    await client.connect()

    # Place a 1$ CALL trade for 1 minute on EURUSD_otc
    order_id = await client.buy(
        asset="EURUSD_otc",
        direction=Direction.CALL,
        amount=1.0,
        period=Period.M1
    )
    print(f"Trade submitted with ID: {order_id}")

    # Await settlement result at expiry
    result = await client.wait_for_result(order_id, timeout=90)
    if result:
        outcome = "WIN 🎉" if result.win else "LOSS ❌"
        print(f"Result: {outcome} | PnL: ${result.profit:+.2f}")

    await client.close()

asyncio.run(main())
```

---

## 🏗️ Architecture

```
pybinolla/
├── pybinolla/
│   ├── __init__.py          # Public exports
│   ├── client.py            # Binolla — high-level unified interface
│   ├── api.py               # BinollaAPI — WebSocket + HTTP orchestration
│   ├── websocket_client.py  # BinollaWebSocket — Socket.IO v4 wire engine
│   ├── http_client.py       # BrowserSession + TLS Cipher Adapter (Cloudflare bypass)
│   ├── connection.py        # ConnectionState + async wait primitives
│   ├── event_registry.py    # Async key-value event bus
│   ├── candles.py           # Tick aggregation & candle parser
│   ├── models.py            # Candle, Balance, Order, OrderResult dataclasses
│   ├── enums.py             # Direction, AccountType, Period constants
│   └── exceptions.py        # Custom typed exceptions
├── examples/
│   ├── 01_get_balance.py    # Balance retrieval example
│   ├── 02_fetch_candles.py  # Historical candles fetching
│   ├── 03_stream_quotes.py  # Real-time price streaming
│   └── 04_demo_trade.py     # Demo trade execution & settlement
├── pyproject.toml           # Modern package configuration
├── LICENSE                  # MIT License
└── README.md                # Documentation
```

---

## 🔌 Wire Protocol Details

Binolla runs an event-driven Engine.IO / Socket.IO v4 cluster:

- **WebSocket Gateway:** `wss://ws3.binolla.com/socket.io/?EIO=4&transport=websocket`
- **Handshake:** `0{"sid":...} -> 40 -> 40{"sid":...}`
- **Authentication:** `42["authorization", {"token": "<JWT>", "userAccountType": 1, "uaid": 1}]`
- **Responses:** `42["s_authorization"]`, `42["s_assets/list"]`, `42["s_balances/list"]`
- **Quotes Stream:** `42["s_quotes/list", [["EURUSD_otc", timestamp, price, is_tick]]]`
- **History Request:** `42["history/region", {"asset": ..., "time": ..., "offset": ..., "period": ...}]`

---

## ⚠️ Disclaimer

- This is an **unofficial** educational library and is not affiliated with or endorsed by Binolla.
- Automated and binary options trading involves substantial risk of financial loss. Always practice on a Demo account.
- The authors assume no liability for financial loss resulting from the use of this software.

---

## 📄 License

Distributed under the **MIT License**. See `LICENSE` for details.

---

## 🔎 Keywords

`binolla` · `binolla api` · `binolla python` · `binolla bot` · `binolla trading bot` · `binary options python` · `binary options api` · `binary options bot` · `pybinolla` · `binolla websocket` · `binolla async` · `binolla candles` · `binolla demo trading` · `python trading bot` · `asyncio trading` · `socket.io trading`
