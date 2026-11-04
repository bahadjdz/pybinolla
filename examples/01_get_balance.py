"""
Example 01: Check Account Balance
==================================
Demonstrates how to log in, connect to the WebSocket, and query
both DEMO and LIVE balances.
"""

import asyncio
from pybinolla import Binolla, AccountType

EMAIL = "YOUR_EMAIL@example.com"
PASSWORD = "YOUR_PASSWORD"


async def main():
    client = Binolla(email=EMAIL, password=PASSWORD)

    print("Connecting to Binolla (Demo mode)...")
    ok, reason = await client.connect(account=AccountType.DEMO)
    if not ok:
        print(f"Failed to connect: {reason}")
        return

    print("Successfully connected and authenticated!")

    # Wait briefly for balance frame
    await asyncio.sleep(1.0)
    balance = await client.get_balance()

    if balance:
        print(f"Demo Balance : ${balance.demo_balance:.2f} {balance.currency}")
        print(f"Live Balance : ${balance.live_balance:.2f} {balance.currency}")
    else:
        print("Could not retrieve balance.")

    await client.close()
    print("Disconnected.")


if __name__ == "__main__":
    asyncio.run(main())

