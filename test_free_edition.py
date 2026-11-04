"""
test_free_edition.py
====================
Comprehensive test for pybinolla Free Community Edition.

Tests:
  1. Import & version
  2. Pro blocks (REAL account raises BinollaProFeatureError)
  3. Live DEMO connect
  4. get_balance()
  5. get_candles()
  6. Demo buy() + wait_for_result()
  7. Pro block on buy() with REAL account_type (without reconnecting)
"""

import asyncio
import sys
import os

# Allow running from the pybinolla-free folder
sys.path.insert(0, os.path.dirname(__file__))

from pybinolla import Binolla, AccountType, Direction, Period, BinollaProFeatureError

EMAIL    = "d3f3ed4883@emailnox.live"
PASSWORD = "d3f3ed4883@emailnox.live"

PASS = "✅ PASS"
FAIL = "❌ FAIL"


def section(title: str):
    print(f"\n{'='*55}")
    print(f"  {title}")
    print('='*55)


async def main():
    errors = 0

    # ------------------------------------------------------------------ #
    # 1. Import & version
    # ------------------------------------------------------------------ #
    section("1 · Import & version")
    import pybinolla
    ver = pybinolla.__version__
    print(f"  pybinolla version : {ver}")
    if "free" in ver.lower() or ver.startswith("1."):
        print(f"  {PASS}  version OK")
    else:
        print(f"  {FAIL}  unexpected version: {ver}")
        errors += 1

    exports = ["Binolla", "AccountType", "Direction", "Period",
               "BinollaProFeatureError"]
    for name in exports:
        if hasattr(pybinolla, name):
            print(f"  {PASS}  {name} exported")
        else:
            print(f"  {FAIL}  {name} missing from pybinolla.__init__")
            errors += 1

    # ------------------------------------------------------------------ #
    # 2. Pro block — connect(REAL) must raise BinollaProFeatureError
    # ------------------------------------------------------------------ #
    section("2 · Pro block: connect(REAL)")
    client = Binolla(email=EMAIL, password=PASSWORD)
    try:
        await client.connect(AccountType.REAL)
        print(f"  {FAIL}  No exception raised!")
        errors += 1
    except BinollaProFeatureError as e:
        print(f"  {PASS}  BinollaProFeatureError raised:")
        print(f"         {str(e)[:80]}...")
    except Exception as e:
        print(f"  {FAIL}  Wrong exception type: {type(e).__name__}: {e}")
        errors += 1

    # ------------------------------------------------------------------ #
    # 3. Pro block — change_account(REAL) must raise BinollaProFeatureError
    # ------------------------------------------------------------------ #
    section("3 · Pro block: change_account(REAL)")
    client2 = Binolla(email=EMAIL, password=PASSWORD)
    try:
        await client2.change_account(AccountType.REAL)
        print(f"  {FAIL}  No exception raised!")
        errors += 1
    except BinollaProFeatureError as e:
        print(f"  {PASS}  BinollaProFeatureError raised")
    except Exception as e:
        print(f"  {FAIL}  Wrong exception: {type(e).__name__}: {e}")
        errors += 1

    # ------------------------------------------------------------------ #
    # 4. DEMO connect (live network)
    # ------------------------------------------------------------------ #
    section("4 · DEMO connect (live)")
    client = Binolla(email=EMAIL, password=PASSWORD)
    ok, reason = await client.connect(AccountType.DEMO)
    if ok:
        print(f"  {PASS}  connect → True  ({reason})")
    else:
        print(f"  {FAIL}  connect failed: {reason}")
        errors += 1
        print("  Skipping remaining network tests.")
        print(f"\n{'='*55}")
        print(f"  Result: {errors} failure(s)")
        print('='*55)
        return

    # ------------------------------------------------------------------ #
    # 5. get_balance()
    # ------------------------------------------------------------------ #
    section("5 · get_balance()")
    await asyncio.sleep(1)
    bal = await client.get_balance()
    if bal:
        print(f"  {PASS}  demo_balance = ${bal.demo_balance:.2f}")
    else:
        print(f"  {FAIL}  balance is None")
        errors += 1

    # ------------------------------------------------------------------ #
    # 6. get_candles()
    # ------------------------------------------------------------------ #
    section("6 · get_candles(EURUSD_otc, count=20)")
    candles = await client.get_candles("EURUSD_otc", count=20, period=Period.M1)
    if len(candles) >= 10:
        print(f"  {PASS}  received {len(candles)} candles")
        print(f"         newest: O={candles[-1].open:.5f} C={candles[-1].close:.5f}")
    else:
        print(f"  {FAIL}  only {len(candles)} candles (expected ≥10)")
        errors += 1

    # ------------------------------------------------------------------ #
    # 7. is_connected()
    # ------------------------------------------------------------------ #
    section("7 · is_connected()")
    alive = await client.is_connected()
    if alive:
        print(f"  {PASS}  is_connected → True")
    else:
        print(f"  {FAIL}  is_connected → False")
        errors += 1

    # ------------------------------------------------------------------ #
    # 8. Demo buy() + wait_for_result()
    # ------------------------------------------------------------------ #
    section("8 · Demo buy() + wait_for_result() [~60s wait]")
    print("  Placing $1 CALL on EURUSD_otc (1-min expiry)…")
    bal_before = await client.get_balance()
    order_id = await client.buy(
        "EURUSD_otc", Direction.CALL, amount=1.0, period=Period.M1
    )
    print(f"  order_id = {order_id}")
    if order_id:
        print("  Waiting for settlement…")
        result = await client.wait_for_result(order_id, timeout=120)
        if result:
            outcome = "WIN" if result.win else "LOSS"
            print(f"  {PASS}  Result: {outcome}  profit={result.profit:.2f}")
        else:
            print(f"  {FAIL}  wait_for_result returned None")
            errors += 1
    else:
        print(f"  {FAIL}  buy() returned 0/None")
        errors += 1

    # ------------------------------------------------------------------ #
    # 9. Pro block on buy() — manually set _account_type = REAL
    # ------------------------------------------------------------------ #
    section("9 · Pro block: buy() with REAL account_type")
    client._account_type = AccountType.REAL
    try:
        await client.buy("EURUSD_otc", Direction.CALL, amount=1.0)
        print(f"  {FAIL}  No exception raised!")
        errors += 1
    except BinollaProFeatureError as e:
        print(f"  {PASS}  BinollaProFeatureError raised on buy(REAL)")
    except Exception as e:
        print(f"  {FAIL}  Wrong exception: {type(e).__name__}: {e}")
        errors += 1

    # ------------------------------------------------------------------ #
    # Cleanup
    # ------------------------------------------------------------------ #
    await client.close()

    section("SUMMARY")
    if errors == 0:
        print(f"  🎉  ALL TESTS PASSED — pybinolla-free is GitHub-ready!")
    else:
        print(f"  ⚠️   {errors} test(s) failed — fix before publishing.")
    print('='*55)


if __name__ == "__main__":
    asyncio.run(main())

