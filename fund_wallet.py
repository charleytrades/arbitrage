#!/usr/bin/env python3
"""Swap MATIC -> USDC.e on Polygon via QuickSwap V2.

Throwaway script — run once to fund the bot wallet.
Uses the MATIC already in the wallet, swaps most of it to USDC.e,
keeps a reserve for gas. No external accounts or bridges needed.
"""
import asyncio
import os
import sys
import time

import httpx
from dotenv import load_dotenv
from eth_abi import encode, decode
from eth_account import Account

load_dotenv(os.path.join(os.path.dirname(__file__), ".env"))

RPC = "https://polygon-bor-rpc.publicnode.com"
QUICKSWAP_V2_ROUTER = "0xa5E0829CaCEd8fFDD4De3c43696c57F7D7A678ff"
WMATIC = "0x0d500B1d8E8eF31E21C99d1Db9A6444d3ADf1270"
USDC_E = "0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174"
CHAIN_ID = 137
KEEP_MATIC = 10       # reserve for gas
SLIPPAGE_PCT = 3      # slippage tolerance


async def rpc_call(client, method, params):
    resp = await client.post(RPC, json={
        "jsonrpc": "2.0", "method": method, "params": params, "id": 1,
    })
    data = resp.json()
    if "error" in data:
        raise Exception(f"RPC error: {data['error']}")
    return data["result"]


async def main():
    private_key = os.getenv("PRIVATE_KEY", "")
    if not private_key:
        sys.exit("ERROR: PRIVATE_KEY not found in .env")
    if not private_key.startswith("0x"):
        private_key = "0x" + private_key

    account = Account.from_key(private_key)
    address = account.address

    async with httpx.AsyncClient(timeout=30) as client:
        # ── 1. Check MATIC balance ──────────────────────────────────
        bal_hex = await rpc_call(client, "eth_getBalance", [address, "latest"])
        bal_matic = int(bal_hex, 16) / 1e18

        print(f"Wallet:        {address}")
        print(f"MATIC balance: {bal_matic:.4f}")

        if bal_matic <= KEEP_MATIC + 1:
            sys.exit(f"Need > {KEEP_MATIC + 1} MATIC, have {bal_matic:.4f}")

        swap_matic = bal_matic - KEEP_MATIC
        swap_wei = int(swap_matic * 1e18)

        # ── 2. Get quote ────────────────────────────────────────────
        path = [WMATIC, USDC_E]
        quote_data = "0xd06ca61f" + encode(
            ["uint256", "address[]"], [swap_wei, path],
        ).hex()

        quote_hex = await rpc_call(client, "eth_call", [
            {"to": QUICKSWAP_V2_ROUTER, "data": quote_data}, "latest",
        ])
        amounts = decode(["uint256[]"], bytes.fromhex(quote_hex[2:]))[0]
        expected_usdc = amounts[-1] / 1e6
        min_out = int(amounts[-1] * (100 - SLIPPAGE_PCT) / 100)

        print(f"\n--- Swap Plan ---")
        print(f"Sell:     {swap_matic:.4f} MATIC")
        print(f"Receive:  ~{expected_usdc:.2f} USDC.e")
        print(f"Min out:  {min_out / 1e6:.2f} USDC.e ({SLIPPAGE_PCT}% slippage)")
        print(f"Keep:     {KEEP_MATIC} MATIC for gas")

        # ── 3. Build swap tx ────────────────────────────────────────
        deadline = int(time.time()) + 300
        swap_calldata = bytes.fromhex("7ff36ab5") + encode(
            ["uint256", "address[]", "address", "uint256"],
            [min_out, path, address, deadline],
        )

        nonce = int(await rpc_call(
            client, "eth_getTransactionCount", [address, "latest"],
        ), 16)

        gas_price = int(int(await rpc_call(
            client, "eth_gasPrice", [],
        ), 16) * 1.2)

        tx = {
            "nonce": nonce,
            "gasPrice": gas_price,
            "gas": 300_000,
            "to": QUICKSWAP_V2_ROUTER,
            "value": swap_wei,
            "data": swap_calldata,
            "chainId": CHAIN_ID,
        }

        # ── 4. Sign & send ─────────────────────────────────────────
        print("\nSending swap transaction...")
        signed = Account.sign_transaction(tx, private_key)
        tx_hash = await rpc_call(
            client, "eth_sendRawTransaction",
            ["0x" + signed.raw_transaction.hex()],
        )
        print(f"TX hash: {tx_hash}")

        # ── 5. Wait for receipt ─────────────────────────────────────
        print("Waiting for confirmation...", end="", flush=True)
        for _ in range(60):
            receipt = await rpc_call(
                client, "eth_getTransactionReceipt", [tx_hash],
            )
            if receipt:
                ok = receipt["status"] == "0x1"
                print(f" {'SUCCESS' if ok else 'FAILED'}")
                if not ok:
                    sys.exit("Swap failed on-chain. Check tx on polygonscan.")
                break
            print(".", end="", flush=True)
            await asyncio.sleep(2)
        else:
            sys.exit("Timed out. Check tx manually on polygonscan.")

        # ── 6. Final balances ───────────────────────────────────────
        final_matic = int(await rpc_call(
            client, "eth_getBalance", [address, "latest"],
        ), 16) / 1e18

        usdc_call = "0x70a08231" + "0" * 24 + address[2:].lower()
        final_usdc = int(await rpc_call(client, "eth_call", [
            {"to": USDC_E, "data": usdc_call}, "latest",
        ]), 16) / 1e6

        print(f"\n--- Done ---")
        print(f"MATIC:  {final_matic:.4f}")
        print(f"USDC.e: {final_usdc:.2f}")
        print(f"\nWallet funded. Ready to trade.")


if __name__ == "__main__":
    asyncio.run(main())
