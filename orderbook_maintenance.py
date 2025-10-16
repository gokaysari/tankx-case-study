#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
TankX – Case Study 1: Order Book Maintenance (Parameterized CLI + CSV Export)

Usage examples:
  # 1) Process full stream and write top-10 to CSV
  python orderbook_maintenance.py \
    --file /mnt/data/orderbooks-10.csv \
    --symbol "BTC/USD" \
    --until 1e20 \
    --out-prefix results/btc_depth10

  # 2) Include NotionalAhead result
  python orderbook_maintenance.py \
    --file /mnt/data/orderbooks-10.csv \
    --symbol "BTC/USD" \
    --until 1e20 \
    --notional-ahead bid 112300 \
    --out-prefix results/na_demo

  # 3) Simulate a limit order and export post-trade top-10
  python orderbook_maintenance.py \
    --file /mnt/data/orderbooks-1000.csv \
    --symbol "BTC/USD" \
    --until 1e20 \
    --place-limit buy 112300 0.5 \
    --out-prefix results/limit_after
"""

from typing import List, Tuple, Dict, Optional
from dataclasses import dataclass, field
import csv
import ast
import bisect
import time
import argparse
import os


# -----------------------------
# Order Book Implementation
# -----------------------------

@dataclass
class SideBook:
    """Maintains one side (bids or asks) of the order book."""
    is_ask: bool
    prices: List[float] = field(default_factory=list)
    qty: Dict[float, float] = field(default_factory=dict)

    def _key_index(self, price: float) -> int:
        """Return insertion index obeying side ordering (asks↑, bids↓)."""
        if self.is_ask:
            return bisect.bisect_left(self.prices, price)
        # bids: descending; emulate bisect via negatives
        negs = [-p for p in self.prices]
        return bisect.bisect_left(negs, -price)

    def update_level(self, price: float, new_qty: float) -> None:
        """Set aggregate quantity at price; remove level if new_qty <= 0."""
        if new_qty <= 0:
            if price in self.qty:
                idx = self.prices.index(price)
                self.prices.pop(idx)
                self.qty.pop(price, None)
            return
        if price in self.qty:
            self.qty[price] = new_qty
        else:
            idx = self._key_index(price)
            self.prices.insert(idx, price)
            self.qty[price] = new_qty

    def notional_ahead(self, price: float) -> float:
        """
        Sum(price_i * qty_i) for all levels better-or-equal than `price`.
        For asks: better means lower-or-equal; for bids: higher-or-equal.
        """
        if not self.prices:
            return 0.0
        total = 0.0
        if self.is_ask:  # ascending
            idx = bisect.bisect_right(self.prices, price) - 1
            if idx < 0:
                return 0.0
            for p in self.prices[: idx + 1]:
                total += p * self.qty.get(p, 0.0)
        else:  # descending
            for p in self.prices:
                if p >= price:
                    total += p * self.qty.get(p, 0.0)
                else:
                    break
        return total

    def best_n(self, n: int) -> List[Tuple[float, float]]:
        """Return top-n levels as (price, quantity)."""
        return [(p, self.qty[p]) for p in self.prices[:n]]


class OrderBook:
    """Full order book: bids + asks; applies snapshot/diff; queries and sims."""
    def __init__(self) -> None:
        self.bids = SideBook(is_ask=False)
        self.asks = SideBook(is_ask=True)

    def apply_snapshot(self, bids: List[List[float]], asks: List[List[float]]) -> None:
        self.bids = SideBook(is_ask=False)
        self.asks = SideBook(is_ask=True)
        for price, qty in bids:
            self.bids.update_level(float(price), float(qty))
        for price, qty in asks:
            self.asks.update_level(float(price), float(qty))

    def apply_diff(self, bids: List[List[float]], asks: List[List[float]]) -> None:
        for price, qty in bids:
            self.bids.update_level(float(price), float(qty))
        for price, qty in asks:
            self.asks.update_level(float(price), float(qty))

    def notional_ahead(self, side: str, price: float) -> float:
        s = side.lower()
        if s in ("ask", "asks", "sell"):
            return self.asks.notional_ahead(price)
        if s in ("bid", "bids", "buy"):
            return self.bids.notional_ahead(price)
        raise ValueError("side must be bid/buy or ask/sell")

    def place_limit_order(self, side: str, price: float, qty: float):
        """
        Simulate a limit order: cross at book prices if marketable; residual
        rests at the limit price on own side. Return new top-10.
        """
        s = side.lower()
        price = float(price)
        qty = float(qty)
        if qty <= 0:
            return self.top10()

        if s in ("buy", "bid"):
            while self.asks.prices and self.asks.prices[0] <= price and qty > 1e-15:
                p = self.asks.prices[0]
                level_q = self.asks.qty[p]
                take = min(level_q, qty)
                self.asks.update_level(p, level_q - take)
                if p in self.asks.qty and self.asks.qty[p] <= 1e-15:
                    self.asks.update_level(p, 0.0)
                qty -= take
            if qty > 1e-15:
                existing = self.bids.qty.get(price, 0.0)
                self.bids.update_level(price, existing + qty)

        elif s in ("sell", "ask"):
            while self.bids.prices and self.bids.prices[0] >= price and qty > 1e-15:
                p = self.bids.prices[0]
                level_q = self.bids.qty[p]
                take = min(level_q, qty)
                self.bids.update_level(p, level_q - take)
                if p in self.bids.qty and self.bids.qty[p] <= 1e-15:
                    self.bids.update_level(p, 0.0)
                qty -= take
            if qty > 1e-15:
                existing = self.asks.qty.get(price, 0.0)
                self.asks.update_level(price, existing + qty)
        else:
            raise ValueError("side must be buy/bid or sell/ask")

        return self.top10()

    def top10(self):
        return {"bids": self.bids.best_n(10), "asks": self.asks.best_n(10)}


class OrderBookEngine:
    """Streams a CSV and maintains per-symbol order books."""
    def __init__(self, path: str) -> None:
        self.path = path
        self.books: Dict[str, OrderBook] = {}

    def _book(self, symbol: str) -> OrderBook:
        if symbol not in self.books:
            self.books[symbol] = OrderBook()
        return self.books[symbol]

    @staticmethod
    def _parse(cell: str) -> List[List[float]]:
        if not cell or cell == "[]":
            return []
        data = ast.literal_eval(cell)  # safe literal parse
        return [[float(p), float(q)] for p, q in data]

    def build_until(self, symbol: str, until_ts: float):
        book = self._book(symbol)
        seen = False
        with open(self.path, "r", newline="") as f:
            reader = csv.DictReader(f)
            for row in reader:
                if row["symbol"] != symbol:
                    continue
                ts = float(row["time"])
                bids = self._parse(row["bids"])
                asks = self._parse(row["asks"])
                if not seen:
                    book.apply_snapshot(bids, asks)
                    seen = True
                else:
                    book.apply_diff(bids, asks)
                if ts >= until_ts:
                    break
        return book.top10()

    def expose(self, symbol: str) -> Optional[OrderBook]:
        return self.books.get(symbol, None)


# -----------------------------
# CSV Export Helpers
# -----------------------------

def ensure_dir_for(prefix: str) -> None:
    d = os.path.dirname(prefix)
    if d and not os.path.exists(d):
        os.makedirs(d, exist_ok=True)

def write_levels_csv(path: str, rows: List[Tuple[float, float]]) -> None:
    with open(path, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["price", "quantity"])
        for p, q in rows:
            w.writerow([f"{p:.10f}", f"{q:.10f}"])

def write_summary_csv(path: str, summary: Dict[str, str]) -> None:
    with open(path, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["key", "value"])
        for k, v in summary.items():
            w.writerow([k, v])


# -----------------------------
# CLI
# -----------------------------

def main():
    ap = argparse.ArgumentParser(description="TankX Case Study 1 – Order Book Maintenance (CSV Export)")
    ap.add_argument("--file", required=True, help="Path to orderbook CSV (orderbooks-10.csv or orderbooks-1000.csv)")
    ap.add_argument("--symbol", required=True, help="Symbol to process, e.g., BTC/USD")
    ap.add_argument("--until", type=float, required=True, help="Timestamp (inclusive) to process up to")

    ap.add_argument("--out-prefix", required=True, help="Output prefix for CSV files, e.g., results/run1")
    ap.add_argument("--notional-ahead", nargs=2, metavar=("SIDE", "PRICE"),
                    help="Compute notional ahead for side at price (e.g., bid 112300) and add to summary")
    ap.add_argument("--place-limit", nargs=3, metavar=("SIDE", "PRICE", "QTY"),
                    help="Simulate a limit order and export the new top-10")

    args = ap.parse_args()

    ensure_dir_for(args.out_prefix)

    # 1) Build the book
    t0 = time.time()
    engine = OrderBookEngine(args.file)
    top10 = engine.build_until(args.symbol, args.until)
    elapsed = time.time() - t0

    # 2) Export top-10
    bids_path = f"{args.out_prefix}_top10_bids.csv"
    asks_path = f"{args.out_prefix}_top10_asks.csv"
    write_levels_csv(bids_path, top10["bids"])
    write_levels_csv(asks_path, top10["asks"])

    # 3) Optional: NotionalAhead
    notional_value = None
    if args.notional_ahead:
        side, price_str = args.notional_ahead
        price = float(price_str)
        ob = engine.expose(args.symbol)
        if not ob:
            raise RuntimeError("OrderBook not found after build_until.")
        notional_value = ob.notional_ahead(side, price)

    # 4) Optional: PlaceLimitOrder and export post-trade top-10
    after_bids_path = after_asks_path = None
    if args.place_limit:
        side, price_str, qty_str = args.place_limit
        price, qty = float(price_str), float(qty_str)
        ob = engine.expose(args.symbol)
        if not ob:
            raise RuntimeError("OrderBook not found after build_until.")
        after = ob.place_limit_order(side, price, qty)
        after_bids_path = f"{args.out_prefix}_after_place_limit_bids.csv"
        after_asks_path = f"{args.out_prefix}_after_place_limit_asks.csv"
        write_levels_csv(after_bids_path, after["bids"])
        write_levels_csv(after_asks_path, after["asks"])

    # 5) Summary CSV
    summary = {
        "file": args.file,
        "symbol": args.symbol,
        "until": str(args.until),
        "process_seconds": f"{elapsed:.6f}",
        "top10_bids_csv": bids_path,
        "top10_asks_csv": asks_path,
    }
    if notional_value is not None:
        summary["notional_ahead"] = f"{notional_value:.10f}"
        summary["notional_side"] = args.notional_ahead[0]
        summary["notional_price"] = args.notional_ahead[1]
    if after_bids_path:
        summary["after_place_limit_bids_csv"] = after_bids_path
    if after_asks_path:
        summary["after_place_limit_asks_csv"] = after_asks_path
    write_summary_csv(f"{args.out_prefix}_summary.csv", summary)

    # Console info
    print("Done.")
    print("Top-10 written to:")
    print(" ", bids_path)
    print(" ", asks_path)
    if notional_value is not None:
        print(f"NotionalAhead({args.notional_ahead[0]},{args.notional_ahead[1]}) = {notional_value:.10f}")
    if after_bids_path and after_asks_path:
        print("After PlaceLimitOrder written to:")
        print(" ", after_bids_path)
        print(" ", after_asks_path)
    print("Summary:", f"{args.out_prefix}_summary.csv")


if __name__ == "__main__":
    main()
