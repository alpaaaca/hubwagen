"""Hauls passing dataframes between lifts; test_frames.py runs them on a
real worker (needs polars, pandas and pyarrow)."""

import pandas as pd
import polars as pl

import hubwagen as hw


@hw.lift(memo=True)
def frame_orders(day: str) -> pl.DataFrame:
    return pl.DataFrame({"region": ["eu", "us", "eu"], "amount": [10, 20, 5], "day": [day] * 3})


@hw.lift(memo=True)
def frame_totals(orders: pl.LazyFrame) -> pl.DataFrame:
    return orders.group_by("region").agg(pl.col("amount").sum()).sort("region").collect()


@hw.lift
def frame_report(totals: pd.DataFrame, raw: hw.Pallet) -> dict:
    return {"totals": dict(zip(totals["region"], totals["amount"].astype(int))),
            "type": type(totals).__name__, "raw_files": [p.name for p in raw.files()]}


@hw.haul
def frames(ctx, day: str):
    orders = ctx.lift(frame_orders, day)
    totals = ctx.lift(frame_totals, orders)
    report = ctx.lift(frame_report, totals, orders)
    return {"report": report, "orders": orders.result, "dataset": orders.dataset,
            "reused": [orders.reused, totals.reused]}
