"""Hauls passing dataframes between lifts; test_frames.py runs them on a
real worker (needs polars, pandas and pyarrow)."""

import pandas as pd
import polars as pl

import moksha as mk


@mk.lift(memo=True)
def frame_orders(day: str) -> pl.DataFrame:
    return pl.DataFrame({"region": ["eu", "us", "eu"], "amount": [10, 20, 5], "day": [day] * 3})


@mk.lift(memo=True)
def frame_totals(orders: pl.LazyFrame) -> pl.DataFrame:
    return orders.group_by("region").agg(pl.col("amount").sum()).sort("region").collect()


@mk.lift
def frame_report(totals: pd.DataFrame, raw: mk.Pallet) -> dict:
    return {"totals": dict(zip(totals["region"], totals["amount"].astype(int))),
            "type": type(totals).__name__, "raw_files": [p.name for p in raw.files()]}


@mk.haul
def frames(ctx, day: str):
    orders = ctx.lift(frame_orders, day)
    totals = ctx.lift(frame_totals, orders)
    report = ctx.lift(frame_report, totals, orders)
    return {"report": report, "orders": orders.result, "dataset": orders.dataset,
            "reused": [orders.reused, totals.reused]}
