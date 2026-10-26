"""Daily revenue: extract → transform → report, one haul per day.

    hubwagen dev
    python -m hubwagen.worker revenue.hauls        # from pipelines/examples/revenue
    hubwagen replay daily --param day=2026-10-01..2026-10-07

extract_orders reads a source that can change, so it is not memoized: it
runs every time. daily_revenue and revenue_report are memoized: when the
orders for a day come out identical (same content hash), they are reused,
and when their own code changes, they run again.
"""

import csv
from decimal import Decimal
from pathlib import Path

import hubwagen as hw

SOURCE = Path(__file__).resolve().parent.parent / "data" / "orders.csv"


@hw.lift(output=hw.Dataset("raw.orders", partition="day"))
def extract_orders(day: str) -> dict:
    """The day's orders from the source system (here: a CSV file)."""
    with SOURCE.open() as f:
        rows = [r for r in csv.DictReader(f) if r["day"] == day]
    out = hw.output()
    with (out.path / "orders.csv").open("w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["order_id", "region", "amount"])
        w.writerows([r["order_id"], r["region"], r["amount"]] for r in rows)
    out.stats["rows"] = len(rows)
    return {"rows": len(rows)}


@hw.lift(memo=True, output=hw.Dataset("mart.daily_revenue", partition="day"))
def daily_revenue(orders: hw.Pallet, day: str) -> dict:
    """Revenue per region."""
    totals: dict[str, Decimal] = {}
    with orders.path("orders.csv").open() as f:
        for r in csv.DictReader(f):
            totals[r["region"]] = totals.get(r["region"], Decimal(0)) + Decimal(r["amount"])
    totals = {region: amount.quantize(Decimal("0.01")) for region, amount in totals.items()}
    out = hw.output()
    with (out.path / "revenue.csv").open("w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["region", "revenue"])
        w.writerows(sorted(totals.items()))
    total = sum(totals.values(), Decimal(0))
    out.meta["total"] = str(total)
    return {"regions": len(totals), "total": str(total)}


@hw.lift(memo=True, output=hw.Dataset("reports.revenue", partition="day"))
def revenue_report(revenue: hw.Pallet, day: str) -> str:
    """A short text report."""
    with revenue.path("revenue.csv").open() as f:
        rows = list(csv.DictReader(f))
    lines = [f"Revenue on {day}"] + [f"  {r['region']:<5} {r['revenue']:>10}" for r in rows]
    (hw.output().path / "report.txt").write_text("\n".join(lines) + "\n")
    return lines[0]


@hw.haul
def daily(ctx, day: str):
    orders = ctx.lift(extract_orders, day)
    revenue = ctx.lift(daily_revenue, orders, day)
    report = ctx.lift(revenue_report, revenue, day)
    return {"orders": orders.result["rows"], "total": revenue.result["total"], "report": report.id}
