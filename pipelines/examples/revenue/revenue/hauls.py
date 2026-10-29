"""Daily revenue: extract → transform → report, one run per day.

    hubwagen dev
    python -m hubwagen.worker revenue.hauls        # from pipelines/examples/revenue
    hubwagen replay daily --param day=2026-10-01..2026-10-07

extract_orders reads a source that can change, so it is not memoized: it
runs every time. daily_revenue and revenue_report are memoized: when the
orders for a day come out identical (same content hash), they are reused,
and when their own code changes, they run again.

Each step prints and logs what it does; `hubwagen dev` and the web UI show
it with the step (print() and logging both work).
"""

import csv
import logging
from decimal import Decimal
from pathlib import Path

import hubwagen as hw

SOURCE = Path(__file__).resolve().parent.parent / "data" / "orders.csv"
log = logging.getLogger(__name__)


@hw.step(output=hw.Dataset("raw.orders", partition="day"))
def extract_orders(day: str) -> dict:
    """The day's orders from the source system (here: a CSV file)."""
    log.info("reading %s", SOURCE.name)
    with SOURCE.open() as f:
        rows = [r for r in csv.DictReader(f) if r["day"] == day]
    if not rows:
        log.warning("no orders for %s in %s: the report will be empty", day, SOURCE.name)
    else:
        by_region: dict[str, int] = {}
        for r in rows:
            by_region[r["region"]] = by_region.get(r["region"], 0) + 1
        print(f"{len(rows)} orders for {day}: " + ", ".join(f"{n} {region}" for region, n in sorted(by_region.items())))
    out = hw.output()
    with (out.path / "orders.csv").open("w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["order_id", "region", "amount"])
        w.writerows([r["order_id"], r["region"], r["amount"]] for r in rows)
    out.stats["rows"] = len(rows)
    return {"rows": len(rows)}


@hw.step(memo=True, output=hw.Dataset("mart.daily_revenue", partition="day"))
def daily_revenue(orders: hw.DataVersion, day: str) -> dict:
    """Revenue per region."""
    totals: dict[str, Decimal] = {}
    with orders.path("orders.csv").open() as f:
        for r in csv.DictReader(f):
            totals[r["region"]] = totals.get(r["region"], Decimal(0)) + Decimal(r["amount"])
    totals = {region: amount.quantize(Decimal("0.01")) for region, amount in totals.items()}
    grand = sum(totals.values(), Decimal(0))
    for region, amount in sorted(totals.items(), key=lambda kv: -kv[1]):
        share = amount / grand * 100 if grand else Decimal(0)
        print(f"{region:<5} {amount:>9}  {share:4.1f}%")
    if totals:
        top = max(totals, key=lambda region: totals[region])
        log.info("%s brings the most revenue on %s", top, day)
    out = hw.output()
    with (out.path / "revenue.csv").open("w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["region", "revenue"])
        w.writerows(sorted(totals.items()))
    total = sum(totals.values(), Decimal(0))
    out.meta["total"] = str(total)
    return {"regions": len(totals), "total": str(total)}


@hw.step(memo=True, output=hw.Dataset("reports.revenue", partition="day"))
def revenue_report(revenue: hw.DataVersion, day: str) -> str:
    """A short text report."""
    with revenue.path("revenue.csv").open() as f:
        rows = list(csv.DictReader(f))
    lines = [f"Revenue on {day}"] + [f"  {r['region']:<5} {r['revenue']:>10}" for r in rows]
    text = "\n".join(lines) + "\n"
    (hw.output().path / "report.txt").write_text(text)
    print(text, end="")
    log.info("wrote report.txt (%d bytes)", len(text.encode()))
    return lines[0]


@hw.pipeline(title="Daily revenue")
def daily(ctx, day: str):
    """Orders for one day, revenue per region from them, and a short report."""
    orders = ctx.step(extract_orders, day)
    revenue = ctx.step(daily_revenue, orders, day)
    report = ctx.step(revenue_report, revenue, day)
    return {"orders": orders.result["rows"], "total": revenue.result["total"], "report": report.id}
