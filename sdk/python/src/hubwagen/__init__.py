"""hubwagen: durable hauls and memoized lifts.

    import hubwagen as hw

    @hw.lift(memo=True, output=hw.Dataset("raw.orders", partition="day"))
    def extract(day: str) -> int:
        out = hw.output()
        (out.path / "orders.csv").write_text(...)
        return 42

    @hw.haul
    def daily(ctx, day: str):
        orders = ctx.lift(extract, day)   # a Pallet
        ...

Run a worker with `python -m hubwagen.worker app.hauls`.
"""

from ._codecs import register_codec
from ._guards import NondeterminismError
from ._model import Dataset, Interval, Keep, LiftFailed, NonRetryable, Pallet, Retry, Schedule, Subscription, haul, lift
from ._output import Output, output
from ._replay import Blocked, ChildFailed, Ctx, Handle, SignalTimeout
from .client import Client, ConnectError

__all__ = [
    "Blocked", "ChildFailed", "Client", "ConnectError", "Ctx", "Dataset", "Handle", "Interval", "Keep", "LiftFailed",
    "NondeterminismError", "NonRetryable", "Output", "Pallet", "Retry", "Schedule", "SignalTimeout", "Subscription", "haul", "lift",
    "output", "register_codec",
]
