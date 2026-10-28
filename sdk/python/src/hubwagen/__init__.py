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
from ._model import Dataset, LiftFailed, NonRetryable, Pallet, Retry, haul, lift
from ._output import Output, output
from ._replay import Blocked, Ctx, Handle
from .client import Client, ConnectError

__all__ = [
    "Blocked", "Client", "ConnectError", "Ctx", "Dataset", "Handle", "LiftFailed", "NondeterminismError", "NonRetryable",
    "Output", "Pallet", "Retry", "haul", "lift", "output", "register_codec",
]
