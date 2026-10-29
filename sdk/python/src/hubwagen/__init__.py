"""hubwagen: pipelines as plain code, steps that are reused when nothing changed.

    import hubwagen as hw

    @hw.step(memo=True, output=hw.Dataset("raw.orders", partition="day"))
    def extract(day: str) -> int:
        out = hw.output()
        (out.path / "orders.csv").write_text(...)
        return 42

    @hw.pipeline
    def daily(ctx, day: str):
        orders = ctx.step(extract, day)   # a DataVersion of raw.orders
        ...

Run a worker with `python -m hubwagen.worker app.pipelines`.

The engine's own names still work: hw.haul (pipeline), hw.lift (step),
hw.Pallet (DataVersion), hw.LiftFailed (StepFailed), hw.ChildFailed
(RunFailed), ctx.lift, ctx.lift_async, ctx.haul, ctx.start_haul and
ctx.latest_pallet.
"""

from ._codecs import register_codec
from ._guards import NondeterminismError
from ._model import (DataVersion, Dataset, Interval, Keep, LiftFailed, NonRetryable, Pallet, Retry, Schedule,
                     StepFailed, Subscription, haul, lift, pipeline, step)
from ._output import Output, output
from ._replay import Blocked, ChildFailed, Ctx, Handle, RunFailed, SignalTimeout
from .client import Client, ConnectError

__all__ = [
    "Blocked", "Client", "ConnectError", "Ctx", "DataVersion", "Dataset", "Handle", "Interval", "Keep",
    "NondeterminismError", "NonRetryable", "Output", "Retry", "RunFailed", "Schedule", "SignalTimeout", "StepFailed",
    "Subscription", "output", "pipeline", "register_codec", "step",
    # the engine's names
    "ChildFailed", "LiftFailed", "Pallet", "haul", "lift",
]
