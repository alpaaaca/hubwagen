# Spike S1: Python replay prototype

Throwaway code behind [docs/spikes/s1-python-replay.md](../../docs/spikes/s1-python-replay.md).
Not the SDK; nothing imports it.

```bash
python3 bench.py          # replay cost, whole-run cost, fan-out, call-site capture
python3 guards_check.py   # patch vs sys.monitoring guards against typical mistakes and libraries
```

- `mkproto.py`: replay until blocked, `Ctx` with `step`, `step_async`, `gather`, `sleep`
- `guards.py`: the two guards
- `user_pipelines.py`: pipeline code with typical mistakes, and legitimate library use
