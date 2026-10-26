# Spike S1: Python replay prototype

Throwaway code behind [docs/spikes/s1-python-replay.md](../../docs/spikes/s1-python-replay.md).
Not the SDK; nothing imports it.

```bash
python3 bench.py          # replay cost, whole-haul cost, fan-out, call-site capture
python3 guards_check.py   # patch vs sys.monitoring guards against typical mistakes and libraries
```

- `hwproto.py`: replay until blocked, `Ctx` with `lift`, `lift_async`, `gather`, `sleep`
- `guards.py`: the two guards
- `user_hauls.py`: haul code with typical mistakes, and legitimate library use
