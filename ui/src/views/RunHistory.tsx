import type { Haul, Lift } from "../gen/hubwagen/v1/haul_pb";
import { ago, duration, liftStatus, steps, took, when } from "../format";
import { Link } from "../router";
import { Hint } from "../words";

// RunHistory shows a pipeline's recent runs at a glance: a column per run,
// oldest on the left, with a bar for how long it took, and a row per step
// with a square for how that step went in that run. A step that fans out
// (many lifts of one name) is one square: the worst of its lifts.
export function RunHistory({ hauls, now }: { hauls: Haul[]; now: number }) {
  const runs = [...hauls].reverse(); // oldest first
  const stepNames: string[] = [];
  for (const h of runs) for (const l of h.lifts) if (!stepNames.includes(l.name)) stepNames.push(l.name);
  const times = runs.map((h) => took(h.createdAt, h.finishedAt, now) ?? 0);
  const longest = Math.max(...times, 1);
  const cols = { gridTemplateColumns: `minmax(120px, max-content) repeat(${runs.length}, 20px)` };

  return (
    <div className="history-scroll">
      <div className="history" style={cols} role="grid" aria-label="run history">
        <div className="history-axis dim small">{duration(longest)}</div>
        {runs.map((h, i) => (
          <Link key={h.id} to={{ name: "haul", id: h.id }} className="history-bar-cell" label={runLabel(h, times[i])}>
            <span className={`history-bar run-${h.status}`} style={{ height: `${Math.max((times[i] / longest) * 100, 4)}%` }} />
          </Link>
        ))}
        {stepNames.map((name) => (
          <div key={name} role="row" style={{ display: "contents" }}>
            <div className="history-step" role="rowheader">
              {name}
            </div>
            {runs.map((h) => {
              const lifts = h.lifts.filter((l) => l.name === name);
              if (lifts.length === 0) return <span key={h.id} className="history-cell none" role="gridcell" title="not in this run" />;
              const status = worst(lifts);
              return (
                <Link key={h.id} to={{ name: "haul", id: h.id, step: lifts[0].seq }} className={`history-cell cell-${status}`} label={cellLabel(h, name, lifts, status, now)}>
                  {lifts.length > 1 ? <span className="history-count">{lifts.length}</span> : null}
                </Link>
              );
            })}
          </div>
        ))}
        <div className="dim small history-axis-label">{runs.length > 0 && axisName(runs[0])}</div>
        {runs.map((h) => (
          <Link key={h.id} to={{ name: "haul", id: h.id }} className="history-label dim small" label={`run of ${inputs(h)}`}>
            {short(h)}
          </Link>
        ))}
      </div>
      <Legend />
    </div>
  );
}

function inputs(h: Haul): string {
  try {
    const p = JSON.parse(h.paramsJson || "{}") as Record<string, unknown>;
    const parts = Object.entries(p).map(([k, v]) => `${k} ${typeof v === "string" ? v : JSON.stringify(v)}`);
    return parts.length ? parts.join(", ") : "no inputs";
  } catch {
    return h.paramsJson;
  }
}

// short labels a run's column by its first input: a date as MM-DD, else its start.
function short(h: Haul): string {
  try {
    const v = Object.values(JSON.parse(h.paramsJson || "{}") as Record<string, unknown>)[0];
    if (v === undefined) return "–";
    const s = typeof v === "string" ? v : JSON.stringify(v);
    return /^\d{4}-\d{2}-\d{2}/.test(s) ? s.slice(5, 10) : s.slice(0, 8);
  } catch {
    return "?";
  }
}

// axisName says what the column labels are: the first input's name.
function axisName(h: Haul): string {
  try {
    return Object.keys(JSON.parse(h.paramsJson || "{}") as Record<string, unknown>)[0] ?? "";
  } catch {
    return "";
  }
}

function runLabel(h: Haul, t: number): string {
  return `${inputs(h)}: ${h.status}, ${h.finishedAt ? "took" : "running for"} ${duration(t)} · ${steps(h)} · started ${ago(h.createdAt)} (${when(h.createdAt)})`;
}

function cellLabel(h: Haul, name: string, lifts: Lift[], status: string, now: number): string {
  if (lifts.length === 1) {
    const t = took(lifts[0].scheduledAt, lifts[0].finishedAt, now);
    return `${name}, ${inputs(h)}: ${status}${t === undefined ? "" : `, ${duration(t)}`}`;
  }
  const counts = new Map<string, number>();
  for (const l of lifts) counts.set(liftStatus(l), (counts.get(liftStatus(l)) ?? 0) + 1);
  return `${name} ×${lifts.length}, ${inputs(h)}: ` + [...counts].map(([s, n]) => `${n} ${s}`).join(", ");
}

function worst(lifts: Lift[]): string {
  const ss = lifts.map(liftStatus);
  for (const s of ["failed", "cancelled", "retrying", "running", "waiting"]) if (ss.includes(s)) return s;
  return ss.every((s) => s === "reused") ? "reused" : "completed";
}

function Legend() {
  return (
    <div className="history-legend dim small">
      {["completed", "reused", "running", "waiting", "retrying", "failed", "cancelled"].map((s) => (
        <span key={s}>
          <span className={`history-cell cell-${s} legend`} /> {s}
          {s === "reused" && <Hint word="reused" />}
        </span>
      ))}
    </div>
  );
}
