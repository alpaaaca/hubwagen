import type { Waiting as WaitingMsg, Worker } from "../gen/hubwagen/v1/haul_pb";
import { ago, when } from "../format";
import { useApi } from "../api";
import { usePoll } from "../hooks";

// Waiting says why a running run's work waits (no worker connected, none
// with its code, all busy), as the server tells it: `code` in its message
// shows as code.
export function Waiting({ waiting, short }: { waiting: WaitingMsg; short?: boolean }) {
  const text = short ? waiting.message.split(". ")[0] : waiting.message;
  return (
    <span className="waiting" title={short ? waiting.message : undefined}>
      Waiting: <Code text={text} />
      {!short && (
        <span className="dim">
          {" "}
          (since <span title={when(waiting.since)}>{ago(waiting.since)}</span>)
        </span>
      )}
    </span>
  );
}

function Code({ text }: { text: string }) {
  return (
    <>
      {text.split("`").map((part, i) => (i % 2 === 1 ? <code key={i}>{part}</code> : part))}
    </>
  );
}

// Workers lists the worker pools the server scales and the workers it saw
// lately, for a run that waits.
export function Workers() {
  const api = useApi();
  const workers = usePoll(() => api.hauls.listWorkers({}), []);
  const pools = usePoll(() => api.hauls.listPools({}), []);
  const ws = workers.data?.workers;
  if (!ws) return null;
  return (
    <>
      {(pools.data?.pools ?? []).map((p) => (
        <p key={p.name} className="small">
          Pool <span className="mono">{p.name}</span> ({p.docks.join(", ")}
          {p.external && ", started elsewhere"}): {p.workers.length} of {p.min}–{p.max} workers
          {p.external && p.desired !== p.workers.length && <>, wants {p.desired}</>}
          {p.starting > 0 && <>, {p.starting} starting</>}
          {p.decision && (
            <span className="dim">
              {" "}
              · {p.decision} <span title={when(p.decidedAt)}>{ago(p.decidedAt)}</span>
            </span>
          )}
        </p>
      ))}
      {ws.length === 0 ? <p className="dim small">No workers connected.</p> : <WorkerTable workers={ws} />}
    </>
  );
}

function WorkerTable({ workers: ws }: { workers: Worker[] }) {
  return (
    <table className="small">
      <thead>
        <tr>
          <th>worker</th>
          <th>docks</th>
          <th>build</th>
          <th>last seen</th>
          <th>now</th>
        </tr>
      </thead>
      <tbody>
        {ws.map((w) => (
          <tr key={w.id}>
            <td className="mono">{w.id}</td>
            <td>{w.docks.join(", ")}</td>
            <td className="mono">{w.buildId ? w.buildId.slice(0, 12) : "–"}</td>
            <td title={when(w.lastSeen)}>{ago(w.lastSeen)}</td>
            <td>{w.running > 0 ? `running ${w.running}` : w.polling ? "waiting for work" : "busy"}</td>
          </tr>
        ))}
      </tbody>
    </table>
  );
}
