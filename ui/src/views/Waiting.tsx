import type { Waiting as WaitingMsg } from "../gen/hubwagen/v1/haul_pb";
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

// Workers lists the workers the server saw lately, for a run that waits.
export function Workers() {
  const api = useApi();
  const workers = usePoll(() => api.hauls.listWorkers({}), []);
  const ws = workers.data?.workers;
  if (!ws) return null;
  if (ws.length === 0) return <p className="dim small">No workers connected.</p>;
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
            <td>{w.polling ? "waiting for work" : "busy"}</td>
          </tr>
        ))}
      </tbody>
    </table>
  );
}
