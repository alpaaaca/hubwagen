import { timestampDate } from "@bufbuild/protobuf/wkt";
import { useState } from "react";

import { useApi } from "../api";
import { POLL_MS, usePoll } from "../hooks";
import type { HistoryEntry } from "../gen/hubwagen/v1/haul_pb";
import { Link } from "../router";
import { Hint } from "../words";

// History tells what happened in a run, in sentences the server writes
// (internal/history). The engine's bookkeeping is left out unless asked
// for, with the raw events.
export function History({ haulId, open, onSelect }: { haulId: string; open: boolean; onSelect: (seq: number) => void }) {
  const api = useApi();
  const res = usePoll(() => api.hauls.getHistory({ haulId }), [haulId, open], open ? POLL_MS : 0);
  const [engine, setEngine] = useState(false);
  const entries = (res.data?.entries ?? []).filter((e) => engine || e.kind !== "engine");

  return (
    <div className="story">
      <div className="toolbar">
        <h2>
          What happened <Hint word="history" />
        </h2>
        <label className="dim small">
          <input type="checkbox" checked={engine} onChange={(e) => setEngine(e.target.checked)} /> engine details
        </label>
      </div>
      {res.error && <p className="error">{res.error}</p>}
      <ol className="history-lines" aria-label="what happened">
        {entries.map((e) => (
          <Line key={String(e.eventSeq)} e={e} engine={engine} onSelect={onSelect} />
        ))}
      </ol>
    </div>
  );
}

function Line({ e, engine, onSelect }: { e: HistoryEntry; engine: boolean; onSelect: (seq: number) => void }) {
  return (
    <li className={`history-line kind-${e.kind}`}>
      <span className="dim mono">{clock(e)}</span>
      <span>
        {e.step && (
          <button type="button" className="step-name" onClick={() => onSelect(e.seq)} title="show its logs">
            {e.step}
          </button>
        )}{" "}
        {e.text}
        {e.haulId && (
          <>
            {" "}
            <Link to={{ name: "haul", id: e.haulId }} label="open that run">
              ↗
            </Link>
          </>
        )}
        {e.detail && <div className={e.kind === "problem" || e.kind === "finished" || e.kind === "stuck" ? "error small" : "dim small"}>{e.detail}</div>}
        {engine && (
          <div className="dim small mono raw-event">
            {e.eventType} {e.payloadJson}
          </div>
        )}
      </span>
    </li>
  );
}

// clock is the time of day, to the tenth of a second: a run's events are close together.
function clock(e: HistoryEntry): string {
  if (!e.time) return "";
  const d = timestampDate(e.time);
  const pad = (n: number) => String(n).padStart(2, "0");
  return `${pad(d.getHours())}:${pad(d.getMinutes())}:${pad(d.getSeconds())}.${Math.floor(d.getMilliseconds() / 100)}`;
}
