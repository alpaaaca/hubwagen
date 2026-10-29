import { timestampDate } from "@bufbuild/protobuf/wkt";
import { useEffect, useRef } from "react";

import { useApi } from "../api";
import type { Lift } from "../gen/hubwagen/v1/haul_pb";
import type { RunLogLine } from "../gen/hubwagen/v1/log_pb";
import { POLL_MS, usePoll } from "../hooks";

// Logs shows what a run's lifts printed and logged: all of them, lines
// marked with their step, or one step's. It follows along while the run is
// open.
export function Logs({
  haulId,
  lifts,
  seq,
  onSelect,
  open,
}: {
  haulId: string;
  lifts: Lift[];
  seq: number; // 0: all steps
  onSelect: (seq: number) => void;
  open: boolean;
}) {
  const api = useApi();
  const logs = usePoll(() => api.logs.getLogs({ haulId, seq }), [haulId, seq, open], open ? POLL_MS : 0);
  const box = useRef<HTMLDivElement>(null);
  const lines = logs.data?.lines ?? [];
  const atEnd = useRef(true);

  // Stay at the end while following, unless the reader scrolled up.
  useEffect(() => {
    const el = box.current;
    if (el && atEnd.current) el.scrollTop = el.scrollHeight;
  }, [lines.length]);

  const many = (name: string) => lifts.filter((l) => l.name === name).length > 1;

  return (
    <div className="logs">
      <div className="toolbar">
        <h2>Logs</h2>
        <select aria-label="logs of" value={seq} onChange={(e) => onSelect(Number(e.target.value))}>
          <option value={0}>all steps</option>
          {lifts.map((l) => (
            <option key={l.seq} value={l.seq}>
              {many(l.name) ? `${l.name} #${l.seq}` : l.name}
            </option>
          ))}
        </select>
        {open && <span className="dim small">following</span>}
      </div>
      {logs.error && <p className="error">{logs.error}</p>}
      <div
        className="log-lines"
        ref={box}
        role="log"
        aria-label="log lines"
        onScroll={(e) => {
          const el = e.currentTarget;
          atEnd.current = el.scrollHeight - el.scrollTop - el.clientHeight < 24;
        }}
      >
        {logs.data && logs.data.skipped > 0 && <div className="dim">… {logs.data.skipped} earlier lines not shown</div>}
        {logs.data && lines.length === 0 && (
          <div className="dim">{seq ? "this step printed nothing" : "the steps printed nothing"}{open ? " yet" : ""}</div>
        )}
        {lines.map((l, i) => (
          <LogRow key={i} line={l} step={seq ? "" : many(l.lift) ? `${l.lift} #${l.seq}` : l.lift} />
        ))}
      </div>
    </div>
  );
}

function clock(l: RunLogLine): string {
  if (!l.line?.time) return "";
  const d = timestampDate(l.line.time);
  const pad = (n: number, w = 2) => String(n).padStart(w, "0");
  return `${pad(d.getHours())}:${pad(d.getMinutes())}:${pad(d.getSeconds())}.${pad(d.getMilliseconds(), 3)}`;
}

function LogRow({ line: l, step }: { line: RunLogLine; step: string }) {
  const level = l.line?.stream === "log" && l.line.level !== "INFO" ? l.line.level : ""; // like hubwagen dev: only what stands out
  const kind = l.line?.stream === "stderr" || level === "ERROR" || level === "CRITICAL" ? "bad" : level === "WARNING" ? "warn" : "";
  return (
    <div className={`log-line ${kind}`}>
      <span className="dim">{clock(l)}</span>
      {step && <span className="log-step">{step}</span>}
      {level && <span className="log-level">{level}</span>}
      <span className="log-text">{l.line?.text}</span>
      {l.attempt > 1 && <span className="dim small"> (attempt {l.attempt})</span>}
    </div>
  );
}
