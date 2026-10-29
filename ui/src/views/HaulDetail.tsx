import { useState, type FormEvent } from "react";

import { useApi } from "../api";
import type { Haul, Lift } from "../gen/hubwagen/v1/haul_pb";
import { ago, duration, liftStatus, ms, pretty, shortId, Status, steps, took, trigger, Values, when } from "../format";
import { message, usePoll } from "../hooks";
import { usePipelines } from "../pipelines";
import { Link } from "../router";
import { Hint } from "../words";
import { Dag } from "./Dag";
import { History } from "./History";
import { Logs } from "./Logs";

const OPEN = ["running", "stuck"];

// HaulDetail: one run of a pipeline: what went in, how long each step took
// and why it ran, and what came out; what happened, in order; refreshed
// while it is open, with cancel, resume and signal.
export function HaulDetail({ id, step }: { id: string; step?: number }) {
  const api = useApi();
  const pipelines = usePipelines();
  const haul = usePoll(() => api.hauls.getHaul({ haulId: id }), [id]);
  const h = haul.data?.haul;
  const open = h ? OPEN.includes(h.status) : false;
  const children = usePoll(() => api.hauls.listHauls({ parentId: id, limit: 100 }), [id]);
  const [actionError, setActionError] = useState("");
  const [picked, setPicked] = useState<number | undefined>(step); // the step whose logs show; unset: a failed step, else all

  async function act(f: () => Promise<unknown>) {
    setActionError("");
    try {
      await f();
      haul.reload();
    } catch (e) {
      setActionError(message(e));
    }
  }

  if (haul.error && !h) return <p className="error">{haul.error}</p>;
  if (!h) return <p className="dim">loading…</p>;
  const now = Date.now();
  const t = took(h.createdAt, h.finishedAt, now);
  const selected = picked ?? h.lifts.find((l) => l.status === "failed")?.seq ?? 0;
  const pick = (seq: number) => {
    setPicked(seq);
    document.getElementById("logs")?.scrollIntoView?.({ behavior: "smooth", block: "start" });
  };

  return (
    <section>
      <p>
        <Link to={{ name: "pipeline", haul: h.name }}>← {pipelines.title(h.name)}</Link>
      </p>
      <div className="toolbar">
        <h1>{pipelines.title(h.name)}</h1>
        <Values json={h.paramsJson} />
        <Status status={h.status} />
        {open && <button onClick={() => act(() => api.hauls.cancelHaul({ haulId: h.id, by: "the web UI" }))}>Cancel</button>}
        {h.status === "stuck" && <button onClick={() => act(() => api.hauls.resumeHaul({ haulId: h.id }))}>Resume</button>}
      </div>
      <p className="dim">
        started <span title={when(h.createdAt)}>{ago(h.createdAt, now)}</span>
        {h.trigger && <> {trigger(h.trigger)}</>}
        {t !== undefined && <> · {h.finishedAt ? "took" : "running for"} {duration(t)}</>} · {steps(h)}
      </p>
      {actionError && <p className="error">{actionError}</p>}
      {(h.reason || h.message) && (
        <p className={h.status === "completed" ? "" : "error"}>
          {h.reason}
          {h.reason && h.message ? ": " : ""}
          {h.message}
        </p>
      )}
      {(h.parentId || h.continuedAs) && (
        <p>
          {h.parentId && (
            <>
              part of <Link to={{ name: "haul", id: h.parentId }}>another run</Link>.{" "}
            </>
          )}
          {h.continuedAs && (
            <>
              continued in <Link to={{ name: "haul", id: h.continuedAs }}>a new run</Link>.
            </>
          )}
        </p>
      )}

      {h.resultJson && (
        <>
          <h2>Result</h2>
          <Result json={h.resultJson} />
        </>
      )}
      {open && <SignalForm onSend={(name, payload) => act(() => api.hauls.signalHaul({ haulId: h.id, name, payloadJson: payload }))} />}

      <h2>
        Steps <Hint word="step" />
      </h2>
      {h.lifts.length === 0 ? (
        <p className="dim">none yet</p>
      ) : (
        <>
          <Dag lifts={h.lifts} now={now} selected={selected} onSelect={pick} />
          <Steps haul={h} now={now} selected={selected} onSelect={pick} />
          <div id="logs">
            <Logs haulId={h.id} lifts={h.lifts} seq={selected} onSelect={setPicked} open={open} />
          </div>
        </>
      )}

      {children.data && children.data.hauls.length > 0 && (
        <>
          <h2>Runs it started</h2>
          <ul>
            {children.data.hauls.map((c) => (
              <li key={c.id}>
                <Link to={{ name: "haul", id: c.id }}>{pipelines.title(c.name)}</Link> <Values json={c.paramsJson} /> <Status status={c.status} />
              </li>
            ))}
          </ul>
        </>
      )}

      <History haulId={h.id} open={open} onSelect={pick} />
      <p className="dim mono small">run {h.id}</p>
    </section>
  );
}

// Result shows a haul's result: a flat object as a list of names and values,
// anything else as indented JSON.
function Result({ json }: { json: string }) {
  let v: unknown;
  try {
    v = JSON.parse(json);
  } catch {
    return <pre>{json}</pre>;
  }
  if (v && typeof v === "object" && !Array.isArray(v) && Object.values(v).every((x) => x === null || typeof x !== "object")) {
    return (
      <dl className="facts result">
        {Object.entries(v as Record<string, unknown>).map(([k, x]) => (
          <div key={k} style={{ display: "contents" }}>
            <dt>{k}</dt>
            <dd>{typeof x === "string" ? x : JSON.stringify(x)}</dd>
          </div>
        ))}
      </dl>
    );
  }
  return <pre>{pretty(json)}</pre>;
}

// Steps lays the lifts out on the haul's timeline: when each started and how
// long it took, whether it ran or was reused, and what it returned.
function Steps({ haul, now, selected, onSelect }: { haul: Haul; now: number; selected: number; onSelect: (seq: number) => void }) {
  const start = ms(haul.createdAt)!;
  const end = Math.max(ms(haul.finishedAt) ?? now, ...haul.lifts.map((l) => ms(l.finishedAt) ?? now), start + 1);
  const span = end - start;
  return (
    <table className="steps">
      <thead>
        <tr>
          <th>step</th>
          <th>
            status <Hint word="reused" />
          </th>
          <th className="timeline-head">
            timeline <span className="dim">({duration(span)})</span>
          </th>
          <th>took</th>
          <th>why</th>
          <th>
            output <Hint word="version" />
          </th>
        </tr>
      </thead>
      <tbody>
        {haul.lifts.map((l) => (
          <Step key={l.seq} lift={l} start={start} span={span} now={now} selected={l.seq === selected} onSelect={() => onSelect(l.seq)} />
        ))}
      </tbody>
    </table>
  );
}

function Step({ lift: l, start, span, now, selected, onSelect }: { lift: Lift; start: number; span: number; now: number; selected: boolean; onSelect: () => void }) {
  const from = ms(l.scheduledAt) ?? start;
  const to = ms(l.finishedAt) ?? now;
  const left = ((from - start) / span) * 100;
  const width = Math.max(((to - from) / span) * 100, 0.6);
  const t = took(l.scheduledAt, l.finishedAt, now);
  return (
    <tr className={`clickable${selected ? " selected" : ""}`} onClick={onSelect} title="show its logs">
      <td>
        <strong>{l.name}</strong>
      </td>
      <td>
        <Status status={liftStatus(l)} />
        {l.reason && (
          <div className="error small">
            {l.reason}
            {l.message ? `: ${l.message}` : ""}
          </div>
        )}
      </td>
      <td className="timeline">
        <div className="track">
          <div className={`bar bar-${liftStatus(l)}`} style={{ left: `${left}%`, width: `${Math.min(width, 100 - left)}%` }} />
        </div>
      </td>
      <td>{t === undefined ? "–" : duration(t)}</td>
      <td className="dim">
        {l.why}
        {l.attempts > 1 && <div>{l.attempts} attempts</div>}
      </td>
      <td>
        <Values json={l.resultJson} />
        {l.pallet && (
          <div className="dim small" title={`version ${l.pallet}`}>
            {l.reused ? "reused a version of" : "wrote a new version of"} <span className="mono">{l.dataset || shortId(l.pallet)}</span>
          </div>
        )}
      </td>
    </tr>
  );
}

function SignalForm({ onSend }: { onSend: (name: string, payload: string) => Promise<void> }) {
  const [name, setName] = useState("");
  const [payload, setPayload] = useState("");
  const [error, setError] = useState("");

  async function submit(e: FormEvent) {
    e.preventDefault();
    setError("");
    if (payload) {
      try {
        JSON.parse(payload);
      } catch {
        setError("the payload must be JSON");
        return;
      }
    }
    await onSend(name.trim(), payload);
    setName("");
    setPayload("");
  }

  return (
    <form className="inline-form" onSubmit={submit}>
      <input aria-label="signal name" placeholder="signal, e.g. approve" value={name} onChange={(e) => setName(e.target.value)} required />
      <input aria-label="signal payload" className="mono wide" placeholder='payload JSON, e.g. {"by": "ana"}' value={payload} onChange={(e) => setPayload(e.target.value)} />
      <button type="submit">Send signal</button>
      {error && <span className="error">{error}</span>}
    </form>
  );
}

