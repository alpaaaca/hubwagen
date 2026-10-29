import { useState, type FormEvent } from "react";

import { useApi } from "../api";
import type { Haul } from "../gen/hubwagen/v1/haul_pb";
import { ago, duration, HAUL_STATUSES, Status, steps, took, Values, when } from "../format";
import { message, usePoll } from "../hooks";
import { usePipelines, type Pipelines } from "../pipelines";
import { Link, navigate } from "../router";

const OVERVIEW = 500;

// HaulList is the start page: a card per pipeline (hauls by name), then the
// latest runs of all of them.
export function HaulList() {
  const api = useApi();
  const pipelines = usePipelines();
  const [status, setStatus] = useState("");
  const overview = usePoll(() => api.hauls.listHauls({ limit: OVERVIEW }), []);
  const runs = usePoll(() => api.hauls.listHauls({ statuses: status ? [status] : [], limit: 50 }), [status]);

  return (
    <section>
      <h1>Pipelines</h1>
      {overview.error && <p className="error">{overview.error}</p>}
      {overview.data && <Cards hauls={overview.data.hauls} pipelines={pipelines} />}

      <div className="toolbar">
        <h2>Latest runs</h2>
        <StatusFilter value={status} onChange={setStatus} />
      </div>
      {runs.error && <p className="error">{runs.error}</p>}
      {runs.data && <RunsTable hauls={runs.data.hauls} pipelines={pipelines} />}
    </section>
  );
}

export function StatusFilter({ value, onChange }: { value: string; onChange: (s: string) => void }) {
  return (
    <select aria-label="status" value={value} onChange={(e) => onChange(e.target.value)}>
      <option value="">any status</option>
      {HAUL_STATUSES.map((s) => (
        <option key={s}>{s}</option>
      ))}
    </select>
  );
}

// Summary is how a pipeline's runs went, from the newest first.
export function summary(hs: Haul[]) {
  const failed = hs.filter((h) => h.status === "failed" || h.status === "stuck").length;
  const running = hs.filter((h) => h.status === "running").length;
  const times = hs.filter((h) => h.finishedAt).map((h) => took(h.createdAt, h.finishedAt)!);
  return { last: hs[0], failed, running, usually: times.length ? median(times) : undefined };
}

function median(xs: number[]): number {
  const s = [...xs].sort((a, b) => a - b);
  return s[Math.floor(s.length / 2)];
}

// Cards shows one card per pipeline: how its last run went, how often it
// fails, and how long it usually takes.
function Cards({ hauls, pipelines }: { hauls: Haul[]; pipelines: Pipelines }) {
  if (hauls.length === 0) {
    return <p className="dim">Nothing has run yet. Start a run with hubwagen start, or from a pipeline's page.</p>;
  }
  const byName = new Map<string, Haul[]>();
  for (const h of hauls) byName.set(h.name, [...(byName.get(h.name) ?? []), h]); // newest first
  return (
    <nav className="cards" aria-label="pipelines">
      {[...byName.entries()].map(([name, hs]) => {
        const s = summary(hs);
        const title = pipelines.title(name);
        return (
          <Link key={name} to={{ name: "pipeline", haul: name }} className="card" label={title}>
            <div className="card-title">
              <strong>{title}</strong> <Status status={s.last.status} />
            </div>
            {pipelines.info(name)?.description && <div className="card-desc">{pipelines.info(name)!.description}</div>}
            <div className="dim">
              last run {ago(s.last.createdAt)}
              {s.running > 0 && ` · ${s.running} running`}
            </div>
            <div>
              {hs.length} run{hs.length === 1 ? "" : "s"}
              {s.failed > 0 ? <span className="error"> · {s.failed} failed</span> : " · none failed"}
              {s.usually !== undefined && <> · usually {duration(s.usually)}</>}
            </div>
          </Link>
        );
      })}
    </nav>
  );
}

// RunsTable lists runs: what went in, how far they got, how long they took
// and what came out. With pipelines, it says which pipeline each run is of.
export function RunsTable({ hauls, pipelines }: { hauls: Haul[]; pipelines?: Pipelines }) {
  if (hauls.length === 0) return <p className="dim">no runs match</p>;
  const now = Date.now();
  return (
    <table className="runs">
      <thead>
        <tr>
          {pipelines && <th>pipeline</th>}
          <th>inputs</th>
          <th>status</th>
          <th>steps</th>
          <th>started</th>
          <th>took</th>
          <th>result</th>
        </tr>
      </thead>
      <tbody>
        {hauls.map((h) => {
          const t = took(h.createdAt, h.finishedAt, now);
          return (
            <tr key={h.id} className="clickable" onClick={() => navigate({ name: "haul", id: h.id })}>
              {pipelines && (
                <td>
                  <Link to={{ name: "pipeline", haul: h.name }} className="strong" onClick={(e) => e.stopPropagation()}>
                    {pipelines.title(h.name)}
                  </Link>
                  {h.parentId && <div className="dim small">part of another run</div>}
                </td>
              )}
              <td>
                <Values json={h.paramsJson} empty="none" />
              </td>
              <td>
                <Status status={h.status} />
              </td>
              <td className="dim">{steps(h)}</td>
              <td title={when(h.createdAt)}>{ago(h.createdAt, now)}</td>
              <td>{t === undefined ? "–" : duration(t) + (h.finishedAt ? "" : " so far")}</td>
              <td>
                {h.status === "failed" || h.status === "stuck" ? (
                  <span className="error">{h.message || h.reason}</span>
                ) : (
                  <Values json={h.resultJson} />
                )}
              </td>
            </tr>
          );
        })}
      </tbody>
    </table>
  );
}

// StartRun starts a run of one pipeline with the inputs given as JSON.
export function StartRun({ haul, inputs }: { haul: string; inputs: string }) {
  const api = useApi();
  const [params, setParams] = useState<string>();
  const [error, setError] = useState("");
  const value = params ?? inputs;

  async function submit(e: FormEvent) {
    e.preventDefault();
    setError("");
    try {
      JSON.parse(value || "{}");
    } catch {
      setError("inputs must be JSON, e.g. {\"day\": \"2026-10-24\"}");
      return;
    }
    try {
      const res = await api.hauls.startHaul({ tenant: "default", name: haul, paramsJson: value || "{}", trigger: "the web UI" });
      navigate({ name: "haul", id: res.haulId });
    } catch (err) {
      setError(message(err));
    }
  }

  return (
    <form className="inline-form" onSubmit={submit}>
      <input aria-label="inputs" className="mono wide" value={value} onChange={(e) => setParams(e.target.value)} />
      <button type="submit">Start a run</button>
      {error && <span className="error">{error}</span>}
    </form>
  );
}
