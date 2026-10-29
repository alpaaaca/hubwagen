import { useState, type FormEvent } from "react";

import { useApi } from "../api";
import type { Haul } from "../gen/hubwagen/v1/haul_pb";
import { ago, duration, fromNow, HAUL_STATUSES, Status, steps, took, Values, when } from "../format";
import { message, usePoll } from "../hooks";
import { usePipelines, type Pipelines } from "../pipelines";
import { Link, navigate } from "../router";

const OVERVIEW = 500;

// HaulList is the start page: the pipelines (hauls by name) and what to
// know about each: what it is, how it is started, how its recent runs went.
// Their runs are on each pipeline's page.
export function HaulList() {
  const api = useApi();
  const pipelines = usePipelines();
  const overview = usePoll(() => api.hauls.listHauls({ limit: OVERVIEW }), []);
  const byName = new Map<string, Haul[]>();
  for (const name of pipelines.names) byName.set(name, []);
  for (const h of overview.data?.hauls ?? []) byName.set(h.name, [...(byName.get(h.name) ?? []), h]); // newest first
  const names = [...byName.keys()].sort((a, b) => pipelines.title(a).localeCompare(pipelines.title(b)));

  return (
    <section>
      <h1>Pipelines</h1>
      {overview.error && <p className="error">{overview.error}</p>}
      {overview.data && names.length === 0 && (
        <p className="dim">No pipelines yet: start workers with your code (hubwagen dev does, from hubwagen.yaml).</p>
      )}
      {names.length > 0 && (
        <table className="pipelines">
          <thead>
            <tr>
              <th>pipeline</th>
              <th>schedule</th>
              <th>next run</th>
              <th>recent runs</th>
              <th>last run</th>
              <th>succeeded</th>
              <th>usually takes</th>
            </tr>
          </thead>
          <tbody>
            {names.map((name) => (
              <PipelineRow key={name} name={name} hauls={byName.get(name)!} pipelines={pipelines} />
            ))}
          </tbody>
        </table>
      )}
      {overview.data && overview.data.hauls.length === OVERVIEW && (
        <p className="dim small">counted over the newest {OVERVIEW} runs</p>
      )}
    </section>
  );
}

const RECENT = 12;

function PipelineRow({ name, hauls, pipelines }: { name: string; hauls: Haul[]; pipelines: Pipelines }) {
  const info = pipelines.info(name);
  const schedule = pipelines.schedule(name);
  const sub = pipelines.subscription(name);
  const s = hauls.length ? summary(hauls) : undefined;
  const finished = hauls.filter((h) => h.finishedAt);
  const ok = finished.filter((h) => h.status === "completed" || h.status === "continued").length;
  return (
    <tr className="clickable" onClick={() => navigate({ name: "pipeline", haul: name })}>
      <td>
        <Link to={{ name: "pipeline", haul: name }} className="strong" onClick={(e) => e.stopPropagation()}>
          {pipelines.title(name)}
        </Link>
        {info?.title && <span className="mono dim small"> {name}</span>}
        {info?.description && <div className="small pipeline-desc">{info.description}</div>}
      </td>
      <td>
        {schedule ? (
          <>
            <span className="mono">{schedule.cron}</span>
            {schedule.timezone && <span className="dim small"> {schedule.timezone}</span>}
            <div className="dim small">{schedule.interval} data</div>
          </>
        ) : sub ? (
          <>
            when new data arrives
            <div className="dim small mono">{sub.datasets.join(", ")}</div>
          </>
        ) : (
          <span className="dim">on demand</span>
        )}
      </td>
      <td title={schedule ? when(schedule.nextRun) : undefined}>{schedule ? fromNow(schedule.nextRun) : <span className="dim">–</span>}</td>
      <td>
        <span className="recent" aria-label={`recent runs of ${pipelines.title(name)}`}>
          {[...hauls.slice(0, RECENT)].reverse().map((h) => (
            <Link key={h.id} to={{ name: "haul", id: h.id }} className={`recent-run run-${h.status}`} label={`${h.status}, started ${ago(h.createdAt)}`} onClick={(e) => e.stopPropagation()}>
              {""}
            </Link>
          ))}
          {hauls.length === 0 && <span className="dim">never ran</span>}
        </span>
      </td>
      <td>
        {s && (
          <>
            <Status status={s.last.status} /> <span title={when(s.last.createdAt)}>{ago(s.last.createdAt)}</span>
          </>
        )}
      </td>
      <td>
        {finished.length > 0 ? (
          <span className={ok < finished.length ? "error" : ""}>
            {ok} of {finished.length}
          </span>
        ) : (
          <span className="dim">–</span>
        )}
      </td>
      <td>{s?.usually !== undefined ? duration(s.usually) : <span className="dim">–</span>}</td>
    </tr>
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
