import { useState } from "react";

import { useApi } from "../api";
import { ago, duration, fromNow, Status, when } from "../format";
import { usePoll } from "../hooks";
import { usePipelines } from "../pipelines";
import { Link } from "../router";
import { Dag } from "./Dag";
import { RunHistory } from "./RunHistory";
import { RunsTable, StartRun, StatusFilter, summary } from "./HaulList";

const HISTORY = 30;

// Pipeline: one pipeline (a haul by name): what it is and how it is
// started, its recent runs by step (RunHistory), its steps as a graph (from
// its newest run: haul code decides its steps as it runs), and all its runs.
export function Pipeline({ haul }: { haul: string }) {
  const api = useApi();
  const pipelines = usePipelines();
  const [status, setStatus] = useState("");
  const all = usePoll(() => api.hauls.listHauls({ name: haul, limit: 500 }), [haul]);
  const history = usePoll(() => api.hauls.listHauls({ name: haul, limit: HISTORY, withLifts: true }), [haul]);
  const runs = usePoll(() => api.hauls.listHauls({ name: haul, statuses: status ? [status] : [], limit: 100 }), [haul, status]);
  const latest = all.data?.hauls[0];
  const graph = usePoll(() => (latest ? api.hauls.getHaul({ haulId: latest.id }) : Promise.resolve(undefined)), [latest?.id]);
  const info = pipelines.info(haul);
  const s = all.data && all.data.hauls.length ? summary(all.data.hauls) : undefined;
  const schedule = pipelines.schedule(haul);
  const sub = pipelines.subscription(haul);
  const lifts = graph.data?.haul?.lifts ?? [];
  const now = Date.now();

  return (
    <section>
      <p>
        <Link to={{ name: "hauls" }}>← all pipelines</Link>
      </p>
      <div className="toolbar">
        <h1>{pipelines.title(haul)}</h1>
        {s && <Status status={s.last.status} />}
      </div>
      {info?.description && <p className="desc">{info.description}</p>}
      <dl className="facts meta">
        <dt>in code</dt>
        <dd className="mono">{haul}</dd>
        <dt>runs</dt>
        <dd>
          {schedule ? (
            <>
              on a schedule: <span className="mono">{schedule.cron}</span>
              {schedule.timezone && ` (${schedule.timezone})`}, {schedule.interval} data, catch-up {schedule.catchUp || "latest"}
            </>
          ) : sub ? (
            <>
              when new data arrives in <span className="mono">{sub.datasets.join(", ")}</span>
              {sub.waiting > 0 && ` (${sub.waiting} waiting)`}
            </>
          ) : (
            "when started (from here, hubwagen start, or another pipeline)"
          )}
        </dd>
        {schedule && (
          <>
            <dt>next run</dt>
            <dd>
              {fromNow(schedule.nextRun, now)} <span className="dim">({when(schedule.nextRun)})</span>
            </dd>
          </>
        )}
        {s && (
          <>
            <dt>history</dt>
            <dd>
              {all.data!.hauls.length} runs, last {ago(s.last.createdAt, now)}
              {s.usually !== undefined && `, usually ${duration(s.usually)}`}
              {s.failed > 0 && <span className="error">, {s.failed} failed</span>}
            </dd>
          </>
        )}
      </dl>

      <h2>Recent runs</h2>
      {history.data && history.data.hauls.length > 0 ? (
        <RunHistory hauls={history.data.hauls} now={now} />
      ) : (
        <p className="dim">{history.data ? "no runs yet" : "loading…"}</p>
      )}

      <h2>Steps</h2>
      {lifts.length > 0 ? (
        <>
          <Dag lifts={lifts} now={now} plain />
          <p className="dim small">as in the latest run</p>
        </>
      ) : (
        <p className="dim">{all.data ? "no run has steps yet" : "loading…"}</p>
      )}

      <h2>Start a run</h2>
      {/* A scheduled pipeline takes its data interval as an input: start with the latest one. */}
      <StartRun haul={haul} inputs={schedule?.latestParamsJson || latest?.paramsJson || "{}"} />
      {schedule?.latestParamsJson && <p className="dim small">inputs of the latest interval that came due</p>}

      <details className="raw">
        <summary>All runs, as a list</summary>
        <div className="toolbar">
          <StatusFilter value={status} onChange={setStatus} />
        </div>
        {runs.error && <p className="error">{runs.error}</p>}
        {runs.data && <RunsTable hauls={runs.data.hauls} />}
      </details>
    </section>
  );
}
