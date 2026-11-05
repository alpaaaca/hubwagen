import { useState } from "react";

import { useApi } from "../api";
import { ago, duration, fromNow, Status, when } from "../format";
import { usePoll } from "../hooks";
import { usePipelines } from "../pipelines";
import { Link } from "../router";
import { Hint } from "../words";
import { Dag } from "./Dag";
import { RunHistory } from "./RunHistory";
import { RunsTable, StartRun, StatusFilter, summary } from "./RunList";

const HISTORY = 30;

// Pipeline: one pipeline (a run by name): what it is and how it is
// started, its recent runs by step (RunHistory), its steps as a graph (from
// its newest run: pipeline code decides its steps as it runs), and all its runs.
export function Pipeline({ run }: { run: string }) {
  const api = useApi();
  const pipelines = usePipelines();
  const [status, setStatus] = useState("");
  const all = usePoll(() => api.runs.listRuns({ name: run, limit: 500 }), [run]);
  const history = usePoll(() => api.runs.listRuns({ name: run, limit: HISTORY, withSteps: true }), [run]);
  const runs = usePoll(() => api.runs.listRuns({ name: run, statuses: status ? [status] : [], limit: 100 }), [run, status]);
  const latest = all.data?.runs[0];
  const graph = usePoll(() => (latest ? api.runs.getRun({ runId: latest.id }) : Promise.resolve(undefined)), [latest?.id]);
  const info = pipelines.info(run);
  const s = all.data && all.data.runs.length ? summary(all.data.runs) : undefined;
  const schedule = pipelines.schedule(run);
  const sub = pipelines.subscription(run);
  const steps = graph.data?.run?.steps ?? [];
  const now = Date.now();

  return (
    <section>
      <p>
        <Link to={{ name: "runs" }}>← all pipelines</Link>
      </p>
      <div className="toolbar">
        <h1>{pipelines.title(run)}</h1>
        {s && <Status status={s.last.status} />}
      </div>
      {info?.description && <p className="desc">{info.description}</p>}
      <dl className="facts meta">
        <dt>in code</dt>
        <dd className="mono">{run}</dd>
        <dt>runs</dt>
        <dd>
          {schedule ? (
            <>
              on a schedule: <span className="mono">{schedule.cron}</span>
{` (${schedule.timezone || "UTC"})`}, {schedule.interval} data, catch-up {schedule.catchUp || "latest"}
            </>
          ) : sub ? (
            <>
              when new data arrives in <span className="mono">{sub.datasets.join(", ")}</span>
              {sub.waiting > 0 && ` (${sub.waiting} waiting)`}
            </>
          ) : (
            "when started (from here, moksha start, or another pipeline)"
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
              {all.data!.runs.length} runs, last {ago(s.last.createdAt, now)}
              {s.usually !== undefined && `, usually ${duration(s.usually)}`}
              {s.failed > 0 && <span className="error">, {s.failed} failed</span>}
            </dd>
          </>
        )}
      </dl>

      <h2>Recent runs</h2>
      {history.data && history.data.runs.length > 0 ? (
        <RunHistory runs={history.data.runs} now={now} />
      ) : (
        <p className="dim">{history.data ? "no runs yet" : "loading…"}</p>
      )}

      <h2>
        Steps <Hint word="step" />
      </h2>
      {steps.length > 0 ? (
        <>
          <Dag steps={steps} now={now} plain />
          <p className="dim small">as in the latest run</p>
        </>
      ) : (
        <p className="dim">{all.data ? "no run has steps yet" : "loading…"}</p>
      )}

      <h2>Start a run</h2>
      {/* A scheduled pipeline takes its data interval as an input: start with the latest one. */}
      <StartRun run={run} inputs={schedule?.latestParamsJson || latest?.paramsJson || "{}"} />
      {schedule?.latestParamsJson && <p className="dim small">inputs of the latest interval that came due</p>}

      <details className="raw">
        <summary>All runs, as a list</summary>
        <div className="toolbar">
          <StatusFilter value={status} onChange={setStatus} />
        </div>
        {runs.error && <p className="error">{runs.error}</p>}
        {runs.data && <RunsTable runs={runs.data.runs} />}
      </details>
    </section>
  );
}
