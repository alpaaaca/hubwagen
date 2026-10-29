import { useState } from "react";

import { useApi } from "../api";
import { ago, duration, Status, when } from "../format";
import { usePoll } from "../hooks";
import { usePipelines } from "../pipelines";
import { Link } from "../router";
import { Dag } from "./Dag";
import { RunsTable, StartRun, StatusFilter, summary } from "./HaulList";

// Pipeline: one pipeline (a haul by name): what it is, its steps as a
// graph (from its newest run: haul code decides its steps as it runs), how
// it is started, and its runs.
export function Pipeline({ haul }: { haul: string }) {
  const api = useApi();
  const pipelines = usePipelines();
  const [status, setStatus] = useState("");
  const all = usePoll(() => api.hauls.listHauls({ name: haul, limit: 500 }), [haul]);
  const runs = usePoll(() => api.hauls.listHauls({ name: haul, statuses: status ? [status] : [], limit: 100 }), [haul, status]);
  const latest = all.data?.hauls[0];
  const graph = usePoll(() => (latest ? api.hauls.getHaul({ haulId: latest.id }) : Promise.resolve(undefined)), [latest?.id]);
  const schedules = usePoll(() => api.hauls.listSchedules({}), [], 15000);
  const info = pipelines.info(haul);
  const s = all.data && all.data.hauls.length ? summary(all.data.hauls) : undefined;
  const schedule = schedules.data?.schedules.find((x) => x.haul === haul);
  const sub = schedules.data?.subscriptions.find((x) => x.haul === haul);
  const lifts = graph.data?.haul?.lifts ?? [];

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
      <p className="dim">
        <span className="mono">{haul}</span> in code
        {schedule && (
          <>
            {" "}
            · runs on a schedule (<span className="mono">{schedule.cron}</span>
            {schedule.timezone && ` ${schedule.timezone}`}), next <span title={when(schedule.nextRun)}>{when(schedule.nextRun)}</span>
          </>
        )}
        {sub && (
          <>
            {" "}
            · runs when new data arrives in {sub.datasets.join(", ")}
            {sub.waiting > 0 && ` (${sub.waiting} waiting)`}
          </>
        )}
        {s && (
          <>
            {" "}
            · {all.data!.hauls.length} runs, last {ago(s.last.createdAt)}
            {s.usually !== undefined && `, usually ${duration(s.usually)}`}
            {s.failed > 0 && <span className="error">, {s.failed} failed</span>}
          </>
        )}
      </p>

      <h2>Steps</h2>
      {lifts.length > 0 ? (
        <>
          <Dag lifts={lifts} now={Date.now()} plain />
          <p className="dim small">as in the latest run; open a run to see how its steps went</p>
        </>
      ) : (
        <p className="dim">{all.data ? "no run has steps yet" : "loading…"}</p>
      )}

      <h2>Start a run</h2>
      <StartRun haul={haul} inputs={latest?.paramsJson || "{}"} />

      <div className="toolbar">
        <h2>Runs</h2>
        <StatusFilter value={status} onChange={setStatus} />
      </div>
      {runs.error && <p className="error">{runs.error}</p>}
      {runs.data && <RunsTable hauls={runs.data.hauls} />}
    </section>
  );
}
