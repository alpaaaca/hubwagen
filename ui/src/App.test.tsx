import { create, type MessageInitShape } from "@bufbuild/protobuf";
import { timestampFromDate, type Timestamp } from "@bufbuild/protobuf/wkt";
import { createRouterTransport } from "@connectrpc/connect";
import { render, screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";

import { ApiProvider, apiFor } from "./api";
import { App } from "./App";
import { HaulSchema, HaulService, LiftCountsSchema, LiftSchema } from "./gen/hubwagen/v1/haul_pb";
import { LogService } from "./gen/hubwagen/v1/log_pb";

const start = new Date(2026, 9, 24, 3, 0, 0);
const at = (seconds: number) => timestampFromDate(new Date(start.getTime() + seconds * 1000));
const t0 = at(0);

function haul(id: string, name: string, status: string, extra: MessageInitShape<typeof HaulSchema> = {}) {
  return create(HaulSchema, { id, tenant: "default", name, status, createdAt: t0, ...extra });
}

// A HaulService in memory, recording what the UI asked.
function fakeServer() {
  const calls: Record<string, unknown[]> = {};
  const record = (m: string, req: unknown) => (calls[m] ??= []).push(req);
  const daily = haul("01M6AAAAAAAAAAAAAAAAAAAAAA", "daily", "running", {
    paramsJson: '{"day":"2026-10-24"}',
    trigger: "schedule",
    liftCounts: create(LiftCountsSchema, { total: 3, completed: 2, reused: 1 }),
    lifts: [
      create(LiftSchema, {
        seq: 1, name: "extract", status: "completed", attempts: 1, runReason: "no previous run", why: "it ran because it hasn't run with these inputs before",
        pallet: "01M6PALLETPALLETPALLET0001", scheduledAt: at(0), finishedAt: at(2.5), resultJson: '{"rows": 6}', dataset: "raw.orders",
      }),
      create(LiftSchema, { seq: 2, name: "build", status: "completed", attempts: 0, reused: true, why: "same inputs and same code as before, so it didn't run again", scheduledAt: at(3), finishedAt: at(3), after: [1] }),
      create(LiftSchema, { seq: 3, name: "report", status: "dispatched", attempts: 1, runReason: "code changed", scheduledAt: at(3), after: [2] }),
    ],
  });
  const done = haul("01M6DDDDDDDDDDDDDDDDDDDDDD", "daily", "completed", {
    paramsJson: '{"day":"2026-10-23"}',
    finishedAt: at(4),
    resultJson: '{"orders": 6, "total": "775.99"}',
    liftCounts: create(LiftCountsSchema, { total: 3, completed: 3, reused: 3 }),
  });
  const stuck = haul("01M6BBBBBBBBBBBBBBBBBBBBBB", "hourly", "stuck", { reason: "nondeterminism", message: "time.time() called" });
  const broken = haul("01M6EEEEEEEEEEEEEEEEEEEEEE", "daily", "failed", {
    paramsJson: '{"day":"2026-10-22"}',
    finishedAt: at(2),
    reason: "LiftFailed",
    lifts: [
      create(LiftSchema, { seq: 1, name: "extract", status: "completed", scheduledAt: at(0), finishedAt: at(1) }),
      create(LiftSchema, { seq: 2, name: "build", status: "failed", reason: "ValueError", message: "no orders", scheduledAt: at(1), finishedAt: at(2), after: [1] }),
    ],
  });
  const waiter = haul("01M6WWWWWWWWWWWWWWWWWWWWWW", "daily", "running", {
    paramsJson: '{"day":"2026-10-25"}',
    waiting: {
      reason: "no_worker", kind: "haul", name: "daily", dock: "default", since: t0,
      message: "no worker is connected to run the pipeline's code. Start one with `python -m hubwagen.worker <modules>` (or `hubwagen workers --config hubwagen.yaml`)",
    },
  });
  const runLogs: Record<string, { seq: number; lift: string; attempt: number; line: { time: Timestamp; stream: string; level?: string; text: string } }[]> = {
    [daily.id]: [
      { seq: 1, lift: "extract", attempt: 1, line: { time: at(0.1), stream: "stdout", text: "reading 6 orders" } },
      { seq: 2, lift: "build", attempt: 1, line: { time: at(3.1), stream: "log", level: "WARNING", text: "one order has no region" } },
    ],
    [broken.id]: [
      { seq: 1, lift: "extract", attempt: 1, line: { time: at(0.1), stream: "stdout", text: "reading 0 orders" } },
      { seq: 2, lift: "build", attempt: 1, line: { time: at(1.1), stream: "stderr", text: "ValueError: no orders" } },
    ],
  };
  const transport = createRouterTransport(({ service }) => {
    service(HaulService, {
      listHauls(req) {
        record("listHauls", req);
        if (req.parentId) return { hauls: [haul("01M6CHILDCHILDCHILDCHILD01", "child", "completed", { parentId: req.parentId })] };
        const all = [daily, done, stuck, broken];
        return { hauls: all.filter((h) => (!req.name || h.name === req.name) && (!req.statuses.length || req.statuses.includes(h.status))) };
      },
      getHaul(req) {
        record("getHaul", req);
        return { haul: [daily, done, stuck, broken, waiter].find((h) => h.id === req.haulId) };
      },
      getHistory(req) {
        record("getHistory", req);
        return {
          entries: [
            { eventSeq: 1n, time: t0, kind: "started", text: "Started by its schedule, for day=2026-10-24", eventType: "HaulStarted", payloadJson: '{"name":"daily"}' },
            { eventSeq: 2n, time: t0, kind: "engine", text: "Waits for a worker to run the pipeline's code", eventType: "HaulTaskQueued", payloadJson: "{}" },
            { eventSeq: 3n, time: at(2.5), kind: "step", seq: 1, step: "extract", text: "done in 2.5 s, wrote a new version of raw.orders for 2026-10-24", eventType: "LiftFinished" },
            { eventSeq: 4n, time: at(3), kind: "reused", seq: 2, step: "build", text: "reused the existing version of mart.revenue: same inputs and same code, so it didn't run again (from the run of daily for day=2026-10-23)", haulId: "01M6DDDDDDDDDDDDDDDDDDDDDD", eventType: "LiftReused" },
          ],
        };
      },
      startHaul(req) {
        record("startHaul", req);
        return { haulId: daily.id };
      },
      cancelHaul(req) {
        record("cancelHaul", req);
        return {};
      },
      resumeHaul(req) {
        record("resumeHaul", req);
        return {};
      },
      signalHaul(req) {
        record("signalHaul", req);
        return { signalId: "s1" };
      },
      listWorkers() {
        record("listWorkers", {});
        return { workers: [] };
      },
      listPools() {
        return {
          pools: [
            { name: "py", docks: ["default"], min: 0, max: 3, workers: ["py-1"], starting: 1, desired: 1, decision: "started py-1: 1 run waits on default", decidedAt: t0 },
            { name: "k8s", docks: ["gpu"], min: 0, max: 5, workers: [], desired: 2, external: true },
          ],
        };
      },
      listPipelines() {
        return { pipelines: [{ name: "daily", title: "Daily revenue", description: "Orders to a report, per day." }, { name: "hourly" }] };
      },
      listSchedules() {
        const next = timestampFromDate(new Date(Date.now() + 5 * 3600 * 1000));
        const latest = '{"interval":{"$interval":{"end":"2026-10-24T13:00:00Z","start":"2026-10-24T12:00:00Z"}}}';
        return {
          schedules: [
            { haul: "daily", cron: "0 3 * * *", timezone: "Europe/Berlin", interval: "daily", nextRun: next },
            { haul: "nightly", cron: "5 * * * *", interval: "hourly", nextRun: next, latestParamsJson: latest },
          ],
          subscriptions: [],
        };
      },
    });
    service(LogService, {
      getLogs(req) {
        record("getLogs", req);
        return { lines: (runLogs[req.haulId] ?? []).filter((l) => !req.seq || l.seq === req.seq) };
      },
    });
  });
  return { api: apiFor(transport), calls };
}

function show(hash: string) {
  window.location.hash = hash;
  const server = fakeServer();
  render(
    <ApiProvider api={server.api}>
      <App />
    </ApiProvider>,
  );
  return server;
}

test("the start page: each pipeline's meta information, not its runs", async () => {
  show("#/hauls");
  const table = await screen.findByRole("table");
  const daily = (await within(table).findByRole("link", { name: "Daily revenue" })).closest("tr")!;
  expect(daily).toHaveTextContent("Orders to a report, per day.");
  expect(daily).toHaveTextContent("0 3 * * *"); // its cron
  expect(daily).toHaveTextContent("Europe/Berlin");
  expect(daily).toHaveTextContent("in 5 h"); // next run
  expect(daily).toHaveTextContent("1 of 2"); // of its finished runs, 1 succeeded
  expect(daily).toHaveTextContent("4.0 s"); // usually takes
  // Recent runs: a square each, oldest first.
  const recent = within(daily).getByLabelText("recent runs of Daily revenue");
  expect(within(recent).getAllByRole("link").map((a) => a.className)).toEqual([
    "recent-run run-failed", "recent-run run-completed", "recent-run run-running",
  ]);
  const hourly = within(table).getByRole("link", { name: "hourly" }).closest("tr")!;
  expect(hourly).toHaveTextContent("on demand");
  // No list of runs here: they are on each pipeline's page.
  expect(screen.queryByText("2026-10-23")).not.toBeInTheDocument();

  await userEvent.click(daily);
  await waitFor(() => expect(window.location.hash).toBe("#/pipelines/daily"));
});

test("a pipeline: its run history by step, its steps, starting a run", async () => {
  const { calls } = show("#/pipelines/daily");
  expect(await screen.findByRole("heading", { name: "Daily revenue" })).toBeInTheDocument();
  expect(screen.getByText(/on a schedule/)).toHaveTextContent("0 3 * * * (Europe/Berlin), daily data");
  expect(screen.getByText("next run").nextElementSibling).toHaveTextContent("in 5 h");

  // The history: a row per step, a column per run (oldest first).
  const grid = await screen.findByRole("grid", { name: "run history" });
  const rows = within(grid).getAllByRole("row");
  expect(rows.map((r) => within(r).getByRole("rowheader").textContent)).toEqual(["extract", "build", "report"]);
  const cells = (r: HTMLElement) => [...r.querySelectorAll(".history-cell")].map((c) => c.className.replace("history-cell ", ""));
  expect(cells(rows[1])).toEqual(["cell-failed", "none", "cell-reused"]); // broken, done (no lifts), daily
  expect(cells(rows[2])).toEqual(["none", "none", "cell-running"]);
  expect(calls.listHauls.find((r) => (r as { withLifts?: boolean }).withLifts)).toMatchObject({ name: "daily", withLifts: true });
  // A square opens the run on that step.
  await userEvent.click(within(rows[1]).getAllByRole("link")[0]);
  await waitFor(() => expect(window.location.hash).toBe("#/hauls/01M6EEEEEEEEEEEEEEEEEEEEEE/2"));
});

test("a pipeline: its steps as a graph, all its runs, starting one", async () => {
  const { calls } = show("#/pipelines/daily");
  const graph = await screen.findByRole("list", { name: "steps graph" });
  expect(within(graph).getAllByRole("listitem").map((n) => n.textContent)).toEqual(["extract→ raw.orders", "build", "report"]);

  await userEvent.click(screen.getByText("All runs, as a list"));
  const runs = await screen.findByRole("table");
  expect(within(runs).getByText("2026-10-23").closest("tr")).toHaveTextContent("3 steps · all reused");
  await userEvent.selectOptions(screen.getByLabelText("status"), "completed");
  await waitFor(() => expect(within(screen.getByRole("table")).queryByText("2026-10-24")).not.toBeInTheDocument());
  expect(calls.listHauls.at(-1)).toMatchObject({ name: "daily", statuses: ["completed"] });

  // Starting a run: the inputs start as the latest run's.
  const params = screen.getByLabelText("inputs");
  expect(params).toHaveValue('{"day":"2026-10-24"}');
  await userEvent.clear(params);
  await userEvent.type(params, "not json");
  await userEvent.click(screen.getByRole("button", { name: "Start a run" }));
  expect(await screen.findByText(/inputs must be JSON/)).toBeInTheDocument();
  expect(calls.startHaul).toBeUndefined();

  await userEvent.clear(params);
  await userEvent.type(params, '{{"day": "2026-10-25"}');
  await userEvent.click(screen.getByRole("button", { name: "Start a run" }));
  await waitFor(() => expect(window.location.hash).toBe("#/hauls/01M6AAAAAAAAAAAAAAAAAAAAAA"));
  expect(calls.startHaul[0]).toMatchObject({ name: "daily", paramsJson: '{"day": "2026-10-25"}', trigger: "the web UI" });
});

test("a scheduled pipeline starts by hand with the latest interval as its inputs", async () => {
  show("#/pipelines/nightly");
  await waitFor(() => expect(screen.getByLabelText("inputs")).toHaveValue('{"interval":{"$interval":{"end":"2026-10-24T13:00:00Z","start":"2026-10-24T12:00:00Z"}}}'));
  expect(screen.getByText("inputs of the latest interval that came due")).toBeInTheDocument();
});

test("a run: steps on a timeline, children, events, signal and cancel", async () => {
  const { calls } = show("#/hauls/01M6AAAAAAAAAAAAAAAAAAAAAA");
  expect(await screen.findByRole("heading", { name: "Daily revenue" })).toBeInTheDocument();
  expect(screen.getByText(/on its schedule/)).toBeInTheDocument();
  const graph = screen.getByRole("list", { name: "steps graph" });
  const nodes = within(graph).getAllByRole("listitem").map((n) => n.textContent);
  expect(nodes.slice(0, 2)).toEqual(["extractcompleted2.5 s→ raw.orders", "buildreused0 ms"]);
  expect(nodes[2]).toMatch(/^reportrunning/); // for as long as it has been running
  const steps = screen.getAllByRole("table")[0];
  const extract = within(steps).getByText("extract").closest("tr")!;
  expect(extract).toHaveTextContent("2.5 s");
  expect(extract).toHaveTextContent("it hasn't run with these inputs before");
  expect(extract).toHaveTextContent("wrote a new version of raw.orders");
  expect(extract).toHaveTextContent("rows 6");
  const build = within(steps).getByText("build").closest("tr")!;
  expect(within(build).getByText("reused")).toBeInTheDocument();
  expect(build).toHaveTextContent("didn't run again");
  expect(within(steps).getByText("report").closest("tr")).toHaveTextContent("running");

  // Logs: all steps, each line marked with its step; a step shows its own.
  const logs = screen.getByRole("log", { name: "log lines" });
  expect(await within(logs).findByText("reading 6 orders")).toBeInTheDocument();
  expect(within(logs).getByText("one order has no region").closest(".log-line")).toHaveTextContent(/build\s*WARNING/);
  await userEvent.click(build);
  await waitFor(() => expect(within(logs).queryByText("reading 6 orders")).not.toBeInTheDocument());
  expect(calls.getLogs.at(-1)).toMatchObject({ haulId: "01M6AAAAAAAAAAAAAAAAAAAAAA", seq: 2 });
  expect(screen.getByLabelText("logs of")).toHaveValue("2");
  expect(await screen.findByText("child")).toBeInTheDocument();
  expect(calls.listHauls[0]).toMatchObject({ parentId: "01M6AAAAAAAAAAAAAAAAAAAAAA" });

  // What happened, in sentences; the engine's bookkeeping only when asked.
  const story = await screen.findByRole("list", { name: "what happened" });
  await waitFor(() => expect(within(story).getAllByRole("listitem")).toHaveLength(3));
  expect(within(story).getAllByRole("listitem")[0]).toHaveTextContent("Started by its schedule, for day=2026-10-24");
  expect(within(story).getAllByRole("listitem")[2]).toHaveTextContent(/build reused the existing version of mart.revenue/);
  expect(within(story).getByRole("link", { name: "open that run" })).toHaveAttribute("href", "#/hauls/01M6DDDDDDDDDDDDDDDDDDDDDD");
  expect(within(story).queryByText(/Waits for a worker/)).not.toBeInTheDocument();
  await userEvent.click(screen.getByLabelText("engine details"));
  expect(within(story).getByText(/Waits for a worker/)).toBeInTheDocument();
  expect(within(story).getByText(/HaulTaskQueued/)).toBeInTheDocument();
  await userEvent.click(within(story).getByRole("button", { name: "extract" }));
  expect(screen.getByLabelText("logs of")).toHaveValue("1");
  expect(screen.getByRole("note", { name: /^step: One function call/ })).toBeInTheDocument();
  expect(screen.getByText("2026-10-24")).toBeInTheDocument();
  expect(screen.queryByRole("button", { name: "Resume" })).not.toBeInTheDocument();

  await userEvent.type(screen.getByLabelText("signal name"), "approve");
  await userEvent.type(screen.getByLabelText("signal payload"), '{{"by": "ana"}');
  await userEvent.click(screen.getByRole("button", { name: "Send signal" }));
  await waitFor(() => expect(calls.signalHaul?.[0]).toMatchObject({ name: "approve", payloadJson: '{"by": "ana"}' }));

  await userEvent.click(screen.getByRole("button", { name: "Cancel" }));
  await waitFor(() => expect(calls.cancelHaul?.[0]).toMatchObject({ haulId: "01M6AAAAAAAAAAAAAAAAAAAAAA" }));
});

test("a finished run shows its result", async () => {
  show("#/hauls/01M6DDDDDDDDDDDDDDDDDDDDDD");
  expect(await screen.findByText("Result")).toBeInTheDocument();
  expect(screen.getByText("total").nextElementSibling).toHaveTextContent("775.99");
  expect(screen.getByText(/took 4.0 s · 3 steps · all reused/)).toBeInTheDocument();
  expect(screen.queryByRole("button", { name: "Cancel" })).not.toBeInTheDocument();
});

test("a run opened on a step shows that step's log", async () => {
  const { calls } = show("#/hauls/01M6AAAAAAAAAAAAAAAAAAAAAA/1");
  const logs = await screen.findByRole("log", { name: "log lines" });
  expect(await within(logs).findByText("reading 6 orders")).toBeInTheDocument();
  expect(within(logs).queryByText("one order has no region")).not.toBeInTheDocument();
  expect(calls.getLogs[0]).toMatchObject({ seq: 1 });
});

test("a failed run opens on the failed step's log", async () => {
  const { calls } = show("#/hauls/01M6EEEEEEEEEEEEEEEEEEEEEE");
  const logs = await screen.findByRole("log", { name: "log lines" });
  expect(await within(logs).findByText("ValueError: no orders")).toBeInTheDocument();
  expect(within(logs).queryByText("reading 0 orders")).not.toBeInTheDocument();
  expect(calls.getLogs[0]).toMatchObject({ seq: 2 });
  await userEvent.selectOptions(screen.getByLabelText("logs of"), "all steps");
  expect(await within(logs).findByText("reading 0 orders")).toBeInTheDocument();
});

test("a stuck haul can be resumed", async () => {
  const { calls } = show("#/hauls/01M6BBBBBBBBBBBBBBBBBBBBBB");
  expect(await screen.findByText(/nondeterminism: time.time\(\) called/)).toBeInTheDocument();
  await userEvent.click(screen.getByRole("button", { name: "Resume" }));
  await waitFor(() => expect(calls.resumeHaul?.[0]).toMatchObject({ haulId: "01M6BBBBBBBBBBBBBBBBBBBBBB" }));
});

test("a run that waits says why, and which workers are connected", async () => {
  const { calls } = show("#/hauls/01M6WWWWWWWWWWWWWWWWWWWWWW");
  const why = await screen.findByText(/^Waiting:/);
  expect(why).toHaveTextContent("Waiting: no worker is connected to run the pipeline's code. Start one with python -m hubwagen.worker <modules>");
  expect(why.querySelector("code")).toHaveTextContent("python -m hubwagen.worker <modules>");
  expect(await screen.findByText("No workers connected.")).toBeInTheDocument();
  const pools = await screen.findAllByText(/^Pool/);
  expect(pools[0]).toHaveTextContent("Pool py (default): 1 of 0–3 workers, 1 starting · started py-1: 1 run waits on default");
  expect(pools[1]).toHaveTextContent("Pool k8s (gpu, started elsewhere): 0 of 0–5 workers, wants 2");
  expect(calls.listWorkers).toBeDefined();
});

test("a run that doesn't wait shows no reason", async () => {
  show("#/hauls/01M6AAAAAAAAAAAAAAAAAAAAAA");
  expect(await screen.findByRole("heading", { name: "Daily revenue" })).toBeInTheDocument();
  expect(screen.queryByText(/^Waiting:/)).not.toBeInTheDocument();
});
