import { create, type MessageInitShape } from "@bufbuild/protobuf";
import { timestampFromDate } from "@bufbuild/protobuf/wkt";
import { createRouterTransport } from "@connectrpc/connect";
import { render, screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";

import { ApiProvider, apiFor } from "./api";
import { App } from "./App";
import { HaulSchema, HaulService, LiftCountsSchema, LiftSchema, LogbookEventSchema } from "./gen/hubwagen/v1/haul_pb";

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
        seq: 1, name: "extract", status: "completed", attempts: 1, runReason: "not memoized",
        pallet: "01M6PALLETPALLETPALLET0001", scheduledAt: at(0), finishedAt: at(2.5), resultJson: '{"rows": 6}', dataset: "raw.orders",
      }),
      create(LiftSchema, { seq: 2, name: "build", status: "completed", attempts: 0, reused: true, scheduledAt: at(3), finishedAt: at(3), after: [1] }),
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
  const transport = createRouterTransport(({ service }) => {
    service(HaulService, {
      listHauls(req) {
        record("listHauls", req);
        if (req.parentId) return { hauls: [haul("01M6CHILDCHILDCHILDCHILD01", "child", "completed", { parentId: req.parentId })] };
        const all = [daily, done, stuck];
        return { hauls: all.filter((h) => (!req.name || h.name === req.name) && (!req.statuses.length || req.statuses.includes(h.status))) };
      },
      getHaul(req) {
        record("getHaul", req);
        return { haul: [daily, done, stuck].find((h) => h.id === req.haulId) };
      },
      getLogbook(req) {
        record("getLogbook", req);
        return { logbook: [create(LogbookEventSchema, { seq: 1n, type: "HaulStarted", time: t0, payloadJson: '{"name":"daily"}' })] };
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
      listPipelines() {
        return { pipelines: [{ name: "daily", title: "Daily revenue", description: "Orders to a report, per day." }, { name: "hourly" }] };
      },
      listSchedules() {
        return { schedules: [{ haul: "daily", cron: "0 3 * * *", timezone: "Europe/Berlin", nextRun: at(86400) }], subscriptions: [] };
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

test("the start page: pipelines by title, and the latest runs", async () => {
  show("#/hauls");
  // A card per pipeline, by its title: its last run and how its runs went.
  const cards = await screen.findByRole("navigation", { name: "pipelines" });
  const card = within(cards).getByRole("link", { name: "Daily revenue" });
  expect(card).toHaveTextContent("Orders to a report, per day.");
  expect(card).toHaveTextContent(/2 runs · none failed · usually 4.0 s/);
  expect(within(cards).getByRole("link", { name: "hourly" })).toHaveTextContent("1 failed"); // no title: its name

  // Runs say what went in, how far they got, how long they took and what came out.
  const runs = screen.getByRole("table");
  const finished = within(runs).getByText("2026-10-23").closest("tr")!;
  expect(finished).toHaveTextContent("Daily revenue");
  expect(finished).toHaveTextContent("3 steps · all reused");
  expect(finished).toHaveTextContent("4.0 s");
  expect(finished).toHaveTextContent("775.99");
  expect(within(runs).getByText("2026-10-24").closest("tr")).toHaveTextContent("2 of 3 done · 1 reused");
  expect(within(runs).getByText("time.time() called")).toBeInTheDocument();

  await userEvent.click(card);
  await waitFor(() => expect(window.location.hash).toBe("#/pipelines/daily"));
});

test("a pipeline: its steps as a graph, its schedule, its runs, starting one", async () => {
  const { calls } = show("#/pipelines/daily");
  expect(await screen.findByRole("heading", { name: "Daily revenue" })).toBeInTheDocument();
  expect(screen.getByText(/runs on a schedule/)).toHaveTextContent("0 3 * * * Europe/Berlin");
  const graph = await screen.findByRole("list", { name: "steps graph" });
  expect(within(graph).getAllByRole("listitem").map((n) => n.textContent)).toEqual(["extract→ raw.orders", "build", "report"]);
  expect(calls.getHaul[0]).toMatchObject({ haulId: "01M6AAAAAAAAAAAAAAAAAAAAAA" }); // the newest run

  const runs = screen.getByRole("table");
  expect(within(runs).queryByText("Daily revenue")).not.toBeInTheDocument(); // every run is of this pipeline
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
  expect(extract).toHaveTextContent("not memoized");
  expect(extract).toHaveTextContent("rows 6");
  const build = within(steps).getByText("build").closest("tr")!;
  expect(within(build).getByText("reused")).toBeInTheDocument();
  expect(build).toHaveTextContent("nothing ran");
  expect(within(steps).getByText("report").closest("tr")).toHaveTextContent("running");
  expect(await screen.findByText("child")).toBeInTheDocument();
  expect(calls.listHauls[0]).toMatchObject({ parentId: "01M6AAAAAAAAAAAAAAAAAAAAAA" });
  expect(await screen.findByText("HaulStarted")).toBeInTheDocument();
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

test("a stuck haul can be resumed", async () => {
  const { calls } = show("#/hauls/01M6BBBBBBBBBBBBBBBBBBBBBB");
  expect(await screen.findByText(/nondeterminism: time.time\(\) called/)).toBeInTheDocument();
  await userEvent.click(screen.getByRole("button", { name: "Resume" }));
  await waitFor(() => expect(calls.resumeHaul?.[0]).toMatchObject({ haulId: "01M6BBBBBBBBBBBBBBBBBBBBBB" }));
});
