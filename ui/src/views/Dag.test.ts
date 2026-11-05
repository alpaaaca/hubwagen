import { create } from "@bufbuild/protobuf";

import { StepSchema } from "../gen/moksha/v1/run_pb";
import { layout } from "./Dag";

const step = (seq: number, name: string, after: number[] = []) => create(StepSchema, { seq, name, after });

test("columns follow what each step reads; a fan-out folds into one node", () => {
  // extract ×3 → combine ← lookup; report after combine.
  const { nodes, edges } = layout([
    step(1, "lookup"),
    step(2, "extract"),
    step(3, "extract"),
    step(4, "extract"),
    step(5, "combine", [1, 2, 3, 4]),
    step(6, "report", [5]),
  ]);
  expect(nodes.map((n) => [n.name, n.col, n.row, n.steps.length])).toEqual([
    ["lookup", 0, 0, 1],
    ["extract", 0, 1, 3],
    ["combine", 1, 0, 1],
    ["report", 2, 0, 1],
  ]);
  expect(edges.map(([a, b]) => `${a.name}>${b.name}`)).toEqual(["lookup>combine", "extract>combine", "combine>report"]);
});

test("a step reading only from outside the run starts a column", () => {
  const { nodes, edges } = layout([step(1, "a"), step(2, "b", [9])]);
  expect(nodes.map((n) => n.col)).toEqual([0, 0]);
  expect(edges).toEqual([]);
});
