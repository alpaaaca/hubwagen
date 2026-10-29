import { create } from "@bufbuild/protobuf";

import { LiftSchema } from "../gen/hubwagen/v1/haul_pb";
import { layout } from "./Dag";

const lift = (seq: number, name: string, after: number[] = []) => create(LiftSchema, { seq, name, after });

test("columns follow what each lift reads; a fan-out folds into one node", () => {
  // extract ×3 → combine ← lookup; report after combine.
  const { nodes, edges } = layout([
    lift(1, "lookup"),
    lift(2, "extract"),
    lift(3, "extract"),
    lift(4, "extract"),
    lift(5, "combine", [1, 2, 3, 4]),
    lift(6, "report", [5]),
  ]);
  expect(nodes.map((n) => [n.name, n.col, n.row, n.lifts.length])).toEqual([
    ["lookup", 0, 0, 1],
    ["extract", 0, 1, 3],
    ["combine", 1, 0, 1],
    ["report", 2, 0, 1],
  ]);
  expect(edges.map(([a, b]) => `${a.name}>${b.name}`)).toEqual(["lookup>combine", "extract>combine", "combine>report"]);
});

test("a lift reading only from outside the haul starts a column", () => {
  const { nodes, edges } = layout([lift(1, "a"), lift(2, "b", [9])]);
  expect(nodes.map((n) => n.col)).toEqual([0, 0]);
  expect(edges).toEqual([]);
});
