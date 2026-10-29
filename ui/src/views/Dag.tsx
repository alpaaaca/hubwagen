import type { Lift } from "../gen/hubwagen/v1/haul_pb";
import { duration, liftStatus, Status, took } from "../format";

const W = 200; // node width
const H = 80; // node height
const GX = 56; // gap between columns
const GY = 14; // gap between rows

type Node = { key: string; name: string; col: number; row: number; lifts: Lift[]; dataset: string };

// layout puts each lift in the column after the lifts it reads from, and
// folds lifts of one name in one column into one node (a fan-out of 100
// extracts is one box, "100 ×").
export function layout(lifts: Lift[]): { nodes: Node[]; edges: [Node, Node][] } {
  const col = new Map<number, number>(); // seq → column
  const nodeOf = new Map<number, Node>(); // seq → node
  const nodes: Node[] = [];
  const rows: number[] = [];
  for (const l of [...lifts].sort((a, b) => a.seq - b.seq)) {
    const known = l.after.filter((a) => col.has(a)); // pallets from outside the haul start a column
    const c = known.length ? Math.max(...known.map((a) => col.get(a)! + 1)) : 0;
    col.set(l.seq, c);
    const key = `${c}:${l.name}`;
    let n = nodes.find((x) => x.key === key);
    if (!n) {
      n = { key, name: l.name, col: c, row: rows[c] ?? 0, lifts: [], dataset: l.dataset };
      rows[c] = n.row + 1;
      nodes.push(n);
    }
    n.lifts.push(l);
    nodeOf.set(l.seq, n);
  }
  const edges: [Node, Node][] = [];
  const seen = new Set<string>();
  for (const l of lifts) {
    for (const a of l.after) {
      const from = nodeOf.get(a);
      const to = nodeOf.get(l.seq)!;
      if (from && from !== to && !seen.has(from.key + ">" + to.key)) {
        seen.add(from.key + ">" + to.key);
        edges.push([from, to]);
      }
    }
  }
  return { nodes, edges };
}

// groupStatus is a node's status: the worst of its lifts', or how far along they are.
function groupStatus(lifts: Lift[]): string {
  const ss = lifts.map(liftStatus);
  for (const s of ["failed", "cancelled", "retrying", "running", "waiting"]) if (ss.includes(s)) return s;
  return ss.every((s) => s === "reused") ? "reused" : "completed";
}

// Dag draws a haul's lifts as boxes in columns, with arrows from the lifts
// whose pallets each one reads.
export function Dag({
  lifts,
  now,
  plain = false,
  selected = 0,
  onSelect,
}: {
  lifts: Lift[];
  now: number;
  plain?: boolean;
  selected?: number; // a lift's seq, to mark its node
  onSelect?: (seq: number) => void; // clicking a node picks its (first) lift
}) {
  const { nodes, edges } = layout(lifts);
  const cols = Math.max(...nodes.map((n) => n.col)) + 1;
  const rows = Math.max(...nodes.map((n) => n.row)) + 1;
  const width = cols * W + (cols - 1) * GX;
  const height = rows * H + (rows - 1) * GY;
  const x = (n: Node) => n.col * (W + GX);
  const y = (n: Node) => n.row * (H + GY);
  return (
    <div className="dag-scroll">
      <div className="dag" style={{ width, height }} role="list" aria-label="steps graph">
        <svg width={width} height={height} aria-hidden="true">
          <defs>
            <marker id="arrow" viewBox="0 0 8 8" refX="7" refY="4" markerWidth="7" markerHeight="7" orient="auto-start-reverse">
              <path d="M0,0 L8,4 L0,8 z" className="dag-arrow" />
            </marker>
          </defs>
          {edges.map(([a, b]) => {
            const x1 = x(a) + W;
            const y1 = y(a) + H / 2;
            const x2 = x(b) - 2;
            const y2 = y(b) + H / 2;
            const mid = (x1 + x2) / 2;
            return <path key={a.key + ">" + b.key} d={`M${x1},${y1} C${mid},${y1} ${mid},${y2} ${x2},${y2}`} className="dag-edge" markerEnd="url(#arrow)" />;
          })}
        </svg>
        {nodes.map((n) => {
          const status = groupStatus(n.lifts);
          const one = n.lifts.length === 1 ? n.lifts[0] : undefined;
          const done = n.lifts.filter((l) => l.status === "completed").length;
          const t = one && took(one.scheduledAt, one.finishedAt, now);
          return (
            <div
              key={n.key}
              role="listitem"
              className={`dag-node${plain ? "" : ` node-${status}`}${onSelect ? " clickable" : ""}${n.lifts.some((l) => l.seq === selected) ? " selected" : ""}`}
              style={{ left: x(n), top: y(n), width: W, height: H }}
              onClick={onSelect && (() => onSelect(n.lifts[0].seq))}
              title={onSelect ? "show its logs" : undefined}
            >
              <div className="dag-name" title={n.name}>
                {n.lifts.length > 1 && <span className="dim">{n.lifts.length} × </span>}
                {n.name}
              </div>
              {!plain && (
                <div className="dag-meta">
                  <Status status={status} />
                  <span className="dim small">{one ? (t === undefined ? "" : duration(t)) : `${done} of ${n.lifts.length} done`}</span>
                </div>
              )}
              {n.dataset && (
                <div className="dim small dag-dataset" title={`writes ${n.dataset}`}>
                  → {n.dataset}
                </div>
              )}
            </div>
          );
        })}
      </div>
    </div>
  );
}
