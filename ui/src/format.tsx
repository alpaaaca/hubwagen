import { timestampDate, type Timestamp } from "@bufbuild/protobuf/wkt";

import type { Haul, Lift } from "./gen/hubwagen/v1/haul_pb";

// Times are always shown in UTC, and say so: hubwagen runs on servers and
// schedules in many places, and one clock for everyone is easier to compare.

// when is a point in time, to the second, in UTC.
export function when(t?: Timestamp): string {
  if (!t) return "–";
  return utc(timestampDate(t)).slice(0, 19) + " UTC";
}

// clock is a time of day in UTC, to the millisecond (digits: how many to keep).
export function clock(t?: Timestamp, digits = 3): string {
  if (!t) return "";
  const s = utc(timestampDate(t)).slice(11, 23);
  return digits === 0 ? s.slice(0, 8) : s.slice(0, 9 + digits);
}

// utc is "YYYY-MM-DD HH:MM:SS.mmm" in UTC.
function utc(d: Date): string {
  return d.toISOString().replace("T", " ").replace("Z", "");
}

// interval shows a data interval in UTC: a whole UTC day as that day, whole
// days as "start – last day", else start and end times.
export function interval(start: string, end: string): string {
  const a = new Date(start);
  const b = new Date(end);
  if (isNaN(a.getTime()) || isNaN(b.getTime())) return `${start} – ${end}`;
  const day = 86400000;
  const midnight = (d: Date) => d.getTime() % day === 0;
  if (midnight(a) && midnight(b)) {
    const last = new Date(b.getTime() - day);
    return b.getTime() - a.getTime() === day ? utc(a).slice(0, 10) : `${utc(a).slice(0, 10)} – ${utc(last).slice(0, 10)}`;
  }
  const sameDay = utc(a).slice(0, 10) === utc(b).slice(0, 10);
  return `${utc(a).slice(0, 16)} – ${sameDay ? utc(b).slice(11, 16) : utc(b).slice(0, 16)} UTC`;
}

export function ms(t?: Timestamp): number | undefined {
  return t ? timestampDate(t).getTime() : undefined;
}

// ago says how long ago t was, for reading at a glance; when() has the exact time.
export function ago(t?: Timestamp, now = Date.now()): string {
  const at = ms(t);
  if (at === undefined) return "–";
  const s = Math.round((now - at) / 1000);
  if (s < 10) return "just now";
  if (s < 60) return `${s}s ago`;
  if (s < 3600) return `${Math.floor(s / 60)} min ago`;
  if (s < 86400) return `${Math.floor(s / 3600)} h ago`;
  const days = Math.floor(s / 86400);
  return days === 1 ? "yesterday" : `${days} days ago`;
}

// fromNow says when a future t is: "in 5 min", "in 3 h", "in 2 days".
export function fromNow(t?: Timestamp, now = Date.now()): string {
  const at = ms(t);
  if (at === undefined) return "–";
  const s = Math.round((at - now) / 1000);
  if (s < 60) return s <= 0 ? "now" : "in a minute";
  if (s < 3600) return `in ${Math.round(s / 60)} min`;
  if (s < 86400) return `in ${Math.round(s / 3600)} h`;
  const days = Math.round(s / 86400);
  return days === 1 ? "tomorrow" : `in ${days} days`;
}

// duration formats milliseconds: "45 ms", "3.2 s", "4 min 05 s", "2 h 10 min".
export function duration(millis: number): string {
  if (millis < 1000) return `${Math.max(0, Math.round(millis))} ms`;
  const s = millis / 1000;
  if (s < 60) return `${s < 10 ? s.toFixed(1) : Math.round(s)} s`;
  const m = Math.floor(s / 60);
  if (m < 60) return `${m} min ${String(Math.round(s % 60)).padStart(2, "0")} s`;
  return `${Math.floor(m / 60)} h ${String(m % 60).padStart(2, "0")} min`;
}

// took is how long a haul or lift ran: until it finished, or until now while it runs.
export function took(from?: Timestamp, to?: Timestamp, now = Date.now()): number | undefined {
  const a = ms(from);
  if (a === undefined) return undefined;
  return (ms(to) ?? now) - a;
}

// shortId keeps a ULID's last characters, which differ between IDs made close together.
export function shortId(id: string): string {
  return id.length > 10 ? "…" + id.slice(-10) : id;
}

// pretty indents JSON for reading, or returns the text as is.
export function pretty(json: string): string {
  if (!json) return "";
  try {
    return JSON.stringify(JSON.parse(json), null, 2);
  } catch {
    return json;
  }
}

function parse(json: string): unknown {
  if (!json) return undefined;
  try {
    return JSON.parse(json);
  } catch {
    return json;
  }
}

// scalar shows one value: dates and intervals (how the SDK encodes them) in
// UTC, other objects as JSON.
function scalar(v: unknown): string {
  if (typeof v === "string") return v;
  if (v && typeof v === "object" && !Array.isArray(v)) {
    const o = v as Record<string, unknown>;
    const iv = o["$interval"] as { start?: string; end?: string } | undefined;
    if (iv && Object.keys(o).length === 1) return interval(String(iv.start), String(iv.end));
    if (typeof o["$date"] === "string") return o["$date"];
    if (typeof o["$datetime"] === "string") {
      const d = new Date(o["$datetime"]);
      return isNaN(d.getTime()) ? o["$datetime"] : utc(d).slice(0, 19) + " UTC";
    }
  }
  return JSON.stringify(v);
}

// Values shows a JSON object as name = value pairs, and anything else as text.
export function Values({ json, empty = "" }: { json: string; empty?: string }) {
  const v = parse(json);
  if (v === undefined || v === null) return empty ? <span className="dim">{empty}</span> : null;
  if (typeof v === "object" && !Array.isArray(v)) {
    const entries = Object.entries(v as Record<string, unknown>);
    if (entries.length === 0) return empty ? <span className="dim">{empty}</span> : null;
    return (
      <span className="values">
        {entries.map(([k, x]) => (
          <span key={k} className="value" title={`${k} = ${scalar(x)}`}>
            <span className="dim">{k}</span> {short(scalar(x))}
          </span>
        ))}
      </span>
    );
  }
  return <span className="value">{short(scalar(v))}</span>;
}

// short keeps chips small: an ID (a ULID, such as a pallet's) by its end, long text by its start.
function short(s: string): string {
  if (/^[0-9A-HJKMNP-TV-Z]{26}$/.test(s)) return shortId(s);
  return s.length > 40 ? s.slice(0, 37) + "…" : s;
}

// liftStatus says where a lift is, in words for people: running, waiting
// (for a worker or a free slot), retrying, reused, or how it ended.
export function liftStatus(l: Lift): string {
  if (l.reused) return "reused";
  switch (l.status) {
    case "dispatched":
    case "committing":
      return "running";
    case "scheduled":
    case "waiting":
      return "waiting";
    case "retry_wait":
      return "retrying";
    default:
      return l.status;
  }
}

export function Status({ status }: { status: string }) {
  return <span className={`status status-${status}`}>{status}</span>;
}

// steps sums up a haul's lifts in a few words.
export function steps(h: Haul): string {
  const c = h.liftCounts;
  if (!c || c.total === 0) return h.status === "running" ? "starting" : "no steps";
  const parts: string[] = [];
  const ran = c.completed - c.reused;
  if (h.status === "running" || c.completed + c.failed + c.cancelled < c.total) {
    parts.push(`${c.completed} of ${c.total} done`);
  } else {
    parts.push(`${c.total} step${c.total === 1 ? "" : "s"}`);
  }
  if (c.reused > 0) parts.push(ran === 0 && c.reused === c.total ? "all reused" : `${c.reused} reused`);
  if (c.failed > 0) parts.push(`${c.failed} failed`);
  if (c.cancelled > 0) parts.push(`${c.cancelled} cancelled`);
  return parts.join(" · ");
}

// trigger says what started a run, in words.
export function trigger(t: string): string {
  if (!t) return "";
  if (t.startsWith("haul ")) return "by another run";
  if (t.startsWith("continue ")) return "continued from an earlier run";
  if (t === "schedule") return "on its schedule";
  if (t === "subscription") return "by new data";
  return `by ${t}`;
}

export const HAUL_STATUSES = ["running", "stuck", "completed", "failed", "cancelled", "continued"];
