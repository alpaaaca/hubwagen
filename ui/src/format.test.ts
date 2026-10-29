import { timestampFromDate } from "@bufbuild/protobuf/wkt";
import { expect, test } from "vitest";

import { clock, interval, when } from "./format";

// Whatever the browser's timezone, times are shown in UTC and say so.
test("times are UTC", () => {
  const t = timestampFromDate(new Date("2026-10-28T23:30:05.123Z"));
  expect(when(t)).toBe("2026-10-28 23:30:05 UTC");
  expect(clock(t)).toBe("23:30:05.123");
  expect(clock(t, 1)).toBe("23:30:05.1");
  expect(clock(t, 0)).toBe("23:30:05");
});

test("intervals are UTC: a day, days, or times", () => {
  expect(interval("2026-10-28T00:00:00Z", "2026-10-29T00:00:00Z")).toBe("2026-10-28");
  expect(interval("2026-10-01T00:00:00Z", "2026-10-08T00:00:00Z")).toBe("2026-10-01 – 2026-10-07");
  expect(interval("2026-10-28T13:00:00Z", "2026-10-28T14:00:00Z")).toBe("2026-10-28 13:00 – 14:00 UTC");
  // A Berlin day is not a UTC day: it shows as the UTC times it covers.
  expect(interval("2026-10-28T00:00:00+01:00", "2026-10-29T00:00:00+01:00")).toBe("2026-10-27 23:00 – 2026-10-28 23:00 UTC");
});
