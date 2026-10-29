import { useApi } from "./api";
import type { Pipeline, Schedule, Subscription } from "./gen/hubwagen/v1/haul_pb";
import { usePoll } from "./hooks";

export type Pipelines = {
  // title is what to call a haul: its title in code, else its name.
  title: (name: string) => string;
  info: (name: string) => Pipeline | undefined;
  // The hauls the registered builds define, by name.
  names: string[];
  schedule: (name: string) => Schedule | undefined;
  subscription: (name: string) => Subscription | undefined;
};

// usePipelines loads the pipelines the registered builds define, with their
// titles, schedules and subscriptions; they change only when new code
// registers, so it reloads rarely.
export function usePipelines(): Pipelines {
  const api = useApi();
  const res = usePoll(() => api.hauls.listPipelines({}), [], 15000);
  const triggers = usePoll(() => api.hauls.listSchedules({}), [], 15000);
  const byName = new Map((res.data?.pipelines ?? []).map((p) => [p.name, p]));
  return {
    title: (name) => byName.get(name)?.title || name,
    info: (name) => byName.get(name),
    names: [...byName.keys()],
    schedule: (name) => triggers.data?.schedules.find((s) => s.haul === name),
    subscription: (name) => triggers.data?.subscriptions.find((s) => s.haul === name),
  };
}
