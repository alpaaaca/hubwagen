import { useApi } from "./api";
import type { Pipeline } from "./gen/hubwagen/v1/haul_pb";
import { usePoll } from "./hooks";

export type Pipelines = {
  // title is what to call a haul: its title in code, else its name.
  title: (name: string) => string;
  info: (name: string) => Pipeline | undefined;
};

// usePipelines loads the pipelines the registered builds define, with their
// titles; they change only when new code registers, so it reloads rarely.
export function usePipelines(): Pipelines {
  const api = useApi();
  const res = usePoll(() => api.hauls.listPipelines({}), [], 15000);
  const byName = new Map((res.data?.pipelines ?? []).map((p) => [p.name, p]));
  return {
    title: (name) => byName.get(name)?.title || name,
    info: (name) => byName.get(name),
  };
}
