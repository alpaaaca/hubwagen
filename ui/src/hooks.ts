import { useCallback, useEffect, useRef, useState } from "react";

// How often lists and the open haul refresh while the page is visible.
export const POLL_MS = 2000;

export type Loaded<T> = { data?: T; error?: string; loading: boolean; reload: () => void };

// usePoll calls load now and every `every` ms (0: once) while the tab is
// visible, keeping the last data while a reload runs. Changing deps reloads.
export function usePoll<T>(load: () => Promise<T>, deps: unknown[], every = POLL_MS): Loaded<T> {
  const [state, setState] = useState<{ data?: T; error?: string; loading: boolean }>({ loading: true });
  const loadRef = useRef(load);
  loadRef.current = load;
  const gen = useRef(0);

  const reload = useCallback(() => {
    const mine = ++gen.current;
    setState((s) => ({ ...s, loading: true }));
    loadRef.current().then(
      (data) => mine === gen.current && setState({ data, loading: false }),
      (e: unknown) => mine === gen.current && setState((s) => ({ ...s, error: message(e), loading: false })),
    );
  }, []);

  useEffect(() => {
    reload();
    if (!every) return;
    const t = setInterval(() => document.visibilityState !== "hidden" && reload(), every);
    return () => clearInterval(t);
  }, [...deps, every]); // eslint-disable-line react-hooks/exhaustive-deps

  return { ...state, reload };
}

export function message(e: unknown): string {
  if (e && typeof e === "object" && "rawMessage" in e) return String((e as { rawMessage: string }).rawMessage);
  return e instanceof Error ? e.message : String(e);
}
