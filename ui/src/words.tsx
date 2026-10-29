// What hubwagen's ideas mean, in a sentence each, for the hints next to
// the words people meet in the UI. The engine's own names (haul, lift,
// pallet) stay out of sight: a pipeline, a run, a step, a version.
export const WORDS = {
  pipeline: "Your code that describes the work: which steps run, in what order, with which inputs.",
  run: "One execution of a pipeline, for some inputs (such as a day).",
  step: "One function call in a pipeline that does real work: reads, computes, writes. What it writes is saved as a new version of a dataset.",
  reused:
    "hubwagen remembers what each step produced, with the inputs and code that produced it. When a step comes again with the same inputs and the same code, it reuses that instead of running again.",
  version:
    "Each time a step writes data, it becomes a new version of a dataset. A version is never changed afterwards, so you can always see what a run read and wrote.",
  dataset: "A named table or file, such as raw.orders, that gets a new version each time a step writes it (per day, for example).",
  inputs: "The values a run was started with, such as the day it is for.",
  history: "What happened in this run, in order: each step queued, started, done or reused, retries and waits.",
  schedule: "When the pipeline runs on its own (a cron expression), and which slice of data (interval) each run covers.",
} as const;

export type Word = keyof typeof WORDS;

// Hint is a small "?" next to a word that says what it means, on hover or focus.
export function Hint({ word }: { word: Word }) {
  return (
    <span className="hint" tabIndex={0} role="note" aria-label={`${word}: ${WORDS[word]}`} title={WORDS[word]}>
      ?
    </span>
  );
}
