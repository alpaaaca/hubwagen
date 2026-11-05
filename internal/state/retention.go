package state

import (
	"context"
	"time"
)

// Retention keeps, per partition of a dataset, the latest committed data version
// and those among the newest Versions or committed less than Age ago; the
// rest of its committed data versions can expire. A zero field is an unset rule.
type Retention struct {
	Dataset   string
	Versions  int
	Age       time.Duration
	BuildID   string
	UpdatedAt time.Time
}

func (t *Tx) PutRetention(v Retention) error {
	return t.exec(`
		INSERT INTO retention (dataset, keep_versions, keep_ns, build_id, updated_at) VALUES (?, ?, ?, ?, ?)
		ON CONFLICT (dataset) DO UPDATE SET keep_versions = excluded.keep_versions, keep_ns = excluded.keep_ns,
			build_id = excluded.build_id, updated_at = excluded.updated_at`,
		v.Dataset, v.Versions, int64(v.Age), v.BuildID, formatTime(v.UpdatedAt))
}

func (t *Tx) DeleteRetention(dataset string) error {
	return t.exec(`DELETE FROM retention WHERE dataset = ?`, dataset)
}

// ListRetention returns every dataset's retention, by dataset.
func (s *Store) ListRetention(ctx context.Context) ([]Retention, error) {
	return queryAll(ctx, s.r, func(sc interface{ Scan(...any) error }) (Retention, error) {
		var (
			v       Retention
			age     int64
			updated string
			err     error
		)
		if err = sc.Scan(&v.Dataset, &v.Versions, &age, &v.BuildID, &updated); err != nil {
			return v, err
		}
		v.Age = time.Duration(age)
		v.UpdatedAt, err = parseTime(updated)
		return v, err
	}, `SELECT dataset, keep_versions, keep_ns, build_id, updated_at FROM retention ORDER BY dataset`)
}

// inUse is every data version something still running may read: data versions built by
// open runs, data versions open runs got as step results (reused ones too) or
// from latest_version, inputs of unfinished steps, and data versions waiting to
// start a subscription's run.
const inUse = `
	WITH open_runs AS (SELECT id FROM runs WHERE status IN ('running', 'stuck')),
	in_use(id) AS (
		SELECT id FROM data_versions WHERE run_id IN (SELECT id FROM open_runs)
		UNION SELECT json_extract(payload, '$.data_version') FROM events
			WHERE type = 'StepFinished' AND run_id IN (SELECT id FROM open_runs)
		UNION SELECT json_extract(payload, '$.data_version.id') FROM events
			WHERE type = 'QueryAnswered' AND run_id IN (SELECT id FROM open_runs)
		UNION SELECT json_extract(j.value, '$.data_version') FROM steps l, json_each(l.inputs_json) j
			WHERE l.inputs_json <> '' AND l.status NOT IN ('completed', 'failed', 'cancelled')
		UNION SELECT data_version_id FROM subscription_data_versions)
`

// ExpiryCandidates returns up to limit committed data versions of r's dataset that
// its retention no longer keeps and nothing running reads, oldest first.
func (s *Store) ExpiryCandidates(ctx context.Context, r Retention, now time.Time, limit int) ([]DataVersion, error) {
	cutoff := now.Add(-r.Age)
	return queryAll(ctx, s.r, scanDataVersion, inUse+`
		SELECT `+dataVersionColumns+` FROM (
			SELECT *, ROW_NUMBER() OVER (PARTITION BY tenant, partition ORDER BY committed_at DESC, id DESC) AS rn
			FROM data_versions WHERE dataset = ? AND state = 'committed')
		WHERE rn > 1
			AND (? = 0 OR rn > ?)
			AND (? = 0 OR committed_at < ?)
			AND id NOT IN (SELECT id FROM in_use WHERE id IS NOT NULL)
		ORDER BY committed_at, id LIMIT ?`,
		r.Dataset, r.Versions, r.Versions, int64(r.Age), formatTime(cutoff), limit)
}
