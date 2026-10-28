package state

import (
	"context"
	"time"
)

// Retention keeps, per partition of a dataset, the latest committed pallet
// and those among the newest Versions or committed less than Age ago; the
// rest of its committed pallets can expire. A zero field is an unset rule.
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

// inUse is every pallet something still running may read: pallets built by
// open hauls, pallets open hauls got as lift results (reused ones too) or
// from latest_pallet, inputs of unfinished lifts, and pallets waiting to
// start a subscription's haul.
const inUse = `
	WITH open_hauls AS (SELECT id FROM hauls WHERE status IN ('running', 'stuck')),
	in_use(id) AS (
		SELECT id FROM pallets WHERE haul_id IN (SELECT id FROM open_hauls)
		UNION SELECT json_extract(payload, '$.pallet') FROM events
			WHERE type = 'LiftFinished' AND haul_id IN (SELECT id FROM open_hauls)
		UNION SELECT json_extract(payload, '$.pallet.id') FROM events
			WHERE type = 'QueryAnswered' AND haul_id IN (SELECT id FROM open_hauls)
		UNION SELECT json_extract(j.value, '$.pallet') FROM lifts l, json_each(l.inputs_json) j
			WHERE l.inputs_json <> '' AND l.status NOT IN ('completed', 'failed', 'cancelled')
		UNION SELECT pallet_id FROM subscription_pallets)
`

// ExpiryCandidates returns up to limit committed pallets of r's dataset that
// its retention no longer keeps and nothing running reads, oldest first.
func (s *Store) ExpiryCandidates(ctx context.Context, r Retention, now time.Time, limit int) ([]Pallet, error) {
	cutoff := now.Add(-r.Age)
	return queryAll(ctx, s.r, scanPallet, inUse+`
		SELECT `+palletColumns+` FROM (
			SELECT *, ROW_NUMBER() OVER (PARTITION BY tenant, partition ORDER BY committed_at DESC, id DESC) AS rn
			FROM pallets WHERE dataset = ? AND state = 'committed')
		WHERE rn > 1
			AND (? = 0 OR rn > ?)
			AND (? = 0 OR committed_at < ?)
			AND id NOT IN (SELECT id FROM in_use WHERE id IS NOT NULL)
		ORDER BY committed_at, id LIMIT ?`,
		r.Dataset, r.Versions, r.Versions, int64(r.Age), formatTime(cutoff), limit)
}
