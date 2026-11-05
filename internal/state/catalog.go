package state

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"time"
)

// The catalog: datasets, the data versions committed to them, and which data versions
// each was built from. It is a projection of run events like everything
// else; the core writes it in the same Append as the events.

// DataVersion states. Only a committed data version is readable: it can be read by
// steps, returned as a dataset's latest, and reused by memoization.
const (
	DataVersionPending   = "pending"   // recorded; the promote hasn't finished
	DataVersionCommitted = "committed" // promoted and visible
	DataVersionInvalid   = "invalid"   // invalidated (bad data); kept as a lineage record
	DataVersionExpired   = "expired"   // its data was removed by retention; kept as a lineage record
)

// Catalog events.
const (
	EventDatasetCreated         EventType = "DatasetCreated"
	EventDataVersionRecorded    EventType = "DataVersionRecorded"
	EventDataVersionCommitted   EventType = "DataVersionCommitted"
	EventDataVersionInvalidated EventType = "DataVersionInvalidated"
	EventDataVersionExpired     EventType = "DataVersionExpired"
)

type Dataset struct {
	Tenant       string
	Name         string
	PartitionKey string // "" = unpartitioned
	CreatedAt    time.Time
}

type DataVersion struct {
	ID          string
	Tenant      string
	Dataset     string
	Partition   string // "" for unpartitioned datasets
	Location    string
	ContentHash string // "" if the storage adapter has none
	Fingerprint string
	Stats       map[string]any
	Meta        map[string]any
	RunID       string // producer
	StepSeq     int
	State       string
	Reason      string
	CreatedAt   time.Time
	CommittedAt *time.Time
	RetiredAt   *time.Time
}

// DataVersionInput is one lineage edge: DataVersionID was built from InputID.
type DataVersionInput struct {
	DataVersionID string
	InputID       string
	Name          string // the step argument it was passed as
}

// PutDataset creates a dataset; its partition key is fixed at creation.
func (t *Tx) PutDataset(v Dataset) error {
	return t.exec(`
		INSERT INTO datasets (tenant, name, partition_key, created_at) VALUES (?, ?, ?, ?)
		ON CONFLICT (tenant, name) DO NOTHING`,
		v.Tenant, v.Name, v.PartitionKey, formatTime(v.CreatedAt))
}

// PutDataVersion inserts or updates a data version. What it points to is fixed at
// creation; its state and the times that go with it change.
func (t *Tx) PutDataVersion(v DataVersion) error {
	stats, err := jsonObject(v.Stats)
	if err != nil {
		return err
	}
	meta, err := jsonObject(v.Meta)
	if err != nil {
		return err
	}
	return t.exec(`
		INSERT INTO data_versions (id, tenant, dataset, partition, location, content_hash, fingerprint,
			stats_json, meta_json, run_id, step_seq, state, reason, created_at, committed_at, retired_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT (id) DO UPDATE SET
			state = excluded.state, reason = excluded.reason,
			committed_at = excluded.committed_at, retired_at = excluded.retired_at`,
		v.ID, v.Tenant, v.Dataset, v.Partition, v.Location, v.ContentHash, v.Fingerprint,
		stats, meta, v.RunID, v.StepSeq, v.State, v.Reason, formatTime(v.CreatedAt),
		formatTimePtr(v.CommittedAt), formatTimePtr(v.RetiredAt))
}

// PutDataVersionInput records a lineage edge. Edges never change, so writing one
// again is a no-op.
func (t *Tx) PutDataVersionInput(v DataVersionInput) error {
	return t.exec(`
		INSERT INTO data_version_inputs (data_version_id, input_id, name) VALUES (?, ?, ?)
		ON CONFLICT (data_version_id, input_id) DO NOTHING`,
		v.DataVersionID, v.InputID, v.Name)
}

// --- read side ---

func (s *Store) GetDataset(ctx context.Context, tenant, name string) (Dataset, error) {
	v, err := scanDataset(s.r.QueryRowContext(ctx,
		`SELECT tenant, name, partition_key, created_at FROM datasets WHERE tenant = ? AND name = ?`, tenant, name))
	if errors.Is(err, sql.ErrNoRows) {
		return v, ErrNotFound
	}
	return v, err
}

// ListDatasets returns a tenant's datasets (all tenants' if tenant is empty)
// by tenant and name.
func (s *Store) ListDatasets(ctx context.Context, tenant string) ([]Dataset, error) {
	if tenant == "" {
		return queryAll(ctx, s.r, scanDataset,
			`SELECT tenant, name, partition_key, created_at FROM datasets ORDER BY tenant, name`)
	}
	return queryAll(ctx, s.r, scanDataset,
		`SELECT tenant, name, partition_key, created_at FROM datasets WHERE tenant = ? ORDER BY name`, tenant)
}

func (s *Store) GetDataVersion(ctx context.Context, id string) (DataVersion, error) {
	v, err := scanDataVersion(s.r.QueryRowContext(ctx, `SELECT `+dataVersionColumns+` FROM data_versions WHERE id = ?`, id))
	if errors.Is(err, sql.ErrNoRows) {
		return v, ErrNotFound
	}
	return v, err
}

// LatestDataVersion returns the most recently committed data version of a dataset
// partition ("" for unpartitioned datasets). Pending, invalid and expired
// data versions are never the latest. ErrNotFound if there is none.
func (s *Store) LatestDataVersion(ctx context.Context, tenant, dataset, partition string) (DataVersion, error) {
	v, err := scanDataVersion(s.r.QueryRowContext(ctx, `
		SELECT `+dataVersionColumns+` FROM data_versions
		WHERE tenant = ? AND dataset = ? AND partition = ? AND state = 'committed'
		ORDER BY committed_at DESC, id DESC LIMIT 1`, tenant, dataset, partition))
	if errors.Is(err, sql.ErrNoRows) {
		return v, ErrNotFound
	}
	return v, err
}

// InvalidatedDataVersion returns the newest data version of a tenant built with
// fingerprint that was invalidated (not one whose promote failed), so a
// rerun can say why the earlier one wasn't reused. ErrNotFound if none.
func (s *Store) InvalidatedDataVersion(ctx context.Context, tenant, fingerprint string) (DataVersion, error) {
	v, err := scanDataVersion(s.r.QueryRowContext(ctx, `
		SELECT `+dataVersionColumns+` FROM data_versions
		WHERE tenant = ? AND fingerprint = ? AND state = 'invalid' AND reason <> 'promote_failed'
		ORDER BY created_at DESC, id DESC LIMIT 1`, tenant, fingerprint))
	if errors.Is(err, sql.ErrNoRows) {
		return v, ErrNotFound
	}
	return v, err
}

// MemoDataVersion returns the newest committed data version of a tenant built with
// fingerprint, committed at or after since (zero: any time). It is what
// memoization reuses. ErrNotFound if there is none.
func (s *Store) MemoDataVersion(ctx context.Context, tenant, fingerprint string, since time.Time) (DataVersion, error) {
	v, err := scanDataVersion(s.r.QueryRowContext(ctx, `
		SELECT `+dataVersionColumns+` FROM data_versions
		WHERE tenant = ? AND fingerprint = ? AND state = 'committed' AND committed_at >= ?
		ORDER BY committed_at DESC, id DESC LIMIT 1`, tenant, fingerprint, formatTime(since)))
	if errors.Is(err, sql.ErrNoRows) {
		return v, ErrNotFound
	}
	return v, err
}

// StepDataVersion returns the data version step (runID, seq) built: its committed one,
// else the newest (a promote that failed leaves an invalid one behind).
// ErrNotFound if it built none.
func (s *Store) StepDataVersion(ctx context.Context, runID string, seq int) (DataVersion, error) {
	v, err := scanDataVersion(s.r.QueryRowContext(ctx, `
		SELECT `+dataVersionColumns+` FROM data_versions WHERE run_id = ? AND step_seq = ?
		ORDER BY state = 'committed' DESC, created_at DESC, id DESC LIMIT 1`, runID, seq))
	if errors.Is(err, sql.ErrNoRows) {
		return v, ErrNotFound
	}
	return v, err
}

// DataVersionFilter selects data versions for ListDataVersions. Empty fields don't filter.
type DataVersionFilter struct {
	Tenant    string
	Dataset   string
	Partition *string // nil: every partition
	RunID     string  // data versions produced by this run
	States    []string
	BeforeID  string // pages backwards: only IDs (ULIDs) lower than this
	Limit     int    // default 50
}

// ListDataVersions returns data versions newest first.
func (s *Store) ListDataVersions(ctx context.Context, f DataVersionFilter) ([]DataVersion, error) {
	var (
		where []string
		args  []any
	)
	if f.Tenant != "" {
		where, args = append(where, "tenant = ?"), append(args, f.Tenant)
	}
	if f.Dataset != "" {
		where, args = append(where, "dataset = ?"), append(args, f.Dataset)
	}
	if f.RunID != "" {
		where, args = append(where, "run_id = ?"), append(args, f.RunID)
	}
	if f.Partition != nil {
		where, args = append(where, "partition = ?"), append(args, *f.Partition)
	}
	if len(f.States) > 0 {
		where = append(where, "state IN (?"+strings.Repeat(", ?", len(f.States)-1)+")")
		for _, st := range f.States {
			args = append(args, st)
		}
	}
	if f.BeforeID != "" {
		where, args = append(where, "id < ?"), append(args, f.BeforeID)
	}
	q := `SELECT ` + dataVersionColumns + ` FROM data_versions`
	if len(where) > 0 {
		q += " WHERE " + strings.Join(where, " AND ")
	}
	limit := f.Limit
	if limit <= 0 {
		limit = 50
	}
	q += " ORDER BY id DESC LIMIT ?"
	args = append(args, limit)
	return queryAll(ctx, s.r, scanDataVersion, q, args...)
}

// Partition is one partition of a dataset: its newest committed data version and
// how many committed data versions it has.
type Partition struct {
	Partition    string
	Latest       DataVersion
	DataVersions int
}

// ListPartitions returns a dataset's partitions that have a committed
// data version, in partition order, at most limit (default 100) starting after
// the partition `after`.
func (s *Store) ListPartitions(ctx context.Context, tenant, dataset, after string, limit int) ([]Partition, error) {
	if limit <= 0 {
		limit = 100
	}
	return queryAll(ctx, s.r, func(sc interface{ Scan(...any) error }) (Partition, error) {
		var v Partition
		var err error
		v.Latest, err = scanDataVersion(withExtra{sc, []any{&v.DataVersions}})
		v.Partition = v.Latest.Partition
		return v, err
	}, `SELECT `+dataVersionColumns+`, n FROM (
			SELECT *, ROW_NUMBER() OVER w AS rn, COUNT(*) OVER (PARTITION BY partition) AS n
			FROM data_versions WHERE tenant = ? AND dataset = ? AND state = 'committed' AND (? = '' OR partition > ?)
			WINDOW w AS (PARTITION BY partition ORDER BY committed_at DESC, id DESC))
		WHERE rn = 1 ORDER BY partition LIMIT ?`, tenant, dataset, after, after, limit)
}

// withExtra scans more columns after the ones its wrapped scanner asks for.
type withExtra struct {
	sc    interface{ Scan(...any) error }
	extra []any
}

func (w withExtra) Scan(dest ...any) error { return w.sc.Scan(append(dest, w.extra...)...) }

// Lineage is a data version's graph in one direction: the data versions reached and the
// edges between them, including the data version itself.
type Lineage struct {
	DataVersions []DataVersion      // ordered by ID
	Edges        []DataVersionInput // ordered by data version, then input
}

// Upstream returns everything id was built from, directly or transitively.
func (s *Store) Upstream(ctx context.Context, id string) (Lineage, error) {
	return s.lineage(ctx, id, `SELECT e.input_id FROM data_version_inputs e JOIN reach r ON e.data_version_id = r.id`)
}

// Downstream returns everything built from id, directly or transitively —
// what an invalidation of id reaches.
func (s *Store) Downstream(ctx context.Context, id string) (Lineage, error) {
	return s.lineage(ctx, id, `SELECT e.data_version_id FROM data_version_inputs e JOIN reach r ON e.input_id = r.id`)
}

func (s *Store) lineage(ctx context.Context, id, step string) (Lineage, error) {
	tx, err := s.r.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return Lineage{}, err
	}
	defer tx.Rollback()

	// UNION (not UNION ALL) visits each data version once, so shared ancestors
	// (diamonds) don't multiply.
	reach := `WITH RECURSIVE reach(id) AS (SELECT ? UNION ` + step + `) `
	var l Lineage
	if l.DataVersions, err = queryAll(ctx, tx, scanDataVersion,
		reach+`SELECT `+dataVersionColumns+` FROM data_versions WHERE id IN (SELECT id FROM reach) ORDER BY id`, id); err != nil {
		return l, err
	}
	if len(l.DataVersions) == 0 {
		return l, ErrNotFound
	}
	l.Edges, err = queryAll(ctx, tx, scanDataVersionInput,
		reach+`SELECT data_version_id, input_id, name FROM data_version_inputs
		 WHERE data_version_id IN (SELECT id FROM reach) AND input_id IN (SELECT id FROM reach)
		 ORDER BY data_version_id, input_id`, id)
	return l, err
}

func scanDataset(sc interface{ Scan(...any) error }) (Dataset, error) {
	var (
		v       Dataset
		created string
	)
	if err := sc.Scan(&v.Tenant, &v.Name, &v.PartitionKey, &created); err != nil {
		return v, err
	}
	var err error
	v.CreatedAt, err = parseTime(created)
	return v, err
}

const dataVersionColumns = `id, tenant, dataset, partition, location, content_hash, fingerprint, stats_json,
	meta_json, run_id, step_seq, state, reason, created_at, committed_at, retired_at`

func scanDataVersion(sc interface{ Scan(...any) error }) (DataVersion, error) {
	var (
		v                    DataVersion
		stats, meta, created string
		committed, retired   sql.NullString
		err                  error
	)
	if err = sc.Scan(&v.ID, &v.Tenant, &v.Dataset, &v.Partition, &v.Location, &v.ContentHash, &v.Fingerprint,
		&stats, &meta, &v.RunID, &v.StepSeq, &v.State, &v.Reason, &created, &committed, &retired); err != nil {
		return v, err
	}
	if v.Stats, err = decodeObject(stats); err != nil {
		return v, err
	}
	if v.Meta, err = decodeObject(meta); err != nil {
		return v, err
	}
	if v.CreatedAt, err = parseTime(created); err != nil {
		return v, err
	}
	if v.CommittedAt, err = parseTimePtr(committed); err != nil {
		return v, err
	}
	v.RetiredAt, err = parseTimePtr(retired)
	return v, err
}

func scanDataVersionInput(sc interface{ Scan(...any) error }) (DataVersionInput, error) {
	var v DataVersionInput
	err := sc.Scan(&v.DataVersionID, &v.InputID, &v.Name)
	return v, err
}
