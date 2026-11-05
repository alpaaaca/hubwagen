package state

import (
	"context"
	"encoding/json"
	"time"
)

// Subscription events. SubscriptionStarted names the run it started and
// the data versions it started it with.
const (
	EventSubscriptionSet     EventType = "SubscriptionSet"
	EventSubscriptionRemoved EventType = "SubscriptionRemoved"
	EventSubscriptionStarted EventType = "SubscriptionStarted"
)

// Subscription starts a run when data versions are committed to its datasets.
type Subscription struct {
	Pipeline  string
	Datasets  []string
	Debounce  time.Duration // start once no data version came for this long
	MaxWait   time.Duration // or once the first data version waited this long
	MaxBatch  int           // or once this many data versions wait
	BuildID   string
	CreatedAt time.Time
	UpdatedAt time.Time
}

// SubscriptionDataVersion is a committed data version waiting for its subscription's
// run to start.
type SubscriptionDataVersion struct {
	Pipeline      string
	Tenant        string
	DataVersionID string
	AddedAt       time.Time
}

func (t *Tx) PutSubscription(v Subscription) error {
	ds, err := json.Marshal(v.Datasets)
	if err != nil {
		return err
	}
	return t.exec(`
		INSERT INTO subscriptions (run, datasets, debounce_ns, max_wait_ns, max_batch, build_id, created_at, updated_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT (run) DO UPDATE SET
			datasets = excluded.datasets, debounce_ns = excluded.debounce_ns, max_wait_ns = excluded.max_wait_ns,
			max_batch = excluded.max_batch, build_id = excluded.build_id, updated_at = excluded.updated_at`,
		v.Pipeline, string(ds), int64(v.Debounce), int64(v.MaxWait), v.MaxBatch, v.BuildID,
		formatTime(v.CreatedAt), formatTime(v.UpdatedAt))
}

// DeleteSubscription removes a subscription and the data versions waiting for it.
func (t *Tx) DeleteSubscription(run string) error {
	if err := t.exec(`DELETE FROM subscription_data_versions WHERE run = ?`, run); err != nil {
		return err
	}
	return t.exec(`DELETE FROM subscriptions WHERE run = ?`, run)
}

func (t *Tx) AddSubscriptionDataVersion(v SubscriptionDataVersion) error {
	return t.exec(`INSERT INTO subscription_data_versions (run, tenant, data_version_id, added_at) VALUES (?, ?, ?, ?)
		ON CONFLICT DO NOTHING`, v.Pipeline, v.Tenant, v.DataVersionID, formatTime(v.AddedAt))
}

// ClearSubscriptionDataVersions removes a batch once its run started.
func (t *Tx) ClearSubscriptionDataVersions(run, tenant string) error {
	return t.exec(`DELETE FROM subscription_data_versions WHERE run = ? AND tenant = ?`, run, tenant)
}

// ListSubscriptions returns every subscription, by run.
func (s *Store) ListSubscriptions(ctx context.Context) ([]Subscription, error) {
	return queryAll(ctx, s.r, func(sc interface{ Scan(...any) error }) (Subscription, error) {
		var (
			v                    Subscription
			ds, created, updated string
			debounce, maxWait    int64
			err                  error
		)
		if err = sc.Scan(&v.Pipeline, &ds, &debounce, &maxWait, &v.MaxBatch, &v.BuildID, &created, &updated); err != nil {
			return v, err
		}
		v.Debounce, v.MaxWait = time.Duration(debounce), time.Duration(maxWait)
		if err = json.Unmarshal([]byte(ds), &v.Datasets); err != nil {
			return v, err
		}
		if v.CreatedAt, err = parseTime(created); err != nil {
			return v, err
		}
		v.UpdatedAt, err = parseTime(updated)
		return v, err
	}, `SELECT run, datasets, debounce_ns, max_wait_ns, max_batch, build_id, created_at, updated_at
		FROM subscriptions ORDER BY run`)
}

// ListSubscriptionDataVersions returns the data versions waiting for their runs, in
// the order they came.
func (s *Store) ListSubscriptionDataVersions(ctx context.Context) ([]SubscriptionDataVersion, error) {
	return queryAll(ctx, s.r, func(sc interface{ Scan(...any) error }) (SubscriptionDataVersion, error) {
		var (
			v     SubscriptionDataVersion
			added string
			err   error
		)
		if err = sc.Scan(&v.Pipeline, &v.Tenant, &v.DataVersionID, &added); err != nil {
			return v, err
		}
		v.AddedAt, err = parseTime(added)
		return v, err
	}, `SELECT run, tenant, data_version_id, added_at FROM subscription_data_versions ORDER BY added_at, data_version_id`)
}
