package state

import (
	"context"
	"encoding/json"
	"time"
)

// Subscription events. SubscriptionStarted names the haul it started and
// the pallets it started it with.
const (
	EventSubscriptionSet     EventType = "SubscriptionSet"
	EventSubscriptionRemoved EventType = "SubscriptionRemoved"
	EventSubscriptionStarted EventType = "SubscriptionStarted"
)

// Subscription starts a haul when pallets are committed to its datasets.
type Subscription struct {
	Haul      string
	Datasets  []string
	Debounce  time.Duration // start once no pallet came for this long
	MaxWait   time.Duration // or once the first pallet waited this long
	MaxBatch  int           // or once this many pallets wait
	BuildID   string
	CreatedAt time.Time
	UpdatedAt time.Time
}

// SubscriptionPallet is a committed pallet waiting for its subscription's
// haul to start.
type SubscriptionPallet struct {
	Haul     string
	Tenant   string
	PalletID string
	AddedAt  time.Time
}

func (t *Tx) PutSubscription(v Subscription) error {
	ds, err := json.Marshal(v.Datasets)
	if err != nil {
		return err
	}
	return t.exec(`
		INSERT INTO subscriptions (haul, datasets, debounce_ns, max_wait_ns, max_batch, build_id, created_at, updated_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT (haul) DO UPDATE SET
			datasets = excluded.datasets, debounce_ns = excluded.debounce_ns, max_wait_ns = excluded.max_wait_ns,
			max_batch = excluded.max_batch, build_id = excluded.build_id, updated_at = excluded.updated_at`,
		v.Haul, string(ds), int64(v.Debounce), int64(v.MaxWait), v.MaxBatch, v.BuildID,
		formatTime(v.CreatedAt), formatTime(v.UpdatedAt))
}

// DeleteSubscription removes a subscription and the pallets waiting for it.
func (t *Tx) DeleteSubscription(haul string) error {
	if err := t.exec(`DELETE FROM subscription_pallets WHERE haul = ?`, haul); err != nil {
		return err
	}
	return t.exec(`DELETE FROM subscriptions WHERE haul = ?`, haul)
}

func (t *Tx) AddSubscriptionPallet(v SubscriptionPallet) error {
	return t.exec(`INSERT INTO subscription_pallets (haul, tenant, pallet_id, added_at) VALUES (?, ?, ?, ?)
		ON CONFLICT DO NOTHING`, v.Haul, v.Tenant, v.PalletID, formatTime(v.AddedAt))
}

// ClearSubscriptionPallets removes a batch once its haul started.
func (t *Tx) ClearSubscriptionPallets(haul, tenant string) error {
	return t.exec(`DELETE FROM subscription_pallets WHERE haul = ? AND tenant = ?`, haul, tenant)
}

// ListSubscriptions returns every subscription, by haul.
func (s *Store) ListSubscriptions(ctx context.Context) ([]Subscription, error) {
	return queryAll(ctx, s.r, func(sc interface{ Scan(...any) error }) (Subscription, error) {
		var (
			v                    Subscription
			ds, created, updated string
			debounce, maxWait    int64
			err                  error
		)
		if err = sc.Scan(&v.Haul, &ds, &debounce, &maxWait, &v.MaxBatch, &v.BuildID, &created, &updated); err != nil {
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
	}, `SELECT haul, datasets, debounce_ns, max_wait_ns, max_batch, build_id, created_at, updated_at
		FROM subscriptions ORDER BY haul`)
}

// ListSubscriptionPallets returns the pallets waiting for their hauls, in
// the order they came.
func (s *Store) ListSubscriptionPallets(ctx context.Context) ([]SubscriptionPallet, error) {
	return queryAll(ctx, s.r, func(sc interface{ Scan(...any) error }) (SubscriptionPallet, error) {
		var (
			v     SubscriptionPallet
			added string
			err   error
		)
		if err = sc.Scan(&v.Haul, &v.Tenant, &v.PalletID, &added); err != nil {
			return v, err
		}
		v.AddedAt, err = parseTime(added)
		return v, err
	}, `SELECT haul, tenant, pallet_id, added_at FROM subscription_pallets ORDER BY added_at, pallet_id`)
}
