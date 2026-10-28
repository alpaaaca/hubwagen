package state

import (
	"context"
	"time"
)

// Schedule events. They carry a tenant but no haul ID: they belong to the
// schedule, and ScheduleFired names the haul it started.
const (
	EventScheduleSet     EventType = "ScheduleSet"
	EventScheduleRemoved EventType = "ScheduleRemoved"
	EventScheduleFired   EventType = "ScheduleFired"
	// EventRetentionSet: a build declared (or, keep unset, removed) a
	// dataset's retention.
	EventRetentionSet EventType = "RetentionSet"
)

// Schedule is a haul's schedule and its cursor.
type Schedule struct {
	Tenant    string
	Haul      string
	Cron      string
	Timezone  string // IANA; "" is UTC
	Interval  string // hourly|daily|weekly|monthly|cron
	CatchUp   string // skip|latest|all
	BuildID   string // the build whose code declared it
	Cursor    time.Time
	CreatedAt time.Time
	UpdatedAt time.Time
}

func (t *Tx) PutSchedule(v Schedule) error {
	return t.exec(`
		INSERT INTO schedules (tenant, haul, cron, timezone, interval, catch_up, build_id, cursor, created_at, updated_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT (tenant, haul) DO UPDATE SET
			cron = excluded.cron, timezone = excluded.timezone, interval = excluded.interval,
			catch_up = excluded.catch_up, build_id = excluded.build_id, cursor = excluded.cursor,
			updated_at = excluded.updated_at`,
		v.Tenant, v.Haul, v.Cron, v.Timezone, v.Interval, v.CatchUp, v.BuildID, formatTime(v.Cursor),
		formatTime(v.CreatedAt), formatTime(v.UpdatedAt))
}

func (t *Tx) DeleteSchedule(tenant, haul string) error {
	return t.exec(`DELETE FROM schedules WHERE tenant = ? AND haul = ?`, tenant, haul)
}

// ListSchedules returns every schedule, by tenant and haul.
func (s *Store) ListSchedules(ctx context.Context) ([]Schedule, error) {
	return queryAll(ctx, s.r, scanSchedule, `
		SELECT tenant, haul, cron, timezone, interval, catch_up, build_id, cursor, created_at, updated_at
		FROM schedules ORDER BY tenant, haul`)
}

func scanSchedule(sc interface{ Scan(...any) error }) (Schedule, error) {
	var (
		v                        Schedule
		cursor, created, updated string
		err                      error
	)
	if err = sc.Scan(&v.Tenant, &v.Haul, &v.Cron, &v.Timezone, &v.Interval, &v.CatchUp, &v.BuildID,
		&cursor, &created, &updated); err != nil {
		return v, err
	}
	if v.Cursor, err = parseTime(cursor); err != nil {
		return v, err
	}
	if v.CreatedAt, err = parseTime(created); err != nil {
		return v, err
	}
	v.UpdatedAt, err = parseTime(updated)
	return v, err
}
