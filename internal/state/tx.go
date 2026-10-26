package state

import (
	"context"
	"database/sql"
	"encoding/json"
)

// Tx writes projections inside a Store.Append transaction. Every method is an
// upsert keyed by the projection's primary key, so the core can write the
// full current state of an object without tracking whether it is new.
type Tx struct {
	ctx context.Context
	tx  *sql.Tx
}

func (t *Tx) exec(query string, args ...any) error {
	_, err := t.tx.ExecContext(t.ctx, query, args...)
	return err
}

func jsonObject(m map[string]any) (string, error) {
	if len(m) == 0 {
		return "{}", nil
	}
	b, err := json.Marshal(m)
	return string(b), err
}
