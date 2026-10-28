package state

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
)

// Staleness. A partition's latest pallet is stale when what built it has
// moved on:
//
//   - an input: the dataset partition of a pallet it was built from now has
//     another latest committed pallet, with other content (the same content
//     would be reused, so it doesn't count);
//   - the code: the newest registered build that defines the lift that
//     built it has another code version for it than the one it ran with
//     (taken from its fingerprint, else from its haul's build).
//
// A rebuild of the haul that built it refreshes it.

// Stale is a partition's latest pallet that is out of date, and why.
type Stale struct {
	Pallet  Pallet
	Lift    string   // the lift that built it
	Reasons []string // e.g. `input raw.orders 2026-10-01 has a newer pallet 01M…`
}

// StalePartitions returns the stale latest pallets of a tenant's dataset,
// or of all its datasets when dataset is "", by dataset and partition.
func (s *Store) StalePartitions(ctx context.Context, tenant, dataset string) ([]Stale, error) {
	var datasets []string
	if dataset != "" {
		datasets = []string{dataset}
	} else {
		dss, err := s.ListDatasets(ctx, tenant)
		if err != nil {
			return nil, err
		}
		for _, d := range dss {
			datasets = append(datasets, d.Name)
		}
	}
	var out []Stale
	for _, ds := range datasets {
		after := ""
		for {
			parts, err := s.ListPartitions(ctx, tenant, ds, after, 1000)
			if err != nil {
				return nil, err
			}
			for _, p := range parts {
				st, err := s.staleness(ctx, p.Latest)
				if err != nil {
					return nil, fmt.Errorf("pallet %s: %w", p.Latest.ID, err)
				}
				if len(st.Reasons) > 0 {
					out = append(out, st)
				}
			}
			if len(parts) < 1000 {
				break
			}
			after = parts[len(parts)-1].Partition
		}
	}
	return out, nil
}

func (s *Store) staleness(ctx context.Context, p Pallet) (Stale, error) {
	st := Stale{Pallet: p}
	inputs, err := queryAll(ctx, s.r, scanPallet, `
		SELECT `+prefixed("i.", palletColumns)+` FROM pallet_inputs e JOIN pallets i ON i.id = e.input_id
		WHERE e.pallet_id = ? ORDER BY i.dataset, i.partition, i.id`, p.ID)
	if err != nil {
		return st, err
	}
	for _, in := range inputs {
		latest, err := s.LatestPallet(ctx, in.Tenant, in.Dataset, in.Partition)
		switch {
		case errors.Is(err, ErrNotFound):
			continue // nothing readable there now: nothing newer to build from
		case err != nil:
			return st, err
		}
		if latest.ID != in.ID && !(latest.ContentHash != "" && latest.ContentHash == in.ContentHash) {
			st.Reasons = append(st.Reasons, fmt.Sprintf("input %s %s has a newer pallet %s",
				in.Dataset, partitionName(in.Partition), latest.ID))
		}
	}

	lift, err := s.GetLift(ctx, p.HaulID, p.LiftSeq)
	if errors.Is(err, ErrNotFound) {
		return st, nil
	}
	if err != nil {
		return st, err
	}
	st.Lift = lift.Name
	built := ""
	var fp struct {
		CodeVersion string `json:"code_version"`
	}
	if len(lift.FPInputs) > 0 && json.Unmarshal(lift.FPInputs, &fp) == nil {
		built = fp.CodeVersion
	}
	if built == "" {
		if err := s.r.QueryRowContext(ctx, `
			SELECT bl.code_version FROM hauls h JOIN build_lifts bl ON bl.build_id = h.build_id AND bl.lift = ?
			WHERE h.id = ?`, lift.Name, p.HaulID).Scan(&built); err != nil && !errors.Is(err, sql.ErrNoRows) {
			return st, err
		}
	}
	var current string
	if err := s.r.QueryRowContext(ctx, `
		SELECT bl.code_version FROM build_lifts bl JOIN builds b ON b.id = bl.build_id
		WHERE bl.lift = ? ORDER BY b.registered_at DESC, b.id DESC LIMIT 1`, lift.Name).Scan(&current); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return st, nil // no build defines the lift any more
		}
		return st, err
	}
	if built != "" && built != current {
		st.Reasons = append(st.Reasons, fmt.Sprintf("lift %s: code changed (%s → %s)", lift.Name, built, current))
	}
	return st, nil
}

func partitionName(p string) string {
	if p == "" {
		return "(unpartitioned)"
	}
	return p
}

// prefixed qualifies a column list with a table alias.
func prefixed(alias, columns string) string {
	var out []byte
	start := true
	for i := 0; i < len(columns); i++ {
		c := columns[i]
		if start && c != ' ' && c != '\n' && c != '\t' {
			out = append(out, alias...)
			start = false
		}
		out = append(out, c)
		if c == ',' {
			start = true
		}
	}
	return string(out)
}
