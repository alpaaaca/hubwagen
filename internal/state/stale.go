package state

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
)

// Staleness. A partition's latest data version is stale when what built it has
// moved on:
//
//   - an input: the dataset partition of a data version it was built from now has
//     another latest committed data version, with other content (the same content
//     would be reused, so it doesn't count);
//   - the code: the newest registered build that defines the step that
//     built it has another code version for it than the one it ran with
//     (taken from its fingerprint, else from its run's build).
//
// A rebuild of the run that built it refreshes it.

// Stale is a partition's latest data version that is out of date, and why.
type Stale struct {
	DataVersion DataVersion
	Step        string   // the step that built it
	Reasons     []string // e.g. `input raw.orders 2026-10-01 has a newer data version 01M…`
}

// StalePartitions returns the stale latest data versions of a tenant's dataset,
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
					return nil, fmt.Errorf("data version %s: %w", p.Latest.ID, err)
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

func (s *Store) staleness(ctx context.Context, p DataVersion) (Stale, error) {
	st := Stale{DataVersion: p}
	inputs, err := queryAll(ctx, s.r, scanDataVersion, `
		SELECT `+prefixed("i.", dataVersionColumns)+` FROM data_version_inputs e JOIN data_versions i ON i.id = e.input_id
		WHERE e.data_version_id = ? ORDER BY i.dataset, i.partition, i.id`, p.ID)
	if err != nil {
		return st, err
	}
	for _, in := range inputs {
		latest, err := s.LatestDataVersion(ctx, in.Tenant, in.Dataset, in.Partition)
		switch {
		case errors.Is(err, ErrNotFound):
			continue // nothing readable there now: nothing newer to build from
		case err != nil:
			return st, err
		}
		if latest.ID != in.ID && !(latest.ContentHash != "" && latest.ContentHash == in.ContentHash) {
			st.Reasons = append(st.Reasons, fmt.Sprintf("input %s %s has a newer version %s",
				in.Dataset, partitionName(in.Partition), latest.ID))
		}
	}

	step, err := s.GetStep(ctx, p.RunID, p.StepSeq)
	if errors.Is(err, ErrNotFound) {
		return st, nil
	}
	if err != nil {
		return st, err
	}
	st.Step = step.Name
	built := ""
	var fp struct {
		CodeVersion string `json:"code_version"`
	}
	if len(step.FPInputs) > 0 && json.Unmarshal(step.FPInputs, &fp) == nil {
		built = fp.CodeVersion
	}
	if built == "" {
		if err := s.r.QueryRowContext(ctx, `
			SELECT bl.code_version FROM runs h JOIN build_steps bl ON bl.build_id = h.build_id AND bl.step = ?
			WHERE h.id = ?`, step.Name, p.RunID).Scan(&built); err != nil && !errors.Is(err, sql.ErrNoRows) {
			return st, err
		}
	}
	var current string
	if err := s.r.QueryRowContext(ctx, `
		SELECT bl.code_version FROM build_steps bl JOIN builds b ON b.id = bl.build_id
		WHERE bl.step = ? ORDER BY b.registered_at DESC, b.id DESC LIMIT 1`, step.Name).Scan(&current); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return st, nil // no build defines the step any more
		}
		return st, err
	}
	if built != "" && built != current {
		st.Reasons = append(st.Reasons, fmt.Sprintf("step %s: code changed (%s → %s)", step.Name, built, current))
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
