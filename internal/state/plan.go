package state

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
)

// LatestRunWithParams is the newest run of pipeline name with exactly
// params, a completed one if there is one; ErrNotFound if none ran with
// them. Params compare as stored (canonical JSON, keys sorted).
func (s *Store) LatestRunWithParams(ctx context.Context, tenant, name string, params map[string]any) (Run, error) {
	p, err := jsonObject(params)
	if err != nil {
		return Run{}, err
	}
	v, err := scanRun(s.r.QueryRowContext(ctx, `SELECT `+runColumns+` FROM runs
		WHERE tenant = ? AND name = ? AND params_json = ?
		ORDER BY status = 'completed' DESC, id DESC LIMIT 1`, tenant, name, p))
	if errors.Is(err, sql.ErrNoRows) {
		return Run{}, ErrNotFound
	}
	return v, err
}

// StepCodeVersions returns, for a step of a run, the code version it ran
// with (what went into its fingerprint, else its run's build) and the
// current one (currentCodeVersion; "" if no build defines it any more).
func (s *Store) StepCodeVersions(ctx context.Context, step Step, buildID string) (then, now string, err error) {
	var fp struct {
		CodeVersion string `json:"code_version"`
	}
	if len(step.FPInputs) > 0 && json.Unmarshal(step.FPInputs, &fp) == nil {
		then = fp.CodeVersion
	}
	if then == "" {
		err = s.r.QueryRowContext(ctx, `SELECT code_version FROM build_steps WHERE build_id = ? AND step = ?`,
			buildID, step.Name).Scan(&then)
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			return "", "", err
		}
	}
	now, err = s.currentCodeVersion(ctx, step.Name)
	return then, now, err
}

// currentCodeVersion is the code version step runs with now: that of a
// live project's current deployment that defines it, else of the newest
// registered build that does ("" if none). Not simply the newest build: a
// deploy of earlier code (v3 the same as v1) runs a build registered
// before the one it replaces.
func (s *Store) currentCodeVersion(ctx context.Context, step string) (string, error) {
	var v string
	err := s.r.QueryRowContext(ctx, `
		SELECT bl.code_version FROM build_steps bl JOIN builds b ON b.id = bl.build_id
		LEFT JOIN projects p ON p.deleted_at IS NULL AND json_extract(p.deployment, '$.build') = bl.build_id
		WHERE bl.step = ?
		ORDER BY p.name IS NOT NULL DESC, b.registered_at DESC, b.id DESC LIMIT 1`, step).Scan(&v)
	if errors.Is(err, sql.ErrNoRows) {
		return "", nil
	}
	return v, err
}
