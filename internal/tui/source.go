package tui

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/alper/hubwagen/internal/core"
	"github.com/alper/hubwagen/internal/devmode"
	"github.com/alper/hubwagen/internal/spec"
	"github.com/alper/hubwagen/internal/state"
)

// Source is everything the dashboard reads and does. EngineSource serves it
// from an in-process dev engine; the control-plane API (M3) will be another
// implementation, so the screens never touch the store or the core directly.
type Source interface {
	Pipelines(ctx context.Context) ([]PipelineInfo, error)
	Runs(ctx context.Context, tenant, pipeline string, limit int) ([]state.Run, error)
	Tasks(ctx context.Context, runID string) ([]TaskInfo, error)
	// Logs returns an attempt's log lines so far, from its logs URI.
	Logs(ctx context.Context, logsURI string) ([]string, error)
	Trigger(ctx context.Context, tenant, pipeline string) (core.TriggerResult, error)
}

// PipelineInfo is one row of the pipeline list: a published pipeline, or a
// file that failed to load (Err set, Name is the file name).
type PipelineInfo struct {
	Tenant   string
	Name     string
	Err      string
	Schedule string // "" for manual-only
	Paused   bool
	NextRun  *time.Time
	Tasks    []TaskShape // the current version's graph
}

func (p PipelineInfo) Key() string {
	if p.Tenant == "" {
		return p.Name
	}
	return p.Tenant + "/" + p.Name
}

type TaskShape struct {
	ID        string
	DependsOn []string
}

// TaskInfo is a task run plus what the screens need from its newest attempt.
type TaskInfo struct {
	state.TaskRun
	LogsURI string // newest attempt's logs; "" if never dispatched
	Reason  string // newest attempt's outcome reason
}

// EngineSource implements Source over a dev-mode engine. Loader (optional)
// adds files that failed to load as invalid rows.
type EngineSource struct {
	Engine *devmode.Engine
	Loader *devmode.Loader
}

func (s EngineSource) Pipelines(ctx context.Context) ([]PipelineInfo, error) {
	pipes, err := s.Engine.Store.ListPipelines(ctx, "")
	if err != nil {
		return nil, err
	}
	now := time.Now()
	out := make([]PipelineInfo, 0, len(pipes))
	for _, p := range pipes {
		info := PipelineInfo{Tenant: p.Tenant, Name: p.Name, Paused: p.Paused}
		ver, err := s.Engine.Store.GetVersion(ctx, p.CurrentVersion)
		if err != nil {
			return nil, err
		}
		sp, err := spec.Decode(ver.SpecJSON)
		if err != nil {
			info.Err = err.Error()
			out = append(out, info)
			continue
		}
		for _, t := range sp.Tasks {
			info.Tasks = append(info.Tasks, TaskShape{ID: t.ID, DependsOn: t.DependsOn})
		}
		if sc := sp.Schedule; sc != nil {
			info.Schedule = sc.Cron
			if sc.Timezone != "UTC" {
				info.Schedule += " " + sc.Timezone
			}
			if cal, err := spec.NewCalendar(sc); err == nil && !p.Paused {
				next := cal.Next(now)
				info.NextRun = &next
			}
		}
		out = append(out, info)
	}
	if s.Loader != nil {
		for _, f := range s.Loader.Files() {
			if f.Err != "" {
				out = append(out, PipelineInfo{Name: filepath.Base(f.Path), Err: f.Err})
			}
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Key() < out[j].Key() })
	return out, nil
}

func (s EngineSource) Runs(ctx context.Context, tenant, pipeline string, limit int) ([]state.Run, error) {
	return s.Engine.Store.ListRuns(ctx, state.RunFilter{Tenant: tenant, Pipeline: pipeline, Limit: limit})
}

func (s EngineSource) Tasks(ctx context.Context, runID string) ([]TaskInfo, error) {
	trs, err := s.Engine.Store.ListTaskRuns(ctx, runID)
	if err != nil {
		return nil, err
	}
	atts, err := s.Engine.Store.ListAttempts(ctx, runID, "")
	if err != nil {
		return nil, err
	}
	newest := map[string]state.Attempt{} // attempts come ordered by task, number
	for _, a := range atts {
		newest[a.TaskID] = a
	}
	out := make([]TaskInfo, len(trs))
	for i, tr := range trs {
		a := newest[tr.TaskID]
		out[i] = TaskInfo{TaskRun: tr, LogsURI: a.LogsURI, Reason: a.Reason}
	}
	return out, nil
}

func (s EngineSource) Logs(ctx context.Context, logsURI string) ([]string, error) {
	return ReadLogs(ctx, s.Engine.Bucket, logsURI)
}

func (s EngineSource) Trigger(ctx context.Context, tenant, pipeline string) (core.TriggerResult, error) {
	return s.Engine.Core.Trigger(ctx, core.TriggerRequest{Tenant: tenant, Pipeline: pipeline, Trigger: "manual", By: "tui"})
}

// bucketReader is the part of blob.Bucket ReadLogs needs.
type bucketReader interface {
	List(ctx context.Context, prefix string) ([]string, error)
	Get(ctx context.Context, uri string) ([]byte, error)
}

// ReadLogs reads every log chunk under logsURI (see docs/task-contract.md)
// and renders one line per log line: stdout as is, other streams prefixed.
func ReadLogs(ctx context.Context, b bucketReader, logsURI string) ([]string, error) {
	if logsURI == "" {
		return nil, nil
	}
	uris, err := b.List(ctx, logsURI)
	if err != nil {
		return nil, err
	}
	var out []string
	for _, u := range uris {
		if !strings.HasSuffix(u, ".log") {
			continue
		}
		data, err := b.Get(ctx, u)
		if err != nil {
			return nil, err
		}
		sc := bufio.NewScanner(bytes.NewReader(data))
		sc.Buffer(nil, 1<<20)
		for sc.Scan() {
			var l struct {
				Stream string `json:"stream"`
				Line   string `json:"line"`
			}
			if json.Unmarshal(sc.Bytes(), &l) != nil {
				continue
			}
			if l.Stream != "stdout" {
				l.Line = "[" + l.Stream + "] " + l.Line
			}
			out = append(out, l.Line)
		}
	}
	return out, nil
}
