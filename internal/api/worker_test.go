package api

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/durationpb"

	hubwagenv1 "github.com/alper/hubwagen/internal/api/gen/hubwagen/v1"
	"github.com/alper/hubwagen/internal/api/gen/hubwagen/v1/hubwagenv1connect"
	"github.com/alper/hubwagen/internal/blob"
	"github.com/alper/hubwagen/internal/clock"
	"github.com/alper/hubwagen/internal/core"
	"github.com/alper/hubwagen/internal/layout"
	"github.com/alper/hubwagen/internal/pallet"
	"github.com/alper/hubwagen/internal/state"
)

var ctx = context.Background()

type server struct {
	core   *core.Core
	store  *state.Store
	bucket blob.Bucket
	url    string
	client hubwagenv1connect.WorkerServiceClient
}

// start runs a core on a file-backed store and serves the worker API over a
// real HTTP listener.
func start(t *testing.T) *server {
	t.Helper()
	dir := t.TempDir()
	st, err := state.Open(filepath.Join(dir, "state.db"))
	require.NoError(t, err)
	term, err := st.StartTerm(ctx, 0)
	require.NoError(t, err)
	b, err := blob.Open(blob.FileURI(filepath.Join(dir, "bucket")))
	require.NoError(t, err)
	lay := layout.New(b.Base())
	c := core.New(core.Config{Store: st, Term: term, Clock: clock.Real{}, Layout: lay, Pallets: pallet.NewStore(b, lay)})
	runCtx, cancel := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() { done <- c.Run(runCtx) }()
	require.NoError(t, c.Barrier(ctx))

	mux := http.NewServeMux()
	path, h := NewWorkerHandler(c, 2*time.Second)
	mux.Handle(path, h)
	srv := httptest.NewServer(mux)
	t.Cleanup(func() {
		srv.Close()
		cancel()
		require.NoError(t, <-done)
		st.Close()
	})
	return &server{core: c, store: st, bucket: b, url: srv.URL,
		client: hubwagenv1connect.NewWorkerServiceClient(srv.Client(), srv.URL)}
}

// The test worker's haul: extract (memoized, writes raw), then build
// (memoized, reads extract's pallet, writes mart).
func (s *server) replay(logbook []*hubwagenv1.Event) ([]*hubwagenv1.Command, *hubwagenv1.HaulOutcome) {
	pallets := map[int32]string{}
	for _, e := range logbook {
		if e.Type == string(state.EventLiftFinished) {
			var p struct {
				Seq    int32  `json:"seq"`
				Pallet string `json:"pallet"`
			}
			if err := json.Unmarshal([]byte(e.PayloadJson), &p); err != nil {
				panic(err)
			}
			pallets[p.Seq] = p.Pallet
		}
	}
	extract := &hubwagenv1.Command{Seq: 1, Source: "app.py:3", Kind: &hubwagenv1.Command_Lift{Lift: &hubwagenv1.LiftCommand{
		Name: "extract", ArgsJson: `{"day":"2026-10-24"}`, Memo: "fingerprint",
		Output: &hubwagenv1.Output{Dataset: "raw.orders"}, Timeout: durationpb.New(time.Minute)}}}
	cmds := []*hubwagenv1.Command{extract}
	if pallets[1] == "" {
		return cmds, nil
	}
	cmds = append(cmds, &hubwagenv1.Command{Seq: 2, Source: "app.py:4", Kind: &hubwagenv1.Command_Lift{Lift: &hubwagenv1.LiftCommand{
		Name: "build", Memo: "fingerprint", Output: &hubwagenv1.Output{Dataset: "mart.revenue"},
		Inputs: []*hubwagenv1.InputRef{{Name: "orders", Pallet: pallets[1]}}}}})
	if pallets[2] == "" {
		return cmds, nil
	}
	return cmds, &hubwagenv1.HaulOutcome{Status: hubwagenv1.HaulOutcome_STATUS_COMPLETED, ResultJson: `{"pallet":"` + pallets[2] + `"}`}
}

// work runs haul tasks and lifts over the API until the haul ends, and
// returns the lifts it ran.
func (s *server) work(t *testing.T, haulID string) []string {
	t.Helper()
	var ran []string
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		hr, err := s.client.PollHaulTask(ctx, connect.NewRequest(&hubwagenv1.PollHaulTaskRequest{
			WorkerId: "w1", Docks: []string{core.DefaultDock}, Wait: durationpb.New(50 * time.Millisecond)}))
		require.NoError(t, err)
		if task := hr.Msg.Task; task != nil {
			assert.JSONEq(t, `{"day":"2026-10-24"}`, task.ParamsJson)
			cmds, outcome := s.replay(task.Logbook)
			_, err := s.client.CompleteHaulTask(ctx, connect.NewRequest(&hubwagenv1.CompleteHaulTaskRequest{
				TaskId: task.Id, BuildId: "b1", Commands: cmds, Outcome: outcome}))
			require.NoError(t, err)
		}
		lr, err := s.client.PollLiftTask(ctx, connect.NewRequest(&hubwagenv1.PollLiftTaskRequest{
			WorkerId: "w1", Docks: []string{core.DefaultDock}, Wait: durationpb.New(50 * time.Millisecond)}))
		require.NoError(t, err)
		if task := lr.Msg.Task; task != nil {
			ran = append(ran, task.Name)
			hb, err := s.client.Heartbeat(ctx, connect.NewRequest(&hubwagenv1.HeartbeatRequest{WorkerId: "w1", TaskIds: []string{task.Id}}))
			require.NoError(t, err)
			assert.Empty(t, hb.Msg.Stop)

			content := []byte(task.Name + " data\n")
			require.NoError(t, s.bucket.Put(ctx, task.Staging+"part-0.csv", content))
			sum := sha256.Sum256(content)
			_, err = s.client.CompleteLiftTask(ctx, connect.NewRequest(&hubwagenv1.CompleteLiftTaskRequest{
				AttemptId: task.Id, Status: hubwagenv1.CompleteLiftTaskRequest_STATUS_SUCCEEDED,
				ResultJson: `{"rows":1}`,
				Output: &hubwagenv1.LiftOutput{
					Files:     []*hubwagenv1.File{{Path: "part-0.csv", Size: int64(len(content)), Sha256: hex.EncodeToString(sum[:])}},
					StatsJson: `{"rows":1}`, MetaJson: `{"max_ts":"2026-10-24T23:00:00Z"}`,
				}}))
			require.NoError(t, err)
		}
		h, err := s.store.GetHaul(ctx, haulID)
		require.NoError(t, err)
		if !state.HaulOpen(h.Status) {
			return ran
		}
	}
	t.Fatalf("haul %s didn't finish", haulID)
	return nil
}

func TestHaulRunsEndToEndOverTheAPI(t *testing.T) {
	s := start(t)
	_, err := s.client.RegisterBuild(ctx, connect.NewRequest(&hubwagenv1.RegisterBuildRequest{
		BuildId: "b1", LiftVersions: map[string]string{"extract": "v1", "build": "v1"}}))
	require.NoError(t, err)

	start := func() string {
		res, err := s.core.StartHaul(ctx, core.StartHaulRequest{Tenant: "t", Name: "daily_revenue", Params: map[string]any{"day": "2026-10-24"}})
		require.NoError(t, err)
		return res.HaulID
	}
	first := start()
	assert.Equal(t, []string{"extract", "build"}, s.work(t, first))
	h, err := s.store.GetHaul(ctx, first)
	require.NoError(t, err)
	assert.Equal(t, state.HaulCompleted, h.Status)

	mart, err := s.store.LatestPallet(ctx, "t", "mart.revenue", "")
	require.NoError(t, err)
	assert.JSONEq(t, `{"pallet":"`+mart.ID+`"}`, string(h.Result))
	up, err := s.store.Upstream(ctx, mart.ID)
	require.NoError(t, err)
	assert.Len(t, up.Pallets, 2, "mart was built from raw.orders")

	// The same haul again: both lifts are reused, nothing runs.
	second := start()
	assert.Empty(t, s.work(t, second))
	h, err = s.store.GetHaul(ctx, second)
	require.NoError(t, err)
	assert.Equal(t, state.HaulCompleted, h.Status)
	assert.JSONEq(t, `{"pallet":"`+mart.ID+`"}`, string(h.Result))
}

func TestPollWaitsAndReturnsNothing(t *testing.T) {
	s := start(t)
	began := time.Now()
	res, err := s.client.PollLiftTask(ctx, connect.NewRequest(&hubwagenv1.PollLiftTaskRequest{
		WorkerId: "w1", Docks: []string{"default"}, Wait: durationpb.New(300 * time.Millisecond)}))
	require.NoError(t, err)
	assert.Nil(t, res.Msg.Task)
	assert.GreaterOrEqual(t, time.Since(began), 300*time.Millisecond)

	// The server caps the wait (2s in these tests).
	began = time.Now()
	_, err = s.client.PollHaulTask(ctx, connect.NewRequest(&hubwagenv1.PollHaulTaskRequest{
		WorkerId: "w1", Docks: []string{"default"}, Wait: durationpb.New(time.Hour)}))
	require.NoError(t, err)
	assert.Less(t, time.Since(began), 5*time.Second)
}

func TestPlainHTTPJSON(t *testing.T) {
	s := start(t)
	post := func(method, body string) (int, string) {
		resp, err := http.Post(s.url+"/hubwagen.v1.WorkerService/"+method, "application/json", strings.NewReader(body))
		require.NoError(t, err)
		defer resp.Body.Close()
		b, err := io.ReadAll(resp.Body)
		require.NoError(t, err)
		return resp.StatusCode, string(b)
	}

	code, body := post("RegisterBuild", `{"buildId":"b1","liftVersions":{"extract":"v1"}}`)
	assert.Equal(t, http.StatusOK, code)
	assert.JSONEq(t, `{}`, body)

	code, body = post("Heartbeat", `{"workerId":"w1","taskIds":["nope"]}`)
	assert.Equal(t, http.StatusOK, code)
	assert.JSONEq(t, `{"stop":["nope"]}`, body)

	code, body = post("PollLiftTask", `{"workerId":"w1","docks":["default"],"wait":"0.1s"}`)
	assert.Equal(t, http.StatusOK, code)
	assert.JSONEq(t, `{}`, body)

	code, body = post("RegisterBuild", `{"buildId":"b1","liftVersions":{"extract":"v2"}}`)
	assert.Equal(t, http.StatusBadRequest, code)
	assert.Contains(t, body, `"code":"invalid_argument"`)
}

func TestErrorsMapToConnectCodes(t *testing.T) {
	s := start(t)
	_, err := s.client.CompleteLiftTask(ctx, connect.NewRequest(&hubwagenv1.CompleteLiftTaskRequest{
		AttemptId: "nope", Status: hubwagenv1.CompleteLiftTaskRequest_STATUS_SUCCEEDED}))
	assert.Equal(t, connect.CodeNotFound, connect.CodeOf(err))

	_, err = s.client.CompleteLiftTask(ctx, connect.NewRequest(&hubwagenv1.CompleteLiftTaskRequest{AttemptId: "nope"}))
	assert.Equal(t, connect.CodeInvalidArgument, connect.CodeOf(err), "status unset")

	_, err = s.client.CompleteLiftTask(ctx, connect.NewRequest(&hubwagenv1.CompleteLiftTaskRequest{
		AttemptId: "nope", Status: hubwagenv1.CompleteLiftTaskRequest_STATUS_SUCCEEDED, ResultJson: "{not json"}))
	assert.Equal(t, connect.CodeInvalidArgument, connect.CodeOf(err))

	_, err = s.client.CompleteHaulTask(ctx, connect.NewRequest(&hubwagenv1.CompleteHaulTaskRequest{
		TaskId: "t", Commands: []*hubwagenv1.Command{{Seq: 1}}}))
	assert.Equal(t, connect.CodeInvalidArgument, connect.CodeOf(err), "a command without a kind")

	_, err = s.client.PollHaulTask(ctx, connect.NewRequest(&hubwagenv1.PollHaulTaskRequest{Docks: []string{"default"}}))
	assert.Equal(t, connect.CodeInvalidArgument, connect.CodeOf(err), "no worker ID")
}
