package api

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"net/http"
	"net/url"
	"strings"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/durationpb"

	hubwagenv1 "github.com/alper/hubwagen/internal/api/gen/hubwagen/v1"
	"github.com/alper/hubwagen/internal/blob"
	"github.com/alper/hubwagen/internal/blob/s3fake"
	"github.com/alper/hubwagen/internal/core"
	"github.com/alper/hubwagen/internal/state"
)

func hexSum(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

// do sends one transfer; relative URLs are the server's own.
func (s *server) do(t *testing.T, method string, tr *hubwagenv1.Transfer, body []byte) (int, []byte) {
	t.Helper()
	u := tr.Url
	if strings.HasPrefix(u, "/") {
		u = s.url + u
	}
	req, err := http.NewRequest(method, u, bytes.NewReader(body))
	require.NoError(t, err)
	for k, v := range tr.Headers {
		req.Header.Set(k, v)
	}
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	got, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	return resp.StatusCode, got
}

func (s *server) upload(t *testing.T, attempt, path string, content []byte) {
	t.Helper()
	res, err := s.client.GetUploadURLs(ctx, connect.NewRequest(&hubwagenv1.GetUploadURLsRequest{AttemptId: attempt,
		Files: []*hubwagenv1.File{{Path: path, Size: int64(len(content)), Sha256: hexSum(content)}}}))
	require.NoError(t, err)
	require.Len(t, res.Msg.Files, 1)
	status, body := s.do(t, http.MethodPut, res.Msg.Files[0], content)
	require.Equal(t, http.StatusOK, status, string(body))
}

// checkInputs reads every input of the lift task through download URLs:
// build reads what extract wrote.
func (s *server) checkInputs(t *testing.T, task *hubwagenv1.LiftTask) {
	t.Helper()
	for _, in := range task.Inputs {
		res, err := s.client.GetDownloadURLs(ctx, connect.NewRequest(&hubwagenv1.GetDownloadURLsRequest{AttemptId: task.Id, Pallet: in.Pallet}))
		require.NoError(t, err)
		var m struct {
			Pallet string `json:"pallet"`
		}
		require.NoError(t, json.Unmarshal([]byte(res.Msg.ManifestJson), &m))
		assert.Equal(t, in.Pallet, m.Pallet)
		require.Len(t, res.Msg.Files, 1)
		assert.Equal(t, "part-0.csv", res.Msg.Files[0].Path)
		status, body := s.do(t, http.MethodGet, res.Msg.Files[0], nil)
		require.Equal(t, http.StatusOK, status)
		assert.Equal(t, "extract data\n", string(body))
	}
}

// A worker that shares no filesystem with the server runs the haul twice,
// moving data only through URLs: the second run reuses both lifts.
func TestLiftDataThroughURLs(t *testing.T) {
	run := func(t *testing.T, s *server) {
		s.viaURLs = true
		_, err := s.client.RegisterBuild(ctx, connect.NewRequest(&hubwagenv1.RegisterBuildRequest{
			BuildId: "b1", Hauls: []string{"daily_revenue"}, LiftVersions: map[string]string{"extract": "v1", "build": "v1"}}))
		require.NoError(t, err)
		for i, want := range [][]string{{"extract", "build"}, nil} {
			res, err := s.core.StartHaul(ctx, core.StartHaulRequest{Tenant: "t", Name: "daily_revenue", Params: map[string]any{"day": "2026-10-24"}})
			require.NoError(t, err)
			assert.Equal(t, want, s.work(t, res.HaulID), "run %d", i+1)
			h, err := s.store.GetHaul(ctx, res.HaulID)
			require.NoError(t, err)
			assert.Equal(t, state.HaulCompleted, h.Status)
		}
	}
	t.Run("file bucket, through the server", func(t *testing.T) { run(t, start(t)) })
	t.Run("s3 bucket, presigned", func(t *testing.T) {
		srv, endpoint := s3fake.Start(t, "hwb")
		b, err := blob.OpenS3(ctx, blob.S3Config{URL: "s3://hwb/prod", Endpoint: endpoint, PathStyle: true, AccessKeyID: "k", SecretAccessKey: "s"})
		require.NoError(t, err)
		s := startWith(t, b, false)
		run(t, s)
		var data int
		for _, k := range srv.Keys("hwb") {
			if strings.HasSuffix(k, "/part-0.csv") {
				data++
			}
		}
		assert.Equal(t, 2, data, "both pallets' files are in the store")
	})
	t.Run("s3 bucket, through the server", func(t *testing.T) {
		_, endpoint := s3fake.Start(t, "hwb")
		b, err := blob.OpenS3(ctx, blob.S3Config{URL: "s3://hwb", Endpoint: endpoint, PathStyle: true, AccessKeyID: "k", SecretAccessKey: "s"})
		require.NoError(t, err)
		run(t, startWith(t, b, true))
	})
}

// The URLs reach only a running attempt's staging and its inputs, and the
// server's own URLs can't be altered or used after they expire.
func TestTransferURLsAreScoped(t *testing.T) {
	s := start(t)
	_, err := s.client.RegisterBuild(ctx, connect.NewRequest(&hubwagenv1.RegisterBuildRequest{
		BuildId: "b1", Hauls: []string{"daily_revenue"}, LiftVersions: map[string]string{"extract": "v1", "build": "v1"}}))
	require.NoError(t, err)
	_, err = s.core.StartHaul(ctx, core.StartHaulRequest{Tenant: "t", Name: "daily_revenue", Params: map[string]any{"day": "2026-10-24"}})
	require.NoError(t, err)
	hr, err := s.client.PollHaulTask(ctx, connect.NewRequest(&hubwagenv1.PollHaulTaskRequest{WorkerId: "w1", Docks: []string{core.DefaultDock}, Wait: durationpb.New(time.Second)}))
	require.NoError(t, err)
	cmds, _ := s.replay(hr.Msg.Task.Logbook)
	_, err = s.client.CompleteHaulTask(ctx, connect.NewRequest(&hubwagenv1.CompleteHaulTaskRequest{TaskId: hr.Msg.Task.Id, BuildId: "b1", Commands: cmds}))
	require.NoError(t, err)
	lr, err := s.client.PollLiftTask(ctx, connect.NewRequest(&hubwagenv1.PollLiftTaskRequest{WorkerId: "w1", Docks: []string{core.DefaultDock}, Wait: durationpb.New(time.Second)}))
	require.NoError(t, err)
	task := lr.Msg.Task
	require.NotNil(t, task)
	assert.Equal(t, s.bucket.Base(), task.Bucket)
	assert.Equal(t, "bucket-id", task.BucketId)

	content := []byte("a,b\n")
	upload := func(path string) (*hubwagenv1.Transfer, error) {
		r, err := s.client.GetUploadURLs(ctx, connect.NewRequest(&hubwagenv1.GetUploadURLsRequest{AttemptId: task.Id,
			Files: []*hubwagenv1.File{{Path: path, Size: int64(len(content)), Sha256: hexSum(content)}}}))
		if err != nil {
			return nil, err
		}
		return r.Msg.Files[0], nil
	}
	for _, bad := range []string{"../escape.csv", "/abs.csv", "a/../../b", "", `a\b`} {
		_, err := upload(bad)
		assert.Equal(t, connect.CodeInvalidArgument, connect.CodeOf(err), bad)
	}
	_, err = s.client.GetUploadURLs(ctx, connect.NewRequest(&hubwagenv1.GetUploadURLsRequest{AttemptId: "not-running"}))
	assert.Equal(t, connect.CodeNotFound, connect.CodeOf(err))
	_, err = s.client.GetDownloadURLs(ctx, connect.NewRequest(&hubwagenv1.GetDownloadURLsRequest{AttemptId: task.Id, Pallet: "not-an-input"}))
	assert.Equal(t, connect.CodeNotFound, connect.CodeOf(err))

	tr, err := upload("dir/part 0.csv")
	require.NoError(t, err)
	assert.True(t, strings.HasPrefix(tr.Url, DataPath), "the server's own URL, relative: %s", tr.Url)

	status, _ := s.do(t, http.MethodPut, tr, []byte("a,c\n"))
	assert.Equal(t, http.StatusBadRequest, status, "other bytes of the same size")
	status, _ = s.do(t, http.MethodPut, tr, []byte("a,b\nmore"))
	assert.Equal(t, http.StatusBadRequest, status, "more bytes")
	status, _ = s.do(t, http.MethodGet, tr, nil)
	assert.Equal(t, http.StatusMethodNotAllowed, status, "a PUT URL is no GET URL")
	tampered := strings.Replace(tr.Url, "part%200.csv", "part%201.csv", 1)
	require.NotEqual(t, tr.Url, tampered)
	status, _ = s.do(t, http.MethodPut, &hubwagenv1.Transfer{Url: tampered}, content)
	assert.Equal(t, http.StatusForbidden, status, "another path")
	_, err = s.bucket.Get(ctx, task.Staging+"dir/part 0.csv")
	assert.ErrorIs(t, err, blob.ErrNotExist, "no bad upload became an object")

	status, body := s.do(t, http.MethodPut, tr, content)
	require.Equal(t, http.StatusOK, status, string(body))
	got, err := s.bucket.Get(ctx, task.Staging+"dir/part 0.csv")
	require.NoError(t, err)
	assert.Equal(t, content, got)

	u, err := url.Parse(tr.Url)
	require.NoError(t, err)
	q := u.Query()
	q.Set("exp", "1")
	u.RawQuery = q.Encode()
	status, _ = s.do(t, http.MethodPut, &hubwagenv1.Transfer{Url: u.String()}, content)
	assert.Equal(t, http.StatusForbidden, status, "the expiry is signed")

	// Once the attempt has finished, it gets no more URLs.
	_, err = s.client.CompleteLiftTask(ctx, connect.NewRequest(&hubwagenv1.CompleteLiftTaskRequest{
		AttemptId: task.Id, Status: hubwagenv1.CompleteLiftTaskRequest_STATUS_FAILED, Reason: "x", NoRetry: true}))
	require.NoError(t, err)
	_, err = upload("late.csv")
	assert.Equal(t, connect.CodeNotFound, connect.CodeOf(err))
}
