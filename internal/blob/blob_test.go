package blob

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/oklog/ulid/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/alper/hubwagen/internal/blob/s3fake"
)

var ctx = context.Background()

func open(t *testing.T) (Bucket, string) {
	t.Helper()
	dir := filepath.Join(t.TempDir(), "bucket with space")
	b, err := Open(FileURI(dir))
	require.NoError(t, err)
	return b, dir
}

// openFake opens an s3:// bucket on s3fake.
func openFake(t *testing.T) (Bucket, *s3fake.Server) {
	t.Helper()
	srv, endpoint := s3fake.Start(t, "hwb")
	b, err := OpenS3(ctx, S3Config{URL: "s3://hwb/some/prefix", Endpoint: endpoint, PathStyle: true,
		AccessKeyID: "key", SecretAccessKey: "secret"})
	require.NoError(t, err)
	return b, srv
}

// openReal opens a real S3-compatible store named by HUBWAGEN_TEST_S3
// (http(s)://key:secret@host:port/bucket, e.g. a local MinIO), under a
// fresh prefix; the test is skipped without it.
func openReal(t *testing.T) Bucket {
	t.Helper()
	raw := os.Getenv("HUBWAGEN_TEST_S3")
	if raw == "" {
		t.Skip("HUBWAGEN_TEST_S3 not set (http://key:secret@host:port/bucket of an S3-compatible store)")
	}
	u, err := url.Parse(raw)
	require.NoError(t, err)
	secret, _ := u.User.Password()
	b, err := OpenS3(ctx, S3Config{
		URL:      "s3://" + strings.Trim(u.Path, "/") + "/hubwagen-test/" + ulid.Make().String(),
		Endpoint: u.Scheme + "://" + u.Host, PathStyle: true, AccessKeyID: u.User.Username(), SecretAccessKey: secret})
	require.NoError(t, err)
	t.Cleanup(func() { _ = b.DeletePrefix(ctx, b.Base()+"/") })
	return b
}

// each runs f on every kind of bucket.
func each(t *testing.T, f func(t *testing.T, b Bucket)) {
	t.Run("file", func(t *testing.T) { b, _ := open(t); f(t, b) })
	t.Run("s3fake", func(t *testing.T) { b, _ := openFake(t); f(t, b) })
	t.Run("s3", func(t *testing.T) { f(t, openReal(t)) })
}

func TestPutGetList(t *testing.T) {
	each(t, func(t *testing.T, b Bucket) {
		base := b.Base()

		require.NoError(t, b.Put(ctx, base+"/logs/r1/a/1/000001.log", []byte("one")))
		require.NoError(t, b.Put(ctx, base+"/logs/r1/a/1/000000.log", []byte("zero")))
		require.NoError(t, b.Put(ctx, base+"/logs/r1/a/1/000000.log", []byte("zero again")))
		require.NoError(t, b.Put(ctx, base+"/logs/r1x/elsewhere", []byte("not under r1/")))

		got, err := b.Get(ctx, base+"/logs/r1/a/1/000000.log")
		require.NoError(t, err)
		assert.Equal(t, "zero again", string(got))

		list, err := b.List(ctx, base+"/logs/r1/")
		require.NoError(t, err)
		assert.Equal(t, []string{base + "/logs/r1/a/1/000000.log", base + "/logs/r1/a/1/000001.log"}, list)

		empty, err := b.List(ctx, base+"/nothing/")
		require.NoError(t, err)
		assert.Empty(t, empty)

		_, err = b.Get(ctx, base+"/missing")
		assert.ErrorIs(t, err, ErrNotExist)
		_, err = b.GetStream(ctx, base+"/missing")
		assert.ErrorIs(t, err, ErrNotExist)
	})
}

func TestStreams(t *testing.T) {
	each(t, func(t *testing.T, b Bucket) {
		uri := b.Base() + "/data/big.bin"
		data := bytes.Repeat([]byte("0123456789"), 100_000)
		require.NoError(t, b.PutStream(ctx, uri, bytes.NewReader(data), int64(len(data))))
		r, err := b.GetStream(ctx, uri)
		require.NoError(t, err)
		got, err := io.ReadAll(r)
		require.NoError(t, err)
		require.NoError(t, r.Close())
		assert.Equal(t, data, got)

		assert.Error(t, b.PutStream(ctx, b.Base()+"/data/short", strings.NewReader("abc"), 10), "fewer bytes than promised")
		_, err = b.Get(ctx, b.Base()+"/data/short")
		assert.ErrorIs(t, err, ErrNotExist, "nothing is left of a failed write")
	})
}

func TestListPaginates(t *testing.T) {
	each(t, func(t *testing.T, b Bucket) {
		var want []string
		for i := range 250 { // s3fake pages by 100
			uri := fmt.Sprintf("%s/many/f%03d", b.Base(), i)
			require.NoError(t, b.Put(ctx, uri, []byte{byte(i)}))
			want = append(want, uri)
		}
		got, err := b.List(ctx, b.Base()+"/many/")
		require.NoError(t, err)
		assert.Equal(t, want, got)
		require.NoError(t, b.DeletePrefix(ctx, b.Base()+"/many/"))
		got, err = b.List(ctx, b.Base()+"/many/")
		require.NoError(t, err)
		assert.Empty(t, got)
	})
}

func TestLocalPath(t *testing.T) {
	b, dir := open(t)
	p, ok := b.LocalPath(b.Base() + "/data/x/")
	assert.True(t, ok)
	assert.Equal(t, filepath.Join(dir, "data", "x"), p)

	s, _ := openFake(t)
	_, ok = s.LocalPath(s.Base() + "/data/x/")
	assert.False(t, ok)
}

func TestEscapedSegmentsMapConsistently(t *testing.T) {
	b, dir := open(t)
	uri := b.Base() + "/data/t/p/a/o/t%2Fp%2Fkey:1/gen-1/_COMMITTED"
	require.NoError(t, b.Put(ctx, uri, []byte("x")))
	_, err := os.Stat(filepath.Join(dir, "data/t/p/a/o/t/p/key:1/gen-1/_COMMITTED"))
	assert.NoError(t, err)

	s, srv := openFake(t)
	require.NoError(t, s.Put(ctx, s.Base()+"/data/t/p/a/o/t%2Fp%2Fkey:1/a+b c/_COMMITTED", []byte("x")))
	assert.Equal(t, []string{"some/prefix/data/t/p/a/o/t/p/key:1/a+b c/_COMMITTED"}, srv.Keys("hwb"))

	each(t, func(t *testing.T, b Bucket) {
		uri := b.Base() + "/data/t%2Fp/a+b c/f.json"
		require.NoError(t, b.Put(ctx, uri, []byte("x")))
		got, err := b.Get(ctx, uri)
		require.NoError(t, err)
		assert.Equal(t, "x", string(got))
		list, err := b.List(ctx, b.Base()+"/data/t%2Fp/")
		require.NoError(t, err)
		assert.Equal(t, []string{b.Base() + "/data/t%2Fp/a+b c/f.json"}, list, "listed names are decoded, like the file bucket's")
	})
}

func TestRejectsURIsOutsideTheBucket(t *testing.T) {
	each(t, func(t *testing.T, b Bucket) {
		for _, uri := range []string{"file:///etc/passwd", "s3://other/x", b.Base() + "/../escape", b.Base() + "x/y"} {
			assert.Error(t, b.Put(ctx, uri, nil), uri)
		}
	})
	_, err := Open("s3://bucket")
	assert.ErrorContains(t, err, "OpenS3")
	_, err = Open("gs://bucket")
	assert.ErrorContains(t, err, "not supported")
	_, err = Open("file://relative/path")
	assert.Error(t, err)
	for _, u := range []string{"s3://", "file:///x", "s3://b?x=1"} {
		_, err = OpenS3(ctx, S3Config{URL: u, Endpoint: "http://127.0.0.1:1"})
		assert.ErrorContains(t, err, "want s3://bucket", u)
	}
	_, err = OpenS3(ctx, S3Config{URL: "s3://b", Endpoint: "http://127.0.0.1:1", AccessKeyID: "only"})
	assert.ErrorContains(t, err, "both")
}

func TestPutIfAbsentHasExactlyOneWinner(t *testing.T) {
	each(t, func(t *testing.T, b Bucket) {
		uri := b.Base() + "/commits/t/p/a/k/gen-1.json"

		var (
			wg      sync.WaitGroup
			mu      sync.Mutex
			winners []int
		)
		for i := 0; i < 50; i++ {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				err := b.PutIfAbsent(ctx, uri, []byte(fmt.Sprintf("attempt-%d", i)))
				if err == nil {
					mu.Lock()
					winners = append(winners, i)
					mu.Unlock()
					return
				}
				assert.ErrorIs(t, err, ErrExists)
			}(i)
		}
		wg.Wait()
		require.Len(t, winners, 1)

		got, err := b.Get(ctx, uri)
		require.NoError(t, err)
		assert.Equal(t, fmt.Sprintf("attempt-%d", winners[0]), string(got))

		list, err := b.List(ctx, b.Base()+"/commits/")
		require.NoError(t, err)
		assert.Len(t, list, 1, "no temp files left behind")
	})
}

func TestStoresWithoutConditionalWritesAreRefused(t *testing.T) {
	srv, endpoint := s3fake.Start(t, "hwb")
	srv.NoConditionalWrites = true
	_, err := OpenS3(ctx, S3Config{URL: "s3://hwb", Endpoint: endpoint, PathStyle: true, AccessKeyID: "k", SecretAccessKey: "s"})
	assert.ErrorContains(t, err, "ignores conditional writes")
	assert.Empty(t, srv.Keys("hwb"), "the probe cleans up")

	_, err = OpenS3(ctx, S3Config{URL: "s3://nope", Endpoint: endpoint, PathStyle: true, AccessKeyID: "k", SecretAccessKey: "s"})
	assert.Error(t, err, "a bucket that doesn't exist")
}

func TestDeletePrefix(t *testing.T) {
	each(t, func(t *testing.T, b Bucket) {
		base := b.Base()
		require.NoError(t, b.Put(ctx, base+"/lifts/t/h1/1/a1/part-0.csv", []byte("x")))
		require.NoError(t, b.Put(ctx, base+"/lifts/t/h1/1/a1/sub/part-1.csv", []byte("y")))
		require.NoError(t, b.Put(ctx, base+"/lifts/t/h1/1/a2/part-0.csv", []byte("z")))
		require.NoError(t, b.Put(ctx, base+"/lifts/t/h1/1/a10/part-0.csv", []byte("z")))

		require.NoError(t, b.DeletePrefix(ctx, base+"/lifts/t/h1/1/a1/"))
		left, err := b.List(ctx, base+"/lifts/")
		require.NoError(t, err)
		assert.Equal(t, []string{base + "/lifts/t/h1/1/a10/part-0.csv", base + "/lifts/t/h1/1/a2/part-0.csv"}, left)

		require.NoError(t, b.DeletePrefix(ctx, base+"/nothing/"), "removing nothing is fine")
		assert.Error(t, b.DeletePrefix(ctx, base+"/lifts"), "needs a directory URI")
		assert.Error(t, b.DeletePrefix(ctx, base+"/"), "never the whole bucket")
		assert.Error(t, b.DeletePrefix(ctx, "file:///elsewhere/"), "never outside the bucket")
	})
}

// Presigned URLs work with a plain HTTP client and no credentials; a PUT
// URL takes only the bytes it was signed for.
func TestPresignedURLs(t *testing.T) {
	presigned := func(t *testing.T, b Bucket) {
		p, ok := b.(Presigner)
		require.True(t, ok)
		uri := b.Base() + "/data/t/ds/_/a1/part 0.csv"
		data := []byte("day,revenue\n2026-10-01,12\n")

		put, err := p.PresignPut(ctx, uri, sha256Hex(data), time.Minute)
		require.NoError(t, err)
		resp := send(t, http.MethodPut, put, []byte("other bytes"))
		assert.NotEqual(t, http.StatusOK, resp.StatusCode, "the store checks the sha256")
		resp = send(t, http.MethodPut, Transfer{URL: put.URL}, data)
		assert.NotEqual(t, http.StatusOK, resp.StatusCode, "the checksum header is part of the signature")
		resp = send(t, http.MethodPut, put, data)
		require.Equal(t, http.StatusOK, resp.StatusCode)

		get, err := p.PresignGet(ctx, uri, time.Minute)
		require.NoError(t, err)
		resp = send(t, http.MethodGet, get, nil)
		require.Equal(t, http.StatusOK, resp.StatusCode)
		body, _ := io.ReadAll(resp.Body)
		assert.Equal(t, data, body)

		got, err := b.Get(ctx, uri)
		require.NoError(t, err)
		assert.Equal(t, data, got)
	}
	t.Run("s3fake", func(t *testing.T) {
		b, srv := openFake(t)
		presigned(t, b)
		get, err := b.(Presigner).PresignGet(ctx, b.Base()+"/data/t/ds/_/a1/part 0.csv", time.Minute)
		require.NoError(t, err)
		srv.Now = func() time.Time { return time.Now().Add(2 * time.Minute) }
		assert.Equal(t, http.StatusForbidden, send(t, http.MethodGet, get, nil).StatusCode, "expired")
	})
	t.Run("s3", func(t *testing.T) { presigned(t, openReal(t)) })
	b, _ := open(t)
	_, ok := b.(Presigner)
	assert.False(t, ok, "file buckets don't presign; the server serves their data")
}

func TestPublicEndpoint(t *testing.T) {
	_, endpoint := s3fake.Start(t, "hwb")
	b, err := OpenS3(ctx, S3Config{URL: "s3://hwb", Endpoint: endpoint, PublicEndpoint: "https://s3.example.com",
		PathStyle: true, AccessKeyID: "k", SecretAccessKey: "s"})
	require.NoError(t, err)
	tr, err := b.(Presigner).PresignGet(ctx, b.Base()+"/x", time.Minute)
	require.NoError(t, err)
	assert.True(t, strings.HasPrefix(tr.URL, "https://s3.example.com/hwb/x?"), tr.URL)
}

func sha256Hex(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

func send(t *testing.T, method string, tr Transfer, body []byte) *http.Response {
	t.Helper()
	req, err := http.NewRequest(method, tr.URL, bytes.NewReader(body))
	require.NoError(t, err)
	for k, v := range tr.Headers {
		req.Header.Set(k, v)
	}
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	t.Cleanup(func() { resp.Body.Close() })
	return resp
}
