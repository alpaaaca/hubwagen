package blob

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var ctx = context.Background()

func open(t *testing.T) (Bucket, string) {
	t.Helper()
	dir := filepath.Join(t.TempDir(), "bucket with space")
	b, err := Open(FileURI(dir))
	require.NoError(t, err)
	return b, dir
}

func TestPutGetList(t *testing.T) {
	b, dir := open(t)
	base := b.Base()

	require.NoError(t, b.Put(ctx, base+"/logs/r1/a/1/000001.log", []byte("one")))
	require.NoError(t, b.Put(ctx, base+"/logs/r1/a/1/000000.log", []byte("zero")))
	require.NoError(t, b.Put(ctx, base+"/logs/r1/a/1/000000.log", []byte("zero again")))

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

	p, ok := b.LocalPath(base + "/data/x/")
	assert.True(t, ok)
	assert.Equal(t, filepath.Join(dir, "data", "x"), p)
}

func TestEscapedSegmentsMapConsistently(t *testing.T) {
	b, dir := open(t)
	uri := b.Base() + "/data/t/p/a/o/t%2Fp%2Fkey:1/gen-1/_COMMITTED"
	require.NoError(t, b.Put(ctx, uri, []byte("x")))
	_, err := os.Stat(filepath.Join(dir, "data/t/p/a/o/t/p/key:1/gen-1/_COMMITTED"))
	assert.NoError(t, err)
	got, err := b.Get(ctx, uri)
	require.NoError(t, err)
	assert.Equal(t, "x", string(got))
}

func TestRejectsURIsOutsideTheBucket(t *testing.T) {
	b, _ := open(t)
	for _, uri := range []string{"file:///etc/passwd", b.Base() + "/../escape", b.Base() + "x/y"} {
		assert.Error(t, b.Put(ctx, uri, nil), uri)
	}
	_, err := Open("s3://bucket")
	assert.ErrorContains(t, err, "not supported yet")
	_, err = Open("file://relative/path")
	assert.Error(t, err)
}

func TestPutIfAbsentHasExactlyOneWinner(t *testing.T) {
	b, _ := open(t)
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
}
