package runlog

import (
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var t0 = time.Date(2026, 10, 24, 3, 0, 0, 0, time.UTC)

func at(ms int) time.Time { return t0.Add(time.Duration(ms) * time.Millisecond) }

func texts(es []Entry) []string {
	var out []string
	for _, e := range es {
		out = append(out, e.Text)
	}
	return out
}

func TestAppendAndReadInterleavesLiftsByTime(t *testing.T) {
	s, err := Open(t.TempDir())
	require.NoError(t, err)
	var seen []Entry
	s.OnAppend(func(es []Entry) { seen = append(seen, es...) })

	a1 := Attempt{HaulID: "h1", Seq: 1, Number: 1}
	a2 := Attempt{HaulID: "h1", Seq: 2, Number: 1}
	require.NoError(t, s.Append(a1, []Line{{Time: at(0), Stream: "stdout", Text: "one"}, {Time: at(20), Stream: "stdout", Text: "three"}}))
	require.NoError(t, s.Append(a2, []Line{{Time: at(10), Stream: "log", Level: "INFO", Logger: "app", Text: "two"}}))
	require.NoError(t, s.Append(a1, []Line{{Time: at(20), Stream: "stderr", Text: "four"}})) // same time: stays after three
	require.NoError(t, s.Append(Attempt{HaulID: "h2", Seq: 1, Number: 1}, []Line{{Time: at(5), Text: "other haul"}}))

	all, skipped, err := s.Read("h1", 0, 0)
	require.NoError(t, err)
	assert.Zero(t, skipped)
	assert.Equal(t, []string{"one", "two", "three", "four"}, texts(all))
	assert.Equal(t, Line{Time: at(10), Stream: "log", Level: "INFO", Logger: "app", Text: "two"}, all[1].Line)
	assert.Equal(t, a2, all[1].Attempt)

	one, _, err := s.Read("h1", 2, 0)
	require.NoError(t, err)
	assert.Equal(t, []string{"two"}, texts(one))

	tail, skipped, err := s.Read("h1", 0, 2)
	require.NoError(t, err)
	assert.Equal(t, 2, skipped)
	assert.Equal(t, []string{"three", "four"}, texts(tail))

	none, _, err := s.Read("nope", 0, 0)
	require.NoError(t, err)
	assert.Empty(t, none)
	assert.Len(t, seen, 5)
}

func TestAnAttemptKeepsAtMostMaxBytes(t *testing.T) {
	s, err := Open(t.TempDir())
	require.NoError(t, err)
	a := Attempt{HaulID: "h1", Seq: 1, Number: 1}
	big := strings.Repeat("x", 100_000)
	for i := 0; i < 15; i++ {
		require.NoError(t, s.Append(a, []Line{{Time: at(i), Stream: "stdout", Text: big}}))
	}
	require.NoError(t, s.Append(a, []Line{{Time: at(99), Stream: "stdout", Text: "after the cut"}}))

	es, _, err := s.Read("h1", 1, 0)
	require.NoError(t, err)
	require.Len(t, es, 11, "10 lines of ~100 kB fit, then the cut")
	assert.Contains(t, es[10].Text, "was cut")
	assert.NotContains(t, texts(es), "after the cut")
}

func TestBadNamesAreRefused(t *testing.T) {
	s, err := Open(t.TempDir())
	require.NoError(t, err)
	assert.Error(t, s.Append(Attempt{HaulID: "../x", Seq: 1, Number: 1}, []Line{{Text: "x"}}))
	assert.Error(t, s.Append(Attempt{HaulID: "h", Seq: 0, Number: 1}, []Line{{Text: "x"}}))
	_, _, err = s.Read("../x", 0, 0)
	assert.Error(t, err)
}
