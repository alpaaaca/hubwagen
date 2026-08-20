package tests

import (
	"bytes"
	"fmt"
	"strings"
	"sync"
	"testing"

	"github.com/alper/hubwagen/internal/log"
	"github.com/stretchr/testify/assert"
)

func TestMultiWriter_SingleLine(t *testing.T) {
	var buf bytes.Buffer
	mw := &log.MultiWriter{}
	mw.Add(&buf)

	fmt.Fprint(mw, "hello\n")

	assert.Equal(t, "hello\n", buf.String())
}

func TestMultiWriter_MultipleLines(t *testing.T) {
	var buf bytes.Buffer
	mw := &log.MultiWriter{}
	mw.Add(&buf)

	fmt.Fprint(mw, "line1\nline2\nline3\n")

	lines := strings.Split(strings.TrimRight(buf.String(), "\n"), "\n")
	assert.Equal(t, []string{"line1", "line2", "line3"}, lines)
}

func TestMultiWriter_PartialLineBuffered(t *testing.T) {
	var buf bytes.Buffer
	mw := &log.MultiWriter{}
	mw.Add(&buf)

	fmt.Fprint(mw, "partial")
	assert.Empty(t, buf.String(), "partial line should be buffered, not flushed")

	fmt.Fprint(mw, "-complete\n")
	assert.Equal(t, "partial-complete\n", buf.String())
}

func TestMultiWriter_ChunkedWrite(t *testing.T) {
	var buf bytes.Buffer
	mw := &log.MultiWriter{}
	mw.Add(&buf)

	msg := "chunk-by-chunk\n"
	for _, b := range []byte(msg) {
		fmt.Fprintf(mw, "%c", b)
	}

	assert.Equal(t, msg, buf.String())
}

func TestMultiWriter_MultipleSinks(t *testing.T) {
	var buf1, buf2 bytes.Buffer
	mw := &log.MultiWriter{}
	mw.Add(&buf1)
	mw.Add(&buf2)

	fmt.Fprint(mw, "broadcast\n")

	assert.Equal(t, "broadcast\n", buf1.String())
	assert.Equal(t, "broadcast\n", buf2.String())
}

func TestMultiWriter_ConcurrentWrites(t *testing.T) {
	var buf bytes.Buffer
	mw := &log.MultiWriter{}
	mw.Add(&buf)

	const goroutines = 20
	var wg sync.WaitGroup
	wg.Add(goroutines)
	for i := range goroutines {
		go func(n int) {
			defer wg.Done()
			fmt.Fprintf(mw, "line-%d\n", n)
		}(i)
	}
	wg.Wait()

	lines := strings.Split(strings.TrimRight(buf.String(), "\n"), "\n")
	assert.Len(t, lines, goroutines, "expected one line per goroutine, no interleaving")
	t.Log("concurrent output (order is non-deterministic):\n", buf.String())
}

func TestMultiWriter_WithShellExecutor(t *testing.T) {
	var sink1, sink2 bytes.Buffer
	mw := &log.MultiWriter{}
	mw.Add(&sink1)
	mw.Add(&sink2)

	fmt.Fprint(mw, "step output line 1\n")
	fmt.Fprint(mw, "step output line 2\n")

	assert.Equal(t, sink1.String(), sink2.String(), "both sinks should receive identical output")
	assert.Equal(t, 2, strings.Count(sink1.String(), "\n"))
	t.Log("sink1:\n", sink1.String())
}
