package taskexec

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/alper/hubwagen/internal/blob"
)

const (
	maxLineBytes = 64 << 10  // longer lines are split
	maxBuffered  = 256 << 20 // unshipped log bytes kept while the bucket is unreachable
)

// logShipper writes an attempt's output to the bucket as numbered chunks of
// newline-delimited JSON, every flush interval or chunk size, whichever
// comes first, and an index.json at the end.
type logShipper struct {
	bucket   blob.Bucket
	prefix   string
	task     string
	echo     io.Writer
	interval time.Duration
	chunk    int

	mu      sync.Mutex
	buf     bytes.Buffer
	seq     int
	lines   int
	bytes   int64
	dropped int
	lastErr error

	stop chan struct{}
	done chan struct{}
}

type logLine struct {
	TS     time.Time `json:"ts"`
	Stream string    `json:"stream"`
	Line   string    `json:"line"`
}

type logIndex struct {
	Chunks       int    `json:"chunks"`
	Lines        int    `json:"lines"`
	Bytes        int64  `json:"bytes"`
	DroppedLines int    `json:"dropped_lines,omitempty"`
	Status       string `json:"status"`
	ShipError    string `json:"ship_error,omitempty"`
}

func newLogShipper(b blob.Bucket, prefix, task string, echo io.Writer, interval time.Duration, chunk int) *logShipper {
	if interval == 0 {
		interval = 2 * time.Second
	}
	if chunk == 0 {
		chunk = 1 << 20
	}
	l := &logShipper{bucket: b, prefix: prefix, task: task, echo: echo, interval: interval, chunk: chunk,
		stop: make(chan struct{}), done: make(chan struct{})}
	go l.loop()
	return l
}

func (l *logShipper) loop() {
	defer close(l.done)
	t := time.NewTicker(l.interval)
	defer t.Stop()
	for {
		select {
		case <-l.stop:
			return
		case <-t.C:
			l.mu.Lock()
			l.flushLocked()
			l.mu.Unlock()
		}
	}
}

// read consumes one output stream until EOF.
func (l *logShipper) read(stream string, r io.Reader) {
	br := bufio.NewReaderSize(r, maxLineBytes)
	for {
		line, isPrefix, err := br.ReadLine()
		if len(line) > 0 || (err == nil && !isPrefix) {
			l.add(stream, string(line))
		}
		if err != nil {
			return
		}
	}
}

// system records a line from hubwagen itself (start failures, kills).
func (l *logShipper) system(format string, args ...any) {
	l.add("hubwagen", fmt.Sprintf(format, args...))
}

func (l *logShipper) add(stream, text string) {
	b, _ := json.Marshal(logLine{TS: time.Now().UTC(), Stream: stream, Line: text})
	l.mu.Lock()
	defer l.mu.Unlock()
	l.buf.Write(b)
	l.buf.WriteByte('\n')
	l.lines++
	if l.echo != nil {
		fmt.Fprintf(l.echo, "[%s] %s\n", l.task, text)
	}
	if l.buf.Len() >= l.chunk {
		l.flushLocked()
	}
}

func (l *logShipper) flushLocked() {
	if l.buf.Len() == 0 {
		return
	}
	uri := fmt.Sprintf("%s%06d.log", l.prefix, l.seq)
	// Background context: logs must still ship while the attempt is being
	// cancelled.
	if err := l.bucket.Put(context.Background(), uri, l.buf.Bytes()); err != nil {
		l.lastErr = err
		if l.buf.Len() > maxBuffered {
			l.dropped += bytes.Count(l.buf.Bytes(), []byte{'\n'})
			l.buf.Reset()
		}
		return
	}
	l.seq++
	l.bytes += int64(l.buf.Len())
	l.buf.Reset()
	l.lastErr = nil
}

// close ships what is left and writes index.json.
func (l *logShipper) close(status string) {
	close(l.stop)
	<-l.done
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.dropped > 0 {
		b, _ := json.Marshal(logLine{TS: time.Now().UTC(), Stream: "hubwagen",
			Line: fmt.Sprintf("%d log lines dropped while the bucket was unreachable", l.dropped)})
		l.buf.Write(append(b, '\n'))
	}
	l.flushLocked()
	idx := logIndex{Chunks: l.seq, Lines: l.lines, Bytes: l.bytes, DroppedLines: l.dropped, Status: status}
	if l.lastErr != nil {
		idx.ShipError = l.lastErr.Error()
	}
	b, _ := json.Marshal(idx)
	_ = l.bucket.Put(context.Background(), l.prefix+"index.json", b)
}
