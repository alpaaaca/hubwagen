package log

import (
	"bytes"
	"io"
	"sync"
)

// MultiWriter splits writes on newline boundaries and fans each complete line
// to all registered sinks. Partial lines are buffered until a newline arrives.
type MultiWriter struct {
	mu    sync.Mutex
	sinks []io.Writer
	buf   bytes.Buffer
}

func (m *MultiWriter) Add(sink io.Writer) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.sinks = append(m.sinks, sink)
}

func (m *MultiWriter) Write(p []byte) (int, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.buf.Write(p)
	for {
		data := m.buf.Bytes()
		idx := bytes.IndexByte(data, '\n')
		if idx < 0 {
			break
		}
		line := make([]byte, idx+1)
		copy(line, data[:idx+1])
		m.buf.Next(idx + 1)
		for _, sink := range m.sinks {
			_, _ = sink.Write(line)
		}
	}
	return len(p), nil
}
