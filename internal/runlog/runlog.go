// Package runlog keeps what user code printed and logged while a lift ran,
// per attempt: the output people read in the web UI and `hubwagen dev`.
//
// Logs are not state. They don't go through the event log or the core: an
// attempt's lines are appended as the worker sends them, a lost batch is
// lost output, and nothing is decided by them. Each attempt keeps at most
// MaxBytes; after that its log says it was cut.
package runlog

import (
	"bufio"
	"cmp"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"
)

// MaxBytes caps one attempt's log.
const MaxBytes = 1 << 20

// Line is one line of output.
type Line struct {
	Time   time.Time `json:"t"`
	Stream string    `json:"s"`           // "stdout", "stderr" or "log"
	Level  string    `json:"l,omitempty"` // log records: "INFO", "ERROR", ...
	Logger string    `json:"n,omitempty"` // log records: the logger's name
	Text   string    `json:"m"`
}

// Attempt names whose output a line is: a lift attempt of a haul.
type Attempt struct {
	HaulID string
	Seq    int // the lift
	Number int // the attempt, from 1
}

// Entry is a line with the attempt it came from.
type Entry struct {
	Attempt
	Line
}

// Store keeps logs as files of JSON lines under a directory:
// <haul>/<seq>-<attempt>.jsonl.
type Store struct {
	dir    string
	mu     sync.Mutex
	onLine func([]Entry)
}

// Open keeps logs under dir, creating it if missing.
func Open(dir string) (*Store, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	return &Store{dir: dir}, nil
}

// OnAppend calls fn with the entries of every Append, after they are
// written (hubwagen dev prints them). fn must not block for long.
func (s *Store) OnAppend(fn func([]Entry)) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.onLine = fn
}

func (s *Store) path(a Attempt) (string, error) {
	if a.HaulID == "" || strings.ContainsAny(a.HaulID, `/\.`) || a.Seq < 1 || a.Number < 1 {
		return "", fmt.Errorf("runlog: bad attempt %+v", a)
	}
	return filepath.Join(s.dir, a.HaulID, fmt.Sprintf("%d-%d.jsonl", a.Seq, a.Number)), nil
}

const cut = "… the rest of this output was cut: an attempt keeps at most 1 MiB"

// Append adds lines to an attempt's log, dropping what goes over MaxBytes.
func (s *Store) Append(a Attempt, lines []Line) error {
	p, err := s.path(a)
	if err != nil || len(lines) == 0 {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
		return err
	}
	f, err := os.OpenFile(p, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	defer f.Close()
	st, err := f.Stat()
	if err != nil {
		return err
	}
	size := st.Size()
	if size >= MaxBytes {
		return nil // already cut
	}
	var (
		buf     []byte
		written []Entry
	)
	for _, l := range lines {
		b, err := json.Marshal(l)
		if err != nil {
			return err
		}
		if size+int64(len(buf)+len(b)+1) > MaxBytes {
			last := Line{Time: l.Time, Stream: "stderr", Text: cut}
			b, _ = json.Marshal(last)
			buf = append(append(buf, b...), '\n')
			written = append(written, Entry{a, last})
			// Pad to MaxBytes, so later appends see the log as cut.
			if pad := MaxBytes - size - int64(len(buf)); pad > 0 {
				buf = append(buf, []byte(strings.Repeat(" ", int(pad-1))+"\n")...)
			}
			break
		}
		buf = append(append(buf, b...), '\n')
		written = append(written, Entry{a, l})
	}
	if _, err := f.Write(buf); err != nil {
		return err
	}
	if s.onLine != nil {
		s.onLine(written)
	}
	return nil
}

// Read returns a haul's log lines, of one lift if seq > 0, oldest first
// (attempts in order, each attempt's lines in the order they came). With
// more than limit lines (limit > 0) it returns the last limit, and how many
// it left out.
func (s *Store) Read(haulID string, seq, limit int) (entries []Entry, skipped int, err error) {
	if haulID == "" || strings.ContainsAny(haulID, `/\.`) {
		return nil, 0, fmt.Errorf("runlog: bad haul ID %q", haulID)
	}
	files, err := os.ReadDir(filepath.Join(s.dir, haulID))
	if errors.Is(err, os.ErrNotExist) {
		return nil, 0, nil
	}
	if err != nil {
		return nil, 0, err
	}
	var attempts []Attempt
	for _, f := range files {
		name, ok := strings.CutSuffix(f.Name(), ".jsonl")
		lift, number, ok2 := strings.Cut(name, "-")
		a := Attempt{HaulID: haulID}
		var e1, e2 error
		a.Seq, e1 = strconv.Atoi(lift)
		a.Number, e2 = strconv.Atoi(number)
		if !ok || !ok2 || e1 != nil || e2 != nil || (seq > 0 && a.Seq != seq) {
			continue
		}
		attempts = append(attempts, a)
	}
	slices.SortFunc(attempts, func(x, y Attempt) int { return cmp.Or(cmp.Compare(x.Seq, y.Seq), cmp.Compare(x.Number, y.Number)) })
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, a := range attempts {
		p, _ := s.path(a)
		if err := readFile(p, func(l Line) { entries = append(entries, Entry{a, l}) }); err != nil {
			return nil, 0, err
		}
	}
	// Lifts run side by side: interleave them by time. The sort is stable,
	// so one attempt's lines keep their order.
	slices.SortStableFunc(entries, func(x, y Entry) int { return x.Time.Compare(y.Time) })
	if limit > 0 && len(entries) > limit {
		skipped = len(entries) - limit
		entries = entries[skipped:]
	}
	return entries, skipped, nil
}

func readFile(p string, fn func(Line)) error {
	f, err := os.Open(p)
	if err != nil {
		return err
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 64*1024), MaxBytes+1)
	for sc.Scan() {
		b := sc.Bytes()
		if len(strings.TrimSpace(string(b))) == 0 {
			continue // padding after a cut
		}
		var l Line
		if json.Unmarshal(b, &l) == nil {
			fn(l)
		}
	}
	return sc.Err()
}
