// Package clock abstracts time so the scheduler core can be tested
// deterministically: production uses Real, tests use Fake and advance it.
package clock

import (
	"sort"
	"sync"
	"time"
)

type Clock interface {
	Now() time.Time
	// AfterFunc calls f in its own goroutine (Real) or synchronously from
	// Advance (Fake) once d has elapsed.
	AfterFunc(d time.Duration, f func()) Timer
}

type Timer interface {
	// Stop prevents the timer from firing. It reports whether it did.
	Stop() bool
}

// Real is the system clock.
type Real struct{}

func (Real) Now() time.Time { return time.Now() }

func (Real) AfterFunc(d time.Duration, f func()) Timer { return time.AfterFunc(d, f) }

// Fake is a manually advanced clock.
type Fake struct {
	mu     sync.Mutex
	now    time.Time
	timers []*fakeTimer
	seq    int
}

func NewFake(now time.Time) *Fake { return &Fake{now: now} }

func (f *Fake) Now() time.Time {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.now
}

func (f *Fake) AfterFunc(d time.Duration, fn func()) Timer {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.seq++
	t := &fakeTimer{clock: f, at: f.now.Add(d), fn: fn, seq: f.seq}
	f.timers = append(f.timers, t)
	return t
}

// Advance moves time forward by d, firing due timers in deadline order (ties
// in creation order). Timer functions run synchronously, without the lock
// held, so they may create new timers; those fire too if they fall due.
func (f *Fake) Advance(d time.Duration) {
	f.mu.Lock()
	target := f.now.Add(d)
	f.mu.Unlock()

	for {
		f.mu.Lock()
		sort.Slice(f.timers, func(i, j int) bool {
			if !f.timers[i].at.Equal(f.timers[j].at) {
				return f.timers[i].at.Before(f.timers[j].at)
			}
			return f.timers[i].seq < f.timers[j].seq
		})
		if len(f.timers) == 0 || f.timers[0].at.After(target) {
			f.now = target
			f.mu.Unlock()
			return
		}
		t := f.timers[0]
		f.timers = f.timers[1:]
		if t.at.After(f.now) {
			f.now = t.at
		}
		f.mu.Unlock()
		t.fn()
	}
}

// Pending reports how many timers are waiting to fire.
func (f *Fake) Pending() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.timers)
}

type fakeTimer struct {
	clock *Fake
	at    time.Time
	fn    func()
	seq   int
}

func (t *fakeTimer) Stop() bool {
	f := t.clock
	f.mu.Lock()
	defer f.mu.Unlock()
	for i, x := range f.timers {
		if x == t {
			f.timers = append(f.timers[:i], f.timers[i+1:]...)
			return true
		}
	}
	return false
}
