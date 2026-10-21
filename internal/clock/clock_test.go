package clock

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestFakeFiresInDeadlineOrder(t *testing.T) {
	t0 := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	f := NewFake(t0)
	var fired []string
	f.AfterFunc(2*time.Second, func() { fired = append(fired, "b") })
	f.AfterFunc(time.Second, func() { fired = append(fired, "a") })
	f.AfterFunc(2*time.Second, func() { fired = append(fired, "b2") })
	stopped := f.AfterFunc(time.Second, func() { fired = append(fired, "never") })
	assert.True(t, stopped.Stop())
	assert.False(t, stopped.Stop())

	f.Advance(1500 * time.Millisecond)
	assert.Equal(t, []string{"a"}, fired)
	f.Advance(time.Second)
	assert.Equal(t, []string{"a", "b", "b2"}, fired, "ties fire in creation order")
	assert.Equal(t, t0.Add(2500*time.Millisecond), f.Now())
}

func TestFakeTimersCreatedWhileAdvancing(t *testing.T) {
	f := NewFake(time.Unix(0, 0))
	var at []time.Duration
	var tick func()
	tick = func() {
		at = append(at, f.Now().Sub(time.Unix(0, 0)))
		if len(at) < 5 {
			f.AfterFunc(time.Second, tick)
		}
	}
	f.AfterFunc(time.Second, tick)
	f.Advance(3 * time.Second)
	assert.Equal(t, []time.Duration{time.Second, 2 * time.Second, 3 * time.Second}, at, "fires at each deadline, stops at the target")
	assert.Equal(t, 1, f.Pending())
}
