package core

import (
	"fmt"
	"slices"
)

// Fan-out limits. A step command may carry limits: each caps how many of the
// run's steps in a group hold a slot at once. The SDK puts the steps of one
// gather(..., max_parallel=N) in a group of their own and, with
// @pipeline(max_parallel=N), every step of the run in a run-wide group. A step
// holds a slot in each of its groups while it has an attempt queued or
// running: from startAttempt until the attempt finishes or is withdrawn.
// Retry waits and single-flight waits hold none. A step without a free slot
// waits, unqueued, in its run's throttled list, and the steps there start in
// order as slots free up.
//
// Limits are part of the recorded commands, so a new leader rebuilds them:
// dispatched attempts take their slots back first, then the rest start or
// wait as above.

// ParallelLimit caps how many steps of Group hold a slot at once.
type ParallelLimit struct {
	Group string `json:"group"`
	Max   int    `json:"max"`
}

// maxLimitsPerStep bounds the limits one command may carry.
const maxLimitsPerStep = 8

func validateLimits(limits []ParallelLimit) error {
	if len(limits) > maxLimitsPerStep {
		return fmt.Errorf("at most %d limits per step", maxLimitsPerStep)
	}
	seen := map[string]bool{}
	for j, lim := range limits {
		switch {
		case lim.Group == "":
			return fmt.Errorf("limits[%d]: a limit needs a group", j)
		case lim.Max < 1:
			return fmt.Errorf("limits[%d]: max must be at least 1", j)
		case seen[lim.Group]:
			return fmt.Errorf("limits[%d]: group %q twice", j, lim.Group)
		}
		seen[lim.Group] = true
	}
	return nil
}

// startAttempt queues the step's next attempt if its groups have room, and
// otherwise leaves it waiting for a slot.
func (c *Core) startAttempt(l *stepState) {
	if !c.takeSlots(l) {
		if !slices.Contains(l.run.throttled, l) {
			l.run.throttled = append(l.run.throttled, l)
		}
		return
	}
	c.queueStepAttempt(l)
}

// takeSlots takes a slot in each of l's groups, if every one has room.
func (c *Core) takeSlots(l *stepState) bool {
	if len(l.limits) == 0 || l.slot {
		return true
	}
	h := l.run
	for _, lim := range l.limits {
		if h.slots[lim.Group] >= lim.Max {
			return false
		}
	}
	for _, lim := range l.limits {
		h.slots[lim.Group]++
	}
	l.slot = true
	return true
}

// freeSlots gives l's slots back, once its attempt finished or was
// withdrawn, and starts throttled steps that fit now.
func (c *Core) freeSlots(l *stepState) {
	if !l.slot {
		return
	}
	l.slot = false
	h := l.run
	for _, lim := range l.limits {
		h.slots[lim.Group]--
	}
	for i := 0; i < len(h.throttled); {
		next := h.throttled[i]
		if !c.takeSlots(next) {
			i++
			continue
		}
		h.throttled = slices.Delete(h.throttled, i, i+1)
		c.queueStepAttempt(next)
	}
}

// takeSlotBack counts a restored step's dispatched attempt in its groups,
// over the limit if need be: the attempt is running, whatever the limit.
func (c *Core) takeSlotBack(l *stepState) {
	if len(l.limits) == 0 {
		return
	}
	for _, lim := range l.limits {
		l.run.slots[lim.Group]++
	}
	l.slot = true
}

// unthrottle drops a step that won't run any more from the throttled list.
func (c *Core) unthrottle(l *stepState) {
	h := l.run
	if i := slices.Index(h.throttled, l); i >= 0 {
		h.throttled = slices.Delete(h.throttled, i, i+1)
	}
}
