package core

import (
	"fmt"
	"slices"
)

// Fan-out limits. A lift command may carry limits: each caps how many of the
// haul's lifts in a group hold a slot at once. The SDK puts the lifts of one
// gather(..., max_parallel=N) in a group of their own and, with
// @haul(max_parallel=N), every lift of the haul in a haul-wide group. A lift
// holds a slot in each of its groups while it has an attempt queued or
// running: from startAttempt until the attempt finishes or is withdrawn.
// Retry waits and single-flight waits hold none. A lift without a free slot
// waits, unqueued, in its haul's throttled list, and the lifts there start in
// order as slots free up.
//
// Limits are part of the recorded commands, so a new leader rebuilds them:
// dispatched attempts take their slots back first, then the rest start or
// wait as above.

// ParallelLimit caps how many lifts of Group hold a slot at once.
type ParallelLimit struct {
	Group string `json:"group"`
	Max   int    `json:"max"`
}

// maxLimitsPerLift bounds the limits one command may carry.
const maxLimitsPerLift = 8

func validateLimits(limits []ParallelLimit) error {
	if len(limits) > maxLimitsPerLift {
		return fmt.Errorf("at most %d limits per lift", maxLimitsPerLift)
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

// startAttempt queues the lift's next attempt if its groups have room, and
// otherwise leaves it waiting for a slot.
func (c *Core) startAttempt(l *liftState) {
	if !c.takeSlots(l) {
		if !slices.Contains(l.haul.throttled, l) {
			l.haul.throttled = append(l.haul.throttled, l)
		}
		return
	}
	c.queueLiftAttempt(l)
}

// takeSlots takes a slot in each of l's groups, if every one has room.
func (c *Core) takeSlots(l *liftState) bool {
	if len(l.limits) == 0 || l.slot {
		return true
	}
	h := l.haul
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
// withdrawn, and starts throttled lifts that fit now.
func (c *Core) freeSlots(l *liftState) {
	if !l.slot {
		return
	}
	l.slot = false
	h := l.haul
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
		c.queueLiftAttempt(next)
	}
}

// takeSlotBack counts a restored lift's dispatched attempt in its groups,
// over the limit if need be: the attempt is running, whatever the limit.
func (c *Core) takeSlotBack(l *liftState) {
	if len(l.limits) == 0 {
		return
	}
	for _, lim := range l.limits {
		l.haul.slots[lim.Group]++
	}
	l.slot = true
}

// unthrottle drops a lift that won't run any more from the throttled list.
func (c *Core) unthrottle(l *liftState) {
	h := l.haul
	if i := slices.Index(h.throttled, l); i >= 0 {
		h.throttled = slices.Delete(h.throttled, i, i+1)
	}
}
