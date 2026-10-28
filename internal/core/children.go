package core

import (
	"bytes"
	"encoding/json"
	"fmt"
	"sort"

	"github.com/alper/hubwagen/internal/state"
)

// Child hauls and continue_as_new.
//
// A start_haul command starts another haul (the child) at once, in the
// parent's tenant, and answers in the parent's logbook: ChildStarted with
// the child's ID, or, if it can't start (no build defines it), ChildFinished
// with status failed. With Wait, the parent waits for the child: when the
// child ends, ChildFinished carries its status and result and wakes the
// parent. Cancelling or ending a parent cancels the children it waits for;
// children it doesn't wait for carry on.
//
// A haul that ends with outcome status continued (continue_as_new) is
// finished as continued, and a new haul of the same name starts with the
// outcome's params, a fresh logbook and the old haul's parent link, so a
// waiting parent gets the result of the last haul in the chain.
//
// The parent link is on the child's haul record, so a new leader needs
// nothing more than the hauls it restores.

// startChild handles a start_haul command.
func (c *Core) startChild(h *haulState, cmd Command) {
	params, _ := decodeParams(cmd.Args) // validated with the haul task result
	dock := cmd.Dock
	if dock == "" {
		dock = h.rec.Dock
	}
	res, err := c.startHaul(StartHaulRequest{
		Tenant: h.rec.Tenant, Name: cmd.Name, Dock: dock, Trigger: "haul " + h.rec.ID, Params: params,
		parent: &parentLink{id: h.rec.ID, seq: cmd.Seq, waits: cmd.Wait},
	})
	if err != nil {
		c.emitH(state.EventChildFinished, h, "", childFinishedPayload{
			Seq: cmd.Seq, Name: cmd.Name, Status: state.HaulFailed, Reason: "start_failed", Message: err.Error()})
		h.replay = true
		return
	}
	c.emitH(state.EventChildStarted, h, "", childStartedPayload{Seq: cmd.Seq, Name: cmd.Name, Haul: res.HaulID})
	if !cmd.Wait {
		h.replay = true // the answer is the child's ID; nothing to wait for
	}
}

// parentLink is how a child haul refers to the command that started it.
type parentLink struct {
	id    string
	seq   int
	waits bool
}

// childEnded tells a waiting parent that its child h ended.
func (c *Core) childEnded(h *haulState) {
	r := h.rec
	if r.ParentID == "" || !r.ParentWaits || r.Status == state.HaulContinued {
		return
	}
	p := c.hauls[r.ParentID]
	if p == nil || p.finishing {
		return
	}
	c.emitH(state.EventChildFinished, p, "", childFinishedPayload{
		Seq: r.ParentSeq, Name: r.Name, Haul: r.ID, Status: r.Status, Result: r.Result, Reason: r.Reason, Message: r.Message})
	c.wake(p)
}

// cancelChildren cancels the open children h waits for, as h ends.
func (c *Core) cancelChildren(h *haulState, why string) {
	var ids []string
	for id, ch := range c.hauls {
		if ch.rec.ParentID == h.rec.ID && ch.rec.ParentWaits {
			ids = append(ids, id)
		}
	}
	sort.Strings(ids)
	for _, id := range ids {
		c.finishHaul(c.hauls[id], state.HaulCancelled, "parent_finished", fmt.Sprintf("parent haul %s %s", h.rec.ID, why), nil)
	}
}

// continueAsNew ends h as continued and starts its successor.
func (c *Core) continueAsNew(h *haulState, params map[string]any) {
	r := h.rec
	var parent *parentLink
	if r.ParentID != "" {
		parent = &parentLink{id: r.ParentID, seq: r.ParentSeq, waits: r.ParentWaits}
	}
	res, err := c.startHaul(StartHaulRequest{
		Tenant: r.Tenant, Name: r.Name, Dock: r.Dock, Trigger: "continue " + r.ID, Params: params, parent: parent})
	if err != nil { // the haul's own name: only fails if the request is broken
		c.finishHaul(h, state.HaulFailed, "continue_failed", err.Error(), nil)
		return
	}
	h.rec.ContinuedAs = res.HaulID
	result, _ := json.Marshal(map[string]string{"continued_as": res.HaulID})
	c.finishHaul(h, state.HaulContinued, "", "", result)
}

// decodeParams reads a JSON object of haul parameters, numbers kept exact.
func decodeParams(raw json.RawMessage) (map[string]any, error) {
	if len(bytes.TrimSpace(raw)) == 0 {
		return nil, nil
	}
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	var params map[string]any
	if err := dec.Decode(&params); err != nil {
		return nil, fmt.Errorf("params must be a JSON object: %v", err)
	}
	return params, nil
}
