package core

import (
	"bytes"
	"encoding/json"
	"fmt"
	"sort"

	"github.com/alper/moksha/internal/state"
)

// Child runs and continue_as_new.
//
// A start_run command starts another run (the child) at once, in the
// parent's tenant, and answers in the parent's event log: ChildStarted with
// the child's ID, or, if it can't start (no build defines it), ChildFinished
// with status failed. With Wait, the parent waits for the child: when the
// child ends, ChildFinished carries its status and result and wakes the
// parent. Cancelling or ending a parent cancels the children it waits for;
// children it doesn't wait for carry on.
//
// A run that ends with outcome status continued (continue_as_new) is
// finished as continued, and a new run of the same name starts with the
// outcome's params, a fresh event log and the old run's parent link, so a
// waiting parent gets the result of the last run in the chain.
//
// The parent link is on the child's run record, so a new leader needs
// nothing more than the runs it restores.

// startChild handles a start_run command.
func (c *Core) startChild(h *runState, cmd Command) {
	params, _ := decodeParams(cmd.Args) // validated with the run task result
	queue := cmd.Queue
	if queue == "" {
		queue = h.rec.Queue
	}
	res, err := c.startRun(StartRunRequest{
		Tenant: h.rec.Tenant, Name: cmd.Name, Queue: queue, Trigger: "run " + h.rec.ID, Params: params, Reuse: h.rec.Reuse,
		parent: &parentLink{id: h.rec.ID, seq: cmd.Seq, waits: cmd.Wait},
	})
	if err != nil {
		c.emitH(state.EventChildFinished, h, "", childFinishedPayload{
			Seq: cmd.Seq, Name: cmd.Name, Status: state.RunFailed, Reason: "start_failed", Message: err.Error()})
		h.replay = true
		return
	}
	c.emitH(state.EventChildStarted, h, "", childStartedPayload{Seq: cmd.Seq, Name: cmd.Name, Run: res.RunID})
	if !cmd.Wait {
		h.replay = true // the answer is the child's ID; nothing to wait for
	}
}

// parentLink is how a child run refers to the command that started it.
type parentLink struct {
	id    string
	seq   int
	waits bool
}

// childEnded tells a waiting parent that its child h ended.
func (c *Core) childEnded(h *runState) {
	r := h.rec
	if r.ParentID == "" || !r.ParentWaits || r.Status == state.RunContinued {
		return
	}
	p := c.runs[r.ParentID]
	if p == nil || p.finishing {
		return
	}
	c.emitH(state.EventChildFinished, p, "", childFinishedPayload{
		Seq: r.ParentSeq, Name: r.Name, Run: r.ID, Status: r.Status, Result: r.Result, Reason: r.Reason, Message: r.Message})
	c.wake(p)
}

// cancelChildren cancels the open children h waits for, as h ends.
func (c *Core) cancelChildren(h *runState, why string) {
	var ids []string
	for id, ch := range c.runs {
		if ch.rec.ParentID == h.rec.ID && ch.rec.ParentWaits {
			ids = append(ids, id)
		}
	}
	sort.Strings(ids)
	for _, id := range ids {
		c.finishRun(c.runs[id], state.RunCancelled, "parent_finished", fmt.Sprintf("parent run %s %s", h.rec.ID, why), nil)
	}
}

// continueAsNew ends h as continued and starts its successor.
func (c *Core) continueAsNew(h *runState, params map[string]any) {
	r := h.rec
	var parent *parentLink
	if r.ParentID != "" {
		parent = &parentLink{id: r.ParentID, seq: r.ParentSeq, waits: r.ParentWaits}
	}
	res, err := c.startRun(StartRunRequest{
		Tenant: r.Tenant, Name: r.Name, Queue: r.Queue, Trigger: "continue " + r.ID, Params: params, Reuse: r.Reuse,
		parent: parent})
	if err != nil { // the run's own name: only fails if the request is broken
		c.finishRun(h, state.RunFailed, "continue_failed", err.Error(), nil)
		return
	}
	h.rec.ContinuedAs = res.RunID
	result, _ := json.Marshal(map[string]string{"continued_as": res.RunID})
	c.finishRun(h, state.RunContinued, "", "", result)
}

// decodeParams reads a JSON object of run parameters, numbers kept exact.
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
