package core

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"

	"github.com/alper/hubwagen/internal/state"
)

// Signals are input from outside delivered to a running haul (an approval, a
// file that arrived). SignalHaul records a signal on the haul (SignalReceived);
// a wait_signal command waits for one by name. A signal sent before the haul
// waits is kept until a wait takes it; each signal goes to one wait, and of
// several waits for a name, the oldest takes it first. A wait with a timeout
// starts a timer under its own seq; if it fires first, the wait gets a
// timeout instead. Either way the logbook records SignalDelivered for the
// wait's seq, and that is what a replay reads.
//
// Both waits and kept signals are derived from the logbook, so a new leader
// rebuilds them from the haul's events (restoreSignals).

// ErrHaulFinished: the haul is no longer open (signals can't reach it).
var ErrHaulFinished = errors.New("haul finished")

type signalWait struct {
	seq  int
	name string
}

// keptSignal is a signal received before any wait took it.
type keptSignal struct {
	ID      string          `json:"id"`
	Name    string          `json:"name"`
	Payload json.RawMessage `json:"payload,omitempty"`
}

// SignalRequest sends a signal to a haul. Payload is JSON (default null).
type SignalRequest struct {
	HaulID  string
	Name    string
	Payload json.RawMessage
}

// SignalHaul records a signal for an open haul and delivers it to a waiting
// wait_signal, if there is one. It returns the signal's ID.
func (c *Core) SignalHaul(ctx context.Context, req SignalRequest) (string, error) {
	return submit(ctx, c, func(c *Core) (string, error) {
		if req.Name == "" {
			return "", fmt.Errorf("%w: a signal needs a name", ErrInvalid)
		}
		payload := bytes.TrimSpace(req.Payload)
		if len(payload) == 0 {
			payload = []byte("null")
		}
		if !json.Valid(payload) {
			return "", fmt.Errorf("%w: signal payload is not JSON", ErrInvalid)
		}
		h := c.hauls[req.HaulID]
		if h == nil {
			rec, err := c.cfg.Store.GetHaul(ctx, req.HaulID)
			switch {
			case errors.Is(err, state.ErrNotFound):
				return "", fmt.Errorf("%w: haul %s", ErrNotFound, req.HaulID)
			case err != nil:
				return "", err
			}
			return "", fmt.Errorf("%w: haul %s is %s", ErrHaulFinished, req.HaulID, rec.Status)
		}
		s := keptSignal{ID: c.cfg.NewID(), Name: req.Name, Payload: json.RawMessage(payload)}
		c.emitH(state.EventSignalReceived, h, "", signalReceivedPayload{Signal: s.ID, Name: s.Name, Payload: s.Payload})
		if w := oldestWait(h, s.Name); w != nil {
			c.deliverSignal(h, w, &s)
		} else {
			h.signals = append(h.signals, s)
		}
		return s.ID, nil
	})
}

// waitSignal handles a wait_signal command: a kept signal answers it at once,
// otherwise it waits, with a timer if it has a timeout.
func (c *Core) waitSignal(h *haulState, cmd Command) {
	w := &signalWait{seq: cmd.Seq, name: cmd.Name}
	if i := slices.IndexFunc(h.signals, func(s keptSignal) bool { return s.Name == cmd.Name }); i >= 0 {
		s := h.signals[i]
		h.signals = slices.Delete(h.signals, i, i+1)
		c.deliverSignal(h, w, &s)
		return
	}
	h.waits[cmd.Seq] = w
	if cmd.Duration > 0 {
		c.startTimer(h, cmd)
	}
}

// deliverSignal ends wait w with signal s, or with a timeout if s is nil.
func (c *Core) deliverSignal(h *haulState, w *signalWait, s *keptSignal) {
	delete(h.waits, w.seq)
	p := signalDeliveredPayload{Seq: w.seq, Name: w.name}
	if s == nil {
		p.TimedOut = true
	} else {
		p.Signal, p.Payload = s.ID, s.Payload
		if tm := h.timers[w.seq]; tm != nil { // the signal beat the timeout
			c.cancelTimer(h, tm)
		}
	}
	c.emitH(state.EventSignalDelivered, h, "", p)
	c.wake(h)
}

func oldestWait(h *haulState, name string) *signalWait {
	var oldest *signalWait
	for _, w := range h.waits {
		if w.name == name && (oldest == nil || w.seq < oldest.seq) {
			oldest = w
		}
	}
	return oldest
}

// restoreSignals rebuilds a haul's waits and kept signals from its logbook:
// wait_signal commands without a SignalDelivered are waiting, and signals
// received but not delivered are kept, in the order they arrived.
func restoreSignals(h *haulState, evs []state.Event) error {
	delivered := map[int]bool{}
	taken := map[string]bool{}
	var received []keptSignal
	for _, e := range evs {
		switch e.Type {
		case state.EventSignalReceived:
			var p signalReceivedPayload
			if err := json.Unmarshal(e.Payload, &p); err != nil {
				return fmt.Errorf("haul %s: event %d: %w", h.rec.ID, e.Seq, err)
			}
			received = append(received, keptSignal{ID: p.Signal, Name: p.Name, Payload: p.Payload})
		case state.EventSignalDelivered:
			var p signalDeliveredPayload
			if err := json.Unmarshal(e.Payload, &p); err != nil {
				return fmt.Errorf("haul %s: event %d: %w", h.rec.ID, e.Seq, err)
			}
			delivered[p.Seq] = true
			if p.Signal != "" {
				taken[p.Signal] = true
			}
		}
	}
	for _, s := range received {
		if !taken[s.ID] {
			h.signals = append(h.signals, s)
		}
	}
	for _, cmd := range h.cmds {
		if cmd.Kind == CommandWaitSignal && !delivered[cmd.Seq] {
			h.waits[cmd.Seq] = &signalWait{seq: cmd.Seq, name: cmd.Name}
		}
	}
	return nil
}
