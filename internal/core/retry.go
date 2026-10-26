package core

import "time"

// Reasons that will fail the same way on every attempt, so retrying is
// pointless.
var nonRetryable = map[string]bool{"exec_error": true, "render_error": true, "no_runtime": true, "result_error": true}

// backoffDelay is initial, doubled per earlier attempt when exponential,
// capped at maxDelay.
func backoffDelay(exponential bool, initial, maxDelay time.Duration, n int) time.Duration {
	d := initial
	if exponential {
		for i := 1; i < n && d < maxDelay; i++ {
			d *= 2
		}
	}
	if d > maxDelay {
		d = maxDelay
	}
	return d
}
