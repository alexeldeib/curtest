package main

import (
	"context"
	"time"
)

// ConditionFunc returns true when the condition is satisfied, false if it has not been satisfied yet
type ConditionFunc func(ctx context.Context) (done bool, err error)

// WaitOptions provides configuration for wait operations
type WaitOptions struct {
	// Interval is the time to wait between checks
	Interval time.Duration
	// Timeout is the maximum time to wait
	Timeout time.Duration
}

// DefaultWaitOptions provides default values for WaitOptions
var DefaultWaitOptions = WaitOptions{
	Interval: 250 * time.Millisecond,
	Timeout:  10 * time.Second,
}

// UntilWithContext repeatedly executes the condition function until it returns
// true, context is canceled, or the timeout is reached
func UntilWithContext(ctx context.Context, condition ConditionFunc, options WaitOptions) error {
	timeoutCtx, cancel := context.WithTimeout(ctx, options.Timeout)
	defer cancel()

	ticker := time.NewTicker(options.Interval)
	defer ticker.Stop()

	for {
		done, err := condition(timeoutCtx)
		if err != nil {
			return err
		}
		if done {
			return nil
		}

		select {
		case <-timeoutCtx.Done():
			return timeoutCtx.Err()
		case <-ticker.C:
			// Continue to next check
		}
	}
}

// ForDuration waits for a specific duration using the wait pattern
// This replaces simple time.Sleep calls with a context-aware alternative
func ForDuration(ctx context.Context, duration time.Duration) error {
	deadline := time.Now().Add(duration)
	condition := func(ctx context.Context) (bool, error) {
		return time.Now().After(deadline), nil
	}
	
	options := WaitOptions{
		Interval: 250 * time.Millisecond, // Short polling interval
		Timeout:  duration + time.Second,  // Add buffer to ensure we don't timeout early
	}
	
	return UntilWithContext(ctx, condition, options)
}

// PollUntil executes a condition function until it returns true, an error, or the context is done
func PollUntil(ctx context.Context, interval time.Duration, condition ConditionFunc) error {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		done, err := condition(ctx)
		if err != nil {
			return err
		}
		if done {
			return nil
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			// Continue to next check
		}
	}
}

// WaitForDHTConnectivity polls until two DHT nodes are connected to each other
func WaitForDHTConnectivity(ctx context.Context, node1, node2 *DHT, timeout time.Duration) error {
	options := WaitOptions{
		Interval: 500 * time.Millisecond,
		Timeout:  timeout,
	}

	condition := func(ctx context.Context) (bool, error) {
		// Check if nodes are aware of each other
		peers1 := node1.host.Peerstore().Peers()
		peers2 := node2.host.Peerstore().Peers()
		
		node1ID := node1.host.ID()
		node2ID := node2.host.ID()
		
		foundNode1 := false
		for _, peer := range peers2 {
			if peer == node1ID {
				foundNode1 = true
				break
			}
		}
		
		foundNode2 := false
		for _, peer := range peers1 {
			if peer == node2ID {
				foundNode2 = true
				break
			}
		}
		
		return foundNode1 && foundNode2, nil
	}
	
	return UntilWithContext(ctx, condition, options)
}

// WaitForDHTValue waits until a value is available in the DHT
func WaitForDHTValue(ctx context.Context, node *DHT, key string, timeout time.Duration) error {
	options := WaitOptions{
		Interval: 500 * time.Millisecond,
		Timeout:  timeout,
	}

	condition := func(ctx context.Context) (bool, error) {
		// Try to get the value from DHT
		value, err := node.GetValue(ctx, key)
		if err != nil {
			// Not an error for condition, just not found yet
			return false, nil
		}
		
		return len(value) > 0, nil
	}
	
	return UntilWithContext(ctx, condition, options)
}