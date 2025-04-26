package main

import (
	"context"
	"fmt"
	"time"

	"github.com/opencontainers/go-digest"
)

// obtainLease tries to get a lease for fetching content from the upstream registry
// Returns a lease and a boolean indicating if this node should fetch from upstream (true) or wait (false)
func (r *Registry) obtainLease(ctx context.Context, dgst digest.Digest) (*ContentLease, bool, error) {
	r.leaseMu.Lock()
	defer r.leaseMu.Unlock()

	// Check if there's already a lease for this digest
	if leaseVal, exists := r.leases.Load(dgst.String()); exists {
		// A lease already exists, so we'll wait for it to complete
		lease := leaseVal.(*ContentLease)
		
		// If the lease is already complete or failed, it might be invalid
		lease.mu.Lock()
		if lease.Status == LeaseStatusComplete || lease.Status == LeaseStatusFailed {
			// Check if it's too old (expired)
			if time.Since(lease.Created) > r.leaseTTL {
				// Lease has expired, so remove it
				r.leases.Delete(dgst.String())
				lease.mu.Unlock()
			} else {
				lease.mu.Unlock()
				// Return the existing lease but indicate caller should not fetch
				return lease, false, nil
			}
		} else {
			lease.mu.Unlock()
			// Lease is still active, so just return it
			return lease, false, nil
		}
	}

	// No existing valid lease - check if we can create a new one
	// Limit the number of concurrent upstream requests
	if r.activeLeases >= r.maxActiveLeases {
		return nil, false, fmt.Errorf("too many active upstream requests (max: %d)", r.maxActiveLeases)
	}

	// Create a new lease
	nodeID := "unknown"
	if r.dht != nil && r.dht.host != nil {
		nodeID = r.dht.host.ID().String()
	}

	lease := &ContentLease{
		Digest:  dgst,
		Status:  LeaseStatusPending,
		Creator: nodeID,
		Created: time.Now(),
		waiters: make([]chan bool, 0),
	}

	// Store the lease in the map and increment active leases count
	r.leases.Store(dgst.String(), lease)
	r.activeLeases++

	// Return the lease and indicate caller should fetch from upstream
	return lease, true, nil
}

// waitForLease waits for a lease to complete, with a timeout
func (r *Registry) waitForLease(ctx context.Context, lease *ContentLease) error {
	// Create a channel for notification
	done := make(chan bool, 1)

	// Register our channel with the lease
	lease.mu.Lock()
	// Check if the lease is already complete
	if lease.Status != LeaseStatusPending {
		lease.mu.Unlock()
		return nil
	}
	
	// Add our channel to the waiters
	lease.waiters = append(lease.waiters, done)
	lease.mu.Unlock()

	// Wait for either the lease to complete, context to be done, or timeout
	select {
	case <-done:
		// The lease completed
		lease.mu.Lock()
		status := lease.Status
		err := lease.Error
		lease.mu.Unlock()

		if status == LeaseStatusFailed {
			return fmt.Errorf("lease failed: %w", err)
		}
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(r.leaseTTL):
		return fmt.Errorf("timeout waiting for lease to complete")
	}
}

// completeLease marks a lease as complete and notifies all waiters
func (r *Registry) completeLease(lease *ContentLease, success bool, err error) {
	r.leaseMu.Lock()
	defer r.leaseMu.Unlock()
	
	lease.mu.Lock()
	defer lease.mu.Unlock()
	
	lease.Completed = time.Now()
	
	if success {
		lease.Status = LeaseStatusComplete
	} else {
		lease.Status = LeaseStatusFailed
		lease.Error = err
	}
	
	// Notify all waiters
	for _, ch := range lease.waiters {
		select {
		case ch <- true:
			// Successfully notified
		default:
			// Channel is blocked or closed, skip it
		}
		close(ch)
	}
	
	// Clear the waiters list
	lease.waiters = nil
	
	// Decrement active leases count
	r.activeLeases--
}

// cleanupExpiredLeases removes any expired leases
func (r *Registry) cleanupExpiredLeases() {
	r.leaseMu.Lock()
	defer r.leaseMu.Unlock()
	
	now := time.Now()
	
	// Find expired leases
	var expired []string
	r.leases.Range(func(key, value interface{}) bool {
		lease := value.(*ContentLease)
		lease.mu.Lock()
		defer lease.mu.Unlock()
		
		// Check if the lease has expired
		if now.Sub(lease.Created) > r.leaseTTL {
			expired = append(expired, key.(string))
			
			// If it's still pending, mark it as failed and notify waiters
			if lease.Status == LeaseStatusPending {
				lease.Status = LeaseStatusFailed
				lease.Error = fmt.Errorf("lease expired")
				lease.Completed = now
				
				// Notify all waiters
				for _, ch := range lease.waiters {
					select {
					case ch <- true:
						// Successfully notified
					default:
						// Channel is blocked or closed, skip it
					}
					close(ch)
				}
				
				// Clear the waiters list
				lease.waiters = nil
				
				// Decrement active leases count 
				r.activeLeases--
			}
		}
		
		return true
	})
	
	// Remove expired leases from the map
	for _, key := range expired {
		r.leases.Delete(key)
	}
}

// startLeaseCleanupRoutine starts a background routine to clean up expired leases
func (r *Registry) startLeaseCleanupRoutine(ctx context.Context) {
	go func() {
		ticker := time.NewTicker(r.leaseTTL / 2)
		defer ticker.Stop()
		
		for {
			select {
			case <-ticker.C:
				r.cleanupExpiredLeases()
			case <-ctx.Done():
				return
			}
		}
	}()
}