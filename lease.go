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
	// First check local state for quick checks
	localLease, localExists := r.leases.Load(dgst.String())
	
	if localExists {
		lease := localLease.(*ContentLease)
		lease.mu.Lock()
		defer lease.mu.Unlock()
		
		// If it's a pending local lease that we own, we should fetch
		nodeID := "unknown"
		if r.dht != nil && r.dht.host != nil {
			nodeID = r.dht.host.ID().String()
		}
		
		if lease.Status == LeaseStatusPending && lease.Creator == nodeID {
			return lease, true, nil
		}
		
		// If it's a completed lease that's still valid, just return it
		if (lease.Status == LeaseStatusComplete || lease.Status == LeaseStatusFailed) &&
			time.Since(lease.Created) <= r.leaseTTL {
			return lease, false, nil
		}
	}
	
	// For other cases, use the DHT for coordination if enabled
	if r.useDHTForLeases {
		return r.getOrCreateDHTLease(ctx, dgst)
	}
	
	// If DHT is disabled for leases, create a local-only lease
	r.leaseMu.Lock()
	defer r.leaseMu.Unlock()
	
	// Check if we can create a new lease (limit concurrency)
	if r.activeLeases >= r.maxActiveLeases {
		return nil, false, fmt.Errorf("too many active upstream requests (max: %d)", r.maxActiveLeases)
	}
	
	// Create a new local lease
	nodeID := "local"
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
	
	// Store locally and increment counter
	r.leases.Store(dgst.String(), lease)
	r.activeLeases++
	
	return lease, true, nil
}

// waitForLease waits for a lease to complete, with a timeout
func (r *Registry) waitForLease(ctx context.Context, lease *ContentLease) error {
	// Use DHT-based waiting for cross-node coordination if enabled
	if r.useDHTForLeases {
		return r.waitForDHTLease(ctx, lease)
	}
	
	// Otherwise use local-only waiting
	done := make(chan bool, 1)
	
	// Register our channel with the local lease if it exists
	if existingLease, ok := r.leases.Load(lease.Digest.String()); ok {
		localLease := existingLease.(*ContentLease)
		localLease.mu.Lock()
		if localLease.Status != LeaseStatusPending {
			// Already completed locally
			localLease.mu.Unlock()
			return nil
		}
		
		localLease.waiters = append(localLease.waiters, done)
		localLease.mu.Unlock()
	}
	
	// Wait for notification or timeout
	select {
	case <-done:
		// Lease completed
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(r.leaseTTL):
		return fmt.Errorf("timeout waiting for lease to complete")
	}
}

// completeLease marks a lease as complete and notifies all waiters
func (r *Registry) completeLease(lease *ContentLease, success bool, err error) {
	if r.useDHTForLeases {
		// Create a background context for DHT updates
		dhtCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		
		// Update lease in DHT and notify local waiters
		r.updateLeaseToDHTAndNotify(dhtCtx, lease, success, err)
	} else {
		// Local-only notification
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
}

// cleanupExpiredLeases removes any expired local leases
// and attempts to cleanup expired DHT leases as well
func (r *Registry) cleanupExpiredLeases() {
	r.leaseMu.Lock()
	defer r.leaseMu.Unlock()
	
	now := time.Now()
	bgCtx := context.Background()
	
	// Find expired leases
	var expired []string
	r.leases.Range(func(key, value interface{}) bool {
		lease := value.(*ContentLease)
		lease.mu.Lock()
		
		// Check if the lease has expired
		if now.Sub(lease.Created) > r.leaseTTL {
			expired = append(expired, key.(string))
			
			// If it's still pending, mark it as failed and notify waiters
			if lease.Status == LeaseStatusPending {
				lease.Status = LeaseStatusFailed
				lease.Error = fmt.Errorf("lease expired")
				lease.Completed = now
				
				// If we created this lease, mark it as failed in the DHT too
				if r.dht != nil && r.dht.host != nil && r.dht.host.ID().String() == lease.Creator {
					// Don't block on this, just try our best
					go func(l *ContentLease) {
						ctx, cancel := context.WithTimeout(bgCtx, 5*time.Second)
						defer cancel()
						r.updateLeaseStatusInDHT(ctx, l, LeaseStatusFailed)
					}(lease)
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
		}
		
		lease.mu.Unlock()
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