package main

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/opencontainers/go-digest"
)

// storeLeaseToDHT stores a lease in the DHT for distributed coordination
func (r *Registry) storeLeaseToDHT(ctx context.Context, lease *ContentLease) error {
	// Create a serializable lease
	dhtLease := DHTLease{
		Digest:    lease.Digest.String(),
		Status:    int(lease.Status),
		Creator:   lease.Creator,
		Created:   lease.Created,
		Completed: lease.Completed,
	}

	// Serialize to JSON
	data, err := json.Marshal(dhtLease)
	if err != nil {
		return fmt.Errorf("failed to marshal lease: %w", err)
	}

	// Create a key for the DHT
	key := fmt.Sprintf("/fastreg/lease/%s", lease.Digest.String())

	// Store in DHT
	return r.dht.PutValue(ctx, key, data)
}

// getLeaseFromDHT retrieves a lease from the DHT
func (r *Registry) getLeaseFromDHT(ctx context.Context, dgst digest.Digest) (*DHTLease, error) {
	// Create a key for the DHT
	key := fmt.Sprintf("/fastreg/lease/%s", dgst.String())

	// Try to get from DHT
	data, err := r.dht.GetValue(ctx, key)
	if err != nil {
		return nil, err
	}

	// Deserialize
	var dhtLease DHTLease
	if err := json.Unmarshal(data, &dhtLease); err != nil {
		return nil, fmt.Errorf("failed to unmarshal lease: %w", err)
	}

	return &dhtLease, nil
}

// updateLeaseStatusInDHT updates the status of a lease in the DHT
func (r *Registry) updateLeaseStatusInDHT(ctx context.Context, lease *ContentLease, status LeaseStatus) error {
	// Create a serializable lease
	dhtLease := DHTLease{
		Digest:    lease.Digest.String(),
		Status:    int(status),
		Creator:   lease.Creator,
		Created:   lease.Created,
	}

	if status != LeaseStatusPending {
		dhtLease.Completed = time.Now()
	}

	// Serialize to JSON
	data, err := json.Marshal(dhtLease)
	if err != nil {
		return fmt.Errorf("failed to marshal lease: %w", err)
	}

	// Create a key for the DHT
	key := fmt.Sprintf("/fastreg/lease/%s", lease.Digest.String())

	// Store in DHT
	return r.dht.PutValue(ctx, key, data)
}

// convertDHTLease converts a DHTLease to a ContentLease
func (r *Registry) convertDHTLease(dhtLease *DHTLease) (*ContentLease, error) {
	dgst, err := digest.Parse(dhtLease.Digest)
	if err != nil {
		return nil, fmt.Errorf("invalid digest in DHT lease: %w", err)
	}

	return &ContentLease{
		Digest:    dgst,
		Status:    LeaseStatus(dhtLease.Status),
		Creator:   dhtLease.Creator,
		Created:   dhtLease.Created,
		Completed: dhtLease.Completed,
		waiters:   make([]chan bool, 0),
	}, nil
}

// checkLeaseValid checks if a DHT lease is valid (not expired)
func (r *Registry) checkLeaseValid(dhtLease *DHTLease) bool {
	// Check if the lease has expired
	if time.Since(dhtLease.Created) > r.leaseTTL {
		return false
	}

	return true
}

// getOrCreateDHTLease either gets an existing lease from DHT or creates a new one
func (r *Registry) getOrCreateDHTLease(ctx context.Context, dgst digest.Digest) (*ContentLease, bool, error) {
	// Generate a unique ID for this node if none exists
	nodeID := "unknown"
	if r.dht != nil && r.dht.host != nil {
		nodeID = r.dht.host.ID().String()
	}

	// First, try to get an existing lease from DHT
	dhtLease, err := r.getLeaseFromDHT(ctx, dgst)
	if err == nil && dhtLease != nil {
		// Found a lease in DHT
		
		// Check if it's valid (not expired)
		if !r.checkLeaseValid(dhtLease) {
			// Lease has expired, log this fact for debugging
			fmt.Printf("Found expired lease in DHT for %s, created at %v (TTL: %v, now: %v)\n",
				dgst.String(), dhtLease.Created, r.leaseTTL, time.Now())
			// We'll create a new one below (fall through)
		} else {
			// Convert to ContentLease
			contentLease, err := r.convertDHTLease(dhtLease)
			if err != nil {
				return nil, false, err
			}

			// If this node created the lease, it should fetch
			shouldFetch := contentLease.Creator == nodeID && contentLease.Status == LeaseStatusPending

			// If the lease is complete or failed, check if we should create a new one
			if contentLease.Status != LeaseStatusPending {
				if time.Since(contentLease.Created) > r.leaseTTL {
					// Lease has completed but is too old, we'll create a new one
					fmt.Printf("Found completed but old lease in DHT for %s, creating new one\n", 
						dgst.String())
					// Fall through to create a new lease
				} else {
					// Return the existing lease (no fetch needed)
					return contentLease, false, nil
				}
			} else {
				// Lease is pending, either we're the creator or we need to wait
				return contentLease, shouldFetch, nil
			}
		}
	}

	// Need to create a new lease
	r.leaseMu.Lock()
	defer r.leaseMu.Unlock()

	// Check if we can create a new lease (limit concurrency)
	if r.activeLeases >= r.maxActiveLeases {
		return nil, false, fmt.Errorf("too many active upstream requests (max: %d)", r.maxActiveLeases)
	}

	// Create a new lease
	lease := &ContentLease{
		Digest:  dgst,
		Status:  LeaseStatusPending,
		Creator: nodeID,
		Created: time.Now(),
		waiters: make([]chan bool, 0),
	}

	// Store in DHT first
	if err := r.storeLeaseToDHT(ctx, lease); err != nil {
		return nil, false, fmt.Errorf("failed to store lease in DHT: %w", err)
	}

	// Store locally and increment counter
	r.leases.Store(dgst.String(), lease)
	r.activeLeases++

	return lease, true, nil
}

// updateLeaseToDHTAndNotify updates a lease in DHT and notifies waiters
func (r *Registry) updateLeaseToDHTAndNotify(ctx context.Context, lease *ContentLease, success bool, err error) {
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

	// Update in DHT
	status := LeaseStatusComplete
	if !success {
		status = LeaseStatusFailed
	}
	
	if updateErr := r.updateLeaseStatusInDHT(ctx, lease, status); updateErr != nil {
		fmt.Printf("Warning: Failed to update lease status in DHT: %v\n", updateErr)
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

// waitForDHTLease waits for a lease to complete by periodically checking the DHT
func (r *Registry) waitForDHTLease(ctx context.Context, lease *ContentLease) error {
	// Create a channel for local notification (in case we're on the same node)
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

	// Poll the DHT for updates while also waiting for local notification
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-done:
			// Local notification received
			return nil
		case <-ticker.C:
			// Check DHT for updates
			dhtLease, err := r.getLeaseFromDHT(ctx, lease.Digest)
			if err == nil && dhtLease != nil {
				if dhtLease.Status == int(LeaseStatusComplete) {
					return nil
				} else if dhtLease.Status == int(LeaseStatusFailed) {
					return fmt.Errorf("lease failed according to DHT")
				}
				
				// If lease has expired, return error
				if time.Since(dhtLease.Created) > r.leaseTTL {
					return fmt.Errorf("lease expired in DHT")
				}
			}
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(r.leaseTTL):
			return fmt.Errorf("timeout waiting for lease to complete")
		}
	}
}