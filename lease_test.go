package main

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/opencontainers/go-digest"
)

// TestLeaseCoordination tests that the lease mechanism properly coordinates
// multiple nodes trying to fetch the same content simultaneously
func TestLeaseCoordination(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Setup the DHT
	dht, err := NewDHT(ctx, []string{"/ip4/127.0.0.1/tcp/9901"})
	if err != nil {
		t.Fatalf("Failed to create DHT: %v", err)
	}
	defer dht.Close()

	// Setup the registry
	storageDir := t.TempDir()
	registry, err := NewRegistry(ctx, dht, storageDir, "https://registry-1.docker.io")
	if err != nil {
		t.Fatalf("Failed to create registry: %v", err)
	}

	// Test with a fake digest
	testDigest := digest.FromString("test-content")

	// Make sure the lease system is initialized
	if val, exists := registry.leases.Load(testDigest.String()); exists {
		t.Fatalf("Lease already exists but shouldn't: %v", val)
	}

	// Test obtaining a new lease
	lease, shouldFetch, err := registry.obtainLease(ctx, testDigest)
	if err != nil {
		t.Fatalf("Failed to obtain lease: %v", err)
	}
	if !shouldFetch {
		t.Fatalf("Should have gotten permission to fetch but didn't")
	}
	if lease == nil {
		t.Fatalf("No lease returned")
	}

	// Check lease properties
	if lease.Status != LeaseStatusPending {
		t.Errorf("Expected lease status to be Pending, got %v", lease.Status)
	}
	if lease.Digest != testDigest {
		t.Errorf("Expected lease digest to be %s, got %s", testDigest, lease.Digest)
	}

	// Verify the lease is stored
	leaseVal, exists := registry.leases.Load(testDigest.String())
	if !exists {
		t.Fatalf("Lease not stored in registry")
	}
	storedLease := leaseVal.(*ContentLease)
	if storedLease != lease {
		t.Errorf("Stored lease is not the same as returned lease")
	}

	// Now simulate another node trying to obtain the same lease
	lease2, shouldFetch2, err := registry.obtainLease(ctx, testDigest)
	if err != nil {
		t.Fatalf("Failed to check for existing lease: %v", err)
	}
	if shouldFetch2 {
		t.Fatalf("Second node shouldn't have gotten permission to fetch")
	}
	if lease2 == nil {
		t.Fatalf("No lease returned for second node")
	}
	if lease2 != lease {
		t.Errorf("Second node got different lease object")
	}

	// Test completing the lease
	t.Log("Completing lease...")
	registry.completeLease(lease, true, nil)

	// Check the lease status
	if lease.Status != LeaseStatusComplete {
		t.Errorf("Expected lease status to be Complete, got %v", lease.Status)
	}

	// Test that the lease is still valid and returns the right permissions
	lease3, shouldFetch3, err := registry.obtainLease(ctx, testDigest)
	if err != nil {
		t.Fatalf("Failed to check for existing completed lease: %v", err)
	}
	if shouldFetch3 {
		t.Fatalf("Node shouldn't have gotten permission to fetch already completed lease")
	}
	if lease3 == nil {
		t.Fatalf("No lease returned for completed lease")
	}
	if lease3 != lease {
		t.Errorf("Different lease object returned for completed lease")
	}

	// Test waiting for lease
	t.Log("Testing lease wait functionality...")
	var wg sync.WaitGroup
	wg.Add(2)

	// Create a new lease for testing
	waitTestDigest := digest.FromString("wait-test-content")
	waitLease, shouldFetch, err := registry.obtainLease(ctx, waitTestDigest)
	if err != nil || !shouldFetch || waitLease == nil {
		t.Fatalf("Failed to set up wait test: %v", err)
	}

	// Test waiter functions
	go func() {
		defer wg.Done()
		start := time.Now()

		// Set up a 1-second timeout context
		waitCtx, waitCancel := context.WithTimeout(ctx, 1*time.Second)
		defer waitCancel()

		err := registry.waitForLease(waitCtx, waitLease)
		elapsed := time.Since(start)

		if err != nil {
			t.Errorf("Waiter 1 error: %v (after %v)", err, elapsed)
		} else {
			t.Logf("Waiter 1 completed after %v", elapsed)
		}
	}()

	go func() {
		defer wg.Done()
		start := time.Now()

		// Set up a longer timeout context
		waitCtx, waitCancel := context.WithTimeout(ctx, 5*time.Second)
		defer waitCancel()

		// Wait a bit before completing the lease
		time.Sleep(500 * time.Millisecond)
		registry.completeLease(waitLease, true, nil)
		t.Log("Completed wait test lease")

		err := registry.waitForLease(waitCtx, waitLease)
		elapsed := time.Since(start)

		if err != nil {
			t.Errorf("Waiter 2 error: %v (after %v)", err, elapsed)
		} else {
			t.Logf("Waiter 2 completed after %v", elapsed)
		}
	}()

	// Wait for all goroutines to complete
	wg.Wait()
	t.Log("All waiters completed")

	// Test expired lease cleanup
	t.Log("Testing lease expiration...")
	expiredDigest := digest.FromString("expired-test-content")
	registry.leaseTTL = 200 * time.Millisecond // Set a very short TTL for testing

	_, _, _ = registry.obtainLease(ctx, expiredDigest) // Create the lease
	time.Sleep(500 * time.Millisecond) // Wait longer than TTL

	registry.cleanupExpiredLeases()

	// Check if the expired lease was removed
	if _, exists := registry.leases.Load(expiredDigest.String()); exists {
		t.Errorf("Expired lease should have been removed but wasn't")
	}

	// Special test: simultaneous lease requests
	t.Log("Testing simultaneous lease requests...")
	simDigest := digest.FromString("simultaneous-test")
	const numConcurrent = 10

	var fetchCount int
	var fetchMutex sync.Mutex
	var simWg sync.WaitGroup
	simWg.Add(numConcurrent)

	for i := 0; i < numConcurrent; i++ {
		go func(idx int) {
			defer simWg.Done()
			// Delay slightly to ensure they all run nearly at the same time
			time.Sleep(time.Duration(idx) * time.Millisecond)

			l, shouldFetch, err := registry.obtainLease(ctx, simDigest)
			if err != nil {
				t.Errorf("Goroutine %d failed to get lease: %v", idx, err)
				return
			}

			if shouldFetch {
				fetchMutex.Lock()
				fetchCount++
				fetchMutex.Unlock()

				// Simulate fetching taking some time
				time.Sleep(100 * time.Millisecond)
				registry.completeLease(l, true, nil)
				fmt.Printf("Goroutine %d completed the fetch\n", idx)
			} else {
				// Wait for the lease
				err := registry.waitForLease(ctx, l)
				if err != nil {
					t.Errorf("Goroutine %d failed waiting for lease: %v", idx, err)
				} else {
					fmt.Printf("Goroutine %d successfully waited for fetch\n", idx)
				}
			}
		}(i)
	}

	simWg.Wait()
	t.Logf("Concurrent test complete. %d/%d goroutines fetched content", fetchCount, numConcurrent)

	// Check that only one or a small number of fetches were performed (not all)
	if fetchCount >= numConcurrent {
		t.Errorf("All %d goroutines fetched content, expected only 1 or a few", fetchCount)
	}
	if fetchCount == 0 {
		t.Errorf("No goroutines fetched content, expected at least 1")
	}
	if fetchCount > registry.maxActiveLeases {
		t.Errorf("More goroutines fetched (%d) than the maxActiveLeases limit (%d)",
			fetchCount, registry.maxActiveLeases)
	}
}