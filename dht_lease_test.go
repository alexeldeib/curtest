package main

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/opencontainers/go-digest"
)

// TestDHTLeaseCoordination tests the DHT-based lease coordination between multiple nodes
func TestDHTLeaseCoordination(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Setup two nodes
	node1, err := NewDHT(ctx, []string{"/ip4/127.0.0.1/tcp/9601"})
	if err != nil {
		t.Fatalf("Failed to create DHT node 1: %v", err)
	}
	defer node1.Close()

	node2, err := NewDHT(ctx, []string{"/ip4/127.0.0.1/tcp/9602"})
	if err != nil {
		t.Fatalf("Failed to create DHT node 2: %v", err)
	}
	defer node2.Close()

	// Connect nodes together
	node1Addrs := node1.GetHostAddresses()
	if len(node1Addrs) == 0 {
		t.Fatalf("Node 1 has no addresses")
	}

	err = node2.ConnectToPeers(ctx, node1Addrs)
	if err != nil {
		t.Fatalf("Failed to connect node 2 to node 1: %v", err)
	}

	// Have nodes advertise the same service for discovery
	node1.AdvertiseAndFindPeers(ctx, "test-dht-lease")
	node2.AdvertiseAndFindPeers(ctx, "test-dht-lease")

	// Wait for DHT connections to establish - use longer timeout
	err = WaitForDHTConnectivity(ctx, node1, node2, 10*time.Second)
	if err != nil {
		t.Fatalf("Nodes failed to connect: %v", err)
	}
	
	// Additional explicit connection attempt
	t.Log("Attempting to establish direct connection between nodes...")
	err = node1.ConnectToPeers(ctx, node2.GetHostAddresses())
	if err != nil {
		t.Logf("Warning: Secondary connection attempt from node1 to node2 failed: %v", err)
	}

	// Set up two registry instances
	storageDir1 := t.TempDir()
	storageDir2 := t.TempDir()

	registry1, err := NewRegistry(ctx, node1, storageDir1, "https://registry-1.docker.io")
	if err != nil {
		t.Fatalf("Failed to create registry 1: %v", err)
	}

	registry2, err := NewRegistry(ctx, node2, storageDir2, "https://registry-1.docker.io")
	if err != nil {
		t.Fatalf("Failed to create registry 2: %v", err)
	}

	// Set shorter lease TTL for testing
	registry1.leaseTTL = 5 * time.Second
	registry2.leaseTTL = 5 * time.Second

	// Use these registries to test the DHT lease coordination

	// Test 1: Basic DHT lease creation and retrieval
	t.Run("BasicDHTLease", func(t *testing.T) {
		testDigest := digest.FromString("dht-lease-test-content")

		// Node 1 creates a lease
		lease1, shouldFetch1, err := registry1.obtainLease(ctx, testDigest)
		if err != nil {
			t.Fatalf("Failed to obtain lease on node 1: %v", err)
		}
		if !shouldFetch1 {
			t.Fatalf("Node 1 should fetch but got shouldFetch=false")
		}

		// Give time for DHT to propagate
		time.Sleep(1 * time.Second)

		// Node 2 tries to get a lease for the same content
		lease2, shouldFetch2, err := registry2.obtainLease(ctx, testDigest)
		if err != nil {
			t.Fatalf("Failed to check lease on node 2: %v", err)
		}

		// Node 2 should not fetch since Node 1 has the lease
		if shouldFetch2 {
			t.Fatalf("Node 2 should not fetch but got shouldFetch=true")
		}

		// Record the creator IDs for verification
		node1ID := node1.host.ID().String()
		node2ID := node2.host.ID().String()

		t.Logf("Node 1 ID: %s", node1ID)
		t.Logf("Node 2 ID: %s", node2ID)
		t.Logf("Lease creator: %s", lease2.Creator)

		// The lease creator should be node 1
		if lease2.Creator != node1ID {
			t.Errorf("Expected lease creator to be node 1 (%s) but got %s", 
				node1ID, lease2.Creator)
		}

		// Now have node 1 complete the lease
		t.Log("Node 1 completing lease...")
		registry1.completeLease(lease1, true, nil)

		// Wait for the DHT to update
		time.Sleep(1 * time.Second)

		// Check that node 2 can see the completion
		dhtLease, err := registry2.getLeaseFromDHT(ctx, testDigest)
		if err != nil {
			t.Fatalf("Failed to get lease from DHT on node 2: %v", err)
		}

		if dhtLease.Status != int(LeaseStatusComplete) {
			t.Errorf("Expected lease status to be Complete (1) but got %d", dhtLease.Status)
		}
	})

	// Test 2: Lease completion notification
	t.Run("LeaseCompletionNotification", func(t *testing.T) {
		testDigest := digest.FromString("dht-lease-test-notification")

		// Node 1 creates a lease
		lease1, shouldFetch1, err := registry1.obtainLease(ctx, testDigest)
		if err != nil {
			t.Fatalf("Failed to obtain lease on node 1: %v", err)
		}
		if !shouldFetch1 {
			t.Fatalf("Node 1 should fetch but got shouldFetch=false")
		}

		// Give time for DHT to propagate
		time.Sleep(1 * time.Second)

		// Node 2 tries to get a lease for the same content
		lease2, shouldFetch2, err := registry2.obtainLease(ctx, testDigest)
		if err != nil {
			t.Fatalf("Failed to check lease on node 2: %v", err)
		}

		// Node 2 should not fetch
		if shouldFetch2 {
			t.Fatalf("Node 2 should not fetch but got shouldFetch=true")
		}

		// Start a goroutine for node 2 to wait on the lease
		waitDone := make(chan error, 1)
		go func() {
			t.Log("Node 2 waiting for lease to complete...")
			err := registry2.waitForLease(ctx, lease2)
			waitDone <- err
		}()

		// Give node 2 time to start waiting
		time.Sleep(500 * time.Millisecond)

		// Now have node 1 complete the lease
		t.Log("Node 1 completing lease...")
		registry1.completeLease(lease1, true, nil)

		// Wait for node 2's wait to complete with a timeout
		select {
		case err := <-waitDone:
			if err != nil {
				t.Fatalf("Node 2 wait failed: %v", err)
			}
			t.Log("Node 2 successfully detected lease completion via DHT")
		case <-time.After(5 * time.Second):
			t.Fatalf("Timed out waiting for node 2 to detect lease completion")
		}
	})

	// Test 3: Lease expiration
	t.Run("LeaseExpiration", func(t *testing.T) {
		testDigest := digest.FromString("dht-lease-test-expiration")

		// Set an extremely short lease TTL for this test on both nodes
		origTTL1 := registry1.leaseTTL
		origTTL2 := registry2.leaseTTL
		registry1.leaseTTL = 1 * time.Second
		registry2.leaseTTL = 1 * time.Second
		defer func() {
			registry1.leaseTTL = origTTL1
			registry2.leaseTTL = origTTL2
		}()

		// Get the current node IDs for debugging
		node1ID := node1.host.ID().String()
		node2ID := node2.host.ID().String()
		t.Logf("Test environment: Node 1 ID=%s, Node 2 ID=%s", node1ID, node2ID)

		// Create a DHT lease key directly in the DHT with a fake expired lease
		expiredKey := fmt.Sprintf("/fastreg/lease/%s", testDigest.String())
		
		// Create a fake expired lease (created 5 seconds ago)
		timeCreated := time.Now().Add(-5 * time.Second)
		expired := &DHTLease{
			Digest:  testDigest.String(),
			Status:  int(LeaseStatusPending),
			Creator: node1ID,
			Created: timeCreated,
		}
		
		t.Logf("Creating expired lease: creator=%s, status=%d, created=%v, now=%v, TTL=%v", 
			expired.Creator, expired.Status, expired.Created, time.Now(), registry2.leaseTTL)
		
		// Marshal to JSON
		data, err := json.Marshal(expired)
		if err != nil {
			t.Fatalf("Failed to marshal expired lease: %v", err)
		}
		
		// Store in DHT with retry
		maxRetries := 3
		var storedSuccessfully bool
		
		for i := 0; i < maxRetries; i++ {
			t.Logf("Attempt %d to store lease in DHT", i+1)
			err = node1.PutValue(ctx, expiredKey, data)
			if err != nil {
				t.Logf("Failed to store lease in DHT on attempt %d: %v", i+1, err)
				time.Sleep(1 * time.Second)
				continue
			}
			
			// Success
			storedSuccessfully = true
			break
		}
		
		if !storedSuccessfully {
			t.Fatalf("Failed to store lease in DHT after %d retries", maxRetries)
		}
		
		// Wait for DHT to stabilize
		time.Sleep(2 * time.Second)
		
		// Verify the lease was stored correctly with retry
		var storedData []byte
		var retrievedSuccessfully bool
		
		for i := 0; i < maxRetries; i++ {
			t.Logf("Attempt %d to retrieve lease from DHT", i+1)
			storedData, err = node1.GetValue(ctx, expiredKey)
			if err != nil {
				t.Logf("Failed to retrieve lease from DHT on attempt %d: %v", i+1, err)
				time.Sleep(1 * time.Second)
				continue
			}
			
			// Success
			retrievedSuccessfully = true
			break
		}
		
		if !retrievedSuccessfully {
			t.Fatalf("Failed to retrieve lease from DHT after %d retries", maxRetries)
		}
		
		var storedLease DHTLease
		if err := json.Unmarshal(storedData, &storedLease); err != nil {
			t.Fatalf("Failed to unmarshal stored lease: %v", err)
		}
		
		t.Logf("Stored lease retrieved: creator=%s, status=%d, created=%v", 
			storedLease.Creator, storedLease.Status, storedLease.Created)
		
		// Verify the lease is expired according to the registry's check
		isValid := registry2.checkLeaseValid(&storedLease)
		t.Logf("Lease validity check: isValid=%v (expected false)", isValid)
		if isValid {
			t.Fatalf("Registry thinks the lease is still valid when it should be expired")
		}
		
		// Wait for DHT propagation
		t.Log("Waiting for DHT propagation...")
		time.Sleep(2 * time.Second)
		
		// Now node 2 should be able to get a new lease
		t.Log("Node 2 attempting to obtain a lease...")
		lease2, shouldFetch2, err := registry2.obtainLease(ctx, testDigest)
		if err != nil {
			t.Fatalf("Failed to obtain lease on node 2 after expiration: %v", err)
		}
		
		// Node 2 should now be able to fetch
		t.Logf("Got lease response: shouldFetch=%v, lease.Creator=%s", shouldFetch2, lease2.Creator)
		if !shouldFetch2 {
			// Print more debug info
			dhtLease, getErr := registry2.getLeaseFromDHT(ctx, testDigest)
			if getErr != nil {
				t.Logf("Failed to get lease from DHT: %v", getErr)
			} else {
				timeSinceCreated := time.Since(dhtLease.Created)
				t.Logf("DHT lease: creator=%s, status=%d, created=%v (TTL=%v, now=%v, time since=%v)", 
					dhtLease.Creator, dhtLease.Status, dhtLease.Created, 
					registry2.leaseTTL, time.Now(), timeSinceCreated)
				t.Logf("Lease expired? %v", timeSinceCreated > registry2.leaseTTL)
			}
			
			t.Fatalf("Node 2 should be able to fetch after expiration but got shouldFetch=false")
		}

		t.Logf("Successfully created new lease with node 2 after node 1's lease expired")
		
		// Clean up
		registry2.completeLease(lease2, true, nil)
	})

	t.Log("All DHT lease tests completed successfully")
}