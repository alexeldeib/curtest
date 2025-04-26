package main

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/libp2p/go-libp2p"
	dht "github.com/libp2p/go-libp2p-kad-dht"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/p2p/discovery/routing"
	"github.com/multiformats/go-multiaddr"
)

// DHT represents our Kademlia DHT service
type DHT struct {
	host   host.Host
	kadDHT *dht.IpfsDHT
	mu     sync.Mutex
}

// NewDHT creates a new DHT instance
func NewDHT(ctx context.Context, listenAddrs []string) (*DHT, error) {
	// Parse multiaddresses
	maddrs := make([]multiaddr.Multiaddr, 0, len(listenAddrs))
	for _, addr := range listenAddrs {
		ma, err := multiaddr.NewMultiaddr(addr)
		if err != nil {
			return nil, fmt.Errorf("invalid multiaddress: %w", err)
		}
		maddrs = append(maddrs, ma)
	}

	// Create a libp2p host with options that help with discovery
	h, err := libp2p.New(
		libp2p.ListenAddrs(maddrs...),
		// Enable Hole punching for NAT traversal
		libp2p.EnableHolePunching(),
		// Enable NAT service
		libp2p.EnableNATService(),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create libp2p host: %w", err)
	}

	// Create a DHT in server mode for better discovery
	// Based on the reference test, server mode is important for full DHT participation
	kadDHT, err := dht.New(ctx, h, 
		dht.Mode(dht.ModeServer),
		dht.ProtocolPrefix("/fastreg"), // Use a custom protocol prefix for our app
	)
	if err != nil {
		h.Close()
		return nil, fmt.Errorf("failed to create DHT: %w", err)
	}

	// Bootstrap the DHT
	if err = kadDHT.Bootstrap(ctx); err != nil {
		h.Close()
		return nil, fmt.Errorf("failed to bootstrap DHT: %w", err)
	}

	return &DHT{
		host:   h,
		kadDHT: kadDHT,
	}, nil
}

// ConnectToPeers connects to bootstrap peers
func (d *DHT) ConnectToPeers(ctx context.Context, bootstrapPeers []string) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	for _, peerAddr := range bootstrapPeers {
		addr, err := multiaddr.NewMultiaddr(peerAddr)
		if err != nil {
			return fmt.Errorf("invalid peer address %s: %w", peerAddr, err)
		}

		peerInfo, err := peer.AddrInfoFromP2pAddr(addr)
		if err != nil {
			return fmt.Errorf("failed to get peer info: %w", err)
		}

		if err := d.host.Connect(ctx, *peerInfo); err != nil {
			return fmt.Errorf("failed to connect to peer %s: %w", peerInfo.ID, err)
		}
		fmt.Printf("Connected to peer: %s\n", peerInfo.ID)
	}
	return nil
}

// AdvertiseAndFindPeers advertises this node and looks for others
func (d *DHT) AdvertiseAndFindPeers(ctx context.Context, serviceTag string) {
	// Create a routing discovery service using the DHT
	routingDiscovery := routing.NewRoutingDiscovery(d.kadDHT)

	// Advertise this service
	routingDiscovery.Advertise(ctx, serviceTag)
	fmt.Printf("Advertising service: %s\n", serviceTag)

	// Look for others that have announced the same service
	go func() {
		// Shorter interval for tests and initial discovery
		initialInterval := 1 * time.Second
		// Longer interval for ongoing discovery
		regularInterval := time.Minute
		
		// Use shorter interval for first few attempts
		currentInterval := initialInterval
		attemptCount := 0
		maxInitialAttempts := 5
		
		for {
			peerChan, err := routingDiscovery.FindPeers(ctx, serviceTag)
			if err != nil {
				fmt.Printf("Error finding peers: %s\n", err)
				return
			}

			foundPeers := 0
			for peer := range peerChan {
				if peer.ID == d.host.ID() {
					continue // Skip ourselves
				}
				
				// Only try to connect if we're not already connected
				if d.host.Network().Connectedness(peer.ID) != 2 { // 2 = Connected
					if err := d.host.Connect(ctx, peer); err != nil {
						fmt.Printf("Failed to connect to peer %s: %s\n", peer.ID, err)
						continue
					}
					fmt.Printf("Connected to peer: %s\n", peer.ID)
					foundPeers++
				}
			}
			
			// For testing - if we find peers, actively try to get them to discover each other
			if foundPeers > 0 {
				// Ping the DHT to help discovery
				if err := d.kadDHT.Bootstrap(ctx); err != nil {
					fmt.Printf("Error bootstrapping DHT: %s\n", err)
				}
				
				// Get some random peers from our table and connect them to each other
				// This should accelerate the mesh network formation
				for _, p := range d.host.Peerstore().Peers() {
					if p == d.host.ID() {
						continue
					}
					d.kadDHT.FindPeer(ctx, p)
				}
			}
			
			// Adjust timer interval based on discovery phase
			attemptCount++
			if attemptCount > maxInitialAttempts {
				currentInterval = regularInterval
			}

			// Wait before searching again
			select {
			case <-ctx.Done():
				return
			case <-time.After(currentInterval):
				continue
			}
		}
	}()
}

// GetHostAddresses returns the listen addresses of this host
func (d *DHT) GetHostAddresses() []string {
	addrs := d.host.Addrs()
	hostID := d.host.ID()
	var fullAddrs []string

	for _, addr := range addrs {
		fullAddr := fmt.Sprintf("%s/p2p/%s", addr.String(), hostID.String())
		fullAddrs = append(fullAddrs, fullAddr)
	}

	return fullAddrs
}

// Close shuts down the DHT and host
func (d *DHT) Close() error {
	d.mu.Lock()
	defer d.mu.Unlock()

	var err error
	if err1 := d.kadDHT.Close(); err1 != nil {
		err = err1
	}

	if err2 := d.host.Close(); err2 != nil {
		if err == nil {
			err = err2
		}
	}

	return err
}
