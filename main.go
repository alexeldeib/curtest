package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"
)

func run(ctx context.Context) error {
	// Parse command line flags
	listenAddrs := flag.String("listen", "/ip4/0.0.0.0/tcp/0", "Comma separated list of multiaddresses to listen on")
	bootstrapAddrs := flag.String("bootstrap", "", "Comma separated list of bootstrap peer multiaddresses")
	serviceTag := flag.String("service", "fastreg", "Service identifier tag for peer discovery")
	flag.Parse()

	// Split the listen addresses
	addrStrings := strings.Split(*listenAddrs, ",")

	// Initialize the DHT
	dht, err := NewDHT(ctx, addrStrings)
	if err != nil {
		return fmt.Errorf("failed to initialize DHT: %w", err)
	}
	defer dht.Close()

	// Print our addresses
	fmt.Println("Node addresses:")
	for _, addr := range dht.GetHostAddresses() {
		fmt.Printf("  %s\n", addr)
	}

	// Connect to bootstrap peers if provided
	if *bootstrapAddrs != "" {
		peerAddrs := strings.Split(*bootstrapAddrs, ",")
		if err := dht.ConnectToPeers(ctx, peerAddrs); err != nil {
			return fmt.Errorf("failed to connect to bootstrap peers: %w", err)
		}
	}

	// Start advertising and discovering peers
	dht.AdvertiseAndFindPeers(ctx, *serviceTag)

	// Main loop
	fmt.Println("Application running. Press Ctrl+C to exit.")
	<-ctx.Done()
	fmt.Println("Shutting down gracefully...")
	
	return nil
}

func main() {
	// Create a context that will be canceled on SIGINT
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Set up signal handling
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)

	// Run signal handler in a goroutine
	go func() {
		sig := <-sigCh
		fmt.Printf("Received signal: %s\n", sig)
		cancel() // Cancel the context
	}()

	// Run the application
	if err := run(ctx); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
}