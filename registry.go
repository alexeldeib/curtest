package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/opencontainers/go-digest"
)

// Registry represents our mirror registry with DHT-based content lookup
type Registry struct {
	dht              *DHT
	upstreamURL      string
	storageDir       string
	cache            sync.Map // Cache for DHT lookups
	leases           sync.Map // Active leases for content being pulled
	cacheTTL         time.Duration
	upstreamTimeout  time.Duration
	leaseTTL         time.Duration
	maxActiveLeases  int       // Max concurrent upstream requests allowed
	activeLeases     int       // Current number of active upstream requests
	mu               sync.RWMutex
	leaseMu          sync.Mutex // Separate mutex for lease operations
	useDHTForLeases  bool       // Flag to control whether to use DHT for lease coordination
}

// CacheEntry represents a cached entry for a registry blob
type CacheEntry struct {
	Peers   []string     // Peers that have this content
	Created time.Time    // When this entry was created
	Digest  digest.Digest // The content digest
}

// LeaseStatus represents the status of a content lease
type LeaseStatus int

const (
	// LeaseStatusPending means content is being fetched from upstream
	LeaseStatusPending LeaseStatus = iota
	// LeaseStatusComplete means content has been fetched and is available
	LeaseStatusComplete
	// LeaseStatusFailed means fetching from upstream failed
	LeaseStatusFailed
)

// ContentLease represents a lease for fetching content from upstream
type ContentLease struct {
	Digest    digest.Digest // Content digest
	Status    LeaseStatus   // Current status
	Error     error         // Error if status is Failed
	Creator   string        // ID of node that created the lease
	Created   time.Time     // When the lease was created
	Completed time.Time     // When the lease was completed (if it was)
	mu        sync.Mutex    // Mutex to protect status changes
	waiters   []chan bool   // Channels to notify waiters when lease completes
}

// DHTLease represents a serializable lease for DHT storage
type DHTLease struct {
	Digest    string    `json:"digest"`
	Status    int       `json:"status"`
	Creator   string    `json:"creator"`
	Created   time.Time `json:"created"`
	Completed time.Time `json:"completed,omitempty"`
}

// NewRegistry creates a new registry mirror
func NewRegistry(ctx context.Context, dht *DHT, storageDir, upstreamURL string) (*Registry, error) {
	// Ensure the storage directory exists
	if err := os.MkdirAll(storageDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create storage directory: %w", err)
	}
	
	// Create the blob storage directory
	blobsDir := filepath.Join(storageDir, "blobs")
	if err := os.MkdirAll(blobsDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create blobs directory: %w", err)
	}

	return &Registry{
		dht:             dht,
		upstreamURL:     upstreamURL,
		storageDir:      storageDir,
		cacheTTL:        30 * time.Minute,
		upstreamTimeout: 60 * time.Second, // Increase timeout for Docker Hub
		leaseTTL:        2 * time.Minute,  // Lease timeout duration
		maxActiveLeases: 5,               // Allow up to 5 concurrent upstream requests
		useDHTForLeases: true,            // By default, use DHT for lease coordination
	}, nil
}

// getBlobPath returns the path to store a blob locally
func (r *Registry) getBlobPath(dgst digest.Digest) string {
	// Use the first two characters of the digest as a shard
	algorithm := dgst.Algorithm().String()
	hex := dgst.Hex()
	
	// Create the shard directory if needed
	shardDir := filepath.Join(r.storageDir, "blobs", algorithm, hex[:2])
	if err := os.MkdirAll(shardDir, 0755); err != nil {
		fmt.Printf("Failed to create shard directory: %v\n", err)
	}
	
	return filepath.Join(shardDir, hex)
}

// StoreInDHT stores information about a blob in the DHT
func (r *Registry) StoreInDHT(ctx context.Context, dgst digest.Digest) error {
	// Get our node addresses
	addresses := r.dht.GetHostAddresses()
	if len(addresses) == 0 {
		return fmt.Errorf("no addresses available for local node")
	}

	// Create a key for the DHT
	key := fmt.Sprintf("/fastreg/blob/%s", dgst.String())

	// Store our address in the DHT, so others can fetch from us
	return r.dht.PutValue(ctx, key, []byte(strings.Join(addresses, ",")))
}

// FindInDHT looks for a blob in the DHT and returns peers that have it
func (r *Registry) FindInDHT(ctx context.Context, dgst digest.Digest) ([]string, error) {
	// Check the cache first
	if entry, ok := r.cache.Load(dgst.String()); ok {
		cacheEntry := entry.(CacheEntry)
		if time.Since(cacheEntry.Created) < r.cacheTTL {
			return cacheEntry.Peers, nil
		}
		// Cache expired, remove it
		r.cache.Delete(dgst.String())
	}

	// Create a key for the DHT
	key := fmt.Sprintf("/fastreg/blob/%s", dgst.String())

	// Try to get peer addresses from the DHT
	value, err := r.dht.GetValue(ctx, key)
	if err != nil {
		return nil, err
	}

	// Parse the addresses
	peers := strings.Split(string(value), ",")

	// Cache the result
	r.cache.Store(dgst.String(), CacheEntry{
		Peers:   peers,
		Created: time.Now(),
		Digest:  dgst,
	})

	return peers, nil
}

// FetchFromPeer attempts to fetch a blob from a peer
func (r *Registry) FetchFromPeer(ctx context.Context, dgst digest.Digest, peerAddr string) (io.ReadCloser, error) {
	// Extract the IP address and port from the multiaddress
	// Format is typically like /ip4/127.0.0.1/tcp/9501/p2p/12D3KooWA8MswTVHRNt584LzT3Zr6eFw6xnfSiTw7ae3eUJxYNCM
	parts := strings.Split(peerAddr, "/")
	
	// We need at least 6 parts: ["", "ip4", "127.0.0.1", "tcp", "9501", "p2p", ...]
	if len(parts) < 6 {
		return nil, fmt.Errorf("invalid peer address format: %s", peerAddr)
	}
	
	// Get the IP address and port
	// Skip protocol parts - just extract the address and port
	ipAddr := parts[2]   // e.g., "127.0.0.1"
	port := parts[4]     // e.g., "9501"
	
	// Create a proper HTTP URL 
	url := fmt.Sprintf("http://%s:%s/v2/blobs/%s", ipAddr, port, dgst.String())
	
	// Create an HTTP request with timeout
	reqCtx, reqCancel := context.WithTimeout(context.Background(), 30*time.Second)
	
	req, err := http.NewRequestWithContext(reqCtx, "GET", url, nil)
	if err != nil {
		reqCancel()
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	// Send the request
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		reqCancel()
		return nil, fmt.Errorf("failed to fetch from peer: %w", err)
	}

	// Check status code
	if resp.StatusCode != http.StatusOK {
		resp.Body.Close()
		reqCancel()
		return nil, fmt.Errorf("peer returned status %d", resp.StatusCode)
	}

	// Return body with context cancellation
	return &cancelReadCloser{
		ReadCloser: resp.Body,
		cancel:     reqCancel,
	}, nil
}

// FetchFromUpstream attempts to fetch a blob from the upstream registry
func (r *Registry) FetchFromUpstream(ctx context.Context, repo string, dgst digest.Digest) (io.ReadCloser, error) {
	// Create a new independent context with timeout to avoid cancellation issues from parent
	// This allows the HTTP request to complete even if the parent context is cancelled
	reqCtx, reqCancel := context.WithTimeout(context.Background(), r.upstreamTimeout)
	
	// Create a direct HTTP request to the upstream
	url := fmt.Sprintf("%s/v2/%s/blobs/%s", r.upstreamURL, repo, dgst.String())
	req, err := http.NewRequestWithContext(reqCtx, "GET", url, nil)
	if err != nil {
		reqCancel()
		return nil, fmt.Errorf("failed to create request: %w", err)
	}
	
	// Add Docker Hub specific headers if needed
	if strings.Contains(r.upstreamURL, "docker.io") {
		req.Header.Set("User-Agent", "fastreg/1.0")
	}

	// Send the request
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		reqCancel()
		return nil, fmt.Errorf("failed to fetch from upstream: %w", err)
	}
	
	// Handle auth challenges
	if resp.StatusCode == http.StatusUnauthorized {
		// For Docker Hub, we need to get a token first
		authHeader := resp.Header.Get("Www-Authenticate")
		if authHeader != "" && strings.Contains(authHeader, "Bearer") {
			// Close the first response
			resp.Body.Close()
			
			// Extract the auth parameters
			authParams := extractAuthParams(authHeader)
			
			// Get a token - use the independent context for token request too
			token, err := getDockerHubToken(reqCtx, authParams, repo)
			if err != nil {
				reqCancel()
				return nil, fmt.Errorf("failed to get auth token: %w", err)
			}
			
			// Create a new request with the token
			req, err = http.NewRequestWithContext(reqCtx, "GET", url, nil)
			if err != nil {
				reqCancel()
				return nil, fmt.Errorf("failed to create authorized request: %w", err)
			}
			
			// Add the authorization header
			req.Header.Set("Authorization", fmt.Sprintf("Bearer %s", token))
			if strings.Contains(r.upstreamURL, "docker.io") {
				req.Header.Set("User-Agent", "fastreg/1.0")
			}
			
			// Try again with the token
			resp, err = http.DefaultClient.Do(req)
			if err != nil {
				reqCancel()
				return nil, fmt.Errorf("failed to fetch from upstream with token: %w", err)
			}
		}
	}

	// Check status code
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		reqCancel()
		return nil, fmt.Errorf("upstream returned status %d: %s", resp.StatusCode, string(body))
	}

	// Create a ReadCloser that will cancel the context when closed
	return &cancelReadCloser{
		ReadCloser: resp.Body,
		cancel:     reqCancel,
	}, nil
}

// cancelReadCloser wraps a ReadCloser and cancels a context when closed
type cancelReadCloser struct {
	io.ReadCloser
	cancel context.CancelFunc
}

// Close closes the wrapped ReadCloser and cancels the context
func (c *cancelReadCloser) Close() error {
	err := c.ReadCloser.Close()
	c.cancel()
	return err
}

// Extract auth parameters from WWW-Authenticate header
func extractAuthParams(header string) map[string]string {
	params := make(map[string]string)
	
	// Extract the scheme and parameters
	parts := strings.SplitN(header, " ", 2)
	if len(parts) != 2 || parts[0] != "Bearer" {
		return params
	}
	
	// Parse the parameters
	for _, param := range strings.Split(parts[1], ",") {
		keyValue := strings.SplitN(param, "=", 2)
		if len(keyValue) != 2 {
			continue
		}
		
		key := strings.TrimSpace(keyValue[0])
		value := strings.Trim(strings.TrimSpace(keyValue[1]), "\"")
		params[key] = value
	}
	
	return params
}

// Get a Docker Hub token for a repository
func getDockerHubToken(ctx context.Context, authParams map[string]string, repo string) (string, error) {
	realm := authParams["realm"]
	service := authParams["service"]
	scope := authParams["scope"]
	
	if realm == "" || service == "" {
		return "", fmt.Errorf("missing required auth parameters")
	}
	
	// Create the token request URL
	tokenURL := fmt.Sprintf("%s?service=%s", realm, service)
	if scope != "" {
		tokenURL = fmt.Sprintf("%s&scope=%s", tokenURL, scope)
	}
	
	// Add a client with a reasonable timeout
	client := &http.Client{
		Timeout: 10 * time.Second,
	}
	
	// Make the request, using provided context
	req, err := http.NewRequestWithContext(ctx, "GET", tokenURL, nil)
	if err != nil {
		return "", fmt.Errorf("failed to create token request: %w", err)
	}
	
	// Add user agent for Docker Hub
	req.Header.Set("User-Agent", "fastreg/1.0")
	
	resp, err := client.Do(req)
	if err != nil {
		return "", fmt.Errorf("failed to get token: %w", err)
	}
	defer resp.Body.Close()
	
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return "", fmt.Errorf("token request returned status %d: %s", resp.StatusCode, string(body))
	}
	
	// Parse the response
	var tokenResp struct {
		Token string `json:"token"`
	}
	
	err = json.NewDecoder(resp.Body).Decode(&tokenResp)
	if err != nil {
		return "", fmt.Errorf("failed to parse token response: %w", err)
	}
	
	return tokenResp.Token, nil
}

// StoreBlobLocally stores a blob in the local filesystem
func (r *Registry) StoreBlobLocally(dgst digest.Digest, reader io.Reader) error {
	// Get the path to store the blob
	blobPath := r.getBlobPath(dgst)
	
	// Create a temporary file
	tmpFile, err := os.CreateTemp(filepath.Dir(blobPath), "blob-*.tmp")
	if err != nil {
		return fmt.Errorf("failed to create temporary file: %w", err)
	}
	defer os.Remove(tmpFile.Name())
	
	// Copy the content to the temporary file
	if _, err := io.Copy(tmpFile, reader); err != nil {
		tmpFile.Close()
		return fmt.Errorf("failed to write blob data: %w", err)
	}
	
	// Close the file before renaming
	if err := tmpFile.Close(); err != nil {
		return fmt.Errorf("failed to close temporary file: %w", err)
	}
	
	// Rename the temporary file to the final destination
	if err := os.Rename(tmpFile.Name(), blobPath); err != nil {
		return fmt.Errorf("failed to move blob to final location: %w", err)
	}
	
	return nil
}

// GetLocalBlob retrieves a blob from local storage
func (r *Registry) GetLocalBlob(dgst digest.Digest) (io.ReadCloser, error) {
	blobPath := r.getBlobPath(dgst)
	return os.Open(blobPath)
}

// HandleBlob processes a blob request with DHT-based lookup
func (r *Registry) HandleBlob(w http.ResponseWriter, req *http.Request, repo string, dgst digest.Digest) {
	ctx := req.Context()
	
	// Step 1: Try to get the blob locally
	localBlob, err := r.GetLocalBlob(dgst)
	if err == nil {
		// We have it locally, serve it
		defer localBlob.Close()
		
		// Store in DHT so others can find us
		go r.StoreInDHT(context.Background(), dgst)
		
		// Write the blob content
		w.Header().Set("Content-Type", "application/octet-stream")
		w.Header().Set("Docker-Content-Digest", dgst.String())
		io.Copy(w, localBlob)
		return
	}
	
	// Step 2: Try to find it in the DHT
	peers, err := r.FindInDHT(ctx, dgst)
	if err == nil && len(peers) > 0 {
		// Try each peer until we find one that works
		for _, peer := range peers {
			peerBlob, err := r.FetchFromPeer(ctx, dgst, peer)
			if err == nil {
				// We got it from a peer, save it locally and serve it
				defer peerBlob.Close()
				
				// Create a temporary file to buffer the content
				tmpFile, err := os.CreateTemp("", "blob-buffer-*.tmp")
				if err != nil {
					fmt.Printf("Failed to create temporary buffer file: %v\n", err)
					// If we can't buffer, just stream directly
					w.Header().Set("Content-Type", "application/octet-stream")
					w.Header().Set("Docker-Content-Digest", dgst.String())
					io.Copy(w, peerBlob)
					return
				}
				defer os.Remove(tmpFile.Name())
				defer tmpFile.Close()
				
				// Copy to the temporary file first
				if _, err := io.Copy(tmpFile, peerBlob); err != nil {
					fmt.Printf("Failed to copy to temporary file: %v\n", err)
					// If we can't buffer, just tell the client there was an error
					http.Error(w, "Failed to process content", http.StatusInternalServerError)
					return
				}
				
				// Rewind the file for reading
				if _, err := tmpFile.Seek(0, 0); err != nil {
					fmt.Printf("Failed to rewind temporary file: %v\n", err)
					http.Error(w, "Failed to process content", http.StatusInternalServerError)
					return
				}
				
				// Store locally for future
				if _, err := tmpFile.Seek(0, 0); err != nil {
					fmt.Printf("Failed to rewind temporary file for storage: %v\n", err)
				} else {
					// Store locally for future
					if err := r.StoreBlobLocally(dgst, tmpFile); err != nil {
						fmt.Printf("Failed to store blob locally: %v\n", err)
					} else {
						// Also store in DHT
						go r.StoreInDHT(context.Background(), dgst)
					}
				}
				
				// Rewind again for client response
				if _, err := tmpFile.Seek(0, 0); err != nil {
					fmt.Printf("Failed to rewind temporary file for response: %v\n", err)
					http.Error(w, "Failed to process content", http.StatusInternalServerError)
					return
				}
				
				// Write the blob content to the response
				w.Header().Set("Content-Type", "application/octet-stream")
				w.Header().Set("Docker-Content-Digest", dgst.String())
				io.Copy(w, tmpFile)
				return
			}
		}
	}
	
	// Step 3: Try to get a lease to fetch from upstream
	lease, shouldFetch, err := r.obtainLease(ctx, dgst)
	
	// If we got an error trying to obtain a lease, fall back to direct upstream
	if err != nil {
		fmt.Printf("Failed to obtain lease, falling back to direct upstream: %v\n", err)
		r.fetchAndServeFromUpstream(ctx, w, repo, dgst)
		return
	}
	
	if shouldFetch {
		// We have the lease, so we should fetch from upstream
		nodeID := "unknown"
		if r.dht != nil && r.dht.host != nil {
			nodeID = r.dht.host.ID().String()
		}
		fmt.Printf("Node %s obtained lease to fetch %s from upstream\n", nodeID, dgst.String())
		
		// Fetch from upstream, but make sure to complete the lease when done
		var fetchErr error
		defer func() {
			if lease != nil {
				// This will be called when the function returns, which happens after
				// we've served the content or encountered an error
				status := "success"
				if fetchErr != nil {
					status = "failure"
				}
				fmt.Printf("Node %s completing lease for %s with status %s\n", 
					nodeID, dgst.String(), status)
				r.completeLease(lease, fetchErr == nil, fetchErr)
			}
		}()
		
		// Do the actual fetch and serving
		fetchErr = r.fetchAndServeFromUpstream(ctx, w, repo, dgst)
		return
	} else {
		// We didn't get the lease, so we should wait for it
		nodeID := "unknown"
		if r.dht != nil && r.dht.host != nil {
			nodeID = r.dht.host.ID().String()
		}
		creatorID := lease.Creator
		fmt.Printf("Node %s waiting for node %s to fetch %s from upstream\n", 
			nodeID, creatorID, dgst.String())
		
		// Wait for the lease to complete
		err := r.waitForLease(ctx, lease)
		if err != nil {
			// The lease failed or timed out, fall back to direct upstream
			fmt.Printf("Node %s: lease wait failed, falling back to direct upstream: %v\n", 
				nodeID, err)
			r.fetchAndServeFromUpstream(ctx, w, repo, dgst)
			return
		}
		fmt.Printf("Node %s: lease completed by node %s for %s\n", 
			nodeID, creatorID, dgst.String())
		
		// The lease completed successfully, try to get the blob locally
		localBlob, err := r.GetLocalBlob(dgst)
		if err == nil {
			// We have it locally now, serve it
			defer localBlob.Close()
			
			// Write the blob content
			w.Header().Set("Content-Type", "application/octet-stream")
			w.Header().Set("Docker-Content-Digest", dgst.String())
			io.Copy(w, localBlob)
			return
		}
		
		// If we still don't have it locally, try to find it in the DHT again
		// This could happen if the node that fetched it is different from us
		peers, err := r.FindInDHT(ctx, dgst)
		if err == nil && len(peers) > 0 {
			// Try to fetch from peers
			for _, peer := range peers {
				peerBlob, err := r.FetchFromPeer(ctx, dgst, peer)
				if err == nil {
					// Process and serve the peer blob
					r.processAndServeBlob(w, peerBlob, dgst)
					return
				}
			}
		}
		
		// If we still don't have it, fall back to upstream as a last resort
		fmt.Printf("Lease completed but content not available locally or via P2P, fetching from upstream\n")
		r.fetchAndServeFromUpstream(ctx, w, repo, dgst)
		return
	}
}

// fetchAndServeFromUpstream fetches a blob from upstream and serves it
func (r *Registry) fetchAndServeFromUpstream(ctx context.Context, w http.ResponseWriter, repo string, dgst digest.Digest) error {
	// Fetch from upstream
	upstreamBlob, err := r.FetchFromUpstream(ctx, repo, dgst)
	if err != nil {
		http.Error(w, fmt.Sprintf("failed to fetch from upstream: %v", err), http.StatusInternalServerError)
		return err
	}
	defer upstreamBlob.Close()
	
	// Process and serve the blob
	return r.processAndServeBlob(w, upstreamBlob, dgst)
}

// processAndServeBlob processes a blob from a reader and serves it to the client
func (r *Registry) processAndServeBlob(w http.ResponseWriter, blob io.ReadCloser, dgst digest.Digest) error {
	// Create a temporary file to buffer the content
	tmpFile, err := os.CreateTemp("", "blob-buffer-*.tmp")
	if err != nil {
		fmt.Printf("Failed to create temporary buffer file: %v\n", err)
		// If we can't buffer, just stream directly
		w.Header().Set("Content-Type", "application/octet-stream")
		w.Header().Set("Docker-Content-Digest", dgst.String())
		_, err = io.Copy(w, blob)
		return err
	}
	defer os.Remove(tmpFile.Name())
	defer tmpFile.Close()
	
	// Copy to the temporary file first
	if _, err := io.Copy(tmpFile, blob); err != nil {
		fmt.Printf("Failed to copy to temporary file: %v\n", err)
		// If we can't buffer, just tell the client there was an error
		http.Error(w, "Failed to process content", http.StatusInternalServerError)
		return err
	}
	
	// Rewind the file for reading
	if _, err := tmpFile.Seek(0, 0); err != nil {
		fmt.Printf("Failed to rewind temporary file: %v\n", err)
		http.Error(w, "Failed to process content", http.StatusInternalServerError)
		return err
	}
	
	// Store locally for future
	if _, err := tmpFile.Seek(0, 0); err != nil {
		fmt.Printf("Failed to rewind temporary file for storage: %v\n", err)
	} else {
		// Store locally for future
		if err := r.StoreBlobLocally(dgst, tmpFile); err != nil {
			fmt.Printf("Failed to store blob locally: %v\n", err)
		} else {
			// Also store in DHT
			go r.StoreInDHT(context.Background(), dgst)
		}
	}
	
	// Rewind again for client response
	if _, err := tmpFile.Seek(0, 0); err != nil {
		fmt.Printf("Failed to rewind temporary file for response: %v\n", err)
		http.Error(w, "Failed to process content", http.StatusInternalServerError)
		return err
	}
	
	// Write the blob content to the response
	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("Docker-Content-Digest", dgst.String())
	_, err = io.Copy(w, tmpFile)
	return err
}

// StartRegistry starts the HTTP server for the registry
func (r *Registry) StartRegistry(addr string) error {
	// Create a background context for the lease cleanup routine
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel() // Cancel when the server stops
	
	// Start the lease cleanup routine
	r.startLeaseCleanupRoutine(ctx)
	
	// Create a mux for the server
	mux := http.NewServeMux()
	
	// Handle blob requests
	mux.HandleFunc("/v2/", func(w http.ResponseWriter, req *http.Request) {
		// Basic API version check
		if req.URL.Path == "/v2/" {
			w.Header().Set("Docker-Distribution-API-Version", "registry/2.0")
			w.WriteHeader(http.StatusOK)
			return
		}
		
		// Parse the URL path to extract repository and digest
		parts := strings.Split(strings.TrimPrefix(req.URL.Path, "/v2/"), "/")
		if len(parts) < 3 || parts[len(parts)-2] != "blobs" {
			http.Error(w, "invalid request path", http.StatusBadRequest)
			return
		}
		
		// Extract repository name and digest
		repo := strings.Join(parts[:len(parts)-2], "/")
		digestStr := parts[len(parts)-1]
		
		// Parse the digest
		dgst, err := digest.Parse(digestStr)
		if err != nil {
			http.Error(w, "invalid digest", http.StatusBadRequest)
			return
		}
		
		// Handle the blob request
		r.HandleBlob(w, req, repo, dgst)
	})
	
	// Create the server
	server := &http.Server{
		Addr:    addr,
		Handler: mux,
	}
	
	// Start the server
	return server.ListenAndServe()
}