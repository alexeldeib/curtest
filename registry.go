package main

import (
	"context"
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
	dht             *DHT
	upstreamURL     string
	storageDir      string
	cache           sync.Map // Cache for DHT lookups
	cacheTTL        time.Duration
	upstreamTimeout time.Duration
	mu              sync.RWMutex
}

// CacheEntry represents a cached entry for a registry blob
type CacheEntry struct {
	Peers   []string     // Peers that have this content
	Created time.Time    // When this entry was created
	Digest  digest.Digest // The content digest
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
		upstreamTimeout: 30 * time.Second,
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
	// Create a direct HTTP request to the peer
	url := fmt.Sprintf("http://%s/v2/blobs/%s", peerAddr, dgst.String())
	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	// Send the request
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch from peer: %w", err)
	}

	// Check status code
	if resp.StatusCode != http.StatusOK {
		resp.Body.Close()
		return nil, fmt.Errorf("peer returned status %d", resp.StatusCode)
	}

	return resp.Body, nil
}

// FetchFromUpstream attempts to fetch a blob from the upstream registry
func (r *Registry) FetchFromUpstream(ctx context.Context, repo string, dgst digest.Digest) (io.ReadCloser, error) {
	// Create a timeout context
	ctxTimeout, cancel := context.WithTimeout(ctx, r.upstreamTimeout)
	defer cancel()
	
	// Create a direct HTTP request to the upstream
	url := fmt.Sprintf("%s/v2/%s/blobs/%s", r.upstreamURL, repo, dgst.String())
	req, err := http.NewRequestWithContext(ctxTimeout, "GET", url, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	// Send the request
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch from upstream: %w", err)
	}

	// Check status code
	if resp.StatusCode != http.StatusOK {
		resp.Body.Close()
		return nil, fmt.Errorf("upstream returned status %d", resp.StatusCode)
	}

	return resp.Body, nil
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
	
	// Step 3: Fall back to upstream
	upstreamBlob, err := r.FetchFromUpstream(ctx, repo, dgst)
	if err == nil {
		defer upstreamBlob.Close()
		
		// Create a temporary file to buffer the content
		tmpFile, err := os.CreateTemp("", "blob-buffer-*.tmp")
		if err != nil {
			fmt.Printf("Failed to create temporary buffer file: %v\n", err)
			// If we can't buffer, just stream directly
			w.Header().Set("Content-Type", "application/octet-stream")
			w.Header().Set("Docker-Content-Digest", dgst.String())
			io.Copy(w, upstreamBlob)
			return
		}
		defer os.Remove(tmpFile.Name())
		defer tmpFile.Close()
		
		// Copy to the temporary file first
		if _, err := io.Copy(tmpFile, upstreamBlob); err != nil {
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
	
	// Nothing worked, return not found
	http.Error(w, fmt.Sprintf("blob %s not found", dgst.String()), http.StatusNotFound)
}

// StartRegistry starts the HTTP server for the registry
func (r *Registry) StartRegistry(addr string) error {
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