package main

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"os"
	"sync"
	"time"
	"unsafe"

	"github.com/opencontainers/go-digest"
	"github.com/sirupsen/logrus"
)

// #cgo CFLAGS: -I/usr/include/infiniband
// #cgo LDFLAGS: -libverbs -lrdmacm
// #include <infiniband/verbs.h>
// #include <rdma/rdma_cma.h>
// #include <rdma/rdma_verbs.h>
// #include <stdint.h>
// #include <stdlib.h>
// #include <string.h>
//
// // RDMA Connection Parameters
// #define BUFFER_SIZE 1048576  // 1MB buffer for RDMA operations
// #define MAX_WR 256           // Max work requests
// #define MAX_SGE 4            // Max scatter/gather elements
// #define CQ_CAPACITY 256      // Completion queue capacity
//
// // Message types for RDMA control communications
// typedef enum {
//     MSG_READY = 1,           // Ready to transfer
//     MSG_REQUEST_BLOB = 2,    // Request a blob
//     MSG_METADATA = 3,        // Blob metadata
//     MSG_DONE = 4,            // Transfer completed
//     MSG_ERROR = 5            // Error occurred
// } msg_type;
//
// // Control message structure
// typedef struct {
//     msg_type type;
//     uint64_t blob_size;
//     char digest[128];        // Blob digest string
//     uint32_t chunks;         // Number of chunks to transfer
//     uint32_t chunk_size;     // Size of each chunk
// } control_msg;
//
// // Memory region information
// typedef struct {
//     void *addr;
//     size_t length;
//     uint32_t lkey;
//     uint32_t rkey;
//     struct ibv_mr *mr;
// } rdma_memory_region;
//
// // RDMA server state
// typedef struct {
//     struct rdma_cm_id *listen_id;
//     struct rdma_event_channel *event_channel;
//     int is_running;
// } rdma_server;
//
// // RDMA connection state
// typedef struct {
//     struct rdma_cm_id *id;
//     struct ibv_qp *qp;
//     struct ibv_cq *cq;
//     rdma_memory_region send_region;
//     rdma_memory_region recv_region;
//     rdma_memory_region data_region;
// } rdma_connection;
//
// // Create memory region
// static int rdma_create_memory_region(rdma_connection *conn, rdma_memory_region *region, 
//                                     size_t size, struct ibv_pd *pd) {
//     void *addr = malloc(size);
//     if (!addr) {
//         return -1;
//     }
//     
//     memset(addr, 0, size);
//     
//     struct ibv_mr *mr = ibv_reg_mr(pd, addr, size, 
//                                   IBV_ACCESS_LOCAL_WRITE | 
//                                   IBV_ACCESS_REMOTE_WRITE |
//                                   IBV_ACCESS_REMOTE_READ);
//     if (!mr) {
//         free(addr);
//         return -1;
//     }
//     
//     region->addr = addr;
//     region->length = size;
//     region->mr = mr;
//     region->lkey = mr->lkey;
//     region->rkey = mr->rkey;
//     
//     return 0;
// }
//
// // Destroy memory region
// static void rdma_destroy_memory_region(rdma_memory_region *region) {
//     if (region->mr) {
//         ibv_dereg_mr(region->mr);
//     }
//     
//     if (region->addr) {
//         free(region->addr);
//     }
//     
//     memset(region, 0, sizeof(rdma_memory_region));
// }
//
// // Initialize RDMA connection
// static int rdma_init_connection(rdma_connection *conn) {
//     struct ibv_qp_init_attr qp_attr;
//     
//     // Create completion queue
//     conn->cq = ibv_create_cq(conn->id->verbs, CQ_CAPACITY, NULL, NULL, 0);
//     if (!conn->cq) {
//         return -1;
//     }
//     
//     // Create queue pair
//     memset(&qp_attr, 0, sizeof(qp_attr));
//     qp_attr.send_cq = conn->cq;
//     qp_attr.recv_cq = conn->cq;
//     qp_attr.qp_type = IBV_QPT_RC;
//     qp_attr.cap.max_send_wr = MAX_WR;
//     qp_attr.cap.max_recv_wr = MAX_WR;
//     qp_attr.cap.max_send_sge = MAX_SGE;
//     qp_attr.cap.max_recv_sge = MAX_SGE;
//     
//     int ret = rdma_create_qp(conn->id, NULL, &qp_attr);
//     if (ret) {
//         return ret;
//     }
//     
//     conn->qp = conn->id->qp;
//     
//     // Create memory regions
//     ret = rdma_create_memory_region(conn, &conn->send_region, sizeof(control_msg), conn->id->pd);
//     if (ret) {
//         return ret;
//     }
//     
//     ret = rdma_create_memory_region(conn, &conn->recv_region, sizeof(control_msg), conn->id->pd);
//     if (ret) {
//         return ret;
//     }
//     
//     ret = rdma_create_memory_region(conn, &conn->data_region, BUFFER_SIZE, conn->id->pd);
//     if (ret) {
//         return ret;
//     }
//     
//     // Post initial receive
//     struct ibv_sge sge;
//     memset(&sge, 0, sizeof(sge));
//     sge.addr = (uint64_t)conn->recv_region.addr;
//     sge.length = sizeof(control_msg);
//     sge.lkey = conn->recv_region.lkey;
//     
//     struct ibv_recv_wr recv_wr, *bad_wr;
//     memset(&recv_wr, 0, sizeof(recv_wr));
//     recv_wr.wr_id = 0;
//     recv_wr.sg_list = &sge;
//     recv_wr.num_sge = 1;
//     
//     ret = ibv_post_recv(conn->qp, &recv_wr, &bad_wr);
//     return ret;
// }
//
// // Clean up RDMA connection
// static void rdma_cleanup_connection(rdma_connection *conn) {
//     if (!conn) {
//         return;
//     }
//     
//     rdma_destroy_memory_region(&conn->send_region);
//     rdma_destroy_memory_region(&conn->recv_region);
//     rdma_destroy_memory_region(&conn->data_region);
//     
//     if (conn->qp) {
//         ibv_destroy_qp(conn->qp);
//     }
//     
//     if (conn->cq) {
//         ibv_destroy_cq(conn->cq);
//     }
//     
//     if (conn->id) {
//         rdma_destroy_id(conn->id);
//     }
// }
//
// // Send control message
// static int rdma_send_msg(rdma_connection *conn, msg_type type, 
//                         const char* digest_str, uint64_t blob_size,
//                         uint32_t chunks, uint32_t chunk_size) {
//     control_msg *msg = (control_msg*)conn->send_region.addr;
//     msg->type = type;
//     msg->blob_size = blob_size;
//     msg->chunks = chunks;
//     msg->chunk_size = chunk_size;
//     
//     if (digest_str) {
//         strncpy(msg->digest, digest_str, sizeof(msg->digest) - 1);
//         msg->digest[sizeof(msg->digest) - 1] = '\0';
//     } else {
//         msg->digest[0] = '\0';
//     }
//     
//     struct ibv_sge sge;
//     memset(&sge, 0, sizeof(sge));
//     sge.addr = (uint64_t)conn->send_region.addr;
//     sge.length = sizeof(control_msg);
//     sge.lkey = conn->send_region.lkey;
//     
//     struct ibv_send_wr send_wr, *bad_wr;
//     memset(&send_wr, 0, sizeof(send_wr));
//     send_wr.wr_id = 1;
//     send_wr.sg_list = &sge;
//     send_wr.num_sge = 1;
//     send_wr.opcode = IBV_WR_SEND;
//     send_wr.send_flags = IBV_SEND_SIGNALED;
//     
//     int ret = ibv_post_send(conn->qp, &send_wr, &bad_wr);
//     if (ret) {
//         return ret;
//     }
//     
//     // Wait for completion
//     struct ibv_wc wc;
//     int num_completions = 0;
//     
//     do {
//         num_completions = ibv_poll_cq(conn->cq, 1, &wc);
//     } while (num_completions == 0);
//     
//     if (num_completions < 0) {
//         return -1;
//     }
//     
//     if (wc.status != IBV_WC_SUCCESS) {
//         return -1;
//     }
//     
//     return 0;
// }
//
// // Post a receive for control messages
// static int rdma_post_recv(rdma_connection *conn) {
//     struct ibv_sge sge;
//     memset(&sge, 0, sizeof(sge));
//     sge.addr = (uint64_t)conn->recv_region.addr;
//     sge.length = sizeof(control_msg);
//     sge.lkey = conn->recv_region.lkey;
//     
//     struct ibv_recv_wr recv_wr, *bad_wr;
//     memset(&recv_wr, 0, sizeof(recv_wr));
//     recv_wr.wr_id = 0;
//     recv_wr.sg_list = &sge;
//     recv_wr.num_sge = 1;
//     
//     return ibv_post_recv(conn->qp, &recv_wr, &bad_wr);
// }
//
// // Receive control message
// static int rdma_recv_msg(rdma_connection *conn, control_msg *msg) {
//     struct ibv_wc wc;
//     int num_completions = 0;
//     
//     // Wait for completion
//     do {
//         num_completions = ibv_poll_cq(conn->cq, 1, &wc);
//     } while (num_completions == 0);
//     
//     if (num_completions < 0) {
//         return -1;
//     }
//     
//     if (wc.status != IBV_WC_SUCCESS) {
//         return -1;
//     }
//     
//     // Copy message
//     if (msg) {
//         memcpy(msg, conn->recv_region.addr, sizeof(control_msg));
//     }
//     
//     // Post another receive
//     return rdma_post_recv(conn);
// }
//
// // RDMA write operation - writes data from local data region to remote data region
// static int rdma_write_data(rdma_connection *conn, uint32_t length, uint32_t remote_rkey, uint64_t remote_addr) {
//     struct ibv_sge sge;
//     memset(&sge, 0, sizeof(sge));
//     sge.addr = (uint64_t)conn->data_region.addr;
//     sge.length = length;
//     sge.lkey = conn->data_region.lkey;
//     
//     struct ibv_send_wr send_wr, *bad_wr;
//     memset(&send_wr, 0, sizeof(send_wr));
//     send_wr.wr_id = 2;
//     send_wr.sg_list = &sge;
//     send_wr.num_sge = 1;
//     send_wr.opcode = IBV_WR_RDMA_WRITE;
//     send_wr.send_flags = IBV_SEND_SIGNALED;
//     send_wr.wr.rdma.remote_addr = remote_addr;
//     send_wr.wr.rdma.rkey = remote_rkey;
//     
//     int ret = ibv_post_send(conn->qp, &send_wr, &bad_wr);
//     if (ret) {
//         return ret;
//     }
//     
//     // Wait for completion
//     struct ibv_wc wc;
//     int num_completions = 0;
//     
//     do {
//         num_completions = ibv_poll_cq(conn->cq, 1, &wc);
//     } while (num_completions == 0);
//     
//     if (num_completions < 0) {
//         return -1;
//     }
//     
//     if (wc.status != IBV_WC_SUCCESS) {
//         return -1;
//     }
//     
//     return 0;
// }
//
// // RDMA read operation - reads data from remote data region to local data region
// static int rdma_read_data(rdma_connection *conn, uint32_t length, uint32_t remote_rkey, uint64_t remote_addr) {
//     struct ibv_sge sge;
//     memset(&sge, 0, sizeof(sge));
//     sge.addr = (uint64_t)conn->data_region.addr;
//     sge.length = length;
//     sge.lkey = conn->data_region.lkey;
//     
//     struct ibv_send_wr send_wr, *bad_wr;
//     memset(&send_wr, 0, sizeof(send_wr));
//     send_wr.wr_id = 3;
//     send_wr.sg_list = &sge;
//     send_wr.num_sge = 1;
//     send_wr.opcode = IBV_WR_RDMA_READ;
//     send_wr.send_flags = IBV_SEND_SIGNALED;
//     send_wr.wr.rdma.remote_addr = remote_addr;
//     send_wr.wr.rdma.rkey = remote_rkey;
//     
//     int ret = ibv_post_send(conn->qp, &send_wr, &bad_wr);
//     if (ret) {
//         return ret;
//     }
//     
//     // Wait for completion
//     struct ibv_wc wc;
//     int num_completions = 0;
//     
//     do {
//         num_completions = ibv_poll_cq(conn->cq, 1, &wc);
//     } while (num_completions == 0);
//     
//     if (num_completions < 0) {
//         return -1;
//     }
//     
//     if (wc.status != IBV_WC_SUCCESS) {
//         return -1;
//     }
//     
//     return 0;
// }
import "C"

// RDMATransport represents a transport for content distribution using RDMA
type RDMATransport struct {
	registry      *Registry
	listenAddr    string
	listenPort    int
	isRunning     bool
	connections   map[string]*RDMAConnection // Map of peer ID to connection
	connectionsMu sync.RWMutex
	eventChan     chan RDMAEvent
	closeOnce     sync.Once
	closeChan     chan struct{}
	log           *logrus.Logger
}

// RDMAConnection represents an RDMA connection to a peer
type RDMAConnection struct {
	peerAddr  string
	conn      *C.rdma_connection
	transport *RDMATransport
	active    bool
}

// RDMAEvent represents events from the RDMA transport
type RDMAEvent struct {
	Type      RDMAEventType
	PeerAddr  string
	Digest    digest.Digest
	Size      int64
	Error     error
	Timestamp time.Time
}

// RDMAEventType represents the type of RDMA event
type RDMAEventType int

const (
	RDMAEventConnected RDMAEventType = iota
	RDMAEventDisconnected
	RDMAEventBlobRequested
	RDMAEventBlobProvided
	RDMAEventError
)

// RDMATransferProgress represents progress of an RDMA transfer
type RDMATransferProgress struct {
	Digest    digest.Digest
	Total     int64
	Completed int64
	StartTime time.Time
	EndTime   time.Time
}

// NewRDMATransport creates a new RDMA transport
func NewRDMATransport(registry *Registry, listenAddr string, listenPort int) (*RDMATransport, error) {
	// Check if RDMA is available on this system
	if !IsRDMAAvailable() {
		return nil, fmt.Errorf("RDMA not available on this system")
	}

	transport := &RDMATransport{
		registry:    registry,
		listenAddr:  listenAddr,
		listenPort:  listenPort,
		connections: make(map[string]*RDMAConnection),
		eventChan:   make(chan RDMAEvent, 100),
		closeChan:   make(chan struct{}),
		log:         logrus.New(),
	}

	// Configure logger
	transport.log.SetLevel(logrus.InfoLevel)
	transport.log.SetFormatter(&logrus.TextFormatter{
		FullTimestamp: true,
	})

	return transport, nil
}

// IsRDMAAvailable checks if RDMA is available on the system
func IsRDMAAvailable() bool {
	// Check if necessary RDMA devices exist
	_, err := os.Stat("/dev/infiniband/uverbs0")
	return err == nil
}

// Start starts the RDMA transport server
func (t *RDMATransport) Start(ctx context.Context) error {
	if t.isRunning {
		return fmt.Errorf("RDMA transport already running")
	}

	// Create event channel and listening ID
	var server C.rdma_server
	server.event_channel = C.rdma_create_event_channel()
	if server.event_channel == nil {
		return fmt.Errorf("failed to create RDMA event channel")
	}

	port := C.uint16_t(t.listenPort)
	var addr C.struct_sockaddr_in
	addr.sin_family = C.AF_INET
	addr.sin_port = C.htons(port)
	addr.sin_addr.s_addr = C.htonl(C.INADDR_ANY)

	// Create RDMA ID for listening
	ret := C.rdma_create_id(server.event_channel, &server.listen_id, nil, C.RDMA_PS_TCP)
	if ret != 0 {
		return fmt.Errorf("failed to create RDMA ID: %d", ret)
	}

	// Bind address
	ret = C.rdma_bind_addr(server.listen_id, (*C.struct_sockaddr)(unsafe.Pointer(&addr)))
	if ret != 0 {
		return fmt.Errorf("failed to bind RDMA address: %d", ret)
	}

	// Listen for connections
	ret = C.rdma_listen(server.listen_id, 10) // Backlog of 10 connections
	if ret != 0 {
		return fmt.Errorf("failed to listen for RDMA connections: %d", ret)
	}

	t.isRunning = true
	server.is_running = 1

	// Start accepting connections
	go t.acceptLoop(ctx, &server)
	go t.processEvents(ctx)

	t.log.Infof("RDMA transport server started on %s:%d", t.listenAddr, t.listenPort)
	return nil
}

// acceptLoop accepts incoming RDMA connections
func (t *RDMATransport) acceptLoop(ctx context.Context, server *C.rdma_server) {
	for {
		// Check if server was closed
		if server.is_running == 0 {
			break
		}

		// Check if context was cancelled
		select {
		case <-ctx.Done():
			t.log.Info("RDMA accept loop stopping due to context cancellation")
			return
		case <-t.closeChan:
			t.log.Info("RDMA accept loop stopping due to transport close")
			return
		default:
			// Continue accepting
		}

		// Accept connection with timeout
		var event C.struct_rdma_cm_event
		// Use poll to implement timeout
		fds := C.struct_pollfd{
			fd:      C.rdma_get_fd(server.event_channel),
			events:  C.POLLIN,
			revents: 0,
		}
		ret := C.poll(&fds, 1, 1000) // 1 second timeout
		if ret <= 0 {
			continue // Timeout or error
		}

		// Get event
		ret = C.rdma_get_cm_event(server.event_channel, &event)
		if ret != 0 {
			t.log.Errorf("Failed to get RDMA CM event: %d", ret)
			continue
		}

		// Process event
		if event.event == C.RDMA_CM_EVENT_CONNECT_REQUEST {
			t.handleConnectionRequest(server, &event)
		}

		// Ack event
		C.rdma_ack_cm_event(&event)
	}
}

// handleConnectionRequest handles RDMA connection requests
func (t *RDMATransport) handleConnectionRequest(server *C.rdma_server, event *C.struct_rdma_cm_event) {
	// Create connection structure
	conn := &C.rdma_connection{
		id: event.id,
	}

	// Initialize connection
	ret := C.rdma_init_connection(conn)
	if ret != 0 {
		t.log.Errorf("Failed to initialize RDMA connection: %d", ret)
		C.rdma_destroy_id(event.id)
		return
	}

	// Accept connection
	ret = C.rdma_accept(event.id, nil)
	if ret != 0 {
		t.log.Errorf("Failed to accept RDMA connection: %d", ret)
		C.rdma_cleanup_connection(conn)
		return
	}

	// Get peer address
	var addr C.struct_sockaddr_in
	addrLen := C.socklen_t(C.sizeof_struct_sockaddr_in)
	C.getpeername(C.rdma_get_recv_fd(event.id), (*C.struct_sockaddr)(unsafe.Pointer(&addr)), &addrLen)
	peerAddr := fmt.Sprintf("%d.%d.%d.%d:%d",
		uint8(addr.sin_addr.s_addr&0xFF),
		uint8((addr.sin_addr.s_addr>>8)&0xFF),
		uint8((addr.sin_addr.s_addr>>16)&0xFF),
		uint8((addr.sin_addr.s_addr>>24)&0xFF),
		C.ntohs(addr.sin_port))

	// Create connection object
	rdmaConn := &RDMAConnection{
		peerAddr:  peerAddr,
		conn:      conn,
		transport: t,
		active:    true,
	}

	// Add to connections map
	t.connectionsMu.Lock()
	t.connections[peerAddr] = rdmaConn
	t.connectionsMu.Unlock()

	// Send event
	t.eventChan <- RDMAEvent{
		Type:      RDMAEventConnected,
		PeerAddr:  peerAddr,
		Timestamp: time.Now(),
	}

	// Start connection handler
	go t.handleConnection(rdmaConn)
}

// handleConnection handles an established RDMA connection
func (t *RDMATransport) handleConnection(conn *RDMAConnection) {
	t.log.Infof("Handling RDMA connection from %s", conn.peerAddr)

	// Send ready message
	ret := C.rdma_send_msg(conn.conn, C.MSG_READY, nil, 0, 0, 0)
	if ret != 0 {
		t.log.Errorf("Failed to send ready message: %d", ret)
		t.closeConnection(conn)
		return
	}

	// Loop for messages
	for conn.active {
		var msg C.control_msg
		ret = C.rdma_recv_msg(conn.conn, &msg)
		if ret != 0 {
			t.log.Errorf("Failed to receive message: %d", ret)
			t.closeConnection(conn)
			return
		}

		// Handle message based on type
		switch msg.type {
		case C.MSG_REQUEST_BLOB:
			t.handleBlobRequest(conn, &msg)
		case C.MSG_DONE:
			// Peer is done with this blob request
			t.log.Infof("Peer %s completed blob transfer", conn.peerAddr)
		case C.MSG_ERROR:
			// Peer encountered an error
			t.log.Warnf("Peer %s reported error during transfer", conn.peerAddr)
		default:
			t.log.Warnf("Received unknown message type: %d", msg.type)
		}
	}
}

// handleBlobRequest handles a request for a blob via RDMA
func (t *RDMATransport) handleBlobRequest(conn *RDMAConnection, msg *C.control_msg) {
	digestStr := C.GoString(&msg.digest[0])
	dgst, err := digest.Parse(digestStr)
	if err != nil {
		t.log.Errorf("Invalid digest in blob request: %s", digestStr)
		C.rdma_send_msg(conn.conn, C.MSG_ERROR, nil, 0, 0, 0)
		return
	}

	t.log.Infof("Received request for blob %s from %s", dgst, conn.peerAddr)

	// Send event for blob request
	t.eventChan <- RDMAEvent{
		Type:      RDMAEventBlobRequested,
		PeerAddr:  conn.peerAddr,
		Digest:    dgst,
		Timestamp: time.Now(),
	}

	// Check if we have the blob locally
	blob, err := t.registry.GetLocalBlob(dgst)
	if err != nil {
		t.log.Errorf("Blob %s not found locally: %v", dgst, err)
		C.rdma_send_msg(conn.conn, C.MSG_ERROR, nil, 0, 0, 0)
		return
	}
	defer blob.Close()

	// Get blob size
	blobInfo, err := os.Stat(t.registry.getBlobPath(dgst))
	if err != nil {
		t.log.Errorf("Failed to get blob size: %v", err)
		C.rdma_send_msg(conn.conn, C.MSG_ERROR, nil, 0, 0, 0)
		return
	}
	blobSize := blobInfo.Size()

	// Calculate number of chunks and chunk size
	chunkSize := int64(C.BUFFER_SIZE)
	chunks := (blobSize + chunkSize - 1) / chunkSize // ceiling division
	lastChunkSize := blobSize % chunkSize
	if lastChunkSize == 0 && blobSize > 0 {
		lastChunkSize = chunkSize
	}

	// Send metadata message with blob size
	ret := C.rdma_send_msg(conn.conn, C.MSG_METADATA, C.CString(dgst.String()),
		C.uint64_t(blobSize), C.uint32_t(chunks), C.uint32_t(chunkSize))
	if ret != 0 {
		t.log.Errorf("Failed to send metadata message: %d", ret)
		return
	}

	// Wait for control message acknowledgement
	var ackMsg C.control_msg
	ret = C.rdma_recv_msg(conn.conn, &ackMsg)
	if ret != 0 || ackMsg.type != C.MSG_READY {
		t.log.Errorf("Failed to receive ACK for metadata: %d", ret)
		return
	}

	// Create progress tracker
	progress := &RDMATransferProgress{
		Digest:    dgst,
		Total:     blobSize,
		StartTime: time.Now(),
	}

	// Read blob in chunks and send via RDMA
	buffer := make([]byte, chunkSize)
	var totalSent int64

	for chunkIdx := int64(0); chunkIdx < chunks; chunkIdx++ {
		// Calculate current chunk size
		currentChunkSize := chunkSize
		if chunkIdx == chunks-1 && lastChunkSize > 0 {
			currentChunkSize = lastChunkSize
		}

		// Read chunk from blob
		n, err := io.ReadFull(blob, buffer[:currentChunkSize])
		if err != nil && err != io.ErrUnexpectedEOF && err != io.EOF {
			t.log.Errorf("Failed to read blob chunk: %v", err)
			C.rdma_send_msg(conn.conn, C.MSG_ERROR, nil, 0, 0, 0)
			return
		}

		// Copy to RDMA buffer
		C.memcpy(conn.conn.data_region.addr, unsafe.Pointer(&buffer[0]), C.size_t(n))

		// Send data via RDMA
		ret = C.rdma_write_data(conn.conn, C.uint32_t(n), ackMsg.rkey, C.uint64_t(chunkIdx*chunkSize))
		if ret != 0 {
			t.log.Errorf("Failed to write data via RDMA: %d", ret)
			C.rdma_send_msg(conn.conn, C.MSG_ERROR, nil, 0, 0, 0)
			return
		}

		totalSent += int64(n)
		progress.Completed = totalSent

		// Notify of chunk completion
		t.log.Debugf("Sent chunk %d/%d (%d bytes) for blob %s", 
			chunkIdx+1, chunks, n, dgst)
	}

	// Send completion notification
	ret = C.rdma_send_msg(conn.conn, C.MSG_DONE, nil, C.uint64_t(totalSent), 0, 0)
	if ret != 0 {
		t.log.Errorf("Failed to send completion message: %d", ret)
		return
	}

	// Update progress
	progress.EndTime = time.Now()
	transferTime := progress.EndTime.Sub(progress.StartTime)
	transferRateMBps := float64(blobSize) / transferTime.Seconds() / 1024 / 1024

	t.log.Infof("Completed RDMA transfer of blob %s to %s: %.2f MB/s", 
		dgst, conn.peerAddr, transferRateMBps)

	// Send event for successful transfer
	t.eventChan <- RDMAEvent{
		Type:      RDMAEventBlobProvided,
		PeerAddr:  conn.peerAddr,
		Digest:    dgst,
		Size:      blobSize,
		Timestamp: time.Now(),
	}
}

// FetchBlobViaRDMA fetches a blob from a peer via RDMA
func (t *RDMATransport) FetchBlobViaRDMA(ctx context.Context, peerAddr string, dgst digest.Digest) (io.ReadCloser, error) {
	// Establish connection if not already connected
	conn, err := t.connectTo(ctx, peerAddr)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to peer: %w", err)
	}

	// Send blob request
	ret := C.rdma_send_msg(conn.conn, C.MSG_REQUEST_BLOB, C.CString(dgst.String()), 0, 0, 0)
	if ret != 0 {
		return nil, fmt.Errorf("failed to send blob request: %d", ret)
	}

	// Wait for metadata response
	var metaMsg C.control_msg
	ret = C.rdma_recv_msg(conn.conn, &metaMsg)
	if ret != 0 {
		return nil, fmt.Errorf("failed to receive metadata: %d", ret)
	}

	if metaMsg.type == C.MSG_ERROR {
		return nil, fmt.Errorf("peer returned error for blob %s", dgst)
	}

	if metaMsg.type != C.MSG_METADATA {
		return nil, fmt.Errorf("expected metadata message, got type %d", metaMsg.type)
	}

	// Get blob details
	blobSize := int64(metaMsg.blob_size)
	chunks := int(metaMsg.chunks)
	chunkSize := int(metaMsg.chunk_size)

	// Create temporary file to store the blob
	tmpFile, err := os.CreateTemp("", fmt.Sprintf("rdma-blob-%s-*.tmp", dgst))
	if err != nil {
		return nil, fmt.Errorf("failed to create temporary file: %w", err)
	}

	// Create buffer for receiving chunks
	buffer := make([]byte, chunkSize)

	// Send ready message with our memory region key
	ret = C.rdma_send_msg(conn.conn, C.MSG_READY, nil, 0, 0, C.uint32_t(conn.conn.data_region.rkey))
	if ret != 0 {
		tmpFile.Close()
		os.Remove(tmpFile.Name())
		return nil, fmt.Errorf("failed to send ready message: %d", ret)
	}

	// Create a pipe for streaming data while receiving via RDMA
	pr, pw := io.Pipe()

	// Start a goroutine to write data to the pipe as it's received
	go func() {
		defer pw.Close()
		var totalReceived int64

		for chunkIdx := 0; chunkIdx < chunks; chunkIdx++ {
			// Calculate current chunk size
			currentChunkSize := int64(chunkSize)
			if chunkIdx == chunks-1 && blobSize%int64(chunkSize) > 0 {
				currentChunkSize = blobSize % int64(chunkSize)
			}

			// Get data via RDMA
			ret = C.rdma_read_data(conn.conn, C.uint32_t(currentChunkSize), 
				metaMsg.rkey, C.uint64_t(chunkIdx*chunkSize))
			if ret != 0 {
				t.log.Errorf("Failed to read data via RDMA: %d", ret)
				pw.CloseWithError(fmt.Errorf("RDMA read failed"))
				return
			}

			// Copy from RDMA buffer to our buffer
			C.memcpy(unsafe.Pointer(&buffer[0]), conn.conn.data_region.addr, C.size_t(currentChunkSize))

			// Write to the pipe
			n, err := pw.Write(buffer[:currentChunkSize])
			if err != nil {
				t.log.Errorf("Failed to write data to pipe: %v", err)
				pw.CloseWithError(err)
				return
			}

			// Write to the file in parallel
			_, err = tmpFile.Write(buffer[:currentChunkSize])
			if err != nil {
				t.log.Errorf("Failed to write data to file: %v", err)
				pw.CloseWithError(err)
				return
			}

			totalReceived += int64(n)
			t.log.Debugf("Received chunk %d/%d (%d bytes) for blob %s",
				chunkIdx+1, chunks, n, dgst)
		}

		// Wait for completion message
		var doneMsg C.control_msg
		ret = C.rdma_recv_msg(conn.conn, &doneMsg)
		if ret != 0 || doneMsg.type != C.MSG_DONE {
			pw.CloseWithError(fmt.Errorf("failed to receive completion message"))
			return
		}

		if int64(doneMsg.blob_size) != totalReceived {
			t.log.Warnf("Size mismatch: expected %d, received %d", doneMsg.blob_size, totalReceived)
		}

		// Store blob locally for future use after completion
		err := os.Rename(tmpFile.Name(), t.registry.getBlobPath(dgst))
		if err != nil {
			t.log.Errorf("Failed to store blob locally: %v", err)
		} else {
			// Announce to DHT
			go t.registry.StoreInDHT(context.Background(), dgst)
		}

		// Send completion acknowledgement
		C.rdma_send_msg(conn.conn, C.MSG_DONE, nil, 0, 0, 0)
	}()

	// Return a reader that will close the file when done
	return &rdmaReadCloser{
		ReadCloser: pr,
		tempFile:   tmpFile,
		cleanup: func() {
			tmpFile.Close()
			// Note: We don't remove the file here as it's either renamed or will be removed on error
		},
	}, nil
}

// rdmaReadCloser manages a ReadCloser for RDMA transfers with cleanup
type rdmaReadCloser struct {
	io.ReadCloser
	tempFile *os.File
	cleanup  func()
}

// Close implements io.Closer
func (r *rdmaReadCloser) Close() error {
	err := r.ReadCloser.Close()
	r.cleanup()
	return err
}

// connectTo establishes an RDMA connection to a peer
func (t *RDMATransport) connectTo(ctx context.Context, peerAddr string) (*RDMAConnection, error) {
	// Check if already connected
	t.connectionsMu.RLock()
	conn, exists := t.connections[peerAddr]
	t.connectionsMu.RUnlock()

	if exists && conn.active {
		return conn, nil
	}

	// Parse peer address
	host, portStr, err := net.SplitHostPort(peerAddr)
	if err != nil {
		return nil, fmt.Errorf("invalid peer address: %w", err)
	}
	port, err := binary.ParseUint(portStr, 10, 16)
	if err != nil {
		return nil, fmt.Errorf("invalid port: %w", err)
	}

	// Create event channel
	eventChannel := C.rdma_create_event_channel()
	if eventChannel == nil {
		return nil, fmt.Errorf("failed to create RDMA event channel")
	}

	// Create RDMA ID
	var id *C.struct_rdma_cm_id
	ret := C.rdma_create_id(eventChannel, &id, nil, C.RDMA_PS_TCP)
	if ret != 0 {
		C.rdma_destroy_event_channel(eventChannel)
		return nil, fmt.Errorf("failed to create RDMA ID: %d", ret)
	}

	// Resolve address
	var addr C.struct_sockaddr_in
	addr.sin_family = C.AF_INET
	addr.sin_port = C.htons(C.uint16_t(port))

	// Convert IP address
	ipAddrs, err := net.LookupHost(host)
	if err != nil {
		C.rdma_destroy_id(id)
		C.rdma_destroy_event_channel(eventChannel)
		return nil, fmt.Errorf("failed to resolve hostname: %w", err)
	}
	if len(ipAddrs) == 0 {
		C.rdma_destroy_id(id)
		C.rdma_destroy_event_channel(eventChannel)
		return nil, fmt.Errorf("no IP addresses for host: %s", host)
	}

	ip := net.ParseIP(ipAddrs[0]).To4()
	if ip == nil {
		C.rdma_destroy_id(id)
		C.rdma_destroy_event_channel(eventChannel)
		return nil, fmt.Errorf("invalid IPv4 address: %s", ipAddrs[0])
	}

	addr.sin_addr.s_addr = C.htonl(C.uint32_t(
		uint32(ip[0])<<24 | uint32(ip[1])<<16 | uint32(ip[2])<<8 | uint32(ip[3])))

	// Resolve address
	ret = C.rdma_resolve_addr(id, nil, (*C.struct_sockaddr)(unsafe.Pointer(&addr)), 2000) // 2 second timeout
	if ret != 0 {
		C.rdma_destroy_id(id)
		C.rdma_destroy_event_channel(eventChannel)
		return nil, fmt.Errorf("failed to resolve address: %d", ret)
	}

	// Wait for address resolution
	var event C.struct_rdma_cm_event
	ret = C.rdma_get_cm_event(eventChannel, &event)
	if ret != 0 || event.event != C.RDMA_CM_EVENT_ADDR_RESOLVED {
		C.rdma_destroy_id(id)
		C.rdma_destroy_event_channel(eventChannel)
		return nil, fmt.Errorf("failed to resolve address: %d", ret)
	}
	C.rdma_ack_cm_event(&event)

	// Resolve route
	ret = C.rdma_resolve_route(id, 2000) // 2 second timeout
	if ret != 0 {
		C.rdma_destroy_id(id)
		C.rdma_destroy_event_channel(eventChannel)
		return nil, fmt.Errorf("failed to resolve route: %d", ret)
	}

	// Wait for route resolution
	ret = C.rdma_get_cm_event(eventChannel, &event)
	if ret != 0 || event.event != C.RDMA_CM_EVENT_ROUTE_RESOLVED {
		C.rdma_destroy_id(id)
		C.rdma_destroy_event_channel(eventChannel)
		return nil, fmt.Errorf("failed to resolve route: %d", ret)
	}
	C.rdma_ack_cm_event(&event)

	// Create connection structure
	connObj := &C.rdma_connection{
		id: id,
	}

	// Initialize connection
	ret = C.rdma_init_connection(connObj)
	if ret != 0 {
		C.rdma_destroy_id(id)
		C.rdma_destroy_event_channel(eventChannel)
		return nil, fmt.Errorf("failed to initialize connection: %d", ret)
	}

	// Connect
	ret = C.rdma_connect(id, nil)
	if ret != 0 {
		C.rdma_cleanup_connection(connObj)
		C.rdma_destroy_event_channel(eventChannel)
		return nil, fmt.Errorf("failed to connect: %d", ret)
	}

	// Wait for connection
	ret = C.rdma_get_cm_event(eventChannel, &event)
	if ret != 0 || event.event != C.RDMA_CM_EVENT_ESTABLISHED {
		C.rdma_cleanup_connection(connObj)
		C.rdma_destroy_event_channel(eventChannel)
		return nil, fmt.Errorf("failed to establish connection: %d", ret)
	}
	C.rdma_ack_cm_event(&event)

	// Create connection object
	rdmaConn := &RDMAConnection{
		peerAddr:  peerAddr,
		conn:      connObj,
		transport: t,
		active:    true,
	}

	// Add to connections map
	t.connectionsMu.Lock()
	t.connections[peerAddr] = rdmaConn
	t.connectionsMu.Unlock()

	// Send event
	t.eventChan <- RDMAEvent{
		Type:      RDMAEventConnected,
		PeerAddr:  peerAddr,
		Timestamp: time.Now(),
	}

	// Wait for ready message from the server
	var readyMsg C.control_msg
	ret = C.rdma_recv_msg(rdmaConn.conn, &readyMsg)
	if ret != 0 || readyMsg.type != C.MSG_READY {
		t.closeConnection(rdmaConn)
		return nil, fmt.Errorf("server did not send ready message")
	}

	return rdmaConn, nil
}

// processEvents processes events from the RDMA transport
func (t *RDMATransport) processEvents(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.closeChan:
			return
		case event := <-t.eventChan:
			// Process event
			switch event.Type {
			case RDMAEventConnected:
				t.log.Infof("RDMA connection established with %s", event.PeerAddr)
			case RDMAEventDisconnected:
				t.log.Infof("RDMA connection closed with %s", event.PeerAddr)
			case RDMAEventBlobRequested:
				t.log.Infof("Blob %s requested by %s", event.Digest, event.PeerAddr)
			case RDMAEventBlobProvided:
				t.log.Infof("Blob %s (size: %d) provided to %s", 
					event.Digest, event.Size, event.PeerAddr)
			case RDMAEventError:
				t.log.Errorf("RDMA error with %s: %v", event.PeerAddr, event.Error)
			}
		}
	}
}

// closeConnection closes an RDMA connection
func (t *RDMATransport) closeConnection(conn *RDMAConnection) {
	if conn == nil || !conn.active {
		return
	}

	conn.active = false

	// Cleanup connection resources
	C.rdma_cleanup_connection(conn.conn)

	// Remove from connections map
	t.connectionsMu.Lock()
	delete(t.connections, conn.peerAddr)
	t.connectionsMu.Unlock()

	// Send event
	t.eventChan <- RDMAEvent{
		Type:      RDMAEventDisconnected,
		PeerAddr:  conn.peerAddr,
		Timestamp: time.Now(),
	}
}

// Close stops the RDMA transport and releases resources
func (t *RDMATransport) Close() error {
	t.closeOnce.Do(func() {
		// Signal close
		close(t.closeChan)

		// Close all connections
		t.connectionsMu.Lock()
		for _, conn := range t.connections {
			t.closeConnection(conn)
		}
		t.connectionsMu.Unlock()

		t.isRunning = false
	})
	return nil
}

// RegisterRDMATransport registers RDMA as a transport with the registry
func RegisterRDMATransport(ctx context.Context, registry *Registry, listenAddr string, listenPort int) error {
	// Create RDMA transport
	transport, err := NewRDMATransport(registry, listenAddr, listenPort)
	if err != nil {
		return fmt.Errorf("failed to create RDMA transport: %w", err)
	}

	// Start transport
	err = transport.Start(ctx)
	if err != nil {
		return fmt.Errorf("failed to start RDMA transport: %w", err)
	}

	// TODO: Register with registry for content retrieval

	return nil
}