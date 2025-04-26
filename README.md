# FastReg

A lightweight service discovery and registration tool built on libp2p's Kademlia DHT.

## Getting Started

### Building

```bash
go build -o fastreg
```

### Running

Start a node:

```bash
./fastreg
```

By default, the node will listen on a random port. You can specify one or more addresses to listen on:

```bash
./fastreg -listen /ip4/0.0.0.0/tcp/8000
```

To start a node and bootstrap it with another node:

```bash
./fastreg -bootstrap /ip4/192.168.1.100/tcp/8000/p2p/QmNodeID
```

You can also specify a service tag for peer discovery (default is "fastreg"):

```bash
./fastreg -service my-app-cluster
```

## Features

- Decentralized peer discovery using Kademlia DHT
- No central registry required
- Automatic peer discovery within the same network
- Service-based node grouping
- Distributed key-value storage and retrieval
- Mesh network formation independent of bootstrap nodes
- Docker registry mirror with P2P content distribution

## Example Usage

1. Start a bootstrap node:
   ```bash
   ./fastreg -listen /ip4/0.0.0.0/tcp/8000
   ```

2. Note the address printed by the bootstrap node, which will include its peer ID.

3. Start additional nodes that connect to the bootstrap node:
   ```bash
   ./fastreg -bootstrap /ip4/192.168.1.100/tcp/8000/p2p/QmNodeID
   ```

Nodes will automatically discover each other through the DHT, even if they're not directly connected to the bootstrap node.

## Key-Value Storage and Retrieval

FastReg allows storing and retrieving values in the distributed hash table, making it useful for service discovery and configuration sharing:

```go
// Store a value
err := node.PutValue(ctx, "/fastreg/mykey", []byte("myvalue"))

// Retrieve a value (can be from any node in the network)
value, err := node.GetValue(ctx, "/fastreg/mykey")
```

Values stored in the DHT are replicated across multiple nodes for redundancy and can be retrieved from any node in the network, even if the original node that stored the value is offline.

## Registry Mirror

FastReg includes a Docker registry mirror that leverages the DHT for peer-to-peer content distribution:

```bash
# Start a node with registry mirror enabled
./fastreg -enable-registry -registry-addr 127.0.0.1:5000 -storage-dir /tmp/registry-data

# Configure Docker to use the mirror
# In your Docker daemon configuration (usually /etc/docker/daemon.json):
{
  "registry-mirrors": ["http://127.0.0.1:5000"]
}
```

The registry mirror works as follows:

1. When a container image is requested, the mirror first checks its local storage
2. If not found locally, it queries the DHT to find peers who have the content
3. If peers with the content are found, the blob is fetched directly from them
4. If not available through P2P, a lease system coordinates upstream requests:
   - Only one node (or a small set of nodes) will fetch the same content from upstream 
   - Other nodes requesting the same content will wait for it to become available
   - Once the content is fetched, it's shared with all waiting nodes via P2P
5. After retrieving content, it's stored locally and advertised in the DHT

This approach significantly reduces bandwidth usage and upstream load in two ways:
- Content is shared directly between peers in the network
- The lease mechanism prevents flooding the upstream registry when many nodes request the same content simultaneously

### Lease Mechanism

The lease system ensures efficient content distribution when many nodes need the same content:

- When multiple nodes request the same blob simultaneously, they coordinate through leases
- Only a configurable number of nodes (default: 5) will pull from upstream at the same time
- Other nodes wait for the content to become available via P2P
- If a node holding a lease fails, the lease expires and another node takes over
- This prevents unnecessary duplicate upstream requests while ensuring content availability
- Each node maintains its own lease state, making the system fully distributed with no central authority