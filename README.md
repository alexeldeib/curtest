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