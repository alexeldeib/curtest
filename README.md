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