# RDMA Support for FastReg

This document outlines how to use RDMA (Remote Direct Memory Access) with FastReg for high-performance, low-latency content distribution.

## Overview

FastReg now includes support for RDMA to accelerate blob transfers between registry nodes. RDMA bypasses the traditional network stack and OS, enabling direct memory-to-memory transfers with:

- Near zero-copy data transfer
- Kernel bypass (reduced CPU overhead)
- High throughput (up to 200Gbps with modern hardware)
- Ultra-low latency (microseconds instead of milliseconds)
- Offloaded protocol processing

This makes FastReg particularly suitable for high-performance computing environments, data centers, and any setup where minimizing latency and maximizing throughput are priorities.

## Requirements

To use RDMA with FastReg, you need:

- RDMA-capable hardware (e.g., Mellanox/NVIDIA ConnectX cards, Intel Omni-Path, etc.)
- RDMA drivers installed on the host system
- RDMA userspace libraries (libibverbs, librdmacm)
- Properly configured RDMA network infrastructure

## Supported RDMA Technologies

FastReg supports:

- InfiniBand
- RoCE (RDMA over Converged Ethernet) v1 and v2
- iWARP (Internet Wide Area RDMA Protocol)

## Building with RDMA Support

To build FastReg with RDMA support:

```bash
# Install RDMA development packages (Debian/Ubuntu)
sudo apt-get install -y libibverbs-dev librdmacm-dev rdma-core

# Build with CGO enabled
CGO_ENABLED=1 go build
```

## Docker Deployment with RDMA

A specialized Dockerfile (`Dockerfile.rdma`) is provided for running FastReg with RDMA support in a container. To build and run:

```bash
# Build the RDMA-enabled container
docker build -f Dockerfile.rdma -t fastreg-rdma .

# Run with RDMA device access
docker run --cap-add=IPC_LOCK --device=/dev/infiniband -v /dev/infiniband:/dev/infiniband \
  -p 8080:8080 -p 9999:9999 \
  fastreg-rdma --rdma-enabled --rdma-listen-port=9999
```

## Docker Compose Deployment

A Docker Compose configuration is provided in `docker-compose.rdma.yml` for deploying multiple RDMA-enabled FastReg nodes:

```bash
docker-compose -f docker-compose.rdma.yml up -d
```

## Configuration Options

| Flag | Description | Default |
|------|-------------|---------|
| `--rdma-enabled` | Enable RDMA transport | `false` |
| `--rdma-listen-addr` | RDMA listen address | `0.0.0.0` |
| `--rdma-listen-port` | RDMA listen port | `9999` |

## Performance Considerations

For optimal RDMA performance:

1. Use the latest RDMA drivers and firmware
2. Configure your network for jumbo frames (MTU 9000)
3. Tune RDMA parameters for your specific hardware
4. Enable PFC (Priority Flow Control) when using RoCE
5. Consider using dedicated RDMA networks in production

## Monitoring RDMA Performance

You can monitor RDMA performance with standard tools:

```bash
# Check RDMA device status
ibv_devinfo

# Monitor RDMA throughput
rdma-core perftest tools:
ib_read_bw
ib_write_bw
ib_send_bw
```

## Troubleshooting

Common RDMA issues and solutions:

1. **No RDMA devices found**
   - Check that RDMA drivers are installed: `lsmod | grep mlx`
   - Verify hardware with `ibv_devinfo`

2. **Connection failures**
   - Check network connectivity between nodes
   - Ensure ports are open in firewalls
   - Verify IP addresses and routing

3. **Performance issues**
   - Ensure MTU settings are consistent (typically 4096 for InfiniBand, 9000 for RoCE)
   - Check for packet drops or congestion
   - Monitor CPU, memory, and RDMA queue utilization

## Performance Benchmarks

With RDMA enabled, you can expect:

- Latency reduction of 50-80% compared to TCP/IP for blob transfers
- Throughput improvements of 2-5x for large blobs
- Reduced CPU utilization during transfers
- More efficient distribution with many concurrent nodes

## RDMA vs Traditional TCP

| Metric | TCP | RDMA |
|--------|-----|------|
| Latency | Milliseconds | Microseconds |
| CPU Usage | High | Low |
| Bandwidth Utilization | Moderate | High |
| Implementation Complexity | Low | High |
| Hardware Requirements | Standard NICs | Specialized NICs |

## Technical Details

The RDMA implementation in FastReg uses:

- Reliable Connected (RC) queue pairs for reliable, in-order delivery
- RDMA Write operations for transferring blob data
- Send/Receive operations for control messages
- Memory registration and pinning for DMA operations
- Event-based connection management with CM events
- Completion queues for operation completion notification

This provides a balance of performance and reliability appropriate for Docker registry blob transfers.