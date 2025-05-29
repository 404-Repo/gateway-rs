# Gateway API

A high-performance, distributed gateway system for Subnet 17 designed to handle 100,000+ requests per second with advanced load balancing and consensus-based coordination.

## Overview

The Gateway API provides robust subnet access for validators processing 3D model workloads. Built on distributed consensus principles, it offers real-time load balancing, geographic optimization, and a foundation for future monetization strategies.

## Key Features

- **High Throughput**: Supports 100,000+ concurrent user requests
- **Distributed Consensus**: Raft-based coordination across gateway nodes
- **Geographic Load Balancing**: Dynamic routing based on latency and regional availability
- **Real-time State Sharing**: JSON-based global state accessible from any node
- **Modern Protocols**: HTTP/3 API layer with QUIC transport and TLS 1.3 encryption
- **Non-blocking Architecture**: Lock-free algorithms for maximum performance

## Architecture

### Core Components

- **Consensus Engine**: OpenRaft implementation for distributed coordination
- **Transport**: QUIC protocol for fast, encrypted communication
- **API Layer**: HTTP/3 interface for modern web standards
- **Security**: Rustls with TLS 1.3 encryption
- **Concurrency**: Non-blocking, lock-free design

### Global State Structure

Each node maintains a shared JSON state:

```json
{
  "gateways": [
    {
      "node_id": 1,
      "domain": "gateway-eu.404.xyz",
      "ip": "5.9.29.227",
      "name": "node-1-eu",
      "http_port": 4443,
      "available_tasks": 0,
      "last_task_acquisition": 1746010976,
      "last_update": 1746011013
    }
  ]
}
```

## How It Works

1. **Leader Election**: Nodes elect a leader using Raft consensus
2. **Task Distribution**: Leader coordinates task allocation across the cluster
3. **Load Balancing**: Validators choose optimal gateways based on:
   - Geographic proximity
   - Available task count
   - Latency metrics
   - Last task acquisition time
4. **State Replication**: All changes propagate through the cluster via log replication

## Validator Optimization

- **Geographic Routing**: Validators prioritize nearby gateway nodes
- **Dynamic Load Balancing**: Automatic switching to less loaded nodes
- **Latency Tracking**: Continuous optimization based on response times
- **Flexible Participation**: Optional organic traffic handling
- **Multi-cluster Support**: Interface with multiple gateway clusters simultaneously

## Getting Started

```bash
# Clone the repository
git clone [repository-url]

# Build and run
cargo build --release
cargo run
```

## Configuration

Validators can:
- Set geographic preferences
- Configure load balancing thresholds
- Establish custom gateway clusters
- Manage traffic participation settings

## Performance

- **Scale**: 100,000+ requests/second
- **Latency**: Optimized for 3D model file processing
- **Availability**: Majority quorum ensures continuous operation
- **Consistency**: Raft consensus guarantees data integrity
