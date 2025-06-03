# Gateway API

To support upcoming partnerships, public API access, and rising traffic, Subnet 17 built a high-throughput, low-latency subnet access layer. It’s designed to handle a large number of concurrent uploads from validators, real-time stats, and with support for dynamic load balancing, for more information read the [whitepaper](https://doc.404.xyz/gateway-api-whitepaper).

## Key Components of the Project:

- **Consensus Engine** – Based on Raft (OpenRaft), the system achieves distributed consensus across nodes and shares the internal state for every node in the cluster.
- **Transport Protocol** – QUIC transports RAFT messages, ensuring fast, low-latency communication while also encrypting all data.
- **API Layer** – The interface operates over HTTP/3, adhering to modern web standards for responsive client interactions.
- **Encryption** – Rustls provides TLS 1.3 encryption to secure communication channels.
- **Concurrency Model** – The solution is entirely non-blocking and employs lock-free algorithms throughout, with the only exception being an RwLock used in the commit log for consistency.

## Development Environment

To run the project with 3 nodes, just execute:

```bash
./start-dev-env.sh
```

## License

This project is dual-licensed under either the **Apache-2.0** or **MIT** license, at your choice:
- [Apache License, Version 2.0](http://www.apache.org/licenses/LICENSE-2.0)
- [MIT License](http://opensource.org/licenses/MIT)
