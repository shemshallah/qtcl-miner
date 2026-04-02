# QTCL Miner Client

[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](LICENSE)
[![Python](https://img.shields.io/badge/Python-3.8+-blue.svg)](https://python.org)
[![Status](https://img.shields.io/badge/Status-Alpha-yellow.svg)](https://github.com/shemshallah/qtcl-miner)

> **QTCL** (Quantum Temporal Coherence Ledger) — A quantum-classical hybrid blockchain protocol where consensus emerges from tripartite W-state entanglement across distributed oracles.

The `qtcl-miner` repository provides the reference Python client for participating in the QTCL network as a mining node, wallet holder, or oracle participant.

---

## 🌌 Overview

QTCL Miner is a production-ready client implementation for the **Quantum Temporal Coherence Ledger**, a novel blockchain architecture that integrates:

- 🔐 **Lattice-based cryptography** (HLWE) for post-quantum security
- ⚛️ **Tripartite W-state entanglement** for consensus validation
- 🎲 **Multi-source quantum randomness** (QRNG ensemble) for entropy generation
- 🌐 **P2P gossip protocol** for decentralized peer discovery
- 🧠 **Hyperbolic entropy mixing** via {8,3} Möbius transformations

Each oracle node maintains a 3-qubit W-state (`|W₃⟩ = (|100⟩+|010⟩+|001⟩)/√3`) and performs independent measurements. The network synthesizes a temporal coherence average over time, creating a consensus mechanism rooted in quantum information theory.

---

## ✨ Features

### ✅ Implemented
- [x] **Mining Engine**: Block validation, proof-of-quantum-work, difficulty adjustment
- [x] **Transaction Handling**: Create, sign, broadcast, and verify QTCL transactions
- [x] **Wallet Management**: 
  - BIP39 mnemonic generation (12-24 words)
  - BIP44 hierarchical deterministic key derivation
  - HLWE lattice-based address generation
  - Local encrypted storage + optional Supabase sync
- [x] **Quantum Entropy Pipeline**:
  - Multi-source QRNG aggregation (random.org, ANU Quantum Numbers, QBICK)
  - XOR₃ fusion + hyperbolic Möbius mixing
  - Fallback to server entropy + `os.urandom()` hedge
- [x] **Oracle Client**: Fetch W-state snapshots, PQ0 qubit data, and lattice state
- [x] **RPC Interface**: JSON-RPC 2.0 client for chain queries, block submission, peer exchange

### 🚧 In Progress
- [ ] **P2P Networking**: Gossip protocol, DHT bootstrap, NAT traversal via STUN
- [ ] **Android Port**: Kotlin/Python bridge for mobile mining (experimental)
- [ ] **Quantum Circuit Cache**: LRU caching for compiled entanglement circuits
- [ ] **Adaptive Timeout Manager**: Latency-aware peer communication tuning

---

## 📦 Installation

### Prerequisites
- Python 3.8 or higher
- pip package manager
- Git

### Quick Start

```bash
# Clone the repository
git clone https://github.com/shemshallah/qtcl-miner.git
cd qtcl-miner

# Install dependencies
pip install -r requirements.txt

# Run the client
python3 qtcl_client.py
```

### Environment Configuration (Optional)

Create a `.env` file or export variables to customize behavior:

```bash
# QRNG API Keys (enable multi-source entropy)
export RANDOM_ORG_KEY="your_random_org_api_key"
export ANU_API_KEY="your_anu_quantum_api_key"
export QRNG_API_KEY="your_qbick_api_key"

# Server Configuration
export ENTROPY_SERVER="https://qtcl-blockchain.koyeb.app"
export ENTROPY_API_KEY="your_server_entropy_key"

# P2P Settings
export P2P_EXTERNAL_HOST="your.public.ip"
export P2P_EXTERNAL_PORT="9091"

# Wallet Security
export QTCL_DATA_DIR="$HOME/.qtcl"  # Override default data directory
```

### Docker Deployment

```dockerfile
FROM python:3.11-slim

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .
CMD ["python3", "qtcl_client.py"]
```

Build and run:
```bash
docker build -t qtcl-miner .
docker run -it --env-file .env qtcl-miner
```

---

## 🧭 Usage

### Basic Client Launch

```bash
python3 qtcl_client.py --help
```

### Wallet Operations

```python
from qtcl_client import QTCLClient

client = QTCLClient()

# Generate new mnemonic (24-word maximum security)
mnemonic = client.wallet.generate_mnemonic(strength="MAXIMUM")
print(f"Backup this phrase: {mnemonic}")

# Derive receiving address
address = client.wallet.derive_address(account=0, change=0, index=0)
print(f"Your QTCL address: {address}")

# Check balance
balance = client.get_balance(address)
print(f"Balance: {balance} QTCL")
```

### Mining a Block

```python
# Start mining loop (non-blocking)
client.miner.start_mining(
    reward_address=address,
    threads=4,  # CPU threads for proof-of-quantum-work
    entropy_source="hybrid"  # "qrng", "server", or "hybrid"
)

# Monitor mining status
status = client.miner.get_status()
print(f"Current height: {status['height']}, Hashrate: {status['hashrate']} H/s")
```

### Oracle Participation

```python
# Fetch current W-state snapshot
snapshot = client.oracle.get_w_state()
print(f"Entanglement fidelity: {snapshot['fidelity']}")

# Submit local measurement for consensus
client.oracle.submit_measurement(
    qubit_id="pq0",
    measurement_basis="Z",
    result=1,
    timestamp=time.time()
)
```

### P2P Peer Discovery (Experimental)

```bash
# Register this node with the bootstrap network
python3 qtcl_client.py --register-peer --external-addr your.ip:9091

# List discovered peers
python3 qtcl_client.py --list-peers
```

---

## 🔐 Security Model

### Entropy Generation Pipeline
```
[QRNG Sources] → XOR₃ Fusion → {8,3} Möbius Walk (d=64) 
       ↓
[Server Entropy] → Hyperbolic Mixing → os.urandom(8) Hedge
       ↓
[Final 32-byte Output] → SHA3-256 / SHAKE-256 Expansion
```

- **No single point of failure**: Compromise of any single QRNG source does not weaken entropy
- **Local entropy hedge**: `os.urandom(8)` is mixed into every output to guarantee minimum entropy
- **Server pre-processing**: Entropy server applies first-pass hyperbolic transformation; client applies final Möbius walk

### Key Derivation
- **HLWE Lattice Parameters**: n=256, q=2³²−5, χ-error bound=256, targeting 256-bit security
- **BIP32/BIP44 Paths**: `m/44'/0'/0'/0/{index}` for receiving addresses
- **Encrypted Storage**: Mnemonics encrypted with Argon2id + AES-256-GCM before local/Supabase storage

---

## 📁 Project Structure

```
qtcl-miner/
├── qtcl_client.py              # Main client implementation
├── qtcl_client_backup.py       # Development backup
├── qtcl_client_mar30_stable.py # Pre-P2P stable release
├── requirements.txt            # Python dependencies
├── LICENSE                     # Apache-2.0 License
├── README.md                   # This file
└── data/                       # Runtime data directory (auto-created)
    └── qtcl_blockchain.db      # Local SQLite blockchain state
```

### Core Modules (within `qtcl_client.py`)
| Module | Purpose |
|--------|---------|
| `HyperbolicEntropyPool` | Multi-source quantum entropy aggregation & mixing |
| `NumPyEntanglementEngine` | Pure-NumPy W-state simulation & partial trace operations |
| `EntanglementLineageTracker` | Ancestry graph for quantum state provenance |
| `QuantumCircuitCache` | LRU cache for compiled entanglement circuits |
| `AdaptiveTimeoutManager` | Latency-aware P2P communication tuning |
| `LocalBlockchainDB` | SQLite-backed local chain state & transaction ledger |
| `WalletManager` | BIP39/BIP44 key derivation + HLWE address generation |

---

## 🌐 Network Architecture

```
┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐
│   Miner Node    │     │  Oracle Node    │     │  Validator Node │
│                 │     │                 │     │                 │
│ • PoQW Mining   │◄───►│ • W-State Maint │◄───►│ • Consensus     │
│ • Tx Propagation│     │ • Measurement   │     │ • Block Finality│
│ • Wallet Ops    │     │ • Entropy Feed  │     │ • P2P Gossip    │
└────────┬────────┘     └────────┬────────┘     └────────┬────────┘
         │                       │                       │
         ▼                       ▼                       ▼
┌─────────────────────────────────────────────────────────────┐
│              QTCL Bootstrap Server (Koyeb)                   │
│  • STUN: External IP discovery                              │
│  • Peer Registry: Active node directory                     │
│  • Entropy API: Pre-processed quantum randomness            │
│  • RPC Gateway: Chain queries, block submission             │
└─────────────────────────────────────────────────────────────┘
```

All nodes initially bootstrap from `https://qtcl-blockchain.koyeb.app`. P2P mode (when enabled) allows direct peer-to-peer communication via gossip protocol.

---

## 🧪 Development

### Running Tests

```bash
# Install dev dependencies
pip install pytest pytest-asyncio

# Run unit tests
pytest tests/ -v

# Run async tests
pytest tests/ -v --asyncio-mode=auto
```

### Code Style

```bash
# Format with black
black qtcl_client.py

# Lint with flake8
flake8 --max-line-length=120 qtcl_client.py
```

### Contributing

1. Fork the repository
2. Create a feature branch: `git checkout -b feat/your-feature`
3. Commit changes with conventional messages: `git commit -m "feat: add X"`
4. Push and open a Pull Request

Please ensure:
- All new code includes type hints
- Entropy-critical paths have unit tests
- Quantum simulation changes include fidelity validation

---

## 📚 Technical References

- **W-State Entanglement**: [Dur et al., Phys. Rev. A 62, 062314 (2000)](https://journals.aps.org/pra/abstract/10.1103/PhysRevA.62.062314)
- **HLWE Cryptography**: [Brakerski et al., FOCS 2013](https://eprint.iacr.org/2013/332)
- **Hyperbolic Mixing**: Internal QTCL specification (see `docs/entropy_protocol.md`)
- **BIP39/BIP44**: [Bitcoin Improvement Proposals](https://github.com/bitcoin/bips)

---

## ⚠️ Disclaimer

> This software is **alpha-stage research code**. Do not use with mainnet assets or in production environments without thorough security review. Quantum consensus mechanisms are experimental and not yet cryptographically audited.

---

## 📄 License

Distributed under the **Apache License 2.0**. See [`LICENSE`](LICENSE) for details.

```
Copyright 2026 QTCL Contributors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
```

---

## 🤝 Community & Support

- **GitHub Issues**: [Report bugs or request features](https://github.com/shemshallah/qtcl-miner/issues)
- **Documentation**: `docs/` directory (WIP)
- **Server Status**: [qtcl-blockchain.koyeb.app](https://qtcl-blockchain.koyeb.app)
- **Contact**: @shemshallah on GitHub

*Built with quantum curiosity and classical rigor.* ⚛️🔗
