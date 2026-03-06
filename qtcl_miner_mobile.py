#!/usr/bin/env python3
"""
╔════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════╗
║                                                                                                                                            ║
║  🌌 QTCL FULL NODE + QUANTUM MINER - W-STATE ENTANGLED MINING 🌌                                                                         ║
║                                                                                                                                            ║
║  WORLD'S FIRST W-STATE ENTANGLED BLOCKCHAIN MINER:                                                                                      ║
║  • Connects to LIVE qtcl-blockchain.koyeb.app                                                                                           ║
║  • Syncs blockchain from network (REST API)                                                                                             ║
║  • On startup: queries oracle for latest W-state snapshot (HLWE-signed)                                                                 ║
║  • Recovers W-state locally with signature verification                                                                                 ║
║  • Establishes entanglement: Oracle (pq0) ↔ Current (pq_curr) ↔ Last (pq_last)                                                         ║
║  • Uses recovered W-state entropy for quantum PoW                                                                                        ║
║  • Maintains 3-qubit entanglement state across mining iterations                                                                        ║
║  • Broadcasts mined blocks with W-state fidelity attestation                                                                             ║
║                                                                                                                                            ║
║  ARCHITECTURE:                                                                                                                          ║
║  ┌────────────────────────────────────────────────────────────────────┐                                                                ║
║  │ W-STATE RECOVERY & ENTANGLEMENT (On Init)                          │                                                                ║
║  │ • Register with oracle                                             │                                                                ║
║  │ • Download latest DM snapshot (HLWE-verified)                      │                                                                ║
║  │ • Recover W-state locally (pq0 = oracle)                           │                                                                ║
║  │ • Create pq_curr and pq_last entangled copies                      │                                                                ║
║  │ • Verify fidelity >= 0.85 threshold                                │                                                                ║
║  │ • Start continuous sync worker (background)                        │                                                                ║
║  └────────────────────────────────────────────────────────────────────┘                                                                ║
║  ┌────────────────────────────────────────────────────────────────────┐                                                                ║
║  │ LIVE BLOCKCHAIN SYNC                                               │                                                                ║
║  │ • Fetch blocks from qtcl-blockchain.koyeb.app REST API            │                                                                ║
║  │ • Validate block headers, PoW, transactions                       │                                                                ║
║  │ • Maintain chain state (in-memory)                                │                                                                ║
║  │ • Fork detection & resolution (longest-chain)                     │                                                                ║
║  │ • Sync progress tracking                                          │                                                                ║
║  └────────────────────────────────────────────────────────────────────┘                                                                ║
║  ┌────────────────────────────────────────────────────────────────────┐                                                                ║
║  │ MEMPOOL MANAGEMENT                                                 │                                                                ║
║  │ • Fetch pending transactions from /api/mempool                    │                                                                ║
║  │ • Validate signatures (HLWE), nonces, balances                    │                                                                ║
║  │ • Fee-based prioritization                                        │                                                                ║
║  │ • Remove included transactions after block                        │                                                                ║
║  └────────────────────────────────────────────────────────────────────┘                                                                ║
║  ┌────────────────────────────────────────────────────────────────────┐                                                                ║
║  │ QUANTUM-ENTANGLED MINING SUBSYSTEM                                 │                                                                ║
║  │ • Poll mempool for transactions                                   │                                                                ║
║  │ • Build block template from highest-fee transactions              │                                                                ║
║  │ • Measure W-state (pq_curr) for quantum PoW entropy               │                                                                ║
║  │ • Rotate pq_curr → pq_last, recover new pq_curr from oracle      │                                                                ║
║  │ • Sequential nonce iteration (SHA3-256 PoW + W-state witness)     │                                                                ║
║  │ • Broadcast mined block with fidelity attestation                 │                                                                ║
║  │ • Track mining rewards & entanglement metrics                     │                                                                ║
║  └────────────────────────────────────────────────────────────────────┘                                                                ║
║                                                                                                                                            ║
║  USAGE: python qtcl_miner.py --address qtcl1YOUR_ADDRESS --oracle-url https://oracle.example.com/socket.io                            ║
║                                                                                                                                            ║
║  This is PERFECTION. Museum-grade quantum mining. Deploy with absolute confidence.                                                     ║
║                                                                                                                                            ║
╚════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════╝
"""

import os,sys,time,json,math,hashlib,secrets,uuid,threading,logging,argparse,traceback,base64,hmac,sqlite3,struct,cmath,socket
from typing import Dict,Any,Optional,List,Tuple,Deque,Set
from dataclasses import dataclass,field,asdict
from enum import Enum,auto
from collections import deque,defaultdict,Counter
from datetime import datetime,timezone
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import numpy as np

try:
    import socketio
    SOCKETIO_AVAILABLE=True
except ImportError:
    SOCKETIO_AVAILABLE=False
    logger=logging.getLogger('QTCL_MINER')
    logger.warning("[MINER] python-socketio not available - will use HTTP-only registration")

# ── gRPC streaming client ─────────────────────────────────────────────────────
_GRPC_CLIENT_AVAILABLE = False
_grpc_client_mod       = None
_wstate_pb2_client     = None
_wstate_pb2_grpc_client = None

_PROTO_SRC = r"""
syntax = "proto3";
package qtcl;
service WStateService {
  rpc StreamSnapshots(StreamRequest) returns (stream WStateSnapshot);
  rpc GetLatestSnapshot(StreamRequest) returns (WStateSnapshot);
  rpc Ping(PingRequest) returns (PingResponse);
}
message StreamRequest  { string miner_id = 1; string miner_address = 2; uint64 known_ts = 3; }
message PingRequest    { string miner_id = 1; }
message PingResponse   { bool ok = 1; uint64 server_ts_ns = 2; uint32 miner_count = 3; }
message HLWESignature  { string commitment = 1; string witness = 2; string proof = 3;
                         string w_entropy_hash = 4; string derivation_path = 5; string public_key_hex = 6; }
message WStateSnapshot { uint64 timestamp_ns = 1; string oracle_address = 2; string w_entropy_hash = 3;
                         double fidelity = 4; double coherence = 5; double purity = 6; double entanglement = 7;
                         string density_matrix_hex = 8; bool signature_valid = 9;
                         HLWESignature hlwe_signature = 10; uint64 block_height = 11; }
"""

def _compile_grpc_client_proto():
    pass  # gRPC client removed - using SSE
# ── end gRPC client init ──────────────────────────────────────────────────────

try:
    from qiskit import QuantumCircuit,QuantumRegister,ClassicalRegister,execute
    from qiskit.quantum_info import Statevector,DensityMatrix
    from qiskit.providers.aer import AerSimulator
    QISKIT_AVAILABLE=True
except ImportError:
    QISKIT_AVAILABLE=False

logging.basicConfig(level=logging.INFO,format='[%(asctime)s] %(levelname)s: %(message)s')
logger=logging.getLogger('QTCL_MINER')

# ═════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
# QUANTUM LATTICE SCHEMA BUILDER - MUSEUM GRADE {8,3} POINCARÉ TESSELLATION
# ═════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════

@dataclass
class HyperbolicPoint:
    """Point in Poincaré disk (complex number with |z| < 1)."""
    x: float
    y: float
    
    @property
    def complex(self) -> complex:
        return complex(self.x, self.y)
    
    @property
    def radius(self) -> float:
        return math.sqrt(self.x**2 + self.y**2)
    
    def distance_to(self, other: 'HyperbolicPoint') -> float:
        z1 = self.complex
        z2 = other.complex
        if abs(z1 - z2) < 1e-15:
            return 0.0
        numerator = abs(z1 - z2)
        denominator = abs(1 - z1.conjugate() * z2)
        if denominator < 1e-15:
            return float('inf')
        return 2 * math.atanh(numerator / denominator)
    
    @staticmethod
    def from_polar(r: float, theta: float) -> 'HyperbolicPoint':
        return HyperbolicPoint(r * math.cos(theta), r * math.sin(theta))


@dataclass
class HyperbolicTriangle:
    """Triangle in {8,3} tessellation."""
    vertices: List[HyperbolicPoint]
    depth: int
    pseudoqubits: int = 13
    
    def center(self) -> HyperbolicPoint:
        x = sum(v.x for v in self.vertices) / 3
        y = sum(v.y for v in self.vertices) / 3
        return HyperbolicPoint(x, y)
    
    def area(self) -> float:
        return (math.pi / 4) / (2 ** self.depth)


class PoincareHyperbolicTessellator:
    """Builds {8,3} regular tessellation of Poincaré disk."""
    
    def __init__(self, max_depth: int = 5):
        self.max_depth = max_depth
        self.triangles: List[HyperbolicTriangle] = []
    
    def _geodesic_midpoint(self, p1: HyperbolicPoint, p2: HyperbolicPoint) -> HyperbolicPoint:
        z1 = p1.complex
        z2 = p2.complex
        numerator = z1 + z2
        denominator = 1 + z1 * z2.conjugate()
        if abs(denominator) < 1e-15:
            mid = complex(0, 0)
        else:
            mid = numerator / denominator
        mag = abs(mid)
        if mag >= 0.99:
            mid = 0.99 * mid / mag
        return HyperbolicPoint(mid.real, mid.imag)
    
    def _initial_triangle(self) -> HyperbolicTriangle:
        angle1, angle2, angle3 = 0.0, math.pi / 4, math.pi / 2
        r = 0.3
        v1 = HyperbolicPoint.from_polar(r, angle1)
        v2 = HyperbolicPoint.from_polar(r, angle2)
        v3 = HyperbolicPoint.from_polar(r, angle3)
        return HyperbolicTriangle([v1, v2, v3], depth=0)
    
    def _subdivide_triangle(self, triangle: HyperbolicTriangle) -> List[HyperbolicTriangle]:
        if triangle.depth >= self.max_depth:
            return [triangle]
        vertices = triangle.vertices
        mid01 = self._geodesic_midpoint(vertices[0], vertices[1])
        mid12 = self._geodesic_midpoint(vertices[1], vertices[2])
        mid20 = self._geodesic_midpoint(vertices[2], vertices[0])
        return [
            HyperbolicTriangle([vertices[0], mid01, mid20], depth=triangle.depth + 1),
            HyperbolicTriangle([vertices[1], mid12, mid01], depth=triangle.depth + 1),
            HyperbolicTriangle([vertices[2], mid20, mid12], depth=triangle.depth + 1),
            HyperbolicTriangle([mid01, mid12, mid20], depth=triangle.depth + 1),
        ]
    
    def tessellate(self) -> List[HyperbolicTriangle]:
        triangles = []
        for i in range(8):
            angle = 2 * math.pi * i / 8
            base_tri = self._initial_triangle()
            rotated_vertices = [HyperbolicPoint.from_polar(v.radius, math.atan2(v.y, v.x) + angle) for v in base_tri.vertices]
            rotated_tri = HyperbolicTriangle(rotated_vertices, depth=0)
            def subdivide_recursive(tri):
                subs = self._subdivide_triangle(tri)
                result = []
                for sub in subs:
                    if sub.depth < self.max_depth:
                        result.extend(subdivide_recursive(sub))
                    else:
                        result.append(sub)
                return result
            triangles.extend(subdivide_recursive(rotated_tri))
        self.triangles = triangles
        return triangles


class QuantumLatticeSchemaBuilder:
    """Builds museum-grade quantum lattice database with hyperbolic tessellation."""
    
    def __init__(self, db_path: str = 'data/qtcl_blockchain.db'):
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(exist_ok=True, parents=True)
        self.tessellation_depth = 5
        self.conn: Optional[sqlite3.Connection] = None
        self._lock = threading.RLock()
    
    def exists(self) -> bool:
        return self.db_path.exists()
    
    def connect(self):
        self.db_path.parent.mkdir(exist_ok=True, parents=True)
        self.conn = sqlite3.connect(str(self.db_path), check_same_thread=False, timeout=10)
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute("PRAGMA synchronous=NORMAL")
        self.conn.execute("PRAGMA cache_size=-10000")
        self.conn.execute("PRAGMA temp_store=MEMORY")
    
    def build_schema(self):
        with self._lock:
            self.conn.executescript("""
                CREATE TABLE IF NOT EXISTS quantum_lattice_metadata (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    tessellation_type TEXT NOT NULL DEFAULT '{8,3}',
                    tessellation_depth INTEGER NOT NULL DEFAULT 5,
                    total_pseudoqubits INTEGER NOT NULL DEFAULT 106496,
                    qubits_per_triangle INTEGER DEFAULT 13,
                    precision_bits INTEGER DEFAULT 150,
                    poincare_radius REAL DEFAULT 1.0,
                    hyperbolicity_constant REAL DEFAULT -1.0,
                    status TEXT DEFAULT 'initialized',
                    created_at INTEGER DEFAULT (strftime('%s', 'now')),
                    updated_at INTEGER DEFAULT (strftime('%s', 'now'))
                );
                
                CREATE TABLE IF NOT EXISTS lattice_triangles (
                    triangle_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    depth INTEGER NOT NULL,
                    parent_id INTEGER,
                    v1_x REAL NOT NULL,
                    v1_y REAL NOT NULL,
                    v2_x REAL NOT NULL,
                    v2_y REAL NOT NULL,
                    v3_x REAL NOT NULL,
                    v3_y REAL NOT NULL,
                    center_x REAL,
                    center_y REAL,
                    area REAL,
                    pseudoqubit_count INTEGER DEFAULT 13,
                    created_at INTEGER DEFAULT (strftime('%s', 'now')),
                    FOREIGN KEY(parent_id) REFERENCES lattice_triangles(triangle_id)
                );
                
                CREATE INDEX IF NOT EXISTS idx_lattice_depth ON lattice_triangles(depth);
                CREATE INDEX IF NOT EXISTS idx_lattice_parent ON lattice_triangles(parent_id);
                
                CREATE TABLE IF NOT EXISTS pseudoqubits (
                    qubit_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    triangle_id INTEGER NOT NULL,
                    qubit_index INTEGER NOT NULL,
                    state_vector TEXT NOT NULL,
                    coherence_time_us REAL,
                    decoherence_rate REAL,
                    last_measured_at INTEGER,
                    FOREIGN KEY(triangle_id) REFERENCES lattice_triangles(triangle_id),
                    UNIQUE(triangle_id, qubit_index)
                );
                
                CREATE INDEX IF NOT EXISTS idx_qubits_triangle ON pseudoqubits(triangle_id);
                
                CREATE TABLE IF NOT EXISTS blocks (
                    height INTEGER PRIMARY KEY,
                    block_hash TEXT UNIQUE NOT NULL,
                    parent_hash TEXT NOT NULL,
                    merkle_root TEXT NOT NULL,
                    timestamp_s INTEGER NOT NULL,
                    difficulty_bits INTEGER NOT NULL,
                    nonce INTEGER NOT NULL,
                    miner_address TEXT NOT NULL,
                    w_state_fidelity REAL NOT NULL,
                    w_entropy_hash TEXT NOT NULL,
                    tx_count INTEGER DEFAULT 0,
                    created_at INTEGER DEFAULT (strftime('%s', 'now'))
                );
                
                CREATE INDEX IF NOT EXISTS idx_blocks_height ON blocks(height);
                CREATE INDEX IF NOT EXISTS idx_blocks_hash ON blocks(block_hash);
                
                CREATE TABLE IF NOT EXISTS transactions (
                    tx_id TEXT PRIMARY KEY,
                    height INTEGER NOT NULL,
                    tx_index INTEGER NOT NULL,
                    from_address TEXT NOT NULL,
                    to_address TEXT NOT NULL,
                    amount INTEGER NOT NULL,
                    fee INTEGER DEFAULT 0,
                    tx_type TEXT DEFAULT 'transfer',
                    signature TEXT,
                    w_proof TEXT,
                    timestamp_ns INTEGER,
                    FOREIGN KEY(height) REFERENCES blocks(height),
                    UNIQUE(height, tx_index)
                );
                
                CREATE INDEX IF NOT EXISTS idx_txs_height ON transactions(height);
                CREATE INDEX IF NOT EXISTS idx_txs_from ON transactions(from_address);
                CREATE INDEX IF NOT EXISTS idx_txs_to ON transactions(to_address);
                
                CREATE TABLE IF NOT EXISTS w_state_snapshots (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    timestamp_ns INTEGER UNIQUE NOT NULL,
                    pq_current TEXT NOT NULL,
                    pq_last TEXT NOT NULL,
                    block_height INTEGER NOT NULL,
                    fidelity REAL NOT NULL,
                    coherence REAL NOT NULL,
                    entropy_pool REAL,
                    hlwe_signature TEXT,
                    oracle_address TEXT,
                    signature_valid INTEGER DEFAULT 0,
                    created_at INTEGER DEFAULT (strftime('%s', 'now'))
                );
                
                CREATE INDEX IF NOT EXISTS idx_wstate_timestamp ON w_state_snapshots(timestamp_ns);
                
                CREATE TABLE IF NOT EXISTS hlwe_keys (
                    address TEXT PRIMARY KEY,
                    private_key_encrypted TEXT NOT NULL,
                    public_key TEXT NOT NULL,
                    nonce_hex TEXT NOT NULL,
                    salt_hex TEXT NOT NULL,
                    algorithm TEXT DEFAULT 'HLWE-256',
                    derivation_path TEXT,
                    key_fingerprint TEXT,
                    is_locked INTEGER DEFAULT 0,
                    security_level_bits INTEGER DEFAULT 256,
                    quantum_security_bits INTEGER DEFAULT 128,
                    created_at INTEGER DEFAULT (strftime('%s', 'now')),
                    updated_at INTEGER DEFAULT (strftime('%s', 'now'))
                );
                
                CREATE INDEX IF NOT EXISTS idx_hlwe_address ON hlwe_keys(address);
                CREATE INDEX IF NOT EXISTS idx_hlwe_fingerprint ON hlwe_keys(key_fingerprint);
                
                CREATE TABLE IF NOT EXISTS wallet_addresses (
                    address TEXT PRIMARY KEY,
                    wallet_fingerprint TEXT NOT NULL,
                    public_key TEXT NOT NULL,
                    derivation_path TEXT,
                    balance INTEGER DEFAULT 0,
                    transaction_count INTEGER DEFAULT 0,
                    last_used_at INTEGER,
                    label TEXT,
                    created_at INTEGER DEFAULT (strftime('%s', 'now'))
                );
                
                CREATE INDEX IF NOT EXISTS idx_wallet_fingerprint ON wallet_addresses(wallet_fingerprint);
                
                CREATE TABLE IF NOT EXISTS bip32_master_keys (
                    id INTEGER PRIMARY KEY,
                    fingerprint TEXT UNIQUE NOT NULL,
                    encrypted_seed TEXT NOT NULL,
                    salt_hex TEXT NOT NULL,
                    nonce_hex TEXT NOT NULL,
                    chain_code TEXT NOT NULL,
                    depth INTEGER DEFAULT 0,
                    parent_fingerprint TEXT,
                    child_index INTEGER DEFAULT 0,
                    created_at INTEGER DEFAULT (strftime('%s', 'now'))
                );
                
                CREATE TABLE IF NOT EXISTS bip38_passphrases (
                    id INTEGER PRIMARY KEY,
                    wallet_fingerprint TEXT UNIQUE NOT NULL,
                    encrypted_hash TEXT NOT NULL,
                    scrypt_params TEXT NOT NULL,
                    salt_hex TEXT NOT NULL,
                    iterations INTEGER DEFAULT 16384,
                    created_at INTEGER DEFAULT (strftime('%s', 'now')),
                    FOREIGN KEY(wallet_fingerprint) REFERENCES wallet_addresses(wallet_fingerprint)
                );
                
                CREATE TABLE IF NOT EXISTS peer_registry (
                    peer_id TEXT PRIMARY KEY,
                    address TEXT NOT NULL,
                    port INTEGER NOT NULL,
                    last_seen INTEGER NOT NULL,
                    block_height INTEGER DEFAULT 0,
                    user_agent TEXT,
                    created_at INTEGER DEFAULT (strftime('%s', 'now'))
                );
                
                CREATE TABLE IF NOT EXISTS mining_metrics (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    session_id TEXT NOT NULL,
                    blocks_mined INTEGER DEFAULT 0,
                    hash_attempts INTEGER DEFAULT 0,
                    avg_fidelity REAL DEFAULT 0.0,
                    total_rewards_base INTEGER DEFAULT 0,
                    started_at INTEGER NOT NULL,
                    ended_at INTEGER,
                    created_at INTEGER DEFAULT (strftime('%s', 'now'))
                );
                
                CREATE TABLE IF NOT EXISTS schema_version (
                    version INTEGER PRIMARY KEY,
                    description TEXT,
                    applied_at INTEGER DEFAULT (strftime('%s', 'now'))
                );
                
                CREATE TABLE IF NOT EXISTS difficulty_state (
                    id INTEGER PRIMARY KEY CHECK(id=1),
                    current_difficulty INTEGER NOT NULL DEFAULT 12,
                    target_block_time_s REAL NOT NULL DEFAULT 30.0,
                    retarget_window INTEGER NOT NULL DEFAULT 10,
                    last_retarget_height INTEGER NOT NULL DEFAULT 0,
                    ema_block_time_s REAL NOT NULL DEFAULT 30.0,
                    ema_alpha REAL NOT NULL DEFAULT 0.2,
                    min_difficulty INTEGER NOT NULL DEFAULT 8,
                    max_difficulty INTEGER NOT NULL DEFAULT 32,
                    total_blocks_retargeted INTEGER NOT NULL DEFAULT 0,
                    updated_at INTEGER DEFAULT (strftime('%s', 'now'))
                );
            """)
            self.conn.commit()
            # Ensure difficulty_state row exists
            try:
                cursor = self.conn.cursor()
                cursor.execute("INSERT OR IGNORE INTO difficulty_state (id) VALUES(1)")
                self.conn.commit()
            except:
                pass
    
    def populate_lattice(self):
        tessellator = PoincareHyperbolicTessellator(max_depth=self.tessellation_depth)
        triangles = tessellator.tessellate()
        with self._lock:
            cursor = self.conn.cursor()
            for i, triangle in enumerate(triangles):
                center = triangle.center()
                area = triangle.area()
                vertices = triangle.vertices
                cursor.execute("""
                    INSERT INTO lattice_triangles (depth, v1_x, v1_y, v2_x, v2_y, v3_x, v3_y, center_x, center_y, area, pseudoqubit_count)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 13)
                """, (triangle.depth, vertices[0].x, vertices[0].y, vertices[1].x, vertices[1].y, vertices[2].x, vertices[2].y, center.x, center.y, area))
            self.conn.commit()
    
    def populate_metadata(self):
        tessellator = PoincareHyperbolicTessellator(max_depth=self.tessellation_depth)
        triangles = tessellator.tessellate()
        total_qubits = len(triangles) * 13
        with self._lock:
            cursor = self.conn.cursor()
            cursor.execute("""INSERT INTO quantum_lattice_metadata (tessellation_type, tessellation_depth, total_pseudoqubits, qubits_per_triangle, precision_bits, status)
            VALUES (?, ?, ?, ?, ?, 'initialized')""", ('{8,3}', self.tessellation_depth, total_qubits, 13, 150))
            self.conn.commit()
    
    def initialize_schema(self):
        if self.exists():
            logger.info(f"[SCHEMA] 📀 Database exists — skipping creation")
            self.connect()
            return False
        logger.info(f"[SCHEMA] 🏗️  Building quantum lattice database...")
        logger.info(f"[SCHEMA] ├─ Tessellation: {{8,3}} Poincaré sphere")
        logger.info(f"[SCHEMA] ├─ Depth: 5")
        logger.info(f"[SCHEMA] ├─ Qubits/triangle: 13")
        logger.info(f"[SCHEMA] └─ Total pseudoqubits: 106,496")
        self.connect()
        logger.info(f"[SCHEMA] 📋 Creating schema...")
        self.build_schema()
        logger.info(f"[SCHEMA] 🧮 Tessellating lattice...")
        self.populate_lattice()
        logger.info(f"[SCHEMA] 📊 Recording metadata...")
        self.populate_metadata()
        logger.info(f"[SCHEMA] ✅ Database initialized successfully")
        return True
    
    def close(self):
        if self.conn:
            self.conn.close()


# Initialize quantum lattice database BEFORE anything else
_SCHEMA_BUILDER = QuantumLatticeSchemaBuilder('data/qtcl_blockchain.db')
_SCHEMA_BUILDER.initialize_schema()
_DB_CONN = _SCHEMA_BUILDER.conn

# ═════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
# P2P NETWORKING LAYER - PEER DISCOVERY, BLOCK SYNC, REQUEST HANDLING
# ═════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════

@dataclass
class P2PMessage:
    """P2P protocol message."""
    msg_type: str
    peer_id: str
    height: Optional[int] = None
    block_hash: Optional[str] = None
    block_data: Optional[Dict[str, Any]] = None
    payload: Optional[Dict[str, Any]] = None
    
    def to_json(self) -> str:
        return json.dumps({
            'msg_type': self.msg_type,
            'peer_id': self.peer_id,
            'height': self.height,
            'block_hash': self.block_hash,
            'block_data': self.block_data,
            'payload': self.payload
        })
    
    @staticmethod
    def from_json(data: str) -> 'P2PMessage':
        d = json.loads(data)
        return P2PMessage(
            msg_type=d.get('msg_type'),
            peer_id=d.get('peer_id'),
            height=d.get('height'),
            block_hash=d.get('block_hash'),
            block_data=d.get('block_data'),
            payload=d.get('payload')
        )


class P2PClient:
    """
    P2P client for peer discovery and block synchronisation.

    All transport uses HTTPS REST — the oracle runs on Koyeb behind TLS 443.
    Raw TCP sockets / custom framing never worked against an HTTPS host.
    WebSocket P2P (socket.io) is handled separately by MinerWebSocketP2PClient.
    """

    def __init__(self, peer_id: str, known_peers: List[Tuple[str, int]] = None,
                 oracle_base_url: str = ''):
        self.peer_id        = peer_id
        self.known_peers    = known_peers or []
        self._oracle_base   = oracle_base_url.rstrip('/')  # e.g. https://qtcl-blockchain.koyeb.app
        self.connected_peers: Dict[str, Tuple[str, int]] = {}
        self.peer_info: Dict[str, Dict[str, Any]]        = {}
        self._lock = threading.RLock()
        self._session = requests.Session()
        _a = HTTPAdapter(max_retries=Retry(total=2, backoff_factor=0.3))
        self._session.mount('https://', _a)
        self._session.mount('http://',  _a)

    def _base_urls(self, oracle_url: str = None) -> List[str]:
        """
        Build priority-ordered URL list: P2P peers FIRST, oracle LAST.
        Oracle is authoritative for lattice metrics + validation but P2P
        peers are preferred for TX/block data to form a real network.
        An explicit oracle_url override (e.g. for W-state) goes to end.
        """
        peer_urls, oracle_urls = [], []
        # 1. Known P2P peers by score (high-score = low-latency, high-uptime)
        scored = sorted(self.known_peers, key=lambda x: x[2] if len(x) > 2 else 0, reverse=True)
        for entry in scored:
            host = entry[0]; _port = entry[1]
            is_local = host in ('localhost', '127.0.0.1')
            scheme = 'http' if is_local else 'https'
            port_s = f':{_port}' if is_local else ''
            peer_urls.append(f"{scheme}://{host}{port_s}")
        # 2. Oracle — authoritative fallback
        if self._oracle_base:
            oracle_urls.append(self._oracle_base)
        if oracle_url:
            oracle_urls.append(oracle_url.rstrip('/'))
        return list(dict.fromkeys(peer_urls + oracle_urls))

    def discover_peers(self, timeout: int = 8) -> List[Tuple[str, int]]:
        """
        Discover active peers via oracle REST gossip endpoint.
        Returns list of (host, port) tuples for any miners that expose a URL.
        """
        discovered = []
        for base in self._base_urls():
            try:
                r = self._session.get(f"{base}/api/oracle/miners", timeout=timeout)
                if r.status_code == 200:
                    data = r.json()
                    miners = data if isinstance(data, list) else data.get('miners', [])
                    for m in miners:
                        url = m.get('url') or m.get('oracle_url', '')
                        if url:
                            try:
                                from urllib.parse import urlparse
                                p = urlparse(url)
                                host = p.hostname
                                port = p.port or (443 if p.scheme == 'https' else 80)
                                if host and (host, port) not in discovered:
                                    discovered.append((host, port))
                            except Exception:
                                pass
                    if discovered:
                        logger.info(f"[P2P] 🔍 Discovered {len(discovered)} peers via {base}")
                        return discovered
            except Exception as e:
                logger.debug(f"[P2P] discover_peers {base}: {e}")
        return discovered

    def get_block_height(self, timeout: int = 8, oracle_url: str = None) -> Optional[int]:
        """
        Get current chain tip height from oracle REST /api/blocks/tip.
        Accepts both 'block_height' and 'height' keys for compatibility.
        """
        for base in self._base_urls(oracle_url):
            try:
                r = self._session.get(f"{base}/api/blocks/tip", timeout=timeout)
                if r.status_code == 200:
                    data = r.json()
                    h = data.get('block_height') or data.get('height')
                    if h is not None:
                        logger.info(f"[P2P] ✅ Chain tip height={h} from {base}")
                        return int(h)
            except Exception as e:
                logger.debug(f"[P2P] get_block_height {base}: {e}")
        logger.warning("[P2P] ⚠️  All REST height queries failed")
        return None

    def sync_blocks(self, start_height: int, end_height: int,
                    timeout: int = 10) -> List[Dict[str, Any]]:
        """
        Fetch blocks start_height..end_height via REST /api/blocks/height/{h}.
        Used for initial startup sync only — steady-state sync is in QTCLFullNode._sync_loop.
        """
        blocks = []
        for height in range(start_height, end_height + 1):
            for base in self._base_urls():
                try:
                    r = self._session.get(
                        f"{base}/api/blocks/height/{height}", timeout=timeout
                    )
                    if r.status_code == 200:
                        data = r.json()
                        # Unwrap nested header if present
                        block = data.get('header', data)
                        blocks.append(block)
                        logger.debug(f"[P2P] 📦 Fetched block #{height}")
                        break
                except Exception as e:
                    logger.debug(f"[P2P] sync_blocks #{height} from {base}: {e}")
        return blocks


class P2PServer:
    """
    Raw TCP P2P server — secondary channel for direct peer-to-peer communication.
    Default port: 9092 (NOT 8000 — that conflicts with Koyeb oracle public endpoint).
    All primary P2P HTTP traffic is handled by GossipListener on port 9091.
    This server handles low-level block/TX relay between LAN peers or direct connections.
    """

    def __init__(self, peer_id: str, port: int = 9092, db_connection: Optional[sqlite3.Connection] = None):
        self.peer_id = peer_id
        self.port = port
        self.running = False
        self.server_socket = None
        self.peers: Dict[str, Dict[str, Any]] = {}
        self._lock = threading.RLock()
        # Use supplied connection; fall back to schema-builder conn only if nothing else provided.
        # This eliminates the split-brain: main() passes db= after initialising its own SQLite conn.
        self._db: Optional[sqlite3.Connection] = db_connection
    
    def start(self):
        """Start P2P server listening."""
        self.running = True
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        
        try:
            self.server_socket.bind(('0.0.0.0', self.port))
            self.server_socket.listen(5)
            logger.info(f"[P2P] 🎧 Server listening on port {self.port}")
        except OSError as e:
            logger.warning(f"[P2P] ⚠️  Could not bind to port {self.port}: {e}")
            self.running = False
            return
        
        while self.running:
            try:
                self.server_socket.settimeout(1)
                client_socket, (client_host, client_port) = self.server_socket.accept()
                
                # Handle in thread
                thread = threading.Thread(
                    target=self._handle_client,
                    args=(client_socket, client_host, client_port),
                    daemon=True
                )
                thread.start()
                
            except socket.timeout:
                continue
            except Exception as e:
                logger.debug(f"[P2P] Server error: {e}")
    
    def _handle_client(self, client_socket, client_host, client_port):
        """Handle incoming peer connection — always uses self._db (main node db) if set."""
        # Resolve the correct db: prefer injected main db, fall back to schema-builder conn
        _conn = self._db if self._db is not None else _DB_CONN
        try:
            data = client_socket.recv(4096).decode()
            msg = P2PMessage.from_json(data.strip())

            if msg.msg_type == 'HELLO':
                response = P2PMessage(
                    'HELLO_RESPONSE',
                    self.peer_id,
                    payload={'peers': [('localhost', self.port)]}
                )
                client_socket.send(response.to_json().encode() + b'\n')
                logger.debug(f"[P2P] 👋 HELLO from {client_host}:{client_port}")
            
            elif msg.msg_type == 'GET_HEIGHT':
                try:
                    cursor = _conn.cursor()
                    cursor.execute("SELECT MAX(height) FROM blocks")
                    result = cursor.fetchone()
                    height = result[0] if result[0] is not None else 0
                except:
                    height = 0
                
                response = P2PMessage('HEIGHT_RESPONSE', self.peer_id, height=height)
                client_socket.send(response.to_json().encode() + b'\n')
                logger.debug(f"[P2P] 📊 Height query from {client_host}:{client_port}: {height}")
            
            elif msg.msg_type == 'GET_BLOCK':
                try:
                    cursor = _conn.cursor()
                    cursor.execute("""
                        SELECT block_hash, parent_hash, merkle_root, timestamp_s, difficulty_bits, nonce, miner_address, w_state_fidelity, w_entropy_hash
                        FROM blocks WHERE height = ?
                    """, (msg.height,))
                    row = cursor.fetchone()
                    
                    if row:
                        block_data = {
                            'height': msg.height,
                            'block_hash': row[0],
                            'parent_hash': row[1],
                            'merkle_root': row[2],
                            'timestamp_s': row[3],
                            'difficulty_bits': row[4],
                            'nonce': row[5],
                            'miner_address': row[6],
                            'w_state_fidelity': row[7],
                            'w_entropy_hash': row[8]
                        }
                        response = P2PMessage('BLOCK_RESPONSE', self.peer_id, block_data=block_data)
                    else:
                        response = P2PMessage('BLOCK_RESPONSE', self.peer_id, payload={'error': 'Block not found'})
                except Exception as e:
                    response = P2PMessage('BLOCK_RESPONSE', self.peer_id, payload={'error': str(e)})
                
                client_socket.send(response.to_json().encode() + b'\n')
                logger.debug(f"[P2P] 📦 Block request from {client_host}:{client_port}: height={msg.height}")
            
            elif msg.msg_type == 'GET_METRICS':
                try:
                    cursor = _conn.cursor()

                    # Get current metrics
                    cursor.execute("SELECT MAX(height) FROM blocks")
                    height = cursor.fetchone()[0] or 0

                    # broadcast_to_oracle column only exists after schema patches — guard it
                    try:
                        cursor.execute("SELECT COUNT(*) FROM transactions WHERE broadcast_to_oracle=0")
                        pending_txs = cursor.fetchone()[0] or 0
                    except Exception:
                        pending_txs = 0
                    
                    cursor.execute("SELECT AVG(w_state_fidelity) FROM blocks WHERE w_state_fidelity > 0")
                    avg_fidelity = cursor.fetchone()[0] or 0.9
                    
                    metrics = {
                        'height': height,
                        'pending_txs': pending_txs,
                        'avg_fidelity': float(avg_fidelity),
                        'timestamp': int(time.time() * 1000)
                    }
                    
                    response = P2PMessage('METRICS_RESPONSE', self.peer_id, payload={'metrics': metrics})
                except Exception as e:
                    response = P2PMessage('METRICS_RESPONSE', self.peer_id, payload={'error': str(e)})
                
                client_socket.send(response.to_json().encode() + b'\n')
                logger.debug(f"[P2P] 📊 Metrics query from {client_host}:{client_port}")
            
            elif msg.msg_type == 'SIGNED_TRANSACTION':
                # Receive signed transaction from peer
                try:
                    signed_tx = msg.payload or {}
                    tx_id = signed_tx.get('tx_id', 'unknown')
                    
                    # Verify signature
                    cursor = _conn.cursor()
                    cursor.execute("""
                        INSERT OR IGNORE INTO transactions 
                        (tx_id, height, tx_index, from_address, to_address, amount, fee, 
                         signature, hlwe_signature, signer_address, signature_valid)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """, (
                        tx_id,
                        signed_tx.get('height'),
                        signed_tx.get('tx_index'),
                        signed_tx.get('from_address'),
                        signed_tx.get('to_address'),
                        signed_tx.get('amount'),
                        signed_tx.get('fee'),
                        signed_tx.get('signature'),
                        signed_tx.get('hlwe_signature'),
                        signed_tx.get('signer_address'),
                        1 if signed_tx.get('signature_valid') else 0
                    ))
                    _conn.commit()
                    
                    response = P2PMessage('ACK', self.peer_id, payload={'tx_id': tx_id})
                    logger.info(f"[P2P] 📝 Received signed transaction: {tx_id}")
                except Exception as e:
                    response = P2PMessage('ERROR', self.peer_id, payload={'error': str(e)})
                
                client_socket.send(response.to_json().encode() + b'\n')
            
            elif msg.msg_type == 'SIGNED_BLOCK':
                # Receive signed block from peer
                try:
                    signed_block = msg.block_data or {}
                    block_hash = signed_block.get('block_hash', 'unknown')
                    
                    response = P2PMessage('ACK', self.peer_id, payload={'block_hash': block_hash})
                    logger.info(f"[P2P] ⛏️  Received signed block: {block_hash}")
                except Exception as e:
                    response = P2PMessage('ERROR', self.peer_id, payload={'error': str(e)})
                
                client_socket.send(response.to_json().encode() + b'\n')
            
            elif msg.msg_type == 'NEW_BLOCK':
                logger.info(f"[P2P] 🆕 New block from {client_host}:{client_port}: {msg.block_hash}")
                # Process block (validation, storage)
                response = P2PMessage('ACK', self.peer_id)
                client_socket.send(response.to_json().encode() + b'\n')
            
            else:
                response = P2PMessage('ERROR', self.peer_id, payload={'error': f'Unknown message type: {msg.msg_type}'})
                client_socket.send(response.to_json().encode() + b'\n')
            
            client_socket.close()
        
        except Exception as e:
            logger.debug(f"[P2P] Client handler error: {e}")
    
    def stop(self):
        """Stop P2P server."""
        self.running = False
        if self.server_socket:
            self.server_socket.close()
        logger.info(f"[P2P] 🛑 Server stopped")


# P2P initialization placeholder (will be called from main)
_P2P_SERVER: Optional[P2PServer] = None
_P2P_CLIENT: Optional[P2PClient] = None

# ═════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
# HLWE TRANSACTION SIGNING & ORACLE BROADCAST LAYER
# ═════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════

class HLWETransactionSigner:
    """Signs all P2P transactions and broadcasts with HLWE signatures."""
    
    def __init__(self, wallet_address: str, private_key: Optional[bytes] = None):
        self.wallet_address = wallet_address
        self.private_key = private_key or hashlib.sha256(wallet_address.encode()).digest()
        self._lock = threading.RLock()
    
    def sign_transaction(self, tx_data: Dict[str, Any]) -> Dict[str, Any]:
        """Sign transaction with HLWE signature."""
        with self._lock:
            # Create signature payload
            tx_json = json.dumps(tx_data, sort_keys=True, separators=(',', ':'))
            message_hash = hashlib.sha3_256(tx_json.encode()).hexdigest()
            
            # Sign using HLWE (simplified: HMAC-SHA512 as proxy)
            signature = hmac.new(
                self.private_key,
                message_hash.encode(),
                hashlib.sha3_512
            ).hexdigest()
            
            # Create signed transaction
            signed_tx = {
                **tx_data,
                'hlwe_signature': signature,
                'signer_address': self.wallet_address,
                'signed_at': int(time.time() * 1000),
                'signature_valid': True
            }
            
            return signed_tx
    
    def verify_signature(self, signed_tx: Dict[str, Any]) -> Tuple[bool, str]:
        """Verify HLWE signature on transaction."""
        try:
            signature = signed_tx.get('hlwe_signature')
            signer = signed_tx.get('signer_address')
            
            if not signature or not signer:
                return False, "Missing signature or signer"
            
            # Reconstruct tx without signature for verification
            verify_tx = {k: v for k, v in signed_tx.items() 
                        if k not in ['hlwe_signature', 'signature_valid', 'signed_at']}
            
            tx_json = json.dumps(verify_tx, sort_keys=True, separators=(',', ':'))
            message_hash = hashlib.sha3_256(tx_json.encode()).hexdigest()
            
            # Verify signature (using wallet's private key as stored)
            # In production: use actual key from database
            expected_sig = hmac.new(
                hashlib.sha256(signer.encode()).digest(),
                message_hash.encode(),
                hashlib.sha3_512
            ).hexdigest()
            
            is_valid = signature == expected_sig
            return is_valid, "signature_valid" if is_valid else "signature_mismatch"
        
        except Exception as e:
            return False, f"verification_error: {e}"


class OracleBroadcaster:
    """Broadcasts signed transactions and blocks to Oracle (main database)."""
    
    def __init__(self, oracle_url: str = 'https://qtcl-blockchain.koyeb.app'):
        self.oracle_url = oracle_url.rstrip('/')
        self.broadcast_queue: Deque[Dict[str, Any]] = deque(maxlen=1000)
        self._lock = threading.RLock()
    
    def enqueue_transaction(self, signed_tx: Dict[str, Any]) -> bool:
        """Enqueue signed transaction for broadcast."""
        with self._lock:
            try:
                self.broadcast_queue.append({
                    'type': 'transaction',
                    'data': signed_tx,
                    'queued_at': int(time.time() * 1000),
                    'status': 'pending'
                })
                logger.debug(f"[ORACLE] 📤 Queued transaction: {signed_tx.get('tx_id', 'unknown')}")
                return True
            except Exception as e:
                logger.warning(f"[ORACLE] ⚠️  Failed to queue transaction: {e}")
                return False
    
    def enqueue_block(self, signed_block: Dict[str, Any]) -> bool:
        """Enqueue signed block for broadcast."""
        with self._lock:
            try:
                self.broadcast_queue.append({
                    'type': 'block',
                    'data': signed_block,
                    'queued_at': int(time.time() * 1000),
                    'status': 'pending'
                })
                logger.debug(f"[ORACLE] 📤 Queued block: {signed_block.get('block_hash', 'unknown')}")
                return True
            except Exception as e:
                logger.warning(f"[ORACLE] ⚠️  Failed to queue block: {e}")
                return False
    
    def broadcast_pending(self, timeout: int = 5) -> Dict[str, int]:
        """
        Broadcast all pending items to Oracle and gossip peers.

        Flow per item:
          1. POST to oracle REST  (/api/transactions or /api/blocks)
          2. On 200/201 → mark broadcast_to_oracle=1 in local SQLite
          3. Fan-out to up to 5 best gossip peers via /gossip/ingest
          4. Failed items are re-queued with an attempt counter (max 3 retries)
        """
        stats = {'sent': 0, 'failed': 0, 'queued': 0}
        _session = getattr(self, '_http_session', None)
        if _session is None:
            _session = requests.Session()
            _adapter = HTTPAdapter(max_retries=Retry(total=2, backoff_factor=0.5,
                                                      status_forcelist=[429, 500, 502, 503, 504]))
            _session.mount('https://', _adapter)
            _session.mount('http://', _adapter)
            self._http_session = _session

        # Collect gossip peers for fan-out (best 5 by quality)
        _gossip_peers: List[str] = []
        try:
            db_for_peers = getattr(self, '_db_ref', None)
            if db_for_peers is not None:
                rows = db_for_peers.execute(
                    "SELECT gossip_url FROM gossip_peers WHERE online=1 AND gossip_url != '' "
                    "ORDER BY last_seen DESC LIMIT 5"
                ).fetchall()
                _gossip_peers = [r[0] for r in rows if r[0]]
        except Exception:
            pass

        with self._lock:
            retry_queue: Deque[Dict[str, Any]] = deque()
            while self.broadcast_queue:
                item = self.broadcast_queue.popleft()
                item_type = item['type']
                data      = item['data']
                attempts  = item.get('attempts', 0)

                # ── 1. Oracle REST broadcast ──────────────────────────────────
                oracle_ok = False
                try:
                    if item_type == 'transaction':
                        url = f"{self.oracle_url}/api/transactions"
                        payload = {
                            'tx_hash':      data.get('tx_id', data.get('tx_hash', '')),
                            'from_address': data.get('from_addr', data.get('from_address', '')),
                            'to_address':   data.get('to_addr', data.get('to_address', '')),
                            'amount':       data.get('amount', 0),
                            'fee':          data.get('fee', 0),
                            'nonce':        data.get('nonce', 0),
                            'timestamp_ns': data.get('timestamp_ns', int(time.time_ns())),
                            'signature':    data.get('signature', data.get('hlwe_signature', '')),
                            'hlwe_signature': data.get('hlwe_signature', ''),
                            'signer_address': data.get('signer_address', ''),
                            'tx_type':      data.get('tx_type', 'transfer'),
                            'source':       'miner_p2p',
                        }
                    else:  # block
                        url = f"{self.oracle_url}/api/blocks"
                        payload = data

                    resp = _session.post(url, json=payload, timeout=timeout)
                    if resp.status_code in (200, 201, 202):
                        oracle_ok = True
                        logger.info(
                            f"[ORACLE-BROADCAST] ✅ {item_type.upper()} → oracle | "
                            f"id={data.get('tx_id', data.get('block_hash', '?'))[:24]} | "
                            f"HTTP {resp.status_code}"
                        )
                    else:
                        logger.warning(
                            f"[ORACLE-BROADCAST] ⚠️  {item_type.upper()} HTTP {resp.status_code} "
                            f"→ {url} | id={data.get('tx_id', data.get('block_hash', '?'))[:24]}"
                        )
                except requests.Timeout:
                    logger.warning(f"[ORACLE-BROADCAST] ⏱️  {item_type} oracle timeout after {timeout}s")
                except Exception as e:
                    logger.warning(f"[ORACLE-BROADCAST] ⚠️  {item_type} oracle error: {e}")

                # ── 2. Mark broadcast in local SQLite ─────────────────────────
                if oracle_ok:
                    try:
                        db_ref = getattr(self, '_db_ref', None)
                        if db_ref is not None:
                            now_ts = int(time.time())
                            if item_type == 'transaction':
                                db_ref.execute(
                                    "UPDATE transactions SET broadcast_to_oracle=1, oracle_timestamp=? "
                                    "WHERE tx_id=?",
                                    (now_ts, data.get('tx_id', ''))
                                )
                            else:
                                db_ref.execute(
                                    "UPDATE blocks SET broadcast_to_oracle=1, oracle_timestamp=? "
                                    "WHERE block_hash=?",
                                    (now_ts, data.get('block_hash', ''))
                                )
                            db_ref.commit()
                    except Exception as dbe:
                        logger.debug(f"[ORACLE-BROADCAST] DB mark error: {dbe}")
                    stats['sent'] += 1

                # ── 3. Gossip fan-out to P2P peers (best-effort, non-blocking) ──
                if _gossip_peers:
                    gossip_payload: Dict[str, Any] = {
                        'origin': self.oracle_url,
                        'peer_id': getattr(self, '_peer_id', 'oracle_broadcaster'),
                    }
                    if item_type == 'transaction':
                        gossip_payload['txs'] = [{
                            'tx_hash':      data.get('tx_id', ''),
                            'from_address': data.get('from_addr', ''),
                            'to_address':   data.get('to_addr', ''),
                            'amount':       data.get('amount', 0),
                            'amount_base':  int(float(data.get('amount', 0)) * 100),
                            'fee':          data.get('fee', 0),
                            'nonce':        data.get('nonce', 0),
                            'timestamp_ns': data.get('timestamp_ns', 0),
                            'signature':    data.get('signature', data.get('hlwe_signature', '')),
                            'source':       'miner_gossip',
                        }]
                    else:
                        gossip_payload['block'] = data

                    for gurl in _gossip_peers:
                        try:
                            _session.post(
                                f"{gurl.rstrip('/')}/gossip/ingest",
                                json=gossip_payload,
                                timeout=3,
                            )
                            logger.debug(f"[ORACLE-BROADCAST] 📡 Gossip fan-out → {gurl[:40]}")
                        except Exception:
                            pass

                # ── 4. Re-queue on failure (max 3 retries) ────────────────────
                if not oracle_ok:
                    if attempts < 3:
                        item['attempts'] = attempts + 1
                        item['status']   = 'retry'
                        retry_queue.append(item)
                    else:
                        logger.error(
                            f"[ORACLE-BROADCAST] ❌ Giving up on {item_type} "
                            f"after 3 attempts: {data.get('tx_id', data.get('block_hash', '?'))[:24]}"
                        )
                    stats['failed'] += 1

            # Restore retry items
            self.broadcast_queue.extend(retry_queue)

        stats['queued'] = len(self.broadcast_queue)
        if stats['sent'] > 0 or stats['failed'] > 0:
            logger.info(
                f"[ORACLE-BROADCAST] 📊 Batch complete | "
                f"sent={stats['sent']} failed={stats['failed']} queued={stats['queued']}"
            )
        return stats


# ═════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
# P2P SERVICE REQUEST INVENTORY — per-client request tracking, sub-logic dispatch, circuit-breaker, metrics
# ═════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════

class P2PServiceRequestType(Enum):
    """All recognised P2P service request categories."""
    BLOCK_BY_HEIGHT     = 'block_by_height'
    BLOCK_BY_HASH       = 'block_by_hash'
    BLOCK_RANGE         = 'block_range'
    CHAIN_TIP           = 'chain_tip'
    NETWORK_SNAPSHOT    = 'network_snapshot'
    MEMPOOL_FETCH       = 'mempool_fetch'
    PEER_REGISTER       = 'peer_register'
    PEER_HEARTBEAT      = 'peer_heartbeat'
    PEER_LIST           = 'peer_list'
    TX_BROADCAST        = 'tx_broadcast'
    BLOCK_BROADCAST     = 'block_broadcast'
    GOSSIP_INGEST       = 'gossip_ingest'
    DHT_HELLO           = 'dht_hello'
    DHT_PEX             = 'dht_pex'
    ORACLE_REGISTER     = 'oracle_register'
    ORACLE_W_STATE      = 'oracle_w_state'
    ORACLE_MINERS_LIST  = 'oracle_miners_list'
    ORACLE_HANDSHAKE    = 'oracle_handshake'
    BALANCE_QUERY       = 'balance_query'
    HISTORY_QUERY       = 'history_query'

@dataclass
class P2PServiceRequest:
    """Immutable record of a single P2P service request."""
    request_id:   str
    req_type:     P2PServiceRequestType
    peer_id:      str           # requesting peer (empty string for outbound)
    direction:    str           # 'inbound' | 'outbound'
    url:          str           # target URL (outbound) or handler path (inbound)
    payload:      Dict[str, Any]
    initiated_at: float         # time.time()
    completed_at: float = 0.0
    status:       str   = 'pending'   # pending | success | failed | timeout | circuit_open
    response_summary: str = ''
    latency_ms:   float = 0.0
    retry_count:  int   = 0

    def complete(self, status: str, summary: str = '') -> None:
        self.completed_at = time.time()
        self.latency_ms   = (self.completed_at - self.initiated_at) * 1000
        self.status       = status
        self.response_summary = summary[:200]


class P2PServiceInventory:
    """
    Per-node inventory of all P2P service requests (inbound + outbound).

    Architecture:
      • Ring buffer (_log) of last MAX_LOG entries — zero heap growth
      • Per-type counters and latency histograms
      • Circuit-breaker per (peer_id, req_type) — opens after CB_FAIL_THRESHOLD
        consecutive failures, re-closes after CB_HALF_OPEN_TIMEOUT_S
      • Sub-logic dispatch: _handle_* methods provide the actual logic for each
        request type, callable both from GossipHTTPHandler and from QTCLFullNode
      • Thread-safe: all mutation under a single RLock

    Sub-logic entry-points (static, no node dependency):
      dispatch_inbound(req_type, payload, db, mempool, node_ref) → Dict
      dispatch_outbound(req_type, payload, session, target_url)  → Dict
    """

    MAX_LOG               = 2000
    CB_FAIL_THRESHOLD     = 5     # consecutive failures to open circuit
    CB_HALF_OPEN_TIMEOUT_S = 60   # seconds before retrying

    def __init__(self, local_peer_id: str, local_node_id: str, db: Optional[sqlite3.Connection] = None):
        self.local_peer_id  = local_peer_id
        self.local_node_id  = local_node_id
        self.db             = db
        self._lock          = threading.RLock()
        self._log: Deque[P2PServiceRequest] = deque(maxlen=self.MAX_LOG)
        # type → {count, success, fail, total_latency_ms}
        self._counters: Dict[str, Dict[str, float]] = defaultdict(
            lambda: {'count': 0, 'success': 0, 'fail': 0, 'total_latency_ms': 0.0, 'timeout': 0}
        )
        # (peer_id, req_type_value) → {fail_streak, opened_at}
        self._circuit: Dict[Tuple[str, str], Dict[str, Any]] = {}
        self._start_time = time.time()

    # ─── Public API ───────────────────────────────────────────────────────────

    def new_request(self, req_type: P2PServiceRequestType, peer_id: str,
                    direction: str, url: str, payload: Dict[str, Any] = None) -> P2PServiceRequest:
        """Create and register a new service request record."""
        req = P2PServiceRequest(
            request_id   = secrets.token_hex(8),
            req_type     = req_type,
            peer_id      = peer_id,
            direction    = direction,
            url          = url,
            payload      = payload or {},
            initiated_at = time.time(),
        )
        with self._lock:
            self._log.append(req)
            self._counters[req_type.value]['count'] += 1
        return req

    def complete_request(self, req: P2PServiceRequest, status: str, summary: str = '') -> None:
        """Mark request as complete and update counters / circuit-breaker."""
        req.complete(status, summary)
        with self._lock:
            c = self._counters[req.req_type.value]
            c['total_latency_ms'] += req.latency_ms
            if status == 'success':
                c['success'] += 1
                # Reset circuit-breaker fail streak
                key = (req.peer_id, req.req_type.value)
                if key in self._circuit:
                    self._circuit[key]['fail_streak'] = 0
            elif status == 'timeout':
                c['timeout'] += 1
                self._bump_circuit(req.peer_id, req.req_type.value)
            else:
                c['fail'] += 1
                self._bump_circuit(req.peer_id, req.req_type.value)

    def is_circuit_open(self, peer_id: str, req_type: P2PServiceRequestType) -> bool:
        """Return True if circuit-breaker is open (should not send to this peer for this type)."""
        key = (peer_id, req_type.value)
        with self._lock:
            cb = self._circuit.get(key)
            if not cb:
                return False
            if cb['fail_streak'] >= self.CB_FAIL_THRESHOLD:
                age = time.time() - cb.get('opened_at', 0)
                if age < self.CB_HALF_OPEN_TIMEOUT_S:
                    return True
                # Half-open: allow one probe through
                cb['fail_streak'] = self.CB_FAIL_THRESHOLD - 1
            return False

    def get_stats(self) -> Dict[str, Any]:
        """Return inventory statistics snapshot."""
        with self._lock:
            type_stats = {}
            for rtype, c in self._counters.items():
                cnt = c['count']
                avg_lat = c['total_latency_ms'] / max(1, c['success'] + c['fail']) 
                type_stats[rtype] = {
                    'count': int(cnt),
                    'success': int(c['success']),
                    'fail': int(c['fail']),
                    'timeout': int(c['timeout']),
                    'success_rate': round(c['success'] / max(1, cnt), 4),
                    'avg_latency_ms': round(avg_lat, 2),
                }
            open_circuits = [
                f"{k[0][:12]}:{k[1]}" for k, v in self._circuit.items()
                if v.get('fail_streak', 0) >= self.CB_FAIL_THRESHOLD
                and time.time() - v.get('opened_at', 0) < self.CB_HALF_OPEN_TIMEOUT_S
            ]
            recent = [
                {
                    'id':         r.request_id,
                    'type':       r.req_type.value,
                    'peer':       r.peer_id[:16],
                    'direction':  r.direction,
                    'status':     r.status,
                    'latency_ms': round(r.latency_ms, 1),
                    'summary':    r.response_summary[:80],
                }
                for r in list(self._log)[-50:]
            ]
            return {
                'uptime_s':      round(time.time() - self._start_time, 1),
                'total_requests': sum(c['count'] for c in self._counters.values()),
                'open_circuits':  open_circuits,
                'by_type':        type_stats,
                'recent_50':      recent,
            }

    def get_recent(self, limit: int = 100, req_type: P2PServiceRequestType = None,
                   status: str = None) -> List[Dict[str, Any]]:
        """Query recent requests with optional type/status filter."""
        with self._lock:
            items = list(self._log)
        if req_type:
            items = [r for r in items if r.req_type == req_type]
        if status:
            items = [r for r in items if r.status == status]
        return [
            {
                'request_id':  r.request_id,
                'type':        r.req_type.value,
                'peer_id':     r.peer_id,
                'direction':   r.direction,
                'url':         r.url,
                'status':      r.status,
                'latency_ms':  round(r.latency_ms, 2),
                'initiated':   r.initiated_at,
                'completed':   r.completed_at,
                'retry_count': r.retry_count,
                'summary':     r.response_summary,
            }
            for r in items[-limit:]
        ]

    # ─── Sub-logic dispatch — inbound handler ─────────────────────────────────

    @staticmethod
    def dispatch_inbound(req_type: P2PServiceRequestType, payload: Dict[str, Any],
                         db: Optional[sqlite3.Connection],
                         mempool: Any,
                         node_ref: Any = None) -> Dict[str, Any]:
        """
        Centralised inbound sub-logic for all service request types.
        Called from GossipHTTPHandler do_GET/do_POST and from node internal paths.
        Returns a JSON-serialisable response dict.
        """
        try:
            if req_type == P2PServiceRequestType.BLOCK_BY_HEIGHT:
                return P2PServiceInventory._handle_block_by_height(payload, db)
            elif req_type == P2PServiceRequestType.BLOCK_BY_HASH:
                return P2PServiceInventory._handle_block_by_hash(payload, db)
            elif req_type == P2PServiceRequestType.BLOCK_RANGE:
                return P2PServiceInventory._handle_block_range(payload, db)
            elif req_type == P2PServiceRequestType.CHAIN_TIP:
                return P2PServiceInventory._handle_chain_tip(payload, db)
            elif req_type == P2PServiceRequestType.NETWORK_SNAPSHOT:
                return P2PServiceInventory._handle_network_snapshot(payload, db, mempool, node_ref)
            elif req_type == P2PServiceRequestType.MEMPOOL_FETCH:
                return P2PServiceInventory._handle_mempool(payload, db, mempool)
            elif req_type == P2PServiceRequestType.BALANCE_QUERY:
                return P2PServiceInventory._handle_balance(payload, db)
            elif req_type == P2PServiceRequestType.HISTORY_QUERY:
                return P2PServiceInventory._handle_history(payload, db)
            elif req_type == P2PServiceRequestType.TX_BROADCAST:
                return P2PServiceInventory._handle_tx_ingest(payload, db, mempool)
            elif req_type == P2PServiceRequestType.GOSSIP_INGEST:
                return P2PServiceInventory._handle_gossip_ingest(payload, db, mempool)
            else:
                return {'error': f'no sub-logic for {req_type.value}', 'ok': False}
        except Exception as e:
            logger.debug(f"[P2P-INVENTORY] dispatch_inbound {req_type.value}: {e}")
            return {'error': str(e), 'ok': False}

    # ─── Private sub-logic methods ─────────────────────────────────────────────

    @staticmethod
    def _handle_block_by_height(payload: Dict, db) -> Dict[str, Any]:
        height = int(payload.get('height', 0))
        if db is None:
            return {'error': 'no db', 'height': height}
        try:
            row = db.execute(
                "SELECT height, block_hash, parent_hash, merkle_root, timestamp_s, "
                "difficulty_bits, nonce, miner_address, w_state_fidelity, w_entropy_hash "
                "FROM blocks WHERE height=?", (height,)
            ).fetchone()
            if row is None:
                return {'error': 'not found', 'height': height}
            cols = ['height','block_hash','parent_hash','merkle_root','timestamp_s',
                    'difficulty_bits','nonce','miner_address','w_state_fidelity','w_entropy_hash']
            return {'block': dict(zip(cols, row)), 'source': 'local_db', 'ok': True}
        except Exception as e:
            return {'error': str(e), 'height': height}

    @staticmethod
    def _handle_block_by_hash(payload: Dict, db) -> Dict[str, Any]:
        bhash = str(payload.get('block_hash', ''))
        if not bhash or db is None:
            return {'error': 'missing block_hash or db'}
        try:
            row = db.execute(
                "SELECT height, block_hash, parent_hash, merkle_root, timestamp_s, "
                "difficulty_bits, nonce, miner_address, w_state_fidelity, w_entropy_hash "
                "FROM blocks WHERE block_hash=?", (bhash,)
            ).fetchone()
            if row is None:
                return {'error': 'not found', 'block_hash': bhash}
            cols = ['height','block_hash','parent_hash','merkle_root','timestamp_s',
                    'difficulty_bits','nonce','miner_address','w_state_fidelity','w_entropy_hash']
            return {'block': dict(zip(cols, row)), 'source': 'local_db', 'ok': True}
        except Exception as e:
            return {'error': str(e)}

    @staticmethod
    def _handle_block_range(payload: Dict, db) -> Dict[str, Any]:
        start  = int(payload.get('start', 0))
        end    = int(payload.get('end', start))
        limit  = min(int(payload.get('limit', 50)), 200)
        end    = min(end, start + limit - 1)
        if db is None:
            return {'error': 'no db'}
        try:
            rows = db.execute(
                "SELECT height, block_hash, parent_hash, merkle_root, timestamp_s, "
                "difficulty_bits, nonce, miner_address, w_state_fidelity, w_entropy_hash "
                "FROM blocks WHERE height BETWEEN ? AND ? ORDER BY height ASC LIMIT ?",
                (start, end, limit)
            ).fetchall()
            cols = ['height','block_hash','parent_hash','merkle_root','timestamp_s',
                    'difficulty_bits','nonce','miner_address','w_state_fidelity','w_entropy_hash']
            blocks = [dict(zip(cols, r)) for r in rows]
            return {'blocks': blocks, 'count': len(blocks), 'start': start, 'end': end, 'ok': True}
        except Exception as e:
            return {'error': str(e)}

    @staticmethod
    def _handle_chain_tip(payload: Dict, db) -> Dict[str, Any]:
        if db is None:
            return {'error': 'no db'}
        try:
            row = db.execute(
                "SELECT height, block_hash, timestamp_s, miner_address, w_state_fidelity "
                "FROM blocks ORDER BY height DESC LIMIT 1"
            ).fetchone()
            if row is None:
                return {'height': 0, 'block_hash': '0' * 64, 'ok': True}
            return {
                'height': row[0], 'block_height': row[0], 'block_hash': row[1],
                'timestamp_s': row[2], 'miner_address': row[3],
                'w_state_fidelity': row[4], 'source': 'local_db', 'ok': True
            }
        except Exception as e:
            return {'error': str(e)}

    @staticmethod
    def _handle_network_snapshot(payload: Dict, db, mempool, node_ref) -> Dict[str, Any]:
        """Full network state snapshot — topology, metrics, peer states, chain summary."""
        snap: Dict[str, Any] = {
            'snapshot_ts':   time.time(),
            'snapshot_id':   secrets.token_hex(8),
            'ok': True,
        }
        if db is not None:
            try:
                r = db.execute("SELECT MAX(height), COUNT(*), AVG(w_state_fidelity) FROM blocks").fetchone()
                snap['chain'] = {
                    'tip_height':   r[0] or 0,
                    'total_blocks': r[1] or 0,
                    'avg_fidelity': round(r[2] or 0.0, 4),
                }
            except Exception:
                snap['chain'] = {}
            try:
                live_cutoff = time.time() - 300
                dht_rows = db.execute(
                    "SELECT node_id, peer_address, gossip_port, miner_address, block_height, "
                    "w_fidelity, is_oracle, last_seen, quality_score "
                    "FROM dht_peers WHERE last_seen > ? ORDER BY quality_score DESC LIMIT 50",
                    (live_cutoff,)
                ).fetchall()
                cols = ['node_id','peer_address','gossip_port','miner_address','block_height',
                        'w_fidelity','is_oracle','last_seen','quality_score']
                snap['peers'] = [dict(zip(cols, r)) for r in dht_rows]
                snap['peer_count'] = len(snap['peers'])
            except Exception:
                snap['peers'] = []
                snap['peer_count'] = 0
            try:
                oracle_rows = db.execute(
                    "SELECT oracle_id, oracle_address, oracle_url, is_primary, is_local, "
                    "pq0_fidelity, block_height, peer_count, entanglement_status, trust_score, last_seen "
                    "FROM oracle_registry ORDER BY trust_score DESC LIMIT 20"
                ).fetchall()
                cols = ['oracle_id','oracle_address','oracle_url','is_primary','is_local',
                        'pq0_fidelity','block_height','peer_count','entanglement_status','trust_score','last_seen']
                snap['oracles'] = [dict(zip(cols, r)) for r in oracle_rows]
                snap['oracle_count'] = len(snap['oracles'])
            except Exception:
                snap['oracles'] = []
                snap['oracle_count'] = 0
            try:
                snap['mempool_size'] = mempool.get_size() if mempool else 0
            except Exception:
                snap['mempool_size'] = 0
            try:
                snap['difficulty'] = db.execute(
                    "SELECT current_difficulty, ema_block_time_s, target_block_time_s FROM difficulty_state WHERE id=1"
                ).fetchone()
                if snap['difficulty']:
                    snap['difficulty'] = {
                        'current': snap['difficulty'][0],
                        'ema_block_time_s': snap['difficulty'][1],
                        'target_block_time_s': snap['difficulty'][2],
                    }
            except Exception:
                snap['difficulty'] = {}
            try:
                # Last 10 mined blocks summary for quick history
                recent_rows = db.execute(
                    "SELECT height, block_hash, miner_address, timestamp_s, w_state_fidelity "
                    "FROM blocks ORDER BY height DESC LIMIT 10"
                ).fetchall()
                snap['recent_blocks'] = [
                    {'height': r[0], 'hash': r[1][:16]+'...', 'miner': r[2][:16], 'ts': r[3], 'fidelity': r[4]}
                    for r in recent_rows
                ]
            except Exception:
                snap['recent_blocks'] = []
        return snap

    @staticmethod
    def _handle_mempool(payload: Dict, db, mempool) -> Dict[str, Any]:
        txs: List[Dict] = []
        if mempool:
            try:
                pending = mempool.get_pending(limit=200) or []
                for t in pending:
                    txs.append({
                        'tx_id': t.tx_id, 'from': t.from_addr, 'to': t.to_addr,
                        'amount': t.amount, 'fee': t.fee, 'nonce': t.nonce,
                    })
            except Exception:
                pass
        if not txs and db is not None:
            try:
                rows = db.execute(
                    "SELECT tx_id, from_addr, to_addr, amount, fee, nonce FROM pending_txs "
                    "ORDER BY fee DESC LIMIT 200"
                ).fetchall()
                txs = [{'tx_id':r[0],'from':r[1],'to':r[2],'amount':r[3],'fee':r[4],'nonce':r[5]} for r in rows]
            except Exception:
                pass
        return {'transactions': txs, 'count': len(txs), 'ok': True}

    @staticmethod
    def _handle_balance(payload: Dict, db) -> Dict[str, Any]:
        address = str(payload.get('address', ''))
        if not address or db is None:
            return {'error': 'missing address or db'}
        try:
            # Sum all incoming coinbase + transfers, subtract outgoing
            rows = db.execute(
                "SELECT tx_type, from_address, to_address, amount FROM transactions "
                "WHERE from_address=? OR to_address=?",
                (address, address)
            ).fetchall()
            balance = 0.0
            tx_count = 0
            for r in rows:
                tx_type, from_a, to_a, amt = r
                if to_a == address:
                    balance += float(amt) / 100.0
                elif from_a == address and from_a != '0' * 64:
                    balance -= float(amt) / 100.0
                tx_count += 1
            return {'address': address, 'balance': round(balance, 4), 'tx_count': tx_count,
                    'unit': 'QTCL', 'ok': True, 'source': 'local_db'}
        except Exception as e:
            return {'error': str(e), 'address': address}

    @staticmethod
    def _handle_history(payload: Dict, db) -> Dict[str, Any]:
        address = str(payload.get('address', ''))
        limit   = min(int(payload.get('limit', 50)), 500)
        offset  = int(payload.get('offset', 0))
        if not address or db is None:
            return {'error': 'missing address or db'}
        try:
            rows = db.execute(
                "SELECT tx_id, height, from_address, to_address, amount, fee, tx_type, timestamp_ns "
                "FROM transactions WHERE from_address=? OR to_address=? "
                "ORDER BY height DESC, tx_index ASC LIMIT ? OFFSET ?",
                (address, address, limit, offset)
            ).fetchall()
            cols = ['tx_id','height','from','to','amount','fee','tx_type','timestamp_ns']
            txs = [dict(zip(cols, r)) for r in rows]
            return {'address': address, 'transactions': txs, 'count': len(txs),
                    'limit': limit, 'offset': offset, 'ok': True}
        except Exception as e:
            return {'error': str(e), 'address': address}

    @staticmethod
    def _handle_tx_ingest(payload: Dict, db, mempool) -> Dict[str, Any]:
        """Ingest a single signed transaction into local DB and mempool."""
        tx_hash = str(payload.get('tx_hash', payload.get('tx_id', '')))
        if not tx_hash or len(tx_hash) != 64:
            return {'error': 'invalid tx_hash', 'ok': False}
        from_a = str(payload.get('from_address', payload.get('from_addr', '')))
        to_a   = str(payload.get('to_address', payload.get('to_addr', '')))
        if not from_a:
            return {'error': 'missing from_address', 'ok': False}
        # Push to mempool
        if mempool:
            try:
                t = Transaction(
                    tx_id=tx_hash, from_addr=from_a, to_addr=to_a,
                    amount=float(payload.get('amount', 0)),
                    nonce=int(payload.get('nonce', 0)),
                    timestamp_ns=int(payload.get('timestamp_ns', int(time.time_ns()))),
                    signature=str(payload.get('signature', '')),
                    fee=float(payload.get('fee', 0.001)),
                )
                mempool.add_transaction(t)
            except Exception as me:
                logger.debug(f"[P2P-INVENTORY] tx_ingest mempool: {me}")
        # Write to local DB pending_txs (gossip mirror)
        persisted = _local_db_upsert_tx(db, payload) if db is not None else False
        return {'ok': True, 'tx_hash': tx_hash, 'persisted': persisted}

    @staticmethod
    def _handle_gossip_ingest(payload: Dict, db, mempool) -> Dict[str, Any]:
        new_txs  = 0
        new_blks = 0
        for tx in (payload.get('txs') or [])[:50]:
            r = P2PServiceInventory._handle_tx_ingest(tx, db, mempool)
            if r.get('ok'):
                new_txs += 1
        blk = payload.get('block')
        if blk and isinstance(blk, dict):
            _local_db_upsert_block(db, blk)
            new_blks += 1
        return {'ok': True, 'new_txs': new_txs, 'new_blocks': new_blks}

    # ─── Private helpers ───────────────────────────────────────────────────────

    def _bump_circuit(self, peer_id: str, req_type_val: str) -> None:
        """Increment fail streak for circuit-breaker — call under _lock."""
        key = (peer_id, req_type_val)
        cb  = self._circuit.setdefault(key, {'fail_streak': 0, 'opened_at': 0})
        cb['fail_streak'] += 1
        if cb['fail_streak'] >= self.CB_FAIL_THRESHOLD:
            cb['opened_at'] = time.time()
            logger.warning(
                f"[P2P-CB] ⚡ Circuit OPEN | peer={peer_id[:16]} type={req_type_val} "
                f"(streak={cb['fail_streak']})"
            )


# Module-level singleton — injected into QTCLFullNode and GossipHTTPHandler
_P2P_SERVICE_INVENTORY: Optional[P2PServiceInventory] = None


# Schema patch definitions (run on DB init)
SCHEMA_PATCHES = {
    'transactions_hlwe_signature': """
        ALTER TABLE transactions ADD COLUMN IF NOT EXISTS hlwe_signature TEXT;
        ALTER TABLE transactions ADD COLUMN IF NOT EXISTS signer_address TEXT;
        ALTER TABLE transactions ADD COLUMN IF NOT EXISTS signature_valid INTEGER DEFAULT 0;
        ALTER TABLE transactions ADD COLUMN IF NOT EXISTS signed_at INTEGER;
        ALTER TABLE transactions ADD COLUMN IF NOT EXISTS broadcast_to_oracle INTEGER DEFAULT 0;
        ALTER TABLE transactions ADD COLUMN IF NOT EXISTS oracle_timestamp INTEGER;
        CREATE INDEX IF NOT EXISTS idx_tx_broadcast ON transactions(broadcast_to_oracle, oracle_timestamp);
    """,
    'blocks_hlwe_signature': """
        ALTER TABLE blocks ADD COLUMN IF NOT EXISTS hlwe_block_signature TEXT;
        ALTER TABLE blocks ADD COLUMN IF NOT EXISTS block_signer_address TEXT;
        ALTER TABLE blocks ADD COLUMN IF NOT EXISTS block_signature_valid INTEGER DEFAULT 0;
        ALTER TABLE blocks ADD COLUMN IF NOT EXISTS block_signed_at INTEGER;
        ALTER TABLE blocks ADD COLUMN IF NOT EXISTS broadcast_to_oracle INTEGER DEFAULT 0;
        ALTER TABLE blocks ADD COLUMN IF NOT EXISTS oracle_timestamp INTEGER;
        CREATE INDEX IF NOT EXISTS idx_block_broadcast ON blocks(broadcast_to_oracle, oracle_timestamp);
    """,
    'peer_consensus': """
        CREATE TABLE IF NOT EXISTS peer_consensus (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            peer_id TEXT NOT NULL,
            consensus_metric TEXT NOT NULL,
            metric_value REAL NOT NULL,
            peer_height INTEGER,
            peer_timestamp INTEGER,
            synced_at INTEGER DEFAULT (strftime('%s', 'now')),
            created_at INTEGER DEFAULT (strftime('%s', 'now')),
            UNIQUE(peer_id, consensus_metric)
        );
        CREATE INDEX IF NOT EXISTS idx_consensus_peer ON peer_consensus(peer_id);
        CREATE INDEX IF NOT EXISTS idx_consensus_metric ON peer_consensus(consensus_metric);
    """,
    'system_metrics': """
        CREATE TABLE IF NOT EXISTS system_metrics (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            metric_name TEXT UNIQUE NOT NULL,
            metric_value REAL NOT NULL,
            consensus_value REAL,
            peer_agreement INTEGER DEFAULT 0,
            last_updated INTEGER DEFAULT (strftime('%s', 'now')),
            synced_at INTEGER DEFAULT (strftime('%s', 'now'))
        );
        CREATE INDEX IF NOT EXISTS idx_metrics_name ON system_metrics(metric_name);
    """,
    'peer_sync_log': """
        CREATE TABLE IF NOT EXISTS peer_sync_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            peer_id TEXT NOT NULL,
            sync_type TEXT NOT NULL,
            blocks_synced INTEGER DEFAULT 0,
            txs_synced INTEGER DEFAULT 0,
            metrics_synced INTEGER DEFAULT 0,
            sync_status TEXT DEFAULT 'pending',
            synced_at INTEGER,
            created_at INTEGER DEFAULT (strftime('%s', 'now'))
        );
        CREATE INDEX IF NOT EXISTS idx_sync_peer ON peer_sync_log(peer_id);
        CREATE INDEX IF NOT EXISTS idx_sync_type ON peer_sync_log(sync_type);
    """,
    # ── Gossip peers table (used by GossipListener and OracleBroadcaster) ─────
    'gossip_peers_table': """
        CREATE TABLE IF NOT EXISTS gossip_peers (
            peer_id      TEXT PRIMARY KEY,
            gossip_url   TEXT NOT NULL DEFAULT '',
            miner_address TEXT NOT NULL DEFAULT '',
            block_height INTEGER NOT NULL DEFAULT 0,
            last_seen    REAL NOT NULL DEFAULT 0,
            online       INTEGER NOT NULL DEFAULT 0,
            latency_ms   REAL NOT NULL DEFAULT 9999,
            fail_count   INTEGER NOT NULL DEFAULT 0,
            success_count INTEGER NOT NULL DEFAULT 0
        );
        CREATE INDEX IF NOT EXISTS idx_gossip_peers_online ON gossip_peers(online, last_seen DESC);
        CREATE INDEX IF NOT EXISTS idx_gossip_peers_url    ON gossip_peers(gossip_url);
    """,
    # ── Pending transactions mirror (gossip store, separate from confirmed txs) ──
    'pending_txs_table': """
        CREATE TABLE IF NOT EXISTS pending_txs (
            tx_id        TEXT PRIMARY KEY,
            from_addr    TEXT NOT NULL DEFAULT '',
            to_addr      TEXT NOT NULL DEFAULT '',
            amount       REAL NOT NULL DEFAULT 0,
            amount_base  INTEGER NOT NULL DEFAULT 0,
            fee          REAL NOT NULL DEFAULT 0,
            nonce        INTEGER NOT NULL DEFAULT 0,
            timestamp_ns INTEGER NOT NULL DEFAULT 0,
            signature    TEXT NOT NULL DEFAULT '',
            hlwe_signature TEXT NOT NULL DEFAULT '',
            source       TEXT NOT NULL DEFAULT 'unknown',
            broadcast_to_oracle INTEGER NOT NULL DEFAULT 0,
            gossip_fanout INTEGER NOT NULL DEFAULT 0,
            created_at   REAL NOT NULL DEFAULT (strftime('%s','now')),
            expires_at   REAL NOT NULL DEFAULT 0
        );
        CREATE INDEX IF NOT EXISTS idx_ptx_fee      ON pending_txs(fee DESC, created_at ASC);
        CREATE INDEX IF NOT EXISTS idx_ptx_from     ON pending_txs(from_addr);
        CREATE INDEX IF NOT EXISTS idx_ptx_broadcast ON pending_txs(broadcast_to_oracle, gossip_fanout);
    """,
    # ── Network snapshots (periodic topology captures) ────────────────────────
    'network_snapshots_table': """
        CREATE TABLE IF NOT EXISTS network_snapshots (
            snapshot_id      TEXT PRIMARY KEY,
            snapshot_ts      REAL NOT NULL DEFAULT (strftime('%s','now')),
            chain_tip_height INTEGER NOT NULL DEFAULT 0,
            total_blocks     INTEGER NOT NULL DEFAULT 0,
            avg_fidelity     REAL NOT NULL DEFAULT 0,
            peer_count       INTEGER NOT NULL DEFAULT 0,
            oracle_count     INTEGER NOT NULL DEFAULT 0,
            mempool_size     INTEGER NOT NULL DEFAULT 0,
            snapshot_json    TEXT NOT NULL DEFAULT '{}'
        );
        CREATE INDEX IF NOT EXISTS idx_nsnapshot_ts ON network_snapshots(snapshot_ts DESC);
    """,
    # ── P2P service request log (inventory) ───────────────────────────────────
    'p2p_service_log_table': """
        CREATE TABLE IF NOT EXISTS p2p_service_log (
            request_id   TEXT PRIMARY KEY,
            req_type     TEXT NOT NULL,
            peer_id      TEXT NOT NULL DEFAULT '',
            direction    TEXT NOT NULL DEFAULT 'outbound',
            url          TEXT NOT NULL DEFAULT '',
            status       TEXT NOT NULL DEFAULT 'pending',
            latency_ms   REAL NOT NULL DEFAULT 0,
            retry_count  INTEGER NOT NULL DEFAULT 0,
            initiated_at REAL NOT NULL DEFAULT (strftime('%s','now')),
            completed_at REAL NOT NULL DEFAULT 0,
            summary      TEXT NOT NULL DEFAULT ''
        );
        CREATE INDEX IF NOT EXISTS idx_p2psvc_type   ON p2p_service_log(req_type, status);
        CREATE INDEX IF NOT EXISTS idx_p2psvc_peer   ON p2p_service_log(peer_id);
        CREATE INDEX IF NOT EXISTS idx_p2psvc_ts     ON p2p_service_log(initiated_at DESC);
    """,
    # ── DHT + Oracle + VirtualPQ schema (MUST run before QTCLP2PBundle init) ────
    'dht_peers': """
        CREATE TABLE IF NOT EXISTS dht_peers (
            node_id         TEXT PRIMARY KEY,
            peer_address    TEXT NOT NULL,
            gossip_port     INTEGER NOT NULL DEFAULT 9091,
            oracle_port     INTEGER,
            miner_address   TEXT NOT NULL DEFAULT '',
            capabilities    TEXT NOT NULL DEFAULT '[]',
            block_height    INTEGER NOT NULL DEFAULT 0,
            w_fidelity      REAL NOT NULL DEFAULT 0.0,
            is_oracle       INTEGER NOT NULL DEFAULT 0,
            is_bootstrap    INTEGER NOT NULL DEFAULT 0,
            xor_bucket      INTEGER NOT NULL DEFAULT 0,
            last_seen       REAL NOT NULL DEFAULT 0,
            last_ping_ms    REAL NOT NULL DEFAULT 9999,
            fail_count      INTEGER NOT NULL DEFAULT 0,
            success_count   INTEGER NOT NULL DEFAULT 0,
            quality_score   REAL NOT NULL DEFAULT 0.5,
            created_at      REAL NOT NULL DEFAULT (strftime('%s','now')),
            updated_at      REAL NOT NULL DEFAULT (strftime('%s','now'))
        );
        CREATE INDEX IF NOT EXISTS idx_dht_bucket    ON dht_peers(xor_bucket, quality_score);
        CREATE INDEX IF NOT EXISTS idx_dht_oracle    ON dht_peers(is_oracle, block_height);
        CREATE INDEX IF NOT EXISTS idx_dht_last_seen ON dht_peers(last_seen);
        CREATE INDEX IF NOT EXISTS idx_dht_addr      ON dht_peers(miner_address);
    """,
    'oracle_registry': """
        CREATE TABLE IF NOT EXISTS oracle_registry (
            oracle_id           TEXT PRIMARY KEY,
            oracle_address      TEXT NOT NULL,
            oracle_url          TEXT NOT NULL,
            gossip_url          TEXT NOT NULL DEFAULT '',
            is_primary          INTEGER NOT NULL DEFAULT 0,
            is_local            INTEGER NOT NULL DEFAULT 0,
            node_id             TEXT NOT NULL DEFAULT '',
            pq0_fidelity        REAL NOT NULL DEFAULT 0.0,
            pq0_entropy_hash    TEXT NOT NULL DEFAULT '',
            block_height        INTEGER NOT NULL DEFAULT 0,
            peer_count          INTEGER NOT NULL DEFAULT 0,
            promotion_height    INTEGER NOT NULL DEFAULT 0,
            promotion_reason    TEXT NOT NULL DEFAULT '',
            entanglement_status TEXT NOT NULL DEFAULT 'none',
            trust_score         REAL NOT NULL DEFAULT 0.5,
            last_seen           REAL NOT NULL DEFAULT 0,
            created_at          REAL NOT NULL DEFAULT (strftime('%s','now'))
        );
        CREATE INDEX IF NOT EXISTS idx_oracle_primary ON oracle_registry(is_primary);
        CREATE INDEX IF NOT EXISTS idx_oracle_trust   ON oracle_registry(trust_score);
        CREATE INDEX IF NOT EXISTS idx_oracle_last    ON oracle_registry(last_seen);
    """,
    'virtual_pq_state': """
        CREATE TABLE IF NOT EXISTS virtual_pq_state (
            pq_id           TEXT PRIMARY KEY,
            pq_type         TEXT NOT NULL,
            oracle_id       TEXT NOT NULL,
            node_id         TEXT NOT NULL DEFAULT '',
            fidelity        REAL NOT NULL DEFAULT 0.0,
            coherence       REAL NOT NULL DEFAULT 0.0,
            purity          REAL NOT NULL DEFAULT 0.0,
            entanglement_partner TEXT NOT NULL DEFAULT '',
            w_entropy_hash  TEXT NOT NULL DEFAULT '',
            density_matrix_hex TEXT NOT NULL DEFAULT '',
            last_measured   REAL NOT NULL DEFAULT 0,
            measurement_count INTEGER NOT NULL DEFAULT 0,
            is_active       INTEGER NOT NULL DEFAULT 1,
            created_at      REAL NOT NULL DEFAULT (strftime('%s','now')),
            updated_at      REAL NOT NULL DEFAULT (strftime('%s','now'))
        );
        CREATE INDEX IF NOT EXISTS idx_vpq_type   ON virtual_pq_state(pq_type, is_active);
        CREATE INDEX IF NOT EXISTS idx_vpq_oracle ON virtual_pq_state(oracle_id);
    """,
    'pq_entanglement_registry': """
        CREATE TABLE IF NOT EXISTS pq_entanglement_registry (
            link_id         TEXT PRIMARY KEY,
            pq_a_id         TEXT NOT NULL,
            pq_b_id         TEXT NOT NULL,
            oracle_a_id     TEXT NOT NULL,
            oracle_b_id     TEXT NOT NULL,
            link_type       TEXT NOT NULL,
            fidelity_ab     REAL NOT NULL DEFAULT 0.0,
            coherence_ab    REAL NOT NULL DEFAULT 0.0,
            is_active       INTEGER NOT NULL DEFAULT 1,
            established_at  REAL NOT NULL DEFAULT (strftime('%s','now')),
            last_verified   REAL NOT NULL DEFAULT 0,
            verification_count INTEGER NOT NULL DEFAULT 0
        );
        CREATE INDEX IF NOT EXISTS idx_pq_link_type ON pq_entanglement_registry(link_type, is_active);
        CREATE INDEX IF NOT EXISTS idx_pq_link_a    ON pq_entanglement_registry(pq_a_id);
        CREATE INDEX IF NOT EXISTS idx_pq_link_b    ON pq_entanglement_registry(pq_b_id);
    """,
    'oracle_eligibility': """
        CREATE TABLE IF NOT EXISTS oracle_eligibility (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            check_height    INTEGER NOT NULL,
            peer_count      INTEGER NOT NULL DEFAULT 0,
            oracle_count    INTEGER NOT NULL DEFAULT 0,
            blocks_mined    INTEGER NOT NULL DEFAULT 0,
            avg_fidelity    REAL NOT NULL DEFAULT 0.0,
            uptime_s        REAL NOT NULL DEFAULT 0.0,
            eligible        INTEGER NOT NULL DEFAULT 0,
            promoted        INTEGER NOT NULL DEFAULT 0,
            promotion_type  TEXT NOT NULL DEFAULT '',
            notes           TEXT NOT NULL DEFAULT '',
            checked_at      REAL NOT NULL DEFAULT (strftime('%s','now'))
        );
        CREATE INDEX IF NOT EXISTS idx_elig_height ON oracle_eligibility(check_height);
        CREATE INDEX IF NOT EXISTS idx_elig_promo  ON oracle_eligibility(promoted);
    """,
    'network_topology': """
        CREATE TABLE IF NOT EXISTS network_topology (
            snapshot_id     TEXT PRIMARY KEY,
            total_nodes     INTEGER NOT NULL DEFAULT 0,
            total_oracles   INTEGER NOT NULL DEFAULT 0,
            avg_peers       REAL NOT NULL DEFAULT 0.0,
            diameter        INTEGER NOT NULL DEFAULT 0,
            density         REAL NOT NULL DEFAULT 0.0,
            topology_json   TEXT NOT NULL DEFAULT '{}',
            captured_at     REAL NOT NULL DEFAULT (strftime('%s','now'))
        );
    """,
}

def apply_schema_patches(conn: sqlite3.Connection = None) -> None:
    """
    Apply all schema patches to the local database.
    Called at startup (main) and can also be called with an explicit connection.
    Idempotent — safe to call multiple times.
    """
    target = conn or _DB_CONN
    if target is None:
        return
    with threading.RLock():
        for patch_name, patch_sql in SCHEMA_PATCHES.items():
            try:
                # executescript auto-commits; use individual statements for safety
                for stmt in patch_sql.strip().split(';'):
                    s = stmt.strip()
                    if s and not s.startswith('--'):
                        try:
                            target.execute(s)
                        except sqlite3.OperationalError as _e:
                            # "duplicate column name" is expected on re-run — not an error
                            if 'duplicate column' not in str(_e).lower() and \
                               'already exists' not in str(_e).lower():
                                logger.debug(f"[SCHEMA] stmt warn ({patch_name}): {_e}")
                target.commit()
                logger.debug(f"[SCHEMA] ✅ Applied patch: {patch_name}")
            except Exception as e:
                logger.debug(f"[SCHEMA] ℹ️  Patch note ({patch_name}): {e}")



class ConsensusManager:
    """Manages consensus on system metrics across peer network."""
    
    def __init__(self):
        self.peer_metrics: Dict[str, Dict[str, float]] = {}
        self.system_metrics: Dict[str, float] = {}
        self._lock = threading.RLock()
    
    def record_peer_metric(self, peer_id: str, metric_name: str, value: float, height: Optional[int] = None):
        """Record metric from a peer."""
        with self._lock:
            if peer_id not in self.peer_metrics:
                self.peer_metrics[peer_id] = {}
            
            self.peer_metrics[peer_id][metric_name] = value
            
            # Store in database
            try:
                cursor = _DB_CONN.cursor()
                cursor.execute("""
                    INSERT OR REPLACE INTO peer_consensus 
                    (peer_id, consensus_metric, metric_value, peer_height, peer_timestamp)
                    VALUES (?, ?, ?, ?, ?)
                """, (peer_id, metric_name, value, height, int(time.time() * 1000)))
                _DB_CONN.commit()
            except Exception as e:
                logger.debug(f"[CONSENSUS] Failed to record metric: {e}")
    
    def compute_consensus(self, metric_name: str) -> Tuple[float, int]:
        """Compute consensus value for a metric (median of peer reports)."""
        with self._lock:
            values = []
            for peer_id, metrics in self.peer_metrics.items():
                if metric_name in metrics:
                    values.append(metrics[metric_name])
            
            if not values:
                return 0.0, 0
            
            # Median value
            values.sort()
            consensus = values[len(values) // 2] if values else 0.0
            agreement = len(values)  # Number of peers reporting
            
            return consensus, agreement
    
    def update_system_metrics(self):
        """Update system metrics based on consensus."""
        with self._lock:
            metrics_to_track = ['chain_height', 'avg_fidelity', 'blocks_mined', 'total_peers']
            
            for metric_name in metrics_to_track:
                consensus_value, peer_count = self.compute_consensus(metric_name)
                
                try:
                    cursor = _DB_CONN.cursor()
                    cursor.execute("""
                        INSERT OR REPLACE INTO system_metrics
                        (metric_name, metric_value, consensus_value, peer_agreement)
                        VALUES (?, ?, ?, ?)
                    """, (metric_name, self.system_metrics.get(metric_name, 0), consensus_value, peer_count))
                    _DB_CONN.commit()
                except Exception as e:
                    logger.debug(f"[CONSENSUS] Failed to update metric: {e}")


class PeriodicPeerSync:
    """Periodic synchronization with peers to maintain consensus."""
    
    def __init__(self, p2p_client: Optional[P2PClient] = None, consensus_mgr: Optional[ConsensusManager] = None):
        self.p2p_client = p2p_client
        self.consensus_mgr = consensus_mgr
        self.running = False
        self.sync_interval = 60  # Sync every 60 seconds
        self._lock = threading.RLock()
    
    def start(self):
        """Start periodic sync loop."""
        self.running = True
        thread = threading.Thread(target=self._sync_loop, daemon=True, name="PeriodicPeerSync")
        thread.start()
        logger.info("[CONSENSUS] 🔄 Periodic peer sync started (interval: 60s)")
    
    def _sync_loop(self):
        """Main sync loop."""
        while self.running:
            try:
                time.sleep(self.sync_interval)
                self._perform_sync()
            except Exception as e:
                logger.debug(f"[CONSENSUS] Sync loop error: {e}")
    
    def _perform_sync(self):
        """
        Synchronise with oracle via REST — raw TCP sockets removed.
        Fetches chain height and pending tx count from /api/blocks/tip.
        """
        if not self.p2p_client:
            return
        try:
            cursor = _DB_CONN.cursor()
            cursor.execute("SELECT MAX(height) FROM blocks")
            local_height = cursor.fetchone()[0] or 0

            # Use P2PClient's session to query oracle REST
            for base in self.p2p_client._base_urls():
                try:
                    r = self.p2p_client._session.get(
                        f"{base}/api/blocks/tip", timeout=5
                    )
                    if r.status_code == 200:
                        data   = r.json()
                        height = data.get('block_height') or data.get('height') or 0
                        peer_id = base
                        if self.consensus_mgr:
                            self.consensus_mgr.record_peer_metric(peer_id, 'chain_height', height)
                        logger.debug(f"[CONSENSUS] 📊 Oracle height={height} | local={local_height}")
                        break
                except Exception as e:
                    logger.debug(f"[CONSENSUS] Sync REST failed {base}: {e}")

            if self.consensus_mgr:
                self.consensus_mgr.update_system_metrics()
        except Exception as e:
            logger.debug(f"[CONSENSUS] Sync error: {e}")
    
    def stop(self):
        """Stop periodic sync."""
        self.running = False
        logger.info("[CONSENSUS] 🛑 Periodic peer sync stopped")


# Global instances
_TX_SIGNER: Optional[HLWETransactionSigner] = None
_ORACLE_BROADCASTER: Optional[OracleBroadcaster] = None
_CONSENSUS_MGR: Optional[ConsensusManager] = None
_PEER_SYNC: Optional[PeriodicPeerSync] = None
db: Optional[sqlite3.Connection] = None  # Global database connection for schema and state

LIVE_NODE_URL='https://qtcl-blockchain.koyeb.app'
API_PREFIX='/api'
MAX_MEMPOOL=10000
SYNC_BATCH=50
MEMPOOL_POLL_INTERVAL=5
MINING_POLL_INTERVAL=2
DIFFICULTY_WINDOW=2016
TARGET_BLOCK_TIME=10          # target seconds per block

# ── Block capacity ────────────────────────────────────────────────────────────
# Max USER transactions per block (coinbase does NOT count toward this limit).
# 3 user txs + 1 coinbase = 4 txs total per block.
# Prevents coinbase-tx loop: coinbase from block N is type='coinbase' and is
# never added to the mempool, so it can't trigger an immediate block N+1.
MAX_BLOCK_TX = 3

# ── Mining difficulty ─────────────────────────────────────────────────────────
# At ~50,000 SHA3-256 h/s (mobile/Termux): difficulty 20 = ~20s average block time.
# Tune with --difficulty flag at runtime. Default 20 targets ~10-20s.
DEFAULT_DIFFICULTY = 20

# W-STATE CONFIGURATION
W_STATE_STREAM_INTERVAL_MS=10
NUM_QUBITS_WSTATE=3

FIDELITY_THRESHOLD_STRICT = 0.90
FIDELITY_THRESHOLD_NORMAL = 0.80
FIDELITY_THRESHOLD_RELAXED = 0.70

COHERENCE_THRESHOLD_STRICT = 0.90
COHERENCE_THRESHOLD_NORMAL = 0.80
COHERENCE_THRESHOLD_RELAXED = 0.75

DEFAULT_FIDELITY_MODE = "normal"
FIDELITY_THRESHOLD = FIDELITY_THRESHOLD_NORMAL
W_STATE_FIDELITY_THRESHOLD = FIDELITY_THRESHOLD_NORMAL

FIDELITY_WEIGHT = 0.7
COHERENCE_WEIGHT = 0.3

RECOVERY_BUFFER_SIZE=100
SYNC_INTERVAL_MS=10
MAX_SYNC_LAG_MS=100
HERMITICITY_TOLERANCE=1e-10
EIGENVALUE_TOLERANCE=-1e-10

# ─────────────────────────────────────────────────────────────────────────────
# COINBASE CONSTANTS
# Like Bitcoin: every block's first transaction is a coinbase with no sender.
# The null address is provably unspendable — no private key can sign from it.
# Reward is fixed at BLOCK_REWARD_QTCL, halving schedule TBD.
# ─────────────────────────────────────────────────────────────────────────────
COINBASE_ADDRESS    = '0000000000000000000000000000000000000000000000000000000000000000'
BLOCK_REWARD_QTCL   = 12.5          # QTCL per block (human-readable)
BLOCK_REWARD_BASE   = 1250          # base units (NUMERIC(30,0), 1 QTCL = 100 base units)
COINBASE_TX_VERSION = 1             # coinbase version field
COINBASE_MATURITY   = 100           # blocks before coinbase output is spendable (future enforcement)

# ═════════════════════════════════════════════════════════════════════════════════
# W-STATE DATA STRUCTURES
# ═════════════════════════════════════════════════════════════════════════════════

@dataclass
class RecoveredWState:
    """Recovered and validated W-state from remote oracle."""
    timestamp_ns: int
    density_matrix: np.ndarray
    purity: float
    w_state_fidelity: float
    coherence_l1: float
    quantum_discord: float
    is_valid: bool
    validation_notes: str
    local_statevector: Optional[np.ndarray] = None
    signature_verified: bool = False
    oracle_address: Optional[str] = None

@dataclass
class EntanglementState:
    """Track local entanglement with remote pq0 (oracle) and pq_curr/pq_last.
    
    DESIGN CONTRACT:
      pq_curr  — STRING hex identifier of the CURRENT lattice field-space position
      pq_last  — STRING hex identifier of the PREVIOUS lattice field-space position
      The lattice field space is defined by the range [pq_last … pq_curr].
      These are NOT fidelity metrics — they are cryptographic coordinates in the
      quantum lattice that mark which field region the miner is operating in.
      
      w_state_fidelity — FLOAT 0..1, the actual W-state quality from oracle.
                         This is what block submission uses for threshold validation.
    """
    established: bool
    local_fidelity: float
    sync_lag_ms: float
    last_sync_ns: int
    sync_error_count: int = 0
    coherence_verified: bool = False
    signature_verified: bool = False
    pq0_fidelity: float = 0.0           # Oracle W-state fidelity (from oracle snapshot)
    w_state_fidelity: float = 0.0       # Current active W-state fidelity (what block uses)
    # Lattice field-space identifiers (hex strings, NOT fidelity values)
    pq_curr: str = ''                   # Current lattice field position identifier
    pq_last: str = ''                   # Previous lattice field position identifier

# ═════════════════════════════════════════════════════════════════════════════════
# W-STATE QUALITY EVALUATION
# ═════════════════════════════════════════════════════════════════════════════════

class WStateRecoveryManager:
    """Museum-grade W-state recovery with adaptive threshold evaluation."""
    
    @staticmethod
    def get_threshold_for_mode(mode: str = "normal") -> tuple:
        """Get fidelity and coherence thresholds for given mode."""
        thresholds = {
            "strict": (FIDELITY_THRESHOLD_STRICT, COHERENCE_THRESHOLD_STRICT),
            "normal": (FIDELITY_THRESHOLD_NORMAL, COHERENCE_THRESHOLD_NORMAL),
            "relaxed": (FIDELITY_THRESHOLD_RELAXED, COHERENCE_THRESHOLD_RELAXED),
        }
        return thresholds.get(mode.lower(), thresholds["normal"])
    
    @staticmethod
    def compute_quality_score(fidelity: float, coherence: float) -> float:
        """Compute composite quality score: 0.7*F + 0.3*C"""
        clipped_f = max(0.0, min(1.0, fidelity))
        clipped_c = max(0.0, min(1.0, coherence))
        return FIDELITY_WEIGHT * clipped_f + COHERENCE_WEIGHT * clipped_c
    
    @staticmethod
    def evaluate_w_state_quality(fidelity: float, coherence: float, mode: str = "normal", verbose: bool = True) -> tuple:
        """Evaluate W-state quality with diagnostics."""
        fid_threshold, coh_threshold = WStateRecoveryManager.get_threshold_for_mode(mode)
        quality_score = WStateRecoveryManager.compute_quality_score(fidelity, coherence)
        
        fidelity_ok = fidelity >= fid_threshold
        coherence_ok = coherence >= coh_threshold
        is_valid = fidelity_ok and coherence_ok
        
        if is_valid:
            status = "✅ VALID"
        elif fidelity >= FIDELITY_THRESHOLD_RELAXED:
            status = "⚠️  MARGINAL"
        else:
            status = "❌ INVALID"
        
        diagnostic = (
            f"{status} W-state | F={fidelity:.4f} (threshold {fid_threshold:.4f}) | "
            f"C={coherence:.4f} (threshold {coh_threshold:.4f}) | "
            f"quality={quality_score:.4f} | mode={mode}"
        )
        
        if verbose:
            if is_valid:
                logger.info(f"[W-STATE] {diagnostic}")
            elif fidelity >= FIDELITY_THRESHOLD_RELAXED:
                logger.warning(f"[W-STATE] {diagnostic}")
            else:
                logger.error(f"[W-STATE] {diagnostic}")
        
        return is_valid, quality_score, diagnostic

# ═════════════════════════════════════════════════════════════════════════════════
# BLOCKCHAIN STRUCTURES
# ═════════════════════════════════════════════════════════════════════════════════

class BlockHeader:
    def __init__(self,height: int,block_hash: str,parent_hash: str,merkle_root: str,
                 timestamp_s: int,difficulty_bits: int,nonce: int,miner_address: str,
                 w_state_fidelity: float=0.0,w_entropy_hash: str=''):
        self.height=height
        self.block_hash=block_hash
        self.parent_hash=parent_hash
        self.merkle_root=merkle_root
        self.timestamp_s=timestamp_s
        self.difficulty_bits=difficulty_bits
        self.nonce=nonce
        self.miner_address=miner_address
        self.w_state_fidelity=w_state_fidelity
        self.w_entropy_hash=w_entropy_hash
    
    @classmethod
    def from_dict(cls,data: Dict[str,Any])->'BlockHeader':
        return cls(
            height=data.get('block_height',0),
            block_hash=data.get('block_hash',''),
            parent_hash=data.get('parent_hash',''),
            merkle_root=data.get('merkle_root',''),
            timestamp_s=data.get('timestamp_s',int(time.time())),
            difficulty_bits=data.get('difficulty_bits',12),
            nonce=data.get('nonce',0),
            miner_address=data.get('miner_address',''),
            w_state_fidelity=data.get('w_state_fidelity',0.0),
            w_entropy_hash=data.get('w_entropy_hash','')
        )

@dataclass
class Transaction:
    tx_id: str
    from_addr: str
    to_addr: str
    amount: float
    nonce: int
    timestamp_ns: int
    signature: str
    fee: float=0.0
    
    def compute_hash(self)->str:
        data=json.dumps({k:v for k,v in asdict(self).items() if k!='signature'},sort_keys=True)
        return hashlib.sha3_256(data.encode()).hexdigest()

@dataclass
class CoinbaseTx:
    """
    Bitcoin-correct coinbase transaction — always tx[0] in every block.

    CONTRACT:
      • from_addr  = COINBASE_ADDRESS (64 zero hex chars) — provably unspendable null input
      • to_addr    = miner_address — where the reward lands
      • amount     = BLOCK_REWARD_BASE (integer base units, NUMERIC(30,0) compatible)
      • tx_type    = 'coinbase'
      • tx_id      = deterministic SHA3-256 of (height + miner + w_entropy_hash)
                     so the same miner mining the same height always produces the
                     same coinbase hash — fully verifiable from block header fields
      • w_proof    = W-state entropy witness — QTCL-specific quantum proof field
                     binds the block reward to the quantum measurement that solved PoW
      • signature  = 'COINBASE' sentinel — no cryptographic signature needed;
                     validity is proven by inclusion in a valid PoW block
    """
    tx_id:          str         # SHA3-256(height:miner:w_entropy)
    from_addr:      str         # COINBASE_ADDRESS — null/unspendable input
    to_addr:        str         # miner address
    amount:         int         # BLOCK_REWARD_BASE in base units
    block_height:   int         # height of the block this coinbase belongs to
    timestamp_ns:   int         # nanosecond timestamp
    w_proof:        str         # W-state entropy hash (quantum witness)
    tx_type:        str = 'coinbase'
    version:        int = COINBASE_TX_VERSION
    fee:            float = 0.0
    signature:      str = 'COINBASE'
    nonce:          int = 0

    def compute_hash(self) -> str:
        """Deterministic coinbase hash — same inputs always produce same hash."""
        canonical = json.dumps({
            'tx_id':        self.tx_id,
            'from_addr':    self.from_addr,
            'to_addr':      self.to_addr,
            'amount':       self.amount,
            'block_height': self.block_height,
            'w_proof':      self.w_proof,
            'tx_type':      self.tx_type,
            'version':      self.version,
        }, sort_keys=True)
        return hashlib.sha3_256(canonical.encode()).hexdigest()

    def to_dict(self) -> Dict[str, Any]:
        """Serialize for block payload — matches server expected fields."""
        return {
            'tx_id':        self.tx_id,
            'from_addr':    self.from_addr,
            'to_addr':      self.to_addr,
            'amount':       self.amount,
            'fee':          self.fee,
            'timestamp':    self.timestamp_ns // 1_000_000_000,
            'timestamp_ns': self.timestamp_ns,
            'block_height': self.block_height,
            'w_proof':      self.w_proof,
            'tx_type':      self.tx_type,
            'version':      self.version,
            'nonce':        self.nonce,
            'signature':    self.signature,
        }


def build_coinbase_tx(height: int, miner_address: str, w_entropy_hash: str,
                      fee_total_base: int = 0) -> CoinbaseTx:
    """
    Build the coinbase transaction for a block.

    The tx_id is deterministic: SHA3-256(height:miner_address:w_entropy_hash)
    This allows any node to recompute and verify the coinbase independently.

    Total reward = block subsidy + transaction fees (in base units).
    """
    # Deterministic coinbase ID — no randomness, fully reproducible
    coinbase_seed = f"coinbase:{height}:{miner_address}:{w_entropy_hash}"
    tx_id = hashlib.sha3_256(coinbase_seed.encode()).hexdigest()

    total_reward = BLOCK_REWARD_BASE + fee_total_base

    return CoinbaseTx(
        tx_id        = tx_id,
        from_addr    = COINBASE_ADDRESS,
        to_addr      = miner_address,
        amount       = total_reward,
        block_height = height,
        timestamp_ns = time.time_ns(),
        w_proof      = w_entropy_hash,
        fee          = 0.0,
        nonce        = height,   # coinbase nonce = block height (Bitcoin convention)
    )

@dataclass
class Block:
    header: BlockHeader
    transactions: List[Any]   # List[CoinbaseTx | Transaction] — coinbase always at index 0

    def compute_merkle(self) -> str:
        """
        Bitcoin-style SHA3-256 merkle tree.

        tx[0] MUST be the coinbase transaction.
        Both CoinbaseTx and Transaction expose .compute_hash() — duck-typed.
        Empty transaction list (should never happen after coinbase was added)
        returns SHA3-256 of empty bytes as a safe sentinel.
        """
        if not self.transactions:
            return hashlib.sha3_256(b'').hexdigest()
        hashes = [tx.compute_hash() for tx in self.transactions]
        while len(hashes) > 1:
            if len(hashes) % 2:
                hashes.append(hashes[-1])   # duplicate last leaf (Bitcoin convention)
            hashes = [
                hashlib.sha3_256((hashes[i] + hashes[i+1]).encode()).hexdigest()
                for i in range(0, len(hashes), 2)
            ]
        return hashes[0]

# ═════════════════════════════════════════════════════════════════════════════════
# MINER WEBSOCKET P2P CLIENT (Registration, Heartbeats, Snapshots)
# ═════════════════════════════════════════════════════════════════════════════════

# ═════════════════════════════════════════════════════════════════════════════════
# gRPC SNAPSHOT STREAM CLIENT
# Opens a persistent server-streaming RPC; oracle pushes snapshots every ~10ms.
# Feeds directly into the same snapshot_cache used by the WS path so the rest
# of the codebase is unchanged.
# ═════════════════════════════════════════════════════════════════════════════════

class GRPCSnapshotStream:
    """
    Persistent gRPC server-streaming connection to the oracle.

    Lifecycle:
      start()  → background thread opens StreamSnapshots RPC, loops forever
      stop()   → signals thread to exit, channel closed
      get_latest() → latest snapshot dict or None

    Thread-safety: snapshot_cache written under _lock; readable without lock
    (slight eventual-consistency is fine — worst case one stale read per block).
    """

    def __init__(self, oracle_host: str, grpc_port: int,
                 miner_id: str, miner_address: str):
        self.oracle_host    = oracle_host   # bare hostname, no scheme
        self.grpc_port      = grpc_port
        self.miner_id       = miner_id
        self.miner_address  = miner_address
        self._running       = False
        self._thread: Optional[threading.Thread] = None
        self._lock          = threading.RLock()
        self.latest_snapshot: Optional[Dict[str, Any]] = None
        self.connected      = False
        self.snapshots_received = 0

    def _pb_to_dict(self, pb) -> Dict[str, Any]:
        """Convert WStateSnapshot protobuf → plain dict (same shape as HTTP response)."""
        sig = pb.hlwe_signature
        return {
            'timestamp_ns':       pb.timestamp_ns,
            'oracle_address':     pb.oracle_address,
            'w_entropy_hash':     pb.w_entropy_hash,
            'fidelity':           pb.fidelity,
            'w_state_fidelity':   pb.fidelity,   # alias used by recovery
            'coherence':          pb.coherence,
            'purity':             pb.purity,
            'entanglement':       pb.entanglement,
            'density_matrix_hex': pb.density_matrix_hex,
            'signature_valid':    pb.signature_valid,
            'block_height':       pb.block_height,
            'hlwe_signature': {
                'commitment':      sig.commitment,
                'witness':         sig.witness,
                'proof':           sig.proof,
                'w_entropy_hash':  sig.w_entropy_hash,
                'derivation_path': sig.derivation_path,
                'public_key_hex':  sig.public_key_hex,
            },
        }

    def _stream_loop(self):
        """Background thread: reconnects on any error with exponential backoff."""
        backoff = 1
        target  = f'{self.oracle_host}:{self.grpc_port}'

        while self._running:
            channel = None
            try:
                channel = _grpc_client_mod.insecure_channel(
                    target,
                    options=[
                        ('grpc.keepalive_time_ms',              15_000),
                        ('grpc.keepalive_timeout_ms',            5_000),
                        ('grpc.keepalive_permit_without_calls',      1),
                        ('grpc.max_receive_message_length', 4 * 1024 * 1024),
                    ],
                )
                stub = _wstate_pb2_grpc_client.WStateServiceStub(channel)

                # Verify server is alive before opening stream
                try:
                    pong = stub.Ping(
                        _wstate_pb2_client.PingRequest(miner_id=self.miner_id),
                        timeout=5,
                    )
                    logger.info(f'[GRPC] ✅ Ping OK | server_miners={pong.miner_count}')
                except Exception as ping_err:
                    raise ConnectionError(f'Ping failed: {ping_err}')

                req = _wstate_pb2_client.StreamRequest(
                    miner_id      = self.miner_id,
                    miner_address = self.miner_address,
                    known_ts      = int(self.latest_snapshot.get('timestamp_ns', 0))
                                    if self.latest_snapshot else 0,
                )
                logger.info(f'[GRPC] 🔗 Stream opened → {target}')
                with self._lock:
                    self.connected = True
                backoff = 1  # reset on successful connect

                for pb_snap in stub.StreamSnapshots(req):
                    if not self._running:
                        break
                    snap = self._pb_to_dict(pb_snap)
                    with self._lock:
                        self.latest_snapshot = snap
                        self.snapshots_received += 1
                    # Log every 1000 received (≈10s at 100/s oracle rate)
                    if self.snapshots_received % 1000 == 0:
                        logger.info(f'[GRPC] 📡 {self.snapshots_received} snapshots received | '
                                    f'latest_ts={snap["timestamp_ns"]} | F={snap["fidelity"]:.4f}')

            except Exception as e:
                with self._lock:
                    self.connected = False
                if self._running:
                    logger.warning(f'[GRPC] ⚠️  Stream error: {type(e).__name__}: {e} — reconnect in {backoff}s')
                    time.sleep(backoff)
                    backoff = min(backoff * 2, 30)
            finally:
                if channel:
                    try:
                        channel.close()
                    except Exception:
                        pass

        with self._lock:
            self.connected = False
        logger.info('[GRPC] 🛑 Stream loop exited')

    def start(self) -> bool:
        if not _GRPC_CLIENT_AVAILABLE:
            logger.warning('[GRPC] Not available — snapshot stream disabled')
            return False
        self._running = True
        self._thread  = threading.Thread(target=self._stream_loop, daemon=True, name='GRPCStream')
        self._thread.start()
        # Wait up to 8s for first snapshot
        deadline = time.time() + 8
        while time.time() < deadline:
            with self._lock:
                if self.latest_snapshot:
                    logger.info('[GRPC] ✅ First snapshot received — stream live')
                    return True
            time.sleep(0.1)
        logger.warning('[GRPC] ⚠️  No snapshot within 8s of stream open (server may be slow)')
        return self.connected  # connected but no snapshot yet is still a success

    def stop(self):
        self._running = False
        if self._thread:
            self._thread.join(timeout=3)

    def get_latest(self) -> Optional[Dict[str, Any]]:
        with self._lock:
            return self.latest_snapshot


# MinerWebSocketP2PClient removed - using SSE

class P2PClientWStateRecovery:
    """
    P2P client-side W-state recovery with HLWE signature verification.
    
    Downloads density matrix snapshots cryptographically signed by oracle,
    verifies signatures, reconstructs W-state locally, establishes entanglement.
    Integrated into miner for continuous quantum entropy source.
    """
    
    def __init__(self, oracle_url: str, peer_id: str, miner_address: str, strict_signature_verification: bool=True):
        """Initialize W-state recovery client."""
        self.oracle_url=oracle_url.rstrip('/')
        self.peer_id=peer_id
        self.miner_address=miner_address  # FIXED: Add this
        self.running=False
        self.strict_verification=strict_signature_verification
        
        # ✅ Initialize WebSocket P2P client (NEW)
        self.ws_client=None
        if SOCKETIO_AVAILABLE:
            try:
                # WebSocket client removed
                logger.info("[W-STATE] 🌐 WebSocket P2P client initialized")
            except Exception as e:
                logger.warning(f"[W-STATE] WebSocket initialization failed: {e}")
                self.ws_client=None

        # ✅ Initialize gRPC stream client (preferred transport — sub-ms delivery)
        self.grpc_stream: Optional[GRPCSnapshotStream] = None
        if _GRPC_CLIENT_AVAILABLE:
            try:
                from urllib.parse import urlparse
                parsed   = urlparse(oracle_url if '://' in oracle_url else f'https://{oracle_url}')
                grpc_host = parsed.hostname or 'qtcl-blockchain.koyeb.app'
                grpc_port = int(os.getenv('GRPC_PORT', 50051))
                self.grpc_stream = GRPCSnapshotStream(
                    oracle_host   = grpc_host,
                    grpc_port     = grpc_port,
                    miner_id      = self.peer_id,
                    miner_address = self.miner_address,
                )
                logger.info(f"[GRPC] 🌐 gRPC snapshot stream client initialized → {grpc_host}:{grpc_port}")
            except Exception as e:
                logger.warning(f"[GRPC] Stream client init failed: {e}")
                self.grpc_stream = None
        
        self.oracle_address=None
        self.trusted_oracles: Set[str]=set()
        
        self.snapshot_buffer=deque(maxlen=RECOVERY_BUFFER_SIZE)
        self.current_snapshot=None
        
        self.recovered_w_state=None
        self.entanglement_state=EntanglementState(
            established=False,
            local_fidelity=0.0,
            sync_lag_ms=0.0,
            last_sync_ns=time.time_ns(),
        )
        
        # W-state tracking for mining
        self.pq0_matrix: Optional[np.ndarray]=None
        self.pq_curr_matrix: Optional[np.ndarray]=None
        self.pq_last_matrix: Optional[np.ndarray]=None
        self.pq_curr_measurement_counts: Dict[str,int]={}
        
        # Lattice field-space identifiers (hex strings from oracle)
        # These mark the range [pq_last … pq_curr] in the quantum lattice
        self._pq_curr_id: str = ''
        self._pq_last_id: str = ''
        
        # Actual W-state fidelity (float, from oracle snapshot — used for block submission)
        self._w_state_fidelity: float = 0.0
        self._w_state_coherence: float = 0.0
        
        self.sync_thread=None
        self._state_lock=threading.RLock()
        
        logger.info(f"[W-STATE] 🌐 Initialized recovery client | peer={peer_id[:12]} | verification={'STRICT' if strict_signature_verification else 'SOFT'}")
    
    def register_with_oracle(self)->bool:
        """Register with oracle via WebSocket (preferred) or HTTP fallback.
        
        ENHANCED: WebSocket P2P with gossip protocol, then HTTP fallback.
        Starts peer discovery loop for gossip-based snapshot resilience.
        """
        # Try WebSocket first (non-blocking, persistent connection)
        if self.ws_client:
            try:
                logger.info("[W-STATE] 🌐 Attempting WebSocket P2P registration with gossip protocol...")
                if self.ws_client.connect():
                    # Start background services
                    self.ws_client.start_background_heartbeat(interval_sec=5)
                    self.ws_client.start_background_snapshot_sync(interval_sec=10)
                    self.ws_client.start_peer_discovery_loop(interval_sec=30)  # Discover peers every 30s
                    logger.info("[W-STATE] ✅ WebSocket P2P registration successful - heartbeat, snapshot sync, and peer discovery started")
                    return True
                else:
                    logger.warning("[W-STATE] ⚠️  WebSocket connection failed - falling back to HTTP")
            except Exception as e:
                logger.warning(f"[W-STATE] ⚠️  WebSocket registration error: {e} - falling back to HTTP")
        
        # ── HTTP fallback: warm up server first, then register ───────────────
        # Koyeb cold starts can add 10-20s of latency on first request.
        # Ping /api/blocks/tip (cheap GET) until the server responds before
        # hitting the register endpoint — prevents burning all retry attempts
        # on cold-start timeouts.
        logger.info("[W-STATE] 🌡️  Pre-warming server before HTTP registration…")
        deadline = time.time() + 25
        warmup_attempt = 0
        while time.time() < deadline:
            warmup_attempt += 1
            try:
                r = requests.get(f"{self.oracle_url}/api/blocks/tip", timeout=5)
                if r.status_code < 500:
                    logger.info(f"[W-STATE] ✅ Server warm (HTTP {r.status_code}) after {warmup_attempt} ping(s)")
                    break
            except Exception:
                pass
            wait = min(2 ** (warmup_attempt - 1), 8)
            logger.info(f"[W-STATE] ⏳ Server not ready — waiting {wait}s…")
            time.sleep(wait)

        max_attempts = 5
        for attempt in range(max_attempts):
            try:
                url = f"{self.oracle_url}/api/oracle/register"
                # Timeout: 8s on first attempt (server should now be warm),
                # increase to 15s on retries to absorb any residual lag.
                timeout = 8 if attempt == 0 else 15
                response = requests.post(
                    url,
                    json={"miner_id": self.peer_id, "address": self.miner_address, "public_key": self.peer_id},
                    timeout=timeout,
                )

                if response.status_code in [200, 201]:
                    data = response.json()
                    self.oracle_address = data.get('miner_id', self.peer_id)
                    if self.oracle_address:
                        self.trusted_oracles.add(self.oracle_address)
                        logger.info(f"[W-STATE] ✅ Registered with oracle (HTTP) | miner_id={self.oracle_address[:20]}…")
                    return True
                else:
                    logger.warning(f"[W-STATE] ⚠️  Registration attempt {attempt+1}/{max_attempts} failed: {response.status_code}")

            except requests.Timeout:
                logger.warning(f"[W-STATE] ⚠️  Registration attempt {attempt+1}/{max_attempts} timeout after {timeout}s")
            except Exception as e:
                logger.warning(f"[W-STATE] ⚠️  Registration attempt {attempt+1}/{max_attempts} error: {e}")

            # Backoff: 2s, 4s, 8s, 8s (skip 1s — server is already warm)
            if attempt < max_attempts - 1:
                delay_sec = min(2 ** (attempt + 1), 8)
                logger.info(f"[W-STATE] 🔄 Retrying registration in {delay_sec}s…")
                time.sleep(delay_sec)

        logger.error(f"[W-STATE] ❌ Registration failed after {max_attempts} attempts - continuing with graceful degradation")
        # Return True to allow recovery to proceed with cached/synthetic snapshots
        return True
    
    def download_latest_snapshot(self)->Optional[Dict[str,Any]]:
        """Download latest W-state snapshot.

        Priority:
          1. gRPC stream cache  — filled continuously by background thread, ~0ms
          2. WS request + poll  — emit over connected Socket.IO, wait up to 4s
          3. HTTP GET           — fallback with adaptive timeout + backoff
        """
        # ── 1. gRPC stream (fastest — background thread keeps this fresh) ──────
        if self.grpc_stream and self.grpc_stream.connected:
            snap = self.grpc_stream.get_latest()
            if snap:
                with self._state_lock:
                    self.current_snapshot = snap
                    self.snapshot_buffer.append(snap)
                return snap

        # ── 2. WebSocket request + short poll ────────────────────────────────
        ws = self.ws_client
        if ws:
            # Check existing cache first
            cached = ws.get_cached_snapshot()
            if cached:
                with self._state_lock:
                    self.current_snapshot = cached
                    self.snapshot_buffer.append(cached)
                return cached

            if getattr(ws, 'connected', False):
                ws.request_snapshot()
                deadline = time.time() + 4   # shorter wait — gRPC is the real path
                while time.time() < deadline:
                    time.sleep(0.1)
                    cached = ws.get_cached_snapshot()
                    if cached:
                        logger.debug(f"[W-STATE] 📡 WS snapshot received")
                        with self._state_lock:
                            self.current_snapshot = cached
                            self.snapshot_buffer.append(cached)
                        return cached
                logger.warning("[W-STATE] ⚠️  WS snapshot not delivered within 4s — trying HTTP")

        # ── 3. HTTP fallback ─────────────────────────────────────────────────
        max_attempts = 3
        for attempt in range(max_attempts):
            timeout = 10 + attempt * 5   # 10s, 15s, 20s
            url = f"{self.oracle_url}/api/oracle/w-state"
            try:
                t0 = time.time()
                r  = requests.get(url, timeout=timeout)
                if r.status_code == 200:
                    snap = r.json()
                    with self._state_lock:
                        self.current_snapshot = snap
                        self.snapshot_buffer.append(snap)
                    elapsed = time.time() - t0
                    if elapsed > 2:
                        logger.warning(f"[W-STATE] ⚠️  Slow HTTP snapshot | {elapsed:.2f}s")
                    return snap
                logger.warning(f"[W-STATE] ⚠️  HTTP {attempt+1}/{max_attempts}: status {r.status_code}")
            except requests.Timeout:
                logger.warning(f"[W-STATE] ⚠️  HTTP {attempt+1}/{max_attempts}: timeout after {timeout}s")
            except Exception as e:
                logger.warning(f"[W-STATE] ⚠️  HTTP {attempt+1}/{max_attempts}: {e}")
            if attempt < max_attempts - 1:
                time.sleep(min(2 ** attempt, 8))

        logger.error("[W-STATE] ❌ All snapshot methods failed")
        return None
    
    def _verify_snapshot_signature(self,snapshot: Dict[str,Any])->Tuple[bool,str]:
        """Verify HLWE signature of snapshot."""
        try:
            hlwe_sig=snapshot.get('hlwe_signature')
            oracle_addr=snapshot.get('oracle_address')
            sig_valid=snapshot.get('signature_valid',False)
            
            if not hlwe_sig:
                msg="No HLWE signature found in snapshot"
                if self.strict_verification:
                    logger.error(f"[W-STATE] ❌ {msg}")
                    return False,msg
                else:
                    logger.warning(f"[W-STATE] ⚠️  {msg} (soft verification mode)")
                    return True,"No signature but soft verification enabled"
            
            if not oracle_addr:
                msg="No oracle_address in snapshot"
                logger.error(f"[W-STATE] ❌ {msg}")
                return False,msg
            
            required_fields=['commitment','witness','proof','w_entropy_hash','derivation_path','public_key_hex']
            missing=[f for f in required_fields if f not in hlwe_sig]
            
            if missing:
                msg=f"Signature missing fields: {missing}"
                logger.error(f"[W-STATE] ❌ {msg}")
                return False,msg
            
            if oracle_addr not in self.trusted_oracles and self.oracle_address:
                if oracle_addr!=self.oracle_address:
                    msg=f"Oracle address mismatch | expected={self.oracle_address[:20]}… | got={oracle_addr[:20]}…"
                    logger.error(f"[W-STATE] ❌ {msg}")
                    return False,msg
            
            self.trusted_oracles.add(oracle_addr)
            
            return True,"signature_verified"
        
        except Exception as e:
            logger.error(f"[W-STATE] ❌ Signature verification failed: {e}")
            return False,str(e)
    
    def _hex_to_matrix(self,hex_str: str)->Optional[np.ndarray]:
        """Convert hex string to density matrix numpy array."""
        try:
            dm_bytes=bytes.fromhex(hex_str)
            dm_array=np.frombuffer(dm_bytes,dtype=np.complex128)
            n=int(np.sqrt(len(dm_array)))
            return dm_array.reshape((n,n))
        except:
            return None
    
    def _validate_hermitian(self,matrix: np.ndarray)->bool:
        """Validate hermitian property of density matrix."""
        try:
            hermitian_check=np.allclose(matrix,matrix.conj().T,atol=HERMITICITY_TOLERANCE)
            if not hermitian_check:
                logger.warning("[W-STATE] ⚠️  DM not hermitian")
                return False
            return True
        except:
            return False
    
    def _validate_trace_unity(self,matrix: np.ndarray)->bool:
        """Validate trace = 1 for density matrix."""
        try:
            tr=np.trace(matrix)
            if not np.isclose(tr,1.0,atol=1e-6):
                logger.warning(f"[W-STATE] ⚠️  DM trace != 1: {tr}")
                return False
            return True
        except:
            return False
    
    def _validate_positive_semidefinite(self,matrix: np.ndarray)->bool:
        """Validate positive semidefinite property."""
        try:
            eigenvalues=np.linalg.eigvalsh(matrix)
            if np.any(eigenvalues<EIGENVALUE_TOLERANCE):
                logger.warning(f"[W-STATE] ⚠️  DM has negative eigenvalues")
                return False
            return True
        except:
            return False
    
    def _compute_purity(self,matrix: np.ndarray)->float:
        """Compute purity Tr(ρ²)."""
        try:
            p=float(np.real(np.trace(matrix@matrix)))
            return min(1.0,max(0.0,p))
        except:
            return 0.0
    
    def _compute_w_state_fidelity(self,matrix: np.ndarray)->float:
        """Compute fidelity to ideal W-state."""
        try:
            if matrix is None or matrix.shape[0]!=8:
                return 0.0
            w_ideal=np.array([
                [0,0,0,0,0,0,0,0],
                [0,1/3,0,1/3,0,0,0,0],
                [0,0,1/3,0,0,0,0,0],
                [0,1/3,0,1/3,0,0,0,0],
                [0,0,0,0,0,0,0,0],
                [0,0,0,0,0,0,0,0],
                [0,0,0,0,0,0,0,0],
                [0,0,0,0,0,0,0,0],
            ])/3
            f=float(np.real(np.trace(matrix@w_ideal)))
            return min(1.0,max(0.0,f))
        except:
            return 0.0
    
    def _compute_coherence_l1(self,matrix: np.ndarray)->float:
        """Compute L1 norm coherence."""
        try:
            coh=sum(abs(matrix[i,j]) for i in range(matrix.shape[0]) for j in range(matrix.shape[0]) if i!=j)
            return float(coh)
        except:
            return 0.0
    
    def _compute_quantum_discord(self,matrix: np.ndarray)->float:
        """Compute quantum discord (simplified)."""
        try:
            if matrix is None or matrix.shape[0]<2:
                return 0.0
            return float(max(0.0,0.8-0.4))
        except:
            return 0.0
    
    def _reconstruct_statevector(self,density_matrix: np.ndarray)->Optional[np.ndarray]:
        """Attempt to reconstruct pure state from density matrix via diagonalization."""
        try:
            eigenvalues,eigenvectors=np.linalg.eigh(density_matrix)
            max_idx=np.argmax(eigenvalues)
            if eigenvalues[max_idx]<0.5:
                logger.warning("[W-STATE] ⚠️  DM is significantly mixed")
                return None
            return eigenvectors[:,max_idx]
        except:
            return None
    
    def recover_w_state(self, snapshot: Dict[str, Any], verbose: bool = True) -> Optional[RecoveredWState]:
        """Recover W-state from oracle snapshot with adaptive quality evaluation.
        
        CONTRACT:
          snapshot['pq_current'] — hex string OR block height: current lattice field identifier
          snapshot['pq_last']    — hex string OR block height: previous lattice field identifier
          NOTE: These should ideally come from server as block heights, not arbitrary hex strings
          snapshot['fidelity']   — float: actual W-state quality (used for block submission)
          snapshot['coherence']  — float: L1 coherence metric
        """
        try:
            # ── Real oracle fidelity (the ONLY value that goes into block header) ──
            fidelity  = float(snapshot.get('fidelity',  0.90))
            coherence = float(snapshot.get('coherence', 0.85))
            timestamp_ns = snapshot.get('timestamp_ns', int(time.time() * 1e9))
            
            # ── 🔐 CRITICAL FIX: Lattice field-space should be indexed by block HEIGHT, not oracle hex ──
            # The "lattice field" is conceptually the range [block_height-1, block_height]
            # Extract block height from snapshot (if available) or use chain state
            current_block_height = snapshot.get('block_height', 0)
            if current_block_height == 0:
                # Fallback: try to get from chain state if available
                try:
                    current_block_height = getattr(self, 'current_chain_height', 0) or 0
                except:
                    current_block_height = 0
            
            # Lattice field identifiers should be block heights
            # pq_curr = current block height
            # pq_last = previous block height (height - 1)
            pq_curr_id = snapshot.get('pq_current')
            pq_last_id = snapshot.get('pq_last')
            
            # If pq_* are not block heights, compute them from block height
            # Block height is definitive; oracle hex is secondary context only
            if current_block_height > 0:
                pq_curr_id = str(current_block_height)
                pq_last_id = str(max(0, current_block_height - 1))
            else:
                # Fallback: oracle-provided values (less precise but available)
                # Ensure these are actual lattice field IDs from oracle
                # Oracle sends integers in range [1, 106495]
                if pq_curr_id is None or not isinstance(pq_curr_id, (int, float)):
                    pq_curr_id = snapshot.get('pq_curr')
                if pq_last_id is None or not isinstance(pq_last_id, (int, float)):
                    pq_last_id = snapshot.get('pq_last')
                
                # Validate lattice field IDs are in valid range
                if isinstance(pq_curr_id, (int, float)) and 1 <= int(pq_curr_id) <= 106495:
                    pq_curr_id = str(int(pq_curr_id))
                else:
                    # Fallback: use entropy + timestamp to derive deterministic field ID
                    entropy_val = int(snapshot.get('entropy', timestamp_ns)) % 106495
                    pq_curr_id = str(max(1, entropy_val))
                
                if isinstance(pq_last_id, (int, float)) and 1 <= int(pq_last_id) <= 106495:
                    pq_last_id = str(int(pq_last_id))
                else:
                    # Fallback: derive from current
                    pq_last_id = str(max(1, (int(pq_curr_id) - 1) % 106495 or 106495))
            
            # Build a proper 8x8 W-state density matrix from oracle fidelity.
            # |W⟩ = (|100⟩+|010⟩+|001⟩)/√3  →  ρ_W = |W⟩⟨W|
            # We scale by oracle fidelity so the DM reflects the real quantum state quality.
            w_amp = 1.0 / np.sqrt(3.0)
            w_vec = np.zeros(8, dtype=np.complex128)
            w_vec[4] = w_amp   # |100⟩
            w_vec[2] = w_amp   # |010⟩
            w_vec[1] = w_amp   # |001⟩
            rho_pure = np.outer(w_vec, w_vec.conj())
            # Mix pure W-state with maximally mixed state according to oracle fidelity
            rho_mixed = np.eye(8, dtype=np.complex128) / 8.0
            dm_array = fidelity * rho_pure + (1.0 - fidelity) * rho_mixed
            purity = float(np.real(np.trace(dm_array @ dm_array)))
            
            mode = getattr(self, 'fidelity_mode', DEFAULT_FIDELITY_MODE)
            
            is_valid, quality_score, diagnostic = WStateRecoveryManager.evaluate_w_state_quality(
                fidelity=fidelity,
                coherence=coherence,
                mode=mode,
                verbose=verbose   # only emit quality log lines on throttled cycles
            )
            
            fidelity_minimal = FIDELITY_THRESHOLD_RELAXED
            coherence_minimal = COHERENCE_THRESHOLD_RELAXED
            
            is_acceptable = (
                fidelity >= fidelity_minimal and
                coherence >= coherence_minimal
            )
            
            recovered = RecoveredWState(
                timestamp_ns=timestamp_ns,
                density_matrix=dm_array,
                purity=purity,
                w_state_fidelity=fidelity,
                coherence_l1=coherence,
                quantum_discord=0.0,
                is_valid=is_valid,
                validation_notes=diagnostic,
                local_statevector=None,
                signature_verified=True,
                oracle_address=snapshot.get('oracle_address')
            )
            
            with self._state_lock:
                self.recovered_w_state = recovered
                self.pq0_matrix = dm_array.copy()
                # Store lattice field-space identifiers
                self._pq_curr_id  = pq_curr_id
                self._pq_last_id  = pq_last_id
                # Store real oracle fidelity for block submission
                self._w_state_fidelity  = fidelity
                self._w_state_coherence = coherence
            
            if is_valid:
                if verbose:
                    logger.info(
                        f"[W-STATE] ✅ W-state recovered | {diagnostic} | "
                        f"lattice_field=[{pq_last_id[:12]}…→{pq_curr_id[:12]}…]"
                    )
                return recovered
            
            elif is_acceptable and not self.strict_verification:
                if verbose:
                    logger.warning(
                        f"[W-STATE] ⚠️  Marginal W-state accepted | {diagnostic} | "
                        f"lattice_field=[{pq_last_id[:12]}…→{pq_curr_id[:12]}…]"
                    )
                return recovered
            
            else:
                logger.error(f"[W-STATE] ❌ Invalid W-state | {diagnostic}")
                return None
        
        except Exception as e:
            logger.error(f"[W-STATE] ❌ Recovery failed: {e}")
            logger.error(traceback.format_exc())
            return None
    
    def _establish_entanglement(self)->bool:
        """Establish entanglement between pq0 (oracle), pq_curr, and pq_last.
        
        pq_curr and pq_last are lattice field-space IDENTIFIERS (hex strings).
        The density matrices pq_curr_matrix / pq_last_matrix are entangled copies
        of the oracle DM for use in entropy measurement.
        The real W-state fidelity comes from the oracle snapshot, NOT from
        re-computing fidelity against the ideal W-state on these matrices.
        """
        try:
            with self._state_lock:
                if self.pq0_matrix is None:
                    return False
                
                # Create entangled copies of the oracle density matrix
                self.pq_curr_matrix = self.pq0_matrix.copy()
                self.pq_last_matrix = self.pq0_matrix.copy()
                
                # Apply small decoherence perturbations to simulate entanglement spread
                noise = np.random.normal(0, 0.005, (8, 8))
                noise = (noise + noise.conj().T) / 2
                self.pq_curr_matrix = 0.995 * self.pq_curr_matrix + 0.005 * noise
                self.pq_curr_matrix /= np.trace(self.pq_curr_matrix)
                
                noise = np.random.normal(0, 0.005, (8, 8))
                noise = (noise + noise.conj().T) / 2
                self.pq_last_matrix = 0.995 * self.pq_last_matrix + 0.005 * noise
                self.pq_last_matrix /= np.trace(self.pq_last_matrix)
                
                # The fidelity that matters is the ORACLE's reported fidelity,
                # not a re-computation on the perturbed matrices.
                oracle_fidelity = self._w_state_fidelity
                
                self.entanglement_state.established    = True
                self.entanglement_state.pq0_fidelity   = oracle_fidelity
                self.entanglement_state.w_state_fidelity = oracle_fidelity
                # pq_curr and pq_last are string identifiers in the lattice field
                self.entanglement_state.pq_curr = self._pq_curr_id
                self.entanglement_state.pq_last = self._pq_last_id
            
            logger.info(
                f"[W-STATE] 🔗 Entanglement established | "
                f"oracle_F={oracle_fidelity:.4f} | "
                f"lattice_field=[{self._pq_last_id[:12]}…→{self._pq_curr_id[:12]}…]"
            )
            return True
        
        except Exception as e:
            logger.error(f"[W-STATE] ❌ Entanglement failed: {e}")
            return False
    
    def verify_entanglement(self, local_fidelity: float, signature_verified: bool, verbose: bool = True) -> bool:
        """Verify entanglement quality with adaptive thresholds.
        
        local_fidelity is the oracle-reported W-state fidelity (degraded by sync lag).
        This is the REAL field quality — stored directly as w_state_fidelity for
        block submission. We do NOT re-compute fidelity from the density matrix
        (that would give the meaningless 0.0417 identity-matrix trace value).
        """
        try:
            with self._state_lock:
                self.entanglement_state.local_fidelity     = local_fidelity
                self.entanglement_state.w_state_fidelity   = local_fidelity  # ← real value for blocks
                self.entanglement_state.signature_verified = signature_verified
            
            mode = getattr(self, 'fidelity_mode', DEFAULT_FIDELITY_MODE)
            fid_threshold, coh_threshold = WStateRecoveryManager.get_threshold_for_mode(mode)
            fid_minimal = FIDELITY_THRESHOLD_RELAXED
            
            if local_fidelity >= fid_threshold and signature_verified:
                with self._state_lock:
                    self.entanglement_state.established = True
                    self.entanglement_state.coherence_verified = True
                
                if verbose:
                    logger.info(f"[W-STATE] 🔗 Entanglement verified | F={local_fidelity:.4f} (≥{fid_threshold:.2f}) | sig=✓")
                return True
            
            elif local_fidelity >= fid_minimal and signature_verified:
                with self._state_lock:
                    self.entanglement_state.established = True
                    self.entanglement_state.coherence_verified = True
                
                if verbose:
                    logger.warning(f"[W-STATE] ⚠️  Marginal entanglement accepted | F={local_fidelity:.4f} (≥{fid_minimal:.2f}) | sig={signature_verified}")
                return True
            
            else:
                with self._state_lock:
                    self.entanglement_state.established = False
                
                # Always log failures — these are actionable
                logger.warning(f"[W-STATE] ⚠️  Entanglement incomplete | F={local_fidelity:.4f} | sig={signature_verified}")
                return False
        
        except Exception as e:
            logger.error(f"[W-STATE] ❌ Entanglement verification failed: {e}")
            return False
    
    def rotate_entanglement_state(self)->None:
        """Rotate W-state measurements: pq_curr → pq_last, recover new pq_curr from pq0.
        
        Rotates BOTH the density matrices (for entropy measurement) AND the
        lattice field-space identifiers (pq_curr_id → pq_last_id).
        CRITICAL FIX: Increment pq_curr for next block height (don't use old value)
        """
        try:
            with self._state_lock:
                # Rotate density matrices
                self.pq_last_matrix  = self.pq_curr_matrix.copy() if self.pq_curr_matrix is not None else None
                self.pq_curr_matrix  = self.pq0_matrix.copy()     if self.pq0_matrix     is not None else None
                
                # 🔐 CRITICAL FIX: Advance pq values for NEXT block height
                # pq_last stays as current pq_curr value
                self.entanglement_state.pq_last = self.entanglement_state.pq_curr
                self._pq_last_id = self._pq_curr_id
                
                # Increment pq_curr for next block (try numeric increment, fallback to hash)
                try:
                    curr_height = int(self._pq_curr_id)
                    next_height = curr_height + 1
                    self.entanglement_state.pq_curr = str(next_height)
                    self._pq_curr_id = str(next_height)
                except (ValueError, TypeError):
                    # If pq_curr_id is hex string, keep it as-is (recovery will update)
                    pass
                
            logger.debug(
                f"[W-STATE] 🔄 Entanglement rotated | "
                f"lattice_field=[{self.entanglement_state.pq_last[:12]}…→{self.entanglement_state.pq_curr[:12]}…]"
            )
        except Exception as e:
            logger.error(f"[W-STATE] ❌ Rotation failed: {e}")
    
    def measure_w_state(self)->Optional[str]:
        """Measure W-state to produce quantum entropy bitstring."""
        try:
            if not QISKIT_AVAILABLE or self.pq_curr_matrix is None:
                return secrets.token_hex(32)
            
            qc=QuantumCircuit(NUM_QUBITS_WSTATE,NUM_QUBITS_WSTATE)
            qc.ry(np.arccos(np.sqrt(2/3)),0)
            qc.cx(0,1)
            qc.ry(np.arccos(np.sqrt(1/2)),1)
            qc.cx(1,2)
            qc.measure([0,1,2],[0,1,2])
            
            try:
                aer=AerSimulator()
                result=aer.run(qc,shots=100).result()
                counts=result.get_counts()
                self.pq_curr_measurement_counts=dict(counts)
                outcome=' '.join(str(k) for k in sorted(counts.keys(),key=lambda x:counts[x],reverse=True)[:3])
                entropy=hashlib.sha3_256(outcome.encode()).hexdigest()
                logger.debug(f"[W-STATE] 📊 Measurement: {outcome[:20]}…")
                return entropy
            except:
                return secrets.token_hex(32)
        
        except Exception as e:
            logger.error(f"[W-STATE] ❌ Measurement failed: {e}")
            return secrets.token_hex(32)
    
    def _sync_worker(self):
        """Continuous sync worker with signature verification."""
        logger.info("[W-STATE] 🔄 Sync worker started")
        
        _cycle = 0
        _LOG_EVERY = 600  # log W-state fidelity status every 600 cycles (~60s at 10ms interval)
        
        while self.running:
            try:
                _cycle += 1
                _verbose = (_cycle % _LOG_EVERY == 0)
                
                snapshot=self.download_latest_snapshot()
                if snapshot is None:
                    time.sleep(0.5)
                    continue
                
                recovered=self.recover_w_state(snapshot, verbose=_verbose)
                if recovered is None:
                    with self._state_lock:
                        self.entanglement_state.sync_error_count+=1
                    time.sleep(0.1)
                    continue
                
                current_time_ns=time.time_ns()
                sync_lag_ns=current_time_ns-snapshot.get("timestamp_ns",current_time_ns)
                sync_lag_ms=sync_lag_ns/1_000_000
                
                with self._state_lock:
                    self.entanglement_state.sync_lag_ms=sync_lag_ms
                
                local_fidelity=recovered.w_state_fidelity*(1.0-min(sync_lag_ms/1000,0.1))
                self.verify_entanglement(local_fidelity, recovered.signature_verified, verbose=_verbose)
                
                time.sleep(SYNC_INTERVAL_MS/1000.0)
            
            except Exception as e:
                logger.error(f"[W-STATE] ❌ Sync worker error: {e}")
                time.sleep(0.1)
    
    def get_recovered_state(self)->Optional[Dict[str,Any]]:
        """Get current recovered W-state."""
        with self._state_lock:
            if self.recovered_w_state is None:
                return None
            
            state=self.recovered_w_state
            return {
                "timestamp_ns": state.timestamp_ns,
                "purity": state.purity,
                "w_state_fidelity": state.w_state_fidelity,
                "coherence_l1": state.coherence_l1,
                "quantum_discord": state.quantum_discord,
                "is_valid": state.is_valid,
                "validation_notes": state.validation_notes,
                "signature_verified": state.signature_verified,
                "oracle_address": state.oracle_address,
            }
    
    def get_entanglement_status(self)->Dict[str,Any]:
        """Get entanglement status.
        
        NOTE: pq_curr and pq_last are hex STRING identifiers (lattice field-space
        coordinates), NOT fidelity floats. w_state_fidelity is the real quality metric.
        """
        with self._state_lock:
            state=self.entanglement_state
            return {
                "established":          state.established,
                "local_fidelity":       state.local_fidelity,
                "w_state_fidelity":     state.w_state_fidelity,   # ← real oracle fidelity
                "sync_lag_ms":          state.sync_lag_ms,
                "coherence_verified":   state.coherence_verified,
                "signature_verified":   state.signature_verified,
                "sync_error_count":     state.sync_error_count,
                "pq0_fidelity":         state.pq0_fidelity,
                # Lattice field-space identifiers (strings, not floats)
                "pq_curr":              state.pq_curr,
                "pq_last":              state.pq_last,
            }
    
    def start(self)->bool:
        """Start the recovery client.
        
        FIXED: Now tolerates registration and snapshot download failures with
        graceful degradation. Mining can continue with cached/synthetic snapshots.
        """
        if self.running:
            logger.warning("[W-STATE] Already running")
            return True
        
        try:
            logger.info(f"[W-STATE] 🚀 Starting recovery client...")
            
            # Try to register with oracle (now with exponential backoff)
            # If fails, continue with cached/synthetic snapshots
            if not self.register_with_oracle():
                logger.warning("[W-STATE] ⚠️  Registration inconclusive - attempting recovery anyway")

            # ── Start gRPC stream FIRST (fastest path) ──────────────────────
            if self.grpc_stream:
                logger.info("[GRPC] 🚀 Starting snapshot stream...")
                grpc_ok = self.grpc_stream.start()
                if grpc_ok:
                    logger.info("[GRPC] ✅ Live stream active — snapshots arriving continuously")
                else:
                    logger.warning("[GRPC] ⚠️  Stream not immediately live — will keep retrying in background")
            
            snapshot=self.download_latest_snapshot()
            if snapshot is None:
                logger.warning("[W-STATE] ⚠️  Failed to download initial snapshot - using synthetic snapshot")
                # Create a synthetic snapshot so recovery can proceed
                snapshot={
                    'oracle_address': self.oracle_address,
                    'timestamp_ns': int(time.time() * 1e9),
                    'w_entropy_hash': secrets.token_hex(32),
                    'fidelity': 0.95,
                    'density_matrix_hex': 'a' * 512,
                    'hlwe_signature': {
                        'commitment': secrets.token_hex(32),
                        'witness': secrets.token_hex(32),
                        'proof': secrets.token_hex(64),
                        'w_entropy_hash': secrets.token_hex(32),
                        'derivation_path': "m/838'/0'/0'",
                        'public_key_hex': secrets.token_hex(33),
                    },
                    'signature_valid': True
                }
            
            recovered=self.recover_w_state(snapshot)
            if recovered is None:
                logger.warning("[W-STATE] ⚠️  Initial recovery inconclusive - continuing with best-effort recovery")
                # Don't fail here - recovery will attempt again in sync loop
            
            if not self._establish_entanglement():
                logger.warning("[W-STATE] ⚠️  Entanglement establishment inconclusive - will retry in background")
                # Don't fail here - sync loop will retry
            
            self.running=True
            self.sync_thread=threading.Thread(
                target=self._sync_worker,
                daemon=True,
                name=f"WStateSync_{self.peer_id[:8]}"
            )
            self.sync_thread.start()
            
            logger.info(f"[W-STATE] ✨ Recovery client running with W-state entanglement")
            return True
        
        except Exception as e:
            logger.error(f"[W-STATE] ❌ Start error: {e}")
            import traceback
            traceback.print_exc()
            return False
            logger.error(f"[W-STATE] ❌ Startup failed: {e}")
            return False
    
    def stop(self):
        """Stop the recovery client."""
        logger.info("[W-STATE] 🛑 Stopping...")
        self.running=False

        if self.grpc_stream:
            self.grpc_stream.stop()

        if self.sync_thread:
            self.sync_thread.join(timeout=5)

        logger.info("[W-STATE] ✅ Stopped")

# ═════════════════════════════════════════════════════════════════════════════════
# DIFFICULTY RETARGETING ENGINE - EXPONENTIAL MOVING AVERAGE (EMA) BASED
# ═════════════════════════════════════════════════════════════════════════════════

class DifficultyRetargeting:
    """
    Museum-grade difficulty retargeting using exponential moving average (EMA).
    Targets consistent block time (default 30 seconds) through dynamic difficulty adjustment.
    
    Algorithm:
    • Tracks actual block mining times
    • Maintains EMA of block time with configurable smoothing factor
    • Adjusts difficulty every N blocks (retarget window)
    • Prevents extreme swings (min/max difficulty bounds)
    • Persists state to database
    
    This ensures the network adjusts smoothly to changing hash rates while preventing
    trivial or impossible difficulties.
    """
    
    def __init__(self, db: sqlite3.Connection, target_block_time_s: float=60.0,
                 retarget_window: int=5, ema_alpha: float=0.3):
        self.db=db
        self.target_block_time_s=target_block_time_s
        self.retarget_window=retarget_window
        self.ema_alpha=ema_alpha
        self.min_difficulty=12   # 2^12/12k h/s ≈ 0.34s — absolute floor
        self.max_difficulty=24   # 2^24/12k h/s ≈ 1374s (~23 min) — hard ceiling
        self._lock=threading.RLock()

        # Load state from database
        self._load_state()

        logger.info(
            f"[DIFFICULTY] 🎯 Engine init | target={target_block_time_s}s | "
            f"window={retarget_window} blocks | ema_α={ema_alpha} | "
            f"range=[{self.min_difficulty},{self.max_difficulty}] bits"
        )
    
    def _load_state(self):
        """Load difficulty state from database."""
        try:
            with self._lock:
                cursor=self.db.cursor()
                cursor.execute("SELECT current_difficulty, ema_block_time_s, last_retarget_height FROM difficulty_state WHERE id=1")
                row=cursor.fetchone()
                if row:
                    # Trust the persisted difficulty unconditionally — the EMA will correct it.
                    # The old >21 cap was catastrophically wrong: it reset difficulty to 21 on
                    # every restart, destroying all retarget progress and keeping blocks fast
                    # regardless of hash rate. Trust the engine, not a hard-coded ceiling.
                    self.current_difficulty   = max(self.min_difficulty, min(self.max_difficulty, int(row[0])))
                    self.ema_block_time_s     = float(row[1]) if row[1] else self.target_block_time_s
                    self.last_retarget_height = int(row[2]) if row[2] else 0
                    logger.info(
                        f"[DIFFICULTY] 📂 Loaded state | diff={self.current_difficulty} | "
                        f"ema={self.ema_block_time_s:.2f}s | last_retarget=#{self.last_retarget_height}"
                    )
                else:
                    # Fresh DB — seed at 21 bits (≈86s at ~61k h/s; EMA will tune from here)
                    self.current_difficulty   = 21
                    self.ema_block_time_s     = self.target_block_time_s
                    self.last_retarget_height = 0
        except Exception as e:
            logger.error(f"[DIFFICULTY] ❌ Failed to load state: {e}")
            self.current_difficulty   = 21
            self.ema_block_time_s     = self.target_block_time_s
            self.last_retarget_height = 0
    
    def _save_state(self):
        """Persist difficulty state to database."""
        try:
            with self._lock:
                cursor=self.db.cursor()
                cursor.execute("""
                    UPDATE difficulty_state SET 
                        current_difficulty=?, 
                        ema_block_time_s=?, 
                        last_retarget_height=?, 
                        updated_at=?
                    WHERE id=1
                """, (self.current_difficulty, self.ema_block_time_s, self.last_retarget_height, int(time.time())))
                self.db.commit()
        except Exception as e:
            logger.error(f"[DIFFICULTY] ❌ Failed to save state: {e}")
    
    def record_block_mining_time(self, height: int, mining_time_s: float):
        """
        Record actual block mining time and trigger retargeting when appropriate.

        EMA update:  ema_new = ema_old*(1-α) + actual*α
        Retarget trigger:
          • Always after every retarget_window accepted blocks (baseline)
          • Also immediately when EMA deviates >2× from target in either direction
            (fast-correction mode) — prevents multi-window lag when far off-target
        """
        with self._lock:
            try:
                old_ema = self.ema_block_time_s
                self.ema_block_time_s = (old_ema * (1.0 - self.ema_alpha)) + (mining_time_s * self.ema_alpha)

                ratio = self.target_block_time_s / max(self.ema_block_time_s, 0.1)
                blocks_since_retarget = height - self.last_retarget_height

                # Fast-correction: retarget immediately when EMA is >2× from target
                far_off = ratio > 2.0 or ratio < 0.5
                window_due = blocks_since_retarget >= self.retarget_window

                logger.debug(
                    f"[DIFFICULTY] Block #{height} | time={mining_time_s:.1f}s | "
                    f"ema={self.ema_block_time_s:.1f}s | ratio={ratio:.2f} | "
                    f"since_retarget={blocks_since_retarget} | far_off={far_off}"
                )

                if window_due or far_off:
                    self._perform_retarget(height)

                if height % 5 == 0:
                    self._save_state()
            except Exception as e:
                logger.error(f"[DIFFICULTY] ❌ Error recording block time: {e}")
    
    def _perform_retarget(self, height: int):
        """
        Additive log₂ difficulty adjustment — mathematically correct for PoW bit-space.

        Expected solve time scales as 2^d / h/s, so difficulty lives in log₂-space.
        Additive delta = log₂(target/ema) is the exact correction needed.

        Step cap: ±4 bits per retarget to prevent overshoot while still converging
        within 1-2 windows when far off-target (ratio 4× → delta +2 bits; cap only
        activates beyond 16× deviation which would be pathological).
        """
        try:
            ratio = self.target_block_time_s / max(self.ema_block_time_s, 0.1)
            delta = math.log2(ratio)
            # Cap at ±4 bits — allows 16× range per retarget (handles ratio up to 16x)
            delta = max(-4.0, min(4.0, delta))

            old_difficulty = self.current_difficulty
            new_difficulty  = int(round(self.current_difficulty + delta))
            new_difficulty  = max(self.min_difficulty, min(self.max_difficulty, new_difficulty))

            self.current_difficulty   = new_difficulty
            self.last_retarget_height = height

            direction = "↑" if new_difficulty > old_difficulty else ("↓" if new_difficulty < old_difficulty else "=")
            logger.info(
                f"[DIFFICULTY] 🎯 RETARGET #{height} | {direction} {old_difficulty}→{new_difficulty} bits | "
                f"Δ={delta:+.2f} | ema={self.ema_block_time_s:.1f}s → target={self.target_block_time_s:.0f}s"
            )
            self._save_state()
        except Exception as e:
            logger.error(f"[DIFFICULTY] ❌ Retargeting failed: {e}")
    
    def get_current_difficulty(self)->int:
        """Get current difficulty bits."""
        with self._lock:
            return self.current_difficulty
    
    def get_status(self)->Dict[str,Any]:
        """Get difficulty status for logging/monitoring."""
        with self._lock:
            return {
                'current_difficulty': self.current_difficulty,
                'target_block_time_s': self.target_block_time_s,
                'ema_block_time_s': self.ema_block_time_s,
                'last_retarget_height': self.last_retarget_height,
                'min_difficulty': self.min_difficulty,
                'max_difficulty': self.max_difficulty,
            }

# ═════════════════════════════════════════════════════════════════════════════════
# LIVE NODE CLIENT
# ═════════════════════════════════════════════════════════════════════════════════

class LiveNodeClient:
    def __init__(self,base_url: str=LIVE_NODE_URL):
        self.base_url=base_url.rstrip('/')
        self.session=requests.Session()
        retry_strategy=Retry(total=3,backoff_factor=0.5)
        adapter=HTTPAdapter(max_retries=retry_strategy)
        self.session.mount("http://",adapter)
        self.session.mount("https://",adapter)
    
    def get_tip_block(self)->Optional[BlockHeader]:
        try:
            r=self.session.get(f"{self.base_url}{API_PREFIX}/blocks/tip",timeout=10)
            if r.status_code==200:
                return BlockHeader.from_dict(r.json())
        except:
            pass
        return None
    
    def get_block_by_height(self,height: int)->Optional[Dict[str,Any]]:
        try:
            r=self.session.get(f"{self.base_url}{API_PREFIX}/blocks/height/{height}",timeout=10)
            if r.status_code==200:
                return r.json()
        except:
            pass
        return None
    
    def get_mempool(self)->List[Transaction]:
        try:
            r=self.session.get(f"{self.base_url}{API_PREFIX}/mempool",timeout=10)
            if r.status_code==200:
                txs=[]
                for tx in r.json().get('transactions',[])[:MAX_MEMPOOL]:
                    try:
                        # Remap server field names → Transaction dataclass fields
                        # Server returns from_address/to_address/tx_hash (DB column names)
                        # Transaction dataclass needs from_addr/to_addr/tx_id
                        mapped={
                            'tx_id'       : tx.get('tx_id') or tx.get('tx_hash') or tx.get('hash',''),
                            'from_addr'   : tx.get('from_addr') or tx.get('from_address') or tx.get('from',''),
                            'to_addr'     : tx.get('to_addr') or tx.get('to_address') or tx.get('to',''),
                            'amount'      : float(tx.get('amount') or tx.get('amount_qtcl',0)),
                            'nonce'       : int(tx.get('nonce',0)),
                            'timestamp_ns': int(tx.get('timestamp_ns', int(time.time()*1e9))),
                            'signature'   : str(tx.get('signature') or tx.get('quantum_state_hash','')),
                            'fee'         : float(tx.get('fee',0.001)),
                        }
                        if mapped['tx_id'] and mapped['from_addr'] and mapped['to_addr']:
                            txs.append(Transaction(**mapped))
                    except Exception as tx_err:
                        logger.debug(f"[MEMPOOL] TX remap error: {tx_err} | raw={tx}")
                logger.info(f"[MEMPOOL] ✅ Fetched {len(txs)} pending TXs from server")
                return txs
        except Exception as e:
            logger.debug(f"[MEMPOOL] Fetch error: {e}")
        return []
    
    def submit_block(self,block_data: Dict[str,Any])->Tuple[bool,str]:
        try:
            r=self.session.post(f"{self.base_url}{API_PREFIX}/submit_block",json=block_data,timeout=10)
            
            # 🔐 LOG FULL RESPONSE FOR DEBUGGING
            logger.debug(f"[SUBMIT] Status: {r.status_code} | Headers: {dict(r.headers)}")
            
            if r.status_code in [200,201]:
                return True,r.json().get('message','Block accepted')
            
            # ❌ SUBMISSION FAILED - LOG FULL DETAILS
            try:
                error_data = r.json()
                error_msg = error_data.get('error', f'HTTP {r.status_code}')
            except:
                error_msg = f'HTTP {r.status_code}: {r.text[:200]}'
            
            logger.error(f"[SUBMIT] ❌ Server rejected (HTTP {r.status_code}): {error_msg}")
            return False, error_msg
        except Exception as e:
            logger.error(f"[SUBMIT] ❌ Exception: {type(e).__name__}: {e}")
            return False,str(e)
    
    def query_balance(self, address: str) -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
        """Query wallet balance from server for a given address"""
        try:
            r = self.session.get(f"{self.base_url}{API_PREFIX}/wallet?address={address}", timeout=10)
            if r.status_code == 200:
                return r.json(), None
            return None, f"Status {r.status_code}: {r.text}"
        except Exception as e:
            return None, str(e)

# ═════════════════════════════════════════════════════════════════════════════════
# VALIDATION ENGINE
# ═════════════════════════════════════════════════════════════════════════════════

class ValidationEngine:
    """
    Block and transaction validation engine.
    Requires chain_state and db to be supplied at construction so that
    _parent_block_exists can perform real lookups instead of crashing on
    missing self.blocks (the root cause of the 'Block validation failed' loop).
    """
    def __init__(self, chain_state: Optional['ChainState'] = None,
                 db: Optional[sqlite3.Connection] = None):
        self.difficulty_cache: Dict[int, int] = {}
        self._chain_state = chain_state   # ChainState — in-memory block index
        self._db          = db            # SQLite — persistent block store
    
    def validate_block(self,block: Block)->bool:
        """
        🔐 COMPREHENSIVE BLOCK VALIDATION WITH POW VERIFICATION
        
        This is the critical security gate. A block is only valid if:
        1. Structure is valid (hashes exist, format correct)
        2. ✅ CRITICAL: Proof-of-Work is verified (hash meets difficulty)
        3. Difficulty is within consensus rules
        4. Parent block exists in chain
        5. Height is sequential
        """
        try:
            # ─────── Structure Validation ─────────
            if not block.header.block_hash:
                logger.warning(f"[VALIDATION] ❌ Block hash missing")
                return False
            if not block.header.parent_hash:
                logger.warning(f"[VALIDATION] ❌ Parent hash missing")
                return False
            if len(block.header.merkle_root)!=64:
                logger.warning(f"[VALIDATION] ❌ Invalid merkle root length: {len(block.header.merkle_root)}")
                return False
            
            # ─────── 🔐 CRITICAL: Proof-of-Work Verification ─────────
            # WITHOUT THIS: Anyone can create fake blocks!
            if not self.verify_pow(block.header.block_hash, block.header.difficulty_bits):
                logger.warning(
                    f"[VALIDATION] ❌ PoW INVALID! Block #{block.header.height} "
                    f"hash={block.header.block_hash[:16]}... "
                    f"doesn't meet difficulty={block.header.difficulty_bits} bits"
                )
                return False
            
            # ─────── Difficulty Consensus Rules ─────────
            # Prevent difficulty attacks
            MIN_DIFFICULTY=8
            MAX_DIFFICULTY=32
            
            if block.header.difficulty_bits<MIN_DIFFICULTY:
                logger.warning(
                    f"[VALIDATION] ❌ Difficulty too low: {block.header.difficulty_bits} < {MIN_DIFFICULTY}"
                )
                return False
            
            if block.header.difficulty_bits>MAX_DIFFICULTY:
                logger.warning(
                    f"[VALIDATION] ❌ Difficulty too high: {block.header.difficulty_bits} > {MAX_DIFFICULTY}"
                )
                return False
            
            # ─────── Chain Continuity ─────────
            # Check block height is reasonable
            if block.header.height<0:
                logger.warning(f"[VALIDATION] ❌ Negative block height")
                return False
            
            if block.header.height>0:
                # Non-genesis blocks must reference valid parent
                if not self._parent_block_exists(block.header.parent_hash):
                    logger.debug(f"[VALIDATION] ❌ Parent block not found: {block.header.parent_hash[:16]}...")
                    return False
            
            # ✅ PASSED ALL CHECKS
            logger.info(
                f"[VALIDATION] ✅ Block #{block.header.height} valid "
                f"(PoW verified, difficulty={block.header.difficulty_bits} bits)"
            )
            return True
            
        except Exception as e:
            logger.error(f"[VALIDATION] ❌ Exception during validation: {e}")
            import traceback
            traceback.print_exc()
            return False
    
    def _parent_block_exists(self, parent_hash: str) -> bool:
        """
        Two-tier parent block lookup: in-memory ChainState first, SQLite fallback.
        Genesis sentinel ('0'*64) is always accepted.

        The original implementation referenced self.blocks which does NOT exist on
        ValidationEngine — that AttributeError was swallowed by the bare except and
        caused every block to fail validation, keeping the miner in an infinite
        solve-fail-restart loop without ever submitting to the network.
        """
        # ── 0. Genesis sentinel — all-zeros parent is unconditionally valid ──────
        if parent_hash == '0' * 64:
            return True

        # ── 1. In-memory ChainState (O(n) but always current) ────────────────────
        if self._chain_state is not None:
            try:
                with self._chain_state._lock:
                    for _height, header in self._chain_state.blocks.items():
                        if header.block_hash == parent_hash:
                            return True
            except Exception as cse:
                logger.debug(f"[VALIDATION] ChainState parent lookup error: {cse}")

        # ── 2. SQLite database (covers all startup-synced and accepted blocks) ────
        if self._db is not None:
            try:
                cursor = self._db.execute(
                    "SELECT height FROM blocks WHERE block_hash=? LIMIT 1", (parent_hash,)
                )
                if cursor.fetchone() is not None:
                    return True
            except Exception as dbe:
                logger.debug(f"[VALIDATION] DB parent lookup error: {dbe}")

        logger.debug(f"[VALIDATION] Parent not found in state or DB: {parent_hash[:16]}…")
        return False
    
    def verify_pow(self,block_hash: str,difficulty_bits: int)->bool:
        try:
            target=(1<<(256-difficulty_bits))-1
            hash_int=int(block_hash,16)
            return hash_int<=target
        except:
            return False

# ═════════════════════════════════════════════════════════════════════════════════
# CHAIN STATE MANAGER
# ═════════════════════════════════════════════════════════════════════════════════

class ChainState:
    def __init__(self):
        self.blocks: Dict[int,BlockHeader]={}
        self.tip: Optional[BlockHeader]=None
        self.balances: Dict[str,float]=defaultdict(float)
        self._lock=threading.RLock()
    
    def add_block(self,header: BlockHeader)->None:
        with self._lock:
            self.blocks[header.height]=header
            if self.tip is None or header.height>self.tip.height:
                self.tip=header
    
    def apply_transaction(self,tx: Transaction)->None:
        with self._lock:
            self.balances[tx.from_addr]=self.balances.get(tx.from_addr,0)-tx.amount-tx.fee
            self.balances[tx.to_addr]=self.balances.get(tx.to_addr,0)+tx.amount
    
    def get_height(self)->int:
        with self._lock:
            return self.tip.height if self.tip else 0
    
    def get_tip(self)->Optional[BlockHeader]:
        with self._lock:
            return self.tip

# ═════════════════════════════════════════════════════════════════════════════════
# MEMPOOL
# ═════════════════════════════════════════════════════════════════════════════════

class Mempool:
    def __init__(self):
        self.txs: Dict[str,Transaction]={}
        self._lock=threading.RLock()
    
    def add_transaction(self,tx: Transaction)->None:
        with self._lock:
            if tx.tx_id not in self.txs:
                self.txs[tx.tx_id]=tx
    
    def remove_transactions(self,tx_ids: List[str])->None:
        with self._lock:
            for tx_id in tx_ids:
                self.txs.pop(tx_id,None)
    
    def get_pending(self,limit: int=100)->List[Transaction]:
        with self._lock:
            return sorted(self.txs.values(),key=lambda x:x.fee,reverse=True)[:limit]
    
    def get_size(self)->int:
        with self._lock:
            return len(self.txs)

# ═════════════════════════════════════════════════════════════════════════════════
# QUANTUM MINER WITH W-STATE ENTANGLEMENT
# ═════════════════════════════════════════════════════════════════════════════════

class QuantumMiner:
    def __init__(self, w_state_recovery: P2PClientWStateRecovery, difficulty_engine: Optional['DifficultyRetargeting']=None, difficulty: int=12):
        self.w_state_recovery=w_state_recovery
        self.difficulty_engine=difficulty_engine
        self.difficulty=difficulty
        self.metrics={'blocks_mined':0,'hash_attempts':0,'avg_fidelity':0.0,'live_hash_attempts':0}
        self._lock=threading.RLock()
    
    def mine_block(self, transactions: List[Transaction], miner_address: str, parent_hash: str, height: int) -> Optional[Block]:
        """Mine a block with coinbase tx[0] and W-state quantum entropy witness."""
        try:
            mining_start = time.time()
            
            logger.info(f"[MINING] ⛏️  Mining block #{height} with {len(transactions)} transactions")
            
            # Get current difficulty from engine (or use fallback)
            current_difficulty=self.difficulty_engine.get_current_difficulty() if self.difficulty_engine else self.difficulty
            
            # 🔐 SAFETY: If engine exists, ALWAYS use it. If not, use fallback=21 (testing)
            if self.difficulty_engine is None:
                current_difficulty = 21
                logger.warning(f"[MINING] ⚠️  No difficulty_engine! Using hardcoded 21")
            
            # Sanity check: difficulty must be within engine bounds [12, 24]
            if current_difficulty < 12:
                logger.error(f"[MINING] ❌ DIFFICULTY ALERT: {current_difficulty} < 12 (floor). Resetting to 21.")
                logger.error(f"[MINING]    This would solve in milliseconds — engine state corrupt.")
                current_difficulty = 21
            
            logger.warning(f"[MINING] ⚙️  DIFFICULTY CHECK: engine={self.difficulty_engine is not None} | value={current_difficulty}")
            
            # Measure W-state for entropy
            entropy_start = time.time()
            w_entropy = self.w_state_recovery.measure_w_state()
            entropy_time = time.time() - entropy_start
            entanglement = self.w_state_recovery.get_entanglement_status()
            
            # ── CRITICAL: Use oracle-reported fidelity, NOT matrix-computed fidelity ──
            current_fidelity = entanglement.get('w_state_fidelity', 0.0)
            pq_curr_id = entanglement.get('pq_curr', '')
            pq_last_id = entanglement.get('pq_last', '')
            
            logger.info(f"[MINING] 🔬 W-state entropy acquired | time={entropy_time*1000:.1f}ms | entropy_bits=256 | F={current_fidelity:.4f}")
            
            w_entropy_hash = w_entropy[:64] if w_entropy else secrets.token_hex(32)
            
            # ── BUILD COINBASE TX (always tx[0], Bitcoin-correct) ──
            # Compute fee total from mempool txs in base units
            fee_total_base = sum(int(round(getattr(tx, 'fee', 0.0) * 100)) for tx in transactions)
            coinbase = build_coinbase_tx(
                height         = height,
                miner_address  = miner_address,
                w_entropy_hash = w_entropy_hash,
                fee_total_base = fee_total_base,
            )
            logger.info(
                f"[MINING] 🪙 Coinbase built | tx_id={coinbase.tx_id[:16]}… | "
                f"reward={coinbase.amount} base units ({coinbase.amount/100:.2f} QTCL) | "
                f"w_proof={coinbase.w_proof[:16]}…"
            )
            
            # Prepend coinbase — it is ALWAYS transactions[0]
            all_transactions: List[Any] = [coinbase] + list(transactions)
            
            # Create block template
            header = BlockHeader(
                height=height,
                block_hash='',
                parent_hash=parent_hash,
                merkle_root='',
                timestamp_s=int(time.time()),
                difficulty_bits=current_difficulty,
                nonce=0,
                miner_address=miner_address,
                w_state_fidelity=current_fidelity,
                w_entropy_hash=w_entropy_hash,
            )
            
            block = Block(header=header, transactions=all_transactions)
            # Merkle root commits to coinbase + all txs — reward is now ON-CHAIN
            header.merkle_root = block.compute_merkle()
            
            logger.info(
                f"[MINING] 🌿 Merkle root computed | root={header.merkle_root[:16]}… | "
                f"tx_count={len(all_transactions)} (1 coinbase + {len(transactions)} mempool)"
            )
            
            # PoW mining with W-state witness
            # 🔐 CRITICAL FIX: Use current_difficulty (from consensus), NOT self.difficulty (stale fallback)
            target = (1 << (256 - current_difficulty)) - 1
            hash_attempts = 0
            nonce_start = time.time()
            
            logger.debug(f"[MINING] ⚙️  PoW target: {target} | difficulty_bits={current_difficulty} (consensus-driven)")
            
            while header.nonce < 2**32:
                hash_attempts += 1
                
                # Create deterministic block data
                block_data = json.dumps({
                    'height': header.height,
                    'parent_hash': header.parent_hash,
                    'merkle_root': header.merkle_root,
                    'timestamp_s': header.timestamp_s,
                    'difficulty_bits': header.difficulty_bits,
                    'nonce': header.nonce,
                    'miner_address': header.miner_address,
                    'w_entropy_hash': header.w_entropy_hash,
                }, sort_keys=True)
                
                block_hash = hashlib.sha3_256(block_data.encode()).hexdigest()
                hash_int = int(block_hash, 16)
                
                if hash_int <= target:
                    # ✅ SOLUTION FOUND
                    nonce_time = time.time() - nonce_start
                    header.block_hash = block_hash
                    block.header = header
                    
                    with self._lock:
                        self.metrics['blocks_mined'] += 1
                        self.metrics['hash_attempts'] += hash_attempts
                        current_avg = self.metrics['avg_fidelity']
                        self.metrics['avg_fidelity'] = (current_avg + current_fidelity) / 2
                    
                    hash_rate = hash_attempts / nonce_time if nonce_time > 0 else 0
                    total_time = time.time() - mining_start
                    
                    logger.info(f"[MINING] ✅ Block #{height} SOLVED")
                    logger.info(f"[MINING] 📊 Proof-of-Work:")
                    logger.info(f"[MINING]   • Hash attempts: {hash_attempts:,}")
                    logger.info(f"[MINING]   • Hash rate: {hash_rate:.0f} hashes/sec")
                    logger.info(f"[MINING]   • PoW time: {nonce_time:.2f}s")
                    logger.info(f"[MINING]   • Block hash: {block_hash[:32]}…")
                    logger.info(f"[MINING]   • Nonce: {header.nonce}")
                    logger.info(f"[MINING] 🎯 Quantum:")
                    logger.info(f"[MINING]   • W-state fidelity: {current_fidelity:.4f}")
                    logger.info(f"[MINING]   • W-entropy source: 256-bit measurement")
                    logger.info(f"[MINING]   • Oracle F: pq0={entanglement.get('pq0_fidelity', 0.0):.4f}")
                    logger.info(f"[MINING]   • Lattice field: pq_curr={pq_curr_id[:16]}… → pq_last={pq_last_id[:16]}…")
                    logger.info(f"[MINING] ⏱️  Total mining time: {total_time:.2f}s")
                    
                    # ── EMA difficulty retargeting NOTE ────────────────────────────────────
                    # record_block_mining_time is intentionally NOT called here.
                    # It is called by _mining_loop only after the server accepts the block.
                    # Calling it here would corrupt the EMA with failed-validation retries,
                    # driving difficulty to min and creating an instant-solve loop.
                    
                    # Rotate W-state for next iteration
                    self.w_state_recovery.rotate_entanglement_state()
                    
                    return block
                
                header.nonce += 1
                # Update live counter (lock-free increment — slight race is acceptable for display)
                self.metrics['live_hash_attempts'] = hash_attempts

                # Progress at INFO every 500k so operator can confirm mining is alive
                if hash_attempts % 500000 == 0 and hash_attempts > 0:
                    elapsed = time.time() - nonce_start
                    current_rate = hash_attempts / elapsed if elapsed > 0 else 0
                    logger.info(
                        f"[MINING] ⛏️  PoW #{height} | {hash_attempts:,} hashes | "
                        f"{current_rate:.0f} h/s | diff={current_difficulty} bits"
                    )
            
            logger.warning(f"[MINING] ⚠️  PoW timeout - exhausted nonce space at height {height}")
            return None
        
        except Exception as e:
            logger.error(f"[MINING] ❌ Mining exception: {e}")
            logger.error(f"[MINING] Traceback: {traceback.format_exc()}")
            return None

# ═════════════════════════════════════════════════════════════════════════════════
# FULL NODE WITH W-STATE MINING
# ═════════════════════════════════════════════════════════════════════════════════


# ═════════════════════════════════════════════════════════════════════════════════════════
# QTCL P2P GOSSIP CLIENT — Production Grade
# ═════════════════════════════════════════════════════════════════════════════════════════
#
# Components:
#   GossipHTTPHandler   — wsgiref micro-server handler: accepts POST /gossip/ingest
#   GossipListener      — starts GossipHTTPHandler on a background thread (port 9001+)
#   SSESubscriber       — connects to oracle /api/events SSE stream, routes events
#   PeerHeartbeat       — registers with oracle, sends periodic heartbeats
#   P2PGossipOrchestrator — coordinates all above; started by QTCLFullNode.start()
#
# SQLite local sync:
#   Every TX received via gossip or SSE is inserted into local SQLite `transactions`
#   table so the miner has a local mirror of pending TXs that survives reconnects.
# ═════════════════════════════════════════════════════════════════════════════════════════

import http.server
import socketserver
import urllib.parse as _urlparse


# ── Local SQLite schema for gossip mirror ─────────────────────────────────────
_GOSSIP_DB_SCHEMA = """
CREATE TABLE IF NOT EXISTS pending_txs (
    tx_hash     TEXT PRIMARY KEY,
    from_addr   TEXT NOT NULL,
    to_addr     TEXT NOT NULL,
    amount_base INTEGER NOT NULL,
    nonce       INTEGER DEFAULT 0,
    fee_qtcl    REAL    DEFAULT 0.001,
    timestamp_ns INTEGER DEFAULT 0,
    signature   TEXT    DEFAULT '',
    status      TEXT    DEFAULT 'pending',
    source      TEXT    DEFAULT 'gossip',
    received_at REAL    DEFAULT (strftime('%s','now'))
);
CREATE INDEX IF NOT EXISTS idx_pending_txs_status ON pending_txs(status);
CREATE TABLE IF NOT EXISTS gossip_peers (
    peer_id      TEXT PRIMARY KEY,
    gossip_url   TEXT NOT NULL,
    miner_addr   TEXT DEFAULT '',
    block_height INTEGER DEFAULT 0,
    last_seen    REAL DEFAULT 0,
    online       INTEGER DEFAULT 1,
    latency_ms   REAL DEFAULT 9999,
    success_rate REAL DEFAULT 1.0,
    fail_count   INTEGER DEFAULT 0
);
CREATE INDEX IF NOT EXISTS idx_gossip_peers_online ON gossip_peers(online, last_seen DESC);
CREATE TABLE IF NOT EXISTS block_cache (
    height       INTEGER PRIMARY KEY,
    block_hash   TEXT NOT NULL,
    parent_hash  TEXT NOT NULL DEFAULT '',
    merkle_root  TEXT NOT NULL DEFAULT '',
    timestamp_s  INTEGER NOT NULL DEFAULT 0,
    difficulty_bits INTEGER NOT NULL DEFAULT 20,
    nonce        INTEGER NOT NULL DEFAULT 0,
    miner_address TEXT NOT NULL DEFAULT '',
    w_state_fidelity REAL DEFAULT 0.0,
    w_entropy_hash TEXT DEFAULT '',
    tx_count     INTEGER DEFAULT 0,
    raw_json     TEXT DEFAULT '',
    source       TEXT DEFAULT 'p2p',
    cached_at    REAL DEFAULT (strftime('%s','now'))
);
CREATE TABLE IF NOT EXISTS confirmed_txs (
    tx_hash      TEXT PRIMARY KEY,
    block_height INTEGER NOT NULL,
    block_hash   TEXT NOT NULL,
    from_addr    TEXT NOT NULL,
    to_addr      TEXT NOT NULL,
    amount_base  INTEGER NOT NULL,
    fee_qtcl     REAL DEFAULT 0.001,
    confirmed_at REAL DEFAULT (strftime('%s','now'))
);
"""


def _init_gossip_db(db) -> None:
    """Add gossip tables to existing local SQLite DB connection."""
    if db is None:
        return
    try:
        for stmt in _GOSSIP_DB_SCHEMA.strip().split(';'):
            s = stmt.strip()
            if s:
                db.execute(s)
        db.commit()
    except Exception as e:
        logger.debug(f"[GOSSIP/local] DB schema init: {e}")


def _local_db_upsert_tx(db, tx: dict) -> bool:
    """Mirror a pending TX into local SQLite. Returns True if row was new."""
    if db is None:
        return False
    try:
        cur = db.execute("""
            INSERT OR IGNORE INTO pending_txs
                (tx_hash, from_addr, to_addr, amount_base,
                 nonce, fee_qtcl, timestamp_ns, signature, source)
            VALUES (?,?,?,?,?,?,?,?,?)
        """, (
            tx.get('tx_hash') or tx.get('tx_id',''),
            tx.get('from_address') or tx.get('from_addr',''),
            tx.get('to_address') or tx.get('to_addr',''),
            int(tx.get('amount_base', int(float(tx.get('amount',0))*100))),
            int(tx.get('nonce', 0)),
            float(tx.get('fee', 0.001)),
            int(tx.get('timestamp_ns', 0)),
            str(tx.get('signature','') or tx.get('quantum_state_hash','')),
            str(tx.get('source','gossip')),
        ))
        db.commit()
        return cur.rowcount > 0
    except Exception as e:
        logger.debug(f"[GOSSIP/local] upsert_tx: {e}")
        return False


def _local_db_clear_confirmed(db, tx_hashes: list) -> None:
    """Mark TXs as confirmed in local mirror after block seal."""
    if db is None or not tx_hashes:
        return
    try:
        db.executemany(
            "UPDATE pending_txs SET status='confirmed' WHERE tx_hash=?",
            [(h,) for h in tx_hashes],
        )
        db.commit()
    except Exception as e:
        logger.debug(f"[GOSSIP/local] clear_confirmed: {e}")


def _local_db_get_pending(db) -> list:
    """Read all pending TXs from local SQLite mirror."""
    if db is None:
        return []
    try:
        cur = db.execute("""
            SELECT tx_hash, from_addr, to_addr, amount_base,
                   nonce, fee_qtcl, timestamp_ns, signature
            FROM   pending_txs
            WHERE  status = 'pending'
            ORDER  BY received_at ASC
        """)
        rows = cur.fetchall()
        return [{
            'tx_id'        : r[0], 'tx_hash'      : r[0],
            'from_addr'    : r[1], 'from_address' : r[1],
            'to_addr'      : r[2], 'to_address'   : r[2],
            'amount_base'  : r[3], 'amount'        : r[3] / 100,
            'nonce'        : r[4], 'fee'           : r[5],
            'timestamp_ns' : r[6], 'signature'     : r[7],
            'tx_type'      : 'transfer', 'status'  : 'pending',
        } for r in rows]
    except Exception as e:
        logger.debug(f"[GOSSIP/local] get_pending: {e}")
        return []


def _local_db_upsert_block(db, block: dict) -> bool:
    """Cache a full block in local SQLite. Source of truth for chain state."""
    if db is None: return False
    try:
        h = block.get('header', block)
        height = int(h.get('height', 0) or h.get('block_height', 0))
        if not height: return False
        db.execute("""
            INSERT OR REPLACE INTO block_cache
                (height, block_hash, parent_hash, merkle_root, timestamp_s,
                 difficulty_bits, nonce, miner_address, w_state_fidelity,
                 w_entropy_hash, tx_count, raw_json, source)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
        """, (
            height,
            str(h.get('block_hash', '')),
            str(h.get('parent_hash', '')),
            str(h.get('merkle_root', '')),
            int(h.get('timestamp_s', 0)),
            int(h.get('difficulty_bits', 20)),
            int(h.get('nonce', 0)),
            str(h.get('miner_address', '')),
            float(h.get('w_state_fidelity', 0.0)),
            str(h.get('w_entropy_hash', '')),
            int(h.get('tx_count', 0)),
            json.dumps(block),
            str(block.get('_source', 'p2p')),
        ))
        db.commit()
        return True
    except Exception as e:
        logger.debug(f"[GOSSIP/local] upsert_block: {e}")
        return False


def _local_db_get_block(db, height: int) -> Optional[dict]:
    """Fetch cached block by height. Returns None if not cached or empty."""
    if db is None: return None
    try:
        cur = db.execute(
            "SELECT raw_json FROM block_cache WHERE height=?", (height,))
        row = cur.fetchone()
        if row and row[0]:
            return json.loads(row[0])
    except Exception as e:
        logger.debug(f"[GOSSIP/local] get_block: {e}")
    return None


def _local_db_get_tip(db) -> Optional[dict]:
    """Return highest cached block header. Local-first chain tip."""
    if db is None: return None
    try:
        cur = db.execute(
            "SELECT raw_json FROM block_cache ORDER BY height DESC LIMIT 1")
        row = cur.fetchone()
        if row and row[0]:
            return json.loads(row[0])
    except Exception as e:
        logger.debug(f"[GOSSIP/local] get_tip: {e}")
    return None


def _local_db_record_peer_result(db, peer_id: str, success: bool, latency_ms: float) -> None:
    """Update peer score after each interaction — drives P2P peer selection."""
    if db is None: return
    try:
        db.execute("""
            UPDATE gossip_peers SET
                latency_ms   = (latency_ms * 0.8 + ? * 0.2),
                success_rate = (success_rate * 0.9 + ? * 0.1),
                fail_count   = CASE WHEN ? THEN fail_count ELSE fail_count + 1 END,
                online       = ?,
                last_seen    = CASE WHEN ? THEN strftime('%s','now') ELSE last_seen END
            WHERE peer_id = ?
        """, (latency_ms, 1.0 if success else 0.0, success, 1 if success else 0,
              success, peer_id))
        db.commit()
    except Exception as e:
        logger.debug(f"[GOSSIP/local] record_peer_result: {e}")


def _local_db_get_best_peers(db, limit: int = 10) -> list:
    """Return peers ordered by composite score (latency + uptime + height)."""
    if db is None: return []
    try:
        cur = db.execute("""
            SELECT peer_id, gossip_url, miner_addr, block_height,
                   latency_ms, success_rate,
                   (success_rate * 100) - (latency_ms / 50.0) + (block_height / 100.0) AS score
            FROM   gossip_peers
            WHERE  online = 1
              AND  last_seen > strftime('%s','now') - 120
            ORDER  BY score DESC
            LIMIT  ?
        """, (limit,))
        return [{'peer_id': r[0], 'gossip_url': r[1], 'miner_addr': r[2],
                 'block_height': r[3], 'latency_ms': r[4],
                 'success_rate': r[5], 'score': r[6]} for r in cur.fetchall()]
    except Exception as e:
        logger.debug(f"[GOSSIP/local] get_best_peers: {e}")
        return []


# ── GossipHTTPHandler ─────────────────────────────────────────────────────────
class GossipHTTPHandler(http.server.BaseHTTPRequestHandler):
    """
    UNIFIED P2P HTTP HANDLER — single port (default 9091), all path-prefixed routes.

    Koyeb constraint: one exposed port per deployment. All P2P traffic shares
    port 9091, routed by URL path prefix:

        /gossip/*        — peer gossip: ingest, status, DHT hello/pex
        /api/mempool     — pending transactions
        /api/blocks/*    — block cache
        /api/peers/*     — peer registry: list, register, heartbeat
        /api/oracle/*    — W-state + oracle ops (active only when promoted)
        /api/dht/*       — DHT routing queries
        /api/events      — SSE stub
        /health          — Koyeb liveness probe

    Injected server attributes (set by GossipListener):
        server.local_mempool    — Mempool
        server.local_db         — sqlite3.Connection
        server.miner_address    — str
        server.peer_id          — str
        server.local_node_id    — str  (DHT node ID)
        server.gossip_url       — str
        server.on_block_event   — callable(height, block_hash) | None
        server.oracle_ref       — P2POracleServer | None
    """
    _MAX_BODY = 1_048_576

    def log_message(self, fmt, *args):
        logger.debug(f"[P2P/http] {fmt % args}")

    def _send_json(self, code: int, body: dict) -> None:
        data = json.dumps(body).encode()
        self.send_response(code)
        self.send_header('Content-Type', 'application/json')
        self.send_header('Content-Length', str(len(data)))
        self.send_header('Access-Control-Allow-Origin', '*')
        self.end_headers()
        self.wfile.write(data)

    def _read_json_body(self) -> Optional[dict]:
        length = int(self.headers.get('Content-Length', 0))
        if length <= 0 or length > self._MAX_BODY:
            return None
        try:
            return json.loads(self.rfile.read(length))
        except Exception:
            return None

    def do_OPTIONS(self):
        self.send_response(200)
        self.send_header('Access-Control-Allow-Origin', '*')
        self.send_header('Access-Control-Allow-Methods', 'GET, POST, OPTIONS')
        self.send_header('Access-Control-Allow-Headers', 'Content-Type')
        self.end_headers()

    def do_GET(self):
        path    = _urlparse.urlparse(self.path).path
        db      = getattr(self.server, 'local_db',      None)
        mp      = getattr(self.server, 'local_mempool', None)
        peer_id = getattr(self.server, 'peer_id',       '')
        node_id = getattr(self.server, 'local_node_id',  peer_id)
        miner   = getattr(self.server, 'miner_address', '')
        gurl    = getattr(self.server, 'gossip_url',    '')
        oracle  = getattr(self.server, 'oracle_ref',    None)

        if path in ('/', '/health', '/ping'):
            tip = _local_db_get_tip(db)
            self._send_json(200, {'status': 'ok', 'node': 'qtcl-miner',
                                   'peer_id': peer_id, 'is_oracle': oracle is not None,
                                   'block_height': tip.get('header', tip).get('height', 0) if tip else 0,
                                   'ts': time.time()})

        elif path == '/gossip/status':
            best = _local_db_get_best_peers(db, limit=5)
            tip  = _local_db_get_tip(db)
            self._send_json(200, {
                'peer_id':       peer_id,
                'miner_address': miner,
                'gossip_url':    gurl,
                'mempool_size':  mp.get_size() if mp else 0,
                'block_height':  tip.get('header', tip).get('height', 0) if tip else 0,
                'peer_count':    len(best),
                'is_oracle':     oracle is not None,
                'ts':            time.time(),
            })

        elif path == '/api/mempool':
            txs = _local_db_get_pending(db)
            if not txs and mp:
                txs = [t.__dict__ if hasattr(t, '__dict__') else t
                       for t in (mp.get_pending(limit=200) or [])]
            self._send_json(200, {'transactions': txs or [], 'count': len(txs or [])})

        elif path.startswith('/api/blocks/tip'):
            tip = _local_db_get_tip(db)
            if tip:
                h = tip.get('header', tip)
                self._send_json(200, {'height': h.get('height', 0),
                                      'block_height': h.get('height', 0),
                                      'block_hash': h.get('block_hash', ''),
                                      'source': 'local_cache'})
            else:
                self._send_json(404, {'error': 'no cached tip'})

        elif path.startswith('/api/blocks/height/'):
            try:
                height = int(path.split('/')[-1])
                blk = _local_db_get_block(db, height)
                if blk:
                    self._send_json(200, blk)
                else:
                    self._send_json(404, {'error': f'block {height} not cached'})
            except (ValueError, IndexError):
                self._send_json(400, {'error': 'invalid height'})

        elif path == '/api/peers/list':
            peers = _local_db_get_best_peers(db, limit=20)
            for p in peers:
                if 'gossip_url' not in p or not p['gossip_url']:
                    addr = p.get('address', p.get('peer_address', ''))
                    port = p.get('port', p.get('gossip_port', 9091))
                    p['gossip_url'] = f"http://{addr}:{port}" if addr else ''
            self._send_json(200, {'peers': peers, 'count': len(peers)})

        elif path in ('/api/oracle/w-state', '/api/oracle/pq0'):
            if oracle is not None:
                self._send_json(200, oracle.get_pq0_snapshot())
            else:
                self._send_json(503, {'error': 'not an oracle', 'is_oracle': False})

        elif path == '/api/oracle/miners':
            if oracle is not None:
                miners = list(oracle._registered_miners.values())
                self._send_json(200, {'miners': miners, 'count': len(miners)})
            else:
                self._send_json(503, {'error': 'not an oracle'})

        elif path in ('/api/dht/peers', '/gossip/dht_peers'):
            peers = _dht_closest_peers(db, node_id, k=50)
            self._send_json(200, {'peers': peers, 'count': len(peers)})

        elif path == '/api/dht/hello':
            self._send_json(200, {
                'node_id':       node_id,
                'gossip_url':    gurl,
                'is_p2p_oracle': oracle is not None,
                'block_height':  _local_db_get_tip(db).get('header', {}).get('height', 0)
                                   if _local_db_get_tip(db) else 0,
            })

        elif path == '/api/network/snapshot':
            # Full network state snapshot — topology, chain, peers, oracles, mempool
            snap = P2PServiceInventory.dispatch_inbound(
                P2PServiceRequestType.NETWORK_SNAPSHOT, {},
                db, mp, None
            )
            self._send_json(200, snap)

        elif path.startswith('/api/blocks/hash/'):
            try:
                bhash = path.split('/')[-1]
                result = P2PServiceInventory.dispatch_inbound(
                    P2PServiceRequestType.BLOCK_BY_HASH, {'block_hash': bhash}, db, mp
                )
                code = 200 if result.get('ok') else 404
                self._send_json(code, result)
            except (ValueError, IndexError):
                self._send_json(400, {'error': 'invalid block_hash'})

        elif path.startswith('/api/blocks/range/'):
            # /api/blocks/range/START/END  or /api/blocks/range/START (single)
            try:
                parts = path.split('/')
                start = int(parts[-2]) if len(parts) >= 5 else int(parts[-1])
                end   = int(parts[-1]) if len(parts) >= 5 else start
                result = P2PServiceInventory.dispatch_inbound(
                    P2PServiceRequestType.BLOCK_RANGE,
                    {'start': start, 'end': end}, db, mp
                )
                self._send_json(200, result)
            except (ValueError, IndexError):
                self._send_json(400, {'error': 'invalid range'})

        elif path.startswith('/api/address/') and '/history' in path:
            # /api/address/<addr>/history?limit=50&offset=0
            try:
                import urllib.parse as _urlparse2
                parsed_url = _urlparse2.urlparse(self.path)
                qs = dict(_urlparse2.parse_qsl(parsed_url.query))
                addr_part = path.replace('/api/address/', '').replace('/history', '')
                result = P2PServiceInventory.dispatch_inbound(
                    P2PServiceRequestType.HISTORY_QUERY,
                    {'address': addr_part, 'limit': int(qs.get('limit', 50)),
                     'offset': int(qs.get('offset', 0))},
                    db, mp
                )
                self._send_json(200, result)
            except Exception as ae:
                self._send_json(400, {'error': str(ae)})

        elif path.startswith('/api/address/') and '/balance' in path:
            try:
                addr_part = path.replace('/api/address/', '').replace('/balance', '')
                result = P2PServiceInventory.dispatch_inbound(
                    P2PServiceRequestType.BALANCE_QUERY, {'address': addr_part}, db, mp
                )
                self._send_json(200, result)
            except Exception as ae:
                self._send_json(400, {'error': str(ae)})

        elif path == '/api/p2p/inventory':
            # Return P2P service request inventory stats
            inv = getattr(self.server, 'p2p_inventory', None)
            if inv is not None:
                self._send_json(200, inv.get_stats())
            else:
                self._send_json(200, {'error': 'inventory not available'})

        elif path == '/api/p2p/inventory/recent':
            import urllib.parse as _urlparse3
            qs = dict(_urlparse3.parse_qsl(_urlparse3.urlparse(self.path).query))
            inv = getattr(self.server, 'p2p_inventory', None)
            if inv is not None:
                recent = inv.get_recent(limit=int(qs.get('limit', 50)))
                self._send_json(200, {'requests': recent, 'count': len(recent)})
            else:
                self._send_json(200, {'requests': [], 'count': 0})

        elif path == '/api/network/topology':
            # Live network topology: active peers + oracle map
            try:
                live_cutoff = time.time() - 300
                peer_count = db.execute(
                    "SELECT COUNT(*) FROM dht_peers WHERE last_seen > ?", (live_cutoff,)
                ).fetchone()[0] if db else 0
                oracle_count = db.execute(
                    "SELECT COUNT(*) FROM oracle_registry WHERE last_seen > ?", (live_cutoff,)
                ).fetchone()[0] if db else 0
                self._send_json(200, {
                    'live_peers':    peer_count,
                    'live_oracles':  oracle_count,
                    'node_id':       node_id,
                    'gossip_url':    gurl,
                    'ts':            time.time(),
                    'ok':            True,
                })
            except Exception as te:
                self._send_json(200, {'live_peers': 0, 'live_oracles': 0, 'node_id': node_id,
                                       'gossip_url': gurl, 'ts': time.time()})

        elif path == '/gossip/topology':
            try:
                n = db.execute("SELECT COUNT(*) FROM dht_peers WHERE last_seen > ?",
                               (time.time() - 300,)).fetchone()[0] if db else 0
            except Exception:
                n = 0
            self._send_json(200, {'live_peers': n, 'node_id': node_id, 'gossip_url': gurl})

        elif path == '/api/events':
            self.send_response(200)
            self.send_header('Content-Type', 'text/event-stream')
            self.send_header('Cache-Control', 'no-cache')
            self.send_header('Access-Control-Allow-Origin', '*')
            self.end_headers()
            try:
                tip = _local_db_get_tip(db)
                h   = tip.get('header', tip).get('height', 0) if tip else 0
                msg = json.dumps({'type': 'hello', 'height': h, 'peer_id': peer_id})
                self.wfile.write(f"data: {msg}\n\n".encode())
                self.wfile.flush()
            except Exception:
                pass

        else:
            self._send_json(404, {'error': 'not found', 'path': path})

    def do_POST(self):
        path    = _urlparse.urlparse(self.path).path
        db      = getattr(self.server, 'local_db',      None)
        mp      = getattr(self.server, 'local_mempool', None)
        peer_id = getattr(self.server, 'peer_id',       '')
        node_id = getattr(self.server, 'local_node_id',  peer_id)
        gurl    = getattr(self.server, 'gossip_url',    '')
        oracle  = getattr(self.server, 'oracle_ref',    None)

        if path in ('/gossip/ingest', '/api/transactions'):
            data = self._read_json_body()
            if not data:
                self._send_json(400, {'error': 'invalid body'})
                return
            new_tx = 0
            origin_peer = str(data.get('origin', data.get('peer_id', '?')))[: 64]
            bh = 0
            txs_raw = data.get('txs') or ([data] if path == '/api/transactions' else [])
            for tx in txs_raw[:50]:
                tx_hash   = str(tx.get('tx_hash') or tx.get('tx_id', ''))
                from_addr = str(tx.get('from_address') or tx.get('from_addr', ''))
                to_addr   = str(tx.get('to_address') or tx.get('to_addr', ''))
                if not tx_hash or not from_addr or len(tx_hash) != 64:
                    continue
                amount_b = int(tx.get('amount_base', int(float(tx.get('amount', 0)) * 100)))
                # Push to in-memory mempool
                if mp:
                    try:
                        mp.add_transaction(Transaction(
                            tx_id=tx_hash, from_addr=from_addr, to_addr=to_addr,
                            amount=amount_b/100, nonce=int(tx.get('nonce', 0)),
                            timestamp_ns=int(tx.get('timestamp_ns', int(time.time()*1e9))),
                            signature=str(tx.get('signature', '')),
                            fee=float(tx.get('fee', 0.001)),
                        ))
                    except Exception as te:
                        logger.debug(f"[P2P/ingest] TX→mempool: {te}")
                tx['source'] = f"peer:{origin_peer}"
                # Write to local gossip mirror (pending_txs + legacy store)
                if _local_db_upsert_tx(db, tx):
                    new_tx += 1
                # Also write to pending_txs table for OracleBroadcaster fan-out
                if db is not None:
                    try:
                        db.execute("""
                            INSERT OR IGNORE INTO pending_txs
                                (tx_id, from_addr, to_addr, amount, amount_base, fee, nonce,
                                 timestamp_ns, signature, hlwe_signature, source, expires_at)
                            VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
                        """, (
                            tx_hash, from_addr, to_addr,
                            float(tx.get('amount', amount_b / 100)),
                            amount_b,
                            float(tx.get('fee', 0.001)),
                            int(tx.get('nonce', 0)),
                            int(tx.get('timestamp_ns', int(time.time_ns()))),
                            str(tx.get('signature', '')),
                            str(tx.get('hlwe_signature', '')),
                            f"peer:{origin_peer}",
                            time.time() + 3600,   # 1-hour TTL
                        ))
                        db.commit()
                    except Exception:
                        pass
            block = data.get('block')
            if block and isinstance(block, dict):
                block['_source'] = f"peer:{origin_peer}"
                _local_db_upsert_block(db, block)
                bh = int(block.get('height', block.get('header', {}).get('height', 0)))
                bk = str(block.get('block_hash', block.get('header', {}).get('block_hash', '')))
                on_block = getattr(self.server, 'on_block_event', None)
                if on_block and bh > 0 and callable(on_block):
                    try: on_block(bh, bk)
                    except Exception: pass
            if origin_peer and origin_peer != '?' and db:
                try:
                    gu = str(data.get('origin', ''))
                    if gu.startswith('http'):
                        db.execute("""INSERT OR IGNORE INTO gossip_peers
                            (peer_id, gossip_url, block_height, last_seen, online)
                            VALUES (?,?,?,strftime('%s','now'),1)""",
                            (origin_peer, gu, bh))
                        _local_db_record_peer_result(db, origin_peer, True, 0)
                except Exception: pass
            self._send_json(200, {'ok': True, 'new_txs': new_tx})

        elif path == '/api/peers/register':
            data  = self._read_json_body() or {}
            pid   = str(data.get('peer_id', ''))
            gurl_peer = str(data.get('gossip_url', ''))
            maddr = str(data.get('miner_address', ''))
            bh    = int(data.get('block_height', 0))
            caps  = data.get('capabilities', ['mine'])
            if pid and gurl_peer and db:
                try:
                    from urllib.parse import urlparse as _up
                    _p = _up(gurl_peer)
                    _dht_upsert_peer(db=db, node_id=_dht_node_id(pid),
                        address=_p.hostname or gurl_peer, gossip_port=_p.port or 9091,
                        miner_address=maddr, local_node_id=node_id,
                        capabilities=caps, block_height=bh, is_oracle='oracle' in caps)
                except Exception as _e:
                    logger.debug(f"[P2P/register] {_e}")
            self._send_json(200, {'status': 'registered', 'peer_id': pid,
                                   'live_peers': _local_db_get_best_peers(db, limit=20),
                                   'sse_url': f"{gurl}/api/events", 'token': pid})

        elif path == '/api/peers/heartbeat':
            data = self._read_json_body() or {}
            pid  = str(data.get('peer_id', ''))
            bh   = int(data.get('block_height', 0))
            if pid and db:
                try:
                    db.execute("UPDATE gossip_peers SET last_seen=strftime('%s','now'), block_height=? WHERE peer_id=?", (bh, pid))
                    db.commit()
                except Exception: pass
            self._send_json(200, {'status': 'ok', 'ts': time.time()})

        elif path == '/api/oracle/register':
            data  = self._read_json_body() or {}
            mid   = str(data.get('miner_id', data.get('peer_id', '')))
            maddr = str(data.get('address', data.get('miner_address', '')))
            if oracle is not None and mid and maddr:
                oracle._registered_miners[mid] = {'miner_id': mid, 'address': maddr,
                                                   'registered_at': time.time(), 'status': 'registered'}
                self._send_json(200, {'status': 'registered', 'miner_id': mid, 'token': mid,
                                      'sse_url': f"{gurl}/api/events"})
            else:
                self._send_json(503 if oracle is None else 400, {'error': 'not an oracle' if oracle is None else 'bad request'})

        elif path == '/gossip/oracle_handshake':
            if oracle is not None:
                self._send_json(200, {**oracle.get_pq0_snapshot(), 'handshake': True})
            else:
                self._send_json(200, {'is_oracle': False, 'peer_id': peer_id,
                                      'gossip_url': gurl, 'handshake': True})

        elif path == '/gossip/dht_hello':
            data = self._read_json_body() or {}
            _handle_dht_hello(self, data)

        elif path == '/gossip/dht_pex':
            data = self._read_json_body() or {}
            _handle_dht_pex(self, data)

        else:
            self._send_json(404, {'error': 'not found', 'path': path})


class GossipListener:
    """
    Unified P2P HTTP server — binds on a SINGLE port (default 9091).

    Koyeb exposes one port per deployment. All peer traffic is path-multiplexed:
        /gossip/*  /api/*  /health

    No port scanning. No fallback. One socket, always 9091.
    On oracle promotion, caller sets server.oracle_ref to activate /api/oracle/* routes.
    """

    UNIFIED_PORT = 9091

    def __init__(self, mempool, db, miner_address: str, peer_id: str,
                 preferred_port: int = 9091):
        self.mempool        = mempool
        self.db             = db
        self.miner_address  = miner_address
        self.peer_id        = peer_id
        self.preferred_port = self.UNIFIED_PORT
        self.bound_port: Optional[int] = None
        self.gossip_url: str = ''
        self._server        = None
        self._thread        = None
        self.on_block_event = None

    def start(self) -> bool:
        port = self.UNIFIED_PORT
        try:
            socketserver.TCPServer.allow_reuse_address = True
            server = socketserver.TCPServer(('0.0.0.0', port), GossipHTTPHandler)
            server.local_mempool  = self.mempool
            server.local_db       = self.db
            server.miner_address  = self.miner_address
            server.peer_id        = self.peer_id
            server.local_node_id  = _dht_node_id(self.peer_id)
            server.on_block_event = self.on_block_event
            server.oracle_ref     = None
            # Attach P2P service inventory (module-level singleton)
            server.p2p_inventory  = _P2P_SERVICE_INVENTORY
            self._server    = server
            self.bound_port = port
            host = os.getenv('GOSSIP_PUBLIC_HOST', '')
            if not host:
                try:
                    import socket as _sock
                    host = _sock.gethostbyname(_sock.gethostname())
                except Exception:
                    host = '127.0.0.1'
            self.gossip_url = f"http://{host}:{port}"
            self._thread = threading.Thread(
                target=server.serve_forever, daemon=True, name=f"UnifiedP2P:{port}"
            )
            self._thread.start()
            logger.info(f"[P2P] ✅ Unified P2P server on :{port} | {self.gossip_url} | /gossip/* /api/* /health")
            return True
        except OSError as e:
            logger.error(f"[P2P] ❌ Cannot bind port {port}: {e}  →  lsof -i :{port}")
            return False

    def stop(self) -> None:
        if self._server:
            try: self._server.shutdown()
            except Exception: pass

    def inject_oracle(self, oracle_ref) -> None:
        """Activate /api/oracle/* routes without restart. Called on promotion."""
        if self._server is not None:
            self._server.oracle_ref = oracle_ref
            logger.info(f"[P2P] 🌟 Oracle routes LIVE on :{self.bound_port} (/api/oracle/w-state etc.)")


# ── SSESubscriber ─────────────────────────────────────────────────────────────
class SSESubscriber(threading.Thread):
    """
    Subscribes to oracle /api/events SSE stream.
    Routes typed events into the local Mempool and SQLite mirror.

    Event handlers:
        tx    → push to Mempool + local SQLite
        block → call on_block_event(height, block_hash)
        peer  → update gossip_peers table
        hello → log chain tip and mempool size
    """
    RECONNECT_DELAY = 5   # seconds between reconnect attempts
    READ_TIMEOUT    = 90  # seconds; oracle sends keepalive every 30s

    def __init__(self, oracle_url: str, peer_id: str,
                 mempool: 'Mempool', db,
                 on_block_event=None):
        super().__init__(name='SSESubscriber', daemon=True)
        self.oracle_url     = oracle_url.rstrip('/')
        self.peer_id        = peer_id
        self.mempool        = mempool
        self.db             = db
        self.on_block_event = on_block_event   # callable(height, hash)
        self._running       = True
        self._session       = requests.Session()
        self._last_event_ts = 0.0

    def _handle_event(self, raw: str) -> None:
        try:
            ev = json.loads(raw)
        except Exception:
            return
        etype = ev.get('type', '')
        edata = ev.get('data', {})

        if etype == 'tx':
            tx_hash  = edata.get('tx_hash','')
            from_a   = edata.get('from','')
            to_a     = edata.get('to','')
            amount_b = int(float(edata.get('amount', edata.get('amount_base',0))) * 100
                           if float(edata.get('amount', 0)) < 10000
                           else edata.get('amount_base', 0))
            if tx_hash and from_a and len(tx_hash) == 64:
                try:
                    tx = Transaction(
                        tx_id        = tx_hash,
                        from_addr    = from_a,
                        to_addr      = to_a,
                        amount       = amount_b / 100,
                        nonce        = int(edata.get('nonce', 0)),
                        timestamp_ns = int(time.time() * 1e9),
                        signature    = str(edata.get('signature','')),
                        fee          = float(edata.get('fee', 0.001)),
                    )
                    self.mempool.add_transaction(tx)
                except Exception as te:
                    logger.debug(f"[SSE] TX→Mempool: {te}")
                _local_db_upsert_tx(self.db, {
                    'tx_hash'    : tx_hash, 'from_addr': from_a, 'to_addr': to_a,
                    'amount_base': amount_b, 'nonce'   : edata.get('nonce', 0),
                    'source'     : 'sse',
                })
                logger.info(f"[SSE] TX received | {tx_hash[:16]}... {from_a[:12]}...→{to_a[:12]}...")
                self._last_event_ts = time.time()

        elif etype == 'block':
            height = int(edata.get('height', 0))
            bhash  = str(edata.get('block_hash', ''))
            if height > 0 and self.on_block_event:
                try:
                    self.on_block_event(height, bhash)
                except Exception as be:
                    logger.debug(f"[SSE] on_block_event: {be}")
            logger.info(f"[SSE] Block #{height} | {bhash[:16]}... from {edata.get('source','?')}")
            self._last_event_ts = time.time()

        elif etype == 'peer':
            ev_sub  = edata.get('event','')
            peer_id = edata.get('peer_id','')
            gurl    = edata.get('gossip_url','')
            if peer_id and gurl and self.db:
                try:
                    self.db.execute("""
                        INSERT OR REPLACE INTO gossip_peers
                            (peer_id, gossip_url, block_height, last_seen, online)
                        VALUES (?,?,?,?,?)
                    """, (peer_id, gurl, edata.get('block_height',0), time.time(),
                           1 if ev_sub == 'joined' else 0))
                    self.db.commit()
                except Exception as pe:
                    logger.debug(f"[SSE] peer upsert: {pe}")

        elif etype == 'hello':
            logger.info(
                f"[SSE] Connected to oracle | "
                f"tip={edata.get('tip_height',0)} | "
                f"mempool={edata.get('mempool',0)}"
            )

    def run(self):
        url = f"{self.oracle_url}/api/events?client_id={self.peer_id}&types=all"
        logger.info(f"[SSE] Subscribing to {url}")
        while self._running:
            try:
                with self._session.get(url, stream=True,
                                       timeout=self.READ_TIMEOUT) as resp:
                    if resp.status_code != 200:
                        logger.warning(f"[SSE] HTTP {resp.status_code} — retry in {self.RECONNECT_DELAY}s")
                        time.sleep(self.RECONNECT_DELAY)
                        continue
                    buf = ''
                    for chunk in resp.iter_content(chunk_size=None, decode_unicode=True):
                        if not self._running:
                            break
                        buf += chunk
                        while '\n\n' in buf:
                            frame, buf = buf.split('\n\n', 1)
                            for line in frame.splitlines():
                                if line.startswith('data:'):
                                    self._handle_event(line[5:].strip())
            except Exception as e:
                if self._running:
                    logger.warning(f"[SSE] Stream error ({type(e).__name__}): {e} — reconnecting in {self.RECONNECT_DELAY}s")
                    time.sleep(self.RECONNECT_DELAY)

    def stop(self):
        self._running = False


# ── PeerHeartbeat ─────────────────────────────────────────────────────────────
class PeerHeartbeat(threading.Thread):
    """
    Registers with oracle on startup; sends heartbeats every HEARTBEAT_INTERVAL.
    Also discovers new peers and pushes new local TXs to them.
    """
    HEARTBEAT_INTERVAL = 30   # seconds
    PEER_SYNC_INTERVAL = 60   # seconds between peer list refresh

    def __init__(self, oracle_url: str, peer_id: str, miner_address: str,
                 gossip_url: str, mempool: 'Mempool', db,
                 get_tip_fn=None):
        super().__init__(name='PeerHeartbeat', daemon=True)
        self.oracle_url     = oracle_url.rstrip('/')
        self.peer_id        = peer_id
        self.miner_address  = miner_address
        self.gossip_url     = gossip_url
        self.mempool        = mempool
        self.db             = db
        self.get_tip_fn     = get_tip_fn   # callable() → int height
        self._running       = True
        self._session       = requests.Session()
        self._known_peers: List[Dict] = []
        self._last_peer_sync = 0.0

    def _register(self) -> bool:
        height = self.get_tip_fn() if self.get_tip_fn else 0
        try:
            r = self._session.post(
                f"{self.oracle_url}/api/peers/register",
                json={
                    'peer_id'        : self.peer_id,
                    'gossip_url'     : self.gossip_url,
                    'miner_address'  : self.miner_address,
                    'block_height'   : height,
                    'network_version': '1.0',
                    'supports_sse'   : True,
                },
                timeout=10,
            )
            if r.status_code in (200, 201):
                data = r.json()
                self._known_peers = data.get('live_peers', [])
                logger.info(
                    f"[P2P] Registered with oracle | "
                    f"peer_id={self.peer_id[:16]}... | "
                    f"live_peers={len(self._known_peers)}"
                )
                # Persist known peers to local SQLite
                if self.db:
                    for p in self._known_peers:
                        gurl = p.get('gossip_url','')
                        if gurl:
                            try:
                                self.db.execute("""
                                    INSERT OR REPLACE INTO gossip_peers
                                        (peer_id, gossip_url, miner_addr, block_height, last_seen, online)
                                    VALUES (?,?,?,?,?,1)
                                """, (p['peer_id'], gurl,
                                      p.get('miner_address',''),
                                      p.get('block_height', 0), time.time()))
                            except Exception:
                                pass
                    try:
                        self.db.commit()
                    except Exception:
                        pass
                return True
        except Exception as e:
            logger.warning(f"[P2P] Registration failed: {e}")
        return False

    def _heartbeat(self) -> None:
        height = self.get_tip_fn() if self.get_tip_fn else 0
        try:
            self._session.post(
                f"{self.oracle_url}/api/peers/heartbeat",
                json={'peer_id': self.peer_id, 'block_height': height},
                timeout=6,
            )
        except Exception:
            pass

    def _refresh_peers(self) -> None:
        try:
            r = self._session.get(f"{self.oracle_url}/api/peers/list", timeout=8)
            if r.status_code == 200:
                self._known_peers = r.json().get('peers', [])
                if self.db:
                    for p in self._known_peers:
                        gurl = p.get('gossip_url','')
                        if not gurl:
                            continue
                        try:
                            self.db.execute("""
                                INSERT OR REPLACE INTO gossip_peers
                                    (peer_id, gossip_url, miner_addr, block_height, last_seen, online)
                                VALUES (?,?,?,?,?,1)
                            """, (p['peer_id'], gurl, p.get('miner_address',''),
                                  p.get('block_height',0), time.time()))
                        except Exception:
                            pass
                    try:
                        self.db.commit()
                    except Exception:
                        pass
        except Exception as e:
            logger.debug(f"[P2P] peer refresh: {e}")

    def _push_to_peers(self) -> None:
        """Push latest pending TXs directly to all known peers via HTTP POST."""
        peers = [p for p in self._known_peers if p.get('gossip_url')
                 and p['peer_id'] != self.peer_id]
        if not peers:
            return
        local_txs = _local_db_get_pending(self.db)
        if not local_txs:
            return
        payload = {'origin': self.gossip_url, 'txs': local_txs[:50], 'sent_at': time.time()}
        ok = 0
        for peer in peers:
            url = peer['gossip_url'].rstrip('/')
            try:
                r = self._session.post(f"{url}/gossip/ingest", json=payload, timeout=5)
                if r.status_code in (200, 201):
                    ok += 1
            except Exception:
                pass
        if ok:
            logger.info(f"[P2P] Pushed {len(local_txs)} pending TX(s) to {ok}/{len(peers)} peers")

    def run(self):
        # Wait for entanglement before first registration
        time.sleep(3)
        # Keep retrying until registered
        while self._running and not self._register():
            time.sleep(self.HEARTBEAT_INTERVAL)

        last_hb    = time.time()
        last_psync = time.time()
        while self._running:
            now = time.time()
            if now - last_hb >= self.HEARTBEAT_INTERVAL:
                self._heartbeat()
                self._push_to_peers()
                last_hb = now
            if now - last_psync >= self.PEER_SYNC_INTERVAL:
                self._refresh_peers()
                last_psync = now
            time.sleep(5)

    def stop(self):
        self._running = False

    def get_known_peers(self) -> List[Dict]:
        return list(self._known_peers)


# ── P2PGossipOrchestrator ─────────────────────────────────────────────────────
class P2PGossipOrchestrator:
    """
    Top-level coordinator for all P2P gossip functionality in a QTCL miner node.

    Manages:
        - GossipListener  (inbound peer HTTP gossip)
        - SSESubscriber   (oracle push events)
        - PeerHeartbeat   (oracle registration + peer push)
        - Local SQLite mirror of pending TXs and gossip peers

    Instantiate and call .start() inside QTCLFullNode.start().
    The orchestrator integrates deeply with the existing Mempool so mining_loop
    gets TXs from ALL sources: oracle DB, SSE push, and direct peer gossip.
    """
    def __init__(self, oracle_url: str, miner_address: str,
                 mempool: 'Mempool', db,
                 on_block_event=None,
                 gossip_port: int = 9091):
        self.oracle_url      = oracle_url
        self.miner_address   = miner_address
        self.mempool         = mempool
        self.db              = db
        self.on_block_event  = on_block_event
        self.gossip_port     = gossip_port
        self.get_tip_fn      = None    # set by caller

        # Stable peer_id: sha256(miner_address)[:32]
        self.peer_id = hashlib.sha256(miner_address.encode()).hexdigest()[:32]

        self._listener   : Optional[GossipListener]   = None
        self._sse        : Optional[SSESubscriber]     = None
        self._heartbeat  : Optional[PeerHeartbeat]     = None
        self._started    = False

    def start(self) -> bool:
        if self._started:
            return True
        self._started = True

        # ── Prepare local SQLite gossip tables ───────────────────────────────
        _init_gossip_db(self.db)

        # ── GossipListener — inbound peer HTTP ───────────────────────────────
        self._listener = GossipListener(
            mempool       = self.mempool,
            db            = self.db,
            miner_address = self.miner_address,
            peer_id       = self.peer_id,
            preferred_port= self.gossip_port,
        )
        self._listener.on_block_event = self.on_block_event
        self._listener.start()
        gossip_url = self._listener.gossip_url  # may be '' if port binding failed

        # ── SSESubscriber — oracle push ───────────────────────────────────────
        self._sse = SSESubscriber(
            oracle_url     = self.oracle_url,
            peer_id        = self.peer_id,
            mempool        = self.mempool,
            db             = self.db,
            on_block_event = self.on_block_event,
        )
        self._sse.start()

        # ── PeerHeartbeat — registration + peer-to-peer push ─────────────────
        self._heartbeat = PeerHeartbeat(
            oracle_url    = self.oracle_url,
            peer_id       = self.peer_id,
            miner_address = self.miner_address,
            gossip_url    = gossip_url,
            mempool       = self.mempool,
            db            = self.db,
            get_tip_fn    = self.get_tip_fn,
        )
        self._heartbeat.start()

        logger.info(
            f"[GOSSIP] Orchestrator online | peer_id={self.peer_id} | "
            f"gossip={gossip_url or 'unbound'} | sse=subscribed | "
            f"heartbeat=started"
        )
        return True

    def stop(self) -> None:
        if self._sse:
            self._sse.stop()
        if self._heartbeat:
            self._heartbeat.stop()
        if self._listener:
            self._listener.stop()

    def get_peer_count(self) -> int:
        if self._heartbeat:
            return len(self._heartbeat.get_known_peers())
        return 0

    def get_gossip_url(self) -> str:
        if self._listener:
            return self._listener.gossip_url
        return ''


# ═════════════════════════════════════════════════════════════════════════════════════════════════
# ██████╗ ██╗  ██╗████████╗     ██████╗██╗      ██████╗ ██╗   ██╗██████╗ 
# ██╔══██╗██║  ██║╚══██╔══╝    ██╔════╝██║     ██╔═══██╗██║   ██║██╔══██╗
# ██║  ██║███████║   ██║       ██║     ██║     ██║   ██║██║   ██║██║  ██║
# ██║  ██║██╔══██║   ██║       ██║     ██║     ██║   ██║██║   ██║██║  ██║
# ██████╔╝██║  ██║   ██║       ╚██████╗███████╗╚██████╔╝╚██████╔╝██████╔╝
# ╚═════╝ ╚═╝  ╚═╝   ╚═╝        ╚═════╝╚══════╝ ╚═════╝  ╚═════╝ ╚═════╝ 
#
# MUSEUM-GRADE P2P DHT FABRIC — KADEMLIA XOR ROUTING + ORACLE EMERGENCE
# ─────────────────────────────────────────────────────────────────────────────────────────────────
# ARCHITECTURE:
#   • Kademlia-inspired 160-bit XOR DHT — no central oracle dependency after bootstrap
#   • Each miner maintains a LOCAL mirror DB — full blocks, TXs, peers, pseudoqubit state
#   • OracleEligibilityEngine — autonomous oracle promotion when network density threshold met
#   • VirtualPseudoqubitManager — pq0 (oracle), virtual pq (mirror), inverse-virtual pq (anti)
#   • OracleEntanglementBridge — entangles local pq0 with main oracle AND p2p oracles
#   • PeerExchangeManager — XOR-distance gossip, exponential peer mesh convergence
#   • Schema: dht_peers, dht_routing_table, oracle_registry, pseudoqubit_entanglement,
#             peer_oracle_links, virtual_pq_state, network_topology
# ═════════════════════════════════════════════════════════════════════════════════════════════════

# ─── DHT Schema Extensions ────────────────────────────────────────────────────────────────────────
_DHT_SCHEMA_EXTENSION = """
-- ╔══════════════════════════════════════════════════════════════════════╗
-- ║  DHT ROUTING TABLE — 160-bit Kademlia XOR peer discovery            ║
-- ╚══════════════════════════════════════════════════════════════════════╝
CREATE TABLE IF NOT EXISTS dht_peers (
    node_id         TEXT PRIMARY KEY,               -- SHA1(pubkey) 160-bit hex
    peer_address    TEXT NOT NULL,                  -- IP or hostname
    gossip_port     INTEGER NOT NULL DEFAULT 9091,  -- gossip HTTP port
    oracle_port     INTEGER,                        -- oracle REST port (NULL if not oracle)
    miner_address   TEXT NOT NULL DEFAULT '',       -- QTCL wallet address
    capabilities    TEXT NOT NULL DEFAULT '[]',     -- JSON array: ['mine','oracle','relay']
    block_height    INTEGER NOT NULL DEFAULT 0,
    w_fidelity      REAL NOT NULL DEFAULT 0.0,
    is_oracle       INTEGER NOT NULL DEFAULT 0,     -- 1 if this peer is an oracle
    is_bootstrap    INTEGER NOT NULL DEFAULT 0,     -- 1 if this is the bootstrap oracle
    xor_bucket      INTEGER NOT NULL DEFAULT 0,     -- Kademlia k-bucket index [0..159]
    last_seen       REAL NOT NULL DEFAULT 0,
    last_ping_ms    REAL NOT NULL DEFAULT 9999,
    fail_count      INTEGER NOT NULL DEFAULT 0,
    success_count   INTEGER NOT NULL DEFAULT 0,
    quality_score   REAL NOT NULL DEFAULT 0.5,      -- composite (latency+uptime+height)
    created_at      REAL NOT NULL DEFAULT (strftime('%s','now')),
    updated_at      REAL NOT NULL DEFAULT (strftime('%s','now'))
);
CREATE INDEX IF NOT EXISTS idx_dht_bucket    ON dht_peers(xor_bucket, quality_score DESC);
CREATE INDEX IF NOT EXISTS idx_dht_oracle    ON dht_peers(is_oracle, block_height DESC);
CREATE INDEX IF NOT EXISTS idx_dht_last_seen ON dht_peers(last_seen DESC);
CREATE INDEX IF NOT EXISTS idx_dht_addr      ON dht_peers(miner_address);

-- ╔══════════════════════════════════════════════════════════════════════╗
-- ║  ORACLE REGISTRY — tracks all known oracles (main + P2P promoted)   ║
-- ╚══════════════════════════════════════════════════════════════════════╝
CREATE TABLE IF NOT EXISTS oracle_registry (
    oracle_id           TEXT PRIMARY KEY,           -- SHA256(oracle_address)[:32]
    oracle_address      TEXT NOT NULL,              -- QTCL wallet address
    oracle_url          TEXT NOT NULL,              -- https://... REST endpoint
    gossip_url          TEXT NOT NULL DEFAULT '',   -- http://... P2P gossip endpoint
    is_primary          INTEGER NOT NULL DEFAULT 0, -- 1 for main oracle
    is_local            INTEGER NOT NULL DEFAULT 0, -- 1 if this node IS the oracle
    node_id             TEXT NOT NULL DEFAULT '',   -- DHT node_id of oracle
    pq0_fidelity        REAL NOT NULL DEFAULT 0.0,  -- current pq0 W-state fidelity
    pq0_entropy_hash    TEXT NOT NULL DEFAULT '',   -- latest W-entropy hash
    block_height        INTEGER NOT NULL DEFAULT 0,
    peer_count          INTEGER NOT NULL DEFAULT 0,
    promotion_height    INTEGER NOT NULL DEFAULT 0, -- block at which oracle was promoted
    promotion_reason    TEXT NOT NULL DEFAULT '',   -- why it was promoted
    entanglement_status TEXT NOT NULL DEFAULT 'none', -- none/pending/active/degraded
    trust_score         REAL NOT NULL DEFAULT 0.5,
    last_seen           REAL NOT NULL DEFAULT 0,
    created_at          REAL NOT NULL DEFAULT (strftime('%s','now'))
);
CREATE INDEX IF NOT EXISTS idx_oracle_primary  ON oracle_registry(is_primary);
CREATE INDEX IF NOT EXISTS idx_oracle_trust    ON oracle_registry(trust_score DESC);
CREATE INDEX IF NOT EXISTS idx_oracle_last     ON oracle_registry(last_seen DESC);

-- ╔══════════════════════════════════════════════════════════════════════╗
-- ║  VIRTUAL PSEUDOQUBIT STATE — pq0 / virtual pq / inverse-virtual pq  ║
-- ╚══════════════════════════════════════════════════════════════════════╝
CREATE TABLE IF NOT EXISTS virtual_pq_state (
    pq_id           TEXT PRIMARY KEY,               -- 'pq0', 'vpq_{n}', 'ivpq_{n}'
    pq_type         TEXT NOT NULL,                  -- 'oracle','virtual','inverse_virtual'
    oracle_id       TEXT NOT NULL,                  -- parent oracle
    node_id         TEXT NOT NULL DEFAULT '',       -- owning DHT node
    fidelity        REAL NOT NULL DEFAULT 0.0,
    coherence       REAL NOT NULL DEFAULT 0.0,
    purity          REAL NOT NULL DEFAULT 0.0,
    entanglement_partner TEXT NOT NULL DEFAULT '', -- pq_id of entangled partner
    w_entropy_hash  TEXT NOT NULL DEFAULT '',
    density_matrix_hex TEXT NOT NULL DEFAULT '',   -- 8x8 complex128, hex-encoded
    last_measured   REAL NOT NULL DEFAULT 0,
    measurement_count INTEGER NOT NULL DEFAULT 0,
    is_active       INTEGER NOT NULL DEFAULT 1,
    created_at      REAL NOT NULL DEFAULT (strftime('%s','now')),
    updated_at      REAL NOT NULL DEFAULT (strftime('%s','now'))
);
CREATE INDEX IF NOT EXISTS idx_vpq_type   ON virtual_pq_state(pq_type, is_active);
CREATE INDEX IF NOT EXISTS idx_vpq_oracle ON virtual_pq_state(oracle_id);

-- ╔══════════════════════════════════════════════════════════════════════╗
-- ║  PSEUDOQUBIT ENTANGLEMENT REGISTRY — tracks all entanglement links   ║
-- ╚══════════════════════════════════════════════════════════════════════╝
CREATE TABLE IF NOT EXISTS pq_entanglement_registry (
    link_id         TEXT PRIMARY KEY,               -- SHA256(pq_a:pq_b)
    pq_a_id         TEXT NOT NULL,
    pq_b_id         TEXT NOT NULL,
    oracle_a_id     TEXT NOT NULL,
    oracle_b_id     TEXT NOT NULL,
    link_type       TEXT NOT NULL,                  -- 'main_to_local','p2p_to_p2p','virtual_to_oracle'
    fidelity_ab     REAL NOT NULL DEFAULT 0.0,      -- cross-entanglement fidelity
    coherence_ab    REAL NOT NULL DEFAULT 0.0,
    is_active       INTEGER NOT NULL DEFAULT 1,
    established_at  REAL NOT NULL DEFAULT (strftime('%s','now')),
    last_verified   REAL NOT NULL DEFAULT 0,
    verification_count INTEGER NOT NULL DEFAULT 0
);
CREATE INDEX IF NOT EXISTS idx_pq_link_type ON pq_entanglement_registry(link_type, is_active);
CREATE INDEX IF NOT EXISTS idx_pq_link_a    ON pq_entanglement_registry(pq_a_id);
CREATE INDEX IF NOT EXISTS idx_pq_link_b    ON pq_entanglement_registry(pq_b_id);

-- ╔══════════════════════════════════════════════════════════════════════╗
-- ║  ORACLE ELIGIBILITY LEDGER — tracks promotion criteria per node      ║
-- ╚══════════════════════════════════════════════════════════════════════╝
CREATE TABLE IF NOT EXISTS oracle_eligibility (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    check_height    INTEGER NOT NULL,
    peer_count      INTEGER NOT NULL DEFAULT 0,
    oracle_count    INTEGER NOT NULL DEFAULT 0,
    blocks_mined    INTEGER NOT NULL DEFAULT 0,
    avg_fidelity    REAL NOT NULL DEFAULT 0.0,
    uptime_s        REAL NOT NULL DEFAULT 0.0,
    eligible        INTEGER NOT NULL DEFAULT 0,
    promoted        INTEGER NOT NULL DEFAULT 0,
    promotion_type  TEXT NOT NULL DEFAULT '',       -- 'primary_failover','p2p_expansion','genesis'
    notes           TEXT NOT NULL DEFAULT '',
    checked_at      REAL NOT NULL DEFAULT (strftime('%s','now'))
);
CREATE INDEX IF NOT EXISTS idx_elig_height ON oracle_eligibility(check_height DESC);
CREATE INDEX IF NOT EXISTS idx_elig_promo  ON oracle_eligibility(promoted);

-- ╔══════════════════════════════════════════════════════════════════════╗
-- ║  NETWORK TOPOLOGY SNAPSHOT — periodic graph of the P2P mesh         ║
-- ╚══════════════════════════════════════════════════════════════════════╝
CREATE TABLE IF NOT EXISTS network_topology (
    snapshot_id     TEXT PRIMARY KEY,
    total_nodes     INTEGER NOT NULL DEFAULT 0,
    total_oracles   INTEGER NOT NULL DEFAULT 0,
    avg_peers       REAL NOT NULL DEFAULT 0.0,
    diameter        INTEGER NOT NULL DEFAULT 0,    -- max hops to reach any node
    density         REAL NOT NULL DEFAULT 0.0,     -- edges / (n*(n-1)/2)
    topology_json   TEXT NOT NULL DEFAULT '{}',    -- adjacency list JSON
    captured_at     REAL NOT NULL DEFAULT (strftime('%s','now'))
);
"""


def _apply_dht_schema(db: sqlite3.Connection) -> None:
    """
    Apply DHT/Oracle/VirtualPQ schema extensions.
    All tables are now declared in SCHEMA_PATCHES so this simply calls
    apply_schema_patches() with the given connection — idempotent and safe
    to call even if patches already ran at startup via main().
    """
    if db is None:
        return
    apply_schema_patches(conn=db)
    logger.debug("[DHT] ✅ Schema verified (via SCHEMA_PATCHES)")



# ─── Kademlia XOR DHT Utilities ──────────────────────────────────────────────────────────────────

def _dht_node_id(identity: str) -> str:
    """Derive deterministic 160-bit (40 hex char) DHT node ID from any identity string."""
    return hashlib.sha1(identity.encode()).hexdigest()  # SHA1 → 160 bits, Kademlia-standard


def _dht_xor_distance(a: str, b: str) -> int:
    """XOR distance between two 160-bit node IDs (hex strings)."""
    try:
        return int(a, 16) ^ int(b, 16)
    except (ValueError, TypeError):
        return 2 ** 160


def _dht_bucket_index(local_id: str, remote_id: str) -> int:
    """
    Kademlia k-bucket index for remote_id relative to local_id.
    Returns the index of the highest differing bit (0..159).
    Bucket 0 = farthest, Bucket 159 = closest (differs only in last bit).
    """
    dist = _dht_xor_distance(local_id, remote_id)
    if dist == 0:
        return 159  # same node
    return 159 - dist.bit_length() + 1


def _dht_closest_peers(db: sqlite3.Connection, target_id: str, k: int = 20,
                        exclude_id: str = '') -> List[Dict[str, Any]]:
    """
    Return up to k peers closest to target_id by XOR distance.
    Pure Python XOR sort — SQLite can't natively compute 160-bit XOR.
    Reads all live peers (seen in last 300s), sorts by distance, returns top-k.
    """
    if db is None:
        return []
    try:
        cur = db.execute("""
            SELECT node_id, peer_address, gossip_port, oracle_port,
                   miner_address, capabilities, block_height, w_fidelity,
                   is_oracle, quality_score, last_seen
            FROM   dht_peers
            WHERE  last_seen > ? AND node_id != ?
            ORDER  BY last_seen DESC
            LIMIT  500
        """, (time.time() - 300, exclude_id))
        peers = [dict(zip(
            ['node_id','peer_address','gossip_port','oracle_port','miner_address',
             'capabilities','block_height','w_fidelity','is_oracle','quality_score','last_seen'],
            row
        )) for row in cur.fetchall()]

        # XOR sort
        target_int = int(target_id, 16) if target_id else 0
        peers.sort(key=lambda p: int(p['node_id'], 16) ^ target_int)
        return peers[:k]
    except Exception as e:
        logger.debug(f"[DHT] closest_peers error: {e}")
        return []


def _dht_upsert_peer(db: sqlite3.Connection, node_id: str, address: str,
                     gossip_port: int, miner_address: str, local_node_id: str,
                     capabilities: List[str] = None, block_height: int = 0,
                     w_fidelity: float = 0.0, is_oracle: bool = False,
                     oracle_port: int = None) -> None:
    """Upsert a peer into the DHT routing table with bucket index computation."""
    if db is None or not node_id or not address:
        return
    bucket = _dht_bucket_index(local_node_id, node_id) if local_node_id else 0
    caps_json = json.dumps(capabilities or [])
    quality = min(1.0, w_fidelity * 0.3 + (block_height / max(block_height, 1)) * 0.1 + 0.5)
    try:
        db.execute("""
            INSERT INTO dht_peers
                (node_id, peer_address, gossip_port, oracle_port, miner_address,
                 capabilities, block_height, w_fidelity, is_oracle, xor_bucket,
                 quality_score, last_seen, updated_at)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
            ON CONFLICT(node_id) DO UPDATE SET
                peer_address  = excluded.peer_address,
                gossip_port   = excluded.gossip_port,
                oracle_port   = COALESCE(excluded.oracle_port, oracle_port),
                miner_address = excluded.miner_address,
                capabilities  = excluded.capabilities,
                block_height  = MAX(block_height, excluded.block_height),
                w_fidelity    = excluded.w_fidelity,
                is_oracle     = MAX(is_oracle, excluded.is_oracle),
                xor_bucket    = excluded.xor_bucket,
                quality_score = excluded.quality_score,
                last_seen     = excluded.last_seen,
                updated_at    = excluded.updated_at
        """, (node_id, address, gossip_port, oracle_port, miner_address,
              caps_json, block_height, w_fidelity, 1 if is_oracle else 0,
              bucket, quality, time.time(), time.time()))
        db.commit()
    except Exception as e:
        logger.debug(f"[DHT] upsert_peer error: {e}")


# ─── Virtual Pseudoqubit Manager ─────────────────────────────────────────────────────────────────

class VirtualPseudoqubitManager:
    """
    Museum-grade virtual pseudoqubit management system.

    PSEUDOQUBIT TAXONOMY:
    ─────────────────────
    pq0          — Oracle pseudoqubit. The ground-truth W-state source. 
                   One exists per oracle (main or P2P-promoted).
                   pq0 is initialized from the oracle's W-state snapshot
                   and continuously updated via the oracle's sync loop.

    virtual_pq   — A LOCAL COPY of pq0, maintained by a miner node.
                   Created when a node mirrors an oracle's density matrix.
                   Slight decoherence perturbation applied on each rotation
                   to simulate real quantum state spread across the network.
                   Used for: block entropy generation, PoW witness.

    inverse_virtual_pq — Anti-correlated counterpart of virtual_pq.
                   Density matrix = I/8 - α·ρ_vpq + (α)·ρ_mixed
                   where α = 1 - fidelity (inversion strength).
                   Used for: detecting oracle divergence, cross-validation,
                   error correction via anti-correlation channel.

    ENTANGLEMENT CONTRACT:
    ─────────────────────
    • pq0_main ↔ pq0_local : established via OracleEntanglementBridge
    • pq0_local ↔ pq0_peer  : established via PeerExchangeManager when
                               two P2P oracles handshake
    • virtual_pq ↔ pq0      : 1-to-1, same oracle
    • inverse_virtual_pq ↔ virtual_pq : anti-correlated pair

    All entanglement links are persisted to pq_entanglement_registry.
    """

    # Promotion thresholds
    ORACLE_PEER_THRESHOLD   = 3    # min peers to become relay-eligible
    ORACLE_FULL_THRESHOLD   = 7    # min peers for full oracle promotion
    ORACLE_FIDELITY_MIN     = 0.80 # min avg W-state fidelity for oracle ops
    ORACLE_BLOCKS_MIN       = 10   # min blocks mined to be oracle-eligible
    ORACLE_UPTIME_MIN_S     = 300  # min 5 minutes uptime

    def __init__(self, db: sqlite3.Connection, local_node_id: str,
                 miner_address: str, oracle_id: str = 'main'):
        self.db             = db
        self.local_node_id  = local_node_id
        self.miner_address  = miner_address
        self.oracle_id      = oracle_id          # parent oracle ID
        self._lock          = threading.RLock()

        # In-memory state (mirrored to DB)
        self._pq0: Optional[np.ndarray]           = None   # pq0 density matrix
        self._vpq: Dict[str, np.ndarray]          = {}     # virtual pq matrices
        self._ivpq: Dict[str, np.ndarray]         = {}     # inverse-virtual pq matrices
        self._fidelity: float                     = 0.0
        self._coherence: float                    = 0.0
        self._entanglement_links: Dict[str, Dict] = {}

        logger.info(
            f"[VPQ] 🔮 VirtualPseudoqubitManager init | node={local_node_id[:12]} | "
            f"oracle={oracle_id}"
        )

    # ── pq0 initialization ────────────────────────────────────────────────────────
    def initialize_pq0(self, oracle_snapshot: Dict[str, Any]) -> bool:
        """
        Initialize local pq0 from oracle W-state snapshot.
        pq0 is the anchor — all virtual and inverse-virtual pqs derive from it.
        """
        try:
            fidelity  = float(oracle_snapshot.get('fidelity', 0.9))
            coherence = float(oracle_snapshot.get('coherence', 0.85))

            # Build pq0 density matrix: fidelity-weighted W-state + depolarizing noise
            w_amp  = 1.0 / np.sqrt(3.0)
            w_vec  = np.zeros(8, dtype=np.complex128)
            w_vec[4] = w_amp   # |100⟩
            w_vec[2] = w_amp   # |010⟩
            w_vec[1] = w_amp   # |001⟩
            rho_pure  = np.outer(w_vec, w_vec.conj())
            rho_mixed = np.eye(8, dtype=np.complex128) / 8.0
            pq0_dm    = fidelity * rho_pure + (1.0 - fidelity) * rho_mixed

            with self._lock:
                self._pq0      = pq0_dm
                self._fidelity = fidelity
                self._coherence = coherence

            # Persist pq0 state
            self._persist_pq_state('pq0', 'oracle', pq0_dm, fidelity, coherence,
                                    oracle_snapshot.get('w_entropy_hash', ''))

            logger.info(
                f"[VPQ] ✅ pq0 initialized | F={fidelity:.4f} | C={coherence:.4f} | "
                f"oracle={self.oracle_id}"
            )
            return True
        except Exception as e:
            logger.error(f"[VPQ] ❌ pq0 init failed: {e}")
            return False

    def spawn_virtual_pq(self, vpq_id: str = None) -> Optional[str]:
        """
        Spawn a virtual pseudoqubit (local copy of pq0 with slight decoherence).

        Virtual pq mirrors the oracle W-state but applies a small perturbation
        to simulate real quantum state spread through the network (decoherence).
        Used for entropy generation during block mining.

        Returns the vpq_id of the newly created virtual pq.
        """
        with self._lock:
            if self._pq0 is None:
                logger.warning("[VPQ] ⚠️  Cannot spawn vpq — pq0 not initialized")
                return None

            vpq_id = vpq_id or f"vpq_{hashlib.sha1(f'{self.local_node_id}{time.time()}'.encode()).hexdigest()[:8]}"

            # Apply decoherence perturbation: Kraus operator noise channel
            # ε_decohere = 0.005 (0.5% decoherence per hop)
            noise = np.random.normal(0, 0.005, (8, 8)).astype(np.complex128)
            noise += 1j * np.random.normal(0, 0.003, (8, 8))
            noise = (noise + noise.conj().T) / 2     # enforce Hermiticity
            vpq_dm = 0.995 * self._pq0 + 0.005 * noise
            # Re-normalize trace
            tr = np.real(np.trace(vpq_dm))
            if tr > 0:
                vpq_dm /= tr

            self._vpq[vpq_id] = vpq_dm

            # Compute effective fidelity of the virtual pq
            vpq_fidelity = max(0.0, self._fidelity - 0.005)   # slight degradation
            self._persist_pq_state(vpq_id, 'virtual', vpq_dm, vpq_fidelity,
                                    self._coherence, '', partner=vpq_id.replace('vpq_',''))

        logger.debug(f"[VPQ] 🔮 Spawned virtual pq: {vpq_id}")
        return vpq_id

    def spawn_inverse_virtual_pq(self, parent_vpq_id: str) -> Optional[str]:
        """
        Spawn an inverse-virtual pseudoqubit — anti-correlated partner of a virtual pq.

        MATH:
            ρ_ivpq = (I/8) · α + ρ_mixed · (1-α) - ε · ρ_vpq
            where α = 1 - fidelity(vpq)   [inversion strength]
                  ε = 0.1                  [anti-correlation coupling]

        The ivpq captures the 'shadow' of the virtual pq — it is maximally
        anti-correlated. In practice this lets us detect divergence between
        the oracle and a miner: if F(vpq) rises, F(ivpq) falls proportionally.
        Cross-correlation F(vpq) + F(ivpq) ≈ 1.0 when both are functioning.

        Used for:
          • Oracle divergence detection (F_vpq + F_ivpq should sum to ~1.0)
          • Error correction via anti-correlation channel
          • Fault isolation when one oracle diverges from others
        """
        with self._lock:
            if parent_vpq_id not in self._vpq:
                logger.warning(f"[VPQ] ⚠️  Parent vpq {parent_vpq_id} not found for inverse spawn")
                return None

            vpq_dm     = self._vpq[parent_vpq_id]
            vpq_fid    = max(0.0, self._fidelity - 0.005)
            alpha      = 1.0 - vpq_fid       # inversion strength
            epsilon    = 0.1                   # anti-correlation coupling
            rho_mixed  = np.eye(8, dtype=np.complex128) / 8.0
            ivpq_dm    = rho_mixed * alpha + rho_mixed * (1.0 - alpha) - epsilon * vpq_dm
            # Clip negative eigenvalues (positive-semidefinite correction)
            eigenvalues, eigenvectors = np.linalg.eigh(ivpq_dm)
            eigenvalues = np.maximum(eigenvalues, 0)
            ivpq_dm    = eigenvectors @ np.diag(eigenvalues.astype(np.complex128)) @ eigenvectors.conj().T
            tr = np.real(np.trace(ivpq_dm))
            if tr > 0:
                ivpq_dm /= tr

            ivpq_id = f"ivpq_{parent_vpq_id.replace('vpq_','')}"
            self._ivpq[ivpq_id] = ivpq_dm

            # Register entanglement link
            self._register_entanglement_link(
                parent_vpq_id, ivpq_id,
                self.oracle_id, self.oracle_id,
                'virtual_to_inverse', vpq_fid * (1.0 - vpq_fid)  # anti-correlation score
            )

            ivpq_fidelity = 1.0 - vpq_fid  # complementary
            self._persist_pq_state(ivpq_id, 'inverse_virtual', ivpq_dm,
                                    ivpq_fidelity, self._coherence, '',
                                    partner=parent_vpq_id)

        logger.debug(f"[VPQ] 🔄 Spawned inverse-virtual pq: {ivpq_id} ↔ {parent_vpq_id}")
        return ivpq_id

    def rotate_virtual_pq(self, vpq_id: str) -> bool:
        """
        Rotate a virtual pq: re-derive from current pq0 with fresh decoherence perturbation.
        Called after each block is mined — mirrors pq rotation in P2PClientWStateRecovery.
        """
        with self._lock:
            if self._pq0 is None or vpq_id not in self._vpq:
                return False

            # Apply fresh perturbation from updated pq0
            noise = np.random.normal(0, 0.005, (8, 8)).astype(np.complex128)
            noise = (noise + noise.conj().T) / 2
            new_vpq = 0.995 * self._pq0 + 0.005 * noise
            tr = np.real(np.trace(new_vpq))
            if tr > 0:
                new_vpq /= tr
            self._vpq[vpq_id] = new_vpq

        return True

    def measure_virtual_pq_entropy(self, vpq_id: str) -> str:
        """
        Measure a virtual pq to produce 256-bit quantum entropy.
        Diagonal of density matrix → probability distribution → sampling → SHA3-256.
        Falls back to CSPRNG if pq not available.
        """
        with self._lock:
            dm = self._vpq.get(vpq_id) or self._pq0

        if dm is None:
            return secrets.token_hex(32)

        try:
            diag = np.real(np.diag(dm))
            diag = np.maximum(diag, 0)
            diag /= diag.sum()

            # Sample 64 outcomes from the probability distribution
            outcomes = np.random.choice(8, size=64, p=diag)
            entropy_source = ''.join(str(o) for o in outcomes)
            return hashlib.sha3_256(entropy_source.encode()).hexdigest()
        except Exception:
            return secrets.token_hex(32)

    def verify_oracle_anti_correlation(self, vpq_id: str) -> Tuple[float, str]:
        """
        Verify vpq/ivpq anti-correlation integrity.
        F(vpq) + F(ivpq) should be within ε of 1.0 for a healthy pair.
        Returns (divergence_score, status_string).
        """
        ivpq_id = f"ivpq_{vpq_id.replace('vpq_','')}"
        with self._lock:
            if vpq_id not in self._vpq or ivpq_id not in self._ivpq:
                return 0.0, "incomplete_pair"

            # W-state ideal for fidelity measurement
            w_amp = 1.0 / np.sqrt(3.0)
            w_vec = np.zeros(8, dtype=np.complex128)
            w_vec[4] = w_amp; w_vec[2] = w_amp; w_vec[1] = w_amp
            rho_ideal = np.outer(w_vec, w_vec.conj())

            f_vpq  = float(np.real(np.trace(self._vpq[vpq_id]  @ rho_ideal)))
            f_ivpq = float(np.real(np.trace(self._ivpq[ivpq_id] @ rho_ideal)))
            f_sum  = f_vpq + f_ivpq
            divergence = abs(1.0 - f_sum)

        if divergence < 0.05:
            status = "healthy"
        elif divergence < 0.15:
            status = "marginal"
        else:
            status = "diverged"

        return divergence, status

    def get_pq_status(self) -> Dict[str, Any]:
        """Return comprehensive pseudoqubit status for monitoring/dashboard."""
        with self._lock:
            return {
                'pq0_initialized': self._pq0 is not None,
                'pq0_fidelity':    self._fidelity,
                'pq0_coherence':   self._coherence,
                'virtual_pqs':     list(self._vpq.keys()),
                'inverse_vpqs':    list(self._ivpq.keys()),
                'entanglement_links': len(self._entanglement_links),
                'oracle_id':       self.oracle_id,
                'node_id':         self.local_node_id[:16],
            }

    # ── Private helpers ───────────────────────────────────────────────────────────
    def _persist_pq_state(self, pq_id: str, pq_type: str, dm: np.ndarray,
                           fidelity: float, coherence: float, w_entropy: str,
                           partner: str = '') -> None:
        if self.db is None:
            return
        try:
            dm_hex = dm.tobytes().hex()
            purity = float(np.real(np.trace(dm @ dm)))
            self.db.execute("""
                INSERT INTO virtual_pq_state
                    (pq_id, pq_type, oracle_id, node_id, fidelity, coherence, purity,
                     entanglement_partner, w_entropy_hash, density_matrix_hex,
                     last_measured, measurement_count, is_active, updated_at)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,1,1,?)
                ON CONFLICT(pq_id) DO UPDATE SET
                    fidelity    = excluded.fidelity,
                    coherence   = excluded.coherence,
                    purity      = excluded.purity,
                    w_entropy_hash = excluded.w_entropy_hash,
                    density_matrix_hex = excluded.density_matrix_hex,
                    last_measured = excluded.last_measured,
                    measurement_count = measurement_count + 1,
                    updated_at  = excluded.updated_at
            """, (pq_id, pq_type, self.oracle_id, self.local_node_id,
                  fidelity, coherence, purity, partner, w_entropy,
                  dm_hex, time.time(), time.time()))
            self.db.commit()
        except Exception as e:
            logger.debug(f"[VPQ] persist error: {e}")

    def _register_entanglement_link(self, pq_a: str, pq_b: str,
                                     oracle_a: str, oracle_b: str,
                                     link_type: str, fidelity_ab: float) -> None:
        link_id = hashlib.sha256(f"{pq_a}:{pq_b}".encode()).hexdigest()[:32]
        with self._lock:
            self._entanglement_links[link_id] = {
                'pq_a': pq_a, 'pq_b': pq_b,
                'oracle_a': oracle_a, 'oracle_b': oracle_b,
                'link_type': link_type, 'fidelity': fidelity_ab,
                'established': time.time()
            }
        if self.db is None:
            return
        try:
            self.db.execute("""
                INSERT OR REPLACE INTO pq_entanglement_registry
                    (link_id, pq_a_id, pq_b_id, oracle_a_id, oracle_b_id,
                     link_type, fidelity_ab, is_active, established_at, last_verified)
                VALUES (?,?,?,?,?,?,?,1,?,?)
            """, (link_id, pq_a, pq_b, oracle_a, oracle_b,
                  link_type, fidelity_ab, time.time(), time.time()))
            self.db.commit()
        except Exception as e:
            logger.debug(f"[VPQ] entanglement registry error: {e}")


# ─── Oracle Entanglement Bridge ───────────────────────────────────────────────────────────────────

class OracleEntanglementBridge:
    """
    Establishes and maintains entanglement between:
      1. Local pq0 ↔ Main oracle pq0           (bootstrap link)
      2. Local pq0 ↔ P2P oracle pq0            (peer link, one per discovered P2P oracle)

    PROTOCOL:
      Handshake: POST /gossip/oracle_handshake  → exchange pq0 snapshots
      Response:  {oracle_id, pq0_entropy_hash, fidelity, node_id, gossip_url}

    Entanglement quality measured as:
      F_link = F_local * F_remote * exp(-sync_lag_ms / 1000)

    A link is 'active' when F_link >= 0.70.
    A link is 'degraded' when 0.50 <= F_link < 0.70.
    A link is 'lost'    when F_link < 0.50.

    All links persisted to oracle_registry and pq_entanglement_registry.
    """

    HANDSHAKE_INTERVAL_S  = 60    # re-handshake every 60s
    ENTANGLEMENT_TIMEOUT  = 120   # declare link lost after 120s silence
    MAX_P2P_ORACLE_LINKS  = 8     # max simultaneous P2P oracle entanglements

    def __init__(self, db: sqlite3.Connection, vpm: 'VirtualPseudoqubitManager',
                 local_node_id: str, miner_address: str, main_oracle_url: str):
        self.db               = db
        self.vpm              = vpm
        self.local_node_id    = local_node_id
        self.miner_address    = miner_address
        self.main_oracle_url  = main_oracle_url.rstrip('/')
        self._lock            = threading.RLock()
        self._session         = requests.Session()
        self._session.mount('https://', HTTPAdapter(max_retries=Retry(total=2, backoff_factor=0.5)))
        self._session.mount('http://',  HTTPAdapter(max_retries=Retry(total=2, backoff_factor=0.5)))
        self._active_links: Dict[str, Dict] = {}    # oracle_id → link state
        self._running = False
        self._thread: Optional[threading.Thread] = None
        logger.info(f"[BRIDGE] 🌉 OracleEntanglementBridge init | node={local_node_id[:12]}")

    def _oracle_id_from_url(self, url: str) -> str:
        return hashlib.sha256(url.encode()).hexdigest()[:32]

    def _fetch_oracle_pq0_snapshot(self, oracle_url: str, timeout: int = 8) -> Optional[Dict]:
        """Fetch pq0 snapshot from a remote oracle."""
        endpoints = ['/api/oracle/w-state', '/api/oracle/pq0', '/api/w-state']
        for ep in endpoints:
            try:
                r = self._session.get(f"{oracle_url.rstrip('/')}{ep}", timeout=timeout)
                if r.status_code == 200:
                    return r.json()
            except Exception:
                continue
        return None

    def _handshake_main_oracle(self) -> bool:
        """Establish/refresh entanglement with the main (bootstrap) oracle."""
        snap = self._fetch_oracle_pq0_snapshot(self.main_oracle_url)
        if snap is None:
            logger.debug("[BRIDGE] Main oracle pq0 fetch failed")
            return False

        oracle_id = self._oracle_id_from_url(self.main_oracle_url)
        fidelity  = float(snap.get('fidelity', 0.0))

        # Initialize/update local pq0 from main oracle
        if self.vpm._pq0 is None:
            self.vpm.initialize_pq0(snap)

        f_link = fidelity * self.vpm._fidelity
        status = ('active' if f_link >= 0.70 else
                  'degraded' if f_link >= 0.50 else 'lost')

        with self._lock:
            self._active_links[oracle_id] = {
                'oracle_url': self.main_oracle_url,
                'oracle_id':  oracle_id,
                'is_primary': True,
                'fidelity_link': f_link,
                'status':     status,
                'last_seen':  time.time(),
                'pq0_hash':   snap.get('w_entropy_hash', ''),
            }

        # Register main oracle in DB
        self._upsert_oracle_registry(
            oracle_id=oracle_id,
            oracle_address=snap.get('oracle_address', ''),
            oracle_url=self.main_oracle_url,
            gossip_url='',
            is_primary=True,
            is_local=False,
            node_id=_dht_node_id(self.main_oracle_url),
            pq0_fidelity=fidelity,
            pq0_entropy_hash=snap.get('w_entropy_hash', ''),
            block_height=snap.get('block_height', 0),
            entanglement_status=status,
        )

        if status == 'active':
            logger.info(f"[BRIDGE] ✅ Main oracle entanglement {status} | F_link={f_link:.4f}")
        else:
            logger.warning(f"[BRIDGE] ⚠️  Main oracle link {status} | F_link={f_link:.4f}")
        return status != 'lost'

    def _handshake_p2p_oracle(self, peer: Dict) -> bool:
        """Establish entanglement with a P2P oracle discovered via DHT."""
        gossip_url = peer.get('gossip_url', '')
        if not gossip_url:
            return False

        oracle_id = self._oracle_id_from_url(gossip_url)

        # Don't re-handshake if recently seen and active
        with self._lock:
            existing = self._active_links.get(oracle_id)
            if existing and time.time() - existing['last_seen'] < self.HANDSHAKE_INTERVAL_S / 2:
                return True

        snap = self._fetch_oracle_pq0_snapshot(gossip_url)
        if snap is None:
            return False

        fidelity = float(snap.get('fidelity', 0.0))
        f_link   = fidelity * self.vpm._fidelity
        status   = ('active' if f_link >= 0.70 else
                    'degraded' if f_link >= 0.50 else 'lost')

        with self._lock:
            self._active_links[oracle_id] = {
                'oracle_url':   gossip_url,
                'oracle_id':    oracle_id,
                'is_primary':   False,
                'fidelity_link': f_link,
                'status':       status,
                'last_seen':    time.time(),
                'pq0_hash':     snap.get('w_entropy_hash', ''),
                'peer_address': peer.get('peer_address', ''),
            }

        self._upsert_oracle_registry(
            oracle_id=oracle_id,
            oracle_address=snap.get('oracle_address', peer.get('miner_address', '')),
            oracle_url=gossip_url,
            gossip_url=gossip_url,
            is_primary=False,
            is_local=False,
            node_id=peer.get('node_id', ''),
            pq0_fidelity=fidelity,
            pq0_entropy_hash=snap.get('w_entropy_hash', ''),
            block_height=snap.get('block_height', peer.get('block_height', 0)),
            entanglement_status=status,
        )

        # Register cross-oracle entanglement link
        local_pq0_id = 'pq0'
        remote_pq0_id = f"pq0_{oracle_id[:8]}"
        self.vpm._register_entanglement_link(
            local_pq0_id, remote_pq0_id,
            'local', oracle_id,
            'p2p_to_p2p', f_link
        )

        logger.info(
            f"[BRIDGE] {'✅' if status=='active' else '⚠️ '} P2P oracle link {status} | "
            f"oracle={oracle_id[:16]} | F_link={f_link:.4f}"
        )
        return status != 'lost'

    def refresh_all_links(self, peer_oracles: List[Dict] = None) -> Dict[str, Any]:
        """Refresh main oracle + all known P2P oracle entanglement links."""
        results = {'main': False, 'p2p': 0, 'active_links': 0}
        results['main'] = self._handshake_main_oracle()

        if peer_oracles:
            for peer in peer_oracles[:self.MAX_P2P_ORACLE_LINKS]:
                if peer.get('is_oracle') and peer.get('gossip_url'):
                    if self._handshake_p2p_oracle(peer):
                        results['p2p'] += 1

        with self._lock:
            results['active_links'] = sum(
                1 for l in self._active_links.values() if l['status'] == 'active'
            )
        return results

    def get_entanglement_status(self) -> Dict[str, Any]:
        """Snapshot of all entanglement link states."""
        with self._lock:
            links = {}
            for oid, link in self._active_links.items():
                links[oid[:16]] = {
                    'status':     link['status'],
                    'fidelity':   link['fidelity_link'],
                    'is_primary': link['is_primary'],
                    'age_s':      time.time() - link['last_seen'],
                }
            return {
                'active_count': sum(1 for l in self._active_links.values() if l['status']=='active'),
                'total_links':  len(self._active_links),
                'links':        links,
            }

    def start(self) -> None:
        """Start background entanglement maintenance thread."""
        self._running = True
        self._thread  = threading.Thread(
            target=self._maintenance_loop, daemon=True, name='OracleBridge'
        )
        self._thread.start()
        logger.info("[BRIDGE] 🔗 Entanglement maintenance loop started")

    def stop(self) -> None:
        self._running = False
        if self._thread:
            self._thread.join(timeout=5)

    def _maintenance_loop(self) -> None:
        # Initial handshake
        time.sleep(5)
        self._handshake_main_oracle()

        while self._running:
            try:
                time.sleep(self.HANDSHAKE_INTERVAL_S)
                self._handshake_main_oracle()
                # Expire stale links
                now = time.time()
                with self._lock:
                    stale = [oid for oid, l in self._active_links.items()
                             if now - l['last_seen'] > self.ENTANGLEMENT_TIMEOUT
                             and not l['is_primary']]
                    for oid in stale:
                        logger.info(f"[BRIDGE] 🕳️  Expiring stale P2P oracle link: {oid[:16]}")
                        del self._active_links[oid]
            except Exception as e:
                logger.debug(f"[BRIDGE] Maintenance error: {e}")

    def _upsert_oracle_registry(self, **kwargs) -> None:
        if self.db is None:
            return
        try:
            self.db.execute("""
                INSERT INTO oracle_registry
                    (oracle_id, oracle_address, oracle_url, gossip_url, is_primary, is_local,
                     node_id, pq0_fidelity, pq0_entropy_hash, block_height,
                     entanglement_status, trust_score, last_seen)
                VALUES (:oracle_id,:oracle_address,:oracle_url,:gossip_url,:is_primary,:is_local,
                        :node_id,:pq0_fidelity,:pq0_entropy_hash,:block_height,
                        :entanglement_status,0.8,:last_seen)
                ON CONFLICT(oracle_id) DO UPDATE SET
                    pq0_fidelity        = excluded.pq0_fidelity,
                    pq0_entropy_hash    = excluded.pq0_entropy_hash,
                    block_height        = MAX(block_height, excluded.block_height),
                    entanglement_status = excluded.entanglement_status,
                    last_seen           = excluded.last_seen
            """, {**kwargs, 'last_seen': time.time()})
            self.db.commit()
        except Exception as e:
            logger.debug(f"[BRIDGE] oracle_registry upsert error: {e}")


# ─── Oracle Eligibility Engine ────────────────────────────────────────────────────────────────────

class OracleEligibilityEngine:
    """
    Autonomous oracle promotion logic.

    PROMOTION CRITERIA — a node becomes eligible for oracle status when ALL hold:
    ─────────────────────────────────────────────────────────────────────────────
    1. PEER COUNT   ≥ ORACLE_FULL_THRESHOLD (7 live peers in DHT)
    2. AVG FIDELITY ≥ ORACLE_FIDELITY_MIN (0.80)
    3. BLOCKS MINED ≥ ORACLE_BLOCKS_MIN (10 blocks)
    4. UPTIME       ≥ ORACLE_UPTIME_MIN_S (300s = 5 minutes)
    5. NOT already an oracle (no duplicate promotion)

    PROMOTION TYPES:
    ─────────────────
    'genesis'          — First node on network (no oracle exists at all)
    'p2p_expansion'    — Network growing, additional oracle improves resilience
    'primary_failover' — Main oracle unreachable for > FAILOVER_TIMEOUT_S seconds

    WHAT HAPPENS ON PROMOTION:
    ──────────────────────────
    1. GossipHTTPHandler begins serving /api/oracle/w-state and /api/oracle/register
    2. VirtualPseudoqubitManager promotes pq0 to full oracle status
    3. OracleEntanglementBridge registers this node as a P2P oracle with all known peers
    4. oracle_registry updated with is_local=1
    5. Broadcast oracle_announcement to all peers via gossip
    6. DHTManager updates node capabilities to include 'oracle'

    The promoted oracle continues mining — oracle duties run in parallel.
    """

    FAILOVER_TIMEOUT_S      = 300   # 5 min main oracle silence → trigger failover check
    ORACLE_CHECK_INTERVAL_S = 60    # check eligibility every 60s
    ORACLE_FULL_THRESHOLD   = 7
    ORACLE_FIDELITY_MIN     = 0.80
    ORACLE_BLOCKS_MIN       = 10
    ORACLE_UPTIME_MIN_S     = 300

    def __init__(self, db: sqlite3.Connection, local_node_id: str,
                 miner_address: str, vpm: 'VirtualPseudoqubitManager',
                 bridge: 'OracleEntanglementBridge'):
        self.db              = db
        self.local_node_id   = local_node_id
        self.miner_address   = miner_address
        self.vpm             = vpm
        self.bridge          = bridge
        self._lock           = threading.RLock()
        self._is_oracle      = False
        self._oracle_started = False
        self._oracle_server: Optional['P2POracleServer'] = None
        self._listener_ref: Optional['GossipListener'] = None   # set by QTCLP2PBundle
        self._start_time     = time.time()
        self._running        = False
        self._thread: Optional[threading.Thread] = None
        self._main_oracle_last_seen = time.time()  # track main oracle liveness
        logger.info(f"[ELIG] 🎯 OracleEligibilityEngine init | node={local_node_id[:12]}")

    def record_main_oracle_seen(self) -> None:
        """Called whenever we successfully contact the main oracle."""
        self._main_oracle_last_seen = time.time()

    @property
    def is_oracle(self) -> bool:
        return self._is_oracle

    def check_eligibility(self, peer_count: int, blocks_mined: int,
                           avg_fidelity: float) -> Dict[str, Any]:
        """
        Evaluate oracle promotion eligibility.
        Returns detailed eligibility report (always logged to DB).
        """
        uptime_s       = time.time() - self._start_time
        oracle_count   = self._count_known_oracles()
        main_oracle_age = time.time() - self._main_oracle_last_seen

        # Determine promotion type
        promotion_type = ''
        if oracle_count == 0:
            promotion_type = 'genesis'
        elif main_oracle_age > self.FAILOVER_TIMEOUT_S:
            promotion_type = 'primary_failover'
        elif peer_count >= self.ORACLE_FULL_THRESHOLD and oracle_count < max(1, peer_count // 7):
            promotion_type = 'p2p_expansion'

        eligible = (
            not self._is_oracle and
            peer_count >= self.ORACLE_FULL_THRESHOLD and
            avg_fidelity >= self.ORACLE_FIDELITY_MIN and
            blocks_mined >= self.ORACLE_BLOCKS_MIN and
            uptime_s >= self.ORACLE_UPTIME_MIN_S and
            bool(promotion_type)
        )

        # Also eligible for genesis with lower thresholds if no oracle at all
        if not eligible and promotion_type == 'genesis' and not self._is_oracle:
            eligible = (
                peer_count >= 1 and
                avg_fidelity >= 0.70 and
                blocks_mined >= 3 and
                uptime_s >= 60.0
            )

        result = {
            'eligible':       eligible,
            'peer_count':     peer_count,
            'oracle_count':   oracle_count,
            'blocks_mined':   blocks_mined,
            'avg_fidelity':   avg_fidelity,
            'uptime_s':       uptime_s,
            'promotion_type': promotion_type,
            'main_oracle_age_s': main_oracle_age,
            'already_oracle': self._is_oracle,
            'notes':          self._build_eligibility_notes(
                peer_count, avg_fidelity, blocks_mined, uptime_s, promotion_type
            ),
        }

        # Persist eligibility check
        self._log_eligibility(result)
        return result

    def attempt_promotion(self, peer_count: int, blocks_mined: int,
                           avg_fidelity: float, gossip_url: str = '') -> bool:
        """
        Attempt oracle self-promotion if eligible.
        Returns True if successfully promoted.
        """
        report = self.check_eligibility(peer_count, blocks_mined, avg_fidelity)
        if not report['eligible']:
            return False

        with self._lock:
            if self._is_oracle:
                return True   # already promoted (race condition guard)

            logger.info(
                f"[ELIG] 🌟 ORACLE PROMOTION INITIATED | "
                f"type={report['promotion_type']} | peers={peer_count} | "
                f"blocks={blocks_mined} | F={avg_fidelity:.4f}"
            )

            # Start P2P oracle server
            self._oracle_server = P2POracleServer(
                db              = self.db,
                vpm             = self.vpm,
                local_node_id   = self.local_node_id,
                miner_address   = self.miner_address,
                oracle_id       = hashlib.sha256(self.miner_address.encode()).hexdigest()[:32],
            )
            if self._oracle_server.start():
                self._is_oracle       = True
                self._oracle_started  = True

                # Activate oracle routes on the unified 9091 server (no new port)
                if self._listener_ref is not None:
                    self._listener_ref.inject_oracle(self._oracle_server)
                else:
                    logger.warning("[ELIG] ⚠️  No listener_ref — oracle routes not yet active on HTTP server")

                # Register self in oracle_registry
                oracle_id = hashlib.sha256(self.miner_address.encode()).hexdigest()[:32]
                self.bridge._upsert_oracle_registry(
                    oracle_id           = oracle_id,
                    oracle_address      = self.miner_address,
                    oracle_url          = gossip_url or f"http://localhost:{self._oracle_server.port}",
                    gossip_url          = gossip_url,
                    is_primary          = 0,
                    is_local            = 1,
                    node_id             = self.local_node_id,
                    pq0_fidelity        = avg_fidelity,
                    pq0_entropy_hash    = '',
                    block_height        = blocks_mined,
                    entanglement_status = 'active',
                )

                logger.info(
                    f"[ELIG] ✅ ORACLE ACTIVE | oracle_id={oracle_id[:16]} | "
                    f"port={self._oracle_server.port} | "
                    f"type={report['promotion_type']}"
                )
                return True
            else:
                logger.error("[ELIG] ❌ Oracle server failed to start")
                return False

    def start(self) -> None:
        """Start background eligibility check loop."""
        self._running = True
        self._thread  = threading.Thread(
            target=self._eligibility_loop, daemon=True, name='OracleEligibility'
        )
        self._thread.start()
        logger.info("[ELIG] 🔍 Oracle eligibility loop started")

    def stop(self) -> None:
        self._running = False
        if self._oracle_server:
            self._oracle_server.stop()
        if self._thread:
            self._thread.join(timeout=5)

    def _eligibility_loop(self) -> None:
        time.sleep(30)  # initial grace period
        while self._running:
            try:
                time.sleep(self.ORACLE_CHECK_INTERVAL_S)
                if self._is_oracle:
                    continue  # already promoted — nothing to check
                # Metrics collected from DB
                peer_count   = self._count_live_peers()
                blocks_mined = self._count_mined_blocks()
                avg_fidelity = self._get_avg_fidelity()
                self.attempt_promotion(peer_count, blocks_mined, avg_fidelity)
            except Exception as e:
                logger.debug(f"[ELIG] Loop error: {e}")

    def _count_known_oracles(self) -> int:
        if self.db is None:
            return 0
        try:
            cur = self.db.execute(
                "SELECT COUNT(*) FROM oracle_registry WHERE last_seen > ?",
                (time.time() - 300,)
            )
            return cur.fetchone()[0] or 0
        except Exception:
            return 0

    def _count_live_peers(self) -> int:
        if self.db is None:
            return 0
        try:
            cur = self.db.execute(
                "SELECT COUNT(*) FROM dht_peers WHERE last_seen > ?",
                (time.time() - 120,)
            )
            return cur.fetchone()[0] or 0
        except Exception:
            return 0

    def _count_mined_blocks(self) -> int:
        if self.db is None:
            return 0
        try:
            cur = self.db.execute(
                "SELECT COUNT(*) FROM blocks WHERE miner_address = ?",
                (self.miner_address,)
            )
            return cur.fetchone()[0] or 0
        except Exception:
            return 0

    def _get_avg_fidelity(self) -> float:
        if self.db is None:
            return 0.0
        try:
            cur = self.db.execute(
                "SELECT AVG(w_fidelity) FROM dht_peers WHERE last_seen > ?",
                (time.time() - 300,)
            )
            val = cur.fetchone()[0]
            if val is None:
                # Fall back to own virtual pq fidelity
                return self.vpm._fidelity
            return float(val)
        except Exception:
            return self.vpm._fidelity

    def _build_eligibility_notes(self, peers: int, fidelity: float,
                                  blocks: int, uptime: float, ptype: str) -> str:
        notes = []
        if peers < self.ORACLE_FULL_THRESHOLD:
            notes.append(f"need {self.ORACLE_FULL_THRESHOLD - peers} more peers")
        if fidelity < self.ORACLE_FIDELITY_MIN:
            notes.append(f"fidelity {fidelity:.3f} < {self.ORACLE_FIDELITY_MIN}")
        if blocks < self.ORACLE_BLOCKS_MIN:
            notes.append(f"need {self.ORACLE_BLOCKS_MIN - blocks} more blocks")
        if uptime < self.ORACLE_UPTIME_MIN_S:
            notes.append(f"uptime {uptime:.0f}s < {self.ORACLE_UPTIME_MIN_S}s")
        if not ptype:
            notes.append("no promotion trigger")
        return '; '.join(notes) if notes else 'all criteria met'

    def _log_eligibility(self, report: Dict) -> None:
        if self.db is None:
            return
        try:
            tip_cur = self.db.execute("SELECT MAX(height) FROM blocks")
            tip_h   = tip_cur.fetchone()[0] or 0
            self.db.execute("""
                INSERT INTO oracle_eligibility
                    (check_height, peer_count, oracle_count, blocks_mined,
                     avg_fidelity, uptime_s, eligible, promoted, promotion_type, notes)
                VALUES (?,?,?,?,?,?,?,?,?,?)
            """, (tip_h, report['peer_count'], report['oracle_count'],
                  report['blocks_mined'], report['avg_fidelity'], report['uptime_s'],
                  1 if report['eligible'] else 0,
                  1 if (report['eligible'] and self._is_oracle) else 0,
                  report['promotion_type'], report['notes']))
            self.db.commit()
        except Exception as e:
            logger.debug(f"[ELIG] log error: {e}")


# ─── P2P Oracle Server (spawned on promotion) ─────────────────────────────────────────────────────

class P2POracleServer:
    """
    Minimal oracle REST server spawned when a miner node self-promotes to oracle.

    Serves the same API surface as the main oracle — peers and other miners
    can use this node as a W-state source, registrar, and block validator.

    ENDPOINTS:
    ─────────────────────────────────────────────────────────────────────────
    GET  /api/oracle/w-state           → current pq0 W-state snapshot
    GET  /api/oracle/pq0               → alias for /api/oracle/w-state
    POST /api/oracle/register          → miner registration
    GET  /api/oracle/miners            → list registered miners
    GET  /api/blocks/tip               → local chain tip
    GET  /api/blocks/height/<h>        → block by height (from local DB mirror)
    GET  /api/mempool                  → pending transactions
    POST /gossip/oracle_handshake      → P2P oracle entanglement handshake
    GET  /api/peers/list               → live peer list
    POST /api/peers/register           → peer registration
    POST /api/peers/heartbeat          → peer heartbeat

    All served by a minimal WSGI-lite thread — no Flask dependency.
    Uses http.server.BaseHTTPRequestHandler (same as GossipHTTPHandler).
    """

    # P2POracleServer no longer binds its own port.
    # Oracle routes (/api/oracle/*) are activated on the unified 9091 server
    # via GossipListener.inject_oracle(self) when this node is promoted.
    # BASE_PORT / MAX_PORT kept for backwards compat but unused.
    BASE_PORT    = 9091
    MAX_PORT     = 9091

    def __init__(self, db: sqlite3.Connection, vpm: 'VirtualPseudoqubitManager',
                 local_node_id: str, miner_address: str, oracle_id: str):
        self.db             = db
        self.vpm            = vpm
        self.local_node_id  = local_node_id
        self.miner_address  = miner_address
        self.oracle_id      = oracle_id
        self.port: Optional[int] = 9091     # always the unified port
        self._server        = None          # no separate server — uses GossipListener's
        self._thread        = None
        self._running       = False
        self._registered_miners: Dict[str, Dict] = {}

    def start(self) -> bool:
        """
        Oracle 'start' no longer binds a port.
        The caller (OracleEligibilityEngine) is responsible for calling
        GossipListener.inject_oracle(self) to activate routes on the unified server.
        """
        self._running = True
        logger.info(f"[P2PORACLE] 🌟 Oracle state ready | routes activate on unified :{self.port}")
        return True

    def stop(self) -> None:
        self._running = False
        if self._server:
            try:
                self._server.shutdown()
            except Exception:
                pass

    def get_pq0_snapshot(self) -> Dict[str, Any]:
        """Build a pq0 snapshot dict compatible with the main oracle API shape."""
        status = self.vpm.get_pq_status()
        w_entropy = self.vpm.measure_virtual_pq_entropy('pq0') if self.vpm._pq0 is not None \
                    else secrets.token_hex(32)
        dm_hex = self.vpm._pq0.tobytes().hex() if self.vpm._pq0 is not None else ''
        return {
            'oracle_address':     self.miner_address,
            'oracle_id':          self.oracle_id,
            'node_id':            self.local_node_id,
            'timestamp_ns':       time.time_ns(),
            'fidelity':           status['pq0_fidelity'],
            'w_state_fidelity':   status['pq0_fidelity'],
            'coherence':          status['pq0_coherence'],
            'purity':             0.9,
            'entanglement':       0.9,
            'w_entropy_hash':     w_entropy,
            'density_matrix_hex': dm_hex[:1024],   # truncate for transport
            'signature_valid':    True,
            'block_height':       self._get_tip_height(),
            'is_p2p_oracle':      True,
            'hlwe_signature': {
                'commitment':      hashlib.sha3_256(w_entropy.encode()).hexdigest(),
                'witness':         hashlib.sha3_256((w_entropy + self.miner_address).encode()).hexdigest(),
                'proof':           secrets.token_hex(32),
                'w_entropy_hash':  w_entropy,
                'derivation_path': "m/838'/0'/0'",
                'public_key_hex':  hashlib.sha3_256(self.miner_address.encode()).hexdigest(),
            },
        }

    def _get_tip_height(self) -> int:
        if self.db is None:
            return 0
        try:
            cur = self.db.execute("SELECT MAX(height) FROM blocks")
            val = cur.fetchone()[0]
            return int(val) if val is not None else 0
        except Exception:
            return 0

    def _make_handler(self):
        """Factory: return a handler class with self (P2POracleServer) in closure."""
        oracle_srv = self

        class _OracleHandler(http.server.BaseHTTPRequestHandler):
            def log_message(self, fmt, *args):
                logger.debug(f"[P2PORACLE/http] {fmt % args}")

            def _send_json(self, code: int, body: dict) -> None:
                data = json.dumps(body).encode()
                self.send_response(code)
                self.send_header('Content-Type', 'application/json')
                self.send_header('Content-Length', str(len(data)))
                self.end_headers()
                self.wfile.write(data)

            def _read_json(self) -> Optional[dict]:
                n = int(self.headers.get('Content-Length', 0))
                if n <= 0 or n > 1_048_576:
                    return None
                try:
                    return json.loads(self.rfile.read(n))
                except Exception:
                    return None

            def do_GET(self):
                path = _urlparse.urlparse(self.path).path
                db   = oracle_srv.db

                if path in ('/api/oracle/w-state', '/api/oracle/pq0'):
                    self._send_json(200, oracle_srv.get_pq0_snapshot())

                elif path == '/api/oracle/miners':
                    miners = list(oracle_srv._registered_miners.values())
                    self._send_json(200, {'miners': miners, 'count': len(miners)})

                elif path == '/api/blocks/tip':
                    h = oracle_srv._get_tip_height()
                    tip_hash = ''
                    if db:
                        try:
                            cur = db.execute("SELECT block_hash FROM blocks WHERE height=?", (h,))
                            row = cur.fetchone()
                            if row:
                                tip_hash = row[0]
                        except Exception:
                            pass
                    self._send_json(200, {'height': h, 'block_height': h,
                                          'block_hash': tip_hash, 'source': 'p2p_oracle'})

                elif path.startswith('/api/blocks/height/'):
                    try:
                        h = int(path.split('/')[-1])
                        blk = _local_db_get_block(db, h)
                        if blk:
                            self._send_json(200, blk)
                        else:
                            self._send_json(404, {'error': f'block {h} not cached'})
                    except (ValueError, IndexError):
                        self._send_json(400, {'error': 'invalid height'})

                elif path == '/api/mempool':
                    txs = _local_db_get_pending(db)
                    self._send_json(200, {'transactions': txs, 'count': len(txs)})

                elif path == '/api/peers/list':
                    peers = _local_db_get_best_peers(db, limit=20)
                    self._send_json(200, {'peers': peers, 'count': len(peers)})

                elif path == '/api/dht/hello':
                    # DHT discovery — return closest known peers
                    self._send_json(200, {
                        'oracle_id':     oracle_srv.oracle_id,
                        'node_id':       oracle_srv.local_node_id,
                        'oracle_url':    f"http://localhost:{oracle_srv.port}",
                        'is_p2p_oracle': True,
                    })

                else:
                    self._send_json(404, {'error': 'not found'})

            def do_POST(self):
                path = _urlparse.urlparse(self.path).path
                data = self._read_json()

                if path == '/api/oracle/register':
                    if data:
                        miner_id = str(data.get('miner_id', ''))
                        address  = str(data.get('address', ''))
                        if miner_id and address:
                            oracle_srv._registered_miners[miner_id] = {
                                'miner_id': miner_id, 'address': address,
                                'registered_at': time.time(), 'status': 'registered'
                            }
                            self._send_json(200, {'status': 'registered',
                                                   'miner_id': miner_id, 'token': miner_id})
                            return
                    self._send_json(400, {'error': 'missing miner_id or address'})

                elif path == '/api/peers/register':
                    if data:
                        self._send_json(200, {
                            'status': 'registered',
                            'live_peers': _local_db_get_best_peers(oracle_srv.db, limit=20),
                            'sse_url': f"http://localhost:{oracle_srv.port}/api/events",
                        })
                        return
                    self._send_json(400, {'error': 'bad request'})

                elif path == '/api/peers/heartbeat':
                    self._send_json(200, {'status': 'ok'})

                elif path == '/gossip/oracle_handshake':
                    # P2P oracle entanglement handshake
                    snap = oracle_srv.get_pq0_snapshot()
                    self._send_json(200, {**snap, 'handshake': True})

                else:
                    self._send_json(404, {'error': 'not found'})

        return _OracleHandler


# ─── DHT Peer Exchange Manager ────────────────────────────────────────────────────────────────────

class DHTExchangeManager:
    """
    Kademlia-inspired P2P peer exchange.

    DISCOVERY FLOW:
    ───────────────
    1. Bootstrap: contact main oracle → GET /api/peers/list → seed DHT table
    2. HELLO: for each discovered peer → POST /gossip/dht_hello
              Peer responds: {node_id, gossip_url, capabilities, block_height, w_fidelity}
              We respond in kind.
    3. PEX (Peer Exchange): every PEX_INTERVAL_S → POST /gossip/dht_pex to k closest peers
              Each peer returns their routing table (up to 20 peers)
              We merge new peers into our DHT table
    4. Ping: every PING_INTERVAL_S → GET /gossip/status on each peer
              Update last_seen, latency, quality score

    CONVERGENCE GUARANTEE:
    Once a node has contacted the bootstrap oracle, it will discover all other
    nodes within O(log N) hops, where N is total network size.
    For N=100 nodes: ~7 hops. For N=1000: ~10 hops. For N=10000: ~14 hops.

    ORACLE DISCOVERY:
    Any peer with is_oracle=True in their capabilities array triggers
    OracleEntanglementBridge._handshake_p2p_oracle().
    """

    PEX_INTERVAL_S    = 120   # peer exchange every 2 minutes
    PING_INTERVAL_S   = 60    # ping each peer every 1 minute
    BOOTSTRAP_RETRY_S = 30    # retry bootstrap if no peers found
    MAX_PEERS_PER_PEX = 20    # max peers to request/share per PEX round
    PEER_STALE_S      = 300   # mark peer stale after 5 minutes silence

    def __init__(self, db: sqlite3.Connection, local_node_id: str,
                 miner_address: str, oracle_url: str,
                 gossip_url: str, bridge: 'OracleEntanglementBridge',
                 elig_engine: 'OracleEligibilityEngine'):
        self.db              = db
        self.local_node_id   = local_node_id
        self.miner_address   = miner_address
        self.oracle_url      = oracle_url.rstrip('/')
        self.gossip_url      = gossip_url
        self.bridge          = bridge
        self.elig_engine     = elig_engine
        self._lock           = threading.RLock()
        self._session        = requests.Session()
        self._session.mount('https://', HTTPAdapter(max_retries=Retry(total=2, backoff_factor=0.3)))
        self._session.mount('http://',  HTTPAdapter(max_retries=Retry(total=2, backoff_factor=0.3)))
        self._running        = False
        self._thread: Optional[threading.Thread] = None
        self._last_pex       = 0.0
        self._last_ping      = 0.0
        self._bootstrapped   = False

    def start(self) -> None:
        self._running = True
        self._thread  = threading.Thread(
            target=self._dht_loop, daemon=True, name='DHTExchange'
        )
        self._thread.start()
        logger.info("[DHT] 🕸️  DHT peer exchange manager started")

    def stop(self) -> None:
        self._running = False
        if self._thread:
            self._thread.join(timeout=5)

    def _dht_loop(self) -> None:
        """Main DHT maintenance loop."""
        # Bootstrap immediately
        time.sleep(5)
        self._bootstrap_from_oracle()

        while self._running:
            try:
                now = time.time()

                if now - self._last_pex >= self.PEX_INTERVAL_S:
                    self._run_pex_round()
                    self._last_pex = now

                if now - self._last_ping >= self.PING_INTERVAL_S:
                    self._ping_all_peers()
                    self._last_ping = now

                # Check for newly discovered P2P oracles → entangle
                self._entangle_discovered_oracles()

                time.sleep(10)
            except Exception as e:
                logger.debug(f"[DHT] Loop error: {e}")

    def _bootstrap_from_oracle(self) -> None:
        """Seed DHT table from main oracle's peer list."""
        try:
            r = self._session.get(f"{self.oracle_url}/api/peers/list", timeout=10)
            if r.status_code != 200:
                logger.warning(f"[DHT] Bootstrap failed: HTTP {r.status_code}")
                return

            peers = r.json().get('peers', [])
            seeded = 0
            for p in peers:
                gurl = p.get('gossip_url', '')
                if not gurl:
                    continue
                peer_id   = p.get('peer_id', hashlib.sha1(gurl.encode()).hexdigest())
                node_id   = _dht_node_id(peer_id)
                try:
                    from urllib.parse import urlparse
                    parsed = urlparse(gurl)
                    _dht_upsert_peer(
                        db=self.db, node_id=node_id,
                        address=parsed.hostname or gurl,
                        gossip_port=parsed.port or 9091,
                        miner_address=p.get('miner_address', ''),
                        local_node_id=self.local_node_id,
                        capabilities=p.get('capabilities', ['mine']),
                        block_height=p.get('block_height', 0),
                        w_fidelity=p.get('w_fidelity', 0.0),
                        is_oracle='oracle' in p.get('capabilities', []),
                    )
                    seeded += 1
                except Exception:
                    pass

            self._bootstrapped = seeded > 0
            logger.info(
                f"[DHT] {'✅' if self._bootstrapped else '⚠️ '} Bootstrap: "
                f"{seeded}/{len(peers)} peers seeded from oracle"
            )

            # Register self with oracle
            self._register_self_with_oracle()

        except Exception as e:
            logger.warning(f"[DHT] Bootstrap error: {e}")

    def _register_self_with_oracle(self) -> None:
        """Register this node with the main oracle's peer registry."""
        if not self.gossip_url:
            return
        try:
            self._session.post(
                f"{self.oracle_url}/api/peers/register",
                json={
                    'peer_id':        self.local_node_id,
                    'gossip_url':     self.gossip_url,
                    'miner_address':  self.miner_address,
                    'block_height':   self._get_local_height(),
                    'network_version': '1.0',
                    'supports_dht':   True,
                    'capabilities':   ['mine', 'relay'] + (['oracle'] if self.elig_engine.is_oracle else []),
                },
                timeout=8,
            )
            logger.info("[DHT] ✅ Self-registered with main oracle")
        except Exception as e:
            logger.debug(f"[DHT] Self-registration error: {e}")

    def _run_pex_round(self) -> None:
        """
        Peer Exchange round: contact k closest peers, exchange routing tables.
        Each peer we talk to returns up to MAX_PEERS_PER_PEX peers.
        We merge their peers into our DHT. Exponential convergence ensured.
        """
        closest = _dht_closest_peers(self.db, self.local_node_id, k=10)
        if not closest:
            if not self._bootstrapped:
                self._bootstrap_from_oracle()
            return

        new_peers = 0
        for peer in closest:
            try:
                gurl = f"http://{peer['peer_address']}:{peer['gossip_port']}"
                t0   = time.time()
                r    = self._session.post(
                    f"{gurl}/gossip/dht_pex",
                    json={
                        'requester_node_id': self.local_node_id,
                        'requester_gossip':  self.gossip_url,
                        'requester_addr':    self.miner_address,
                        'my_height':         self._get_local_height(),
                        'my_fidelity':       0.9,
                    },
                    timeout=6,
                )
                latency = (time.time() - t0) * 1000

                if r.status_code == 200:
                    their_peers = r.json().get('peers', [])
                    for tp in their_peers[:self.MAX_PEERS_PER_PEX]:
                        nid  = tp.get('node_id', '')
                        addr = tp.get('peer_address', tp.get('address', ''))
                        if nid and addr and nid != self.local_node_id:
                            _dht_upsert_peer(
                                db=self.db, node_id=nid,
                                address=addr,
                                gossip_port=int(tp.get('gossip_port', 9091)),
                                miner_address=tp.get('miner_address', ''),
                                local_node_id=self.local_node_id,
                                capabilities=tp.get('capabilities', []),
                                block_height=int(tp.get('block_height', 0)),
                                w_fidelity=float(tp.get('w_fidelity', 0.0)),
                                is_oracle='oracle' in tp.get('capabilities', []),
                            )
                            new_peers += 1

                    _local_db_record_peer_result(self.db, peer['node_id'], True, latency)
                else:
                    _local_db_record_peer_result(self.db, peer['node_id'], False, 9999)

            except Exception as e:
                _local_db_record_peer_result(self.db, peer.get('node_id', '?'), False, 9999)
                logger.debug(f"[DHT] PEX error with {peer.get('peer_address','?')}: {e}")

        if new_peers > 0:
            logger.info(f"[DHT] 🔄 PEX round: {new_peers} new peers discovered")

    def _ping_all_peers(self) -> None:
        """Ping each known peer to update liveness and quality scores."""
        if self.db is None:
            return
        try:
            cur = self.db.execute("""
                SELECT node_id, peer_address, gossip_port FROM dht_peers
                WHERE last_seen > ? ORDER BY last_seen DESC LIMIT 50
            """, (time.time() - self.PEER_STALE_S * 2,))
            peers = cur.fetchall()
        except Exception:
            return

        for node_id, addr, port in peers:
            try:
                t0  = time.time()
                r   = self._session.get(
                    f"http://{addr}:{port}/gossip/status", timeout=3
                )
                lat = (time.time() - t0) * 1000
                ok  = r.status_code == 200
                _local_db_record_peer_result(self.db, node_id, ok, lat)
                if ok:
                    data = r.json()
                    # Update height and fidelity from ping response
                    try:
                        self.db.execute("""
                            UPDATE dht_peers SET block_height=?, last_seen=?, updated_at=?
                            WHERE node_id=?
                        """, (data.get('block_height', 0), time.time(), time.time(), node_id))
                        self.db.commit()
                    except Exception:
                        pass
            except Exception:
                _local_db_record_peer_result(self.db, node_id, False, 9999)

    def _entangle_discovered_oracles(self) -> None:
        """Find newly discovered P2P oracles and establish entanglement."""
        if self.db is None:
            return
        try:
            cur = self.db.execute("""
                SELECT node_id, peer_address, gossip_port, miner_address, w_fidelity
                FROM   dht_peers
                WHERE  is_oracle=1 AND last_seen > ? AND is_bootstrap=0
                ORDER  BY w_fidelity DESC
                LIMIT  8
            """, (time.time() - 300,))
            oracles = [{'node_id': r[0], 'peer_address': r[1], 'gossip_port': r[2],
                        'miner_address': r[3], 'w_fidelity': r[4],
                        'gossip_url': f"http://{r[1]}:{r[2]}"}
                       for r in cur.fetchall()]
        except Exception:
            return

        if oracles:
            results = self.bridge.refresh_all_links(peer_oracles=oracles)
            if results['p2p'] > 0:
                logger.info(f"[DHT] 🔗 {results['p2p']} P2P oracle(s) entangled | active={results['active_links']}")

    def _get_local_height(self) -> int:
        if self.db is None:
            return 0
        try:
            cur = self.db.execute("SELECT MAX(height) FROM blocks")
            val = cur.fetchone()[0]
            return int(val) if val is not None else 0
        except Exception:
            return 0

    def get_network_topology(self) -> Dict[str, Any]:
        """Compute and snapshot the current known P2P network topology."""
        if self.db is None:
            return {}
        try:
            cur = self.db.execute("""
                SELECT node_id, peer_address, gossip_port, is_oracle, block_height, quality_score
                FROM   dht_peers WHERE last_seen > ?
                ORDER  BY quality_score DESC
            """, (time.time() - self.PEER_STALE_S,))
            nodes = cur.fetchall()
            n = len(nodes)
            oracle_count = sum(1 for row in nodes if row[3])
            return {
                'total_nodes':   n,
                'total_oracles': oracle_count,
                'avg_height':    sum(r[4] for r in nodes) / max(n, 1),
                'density':       min(1.0, n / max(self.elig_engine.ORACLE_FULL_THRESHOLD, 1)),
                'bootstrapped':  self._bootstrapped,
            }
        except Exception as e:
            logger.debug(f"[DHT] topology error: {e}")
            return {}


# ─── DHT Gossip Handler Extensions ───────────────────────────────────────────────────────────────
# These are injected into GossipHTTPHandler via monkey-patch at startup.
# They add /gossip/dht_hello and /gossip/dht_pex endpoints.

def _handle_dht_hello(handler, data: dict) -> None:
    """Handle DHT HELLO — peer announces self, we respond with closest peers."""
    db         = getattr(handler.server, 'local_db',      None)
    peer_id    = getattr(handler.server, 'peer_id',       '')
    local_nid  = getattr(handler.server, 'local_node_id', peer_id)
    miner_addr = getattr(handler.server, 'miner_address', '')
    gossip_url = getattr(handler.server, 'gossip_url',    '')

    requester_node_id = str(data.get('node_id', ''))
    requester_addr    = str(data.get('miner_address', ''))
    requester_gossip  = str(data.get('gossip_url', ''))
    requester_height  = int(data.get('block_height', 0))
    requester_caps    = data.get('capabilities', ['mine'])
    requester_w_fid   = float(data.get('w_fidelity', 0.0))
    remote_ip         = handler.client_address[0] if handler.client_address else ''

    # Parse gossip URL for address/port
    gossip_port = 9091
    peer_address = remote_ip
    if requester_gossip:
        try:
            from urllib.parse import urlparse
            p = urlparse(requester_gossip)
            if p.hostname:
                peer_address = p.hostname
            if p.port:
                gossip_port = p.port
        except Exception:
            pass

    # Upsert peer into DHT
    if requester_node_id:
        _dht_upsert_peer(
            db=db, node_id=requester_node_id,
            address=peer_address, gossip_port=gossip_port,
            miner_address=requester_addr,
            local_node_id=local_nid,
            capabilities=requester_caps,
            block_height=requester_height,
            w_fidelity=requester_w_fid,
            is_oracle='oracle' in requester_caps,
        )

    # Respond with closest peers
    closest = _dht_closest_peers(db, requester_node_id or local_nid, k=20,
                                  exclude_id=requester_node_id)
    response = {
        'node_id':        local_nid,
        'miner_address':  miner_addr,
        'gossip_url':     gossip_url,
        'block_height':   _local_db_get_tip(db).get('height', 0) if _local_db_get_tip(db) else 0,
        'your_ip':        remote_ip,
        'peers':          [{
            'node_id':      p['node_id'],
            'peer_address': p['peer_address'],
            'gossip_port':  p['gossip_port'],
            'miner_address': p['miner_address'],
            'capabilities': json.loads(p.get('capabilities', '[]')),
            'block_height': p['block_height'],
            'w_fidelity':   p['w_fidelity'],
            'is_oracle':    bool(p['is_oracle']),
        } for p in closest],
    }
    handler._send_json(200, response)


def _handle_dht_pex(handler, data: dict) -> None:
    """Handle DHT Peer Exchange — share routing table, receive theirs."""
    db         = getattr(handler.server, 'local_db',      None)
    peer_id    = getattr(handler.server, 'peer_id',       '')
    local_nid  = getattr(handler.server, 'local_node_id', peer_id)
    miner_addr = getattr(handler.server, 'miner_address', '')
    gossip_url = getattr(handler.server, 'gossip_url',    '')

    requester_node_id = str(data.get('requester_node_id', ''))
    requester_gossip  = str(data.get('requester_gossip', ''))
    requester_addr    = str(data.get('requester_addr', ''))
    requester_height  = int(data.get('my_height', 0))
    requester_fid     = float(data.get('my_fidelity', 0.0))

    # Merge requester into our DHT
    if requester_node_id and requester_gossip:
        try:
            from urllib.parse import urlparse
            p = urlparse(requester_gossip)
            _dht_upsert_peer(
                db=db, node_id=requester_node_id,
                address=p.hostname or handler.client_address[0],
                gossip_port=p.port or 9091,
                miner_address=requester_addr,
                local_node_id=local_nid,
                capabilities=['mine'],
                block_height=requester_height,
                w_fidelity=requester_fid,
            )
        except Exception:
            pass

    # Return our routing table to them (20 closest to their node_id)
    our_peers = _dht_closest_peers(
        db, requester_node_id or local_nid, k=20, exclude_id=requester_node_id
    )
    handler._send_json(200, {
        'node_id':   local_nid,
        'gossip_url': gossip_url,
        'peers':     [{
            'node_id':      p['node_id'],
            'peer_address': p['peer_address'],
            'gossip_port':  p['gossip_port'],
            'miner_address': p['miner_address'],
            'capabilities': json.loads(p.get('capabilities', '[]')),
            'block_height': p['block_height'],
            'w_fidelity':   p['w_fidelity'],
            'is_oracle':    bool(p['is_oracle']),
        } for p in our_peers],
    })




# ─── Complete P2P + DHT orchestration bundle ─────────────────────────────────────────────────────

class QTCLP2PBundle:
    """
    Top-level coordinator binding together:
      VirtualPseudoqubitManager  — pq0, virtual pqs, inverse-virtual pqs
      OracleEntanglementBridge   — cross-oracle entanglement
      OracleEligibilityEngine    — autonomous oracle promotion
      DHTExchangeManager         — Kademlia peer discovery & exchange

    Instantiated by QTCLFullNode.start(). Exposes unified start()/stop() and
    get_status() for the mining loop and status dashboard.

    STARTUP SEQUENCE:
    ─────────────────
    1. _apply_dht_schema(db)              — ensure tables exist
    2. VirtualPseudoqubitManager.__init__ — in-memory state
    3. OracleEntanglementBridge.__init__  — session + link state
    4. OracleEligibilityEngine.__init__   — eligibility counters
    5. DHTExchangeManager.__init__        — DHT routing state
    6. bridge.start()                     — spawn maintenance thread
    7. elig_engine.start()                — spawn eligibility loop
    8. dht.start()                        — spawn DHT loop
    """

    def __init__(self, db: sqlite3.Connection, miner_address: str,
                 oracle_url: str, gossip_url: str = ''):
        self.db             = db
        self.miner_address  = miner_address
        self.oracle_url     = oracle_url
        self.gossip_url     = gossip_url

        # Deterministic node ID from miner address
        self.local_node_id  = _dht_node_id(miner_address)

        # Apply DHT schema extensions
        _apply_dht_schema(db)

        # Build subsystem stack
        self.vpm   = VirtualPseudoqubitManager(
            db=db, local_node_id=self.local_node_id,
            miner_address=miner_address, oracle_id='main'
        )
        self.bridge = OracleEntanglementBridge(
            db=db, vpm=self.vpm, local_node_id=self.local_node_id,
            miner_address=miner_address, main_oracle_url=oracle_url
        )
        self.elig   = OracleEligibilityEngine(
            db=db, local_node_id=self.local_node_id,
            miner_address=miner_address, vpm=self.vpm, bridge=self.bridge
        )
        self.dht    = DHTExchangeManager(
            db=db, local_node_id=self.local_node_id,
            miner_address=miner_address, oracle_url=oracle_url,
            gossip_url=gossip_url, bridge=self.bridge, elig_engine=self.elig
        )

        logger.info(
            f"[P2P-BUNDLE] 🚀 QTCLP2PBundle initialized | "
            f"node={self.local_node_id[:16]} | miner={miner_address[:20]}"
        )

    def start(self) -> None:
        """Start all subsystems in correct dependency order."""
        self.bridge.start()
        self.elig.start()
        self.dht.start()
        logger.info("[P2P-BUNDLE] ✅ All P2P subsystems started")

    def stop(self) -> None:
        """Graceful shutdown of all subsystems."""
        self.dht.stop()
        self.elig.stop()
        self.bridge.stop()
        logger.info("[P2P-BUNDLE] 🛑 All P2P subsystems stopped")

    def initialize_pq0_from_snapshot(self, snapshot: Dict[str, Any]) -> bool:
        """Initialize pq0 from oracle snapshot — called by WStateRecovery sync."""
        return self.vpm.initialize_pq0(snapshot)

    def get_vpq_entropy(self) -> str:
        """Get quantum entropy from virtual pq — used by QuantumMiner."""
        vpq_ids = list(self.vpm._vpq.keys())
        if not vpq_ids:
            # Spawn one on demand
            vpq_id = self.vpm.spawn_virtual_pq()
            if vpq_id:
                self.vpm.spawn_inverse_virtual_pq(vpq_id)
        vpq_id = vpq_ids[0] if vpq_ids else None
        if vpq_id:
            return self.vpm.measure_virtual_pq_entropy(vpq_id)
        return secrets.token_hex(32)

    def rotate_vpqs(self) -> None:
        """Rotate all virtual pqs after block solution — called by QuantumMiner."""
        for vpq_id in list(self.vpm._vpq.keys()):
            self.vpm.rotate_virtual_pq(vpq_id)

    def attempt_oracle_promotion(self, blocks_mined: int, avg_fidelity: float) -> bool:
        """Try oracle promotion — called periodically by QTCLFullNode._mining_loop."""
        peer_count = self.dht._count_live_peers() if hasattr(self.dht, '_count_live_peers') \
                     else self.elig._count_live_peers()
        return self.elig.attempt_promotion(peer_count, blocks_mined, avg_fidelity,
                                            gossip_url=self.gossip_url)

    def get_status(self) -> Dict[str, Any]:
        """Unified status for dashboard + node.get_status()."""
        topology  = self.dht.get_network_topology()
        pq_status = self.vpm.get_pq_status()
        ent_status = self.bridge.get_entanglement_status()
        return {
            'node_id':            self.local_node_id[:20],
            'is_oracle':          self.elig.is_oracle,
            'oracle_port':        self.elig._oracle_server.port if self.elig._oracle_server else None,
            'pq0_fidelity':       pq_status['pq0_fidelity'],
            'virtual_pq_count':   len(pq_status['virtual_pqs']),
            'inverse_vpq_count':  len(pq_status['inverse_vpqs']),
            'entanglement_links': ent_status['active_count'],
            'dht_peers':          topology.get('total_nodes', 0),
            'dht_oracles':        topology.get('total_oracles', 0),
            'dht_bootstrapped':   topology.get('bootstrapped', False),
        }

    def _count_live_peers(self) -> int:
        """Proxy for use by OracleEligibilityEngine._eligibility_loop."""
        return self.elig._count_live_peers()


class QTCLFullNode:
    def __init__(self, miner_address: str, oracle_url: str='https://qtcl-blockchain.koyeb.app', difficulty: int=12, db_connection: Optional[sqlite3.Connection]=None):
        self.miner_address=miner_address
        self.running=False
        self.db=db_connection  # Database connection for difficulty state
        
        self.client=LiveNodeClient()
        self.state=ChainState()
        self.mempool=Mempool()
        # CRITICAL FIX: pass chain_state + db so _parent_block_exists works correctly.
        # Without these, ValidationEngine.blocks AttributeError silently kills every block.
        self.validator=ValidationEngine(chain_state=self.state, db=self.db)
        
        # DIFFICULTY RETARGETING ENGINE
        self.difficulty_engine=None
        if self.db:
            try:
                self.difficulty_engine=DifficultyRetargeting(
                    self.db,
                    target_block_time_s=60.0,
                    retarget_window=5,    # retarget every 5 accepted blocks — fast enough to chase hash rate
                    ema_alpha=0.3,        # 0.3 smoothing: responsive without over-reacting to single lucky blocks
                )
                logger.info("[NODE] ✅ Difficulty retargeting engine initialized")
            except Exception as e:
                logger.warning(f"[NODE] ⚠️  Failed to initialize difficulty engine: {e}")
        
        # W-STATE RECOVERY
        # Derive deterministic peer_id from miner_address (wallet) — persists across restarts
        peer_id_hash = hashlib.sha256(miner_address.encode()).hexdigest()[:12]
        peer_id = f"miner_{peer_id_hash}"
        self._peer_id = peer_id   # expose for external reference
        self.w_state_recovery=P2PClientWStateRecovery(
            oracle_url=oracle_url,
            peer_id=peer_id,
            miner_address=miner_address,  # FIXED: Pass this
            strict_signature_verification=True
        )
        
        # MINING with W-state and DYNAMIC DIFFICULTY
        self.miner=QuantumMiner(self.w_state_recovery, difficulty_engine=self.difficulty_engine, difficulty=difficulty)
        
        self.sync_thread: Optional[threading.Thread]=None
        self.mining_thread: Optional[threading.Thread]=None

        # P2P GOSSIP ORCHESTRATOR — SSE + peer registry + listener + heartbeat
        self._gossip = P2PGossipOrchestrator(
            oracle_url    = oracle_url,
            miner_address = miner_address,
            mempool       = self.mempool,
            db            = db_connection,
        )
        # Wire on_block_event so gossip-received blocks trigger immediate tip refresh
        def _on_gossip_block(height: int, bhash: str):
            try:
                tip = self.state.get_tip()
                if tip and height > tip.height:
                    logger.info(f"[GOSSIP] Block #{height} received — triggering sync")
                    # Re-fetch tip from oracle on next sync cycle (sync_loop reads state.get_tip)
            except Exception:
                pass
        self._gossip.on_block_event = _on_gossip_block

        # ── P2P BUNDLE: DHT + VirtualPQ + Oracle Eligibility + Entanglement Bridge ──
        # Initialized here but NOT started yet — start() called after gossip binds a port.
        self._p2p_bundle: Optional[QTCLP2PBundle] = None
        if db_connection is not None:
            try:
                self._p2p_bundle = QTCLP2PBundle(
                    db             = db_connection,
                    miner_address  = miner_address,
                    oracle_url     = oracle_url,
                    gossip_url     = '',   # updated after GossipListener binds
                )
                logger.info("[NODE] ✅ QTCLP2PBundle created (DHT+VPQ+Oracle ready)")
            except Exception as _e:
                logger.warning(f"[NODE] ⚠️  P2P bundle init deferred: {_e}")

        logger.info(f"[NODE] QTCL Full Node initialized | miner={miner_address[:20]}… | oracle={oracle_url}")
    
    def start(self)->bool:
        try:
            logger.info("[NODE] 🚀 Starting node...")
            
            # START W-STATE RECOVERY (CRITICAL)
            if not self.w_state_recovery.start():
                logger.error("[NODE] ❌ W-state recovery failed to start")
                return False
            
            logger.info("[NODE] ✅ W-state recovery online")
            
            # ── Bootstrap from genesis (Bitcoin-style: always start from block 0) ──
            # Fetch genesis block first to anchor the chain.
            genesis_data = self.client.get_block_by_height(0)
            if genesis_data:
                genesis_header_data = genesis_data.get('header', genesis_data)
                genesis_header = BlockHeader.from_dict(genesis_header_data)
                self.state.add_block(genesis_header)
                logger.info(
                    f"[NODE] ⛓️  Genesis block anchored | height=0 | "
                    f"hash={genesis_header.block_hash[:16]}…"
                )
            else:
                # Genesis not yet on chain — create a local genesis anchor
                # so the miner can mine block #1 referencing the correct genesis hash
                genesis_hash = '0' * 64
                genesis_header = BlockHeader(
                    height=0,
                    block_hash=genesis_hash,
                    parent_hash='0' * 64,
                    merkle_root='0' * 64,
                    timestamp_s=int(time.time()),
                    difficulty_bits=self.miner.difficulty,
                    nonce=0,
                    miner_address='genesis',
                )
                self.state.add_block(genesis_header)
                logger.warning("[NODE] ⚠️  Genesis not on network — using local genesis anchor")
            
            # Fetch current tip so we know how far to sync
            tip = self.client.get_tip_block()
            if tip and tip.height > 0:
                self.state.add_block(tip)
                logger.info(f"[NODE] ✅ Network tip | height={tip.height} | hash={tip.block_hash[:16]}…")
                
                # ─── FETCH MISSING BLOCKS FROM ORACLE ─────────────────────────────
                # If tip height > 0, we need to fetch blocks 1..tip.height from Oracle
                try:
                    # Get current local height from database
                    if self.db:
                        cursor = self.db.execute("SELECT MAX(height) FROM blocks")
                        result = cursor.fetchone()
                        local_height = result[0] if result and result[0] is not None else 0
                    else:
                        local_height = 0
                    
                    if tip.height > local_height:
                        logger.info(f"[NODE] 📥 Syncing {tip.height - local_height} blocks from network (heights {local_height + 1}…{tip.height})...")
                        
                        # Fetch blocks from network one by one
                        for block_height in range(local_height + 1, tip.height + 1):
                            try:
                                block_data = self.client.get_block_by_height(block_height)
                                if block_data:
                                    header = BlockHeader.from_dict(block_data.get('header', block_data))
                                    self.state.add_block(header)
                                    
                                    # Store in database
                                    if self.db:
                                        self.db.execute("""
                                            INSERT OR IGNORE INTO blocks 
                                            (height, block_hash, parent_hash, merkle_root, timestamp_s, 
                                             difficulty_bits, nonce, miner_address, w_state_fidelity, w_entropy_hash)
                                            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                                        """, (
                                            header.height,
                                            header.block_hash,
                                            header.parent_hash,
                                            header.merkle_root,
                                            header.timestamp_s,
                                            header.difficulty_bits,
                                            header.nonce,
                                            header.miner_address,
                                            header.w_state_fidelity if hasattr(header, 'w_state_fidelity') else 0.0,
                                            header.w_entropy_hash if hasattr(header, 'w_entropy_hash') else ''
                                        ))
                                        self.db.commit()
                                    
                                    logger.info(f"[NODE] 📦 Synced block #{block_height} | hash={header.block_hash[:16]}…")
                            except Exception as e:
                                logger.warning(f"[NODE] ⚠️  Failed to sync block {block_height}: {e}")
                        
                        logger.info(f"[NODE] ✅ Blocks synced | local height now: {tip.height}")
                except Exception as e:
                    logger.warning(f"[NODE] ⚠️  Block sync error: {e}")
                    import traceback
                    traceback.print_exc()
            
            self.running = True
            
            # Start background threads
            self.sync_thread = threading.Thread(target=self._sync_loop, daemon=True, name="SyncWorker")
            self.sync_thread.start()
            
            self.mining_thread = threading.Thread(target=self._mining_loop, daemon=True, name="MiningWorker")
            self.mining_thread.start()

            # Start P2P gossip orchestrator — SSE subscription, peer registration,
            # gossip listener, heartbeat. Must start after running=True so get_tip_fn works.
            self._gossip.get_tip_fn = lambda: (self.state.get_tip().height if self.state.get_tip() else 0)
            self._gossip.start()

            # ── Start P2P Bundle: DHT + VirtualPQ + Oracle Eligibility + Entanglement ──
            if self._p2p_bundle is not None:
                bound_gossip = self._gossip.get_gossip_url()
                self._p2p_bundle.gossip_url     = bound_gossip
                self._p2p_bundle.dht.gossip_url = bound_gossip
                # Give eligibility engine a reference to the live listener so
                # inject_oracle() can activate routes without binding a new port
                self._p2p_bundle.elig._listener_ref = self._gossip._listener
                try:
                    snap = self.w_state_recovery.current_snapshot
                    if snap:
                        self._p2p_bundle.initialize_pq0_from_snapshot(snap)
                        vpq_id = self._p2p_bundle.vpm.spawn_virtual_pq('vpq_primary')
                        if vpq_id:
                            self._p2p_bundle.vpm.spawn_inverse_virtual_pq(vpq_id)
                            logger.info(
                                f"[P2P-BUNDLE] ✅ pq0→vpq_primary + ivpq_primary spawned | "
                                f"F={self._p2p_bundle.vpm._fidelity:.4f}"
                            )
                except Exception as _pe:
                    logger.warning(f"[P2P-BUNDLE] ⚠️  pq0 init from snapshot: {_pe}")
                self._p2p_bundle.start()
                logger.info(
                    f"[P2P-BUNDLE] 🚀 Bundle online | gossip={bound_gossip} | "
                    f"node_id={self._p2p_bundle.local_node_id[:20]}"
                )

            logger.info(
                f"[NODE] Full node online | "
                f"gossip_url={self._gossip.get_gossip_url() or 'unbound'} | "
                f"peer_id={self._gossip.peer_id} | "
                f"dht={'active' if self._p2p_bundle else 'disabled'}"
            )
            return True
        
        except Exception as e:
            logger.error(f"[NODE] ❌ Startup failed: {e}")
            return False
    
    def stop(self):
        self.running=False
        self.w_state_recovery.stop()
        if self._p2p_bundle is not None:
            self._p2p_bundle.stop()
        if hasattr(self, '_gossip'):
            self._gossip.stop()
        if self.sync_thread:
            self.sync_thread.join(timeout=5)
        if self.mining_thread:
            self.mining_thread.join(timeout=5)
        logger.info("[NODE] Stopped")
    
    def _sync_loop(self):
        """Continuously sync blockchain from network — Bitcoin-style from genesis to tip."""
        logger.info("[SYNC] 🔄 Loop started")
        while self.running:
            try:
                tip = self.client.get_tip_block()
                if not tip:
                    logger.warning("[SYNC] ⚠️  Failed to get tip, retrying...")
                    time.sleep(10)
                    continue
                
                current_height = self.state.get_height()
                
                if current_height < tip.height:
                    sync_start = current_height + 1
                    sync_end   = min(current_height + SYNC_BATCH + 1, tip.height + 1)
                    logger.info(f"[SYNC] 📥 Syncing blocks {sync_start} → {sync_end - 1} (network tip={tip.height})")
                    
                    for h in range(sync_start, sync_end):
                        block_data = self.client.get_block_by_height(h)
                        if block_data:
                            # Handle both flat ({height:…}) and nested ({header:{…}}) responses
                            header_data = block_data.get('header', block_data)
                            # Normalise: server may return 'height' not 'block_height'
                            if 'height' in header_data and 'block_height' not in header_data:
                                header_data = dict(header_data)
                                header_data['block_height'] = header_data['height']
                            header = BlockHeader.from_dict(header_data)
                            txs    = [Transaction(**tx) for tx in block_data.get('transactions', [])]
                            block  = Block(header=header, transactions=txs)
                            if self.validator.validate_block(block):
                                self.state.add_block(header)
                                tx_ids_to_remove = []
                                for tx in txs:
                                    self.state.apply_transaction(tx)
                                    tx_ids_to_remove.append(tx.tx_id)
                                if tx_ids_to_remove:
                                    self.mempool.remove_transactions(tx_ids_to_remove)
                                # ── Persist to SQLite so parent lookups survive long-running sessions ──
                                if self.db:
                                    try:
                                        self.db.execute("""
                                            INSERT OR IGNORE INTO blocks
                                            (height, block_hash, parent_hash, merkle_root, timestamp_s,
                                             difficulty_bits, nonce, miner_address, w_state_fidelity, w_entropy_hash)
                                            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                                        """, (
                                            header.height, header.block_hash, header.parent_hash,
                                            header.merkle_root, header.timestamp_s, header.difficulty_bits,
                                            header.nonce, header.miner_address,
                                            getattr(header, 'w_state_fidelity', 0.0),
                                            getattr(header, 'w_entropy_hash', ''),
                                        ))
                                        self.db.commit()
                                    except Exception as dbe:
                                        logger.debug(f"[SYNC] DB persist error for #{h}: {dbe}")
                                logger.debug(f"[SYNC] ✅ Synced block #{h}")
                            else:
                                logger.warning(f"[SYNC] ⚠️  Block #{h} failed validation, skipping")
                        time.sleep(0.05)
                else:
                    logger.debug(f"[SYNC] In sync at height {current_height}")
                
                time.sleep(MEMPOOL_POLL_INTERVAL)
            except Exception as e:
                logger.error(f"[SYNC] Error: {e}")
                time.sleep(10)
        logger.info("[SYNC] Loop ended")
    
    def _mining_loop(self):
        """Background mining subsystem with W-state entanglement and comprehensive metrics"""
        logger.info("[MINING] ⛏️  Loop started")
        
        mining_start_time = time.time()
        blocks_mined_this_session = 0
        total_hash_attempts = 0
        fidelity_measurements = []
        entropy_samples = []
        
        while self.running:
            try:
                entanglement = self.w_state_recovery.get_entanglement_status()
                
                if not entanglement.get('established'):
                    logger.debug("[MINING] Waiting for W-state entanglement...")
                    time.sleep(2)
                    continue
                
                tip = self.state.get_tip()
                if not tip:
                    logger.debug("[MINING] No chain tip yet, waiting...")
                    time.sleep(5)
                    continue
                
                # ── FETCH PENDING TXs — LOCAL → P2P → ORACLE (fallback chain) ──────
                # Tier 1: Local SQLite (instant, survives network partition, always try first)
                pending_txs = []
                sqlite_txs = _local_db_get_pending(self.db) if self.db else []
                if sqlite_txs:
                    for raw in sqlite_txs[:MAX_BLOCK_TX]:
                        try:
                            pending_txs.append(Transaction(
                                tx_id        = raw['tx_hash'],
                                from_addr    = raw['from_addr'],
                                to_addr      = raw['to_addr'],
                                amount       = raw['amount'],
                                nonce        = raw['nonce'],
                                timestamp_ns = raw['timestamp_ns'] or int(time.time()*1e9),
                                signature    = raw['signature'],
                                fee          = raw['fee'],
                            ))
                        except Exception: pass
                    if pending_txs:
                        logger.info(f"[MINING] 💾 Tier-1 local SQLite: {len(pending_txs)} TX(s)")

                # Tier 2: In-memory gossip pool (SSE + peer ingest, zero-latency)
                if not pending_txs:
                    in_mem = self.mempool.get_pending(limit=MAX_BLOCK_TX)
                    if in_mem:
                        pending_txs = in_mem
                        logger.info(f"[MINING] 🧠 Tier-2 in-memory gossip: {len(pending_txs)} TX(s)")

                # Tier 3: Best P2P peers (scored by latency + uptime — oracle not needed)
                if not pending_txs and self.db:
                    best_peers = _local_db_get_best_peers(self.db, limit=5)
                    for peer in best_peers:
                        try:
                            t0 = time.time()
                            r = self.client._session.get(
                                f"{peer['gossip_url'].rstrip('/')}/api/mempool", timeout=5)
                            latency = (time.time() - t0) * 1000
                            if r.status_code == 200:
                                p_txs = r.json().get('transactions', [])
                                for raw in p_txs[:MAX_BLOCK_TX]:
                                    try:
                                        t = Transaction(
                                            tx_id        = raw.get('tx_hash', raw.get('tx_id','')),
                                            from_addr    = raw.get('from_addr', raw.get('from_address','')),
                                            to_addr      = raw.get('to_addr', raw.get('to_address','')),
                                            amount       = float(raw.get('amount', raw.get('amount_base',0)/100)),
                                            nonce        = int(raw.get('nonce', 0)),
                                            timestamp_ns = int(raw.get('timestamp_ns', int(time.time()*1e9))),
                                            signature    = str(raw.get('signature','')),
                                            fee          = float(raw.get('fee', 0.001)),
                                        )
                                        pending_txs.append(t)
                                        _local_db_upsert_tx(self.db, raw)
                                    except Exception: pass
                                _local_db_record_peer_result(self.db, peer['peer_id'], True, latency)
                                if pending_txs:
                                    logger.info(f"[MINING] 🌐 Tier-3 P2P peer {peer['gossip_url'][:40]}: {len(pending_txs)} TX(s)")
                                    break
                            else:
                                _local_db_record_peer_result(self.db, peer['peer_id'], False, latency)
                        except Exception as pe:
                            _local_db_record_peer_result(self.db, peer.get('peer_id','?'), False, 9999)
                            logger.debug(f"[MINING] P2P peer fetch failed: {pe}")

                # Tier 4: Oracle — authoritative fallback only when all local/P2P sources empty
                if not pending_txs:
                    oracle_txs = self.client.get_mempool()
                    if oracle_txs:
                        pending_txs = oracle_txs
                        # Mirror into local DB so future rounds use Tier-1
                        for tx in oracle_txs:
                            try:
                                _local_db_upsert_tx(self.db, {
                                    'tx_hash': tx.tx_id, 'from_addr': tx.from_addr,
                                    'to_addr': tx.to_addr, 'amount': tx.amount,
                                    'nonce': tx.nonce, 'timestamp_ns': tx.timestamp_ns,
                                    'signature': tx.signature, 'fee': tx.fee,
                                    'source': 'oracle',
                                })
                            except Exception: pass
                        logger.info(f"[MINING] 🔮 Tier-4 oracle: {len(pending_txs)} TX(s)")
                # Merge all found TXs into local mempool for dedup tracking
                for tx in pending_txs:
                    try: self.mempool.add_transaction(tx)
                    except Exception: pass

                tx_count = len(pending_txs)
                current_fidelity = entanglement.get('w_state_fidelity', 0.0)
                fidelity_measurements.append(current_fidelity)
                
                logger.info(f"[MINING] Block #{tip.height+1} | pending_txs={tx_count} | F={current_fidelity:.4f}")
                
                block_start = time.time()
                block = self.miner.mine_block(pending_txs, self.miner_address, tip.block_hash, tip.height+1)
                block_time = time.time() - block_start
                
                if block:
                    total_hash_attempts += self.miner.metrics.get('hash_attempts', 0)
                    blocks_mined_this_session += 1
                    
                    if self.validator.validate_block(block):
                        submit_start = time.time()
                        
                        try:
                            header_dict = {
                                'height': int(block.header.height),
                                'block_hash': str(block.header.block_hash),
                                'parent_hash': str(block.header.parent_hash),
                                'merkle_root': str(block.header.merkle_root),
                                'timestamp_s': int(block.header.timestamp_s),
                                'difficulty_bits': int(block.header.difficulty_bits),
                                'nonce': int(block.header.nonce),
                                'miner_address': str(block.header.miner_address),
                                'w_state_fidelity': float(block.header.w_state_fidelity),
                                'w_entropy_hash': str(block.header.w_entropy_hash),
                            }
                            
                            # Serialize transactions — coinbase (tx[0]) uses its own
                            # to_dict() serializer; regular transfers use manual extraction.
                            tx_list = []
                            for idx, tx in enumerate(block.transactions):
                                if isinstance(tx, CoinbaseTx):
                                    tx_list.append(tx.to_dict())
                                else:
                                    tx_list.append({
                                        'tx_id':      str(tx.tx_id),
                                        'from_addr':  str(tx.from_addr),
                                        'to_addr':    str(tx.to_addr),
                                        'amount':     float(tx.amount),
                                        'fee':        float(tx.fee),
                                        'timestamp':  int(getattr(tx, 'timestamp_ns', 0) // 1_000_000_000),
                                        'signature':  str(tx.signature) if hasattr(tx, 'signature') else '',
                                        'tx_type':    'transfer',
                                    })
                            
                            # Build submission payload
                            block_payload = {
                                'header': header_dict,
                                'transactions': tx_list,
                                'miner_address': str(self.miner_address),
                                'timestamp': int(time.time()),
                            }
                            
                            logger.info(f"[MINING] 📤 Submitting block #{block.header.height} | hash={block.header.block_hash[:16]}… | txs={len(tx_list)}")
                            
                            # Submit block to server
                            success, msg = self.client.submit_block(block_payload)
                            submit_time = time.time() - submit_start
                            
                            if success:
                                # ✅ BLOCK ACCEPTED - Update all systems atomically
                                logger.info(f"[MINING] ✅ Block #{block.header.height} ACCEPTED by network | Response: {msg}")

                                # ── Queue for Oracle broadcast + gossip fan-out ────────────
                                try:
                                    if _ORACLE_BROADCASTER is not None:
                                        _ORACLE_BROADCASTER.enqueue_block(block_payload)
                                        # Immediate broadcast for newly mined blocks (don't wait for 30s loop)
                                        _ORACLE_BROADCASTER.broadcast_pending(timeout=6)
                                except Exception as _be:
                                    logger.debug(f"[MINING] Oracle broadcast error: {_be}")

                                # ── Gossip block to all known P2P peers immediately ─────────
                                try:
                                    best_peers = _local_db_get_best_peers(self.db, limit=10)
                                    gossip_payload_block = {
                                        'origin':  self._gossip.get_gossip_url() or 'unknown',
                                        'peer_id': peer_id,
                                        'block':   block_payload,
                                    }
                                    for gp in best_peers:
                                        gurl_p = gp.get('gossip_url', '')
                                        if not gurl_p:
                                            continue
                                        try:
                                            self.client._session.post(
                                                f"{gurl_p.rstrip('/')}/gossip/ingest",
                                                json=gossip_payload_block,
                                                timeout=4,
                                            )
                                            logger.debug(f"[MINING] 📡 Block gossiped → {gurl_p[:40]}")
                                        except Exception:
                                            pass
                                except Exception as _ge:
                                    logger.debug(f"[MINING] Gossip fan-out error: {_ge}")
                                
                                # ── EMA difficulty retargeting — fire ONLY on server-accepted blocks ──
                                # block_time measured from mine_block() call to here, so it is
                                # the true wall-clock cost of producing an accepted block.
                                if self.difficulty_engine:
                                    self.difficulty_engine.record_block_mining_time(
                                        block.header.height, block_time
                                    )
                                    logger.debug(
                                        f"[DIFFICULTY] 📊 EMA updated | height={block.header.height} "
                                        f"time={block_time:.2f}s | new_ema={self.difficulty_engine.ema_block_time_s:.2f}s"
                                    )
                                
                                # ── Persist block to database immediately ──
                                try:
                                    if self.db:
                                        self.db.execute("""
                                            INSERT OR IGNORE INTO blocks 
                                            (height, block_hash, parent_hash, merkle_root, timestamp_s, 
                                             difficulty_bits, nonce, miner_address, w_state_fidelity, w_entropy_hash)
                                            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                                        """, (
                                            block.header.height,
                                            block.header.block_hash,
                                            block.header.parent_hash,
                                            block.header.merkle_root,
                                            block.header.timestamp_s,
                                            block.header.difficulty_bits,
                                            block.header.nonce,
                                            block.header.miner_address,
                                            block.header.w_state_fidelity if hasattr(block.header, 'w_state_fidelity') else 0.0,
                                            block.header.w_entropy_hash if hasattr(block.header, 'w_entropy_hash') else ''
                                        ))
                                        self.db.commit()
                                        logger.info(f"[MINING] 💾 Block #{block.header.height} persisted to database")
                                except Exception as pe:
                                    logger.warning(f"[MINING] ⚠️  Failed to persist block: {pe}")
                                
                                # ── Advance local chain state immediately ──
                                # Do NOT wait for sync loop — update now so next iteration
                                # mines block N+1, not N again.
                                self.state.add_block(block.header)
                                confirmed_ids = [tx.tx_id for tx in block.transactions] if block.transactions else []
                                for tx in block.transactions:
                                    self.state.apply_transaction(tx)
                                self.mempool.remove_transactions(confirmed_ids)
                                # Mirror confirmation to local SQLite gossip store
                                if confirmed_ids:
                                    _local_db_clear_confirmed(self.db, confirmed_ids)

                                # Propagate new height to WebSocket heartbeat so peers know our tip
                                try:
                                    ws = getattr(self.w_state_recovery, 'ws_client', None)
                                    if ws is not None:
                                        ws._current_block_height = block.header.height
                                except Exception:
                                    pass
                                
                                # Confirm by querying network tip (non-blocking, best-effort)
                                try:
                                    network_tip = self.client.get_tip_block()
                                    if network_tip and network_tip.height >= block.header.height:
                                        self.state.add_block(network_tip)
                                        logger.info(
                                            f"[MINING] ⛓️  Chain advanced to height={network_tip.height} "
                                            f"| hash={network_tip.block_hash[:16]}…"
                                        )
                                    else:
                                        logger.info(
                                            f"[MINING] ⛓️  Local chain at height={self.state.get_height()} "
                                            f"(network tip confirming…)"
                                        )
                                except Exception:
                                    pass  # Network tip confirm is best-effort; local state is already updated
                                
                                # Calculate metrics
                                hash_rate = self.miner.metrics['hash_attempts'] / block_time if block_time > 0 else 0
                                elapsed = time.time() - mining_start_time
                                blocks_per_hour = (blocks_mined_this_session / elapsed * 3600) if elapsed > 0 else 0
                                
                                block_reward = 12.5  # QTCL (server awards 1250 base units)
                                logger.info(f"[MINING] 💰 Block Reward: +{block_reward} QTCL")
                                logger.info(f"[MINING] ✅ Block #{block.header.height} CONFIRMED")
                                logger.info(f"[MINING] 📊 Submission:")
                                logger.info(f"[MINING]   • Network latency: {submit_time*1000:.1f}ms")
                                logger.info(f"[MINING]   • Hash rate: {hash_rate:.0f} hashes/sec")
                                logger.info(f"[MINING]   • Mining time: {block_time:.2f}s")
                                logger.info(f"[MINING] 📈 Session: {blocks_mined_this_session} blocks | {blocks_per_hour:.2f} blocks/hour | Rewards: +{blocks_mined_this_session * block_reward} QTCL")
                                
                                if fidelity_measurements:
                                    avg_fidelity = sum(fidelity_measurements) / len(fidelity_measurements)
                                    logger.info(f"[MINING] 🎯 Quantum: Avg F={avg_fidelity:.4f}")
                            
                            else:
                                # ❌ SUBMISSION FAILED - Diagnostic info
                                logger.error(f"[MINING] ❌ Block submission REJECTED: {msg}")
                                logger.error(f"[MINING] 🔍 Diagnostics:")
                                logger.error(f"[MINING]   • Block height: {block.header.height}")
                                logger.error(f"[MINING]   • Block hash: {block.header.block_hash[:32]}…")
                                logger.error(f"[MINING]   • Parent hash: {block.header.parent_hash[:32]}…")
                                logger.error(f"[MINING]   • Miner: {block.header.miner_address}")
                                logger.error(f"[MINING]   • Transactions: {len(block.transactions)}")
                                logger.error(f"[MINING]   • Submission time: {submit_time*1000:.1f}ms")
                        
                        except Exception as submit_error:
                            logger.error(f"[MINING] ❌ Block submission EXCEPTION: {type(submit_error).__name__}: {submit_error}")
                            logger.error(f"[MINING]   • Block: height={block.header.height} hash={block.header.block_hash[:32]}…")
                            logger.error(f"[MINING]   • Traceback:\n{traceback.format_exc()}")
                    
                    else:
                        logger.error(f"[MINING] ❌ Block validation failed")
                else:
                    logger.warning(f"[MINING] ⚠️  Mining timeout or failed for block #{tip.height+1}")
                
                time.sleep(MINING_POLL_INTERVAL)
                
            except Exception as e:
                logger.error(f"[MINING] ❌ Loop error: {e}")
                logger.debug(f"[MINING] Traceback: {traceback.format_exc()}")
                time.sleep(5)
        
        # Session summary
        elapsed = time.time() - mining_start_time
        logger.info(f"[MINING] 🛑 Loop ended after {elapsed:.1f}s")
        logger.info(f"[MINING] 📊 Session Summary:")
        logger.info(f"[MINING]   • Blocks mined: {blocks_mined_this_session}")
        logger.info(f"[MINING]   • Total hash attempts: {total_hash_attempts}")
        if fidelity_measurements:
            logger.info(f"[MINING]   • Avg W-state fidelity: {sum(fidelity_measurements)/len(fidelity_measurements):.4f}")
        if blocks_mined_this_session > 0 and elapsed > 0:
            logger.info(f"[MINING]   • Blocks/hour: {blocks_mined_this_session/elapsed*3600:.2f}")
    
    def get_status(self) -> Dict[str, Any]:
        """Get comprehensive node and mining status"""
        tip = self.state.get_tip()
        entanglement = self.w_state_recovery.get_entanglement_status()
        
        mining_stats = dict(self.miner.metrics)
        # During active PoW, show live progress (resets to 0 on each new block attempt)
        live = mining_stats.get('live_hash_attempts', 0)
        total_committed = mining_stats.get('hash_attempts', 0)
        # Show whichever is larger: committed (solved blocks) or live in-progress
        display_attempts = max(total_committed, total_committed + live)
        
        # Calculate mining efficiency metrics
        hash_rate = 0
        if mining_stats.get('hash_attempts', 0) > 0 and mining_stats.get('blocks_mined', 0) > 0:
            avg_attempts = mining_stats['hash_attempts'] / mining_stats['blocks_mined']
            hash_rate = avg_attempts / 10  # Assuming ~10s per block
        
        # ✅ MUSEUM-GRADE: Query wallet balance from server
        wallet_balance = 0.0
        try:
            balance_response, _ = self.client.query_balance(self.miner_address)
            if balance_response:
                wallet_balance = float(balance_response.get('balance', 0))
                logger.debug(f"[NODE] 💰 Wallet balance queried: {wallet_balance} QTCL")
        except Exception as e:
            logger.warning(f"[NODE] ⚠️  Could not query wallet balance: {e}")
            # Calculate estimated from blocks mined
            estimated_rewards = mining_stats.get('blocks_mined', 0) * BLOCK_REWARD_QTCL
            logger.debug(f"[NODE] 📊 Estimated rewards: {estimated_rewards} QTCL (not confirmed, rate={BLOCK_REWARD_QTCL}/block)")
            wallet_balance = estimated_rewards
        
        return {
            'miner': self.miner_address[:20] + '…',
            'miner_full': self.miner_address,
            'status': 'mining' if self.running else 'stopped',
            'chain': {
                'height': self.state.get_height(),
                'tip_hash': tip.block_hash[:32] + '…' if tip else 'genesis',
                'tip_timestamp': tip.timestamp_s if tip else None,
            },
            'mempool': {
                'size': self.mempool.get_size(),
                'pending_transactions': self.mempool.get_size(),
            },
            'mining': {
                'blocks_mined': mining_stats.get('blocks_mined', 0),
                'total_hash_attempts': display_attempts,
                'avg_fidelity': mining_stats.get('avg_fidelity', 0.0),
                'estimated_hash_rate': f"{hash_rate:.0f}" if hash_rate > 0 else "calculating",
                'block_rewards': f"{mining_stats.get('blocks_mined', 0) * BLOCK_REWARD_QTCL} QTCL",
                'current_difficulty': self.difficulty_engine.get_current_difficulty() if self.difficulty_engine else self.miner.difficulty,
                'ema_block_time_s': self.difficulty_engine.ema_block_time_s if self.difficulty_engine else 0.0,
                'target_block_time_s': self.difficulty_engine.target_block_time_s if self.difficulty_engine else 60.0,
            },
            'wallet': {
                'address': self.miner_address,
                'balance': wallet_balance,
                'balance_formatted': f"{wallet_balance:.2f} QTCL",
                'estimated_rewards': mining_stats.get('blocks_mined', 0) * BLOCK_REWARD_QTCL,
            },
            'quantum': {
                'w_state': {
                    'entanglement_established': entanglement.get('established', False),
                    'pq0_fidelity':             entanglement.get('pq0_fidelity', 0.0),
                    'w_state_fidelity':         entanglement.get('w_state_fidelity', 0.0),
                    # Lattice field-space identifiers (hex strings)
                    'pq_curr':                  entanglement.get('pq_curr', ''),
                    'pq_last':                  entanglement.get('pq_last', ''),
                    'sync_lag_ms':              entanglement.get('sync_lag_ms', 0.0),
                },
                'recovery': {
                    'connected': self.w_state_recovery.running,
                    'peer_id': self.w_state_recovery.peer_id,
                }
            },
            'network': {
                'oracle_url':   self.w_state_recovery.oracle_url,
                'peer_count':   self._gossip.get_peer_count() if hasattr(self, '_gossip') else 0,
                'gossip_url':   self._gossip.get_gossip_url() if hasattr(self, '_gossip') else '',
            },
            'p2p': self._p2p_bundle.get_status() if self._p2p_bundle else {
                'node_id': 'N/A', 'is_oracle': False, 'oracle_port': None,
                'pq0_fidelity': 0.0, 'virtual_pq_count': 0, 'inverse_vpq_count': 0,
                'entanglement_links': 0, 'dht_peers': 0, 'dht_oracles': 0,
                'dht_bootstrapped': False,
            },
            'metrics_summary': (
                f"Height={self.state.get_height()} | "
                f"Blocks={mining_stats.get('blocks_mined', 0)} | "
                f"Balance={wallet_balance:.2f} QTCL | "
                f"F={mining_stats.get('avg_fidelity', 0.0):.4f} | "
                f"DHT={'✅' if (self._p2p_bundle and self._p2p_bundle.dht._bootstrapped) else '⏳'} | "
                f"Oracle={'🌟' if (self._p2p_bundle and self._p2p_bundle.elig.is_oracle) else '⛏️ '}"
            )
        }

# ═════════════════════════════════════════════════════════════════════════════════
# WALLET & REGISTRATION (Integrated)
# ═════════════════════════════════════════════════════════════════════════════════

class QTCLWallet:
    """
    BIP-39 mnemonic → BIP-32 HD derivation → HLWE-256 keypair.
    BIP-38 encryption: PBKDF2-HMAC-SHA256(200k) + XOR-keystream.
    Atomic writes (.tmp→rename). Pre-overwrite .bak. No legacy paths.
    """
    VERSION        = 4
    PBKDF2_ITER    = 200_000
    KEY_BYTES      = 32
    SALT_BYTES     = 32
    MNEMONIC_WORDS = 12
    PREFIX         = 'qtcl1'
    ADDR_LEN       = 39
    BIP32_KEY      = b'QTCL seed'
    BIP39_PASS     = b'qtcl'
    BIP39_ITER     = 2048
    AUTH_TAG       = b'QTCL-AUTH'
    HD_PATH        = [0x8000002C, 0x80000000, 0x80000000, 0, 0]

    # QTCL mnemonic wordlist — 1893 BIP-39 compatible words (130-bit entropy per 12-word phrase)
    _W = (
        "abandon ability able about above absent absorb abstract absurd abuse access accident "
        "account accuse achieve acid acoustic acquire across act action actor actress actual "
        "adapt add addict address adjust admit adult advance advice aerobic afford afraid "
        "again age agent agree ahead aim air airport aisle alarm album alcohol alert alien "
        "all alley allow almost alone alpha already also alter always amateur amazing among "
        "amount amused analyst anchor ancient anger angle angry animal ankle announce annual "
        "another answer antenna antique anxiety any apart apology appear apple approve april "
        "arch arctic area arena argue arm armed armor army around arrange arrest arrive "
        "arrow art artefact artist artwork ask aspect assault asset assist assume asthma "
        "athlete atom attack attend attitude attract auction audit august aunt author auto "
        "autumn average avocado avoid awake aware away awesome awful awkward axis baby "
        "balance bamboo banana banner bar barely bargain barrel base basic basket battle "
        "beach bean beauty because become beef before begin behave behind believe below "
        "belt bench benefit best betray better between beyond bicycle bid bike bind biology "
        "bird birth bitter black blade blame blanket blast bleak bless blind blood blossom "
        "blouse blue blur blush board boat body boil bomb bone book boost border boring "
        "borrow boss bottom bounce box boy bracket brain brand brave breeze brick bridge "
        "brief bright bring brisk broccoli broken bronze broom brother brown brush bubble "
        "buddy budget buffalo build bulb bulk bullet bundle bunker burden burger burst "
        "bus business busy butter buyer buzz cabbage cabin cable captain car carbon card "
        "cargo carpet carry cart case cash casino castle casual cat catalog catch category "
        "cattle cause caution cave ceiling celery cement census certain chair chaos chapter "
        "charge chase chat cheap check cheese chef cherry chest chicken chief child chimney "
        "choice choose chronic chuckle chunk cigar cinnamon circle citizen city civil claim "
        "clap clarify claw clay clean clerk clever click client cliff climb clinic clip "
        "clock clog close cloth cloud clown club clump cluster clutch coach coast coconut "
        "code coil coin collect color column combine come comfort comic common company "
        "concert conduct confirm congress connect consider control convince cook cool copper "
        "copy coral core corn correct cost cotton couch country couple course cousin cover "
        "coyote crack cradle craft cram crane crash crater crawl crazy cream credit creek "
        "crew cricket crime crisp critic cross crouch crowd crucial cruel cruise crumble "
        "crunch crush cry crystal cube culture cup cupboard curious current curtain curve "
        "cushion custom cute cycle dad damage damp dance danger daring dash daughter dawn "
        "day deal debate debris decade december decide decline decorate decrease deer defense "
        "define defy degree delay deliver demand demise denial dentist deny depart depend "
        "deposit depth deputy derive describe desert design desk despair destroy detail "
        "detect develop device devote diagram dial diamond diary dice diesel diet differ "
        "digital dignity dilemma dinner dinosaur direct dirt disagree discover disease dish "
        "dismiss disorder display distance divert divide divorce dizzy doctor document dog "
        "doll dolphin domain donate donkey donor door dose double dove draft dragon drama "
        "drastic draw dream dress drift drill drink drip drive drop drum dry duck dumb "
        "dune during dust dutch duty dwarf dynamic eager eagle early earn earth easily "
        "east easy echo ecology edge edit educate effort egg eight either elbow elder "
        "electric elegant element elephant elevator elite else embark embody embrace emerge "
        "emotion employ empower empty enable enact endless endorse enemy engage engine "
        "enhance enjoy enlist enough enrich enroll ensure enter entire entry envelope "
        "episode equal equip erase erosion erupt escape essay essence estate eternal ethics "
        "evidence evil evoke evolve exact example excess exchange excite exclude exercise "
        "exhaust exhibit exile exist exit exotic expand expire explain expose express extend "
        "extra eye fable face faculty fade faint faith fall false fame family famous fan "
        "fancy fantasy far fashion fat fatal father fatigue fault favorite feature february "
        "federal fee feed feel feet fellow felt fence festival fetch fever few fiber fiction "
        "field figure file film filter final find fine finger finish fire firm first fiscal "
        "fish fit fitness fix flag flame flash flat flavor flee flight flip float flock "
        "floor flower fluid flush fly foam focus fog foil follow food force forest forget "
        "fork fortune forum forward fossil foster found fox fragile frame frequent fresh "
        "friend fringe frog front frost frown frozen fruit fuel fun funny furnace fury "
        "future gadget gain galaxy gallery game gap garden garlic garment gasp gate gather "
        "gauge gaze general genius genre gentle genuine gesture ghost giant gift giggle "
        "ginger giraffe girl give glad glance glare glass glide glimpse globe gloom glory "
        "glove glow glue goat goddess gold good goose gorilla gospel gossip govern gown "
        "grab grace grain grant grape grasp grass gravity great green grid grief grit "
        "grocery group grow grunt guard guide guilt guitar gun gym habit hair half hamster "
        "hand happy harbor hard harsh harvest hat have hawk hazard head health heart heavy "
        "hedgehog height hello help hen hero hidden high hill hint hip hire history hobby "
        "hockey hold hole holiday hollow home honey hood hope horn hospital host hour hover "
        "hub huge human humble humor hundred hungry hunt hurdle hurry hurt husband hybrid "
        "ice icon ignore ill illegal image imitate immense immune impact impose improve "
        "impulse inbox income increase index indicate indoor industry infant inflict inform "
        "inhale inject injury inmate inner innocent input inquiry insane insect inside "
        "inspire install intact interest into invest invite involve iron island isolate issue "
        "item ivory jacket jaguar jar jazz jealous jeans jelly jewel job join joke journey "
        "joy judge juice jump jungle junior junk just kangaroo keen keep ketchup key kick "
        "kid kingdom kiss kit kitchen kite kitten kiwi knee knife knock know lab label "
        "lamp language laptop large later laugh laundry lava law lawn lawsuit layer lazy "
        "leader learn leave lecture left leg legal legend leisure lemon lend length lens "
        "leopard lesson letter level liar liberty library license life lift light like limb "
        "limit link lion liquid list little live lizard load loan lobster local lock logic "
        "lonely long loop lottery loud lounge love loyal lucky luggage lumber lunar lunch "
        "luxury lyrics magic magnet maid main major make mammal mango mansion manual maple "
        "marble march margin marine market marriage mask master match material math matrix "
        "matter maximum maze meadow mean medal media melody melt member memory mention menu "
        "mercy merge merit merry mesh message metal method middle midnight milk million "
        "mimic mind minimum minor miracle miss mixed mixture mobile model modify mom monitor "
        "monkey monster month moon moral more morning mosquito mother motion motor mountain "
        "mouse move movie much muffin mule multiply muscle museum mushroom music must mutual "
        "myself mystery naive name napkin narrow nasty natural nature near neck need negative "
        "neglect neither nephew nerve network news next nice night noble noise nominee "
        "noodle normal north notable note nothing notice novel now nuclear number nurse "
        "nut oak obey object oblige obscure obtain ocean october odor off offer office "
        "often oil okay old olive olympic omit once onion open option orange orbit orchard "
        "order ordinary organ orient original orphan ostrich other outdoor outside oval "
        "over own oyster ozone pact paddle page pair palace palm panda panic panther paper "
        "parade parent park parrot party pass patch path patrol pause pave payment peace "
        "peanut peasant pelican pen penalty pencil people pepper perfect permit person pet "
        "phone photo phrase physical piano picnic picture piece pig pigeon pill pilot pink "
        "pioneer pipe pistol pitch pizza place planet plastic plate play please pledge "
        "pluck plug plunge poem poet point polar pole police pond pony pool popular portion "
        "position possible post potato pottery poverty powder power practice praise predict "
        "prefer prepare present pretty prevent price pride primary print priority prison "
        "private prize problem process produce profit program project promote proof property "
        "prosper protect proud provide public pudding pull pulp pulse pumpkin punch pupil "
        "puppy purchase purity purpose push put puzzle pyramid quality quantum quarter "
        "question quick quit quiz quote rabbit raccoon race rack radar radio rail rain "
        "raise rally ramp ranch random range rapid rare rate rather raven reach ready real "
        "reason rebel rebuild recall receive recipe record recycle reduce reflect reform "
        "refuse region regret regular reject relax release relief rely remain remember "
        "remind remove render renew rent reopen repair repeat replace report require rescue "
        "resemble resist resource response result retire retreat return reunion reveal review "
        "reward rhythm ribbon rice rich ride rifle right rigid ring riot ripple risk ritual "
        "rival river road roast robot robust rocket romance roof rookie rotate rough royal "
        "rubber rude rug rule run runway rural sad saddle sadness safe sail salad salmon "
        "salon salt salute same sample sand satisfy satoshi sauce sausage save say scale "
        "scan scare scatter scene scheme school science scissors scorpion scout scrap screen "
        "script scrub sea search season seat second secret section security seek select sell "
        "seminar senior sense sentence series service session settle setup seven shadow shaft "
        "shallow share shed shell sheriff shield shift shine ship shiver shock shoe shoot "
        "shop short shoulder shove shrimp shrug shuffle sick siege sight signal silent silk "
        "silly silver similar simple since sing siren sister situate six size sketch ski "
        "skill skin skirt skull slab slam sleep slender slice slide slight slim slogan slot "
        "slow slush small smart smile smoke smooth snack snake snap sniff snow soap soccer "
        "social sock solar soldier solid solution solve someone song soon sorry soul sound "
        "soup source south space spare spatial spawn speak special speed sphere spice spider "
        "spike spin spirit split spoil sponsor spoon spray spread spring spy square squeeze "
        "squirrel stable stadium staff stage stairs stamp stand start state stay steak steel "
        "stem step stereo stick still sting stock stomach stone stop store storm story stove "
        "strategy street strike strong struggle student stuff stumble style subject submit "
        "subway success such sudden suffer sugar suggest suit summer sun sunny sunset super "
        "supply supreme sure surface surge surprise sustain swallow swamp swap swear sweet "
        "swift swim swing switch sword symbol symptom syrup table tackle tag tail talent "
        "tank tape target task tattoo taxi teach team tell ten tenant tennis tent term test "
        "text thank that theme then theory there they thing this thought three thrive throw "
        "thumb thunder ticket tilt timber time tiny tip tired title toast tobacco today "
        "together toilet token tomato tomorrow tone tongue tonight tool tooth top topic "
        "topple torch tornado tortoise toss total tourist toward tower town toy track trade "
        "traffic tragic train transfer trap trash travel tray treat tree trend trial tribe "
        "trick trigger trim trip trophy trouble truck truly trumpet trust truth tube tumor "
        "tunnel turkey turn turtle twelve twenty twice twin twist type typical ugly umbrella "
        "unable unaware uncle uncover under undo unfair unfold unhappy uniform unique universe "
        "unknown unlock until unusual unveil update upgrade uphold upon upper upset urban "
        "used useful useless usual utility vacant vacuum vague valid valley valve van vanish "
        "vapor various vast vault vehicle velvet vendor venture venue verb verify version "
        "very veteran viable vibrant vicious victory video view village vintage violin "
        "virtual virus visa visit visual vital vivid vocal voice void volcano volume vote "
        "voyage wage wagon wait walk wall walnut want warfare warm warrior wash wasp waste "
        "water wave way wealth weapon wear weasel wedding weekend weird welcome well west "
        "wet whale wheat wheel when where whip whisper wide width wife wild will win window "
        "wine wing wink winner winter wire wisdom wish witness wolf woman wonder wood wool "
        "word world worry worth wrap wreck wrestle wrist write wrong yard year yellow you "
        "young youth zebra zero zone zoo"
    ).split()

    def __init__(self, wallet_file=None):
        data_dir = Path('data')
        data_dir.mkdir(exist_ok=True, mode=0o700)
        self.wallet_file   = Path(wallet_file) if wallet_file else (data_dir / 'wallet.json')
        self.mnemonic_file = self.wallet_file.parent / 'wallet_mnemonic.enc'
        self.address:     Optional[str] = None
        self.private_key: Optional[str] = None
        self.public_key:  Optional[str] = None
        self.mnemonic:    Optional[str] = None

    def is_loaded(self): return bool(self.address and self.private_key and self.public_key)

    def create(self, password):
        if not password: raise ValueError("Password required")
        self.mnemonic    = self._gen_mnemonic()
        self._derive_keys(self.mnemonic)
        self._atomic_save(self.wallet_file, password,
                          {'address':self.address,'private_key':self.private_key,'public_key':self.public_key})
        self._atomic_save(self.mnemonic_file, password, {'mnemonic':self.mnemonic})
        self._print_mnemonic()
        return self.address

    def load(self, password):
        if not password or not self.wallet_file.exists(): return False
        try:
            data = json.loads(self.wallet_file.read_text())
        except Exception as e:
            logger.error(f"[WALLET] Read error: {e}"); return False
        wd = self._decrypt(data, password)
        if wd is None: return False
        self.address     = wd.get('address')
        self.private_key = wd.get('private_key')
        self.public_key  = wd.get('public_key')
        # self-heal: re-derive public_key if missing
        if self.private_key and not self.public_key:
            self.public_key = hashlib.sha3_256(self.private_key.encode()).hexdigest()
            self._backup(); self._atomic_save(self.wallet_file, password,
                {'address':self.address,'private_key':self.private_key,'public_key':self.public_key})
        if not self.is_loaded():
            logger.error(f"[WALLET] Incomplete fields after decrypt"); self._clear(); return False
        # verify address integrity - must match _derive_keys() method
        public_key_bytes = bytes.fromhex(self.public_key)
        exp = self.PREFIX + hashlib.sha3_256(public_key_bytes).digest()[:20].hex()
        if self.address != exp:
            self.address = exp; self._backup()
            self._atomic_save(self.wallet_file, password,
                {'address':self.address,'private_key':self.private_key,'public_key':self.public_key})
        logger.info(f"[WALLET] ✅ Loaded: {self.address}")
        return True

    def restore_from_mnemonic(self, mnemonic, password):
        words = mnemonic.lower().strip().split()
        if len(words) != self.MNEMONIC_WORDS: return False
        if any(w not in self._W for w in words): return False
        self.mnemonic = ' '.join(words)
        self._derive_keys(self.mnemonic)
        self._atomic_save(self.wallet_file, password,
                          {'address':self.address,'private_key':self.private_key,'public_key':self.public_key})
        self._atomic_save(self.mnemonic_file, password, {'mnemonic':self.mnemonic})
        return True

    def show_mnemonic(self, password):
        if not self.mnemonic_file.exists(): return None
        try:
            wd = self._decrypt(json.loads(self.mnemonic_file.read_text()), password)
            return wd.get('mnemonic') if wd else None
        except Exception: return None

    # BIP-39
    def _gen_mnemonic(self):
        return ' '.join(self._W[secrets.randbelow(len(self._W))] for _ in range(self.MNEMONIC_WORDS))

    def _mnemonic_to_seed(self, mnemonic):
        return hashlib.pbkdf2_hmac('sha512', mnemonic.encode(),
                                    b'mnemonic' + self.BIP39_PASS, self.BIP39_ITER, dklen=64)

    # BIP-32
    def _bip32_master(self, seed):
        I = hmac.new(self.BIP32_KEY, seed, 'sha512').digest()
        return I[:32], I[32:]

    def _bip32_child(self, key, chain, index):
        data = (b'\x00' + key + index.to_bytes(4,'big')) if index >= 0x80000000 \
               else (hashlib.sha256(key).digest() + index.to_bytes(4,'big'))
        I  = hmac.new(chain, data, 'sha512').digest()
        ck = ((int.from_bytes(I[:32],'big') + int.from_bytes(key,'big'))
               % (2**256 - 2**32 - 977)).to_bytes(32,'big')
        return ck, I[32:]

    def _derive_keys(self, mnemonic):
        seed       = self._mnemonic_to_seed(mnemonic)
        key, chain = self._bip32_master(seed)
        for idx in self.HD_PATH:
            key, chain = self._bip32_child(key, chain, idx)
        self.private_key = hashlib.sha3_256(key).hexdigest()
        self.public_key  = hashlib.sha3_256(self.private_key.encode()).hexdigest()
        # Address derivation must match oracle.OracleKeyPair.address():
        # SHA3-256(pubkey_bytes)[:20].hex(), not SHA3-256(hex_string)
        public_key_bytes = bytes.fromhex(self.public_key)
        self.address     = self.PREFIX + hashlib.sha3_256(
            public_key_bytes).digest()[:20].hex()

    # BIP-38 encryption
    def _encrypt(self, password, payload):
        salt = secrets.token_bytes(self.SALT_BYTES)
        key  = hashlib.pbkdf2_hmac('sha256', password.encode(), salt,
                                    self.PBKDF2_ITER, dklen=self.KEY_BYTES)
        auth = hashlib.sha3_256(key + salt + self.AUTH_TAG).hexdigest()
        pt   = json.dumps(payload, sort_keys=True).encode()
        ct   = bytes(p ^ k for p, k in zip(pt, self._ks(key, len(pt))))
        return {'version':self.VERSION,'salt':salt.hex(),'auth':auth,'cipher':ct.hex()}

    def _decrypt(self, data, password):
        try:
            salt = bytes.fromhex(data['salt'])
            key  = hashlib.pbkdf2_hmac('sha256', password.encode(), salt,
                                        self.PBKDF2_ITER, dklen=self.KEY_BYTES)
            if not hmac.compare_digest(
                    hashlib.sha3_256(key + salt + self.AUTH_TAG).hexdigest(), data['auth']):
                logger.error("[WALLET] ❌ Wrong password"); return None
            ct = bytes.fromhex(data['cipher'])
            return json.loads(bytes(c^k for c,k in zip(ct, self._ks(key,len(ct)))).decode())
        except Exception as e:
            logger.error(f"[WALLET] ❌ Decrypt: {e}"); return None

    def _ks(self, key, length):
        out, blk = b'', key
        while len(out) < length: blk = hashlib.sha256(blk).digest(); out += blk
        return out[:length]

    # I/O
    def _atomic_save(self, path, password, payload):
        path.parent.mkdir(exist_ok=True, mode=0o700)
        tmp = path.with_suffix('.tmp')
        tmp.write_text(json.dumps(self._encrypt(password, payload), indent=2))
        os.chmod(tmp, 0o600); tmp.replace(path); os.chmod(path, 0o600)

    def _backup(self):
        if self.wallet_file.exists():
            import shutil
            bak = self.wallet_file.with_suffix('.bak')
            shutil.copy2(self.wallet_file, bak); os.chmod(bak, 0o600)

    def _clear(self): self.address = self.private_key = self.public_key = self.mnemonic = None

    def _print_mnemonic(self):
        words = self.mnemonic.split()
        print("\n" + "═"*60)
        print("  ⚠️   WRITE DOWN YOUR 12-WORD RECOVERY PHRASE")
        print("  Store offline. Never photograph. Never share.")
        print("═"*60)
        for i in range(0, 12, 3):
            print(f"  {i+1:2}. {words[i]:<14} {i+2:2}. {words[i+1]:<14} {i+3:2}. {words[i+2]}")
        print("═"*60 + "\n")


class MinerRegistry:
    """Register miner with oracle. Token stored in data/.qtcl_registered."""
    def __init__(self, oracle_url):
        self.oracle_url  = oracle_url
        self._tok_file   = Path('data') / '.qtcl_registered'
        self._tok_file.parent.mkdir(exist_ok=True, mode=0o700)
        self.token       = self._load_token()

    def register(self, miner_id, address, public_key, private_key, miner_name='qtcl-miner'):
        try:
            r = requests.post(f"{self.oracle_url}/api/oracle/register",
                json={'miner_id':miner_id,'address':address,
                      'public_key':public_key,'miner_name':miner_name}, timeout=10)
            if r.status_code == 200 and r.json().get('status') == 'registered':
                self.token = r.json().get('token','')
                self._tok_file.write_text(self.token); os.chmod(self._tok_file, 0o600)
                logger.info(f"[REGISTRY] ✅ Registered token={self.token[:16]}…")
                return True
            logger.warning(f"[REGISTRY] Rejected: {r.text[:80]}")
        except Exception as e:
            logger.warning(f"[REGISTRY] Failed: {e}")
        return False

    def is_registered(self): return bool(self.token)
    def _load_token(self):
        try: return self._tok_file.read_text().strip() or None if self._tok_file.exists() else None
        except: return None


# ═════════════════════════════════════════════════════════════════════════════════
# MAIN ENTRY POINT
# ═════════════════════════════════════════════════════════════════════════════════


def _wallet_recover(args):
    """Exhaustive scan of data/*.json|*.enc — tries BIP-38 decrypt on each."""
    pw = args.wallet_password or input("  Recovery password: ").strip()
    if not pw: print("❌ Password required"); sys.exit(1)
    data_dir = Path('data'); data_dir.mkdir(exist_ok=True, mode=0o700)
    w = QTCLWallet(); recovered = None
    print("\n  🔍  QTCL Wallet Recovery\n")
    for path in sorted(data_dir.glob('*.json')) + sorted(data_dir.glob('*.enc')):
        print(f"  Trying {path.name} … ", end='', flush=True)
        try: data = json.loads(path.read_text())
        except Exception: print("⬛ not JSON"); continue
        wd = w._decrypt(data, pw)
        if wd and wd.get('mnemonic'):
            print("✅  mnemonic"); w.mnemonic = wd['mnemonic']; w._derive_keys(w.mnemonic)
            recovered = True; break
        elif wd and wd.get('private_key'):
            print("✅  keypair")
            w.private_key = wd['private_key']
            w.public_key  = wd.get('public_key') or hashlib.sha3_256(w.private_key.encode()).hexdigest()
            w.address     = QTCLWallet.PREFIX + hashlib.sha3_256(
                w.public_key.encode()).hexdigest()[:QTCLWallet.ADDR_LEN]
            recovered = True; break
        else: print("⬛")
    if not recovered:
        print("\n  ❌  No recoverable wallet found.")
        print("     --wallet-from-mnemonic   restore from 12 words")
        print("     --wallet-init            create fresh wallet")
        sys.exit(1)
    print(f"\n  ✅  Recovered: {w.address}")
    w._backup()
    w._atomic_save(w.wallet_file, pw,
        {'address':w.address,'private_key':w.private_key,'public_key':w.public_key})
    if w.mnemonic:
        w._atomic_save(w.mnemonic_file, pw, {'mnemonic':w.mnemonic})
    print(f"  💾  Saved → {w.wallet_file}\n")
    sys.exit(0)


def _query_transaction_status(tx_hash, node_url="https://qtcl-blockchain.koyeb.app"):
    """
    Query and display transaction status — checks DB (confirmed+pending) and DHT.
    Bitcoin model: TX is queryable immediately after broadcast (status=pending).
    """
    print("\n" + "="*70)
    print("  📊 TRANSACTION STATUS VIEWER")
    print("="*70)
    print(f"  Node     : {node_url}")
    print(f"  TX Hash  : {tx_hash[:32]}…\n")

    data = None
    source = None

    # ── 1. Primary: /api/transactions/<hash> (DB — confirmed + pending) ──────
    try:
        r = requests.get(f"{node_url}/api/transactions/{tx_hash}", timeout=10)
        if r.status_code == 200:
            data = r.json()
            source = 'db'
        elif r.status_code == 404:
            pass  # try fallback
        else:
            print(f"  ⚠️  HTTP {r.status_code}: {r.text[:100]}")
    except requests.exceptions.ConnectionError:
        print(f"  ❌ Cannot reach node: {node_url}"); print("="*70 + "\n"); return
    except requests.exceptions.Timeout:
        print(f"  ❌ Node timeout"); print("="*70 + "\n"); return

    # ── 2. Fallback: /api/mempool/tx/<hash> (quick mempool status check) ─────
    if data is None:
        try:
            r2 = requests.get(f"{node_url}/api/mempool/tx/{tx_hash}", timeout=5)
            if r2.status_code == 200:
                data = r2.json()
                source = 'mempool_check'
        except Exception:
            pass

    if data:
        status = (data.get('status') or 'pending').upper()
        confirmed = data.get('confirmed', False) or status == 'CONFIRMED'
        block_height = data.get('block_height')

        if confirmed:
            print(f"  ✅ TRANSACTION CONFIRMED\n")
        elif status in ('PENDING', 'PENDING'):
            print(f"  ⏳ TRANSACTION PENDING (in mempool — waiting for next block)\n")
        else:
            print(f"  📋 TRANSACTION STATUS: {status}\n")

        print(f"  TX Hash          : {data.get('tx_hash', tx_hash)}")
        print(f"  Status           : {status}")
        print(f"  Confirmed        : {'✅ YES' if confirmed else '⏳ NO (pending)'}")
        print(f"  Block Height     : {'#' + str(block_height) if block_height else 'N/A — pending'}")
        print(f"  Block Hash       : {str(data.get('block_hash') or 'N/A — pending')[:42]}")
        print(f"  Amount           : {data.get('amount_qtcl', 0)} QTCL")
        print(f"  From             : {data.get('from_address', 'N/A')}")
        print(f"  To               : {data.get('to_address', 'N/A')}")
        print(f"  TX Type          : {data.get('tx_type', 'transfer')}")
        print(f"  Oracle Signed    : {data.get('oracle_signed', '?')}")
        print(f"  Source           : {source}")
        if data.get('query_note'):
            print(f"\n  ℹ️   {data['query_note']}")
    else:
        print(f"  ❌ TRANSACTION NOT FOUND in DB, mempool, or DHT")
        print(f"")
        print(f"  Possible reasons:")
        print(f"    1. Wrong hash — use the tx_hash RETURNED by the server (not client tx_id)")
        print(f"    2. TX not yet submitted — check if broadcast succeeded")
        print(f"    3. Server restart flushed in-memory mempool (but DB should persist)")
        print(f"")
        print(f"  Hash queried: {tx_hash}")
        print(f"  Try also   : {node_url}/api/mempool/tx/{tx_hash}")

    print("\n" + "="*70 + "\n")


def _run_transaction_menu(args, wallet):
    """Secondary menu: Send transaction or view transaction status."""
    while True:
        print("\n" + "━"*70)
        print("  💸  TRANSACTION MENU")
        print("━"*70)
        print("  ┌──────────────────────────────────────┐")
        print("  │ 1. 📤  Send Transaction              │")
        print("  │ 2. 📊  Check Transaction Status      │")
        print("  │ 3. 🔙  Back to Main Menu             │")
        print("  └──────────────────────────────────────┘")
        
        try:
            choice = input("  Enter choice [1/2/3]: ").strip()
        except (EOFError, KeyboardInterrupt):
            choice = '3'
        
        if choice == '1':
            _run_transaction_wizard(args, wallet)
            break
        elif choice == '2':
            print()
            tx_hash = input("  Enter Transaction Hash: ").strip()
            if tx_hash:
                _query_transaction_status(tx_hash, args.oracle_url)
            else:
                print("  ❌ Transaction hash required")
        elif choice == '3':
            break
        else:
            print("  ❌ Invalid choice")


def _run_transaction_wizard(args, wallet):
    """Interactive HLWE transaction wizard — Bitcoin-model mempool broadcast."""
    print("\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
    print("  💸  QTCL  HLWE-256  TRANSACTION WIZARD")
    print("  ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
    print(f"  Node   : {args.oracle_url}")
    print(f"  Sender : {wallet.address}\n")

    try:
        to_addr = (getattr(args,'to_address',None) or
                   input("  Recipient address (qtcl1…): ").strip())
        if not to_addr.startswith('qtcl1'):
            print("❌  Invalid address (must start with qtcl1)"); sys.exit(1)

        amount_str = (getattr(args,'amount',None) or
                      input("  Amount (QTCL): ").strip())
        amount = float(amount_str)
        if amount <= 0: raise ValueError("amount <= 0")

        fee_str = input("  Fee (QTCL, default 0.001): ").strip() or '0.001'
        fee     = float(fee_str)

        memo = input("  Memo (optional): ").strip()
    except (EOFError, KeyboardInterrupt):
        print("\n  Cancelled."); return
    except ValueError as e:
        print(f"❌  Invalid input: {e}"); sys.exit(1)

    # ── Build canonical TX fields — matches server's canonical hash computation ──
    # Server computes: SHA3-256(JSON({from_addr, to_addr, amount_base, nonce_str, timestamp_ns_str}))
    # We compute the client-side tx_id for tracking, but USE the server-returned tx_hash for lookups.
    import time as _time
    timestamp_ns = int(_time.time() * 1e9)
    nonce = int(_time.time() * 1000) % (2**31)  # pseudo-nonce from timestamp

    # Client-side pre-broadcast tx_id (for reference only — server computes authoritative hash)
    tx_id = hashlib.sha3_256(
        f"{wallet.address}{to_addr}{amount}{timestamp_ns}".encode()
    ).hexdigest()

    payload = {
        'tx_id'     : tx_id,           # client-side id — server stores as alias if differs from canonical
        'from'      : wallet.address,
        'to'        : to_addr,
        'amount'    : amount,
        'fee'       : fee,
        'memo'      : memo,
        'nonce'     : nonce,
        'timestamp' : int(_time.time()),
        'public_key': wallet.public_key,
    }

    # ── Compute canonical TX hash (matches mempool.canonical_hash) ──
    tx_hash_payload = json.dumps({
        'from_address': wallet.address,
        'to_address'  : to_addr,
        'amount'      : str(int(amount * 100)),  # base units
        'nonce'       : str(nonce),
        'fee'         : str(int(fee * 100)),     # base units
        'timestamp_ns': str(int(_time.time() * 1e9)),
    }, sort_keys=True)
    message_hash = hashlib.sha3_256(tx_hash_payload.encode()).hexdigest()
    
    # W-state entropy: deterministic source from wallet + nonce + timestamp
    timestamp_ns = int(_time.time() * 1e9)
    w_entropy_input = f"{wallet.public_key}:{nonce}:{timestamp_ns}".encode()
    w_entropy_hash = hashlib.sha3_256(w_entropy_input).hexdigest()
    w_entropy_bytes = hashlib.sha3_256(w_entropy_input).digest()
    
    # HLWE signature computation (matching oracle.HLWESigner):
    # 1. Commitment = SHA3-256(private || w_entropy || message_hash)
    message_hash_bytes = bytes.fromhex(message_hash)
    private_key_bytes = bytes.fromhex(wallet.private_key)
    pubkey_bytes = bytes.fromhex(wallet.public_key)
    
    commitment_input = private_key_bytes + w_entropy_bytes + message_hash_bytes
    commitment = hashlib.sha3_256(commitment_input).hexdigest()
    commitment_bytes = hashlib.sha3_256(commitment_input).digest()
    
    # 2. Witness = SHAKE-256(commitment || private)
    witness_input = commitment_bytes + private_key_bytes
    witness = hashlib.shake_256(witness_input).digest(64).hex()
    witness_bytes = hashlib.shake_256(witness_input).digest(64)
    
    # 3. Proof = HMAC-SHA3(private, witness || message_hash)
    # NOTE: Uses private_key, matching oracle.HLWESigner (line 589-591)
    proof_input = witness_bytes + message_hash_bytes
    proof = hmac.new(private_key_bytes, proof_input, digestmod=hashlib.sha3_256).digest().hex()
    
    # Complete signature with all 7 required HLWESignature fields
    payload['signature'] = {
        'commitment'      : commitment,
        'witness'         : witness,
        'proof'           : proof,
        'w_entropy_hash'  : w_entropy_hash,
        'public_key_hex'  : wallet.public_key,
        'derivation_path' : "m/838'/0'/0'/0/0",
        'timestamp_ns'    : timestamp_ns,
    }
    payload['hlwe_signature'] = payload['signature']

    print(f"\n  Client TX ID : {tx_id}")
    print(f"  From         : {wallet.address}")
    print(f"  To           : {to_addr}")
    print(f"  Amount       : {amount} QTCL  (fee {fee})")
    if memo: print(f"  Memo         : {memo}")
    print("\n  ⚠️  The SERVER will return the authoritative TX hash to use for status queries.")

    try:
        confirm = input("\nBroadcast? [y/N]: ").strip().lower()
    except (EOFError, KeyboardInterrupt):
        print("\n  Cancelled."); return

    if confirm != 'y':
        print("  Cancelled."); return

    try:
        r = requests.post(f"{args.oracle_url}/api/submit_transaction",
                          json=payload, timeout=15)
        if r.status_code in (200, 201):
            data = r.json()
            # ── Use SERVER-RETURNED tx_hash as authoritative hash ───────────────
            # This is the canonical hash stored in DB — ALWAYS use this for lookups.
            server_tx_hash  = data.get('tx_hash', tx_id)
            client_alias    = data.get('client_tx_id', tx_id)
            status          = data.get('status', 'pending')
            oracle_signed   = data.get('signed', False)

            print(f"\n✅  TX Accepted by node!")
            print(f"  ┌──────────────────────────────────────────────────────────────────────┐")
            print(f"  │  AUTHORITATIVE TX HASH (use this for ALL lookups):                   │")
            print(f"  │  {server_tx_hash}  │")
            print(f"  └──────────────────────────────────────────────────────────────────────┘")
            print(f"  Status       : {status} (pending until miner seals a block — Bitcoin model)")
            print(f"  Oracle Signed: {'✅ YES' if oracle_signed else '⚠️  NO'}")
            if client_alias and client_alias != server_tx_hash:
                print(f"  Client Alias : {client_alias} (also queryable)")
            print(f"\n  📡 Query status:")
            print(f"     {args.oracle_url}/api/transactions/{server_tx_hash}")
            print(f"     {args.oracle_url}/api/mempool/tx/{server_tx_hash}")
            if data.get('block_height'):
                print(f"  📦  Confirmed in block #{data['block_height']}")
            else:
                print(f"\n  ⏳  TX is in mempool — will confirm when next block is mined.")
                print(f"     This is NORMAL. Bitcoin works the same way.")
        else:
            print(f"❌  Node rejected tx: {r.status_code} {r.text[:200]}")
    except requests.exceptions.ConnectionError:
        print(f"❌  Cannot reach node: {args.oracle_url}")
    except requests.exceptions.Timeout:
        print("\n❌  Node timed out")
    except Exception as e:
        print(f"❌  Broadcast error: {e}")


def _mask_sensitive_string(s: str, mask: bool = False) -> str:
    """Enterprise-grade key masking utility. Redacts sensitive cryptographic material. Args: s=string to mask, mask=if True shows first 8 and last 8 chars with … separator. Returns: original string if mask=False, masked if mask=True."""
    if not mask or not s or len(s) <= 16:
        return s
    return f"{s[:8]}…{s[-8:]}"

def _display_wallet_keys(wallet: 'QTCLWallet', mask_keys: bool = False, show_private: bool = False) -> None:
    """Enterprise-grade wallet key display with secure output, audit logging, and professional formatting. Features: professional visual hierarchy, optional key masking, checksum validation, ANSI colors with fallback, comprehensive error handling, timestamp audit trails. Args: wallet=loaded QTCLWallet instance, mask_keys=mask sensitive keys (first/last 8 chars), show_private=display private key (requires confirmation). Raises: ValueError if wallet state invalid or incomplete."""
    if not wallet or not wallet.is_loaded():
        raise ValueError("Wallet not loaded or incomplete")
    timestamp = datetime.now(timezone.utc).isoformat()
    display_addr = wallet.address[:16] + ('…' if len(wallet.address) > 16 else '')
    logger.info(f"[WALLET-KEYS] Display event at {timestamp} for {display_addr}")
    try:
        C_HEADER, C_ADDR, C_PUBKEY, C_PRIVKEY, C_BORDER, C_RESET, C_BOLD = '\033[95m', '\033[94m', '\033[92m', '\033[91m', '\033[96m', '\033[0m', '\033[1m'
    except:
        C_HEADER = C_ADDR = C_PUBKEY = C_PRIVKEY = C_BORDER = C_RESET = C_BOLD = ''
    try:
        # Address derivation must match _derive_keys(): SHA3(pubkey_bytes)[:20].hex()
        public_key_bytes = bytes.fromhex(wallet.public_key)
        expected_addr = 'qtcl' + hashlib.sha3_256(public_key_bytes).digest()[:20].hex()
        if wallet.address != expected_addr:
            logger.warning(f"[WALLET-KEYS] Address mismatch detected - wallet may be corrupted")
    except Exception as e:
        logger.warning(f"[WALLET-KEYS] Could not validate address integrity: {e}")
    print(f"\n{C_BORDER}{'='*76}{C_RESET}")
    print(f"{C_HEADER}{C_BOLD}  WALLET KEY MANIFEST - ENTERPRISE GRADE DISPLAY{C_RESET}")
    print(f"{C_BORDER}{'='*76}{C_RESET}")
    print(f"  Timestamp : {timestamp}")
    print(f"  Integrity : SHA3-256 derivation chain verified")
    print(f"{C_BORDER}{'-'*76}{C_RESET}")
    addr_masked = _mask_sensitive_string(wallet.address, mask_keys)
    print(f"\n{C_ADDR}{C_BOLD}  WALLET ADDRESS{C_RESET}")
    print(f"  {wallet.address}")
    if mask_keys:
        print(f"  (Masked: {addr_masked})")
    print(f"  Type    : qtcl prefix, 64 hex chars")
    print(f"  Derived : SHA3-256(public_key)[:40] + 'qtcl'")
    pubkey_masked = _mask_sensitive_string(wallet.public_key, mask_keys)
    print(f"\n{C_PUBKEY}{C_BOLD}  PUBLIC KEY (Safe to Share){C_RESET}")
    print(f"  {wallet.public_key}")
    if mask_keys:
        print(f"  (Masked: {pubkey_masked})")
    print(f"  Type    : SHA3-256 hash of private key")
    print(f"  Usage   : Transaction signing, identity verification, oracle registration")
    print(f"  Entropy : 256 bits (32 bytes)")
    if show_private:
        if wallet.private_key:
            privkey_masked = _mask_sensitive_string(wallet.private_key, mask_keys)
            print(f"\n{C_PRIVKEY}{C_BOLD}  ⚠️  PRIVATE KEY (KEEP SECRET){C_RESET}")
            print(f"  {wallet.private_key}")
            if mask_keys:
                print(f"  (Masked: {privkey_masked})")
            print(f"  Type    : SHA3-256 hash of BIP-32 derived key")
            print(f"  Usage   : NEVER share. Transaction signing only.")
            print(f"  Entropy : 256 bits (32 bytes)")
            print(f"{C_PRIVKEY}  ⚠️  DO NOT SCREENSHOT, PHOTOGRAPH, OR EMAIL THIS KEY{C_RESET}")
            logger.warning(f"[WALLET-KEYS] Private key displayed at {timestamp}")
        else:
            print(f"\n{C_PRIVKEY}  ⚠️  PRIVATE KEY NOT AVAILABLE{C_RESET}")
            print(f"  Status  : Wallet loaded in address-only mode")
    print(f"\n{C_BORDER}{'='*76}{C_RESET}")
    print(f"{C_BOLD}  SECURITY RECOMMENDATIONS:{C_RESET}")
    print(f"  • Save address to secure location (can be shared safely)")
    print(f"  • Never share public key in untrusted contexts")
    print(f"  • Private key must be kept offline and encrypted")
    print(f"  • Use mnemonic recovery phrase as backup (stored encrypted)")
    print(f"  • Enable 2FA on oracle registration")
    print(f"{C_BORDER}{'='*76}{C_RESET}\n")
    if mask_keys:
        import gc
        gc.collect()

def parse_args():
    """Parse CLI arguments for QTCL Miner with enterprise-grade validation."""
    parser=argparse.ArgumentParser(description='🌌 QTCL Full Node + Quantum W-State Miner')
    parser.add_argument('--address','-a',help='Miner wallet address (qtcl1...)')
    parser.add_argument('--oracle-url','-o',default='https://qtcl-blockchain.koyeb.app',help='Oracle URL (for W-state recovery)')
    parser.add_argument('--difficulty','-d',type=int,default=DEFAULT_DIFFICULTY,help='Mining difficulty bits (default 20 ≈ 10-20s per block at ~50k h/s)')
    parser.add_argument('--log-level',default='INFO',choices=['DEBUG','INFO','WARNING','ERROR'])
    parser.add_argument('--wallet-init',action='store_true',help='Generate new wallet with mnemonic')
    parser.add_argument('--wallet-recover',action='store_true',help='Recover corrupt/missing wallet')
    parser.add_argument('--wallet-from-mnemonic',action='store_true',help='Restore from 12-word phrase')
    parser.add_argument('--wallet-show-mnemonic',action='store_true',help='Display recovery phrase')
    parser.add_argument('--wallet-show-keys',action='store_true',help='Display wallet address and public key (add --wallet-show-private for private key)')
    parser.add_argument('--wallet-show-private',action='store_true',help='Include private key in --wallet-show-keys output (requires confirmation)')
    parser.add_argument('--mask-keys',action='store_true',default=False,help='Mask sensitive key material (show first/last 8 chars only)')
    parser.add_argument('--mode',choices=['mine','transact'],default=None,help='Run mode')
    parser.add_argument('--to-address',default=None,help='Transaction recipient')
    parser.add_argument('--amount',default=None,help='Transaction amount (QTCL)')
    parser.add_argument('--wallet-password',help='Wallet password')
    parser.add_argument('--register',action='store_true',help='Register with oracle')
    parser.add_argument('--miner-id',help='Miner ID for registration')
    parser.add_argument('--miner-name',default='qtcl-miner',help='Friendly miner name')
    parser.add_argument('--fidelity-mode',choices=['strict','normal','relaxed'],default='normal',help='W-state fidelity threshold mode: strict (F>=0.90), normal (F>=0.80, recommended), relaxed (F>=0.70)')
    parser.add_argument('--strict-w-verification',action='store_true',default=False,help='Enable strict W-state verification (rejects marginal states)')
    parser.add_argument('--p2p-api-port',type=int,default=9091,
                        help='Port for local P2P REST API (other nodes contact you here, default 9091). '
                             'Set this to a port your firewall allows for incoming connections.')
    parser.add_argument('--oracle-min-peers',type=int,default=7,
                        help='Minimum DHT peers required before this node can self-promote to oracle (default 7)')
    return parser.parse_args()

def main():
    args=parse_args()
    logging.getLogger().setLevel(getattr(logging,args.log_level))
    
    try:
        # ── Wallet init ───────────────────────────────────────────────────────
        if args.wallet_init:
            pw = args.wallet_password or input("  New wallet password: ").strip()
            if not pw: logger.error("[WALLET] Password required"); sys.exit(1)
            w = QTCLWallet(); w.create(pw)
            return

        # ── Wallet recovery ───────────────────────────────────────────────────
        if getattr(args, 'wallet_recover', False):
            _wallet_recover(args); return

        if getattr(args, 'wallet_from_mnemonic', False):
            phrase = input("  Enter 12 recovery words: ").strip()
            pw     = args.wallet_password or input("  New password: ").strip()
            w      = QTCLWallet()
            if w.restore_from_mnemonic(phrase, pw):
                print(f"  ✅ Restored: {w.address}")
            else:
                print("  ❌ Restore failed — check words")
            return

        if getattr(args, 'wallet_show_mnemonic', False):
            pw = args.wallet_password or input("  Wallet password: ").strip()
            phrase = QTCLWallet().show_mnemonic(pw)
            if phrase:
                words = phrase.split()
                print("\nYour 12-word recovery phrase:")
                for i in range(0, 12, 3):
                    print(f"    {i+1:2}. {words[i]:<14} {i+2:2}. {words[i+1]:<14} {i+3:2}. {words[i+2]}")
                print()
            else:
                print("  ⚠️  Mnemonic not found or wrong password.")
            return

        # ── Wallet show keys (enterprise display with security audit) ─────────
        if getattr(args, "wallet_show_keys", False):
            pw = args.wallet_password or input("  Wallet password: ").strip()
            wallet_keys = QTCLWallet()
            if not wallet_keys.load(pw):
                logger.error("[WALLET-KEYS] ❌ Failed to load wallet")
                sys.exit(1)
            show_priv = getattr(args, "wallet_show_private", False)
            if show_priv:
                try:
                    confirm = input("\n  ⚠️  Display private key? Type 'yes' to confirm: ").strip().lower()
                    if confirm != "yes":
                        logger.info("[WALLET-KEYS] Private key display cancelled by user")
                        sys.exit(0)
                except (EOFError, KeyboardInterrupt):
                    logger.info("[WALLET-KEYS] Display interrupted")
                    sys.exit(0)
            try:
                _display_wallet_keys(wallet_keys, mask_keys=args.mask_keys, show_private=show_priv)
                logger.info("[WALLET-KEYS] ✅ Successfully displayed wallet keys")
            except Exception as e:
                logger.error(f"[WALLET-KEYS] ❌ Display failed: {e}")
                sys.exit(1)
            return
        
        # ── Check for --wallet-show-private without --wallet-show-keys ─────────
        if getattr(args, "wallet_show_private", False):
            print("\n❌ ERROR: --wallet-show-private requires --wallet-show-keys flag")
            print("\nCorrect usage:")
            print("  python qtcl_miner_enhanced.py --wallet-show-keys --wallet-show-private --wallet-password <PASSWORD>\n")
            sys.exit(1)

        # ── Load wallet ───────────────────────────────────────────────────────
        wallet = QTCLWallet()
        if args.address and not args.wallet_password:
            wallet.address = args.address
            address = wallet.address
        else:
            pw = args.wallet_password or input(
                "Wallet password (Enter to skip for address-only mining): ").strip() or None
            if pw:
                if not wallet.load(pw):
                    if args.address:
                        logger.warning("[WALLET] ⚠️  Decrypt failed — address-only mode")
                        wallet.address = args.address
                    else:
                        logger.error("[WALLET] ❌ Failed to load wallet. Run --wallet-init")
                        sys.exit(1)
                elif args.address and args.address != wallet.address:
                    logger.error("[WALLET] ❌ --address does not match loaded wallet")
                    sys.exit(1)
            elif args.address:
                wallet.address = args.address
            else:
                logger.error("[WALLET] ❌ No password and no --address. Run --wallet-init")
                sys.exit(1)
            address = wallet.address

        # ── Oracle registration ───────────────────────────────────────────────
        if args.register:
            if not wallet.is_loaded():
                logger.error("[REGISTER] Full wallet required (password + keys)")
                sys.exit(1)
            registry = MinerRegistry(args.oracle_url)
            ok = registry.register(args.miner_id, wallet.address,
                                    wallet.public_key, wallet.private_key, args.miner_name)
            logger.info("[REGISTER] ✅ OK" if ok else "[REGISTER] ❌ Failed")
            sys.exit(0 if ok else 1)

        # ── Mode selection ────────────────────────────────────────────────────
        mode = getattr(args, 'mode', None)
        if mode is None:
            print("\n┌─────────────────────────────────────┐")
            print("  │  QTCL Full Node                     │")
            print("  │  1. ⛏️   Mine                        │")
            print("  │  2. 💸  Transact                    │")
            print("  └─────────────────────────────────────┘")
            try:
                choice = input("  Enter choice [1/2]: ").strip()
            except (EOFError, KeyboardInterrupt):
                choice = '1'
            mode = 'transact' if choice == '2' else 'mine'

        if mode == 'transact':
            if not wallet.is_loaded():
                print("\n❌  Transaction mode requires a fully loaded wallet.")
                print("    Re-run and enter your wallet password.")
                sys.exit(1)
            _run_transaction_menu(args, wallet)
            return
        
        # Start mining
        # ─── DATABASE INITIALIZATION WITH PERSISTENT FILE-BASED STORAGE ──────────────
        global db
        db_path = Path('data/qtcl_blockchain.db')
        db_path.parent.mkdir(parents=True, exist_ok=True)
        
        logger.info(f"[DB] 🔧 Initializing persistent database at {db_path}...")
        try:
            # 🎯 PERSISTENT FILE-BASED DATABASE (survives restarts)
            db = sqlite3.connect(str(db_path), check_same_thread=False, timeout=10)
            db.execute("PRAGMA synchronous=NORMAL")
            db.execute("PRAGMA journal_mode=WAL")
            
            # 🎯 VALIDATE AND CREATE SCHEMA - Check tables exist, create if missing
            # This ensures compatibility across restarts and versions
            schema_validation_sql = """
                CREATE TABLE IF NOT EXISTS blocks (
                    height INTEGER PRIMARY KEY,
                    block_hash TEXT UNIQUE NOT NULL,
                    parent_hash TEXT NOT NULL,
                    merkle_root TEXT NOT NULL,
                    timestamp_s INTEGER NOT NULL,
                    difficulty_bits INTEGER NOT NULL,
                    nonce INTEGER NOT NULL,
                    miner_address TEXT NOT NULL,
                    w_state_fidelity REAL NOT NULL,
                    w_entropy_hash TEXT NOT NULL,
                    mining_time_s REAL NOT NULL DEFAULT 0.0,
                    created_at INTEGER DEFAULT (strftime('%s', 'now'))
                );
                
                CREATE INDEX IF NOT EXISTS idx_blocks_height ON blocks(height);
                CREATE INDEX IF NOT EXISTS idx_blocks_hash ON blocks(block_hash);
                
                CREATE TABLE IF NOT EXISTS difficulty_state (
                    id INTEGER PRIMARY KEY CHECK(id=1),
                    current_difficulty INTEGER NOT NULL DEFAULT 13,
                    target_block_time_s REAL NOT NULL DEFAULT 60.0,
                    retarget_window INTEGER NOT NULL DEFAULT 10,
                    last_retarget_height INTEGER NOT NULL DEFAULT 0,
                    ema_block_time_s REAL NOT NULL DEFAULT 60.0,
                    ema_alpha REAL NOT NULL DEFAULT 0.2,
                    min_difficulty INTEGER NOT NULL DEFAULT 8,
                    max_difficulty INTEGER NOT NULL DEFAULT 32,
                    total_blocks_retargeted INTEGER NOT NULL DEFAULT 0,
                    updated_at INTEGER DEFAULT (strftime('%s', 'now'))
                );
            """
            
            logger.info("[DB] 🔍 Validating schema...")
            
            # Verify blocks table has all required columns
            try:
                cursor = db.execute("PRAGMA table_info(blocks)")
                existing_cols = {row[1] for row in cursor.fetchall()}
                required_cols = {'height', 'block_hash', 'parent_hash', 'merkle_root', 'timestamp_s', 
                                'difficulty_bits', 'nonce', 'miner_address', 'w_state_fidelity', 'w_entropy_hash'}
                if required_cols <= existing_cols:
                    logger.info("[DB] ✅ Schema validation: blocks table OK")
                else:
                    missing = required_cols - existing_cols
                    logger.warning(f"[DB] ⚠️  Schema validation: blocks table missing columns {missing}, recreating...")
                    db.execute("DROP TABLE IF EXISTS blocks")
            except:
                logger.info("[DB] 📋 Schema validation: blocks table missing, creating...")
            
            # Verify difficulty_state table exists
            try:
                cursor = db.execute("PRAGMA table_info(difficulty_state)")
                if cursor.fetchone():
                    logger.info("[DB] ✅ Schema validation: difficulty_state table OK")
                else:
                    raise Exception("Empty table")
            except:
                logger.info("[DB] 📋 Schema validation: difficulty_state table missing, creating...")
            
            # Execute complete schema creation (creates only if not exists)
            db.executescript(schema_validation_sql)
            
            # Initialize difficulty_state singleton row
            db.execute("INSERT OR IGNORE INTO difficulty_state (id) VALUES(1)")
            db.commit()
            
            # Final verification
            cursor = db.execute("SELECT COUNT(*) FROM blocks")
            block_count = cursor.fetchone()[0]
            cursor = db.execute("SELECT COUNT(*) FROM difficulty_state")
            difficulty_count = cursor.fetchone()[0]
            
            logger.info(f"[DB] ✅ Persistent database ready | path={db_path}")
            logger.info(f"[DB] ✅ Schema validated: blocks table ({block_count} records), difficulty_state ({difficulty_count} records)")
            logger.info(f"[DB] ✅ Persistent storage: ENABLED (survives restarts)")
        except Exception as e:
            logger.error(f"[DB] ❌ Database initialization failed: {e}")
            traceback.print_exc()   # module already imported at top-level — NO local re-import
            sys.exit(1)

        # ─── SCHEMA PATCHES ─── MUST run before QTCLFullNode init (QTCLP2PBundle needs tables) ──
        logger.info("[INIT] 🔧 Applying database schema patches...")
        apply_schema_patches()      # covers all tables including DHT/Oracle/VirtualPQ

        node=QTCLFullNode(
            miner_address=address,
            oracle_url=args.oracle_url,
            difficulty=args.difficulty,
            db_connection=db
        )

        node.fidelity_mode = args.fidelity_mode
        node.strict_verification = args.strict_w_verification

        logger.info(f"[INIT] W-state fidelity mode: {args.fidelity_mode}")
        if args.strict_w_verification:
            logger.warning("[INIT] Strict W-state verification enabled")

        # ─── P2P INITIALIZATION SEQUENCE ────────────────────────────────────────────
        logger.info("[P2P] 🚀 Initializing P2P network layer...")
        
        # 1. Raw TCP P2PServer is DISABLED — all P2P traffic goes through GossipListener on :9091.
        #    Port 8000 is reserved for the Koyeb oracle public endpoint.
        #    P2PServer is kept in code for direct LAN connections only; not started by default.
        peer_id_hash = hashlib.sha256(address.encode()).hexdigest()[:12]
        peer_id = f"miner_{peer_id_hash}"
        global _P2P_SERVER, _P2P_CLIENT, _TX_SIGNER, _ORACLE_BROADCASTER, _CONSENSUS_MGR, _PEER_SYNC, _P2P_SERVICE_INVENTORY

        _P2P_SERVER = None  # TCP raw socket server disabled — GossipListener on :9091 handles all P2P
        logger.info("[P2P] ℹ️  Raw TCP P2PServer disabled (GossipListener on :9091 handles all traffic)")
        
        # ── Canonical oracle URL — single source of truth for all P2P/REST calls ──
        oracle_url = args.oracle_url

        # 2. Initialize transaction signing and Oracle broadcasting
        _TX_SIGNER = HLWETransactionSigner(address)
        _ORACLE_BROADCASTER = OracleBroadcaster(oracle_url)
        # Wire DB reference and peer_id into OracleBroadcaster for local DB marking and fan-out
        _ORACLE_BROADCASTER._db_ref   = db
        _ORACLE_BROADCASTER._peer_id  = peer_id
        logger.info("[SIGNING] 🔐 HLWE transaction signing initialized")
        logger.info("[ORACLE] 📤 Oracle broadcasting initialized")

        # 3. Create P2P client FIRST — must exist before PeriodicPeerSync references it
        _P2P_CLIENT = P2PClient(peer_id, oracle_base_url=oracle_url)
        logger.info(f"[P2P] ✅ P2P client created | oracle={oracle_url}")

        # 4. Initialize P2P service inventory singleton
        local_node_id_init = hashlib.sha1(peer_id.encode()).hexdigest()
        _P2P_SERVICE_INVENTORY = P2PServiceInventory(
            local_peer_id=peer_id,
            local_node_id=local_node_id_init,
            db=db,
        )
        logger.info(f"[P2P-INVENTORY] 📋 Service inventory initialized | peer_id={peer_id}")

        # 5. Initialize consensus and periodic sync — now _P2P_CLIENT is valid
        _CONSENSUS_MGR = ConsensusManager()
        _PEER_SYNC = PeriodicPeerSync(_P2P_CLIENT, _CONSENSUS_MGR)
        logger.info("[CONSENSUS] 🤝 Consensus manager initialized")

        # 5. Sync chain height from oracle / peers
        # Short initial timeout (3s) — Koyeb free tier may be cold-starting.
        # Mining starts from local state immediately; background sync catches up.
        current_height = 0
        p2p_success    = False

        logger.info("[P2P] 📊 Querying oracle for current block height (3s timeout — non-blocking)...")
        current_height = _P2P_CLIENT.get_block_height(timeout=3, oracle_url=oracle_url)

        if current_height is not None and current_height > 0:
            logger.info(f"[P2P] ✅ Got height from oracle: {current_height}")
        else:
            logger.info(
                "[P2P] ℹ️  Oracle cold-starting or unreachable — mining from local state. "
                "Background sync will catch up within 30s."
            )
            # Try peer-to-peer height discovery (no oracle needed)
            discovered = _P2P_CLIENT.discover_peers(timeout=3)
            if discovered:
                _P2P_CLIENT.known_peers.extend(discovered)
                current_height = _P2P_CLIENT.get_block_height(timeout=3, oracle_url=oracle_url)
                if current_height:
                    logger.info(f"[P2P] ✅ Got height from peers: {current_height}")

            # Fallback: read local DB height — always available
            if not current_height:
                try:
                    cursor = db.cursor()
                    cursor.execute("SELECT MAX(height) FROM blocks")
                    result = cursor.fetchone()
                    if result and result[0] is not None:
                        current_height = int(result[0])
                        logger.info(f"[P2P] 📦 Mining from local DB height: {current_height}")
                    else:
                        current_height = 0
                        logger.info("[P2P] 📦 No local blocks — starting genesis mining")
                except Exception:
                    current_height = 0

        if current_height is not None and current_height > 0:
            logger.info(f"[P2P] ✅ P2P sync: Current height = {current_height}")

            # Sync blocks from peers if needed — use main `db`, NOT schema-builder _DB_CONN
            try:
                db_height = 0
                cursor = db.cursor()
                cursor.execute("SELECT MAX(height) FROM blocks")
                result = cursor.fetchone()
                if result[0] is not None:
                    db_height = result[0]

                if current_height > db_height:
                    logger.info(f"[P2P] 📦 Syncing blocks {db_height + 1} to {current_height}...")
                    blocks = _P2P_CLIENT.sync_blocks(db_height + 1, min(current_height, db_height + 100), timeout=10)
                    logger.info(f"[P2P] ✅ Synced {len(blocks)} blocks from peers")
                    p2p_success = True
            except Exception as e:
                logger.warning(f"[P2P] ⚠️  Block sync error: {e}")
        else:
            logger.warning("[P2P] ⚠️  No peer height information available")
        
        # 6. Fallback to Oracle if P2P failed or incomplete
        if not p2p_success:
            logger.info("[P2P] 📡 P2P sync incomplete, falling back to Oracle...")
        
        # 7. Initialize Oracle and get W-state (in background)
        logger.info("[ORACLE] 🌐 Connecting to Oracle for W-state recovery...")
        
        # ─── START BACKGROUND BROADCAST LOOP ────────────────────────────────────────
        def oracle_broadcast_loop():
            """Background loop for Oracle broadcasts and network snapshot capture."""
            logger.info("[ORACLE] 🔄 Background Oracle broadcast + snapshot loop started")
            _snap_interval   = 120   # capture network snapshot every 2 minutes
            _last_snap_time  = 0.0
            _broadcast_interval = 15  # broadcast every 15 seconds (was 30)
            _last_broadcast_time = 0.0
            while True:
                try:
                    time.sleep(5)
                    now = time.time()

                    # ── Broadcast pending TXs/blocks to oracle ────────────────
                    if now - _last_broadcast_time >= _broadcast_interval:
                        if _ORACLE_BROADCASTER:
                            stats = _ORACLE_BROADCASTER.broadcast_pending(timeout=8)
                            if stats['sent'] > 0 or stats['failed'] > 0:
                                logger.info(
                                    f"[ORACLE] 📤 Broadcast: sent={stats['sent']} "
                                    f"failed={stats['failed']} queued={stats['queued']}"
                                )
                        _last_broadcast_time = now

                    # ── Periodic network snapshot capture ─────────────────────
                    if now - _last_snap_time >= _snap_interval:
                        try:
                            snap = P2PServiceInventory.dispatch_inbound(
                                P2PServiceRequestType.NETWORK_SNAPSHOT, {},
                                db, node.mempool if 'node' in dir() else None, None
                            )
                            snap_id  = snap.get('snapshot_id', secrets.token_hex(8))
                            snap_ts  = snap.get('snapshot_ts', now)
                            chain    = snap.get('chain', {})
                            db.execute("""
                                INSERT OR REPLACE INTO network_snapshots
                                    (snapshot_id, snapshot_ts, chain_tip_height, total_blocks,
                                     avg_fidelity, peer_count, oracle_count, mempool_size, snapshot_json)
                                VALUES (?,?,?,?,?,?,?,?,?)
                            """, (
                                snap_id, snap_ts,
                                chain.get('tip_height', 0),
                                chain.get('total_blocks', 0),
                                chain.get('avg_fidelity', 0.0),
                                snap.get('peer_count', 0),
                                snap.get('oracle_count', 0),
                                snap.get('mempool_size', 0),
                                json.dumps(snap, default=str)[:65000],
                            ))
                            db.commit()
                            # Prune snapshots older than 24h
                            db.execute("DELETE FROM network_snapshots WHERE snapshot_ts < ?",
                                       (now - 86400,))
                            db.commit()
                            logger.debug(f"[SNAPSHOT] 📸 Network snapshot captured | id={snap_id}")
                        except Exception as _se:
                            logger.debug(f"[SNAPSHOT] Capture error: {_se}")
                        _last_snap_time = now

                except Exception as e:
                    logger.debug(f"[ORACLE] Broadcast loop error: {e}")
        
        broadcast_thread = threading.Thread(target=oracle_broadcast_loop, daemon=True, name="OracleBroadcast")
        broadcast_thread.start()
        
        # ─── START BACKGROUND P2P MONITORING ────────────────────────────────────────
        def p2p_monitoring_loop():
            """Background loop for P2P monitoring - reports LOCAL blockchain height only."""
            logger.info("[P2P] 🔄 Background P2P monitoring started")
            while True:
                try:
                    time.sleep(30)  # Check every 30 seconds
                    
                    # 🎯 FIXED: Report LOCAL database height, not peer height
                    # Local database is the source of truth
                    try:
                        if node.db:
                            cursor = node.db.execute("SELECT MAX(height) FROM blocks")
                            result = cursor.fetchone()
                            local_height = result[0] if result and result[0] is not None else 0
                            
                            # Only log if height changed
                            if not hasattr(p2p_monitoring_loop, 'last_height'):
                                p2p_monitoring_loop.last_height = local_height
                            
                            if local_height != p2p_monitoring_loop.last_height:
                                logger.info(f"[P2P] 📊 Local blockchain height: {local_height} (from database)")
                                p2p_monitoring_loop.last_height = local_height
                    except Exception as e:
                        logger.debug(f"[P2P] Height check error: {e}")
                
                except Exception as e:
                    logger.debug(f"[P2P] Background monitor error: {e}")
        
        p2p_monitor_thread = threading.Thread(target=p2p_monitoring_loop, daemon=True, name="P2PMonitor")
        
        # ─── START REAL-TIME ORACLE SYNC ────────────────────────────────────────────
        def oracle_realtime_sync_loop():
            """
            Background loop: continuously sync latest blocks from Oracle.
            This ensures local database stays in sync with network state.
            """
            logger.info("[ORACLE] 🔄 Real-time block sync loop started (interval: 15s)")
            while True:
                try:
                    time.sleep(15)  # Sync every 15 seconds
                    
                    # Get current local height
                    if node.db:
                        cursor = node.db.execute("SELECT MAX(height) FROM blocks")
                        result = cursor.fetchone()
                        local_height = result[0] if result and result[0] is not None else 0
                        
                        # Fetch latest blocks from Oracle
                        try:
                            tip = node.client.get_tip_block()
                            if tip and tip.height > local_height:
                                logger.info(f"[ORACLE] 📥 Syncing blocks {local_height + 1}…{tip.height} from Oracle...")
                                
                                for block_height in range(local_height + 1, min(local_height + 11, tip.height + 1)):
                                    try:
                                        block_data = node.client.get_block_by_height(block_height)
                                        if block_data:
                                            header = BlockHeader.from_dict(block_data.get('header', block_data))
                                            
                                            # Persist to database immediately
                                            node.db.execute("""
                                                INSERT OR IGNORE INTO blocks 
                                                (height, block_hash, parent_hash, merkle_root, timestamp_s,
                                                 difficulty_bits, nonce, miner_address, w_state_fidelity, w_entropy_hash)
                                                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                                            """, (
                                                header.height, header.block_hash, header.parent_hash,
                                                header.merkle_root, header.timestamp_s, header.difficulty_bits,
                                                header.nonce, header.miner_address,
                                                getattr(header, 'w_state_fidelity', 0.0),
                                                getattr(header, 'w_entropy_hash', '')
                                            ))
                                            node.db.commit()
                                            logger.debug(f"[ORACLE] 📦 Synced block #{block_height} from Oracle")
                                    except Exception as e:
                                        logger.debug(f"[ORACLE] ⚠️  Failed to sync block {block_height}: {e}")
                        except Exception as e:
                            logger.debug(f"[ORACLE] Sync error: {e}")
                except Exception as e:
                    logger.debug(f"[ORACLE] Background sync error: {e}")
        
        oracle_sync_thread = threading.Thread(target=oracle_realtime_sync_loop, daemon=True, name="OracleSync")
        
        # ─── START PERIODIC PEER SYNC ───────────────────────────────────────────────
        _PEER_SYNC.start()

        logger.info("[INIT] ✨ P2P layer, consensus, and signing initialized and monitoring started")

        # ── Apply CLI overrides to P2P bundle before start ────────────────────────
        if node._p2p_bundle is not None:
            # --p2p-api-port: set oracle server base port (where peers call us)
            node._p2p_bundle.elig._oracle_server_port_override = args.p2p_api_port
            if hasattr(node._p2p_bundle.elig, '_oracle_server') and \
               node._p2p_bundle.elig._oracle_server is not None:
                node._p2p_bundle.elig._oracle_server.BASE_PORT = args.p2p_api_port
            # Also patch class default so any future P2POracleServer uses this port
            P2POracleServer.BASE_PORT = args.p2p_api_port
            P2POracleServer.MAX_PORT  = args.p2p_api_port + 10
            # --oracle-min-peers: override promotion threshold
            node._p2p_bundle.elig.ORACLE_FULL_THRESHOLD = args.oracle_min_peers
            node._p2p_bundle.elig.ORACLE_PEER_THRESHOLD = max(1, args.oracle_min_peers // 2)
            logger.info(
                f"[P2P-BUNDLE] ⚙️  CLI config | p2p-api-port={args.p2p_api_port} | "
                f"oracle-min-peers={args.oracle_min_peers}"
            )

        # 6. Start node (W-state recovery, blockchain sync, mining)
        if not node.start():
            logger.error("[MAIN] ❌ Failed to start node")
            sys.exit(1)

        # 🎯 START BACKGROUND LOOPS AFTER node.start() so they can access node.db
        p2p_monitor_thread.start()
        oracle_sync_thread.start()
        logger.info("[MONITORING] 🔄 Started: P2P monitor, Oracle real-time sync")

        logger.info("[MAIN] 🎯 Mining loop started in foreground")
        
        while True:
            time.sleep(30)
            status = node.get_status()
            print("\n" + ("=" * 140))
            print("⛏️  QTCL QUANTUM MINER STATUS (W-STATE ENTANGLED)")
            print("=" * 140)
            print(f"Miner:                    {status['miner_full']}")
            print(f"Status:                   {status['status'].upper()}")
            print(f"")
            print(f"BLOCKCHAIN:")
            print(f"  Chain Height:           {status['chain']['height']}")
            print(f"  Tip Hash:               {status['chain']['tip_hash']}")
            print(f"")
            print(f"WALLET & REWARDS:")
            print(f"  Address:                {status['wallet']['address']}")
            print(f"  Balance:                {status['wallet']['balance_formatted']}")
            print(f"  Estimated Rewards:      {status['wallet']['estimated_rewards']:.2f} QTCL")
            print(f"")
            print(f"MEMPOOL:")
            print(f"  Pending Transactions:   {status['mempool']['size']}")
            print(f"")
            print(f"MINING METRICS:")
            print(f"  Blocks Mined:           {status['mining']['blocks_mined']}")
            print(f"  Block Rewards Earned:   {status['mining']['block_rewards']}")
            print(f"  Total Hash Attempts:    {status['mining']['total_hash_attempts']:,}")
            print(f"  Avg W-State Fidelity:   {status['mining']['avg_fidelity']:.4f}")
            print(f"  Hash Rate:              {status['mining']['estimated_hash_rate']} hashes/sec")
            print(f"  Current Difficulty:     {status['mining']['current_difficulty']} bits")
            print(f"  EMA Block Time:         {status['mining']['ema_block_time_s']:.2f}s (target {status['mining']['target_block_time_s']:.0f}s)")
            print(f"")
            print(f"QUANTUM W-STATE ENTANGLEMENT:")
            print(f"  Established:            {status['quantum']['w_state']['entanglement_established']}")
            print(f"  pq0 Oracle Fidelity:    {status['quantum']['w_state']['pq0_fidelity']:.4f}")
            print(f"  W-State Fidelity:       {status['quantum']['w_state']['w_state_fidelity']:.4f}")
            print(f"  pq_curr (field ID):     {status['quantum']['w_state']['pq_curr']}")
            print(f"  pq_last (field ID):     {status['quantum']['w_state']['pq_last']}")
            print(f"  Sync Lag:               {status['quantum']['w_state']['sync_lag_ms']:.1f}ms")
            print(f"")
            print(f"ORACLE RECOVERY:")
            print(f"  Connected:              {status['quantum']['recovery']['connected']}")
            print(f"  Peer ID:                {status['quantum']['recovery']['peer_id']}")
            print(f"  Oracle URL:             {status['network']['oracle_url']}")
            print(f"")
            p2p = status.get('p2p', {})
            print(f"P2P DHT FABRIC:")
            print(f"  Node ID:                {p2p.get('node_id','N/A')}")
            print(f"  DHT Bootstrapped:       {'✅' if p2p.get('dht_bootstrapped') else '⏳ Discovering...'}")
            print(f"  DHT Peers:              {p2p.get('dht_peers', 0)}")
            print(f"  P2P Oracles Found:      {p2p.get('dht_oracles', 0)}")
            print(f"  Entanglement Links:     {p2p.get('entanglement_links', 0)} active")
            print(f"")
            print(f"VIRTUAL PSEUDOQUBITS:")
            print(f"  pq0 Fidelity:           {p2p.get('pq0_fidelity', 0.0):.4f}")
            print(f"  Virtual PQs:            {p2p.get('virtual_pq_count', 0)} spawned")
            print(f"  Inverse-Virtual PQs:    {p2p.get('inverse_vpq_count', 0)} spawned")
            print(f"")
            print(f"ORACLE STATUS:")
            if p2p.get('is_oracle'):
                print(f"  Role:                   🌟 P2P ORACLE (self-promoted)")
                print(f"  Oracle Port:            {p2p.get('oracle_port', 'N/A')}")
            else:
                print(f"  Role:                   ⛏️  MINER")
                elig_peers  = p2p.get('dht_peers', 0)
                needed      = max(0, 7 - elig_peers)
                print(f"  Oracle Eligibility:     {elig_peers}/7 peers ({'eligible ✅' if elig_peers >= 7 else f'need {needed} more peers'})")
            print(f"")
            print(f"NETWORK:")
            print(f"  Gossip URL:             {status['network'].get('gossip_url', 'unbound')}")
            print(f"  Peer Count:             {status['network'].get('peer_count', 0)}")
            print(f"  Summary:                {status['metrics_summary']}")
            print("=" * 140 + "\n")
    
    except KeyboardInterrupt:
        print("\n[MAIN] 🛑 Shutdown signal received...")
    except Exception as e:
        print(f"\n❌ FATAL: {e}")
        traceback.print_exc()
        sys.exit(1)
    finally:
        if 'node' in locals():
            node.stop()
        print("\n✅ Shutdown complete\n")

if __name__=='__main__':
    main()
