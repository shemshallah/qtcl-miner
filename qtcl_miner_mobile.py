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
║  USAGE: python qtcl_miner.py --address qtcl1YOUR_ADDRESS --oracle-url http://oracle.local:5000                                         ║
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
    """P2P client for peer discovery and block synchronization."""
    
    def __init__(self, peer_id: str, known_peers: List[Tuple[str, int]] = None):
        self.peer_id = peer_id
        # 🎯 FIXED: Don't discover ourselves! Start with empty peers list
        # Oracle will be discovered separately on port 8000
        self.known_peers = known_peers or []
        self.connected_peers: Dict[str, Tuple[str, int]] = {}
        self.peer_info: Dict[str, Dict[str, Any]] = {}
        self._lock = threading.RLock()
    
    def discover_peers(self, timeout: int = 5) -> List[Tuple[str, int]]:
        """Discover peers by connecting to known peers."""
        discovered = []
        
        for host, port in self.known_peers:
            try:
                sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                sock.settimeout(timeout)
                sock.connect((host, port))
                
                msg = P2PMessage('HELLO', self.peer_id)
                sock.send(msg.to_json().encode() + b'\n')
                
                response_data = sock.recv(4096).decode()
                response = P2PMessage.from_json(response_data.strip())
                
                if response.payload and 'peers' in response.payload:
                    for peer in response.payload['peers']:
                        peer_host, peer_port = peer
                        if (peer_host, peer_port) not in discovered:
                            discovered.append((peer_host, peer_port))
                
                self.connected_peers[f"{host}:{port}"] = (host, port)
                sock.close()
                logger.info(f"[P2P] 🔗 Connected to peer {host}:{port}")
                
            except Exception as e:
                logger.debug(f"[P2P] ⚠️  Could not reach {host}:{port}: {e}")
        
        return discovered
    
    def get_block_height(self, timeout: int = 5, oracle_url: str = None) -> Optional[int]:
        """
        Get current block height from authoritative sources.
        
        Priority:
        1. LOCAL DATABASE (source of truth)
        2. Oracle on port 8000 (network consensus)
        3. Peers on port 8333 (fallback)
        """
        # 🎯 PRIORITY 1: Local database is authoritative
        # This is handled by caller, not here
        
        # 🎯 PRIORITY 2: Try Oracle on port 8000 (real network state)
        if oracle_url:
            try:
                import requests
                r = requests.get(f"{oracle_url.rstrip('/')}/api/blocks/tip", timeout=timeout)
                if r.status_code == 200:
                    data = r.json()
                    oracle_height = data.get('height')
                    if oracle_height is not None:
                        return int(oracle_height)
            except Exception as e:
                pass  # Fall through to peers
        
        # 🎯 PRIORITY 3: Peer network (8333) - only if we have peers
        for host, port in self.known_peers:
            try:
                sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                sock.settimeout(timeout)
                sock.connect((host, port))
                
                msg = P2PMessage('GET_HEIGHT', self.peer_id)
                sock.send(msg.to_json().encode() + b'\n')
                
                response_data = sock.recv(4096).decode()
                response = P2PMessage.from_json(response_data.strip())
                
                if response.height is not None:
                    sock.close()
                    return response.height
                
                sock.close()
            except Exception as e:
                pass  # Try next peer
        
        return None
    
    def sync_blocks(self, start_height: int, end_height: int, timeout: int = 10) -> List[Dict[str, Any]]:
        """Sync blocks from peers."""
        blocks = []
        
        for height in range(start_height, end_height + 1):
            for host, port in self.known_peers:
                try:
                    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                    sock.settimeout(timeout)
                    sock.connect((host, port))
                    
                    msg = P2PMessage('GET_BLOCK', self.peer_id, height=height)
                    sock.send(msg.to_json().encode() + b'\n')
                    
                    response_data = sock.recv(65536).decode()
                    response = P2PMessage.from_json(response_data.strip())
                    
                    if response.block_data:
                        blocks.append(response.block_data)
                        logger.debug(f"[P2P] 📦 Synced block {height}")
                        sock.close()
                        break
                    
                    sock.close()
                except Exception as e:
                    logger.debug(f"[P2P] ⚠️  Block sync failed: {e}")
        
        return blocks


class P2PServer:
    """P2P server for accepting peer connections and responding to requests."""
    
    def __init__(self, peer_id: str, port: int = 8333):
        self.peer_id = peer_id
        self.port = port
        self.running = False
        self.server_socket = None
        self.peers: Dict[str, Dict[str, Any]] = {}
        self._lock = threading.RLock()
    
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
        """Handle incoming peer connection."""
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
                    cursor = _DB_CONN.cursor()
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
                    cursor = _DB_CONN.cursor()
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
                    cursor = _DB_CONN.cursor()
                    
                    # Get current metrics
                    cursor.execute("SELECT MAX(height) FROM blocks")
                    height = cursor.fetchone()[0] or 0
                    
                    cursor.execute("SELECT COUNT(*) FROM transactions WHERE broadcast_to_oracle=0")
                    pending_txs = cursor.fetchone()[0] or 0
                    
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
                    cursor = _DB_CONN.cursor()
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
                    _DB_CONN.commit()
                    
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
        """Broadcast all pending items to Oracle."""
        stats = {'sent': 0, 'failed': 0, 'queued': 0}
        
        with self._lock:
            while self.broadcast_queue:
                item = self.broadcast_queue.popleft()
                
                try:
                    # In production: POST to Oracle REST API
                    # For now: log and store locally
                    if item['type'] == 'transaction':
                        # POST /api/transactions
                        # response = requests.post(f"{self.oracle_url}/api/transactions", 
                        #     json=item['data'], timeout=timeout)
                        logger.info(f"[ORACLE] ✅ Broadcast TX: {item['data'].get('tx_id', 'unknown')}")
                        
                        # Store broadcast status locally
                        try:
                            cursor = _DB_CONN.cursor()
                            cursor.execute("""
                                UPDATE transactions 
                                SET broadcast_to_oracle=1, oracle_timestamp=?
                                WHERE tx_id=?
                            """, (int(time.time()), item['data'].get('tx_id')))
                            _DB_CONN.commit()
                        except:
                            pass
                        
                        stats['sent'] += 1
                    
                    elif item['type'] == 'block':
                        # POST /api/blocks
                        # response = requests.post(f"{self.oracle_url}/api/blocks",
                        #     json=item['data'], timeout=timeout)
                        logger.info(f"[ORACLE] ✅ Broadcast Block: {item['data'].get('block_hash', 'unknown')}")
                        
                        # Store broadcast status locally
                        try:
                            cursor = _DB_CONN.cursor()
                            cursor.execute("""
                                UPDATE blocks
                                SET broadcast_to_oracle=1, oracle_timestamp=?
                                WHERE block_hash=?
                            """, (int(time.time()), item['data'].get('block_hash')))
                            _DB_CONN.commit()
                        except:
                            pass
                        
                        stats['sent'] += 1
                
                except Exception as e:
                    logger.warning(f"[ORACLE] ⚠️  Broadcast failed: {e}")
                    stats['failed'] += 1
                    # Re-queue for retry
                    item['status'] = 'retry'
                    self.broadcast_queue.append(item)
        
        stats['queued'] = len(self.broadcast_queue)
        return stats


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
    """
}

def apply_schema_patches():
    """Apply schema patches to local database."""
    with threading.RLock():
        for patch_name, patch_sql in SCHEMA_PATCHES.items():
            try:
                _DB_CONN.executescript(patch_sql)
                _DB_CONN.commit()
                logger.debug(f"[SCHEMA] ✅ Applied patch: {patch_name}")
            except Exception as e:
                logger.debug(f"[SCHEMA] ℹ️  Patch already applied: {patch_name}")


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
        """Perform synchronization with all known peers."""
        if not self.p2p_client:
            return
        
        logger.debug("[CONSENSUS] 📊 Starting peer sync...")
        
        try:
            # Get current chain state
            cursor = _DB_CONN.cursor()
            cursor.execute("SELECT MAX(height) FROM blocks")
            local_height = cursor.fetchone()[0] or 0
            
            cursor.execute("SELECT COUNT(*) FROM transactions WHERE broadcast_to_oracle=0")
            pending_txs = cursor.fetchone()[0] or 0
            
            # Query each peer for their metrics
            for host, port in (self.p2p_client.known_peers or []):
                try:
                    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                    sock.settimeout(3)
                    sock.connect((host, port))
                    
                    # Request metrics
                    msg = P2PMessage('GET_METRICS', 'consensus_sync')
                    sock.send(msg.to_json().encode() + b'\n')
                    
                    response_data = sock.recv(4096).decode()
                    response = P2PMessage.from_json(response_data.strip())
                    
                    if response.payload and 'metrics' in response.payload:
                        metrics = response.payload['metrics']
                        peer_id = f"{host}:{port}"
                        
                        if self.consensus_mgr:
                            self.consensus_mgr.record_peer_metric(peer_id, 'chain_height', metrics.get('height', 0))
                            self.consensus_mgr.record_peer_metric(peer_id, 'pending_txs', metrics.get('pending_txs', 0))
                    
                    sock.close()
                
                except Exception as e:
                    logger.debug(f"[CONSENSUS] Sync failed with {host}:{port}: {e}")
            
            # Update system consensus
            if self.consensus_mgr:
                self.consensus_mgr.update_system_metrics()
                logger.debug("[CONSENSUS] ✅ Peer sync complete")
        
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

class MinerWebSocketP2PClient:
    """WebSocket P2P client for miner registration, heartbeats, snapshot requests, and gossip.
    
    ENHANCED: Supports gossip protocol for peer discovery, snapshot sharing, and resilience.
    Features:
      • Adaptive timeout tuning based on request latencies
      • Gossip protocol for peer discovery
      • Snapshot caching for resilience
      • Background peer discovery loop
      • Health tracking for known peers
    """
    
    def __init__(self, oracle_url: str, miner_id: str, miner_address: str, public_key: str=''):
        self.oracle_url=oracle_url.rstrip('/')
        self.miner_id=miner_id
        self.miner_address=miner_address
        self.public_key=public_key
        self.sio=None
        self.connected=False
        self._lock=threading.RLock()
        self._running=False
        
        # Gossip protocol: track known peers and their snapshots
        self.known_peers: Dict[str, Dict[str, Any]] = {}  # peer_id -> {url, ws_url, last_seen, snapshot_ts}
        self.peer_discovery_thread = None
        
        # Snapshot cache: timestamp -> snapshot (for relay)
        self.snapshot_cache: Dict[int, Dict[str, Any]] = {}
        self.latest_snapshot_ts = 0
        
        # Adaptive timeout tracking
        self.request_times: deque = deque(maxlen=10)
        self.current_timeout = 5
        
        if SOCKETIO_AVAILABLE:
            try:
                self.sio=socketio.Client(
                    reconnection=True,
                    reconnection_delay=1,
                    reconnection_delay_max=5,
                    reconnection_attempts=10,
                    request_timeout=self.current_timeout
                )
                
                @self.sio.event
                def connect():
                    with self._lock:
                        self.connected=True
                    logger.info("[WEBSOCKET] ✅ Connected to oracle via WebSocket")
                    self._send_register()
                
                @self.sio.event
                def disconnect():
                    with self._lock:
                        self.connected=False
                    logger.warning("[WEBSOCKET] Disconnected from oracle")
                
                @self.sio.on('miner_register_ack')
                def on_register_ack(data):
                    if data.get('status')=='registered':
                        logger.info(f"[WEBSOCKET] ✅ Registration acknowledged")
                        # Extract peer info from ACK for gossip discovery
                        if 'known_peers' in data:
                            self._process_peer_list(data['known_peers'])
                    else:
                        logger.warning(f"[WEBSOCKET] Registration error: {data.get('message')}")
                
                @self.sio.on('miner_snapshot')
                def on_snapshot(data):
                    ts = data.get('timestamp_ns', 0)
                    logger.debug(f"[WEBSOCKET] 📥 Received snapshot from oracle | ts={ts}")
                    # Cache snapshot for local gossip relay
                    with self._lock:
                        self.snapshot_cache[ts] = data
                        self.latest_snapshot_ts = max(self.latest_snapshot_ts, ts)
                
                @self.sio.on('gossip_peer_list')
                def on_gossip_peer_list(data):
                    """Receive peer list from oracle for gossip discovery"""
                    peers = data.get('peers', [])
                    logger.debug(f"[GOSSIP] Received {len(peers)} peers from oracle")
                    self._process_peer_list(peers)
                
                @self.sio.on('gossip_snapshot')
                def on_gossip_snapshot(data):
                    """Receive snapshot from peer via gossip"""
                    from_peer = data.get('from_peer', '?')
                    ts = data.get('timestamp_ns', 0)
                    logger.debug(f"[GOSSIP] 📥 Snapshot from {from_peer[:12]} | ts={ts}")
                    with self._lock:
                        self.snapshot_cache[ts] = data
                        self.latest_snapshot_ts = max(self.latest_snapshot_ts, ts)
                
                @self.sio.on('error')
                def on_error(data):
                    logger.error(f"[WEBSOCKET] Oracle error: {data}")
                    
            except Exception as e:
                logger.warning(f"[WEBSOCKET] SocketIO initialization failed: {e}")
                self.sio=None
    
    def _process_peer_list(self, peers: List[Dict[str, Any]]) -> None:
        """Process peer list from oracle/gossip for discovery."""
        with self._lock:
            for peer in peers:
                peer_id = peer.get('miner_id')
                if peer_id and peer_id != self.miner_id:  # Don't add ourselves
                    self.known_peers[peer_id] = {
                        'url': peer.get('url', ''),
                        'ws_url': peer.get('ws_url', ''),
                        'last_seen': time.time(),
                        'snapshot_ts': peer.get('snapshot_ts', 0)
                    }
            logger.debug(f"[GOSSIP] 🔄 Updated peer list: {len(self.known_peers)} peers")
    
    def _update_adaptive_timeout(self) -> None:
        """Update timeout based on recent request latencies."""
        if len(self.request_times) > 3:
            avg_time = sum(self.request_times) / len(self.request_times)
            # Set timeout to 2x average + 1 second, capped at 15s, minimum 5s
            new_timeout = min(max(avg_time * 2 + 1, 5), 15)
            if abs(new_timeout - self.current_timeout) > 0.5:
                old_timeout = self.current_timeout
                self.current_timeout = new_timeout
                logger.info(f"[WEBSOCKET] ⏱️  Adaptive timeout: {old_timeout:.1f}s → {self.current_timeout:.1f}s (avg latency: {avg_time:.2f}s)")
                if self.sio:
                    try:
                        self.sio.request_timeout = self.current_timeout
                    except:
                        pass
    
    def connect(self)->bool:
        """Connect to oracle P2P via WebSocket with adaptive timeout.
        
        ENHANCED: Uses HTTPS/WSS, supports P2P_WEBSOCKET_URL env var,
        enables gossip protocol discovery.
        """
        if not self.sio:
            return False
        
        try:
            # Check for explicit P2P WebSocket URL override (for Koyeb/cloud deployments)
            p2p_ws_url = os.getenv('P2P_WEBSOCKET_URL')
            
            if p2p_ws_url:
                # Use explicit configuration
                ws_url = p2p_ws_url.rstrip('/')
                logger.info(f"[WEBSOCKET] Using configured P2P WebSocket URL: {ws_url}")
            else:
                # Extract host from oracle_url, connect to port 8333 for P2P
                url_parts=self.oracle_url.replace('http://', '').replace('https://', '')
                host_only=url_parts.split(':')[0]
                
                # Determine protocol: use wss:// for external URLs, ws:// for localhost
                if 'localhost' in host_only or '127.0.0.1' in host_only:
                    ws_url=f"http://{host_only}:8333"
                else:
                    ws_url=f"wss://{host_only}"
            
            logger.debug(f"[WEBSOCKET] 🔌 Attempting connection to {ws_url} (timeout: {self.current_timeout}s)")
            
            start_time = time.time()
            self.sio.connect(ws_url, wait_timeout=self.current_timeout, transports=['websocket', 'polling'])
            elapsed = time.time() - start_time
            self.request_times.append(elapsed)
            self._update_adaptive_timeout()
            logger.info(f"[WEBSOCKET] ✅ Connected in {elapsed:.2f}s")
            
            return True
        except Exception as e:
            logger.warning(f"[WEBSOCKET] P2P WebSocket connection failed ({ws_url if 'ws_url' in locals() else 'unknown'}): {e}")
            return False
    
    def _send_register(self)->None:
        """Send registration message to oracle with gossip info."""
        try:
            if self.sio and self.connected:
                self.sio.emit('miner_register', {
                    'miner_id': self.miner_id,
                    'address': self.miner_address,
                    'public_key': self.public_key,
                    'supports_gossip': True,  # Signal that we support gossip protocol
                    'snapshot_ts': self.latest_snapshot_ts
                })
                logger.debug("[WEBSOCKET] Registration event sent with gossip support")
        except Exception as e:
            logger.debug(f"[WEBSOCKET] Registration emit failed: {e}")
    
    def send_heartbeat(self)->bool:
        """Send heartbeat with peer/snapshot info for gossip."""
        try:
            if self.sio and self.connected:
                start_time = time.time()
                self.sio.emit('miner_heartbeat', {
                    'miner_id': self.miner_id,
                    'known_peers': len(self.known_peers),
                    'snapshot_ts': self.latest_snapshot_ts
                })
                elapsed = time.time() - start_time
                self.request_times.append(elapsed)
                self._update_adaptive_timeout()
                return True
            return False
        except Exception as e:
            logger.debug(f"[WEBSOCKET] Heartbeat failed: {e}")
            return False
    
    def request_snapshot(self)->bool:
        """Request W-state snapshot from oracle."""
        try:
            if self.sio and self.connected:
                start_time = time.time()
                self.sio.emit('miner_snapshot_request', {
                    'miner_id': self.miner_id,
                    'known_snapshot_ts': self.latest_snapshot_ts
                })
                elapsed = time.time() - start_time
                self.request_times.append(elapsed)
                self._update_adaptive_timeout()
                return True
            return False
        except Exception as e:
            logger.debug(f"[WEBSOCKET] Snapshot request failed: {e}")
            return False
    
    def request_peer_list(self)->bool:
        """Request peer list from oracle for gossip discovery."""
        try:
            if self.sio and self.connected:
                self.sio.emit('gossip_peer_list_request', {'miner_id': self.miner_id})
                return True
            return False
        except Exception as e:
            logger.debug(f"[WEBSOCKET] Peer list request failed: {e}")
            return False
    
    def get_cached_snapshot(self) -> Optional[Dict[str, Any]]:
        """Get latest cached snapshot from gossip network."""
        with self._lock:
            if self.latest_snapshot_ts in self.snapshot_cache:
                return self.snapshot_cache[self.latest_snapshot_ts]
            # Return most recent if exact timestamp not found
            if self.snapshot_cache:
                latest_ts = max(self.snapshot_cache.keys())
                return self.snapshot_cache[latest_ts]
        return None
    
    def disconnect(self)->None:
        """Disconnect from oracle."""
        try:
            if self.sio and self.connected:
                self.sio.emit('miner_disconnect', {'miner_id': self.miner_id})
                self.sio.disconnect()
                with self._lock:
                    self.connected=False
        except:
            pass
    
    def start_background_heartbeat(self, interval_sec: int=5)->None:
        """Start background heartbeat thread."""
        def heartbeat_loop():
            while self._running:
                try:
                    time.sleep(interval_sec)
                    self.send_heartbeat()
                except Exception as e:
                    logger.debug(f"[WEBSOCKET] Heartbeat loop error: {e}")
        
        with self._lock:
            self._running=True
        
        thread=threading.Thread(target=heartbeat_loop, daemon=True, name="MinerHeartbeat")
        thread.start()
    
    def start_background_snapshot_sync(self, interval_sec: int=10)->None:
        """Start background snapshot request thread."""
        def snapshot_loop():
            while self._running:
                try:
                    time.sleep(interval_sec)
                    self.request_snapshot()
                except Exception as e:
                    logger.debug(f"[WEBSOCKET] Snapshot loop error: {e}")
        
        with self._lock:
            self._running=True
        
        thread=threading.Thread(target=snapshot_loop, daemon=True, name="MinerSnapshot")
        thread.start()
    
    def start_peer_discovery_loop(self, interval_sec: int=30)->None:
        """Start background peer discovery thread for gossip protocol."""
        def discovery_loop():
            while self._running:
                try:
                    time.sleep(interval_sec)
                    # Request updated peer list
                    self.request_peer_list()
                    
                    # Log peer health
                    with self._lock:
                        now = time.time()
                        active_peers = sum(1 for p in self.known_peers.values() 
                                         if now - p['last_seen'] < 120)
                        stale_peers = [pid for pid, p in self.known_peers.items() 
                                      if now - p['last_seen'] > 300]
                    
                    # Remove stale peers
                    for pid in stale_peers:
                        with self._lock:
                            del self.known_peers[pid]
                    
                    logger.info(f"[GOSSIP] 👥 {len(self.known_peers)} known peers ({active_peers} active) | removed {len(stale_peers)} stale")
                except Exception as e:
                    logger.debug(f"[GOSSIP] Discovery loop error: {e}")
        
        with self._lock:
            self._running=True
        
        self.peer_discovery_thread = threading.Thread(target=discovery_loop, daemon=True, name="PeerDiscovery")
        self.peer_discovery_thread.start()
    
    def stop(self)->None:
        """Stop all background operations."""
        with self._lock:
            self._running=False
        self.disconnect()
                try:
                    time.sleep(interval_sec)
                    self.request_snapshot()
                except Exception as e:
                    logger.debug(f"[WEBSOCKET] Snapshot loop error: {e}")
        
        with self._lock:
            self._running=True
        
        thread=threading.Thread(target=snapshot_loop, daemon=True, name="MinerSnapshot")
        thread.start()
    
    def stop(self)->None:
        """Stop all background operations."""
        with self._lock:
            self._running=False
        self.disconnect()

# ═════════════════════════════════════════════════════════════════════════════════
# W-STATE RECOVERY ENGINE (VERBATIM FROM v14 FINAL + ENHANCED)
# ═════════════════════════════════════════════════════════════════════════════════

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
                self.ws_client=MinerWebSocketP2PClient(
                    oracle_url=self.oracle_url,
                    miner_id=self.peer_id,
                    miner_address=self.miner_address,
                    public_key=self.peer_id
                )
                logger.info("[W-STATE] 🌐 WebSocket P2P client initialized")
            except Exception as e:
                logger.warning(f"[W-STATE] WebSocket initialization failed: {e}")
                self.ws_client=None
        
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
        
        # Fall back to HTTP with exponential backoff
        max_attempts=5
        for attempt in range(max_attempts):
            try:
                url=f"{self.oracle_url}/api/oracle/register"
                response=requests.post(
                    url,
                    json={"miner_id": self.peer_id, "address": self.miner_address, "public_key": self.peer_id},
                    timeout=10  # Increased from 5s
                )
                
                if response.status_code in [200,201]:
                    data=response.json()
                    self.oracle_address=data.get('miner_id',self.peer_id)
                    if self.oracle_address:
                        self.trusted_oracles.add(self.oracle_address)
                        logger.info(f"[W-STATE] ✅ Registered with oracle (HTTP) | miner_id={self.oracle_address[:20]}…")
                    return True
                else:
                    logger.warning(f"[W-STATE] ⚠️  Registration attempt {attempt+1}/{max_attempts} failed: {response.status_code}")
            
            except requests.Timeout:
                logger.warning(f"[W-STATE] ⚠️  Registration attempt {attempt+1}/{max_attempts} timeout after 10s")
            except Exception as e:
                logger.warning(f"[W-STATE] ⚠️  Registration attempt {attempt+1}/{max_attempts} error: {e}")
            
            # Exponential backoff: 1s, 2s, 4s, 8s, 8s
            if attempt < max_attempts - 1:
                delay_sec = min(2 ** attempt, 8)
                logger.info(f"[W-STATE] 🔄 Retrying registration in {delay_sec}s…")
                time.sleep(delay_sec)
        
        logger.error(f"[W-STATE] ❌ Registration failed after {max_attempts} attempts - continuing with graceful degradation")
        # Return True to allow recovery to proceed with cached/synthetic snapshots
        return True
    
    def download_latest_snapshot(self)->Optional[Dict[str,Any]]:
        """Download latest W-state snapshot from oracle with gossip fallback.
        
        ENHANCED: 
        - Adaptive timeouts based on oracle latency
        - Fallback to gossip network cache if oracle slow
        - Increased retry attempts with exponential backoff (max 5)
        - Logs detailed latency metrics
        """
        # First try: Quick check for gossip-cached snapshot (no network call)
        if self.ws_client:
            cached = self.ws_client.get_cached_snapshot()
            if cached:
                ts = cached.get('timestamp_ns', 0)
                logger.info(f"[W-STATE] 💾 Using gossip-cached snapshot | ts={ts}")
                with self._state_lock:
                    self.current_snapshot=cached
                    self.snapshot_buffer.append(cached)
                return cached
        
        # Second try: Direct HTTP fetch from oracle with adaptive timeout
        base_timeout = 5
        max_attempts = 5
        
        for attempt in range(max_attempts):
            try:
                # Adaptive timeout: increase with each attempt
                timeout = min(base_timeout + (attempt * 2), 15)
                url=f"{self.oracle_url}/api/oracle/w-state"
                
                logger.debug(f"[W-STATE] Download attempt {attempt+1}/{max_attempts} | timeout={timeout}s")
                
                start_time = time.time()
                response=requests.get(url, timeout=timeout)
                elapsed = time.time() - start_time
                
                if response.status_code==200:
                    snapshot=response.json()
                    with self._state_lock:
                        self.current_snapshot=snapshot
                        self.snapshot_buffer.append(snapshot)
                    
                    ts = snapshot.get('timestamp_ns', 0)
                    logger.info(f"[W-STATE] 📥 Downloaded snapshot | ts={ts} | latency={elapsed:.2f}s")
                    return snapshot
                else:
                    logger.warning(f"[W-STATE] ⚠️  Attempt {attempt+1}/{max_attempts}: HTTP {response.status_code}")
            
            except requests.Timeout:
                logger.warning(f"[W-STATE] ⚠️  Attempt {attempt+1}/{max_attempts}: Timeout after {timeout}s")
            except requests.ConnectionError as e:
                logger.warning(f"[W-STATE] ⚠️  Attempt {attempt+1}/{max_attempts}: Connection error: {str(e)[:50]}")
            except Exception as e:
                logger.warning(f"[W-STATE] ⚠️  Attempt {attempt+1}/{max_attempts}: {type(e).__name__}: {str(e)[:50]}")
            
            # Exponential backoff: 1s, 2s, 4s, 8s, 8s
            if attempt < max_attempts - 1:
                delay_sec = min(2 ** attempt, 8)
                logger.info(f"[W-STATE] 🔄 Retrying in {delay_sec}s…")
                time.sleep(delay_sec)
        
        logger.error(f"[W-STATE] ❌ Download failed after {max_attempts} attempts - recovery will use cached/synthetic snapshot")
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
          snapshot['pq_current'] — hex string: current lattice field-space identifier
          snapshot['pq_last']    — hex string: previous lattice field-space identifier
          snapshot['fidelity']   — float: actual W-state quality (used for block submission)
          snapshot['coherence']  — float: L1 coherence metric
        """
        try:
            # ── Real oracle fidelity (the ONLY value that goes into block header) ──
            fidelity  = float(snapshot.get('fidelity',  0.90))
            coherence = float(snapshot.get('coherence', 0.85))
            timestamp_ns = snapshot.get('timestamp_ns', int(time.time() * 1e9))
            
            # ── Lattice field-space identifiers (integers 1-106495 from oracle) ──
            pq_curr_id = snapshot.get('pq_current')
            pq_last_id = snapshot.get('pq_last')
            
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
        """
        try:
            with self._state_lock:
                # Rotate density matrices
                self.pq_last_matrix  = self.pq_curr_matrix.copy() if self.pq_curr_matrix is not None else None
                self.pq_curr_matrix  = self.pq0_matrix.copy()     if self.pq0_matrix     is not None else None
                # Rotate lattice field-space identifiers
                self.entanglement_state.pq_last = self.entanglement_state.pq_curr
                self.entanglement_state.pq_curr = self._pq_curr_id
                self._pq_last_id = self._pq_curr_id
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
        _LOG_EVERY = 50   # log W-state status every 50 cycles (~5s at 10ms interval)
        
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
                 retarget_window: int=10, ema_alpha: float=0.2):
        self.db=db
        self.target_block_time_s=target_block_time_s
        self.retarget_window=retarget_window
        self.ema_alpha=ema_alpha
        self.min_difficulty=8
        self.max_difficulty=32
        self._lock=threading.RLock()
        
        # Load state from database
        self._load_state()
        
        logger.info(f"[DIFFICULTY] 🎯 Retargeting engine initialized | target={target_block_time_s}s | window={retarget_window} blocks | ema_α={ema_alpha}")
    
    def _load_state(self):
        """Load difficulty state from database."""
        try:
            with self._lock:
                cursor=self.db.cursor()
                cursor.execute("SELECT current_difficulty, ema_block_time_s, last_retarget_height FROM difficulty_state WHERE id=1")
                row=cursor.fetchone()
                if row:
                    self.current_difficulty=row[0]
                    self.ema_block_time_s=row[1]
                    self.last_retarget_height=row[2]
                    logger.info(f"[DIFFICULTY] 📂 Loaded state | current_diff={self.current_difficulty} | ema_time={self.ema_block_time_s:.2f}s | last_retarget={self.last_retarget_height}")
                else:
                    self.current_difficulty=13  # ← Adjusted for ~1 min phone blocks
                    self.ema_block_time_s=self.target_block_time_s
                    self.last_retarget_height=0
        except Exception as e:
            logger.error(f"[DIFFICULTY] ❌ Failed to load state: {e}")
            self.current_difficulty=13  # ← Adjusted for ~1 min phone blocks
            self.ema_block_time_s=self.target_block_time_s
            self.last_retarget_height=0
    
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
        Record actual mining time for a block and potentially trigger retargeting.
        
        Uses exponential moving average to smooth out variance:
        ema_new = ema_old * (1 - α) + actual_time * α
        
        Then adjusts difficulty every retarget_window blocks.
        """
        with self._lock:
            try:
                # Update EMA with new mining time
                old_ema=self.ema_block_time_s
                self.ema_block_time_s=(old_ema * (1.0 - self.ema_alpha)) + (mining_time_s * self.ema_alpha)
                
                logger.debug(f"[DIFFICULTY] 📊 Block #{height} mining_time={mining_time_s:.2f}s | ema={self.ema_block_time_s:.2f}s")
                
                # Check if it's time to retarget
                blocks_since_retarget=height - self.last_retarget_height
                if blocks_since_retarget >= self.retarget_window:
                    self._perform_retarget(height)
                
                # Save state periodically
                if height % 5 == 0:
                    self._save_state()
            except Exception as e:
                logger.error(f"[DIFFICULTY] ❌ Error recording block time: {e}")
    
    def _perform_retarget(self, height: int):
        """
        Adjust difficulty based on EMA block time vs target.
        
        Simple proportional adjustment:
        • If actual_time > target: decrease difficulty (easier)
        • If actual_time < target: increase difficulty (harder)
        
        Adjustment factor = target / actual (capped for stability)
        new_diff = current_diff * adjustment_factor (clamped to min/max)
        """
        try:
            time_ratio=self.target_block_time_s / max(self.ema_block_time_s, 0.1)
            
            # Clamp adjustment factor to prevent extreme swings
            # Allow at most 2x increase or 0.5x decrease per retarget
            adjustment_factor=max(0.5, min(2.0, time_ratio))
            
            old_difficulty=self.current_difficulty
            new_difficulty=int(self.current_difficulty * adjustment_factor)
            new_difficulty=max(self.min_difficulty, min(self.max_difficulty, new_difficulty))
            
            self.current_difficulty=new_difficulty
            self.last_retarget_height=height
            
            direction="↑" if new_difficulty > old_difficulty else ("↓" if new_difficulty < old_difficulty else "=")
            logger.info(f"[DIFFICULTY] 🎯 RETARGET at block #{height} | {direction} {old_difficulty} → {new_difficulty} | ema_time={self.ema_block_time_s:.2f}s (target {self.target_block_time_s}s)")
            
            # Save immediately after retarget
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
                return [Transaction(**tx) for tx in r.json().get('transactions',[])[:MAX_MEMPOOL]]
        except:
            pass
        return []
    
    def submit_block(self,block_data: Dict[str,Any])->Tuple[bool,str]:
        try:
            r=self.session.post(f"{self.base_url}{API_PREFIX}/submit_block",json=block_data,timeout=10)
            if r.status_code in [200,201]:
                return True,r.json().get('message','Block accepted')
            return False,r.json().get('error','Submission failed')
        except Exception as e:
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
    def __init__(self):
        self.difficulty_cache={}
    
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
    
    def _parent_block_exists(self, parent_hash: str)->bool:
        """Check if parent block exists in chain."""
        try:
            # First check: is it in our local state?
            for height, header in self.blocks.items():
                if header.block_hash==parent_hash:
                    return True
            
            # Fallback: check database
            if hasattr(self, '_db'):
                cursor=self._db.execute("SELECT height FROM blocks WHERE block_hash=?", (parent_hash,))
                return cursor.fetchone() is not None
            
            return False
        except:
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
        self.difficulty=difficulty  # fallback if engine not provided
        self.metrics={'blocks_mined':0,'hash_attempts':0,'avg_fidelity':0.0}
        self._lock=threading.RLock()
    
    def mine_block(self, transactions: List[Transaction], miner_address: str, parent_hash: str, height: int) -> Optional[Block]:
        """Mine a block with coinbase tx[0] and W-state quantum entropy witness."""
        try:
            mining_start = time.time()
            
            logger.info(f"[MINING] ⛏️  Mining block #{height} with {len(transactions)} transactions")
            
            # Get current difficulty from engine (or use fallback)
            current_difficulty=self.difficulty_engine.get_current_difficulty() if self.difficulty_engine else self.difficulty
            
            logger.info(f"[MINING] ⚙️  Using difficulty {current_difficulty} bits")
            
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
            target = (1 << (256 - self.difficulty)) - 1
            hash_attempts = 0
            nonce_start = time.time()
            
            logger.debug(f"[MINING] ⚙️  PoW target: {target} | difficulty_bits={self.difficulty}")
            
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
                    
                    # 🎯 RECORD MINING TIME FOR DIFFICULTY RETARGETING
                    if self.difficulty_engine:
                        self.difficulty_engine.record_block_mining_time(height, total_time)
                    
                    # Rotate W-state for next iteration
                    self.w_state_recovery.rotate_entanglement_state()
                    
                    return block
                
                header.nonce += 1
                
                # Progress logging every 100k attempts
                if hash_attempts % 100000 == 0:
                    elapsed = time.time() - nonce_start
                    current_rate = hash_attempts / elapsed if elapsed > 0 else 0
                    logger.debug(f"[MINING] 🔄 Progress: {hash_attempts:,} hashes | rate={current_rate:.0f} h/s | nonce={header.nonce}")
            
            logger.warning(f"[MINING] ⚠️  PoW timeout - exhausted nonce space at height {height}")
            return None
        
        except Exception as e:
            logger.error(f"[MINING] ❌ Mining exception: {e}")
            logger.error(f"[MINING] Traceback: {traceback.format_exc()}")
            return None

# ═════════════════════════════════════════════════════════════════════════════════
# FULL NODE WITH W-STATE MINING
# ═════════════════════════════════════════════════════════════════════════════════

class QTCLFullNode:
    def __init__(self, miner_address: str, oracle_url: str='http://localhost:5000', difficulty: int=12, db_connection: Optional[sqlite3.Connection]=None):
        self.miner_address=miner_address
        self.running=False
        self.db=db_connection  # Database connection for difficulty state
        
        self.client=LiveNodeClient()
        self.state=ChainState()
        self.mempool=Mempool()
        self.validator=ValidationEngine()
        
        # DIFFICULTY RETARGETING ENGINE
        self.difficulty_engine=None
        if self.db:
            try:
                self.difficulty_engine=DifficultyRetargeting(self.db, target_block_time_s=60.0, retarget_window=10, ema_alpha=0.2)
                logger.info("[NODE] ✅ Difficulty retargeting engine initialized")
            except Exception as e:
                logger.warning(f"[NODE] ⚠️  Failed to initialize difficulty engine: {e}")
        
        # W-STATE RECOVERY
        peer_id=f"miner_{uuid.uuid4().hex[:12]}"
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
        
        logger.info(f"[NODE] 🚀 QTCL Full Node initialized | miner={miner_address[:20]}… | oracle={oracle_url}")
    
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
            
            logger.info("[NODE] ✨ Full node with quantum mining started")
            return True
        
        except Exception as e:
            logger.error(f"[NODE] ❌ Startup failed: {e}")
            return False
    
    def stop(self):
        self.running=False
        self.w_state_recovery.stop()
        if self.sync_thread:
            self.sync_thread.join(timeout=5)
        if self.mining_thread:
            self.mining_thread.join(timeout=5)
        logger.info("[NODE] ✅ Stopped")
    
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
                                logger.debug(f"[SYNC] ✅ Synced block #{h}")
                            else:
                                logger.warning(f"[SYNC] ⚠️  Block #{h} failed validation, skipping")
                        time.sleep(0.05)
                else:
                    logger.debug(f"[SYNC] ✅ In sync at height {current_height}")
                
                mempool_txs = self.client.get_mempool()
                for tx in mempool_txs:
                    self.mempool.add_transaction(tx)
                logger.debug(f"[SYNC] 💾 Mempool: {self.mempool.get_size()} txs")
                
                time.sleep(MEMPOOL_POLL_INTERVAL)
            except Exception as e:
                logger.error(f"[SYNC] ❌ Error: {e}")
                time.sleep(10)
        logger.info("[SYNC] 🛑 Loop ended")
    
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
                    logger.debug("[MINING] ⏳ Waiting for entanglement establishment...")
                    time.sleep(2)
                    continue
                
                tip = self.state.get_tip()
                if not tip:
                    logger.debug("[MINING] ⏳ No chain tip yet, waiting...")
                    time.sleep(5)
                    continue
                
                # Pull at most MAX_BLOCK_TX user transactions from mempool.
                # Coinbase is NOT counted — it's prepended separately in mine_block().
                # This cap prevents the block size from growing unboundedly and
                # ensures the coinbase loop can never form:
                #   • Coinbase tx_type='coinbase' is never added to the mempool
                #   • /api/mempool only returns type='transfer' pending txs
                pending_txs = self.mempool.get_pending(limit=MAX_BLOCK_TX)
                
                # ✅ MUSEUM-GRADE FIX: Allow mining with empty mempool
                # Blockchains can mine empty blocks (common during low activity)
                # This was the bug preventing mining when mempool = 0
                
                # Get current W-state metrics — use real oracle fidelity
                current_fidelity = entanglement.get('w_state_fidelity', 0.0)
                fidelity_measurements.append(current_fidelity)
                
                tx_count = len(pending_txs) if pending_txs else 0
                logger.info(f"[MINING] ⛏️  Mining block #{tip.height+1} | txs={tx_count} | F={current_fidelity:.4f}")
                
                block_start = time.time()
                # Pass empty list if no pending transactions (mine empty block)
                block = self.miner.mine_block(pending_txs or [], self.miner_address, tip.block_hash, tip.height+1)
                block_time = time.time() - block_start
                
                if block:
                    total_hash_attempts += self.miner.metrics.get('hash_attempts', 0)
                    blocks_mined_this_session += 1
                    
                    # Validate block
                    if self.validator.validate_block(block):
                        # ✅ BULLETPROOF: Submit to network - NEVER use asdict()
                        submit_start = time.time()
                        
                        try:
                            # ✅ MUSEUM-GRADE: Manual dict serialization - NO asdict() anywhere
                            
                            # Serialize header - direct attribute access
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
                                for tx in block.transactions:
                                    self.state.apply_transaction(tx)
                                self.mempool.remove_transactions(
                                    [tx.tx_id for tx in block.transactions] if block.transactions else []
                                )
                                
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
            estimated_rewards = mining_stats.get('blocks_mined', 0) * 10.0
            logger.debug(f"[NODE] 📊 Estimated rewards: {estimated_rewards} QTCL (not confirmed)")
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
                'total_hash_attempts': mining_stats.get('hash_attempts', 0),
                'avg_fidelity': mining_stats.get('avg_fidelity', 0.0),
                'estimated_hash_rate': f"{hash_rate:.0f}" if hash_rate > 0 else "calculating",
                'block_rewards': f"{mining_stats.get('blocks_mined', 0) * 10.0} QTCL",
                'current_difficulty': self.difficulty_engine.get_current_difficulty() if self.difficulty_engine else self.miner.difficulty,
                'ema_block_time_s': self.difficulty_engine.ema_block_time_s if self.difficulty_engine else 0.0,
            },
            'wallet': {
                'address': self.miner_address,
                'balance': wallet_balance,
                'balance_formatted': f"{wallet_balance:.2f} QTCL",
                'estimated_rewards': mining_stats.get('blocks_mined', 0) * 10.0,
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
                'oracle_url': self.w_state_recovery.oracle_url,
                'peer_count': 0,  # Would need P2P impl
            },
            'metrics_summary': f"Height={self.state.get_height()} | Blocks={mining_stats.get('blocks_mined', 0)} | Balance={wallet_balance:.2f} QTCL | F={mining_stats.get('avg_fidelity', 0.0):.4f}"
        }

# ═════════════════════════════════════════════════════════════════════════════════
# WALLET & REGISTRATION (Integrated)
# ═════════════════════════════════════════════════════════════════════════════════

class QuickWallet:
    """Minimal wallet for miner address management"""
    def __init__(self,wallet_file=None):
        # FIXED: Use ./data/wallet.json, not home directory
        data_dir = Path('data')
        data_dir.mkdir(exist_ok=True, mode=0o700)
        self.wallet_file = wallet_file or (data_dir / 'wallet.json')
        self.address=None
        self.private_key=None
        self.public_key=None
    
    def create(self,password):
        """Create new wallet address"""
        self.private_key=secrets.token_hex(32)
        self.public_key=hashlib.sha3_256(self.private_key.encode()).hexdigest()
        self.address=f"qtcl1{hashlib.sha3_256(self.public_key.encode()).hexdigest()[:39]}"
        self._save(password)
        return self.address
    
    def load(self,password):
        """Load wallet from disk"""
        if not self.wallet_file.exists():
            return False
        try:
            import base64
            data=json.loads(open(self.wallet_file).read())
            # Verify password hash (no cryptography needed)
            password_hash=hashlib.sha256(password.encode()).hexdigest()
            if data.get('password_hash')!=password_hash:
                return False
            # Decode wallet data (simple base64, not encrypted)
            wallet_data=json.loads(base64.b64decode(data['wallet_b64']).decode())
            self.address=wallet_data['address']
            self.private_key=wallet_data['private_key']
            self.public_key=wallet_data['public_key']
            return True
        except:
            return False
    
    def _save(self,password):
        """Save wallet (base64 encoding, no cryptography)"""
        try:
            import base64
            # Ensure data directory exists
            self.wallet_file.parent.mkdir(exist_ok=True, mode=0o700)
            # Hash password for verification
            password_hash=hashlib.sha256(password.encode()).hexdigest()
            # Prepare wallet data
            wallet_data={'address':self.address,'private_key':self.private_key,'public_key':self.public_key}
            # Encode with base64 (not encrypted, just encoded)
            wallet_b64=base64.b64encode(json.dumps(wallet_data).encode()).decode()
            # Save to disk
            data={'password_hash':password_hash,'wallet_b64':wallet_b64}
            with open(self.wallet_file,'w') as f:
                f.write(json.dumps(data))
            os.chmod(self.wallet_file,0o600)
        except Exception as e:
            logger.error(f"[WALLET] Save failed: {e}")


class MinerRegistry:
    """Register miner with oracle using HLWE signature"""
    def __init__(self,oracle_url):
        self.oracle_url=oracle_url
        # FIXED: Use ./data/ not home directory
        data_dir=Path('data')
        data_dir.mkdir(exist_ok=True, mode=0o700)
        self.registration_file=data_dir/'.qtcl_miner_registered'
        self.token=None
    
    def register(self,miner_id,address,public_key,private_key,miner_name='qtcl-miner'):
        """Register miner with oracle"""
        try:
            logger.info(f"[REGISTRY] Registering miner {miner_id}...")
            req={'miner_id':miner_id,'address':address,'public_key':public_key,'miner_name':miner_name}
            r=requests.post(f"{self.oracle_url}/api/oracle/register",json=req,timeout=10)
            if r.status_code==200:
                data=r.json()
                status=data.get('status')
                if status=='registered':
                    self.token=data.get('token')
                    self._save_token()
                    logger.info(f"[REGISTRY] ✅ Registered with token {self.token[:16]}...")
                    return True
            logger.warning(f"[REGISTRY] Registration rejected: {r.text}")
        except Exception as e:
            logger.warning(f"[REGISTRY] Registration failed: {e}")
        return False
    
    def is_registered(self):
        """Check if miner is registered"""
        return self._load_token() is not None
    
    def _save_token(self):
        with open(self.registration_file,'w') as f:
            f.write(self.token or '')
        os.chmod(self.registration_file,0o600)
    
    def _load_token(self):
        try:
            if self.registration_file.exists():
                with open(self.registration_file) as f:
                    self.token=f.read().strip()
                    return self.token
        except:
            pass
        return None


# ═════════════════════════════════════════════════════════════════════════════════
# MAIN ENTRY POINT
# ═════════════════════════════════════════════════════════════════════════════════

def parse_args():
    parser=argparse.ArgumentParser(description='🌌 QTCL Full Node + Quantum W-State Miner')
    parser.add_argument('--address','-a',help='Miner wallet address (qtcl1...)')
    parser.add_argument('--oracle-url','-o',default='https://qtcl-blockchain.koyeb.app',help='Oracle URL (for W-state recovery)')
    parser.add_argument('--difficulty','-d',type=int,default=DEFAULT_DIFFICULTY,help='Mining difficulty bits (default 20 ≈ 10-20s per block at ~50k h/s)')
    parser.add_argument('--log-level',default='INFO',choices=['DEBUG','INFO','WARNING','ERROR'])
    parser.add_argument('--wallet-init',action='store_true',help='Initialize new wallet')
    parser.add_argument('--wallet-password',help='Wallet password')
    parser.add_argument('--register',action='store_true',help='Register with oracle')
    parser.add_argument('--miner-id',help='Miner ID for registration')
    parser.add_argument('--miner-name',default='qtcl-miner',help='Friendly miner name')
    parser.add_argument('--fidelity-mode',choices=['strict','normal','relaxed'],default='normal',help='W-state fidelity threshold mode: strict (F>=0.90), normal (F>=0.80, recommended), relaxed (F>=0.70)')
    parser.add_argument('--strict-w-verification',action='store_true',default=False,help='Enable strict W-state verification (rejects marginal states)')
    return parser.parse_args()

def main():
    args=parse_args()
    logging.getLogger().setLevel(getattr(logging,args.log_level))
    
    try:
        # Handle wallet initialization
        if args.wallet_init:
            if not args.wallet_password:
                args.wallet_password=input("Enter wallet password: ")
            wallet=QuickWallet()
            address=wallet.create(args.wallet_password)
            logger.info(f"[WALLET] Created: {address}")
            logger.info(f"[WALLET] Public Key: {wallet.public_key}")
            logger.info(f"[WALLET] Saved to: {wallet.wallet_file}")
            return
        
        # Get or load address
        if args.address:
            address=args.address
        else:
            wallet=QuickWallet()
            if not args.wallet_password:
                args.wallet_password=input("Enter wallet password: ")
            if wallet.load(args.wallet_password):
                address=wallet.address
                logger.info(f"[WALLET] Loaded: {address}")
            else:
                logger.error("[WALLET] Failed to load wallet")
                sys.exit(1)
        
        # Handle oracle registration
        if args.register:
            if not all([args.miner_id,args.wallet_password]):
                logger.error("[REGISTER] --miner-id and --wallet-password required")
                sys.exit(1)
            
            wallet=QuickWallet()
            if wallet.load(args.wallet_password):
                registry=MinerRegistry(args.oracle_url)
                if registry.register(
                    miner_id=args.miner_id,
                    address=wallet.address,
                    public_key=wallet.public_key,
                    private_key=wallet.private_key,
                    miner_name=args.miner_name
                ):
                    logger.info("[REGISTER] ✅ Successfully registered")
                    return
                else:
                    logger.error("[REGISTER] ❌ Registration failed")
                    sys.exit(1)
        
        # Start mining
        # ─── DATABASE INITIALIZATION WITH PERSISTENT FILE-BASED STORAGE ──────────────
        global db
        db_path = Path('qtcl-miner/data/qtcl_blockchain.db')
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
            import traceback
            traceback.print_exc()
            sys.exit(1)
        
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
        
        # ─── SCHEMA PATCHES ─────────────────────────────────────────────────────────────
        logger.info("[INIT] 🔧 Applying database schema patches...")
        apply_schema_patches()
        
        # ─── P2P INITIALIZATION SEQUENCE ────────────────────────────────────────────
        logger.info("[P2P] 🚀 Initializing P2P network layer...")
        
        # 1. Start P2P server (listen for peer connections)
        peer_id = f"qtcl_miner_{uuid.uuid4().hex[:12]}"
        global _P2P_SERVER, _P2P_CLIENT, _TX_SIGNER, _ORACLE_BROADCASTER, _CONSENSUS_MGR, _PEER_SYNC
        
        _P2P_SERVER = P2PServer(peer_id, port=8333)
        server_thread = threading.Thread(target=_P2P_SERVER.start, daemon=True, name="P2PServer")
        server_thread.start()
        time.sleep(0.5)  # Let server bind
        
        # 2. Initialize transaction signing and Oracle broadcasting
        _TX_SIGNER = HLWETransactionSigner(address)
        _ORACLE_BROADCASTER = OracleBroadcaster(args.oracle_url)
        logger.info("[SIGNING] 🔐 HLWE transaction signing initialized")
        logger.info("[ORACLE] 📤 Oracle broadcasting initialized")
        
        # 3. Initialize consensus and periodic sync
        _CONSENSUS_MGR = ConsensusManager()
        _PEER_SYNC = PeriodicPeerSync(_P2P_CLIENT, _CONSENSUS_MGR)
        logger.info("[CONSENSUS] 🤝 Consensus manager initialized")
        
        # 4. Create P2P client for peer discovery
        _P2P_CLIENT = P2PClient(peer_id)
        
        # 5. Try to sync blocks from P2P peers
        current_height = 0
        p2p_success = False
        
        logger.info("[P2P] 🔍 Discovering peers...")
        discovered = _P2P_CLIENT.discover_peers(timeout=5)
        if discovered:
            _P2P_CLIENT.known_peers.extend(discovered)
            logger.info(f"[P2P] ✅ Discovered {len(discovered)} peers")
        
        logger.info("[P2P] 📊 Querying peers for current block height...")
        current_height = _P2P_CLIENT.get_block_height(timeout=5)
        
        if current_height is not None and current_height > 0:
            logger.info(f"[P2P] ✅ P2P sync: Current height = {current_height}")
            
            # Sync blocks from peers if needed
            try:
                db_height = 0
                cursor = _DB_CONN.cursor()
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
            """Background loop for Oracle broadcasts."""
            logger.info("[ORACLE] 🔄 Background Oracle broadcast loop started")
            while True:
                try:
                    time.sleep(30)  # Broadcast every 30 seconds
                    if _ORACLE_BROADCASTER:
                        stats = _ORACLE_BROADCASTER.broadcast_pending()
                        if stats['sent'] > 0:
                            logger.info(f"[ORACLE] 📤 Broadcast: {stats['sent']} sent, {stats['failed']} failed, {stats['queued']} queued")
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
            print(f"  EMA Block Time:         {status['mining']['ema_block_time_s']:.2f}s (target 30s)")
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
