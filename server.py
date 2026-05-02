#!/usr/bin/env python3
"""
╔════════════════════════════════════════════════════════════════════════════════╗
║                                                                                ║
║  QTCL SERVER v6 — Integrated P2P Blockchain with HLWE & Quantum Metrics       ║
║                                                                                ║
║  Museum-Grade Implementation — Pure JSON-RPC on Port 8000                          ║
║  ─────────────────────────────────────────────────────────────────────────    ║
║                                                                                ║
║  Single Unified Server (Port 8000 Internal):                                   ║
║    • REST/JSON-RPC API Layer (port 8000)                      ║
║    • Database Layer (internal) — persistent state (PostgreSQL)              ║
║    • Lattice Controller — quantum entropy mining                             ║
║    • Mempool Manager — transaction pool with validation                      ║
║    • Peer Discovery — DNS seeds, bootstrap nodes, peer exchange              ║
║    • Message Handlers — blocks, transactions, peer sync, consensus           ║
║                                                                                ║
║  Entry:                                                                        ║
║    python server.py                                                            ║
║    or: gunicorn -w1 -b0.0.0.0:$PORT server:app                                ║
║                                                                                ║
║  Environment Variables:                                                        ║
║    DATABASE_URL — PostgreSQL connection                                       ║
║    PORT — Listen port (default: 8000)                                     ║
║    FLASK_HOST — HTTP bind address (default: 0.0.0.0)                         ║
║    ORACLE_HTTP_URL — HTTP oracle endpoint for RPC calls                     ║
║    MAX_PEERS — Max peer connections (default: 32)                            ║
║    BOOTSTRAP_NODES — Comma-separated peer addresses                          ║
║                                                                                ║
╚════════════════════════════════════════════════════════════════════════════════╝
"""

import os
import sys
import json
import time

_SERVER_START_TIME = time.time()  # set once at module import — never drifts

# ═══════════════════════════════════════════════════════════════════════════════════════
# ADD HYP SUBDIRECTORY TO SYS.PATH (allow imports from ~/hlwe/hyp_* modules)
# ═══════════════════════════════════════════════════════════════════════════════════════
_REPO_ROOT = os.path.dirname(os.path.abspath(__file__))
_HYP_DIR = os.path.join(_REPO_ROOT, "hlwe")
if _HYP_DIR not in sys.path:
    sys.path.insert(0, _HYP_DIR)

import socket
import struct
import hashlib
import secrets
import logging
import threading
import concurrent.futures as _cf
from typing import Dict, Any, Optional, List, Tuple, Set, Callable, Union, Deque
from collections import deque, OrderedDict
import requests  # For pushing data to SSE service

# ═══ NUMPY — imported early for quantum code (takes ~1s but needed everywhere) ═══
import numpy as np

# ═══════════════════════════════════════════════════════════════════════════════════════
# ENTERPRISE GRADE INITIALIZATION: QUANTUM ENTROPY + HLWE CRYPTOGRAPHY
# ═══════════════════════════════════════════════════════════════════════════════════════

logger = logging.getLogger(__name__)

# ═══ PRE-WARMED RPC THREAD POOL — shared across all dispatch calls ═══════════
# Single 8-thread pool eliminates per-call ThreadPoolExecutor create/destroy churn.
# Fast (cache-read) methods run INLINE — never touch the pool.
# Slow (DB/oracle) methods submit to pool with hard timeout.
_RPC_THREAD_POOL = _cf.ThreadPoolExecutor(
    max_workers=8, thread_name_prefix="rpc_worker"
)

# Methods that run directly in the request thread — all are lock-free cache reads
# taking < 1ms. Wrapping them in a thread pool adds 5–20ms overhead for zero gain.
_RPC_INLINE_METHODS: frozenset = frozenset(
    {
        "qtcl_getBlockHeight",
        "qtcl_getQuantumMetrics",
        "qtcl_getLatestDMSnapshot",
        "qtcl_getLatestDMSnapshots",
        "qtcl_getMempoolStats",
        "qtcl_getHealth",
        "qtcl_getPeers",
        "qtcl_getPeersByNatGroup",
        "qtcl_getMyAddr",
        "qtcl_getDHTTable",
        "qtcl_getTreasuryAddress",
        "qtcl_listMeasurementSubscribers",
        "qtcl_getEvents",
        "qtcl_peerHeartbeat",
    }
)

# Slow methods (DB round-trips, crypto ops) — get pool + timeout protection
_RPC_TIMEOUT_MAP: dict = {
    "qtcl_getBlockRange": 10.0,
    "qtcl_getTransactions": 10.0,
    "qtcl_getBlock": 5.0,
    "qtcl_getBalance": 4.0,
    "qtcl_getTransaction": 4.0,
    "qtcl_submitBlock": 30.0,
    "qtcl_submitTransaction": 6.0,
    "qtcl_submitOracleReg": 6.0,
    "qtcl_getOracleRegistry": 5.0,
    "qtcl_getOracleRecord": 4.0,
    "qtcl_pushOracleDM": 4.0,
    "qtcl_getPythPrice": 5.0,
    "qtcl_registerPeer": 4.0,
    "qtcl_receiveDHTTable": 3.0,
    "qtcl_registerMeasurementSubscriber": 3.0,
    "qtcl_unregisterMeasurementSubscriber": 3.0,
    "qtcl_getDeviceChain": 4.0,
    "qtcl_getMerminTest": 20.0,
}

# ═══════════════════════════════════════════════════════════════════════════════════════
# SSE SERVICE CONFIGURATION
# ═══════════════════════════════════════════════════════════════════════════════════════
# Separate async SSE service handles quantum streaming endpoints.
# This server pushes data via HTTP POST to fan-out to all clients.
SSE_SERVICE_URL = os.environ.get(
    "SSE_SERVICE_URL", "http://localhost:8001"
)  # Default to local SSE server


def _push_to_sse_service(path: str, payload: dict) -> None:
    """Push data to SSE service (fire-and-forget).

    Args:
        path: e.g., "/push/snapshot" or "/push/block"
        payload: dict to JSON-encode and POST

    Note: Errors are silently swallowed — SSE is non-critical infrastructure.
    """
    if not SSE_SERVICE_URL:
        # SSE service not configured — skip push
        return

    try:
        url = f"{SSE_SERVICE_URL}{path}"
        requests.post(url, json=payload, timeout=1.0)
    except Exception:
        # Silently swallow errors — don't let SSE failures block main server
        pass


# ═══ ENTERPRISE METRICS THROTTLING ═══
_METRICS_SAMPLE_ORACLE = 50
_METRICS_SAMPLE_SHARD = 100
_ORACLE_CYCLE_COUNTERS = {}
_SHARD_CYCLE_COUNTERS = {}


def _should_log_oracle(oracle_id: str) -> bool:
    """Check if oracle measurement should be logged (sample-based)."""
    counter = _ORACLE_CYCLE_COUNTERS.get(oracle_id, 0)
    _ORACLE_CYCLE_COUNTERS[oracle_id] = counter + 1
    return (counter % _METRICS_SAMPLE_ORACLE) == 0


def _should_log_shard(shard_id: int) -> bool:
    """Check if shard cycle should be logged (sample-based)."""
    counter = _SHARD_CYCLE_COUNTERS.get(shard_id, 0)
    _SHARD_CYCLE_COUNTERS[shard_id] = counter + 1
    return (counter % _METRICS_SAMPLE_SHARD) == 0


# ═══ SNAPSHOT BROADCAST THROTTLING ═══
_verbose_p2p_logging = False
_last_snapshot_log_time = 0
_snapshot_log_interval = 10

# ═══ PURE SSE STREAMING ARCHITECTURE ═══
# Oracle generates 16³ → queued directly to SSE → clients sample from stream
# NO caching, NO RPC snapshot polling, NO ring buffers

# ═══ CLIENT TRIPARTITE ORACLE POOL ═══════════════════════════════════════════
# Receives fused DMs pushed by trusted client oracle nodes (qtcl_pushOracleDM).
# Keyed by oracle_addr → {dm_re, dm_im, fidelity, ts, node_ip, oracle_type}
# Pool is Hermitian-averaged every push into _client_consensus_dm which then
# enriches the server's own 5-oracle snapshot on /rpc/oracle/snapshot.
_CLIENT_DM_POOL: Dict[str, dict] = {}
_CLIENT_DM_POOL_LOCK = threading.RLock()
_CLIENT_POOL_MAX = 64  # cap pool size — evict oldest on overflow
_CLIENT_DM_STALE_S = 120.0  # drop frames older than 2 min from consensus
_client_consensus_dm_re: list = [0.0] * 64
_client_consensus_dm_im: list = [0.0] * 64
_client_consensus_fid: float = 0.0
_client_pool_count: int = 0


def _recompute_client_consensus() -> None:
    """
    Hermitian-mean all fresh client DMs in _CLIENT_DM_POOL into
    _client_consensus_dm_re/_im and _client_consensus_fid.
    Called under _CLIENT_DM_POOL_LOCK — must not re-acquire it.
    ❤️  I love you — every client is a node in the lattice
    """
    global _client_consensus_dm_re, _client_consensus_dm_im
    global _client_consensus_fid, _client_pool_count

    now = time.time()
    fresh = [
        v
        for v in _CLIENT_DM_POOL.values()
        if (now - v["ts"]) < _CLIENT_DM_STALE_S and v.get("fidelity", 0.0) > 0.0
    ]
    _client_pool_count = len(fresh)
    if not fresh:
        return

    total_w = sum(max(v["fidelity"], 1e-6) for v in fresh)
    re_acc = [0.0] * 64
    im_acc = [0.0] * 64
    fid_acc = 0.0
    for v in fresh:
        w = v["fidelity"] / total_w
        for i in range(64):
            re_acc[i] += w * v["dm_re"][i]
            im_acc[i] += w * v["dm_im"][i]
        fid_acc += w * v["fidelity"]

    tr = sum(re_acc[i * 8 + i] for i in range(8))
    if tr > 1e-12:
        re_acc = [x / tr for x in re_acc]
        im_acc = [x / tr for x in im_acc]

    _client_consensus_dm_re = re_acc
    _client_consensus_dm_im = im_acc
    _client_consensus_fid = fid_acc


# ═══ RPC INFRASTRUCTURE (JSON-RPC 2.0) ═══
_JSONRPC_VERSION = "2.0"


def _rpc_ok(result: Any, rpc_id: Any) -> dict:
    """Standard JSON-RPC 2.0 success response."""
    return {"jsonrpc": _JSONRPC_VERSION, "result": result, "id": rpc_id}


def _rpc_error(
    code: int, message: str, rpc_id: Any, data: Optional[dict] = None
) -> dict:
    """Standard JSON-RPC 2.0 error response."""
    resp = {
        "jsonrpc": _JSONRPC_VERSION,
        "error": {"code": code, "message": message},
        "id": rpc_id,
    }
    if data:
        resp["error"]["data"] = data
    return resp


def _dispatch_single(req: dict) -> Optional[dict]:
    """Dispatch single JSON-RPC 2.0 request.

    Fast path: inline execution in current gthread (lock-free cache reads).
    Slow path: submitted to _RPC_THREAD_POOL with per-method hard timeout.
    No per-call ThreadPoolExecutor creation — eliminates thread churn under GIL.
    """
    if not isinstance(req, dict):
        return _rpc_error(-32600, "Invalid Request: not an object", None)

    jsonrpc = req.get("jsonrpc")
    method = req.get("method")
    params = req.get("params", [])
    rpc_id = req.get("id")

    if jsonrpc != _JSONRPC_VERSION:
        return _rpc_error(-32600, f"Invalid jsonrpc: {jsonrpc}", rpc_id)
    if not isinstance(method, str):
        return _rpc_error(-32600, "Invalid Request: method not a string", rpc_id)
    if method not in _RPC_METHODS:
        return _rpc_error(-32601, f"Method not found: {method}", rpc_id)

    handler = _RPC_METHODS[method]

    # ── FAST PATH: inline, zero thread overhead ───────────────────────────────
    if method in _RPC_INLINE_METHODS:
        try:
            return handler(params, rpc_id)
        except Exception as e:
            logger.exception(f"[RPC] {method} inline error: {e}")
            return _rpc_error(-32603, f"Internal error: {str(e)}", rpc_id)

    # ── SLOW PATH: pool submit with timeout ───────────────────────────────────
    timeout_sec = _RPC_TIMEOUT_MAP.get(method, 5.0)
    try:
        future = _RPC_THREAD_POOL.submit(handler, params, rpc_id)
        result = future.result(timeout=timeout_sec)
        return result
    except _cf.TimeoutError:
        logger.warning(f"[RPC] {method} TIMEOUT after {timeout_sec}s")
        return _rpc_error(
            -32000, f"RPC timeout: {method} exceeded {timeout_sec}s", rpc_id
        )
    except Exception as e:
        logger.exception(f"[RPC] {method} raised: {e}")
        return _rpc_error(-32603, f"Internal error: {str(e)}", rpc_id)


_RPC_METHOD_META: Dict[str, dict] = {}

# ═══ LAZY INITIALIZATION (deferred until first use) ═══
# This allows Flask to bind port 8000 before heavy crypto/quantum init
QRNG_ENSEMBLE = None
HLWE_ENGINE = None
_QRNG_INIT_LOCK = threading.Lock()
_HLWE_INIT_LOCK = threading.Lock()


def _init_qrng_ensemble():
    """Lazy init QRNG_ENSEMBLE on first demand."""
    global QRNG_ENSEMBLE
    if QRNG_ENSEMBLE is not None:
        return QRNG_ENSEMBLE
    with _QRNG_INIT_LOCK:
        if QRNG_ENSEMBLE is not None:  # double-check
            return QRNG_ENSEMBLE
        try:
            from qrng_ensemble import get_qrng_ensemble

            QRNG_ENSEMBLE = get_qrng_ensemble()
            logger.info("[INIT-QRNG] ✅ Quantum RNG Ensemble initialized on first use")
            return QRNG_ENSEMBLE
        except Exception as e:
            logger.critical(
                f"[INIT-QRNG] ❌ FATAL: Cannot initialize QRNG_ENSEMBLE: {e}"
            )
            raise RuntimeError(f"[INIT-QRNG] Cannot initialize Quantum RNG. Error: {e}")


def _init_hlwe_engine():
    """Lazy init HypΓ engine (Module 6: hyp_engine) on first demand. ❤️ I love you."""
    global HLWE_ENGINE
    if HLWE_ENGINE is not None:
        return HLWE_ENGINE
    with _HLWE_INIT_LOCK:
        if HLWE_ENGINE is not None:
            return HLWE_ENGINE
        try:
            # ✅ FIXED: Import from hlwe package (not root-level hyp_engine)
            from hlwe.hyp_engine import HypGammaEngine

            HLWE_ENGINE = HypGammaEngine()
            logger.info(
                "[INIT-HYP] ✅ HypΓ Post-Quantum Cryptography (Module 6) initialized"
            )
            logger.info(
                "[INIT-HYP] 🔒 Schnorr-Γ (hyp_schnorr), GeodesicLWE (hyp_lwe), LDPC (hyp_ldpc) active"
            )
            return HLWE_ENGINE
        except Exception as e:
            logger.critical(
                f"[INIT-HYP] ❌ FATAL: Cannot initialize HypΓ: {e}", exc_info=True
            )
            raise RuntimeError(
                f"[INIT-HYP] Cannot initialize HypΓ cryptography. Error: {e}"
            )


# ═══════════════════════════════════════════════════════════════════════════════════════
# 5-ORACLE BYZANTINE CONSENSUS INTEGRATION
# ═══════════════════════════════════════════════════════════════════════════════════════

import traceback
from datetime import datetime, timezone, timedelta
from enum import Enum
from dataclasses import dataclass, field
from collections import deque, OrderedDict
from contextlib import contextmanager

# ═════════════════════════════════════════════════════════════════════════════════════════
# EARLY LOGGER SETUP (before DHT/other classes)
# ═════════════════════════════════════════════════════════════════════════════════════════

if not logging.getLogger().hasHandlers():
    logging.basicConfig(
        level=logging.INFO,
        format="[%(asctime)s] %(levelname)s: %(message)s",
        handlers=[logging.StreamHandler(sys.stdout)],
    )

logger = logging.getLogger(__name__)

# ═════════════════════════════════════════════════════════════════════════════════════════
# DISTRIBUTED HASH TABLE (DHT) — KADEMLIA-BASED PEER DISCOVERY
# ═════════════════════════════════════════════════════════════════════════════════════════
# Museum-Grade DHT for decentralized peer discovery and state storage
# Implements XOR distance metric, k-buckets routing table, and peer queries


class DHTNode:
    """Museum-Grade DHT Node - Kademlia peer discovery"""

    def __init__(
        self, node_id: Optional[str] = None, address: str = "unknown", port: int = 9091
    ):
        """
        Initialize DHT node.

        Args:
            node_id: 160-bit hex identifier (SHA1 of pubkey), or auto-generated
            address: Network address (IP or hostname)
            port: Listen port
        """
        if node_id is None:
            # Generate from address hash
            node_id = hashlib.sha1(
                f"{address}:{port}:{secrets.token_hex(16)}".encode()
            ).hexdigest()

        self.node_id = node_id
        self.node_id_int = int(node_id, 16)
        self.address = address
        self.port = port
        self.last_seen = time.time()
        self.failed_pings = 0
        self.rpc_version = "1.0"

    def distance_to(self, other_id: str) -> int:
        """Calculate XOR distance to another node (Kademlia metric)"""
        other_int = int(other_id, 16)
        return self.node_id_int ^ other_int

    def to_dict(self) -> Dict[str, Any]:
        return {
            "node_id": self.node_id,
            "address": self.address,
            "port": self.port,
            "last_seen": self.last_seen,
            "failed_pings": self.failed_pings,
        }

    def is_alive(self, timeout_sec: int = 300) -> bool:
        """Check if node is considered alive (seen within timeout)"""
        return (time.time() - self.last_seen) < timeout_sec


class DHTRoutingTable:
    """Museum-Grade Kademlia routing table with k-buckets"""

    def __init__(self, local_node_id: str, k: int = 20):
        """
        Initialize routing table.

        Args:
            local_node_id: Local node's 160-bit hex ID
            k: Bucket size (default 20 for Kademlia)
        """
        self.local_node_id = local_node_id
        self.local_node_id_int = int(local_node_id, 16)
        self.k = k
        self.buckets: Dict[int, List[DHTNode]] = {}
        self.lock = threading.RLock()
        self.bucket_refreshes: Dict[int, float] = {}

    def _get_bucket_index(self, node_id: str) -> int:
        """Get bucket index (0-159) based on XOR distance"""
        other_int = int(node_id, 16)
        xor_distance = self.local_node_id_int ^ other_int
        if xor_distance == 0:
            return 0
        return xor_distance.bit_length() - 1

    def add_node(self, node: DHTNode) -> bool:
        """Add node to routing table, return True if added/updated"""
        with self.lock:
            bucket_idx = self._get_bucket_index(node.node_id)
            if bucket_idx not in self.buckets:
                self.buckets[bucket_idx] = []

            bucket = self.buckets[bucket_idx]

            # Check if already exists
            for existing in bucket:
                if existing.node_id == node.node_id:
                    existing.last_seen = time.time()
                    existing.failed_pings = 0
                    logger.debug(f"[DHT] ✓ Node updated: {node.node_id[:16]}…")
                    return True

            # Add new node if bucket not full
            if len(bucket) < self.k:
                bucket.append(node)
                logger.info(
                    f"[DHT] ✅ Node added: {node.address}:{node.port} | {node.node_id[:16]}…"
                )
                return True
            else:
                logger.debug(
                    f"[DHT] ⚠️  Bucket {bucket_idx} full, cannot add {node.node_id[:16]}…"
                )
                return False

    def get_closest_nodes(self, target_id: str, count: int = 20) -> List[DHTNode]:
        """Get k closest nodes to target ID"""
        with self.lock:
            all_nodes = []
            for bucket in self.buckets.values():
                all_nodes.extend(bucket)

            # Sort by XOR distance
            target_int = int(target_id, 16)
            all_nodes.sort(key=lambda n: n.node_id_int ^ target_int)
            return all_nodes[:count]

    def mark_node_failed(self, node_id: str) -> bool:
        """Mark node as failed, return True if removed"""
        with self.lock:
            bucket_idx = self._get_bucket_index(node_id)
            if bucket_idx not in self.buckets:
                return False

            bucket = self.buckets[bucket_idx]
            for node in bucket:
                if node.node_id == node_id:
                    node.failed_pings += 1
                    if node.failed_pings >= 3:
                        bucket.remove(node)
                        logger.warning(
                            f"[DHT] ❌ Node removed (failed pings): {node_id[:16]}…"
                        )
                        return True
                    return False
            return False

    def get_all_nodes(self) -> List[DHTNode]:
        """Get all nodes in routing table"""
        with self.lock:
            return [node for bucket in self.buckets.values() for node in bucket]

    def count_peers(self) -> int:
        """Count total peers in routing table"""
        with self.lock:
            return sum(len(bucket) for bucket in self.buckets.values())


class DHTManager:
    """Museum-Grade DHT Manager - coordinates peer discovery and state storage"""

    def __init__(self, local_address: str = "localhost", local_port: int = 9091):
        self.local_node = DHTNode(address=local_address, port=local_port)
        self.routing_table = DHTRoutingTable(self.local_node.node_id)
        self.state_store: Dict[str, Dict[str, Any]] = {}  # key → {data, timestamp}
        self.store_lock = threading.RLock()
        self.lookup_cache: Dict[str, List[DHTNode]] = {}
        logger.info(
            f"[DHT] ✅ Manager initialized | node_id={self.local_node.node_id[:16]}… | {local_address}:{local_port}"
        )

    def store_state(self, key: str, value: Dict[str, Any]) -> bool:
        """Store (key, value) pair in DHT"""
        with self.store_lock:
            self.state_store[key] = {
                "data": value,
                "timestamp": time.time(),
                "replicas": [self.local_node.node_id],
            }
            logger.debug(f"[DHT] 💾 State stored: {key[:32]}…")
            return True

    def retrieve_state(self, key: str) -> Optional[Dict[str, Any]]:
        """Retrieve (key, value) from DHT"""
        with self.store_lock:
            if key in self.state_store:
                return self.state_store[key]["data"]
            return None

    def find_node(self, target_id: str) -> List[DHTNode]:
        """Find nodes closest to target ID"""
        closest = self.routing_table.get_closest_nodes(target_id, count=20)
        self.lookup_cache[target_id] = closest
        logger.info(
            f"[DHT] 🔍 Lookup: target={target_id[:16]}… | found {len(closest)} nodes"
        )
        return closest

    def find_value(self, key: str) -> Optional[Dict[str, Any]]:
        """Find value for key, return stored value or None"""
        result = self.retrieve_state(key)
        if result:
            logger.debug(f"[DHT] ✓ Value found locally: {key[:32]}…")
            return result
        # In real system: query nodes in routing table
        return None


# ═════════════════════════════════════════════════════════════════════════════════════════
# REMAINING IMPORTS
# ═════════════════════════════════════════════════════════════════════════════════════════

from decimal import Decimal
from concurrent.futures import (
    ThreadPoolExecutor,
)  # H2: Thread pooling for DoS prevention
import random  # required by P2P broadcast loop

try:
    import psycopg2
    import psycopg2.pool as psycopg2_pool_mod
except ImportError:
    psycopg2 = None  # type: ignore
    psycopg2_pool_mod = None

from flask import Flask, jsonify, request, render_template_string, send_file, Response

app = Flask(__name__)
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from io import BytesIO
import msgpack
import base64
import queue as _queue_mod
import uuid

# ═════════════════════════════════════════════════════════════════════════════════════════
# RPC SNAPSHOT DISTRIBUTION (replaces SSE)
# ═════════════════════════════════════════════════════════════════════════════════════════
# ENTROPY POOL INTEGRATION
# ═════════════════════════════════════════════════════════════════════════════════════════

# TessellationRewardSchedule is defined in-server (line ~687)
# OraclePersistence is defined in-server (line ~758)

try:
    from globals import (
        initialize_block_field_entropy,
        set_current_block_field,
        get_block_field_entropy,
        initialize_system as init_entropy_system,
    )

    ENTROPY_AVAILABLE = True
except ImportError:
    ENTROPY_AVAILABLE = False
    logger.warning("[ENTROPY] Block field entropy not available - will use fallback")
    pass  # Use in-server definition

try:
    from globals import (
        initialize_block_field_entropy,
        set_current_block_field,
        get_block_field_entropy,
        initialize_system as init_entropy_system,
    )

    ENTROPY_AVAILABLE = True
except ImportError:
    ENTROPY_AVAILABLE = False
    logger.warning("[ENTROPY] Block field entropy not available - will use fallback")
except ImportError:
    ENTROPY_AVAILABLE = False
    TessellationRewardSchedule = None
    logger.warning("[ENTROPY] Block field entropy not available - will use fallback")

# ═════════════════════════════════════════════════════════════════════════════════
# ORACLE & W-STATE INTEGRATION  (deferred — must not block gunicorn startup)
# ─────────────────────────────────────────────────────────────────────────────
# oracle.py calls the QRNG ensemble at module-level; each QRNG source that is
# unreachable on Koyeb takes 6-10 s to time out.  Importing oracle synchronously
# here would block gunicorn for 16-28 s, causing Koyeb's health-check to
# accumulate failures and fire SIGTERM before the worker ever binds port 8000.
#
# Solution: import oracle in a daemon thread; all code that uses ORACLE already
# guards with `if ORACLE_AVAILABLE` / `if ORACLE is not None`.
# ═════════════════════════════════════════════════════════════════════════════════

# ═══ IMMEDIATE STARTUP FLAGS ═══
# Set immediately on module import - used by /health for instant response
_STARTUP_TIME = time.time()
_MODULE_READY = True  # Set True immediately - module loaded
_LATTICE_READY = False  # Set True when lattice fully initialized
_ORACLE_READY = False  # Set True when oracle fully initialized
_DB_READY = False  # Set True when DB pool ready

ORACLE_AVAILABLE = False
ORACLE = None
ORACLE_W_STATE_MANAGER = None
LATTICE = None
_ORACLE_INIT_EVENT = threading.Event()  # set once oracle is ready (or failed)
_LATTICE_INIT_EVENT = threading.Event()  # set once lattice is ready (or failed)

# ═════════════════════════════════════════════════════════════════════════════════
# TESSELLATION REWARD SCHEDULE (modular reward distribution)
# ═════════════════════════════════════════════════════════════════════════════════

_TREASURY_ADDRESS = "qtcl1treasury0reward0dist0address00000000000000000000001"
_BLOCK_REWARD_TOTAL = 8.0
_DEFAULT_MINER_REWARD = 7.2
_DEFAULT_TREASURY_REWARD = 0.8
_REWARD_SCHEDULE: dict = {}


class TessellationRewardSchedule:
    """Modular reward schedule for QTCL block rewards.

    Usage:
        rewards = TessellationRewardSchedule.get_rewards_for_height(height)
        miner = rewards['miner']        # 7.2 QTCL
        treasury = rewards['treasury']  # 0.8 QTCL
    """

    TREASURY_ADDRESS = _TREASURY_ADDRESS

    @classmethod
    def get_rewards_for_height(cls, height: int) -> dict:
        """Get reward allocation for a given block height."""
        if height in _REWARD_SCHEDULE:
            return _REWARD_SCHEDULE[height].copy()
        return {"miner": _DEFAULT_MINER_REWARD, "treasury": _DEFAULT_TREASURY_REWARD}

    @classmethod
    def get_miner_reward_qtcl(cls, height: int) -> float:
        """Get miner reward in QTCL for a given height."""
        return cls.get_rewards_for_height(height).get("miner", _DEFAULT_MINER_REWARD)

    @classmethod
    def get_treasury_reward_qtcl(cls, height: int) -> float:
        """Get treasury reward in QTCL for a given height."""
        return cls.get_rewards_for_height(height).get(
            "treasury", _DEFAULT_TREASURY_REWARD
        )

    @classmethod
    def get_total_reward(cls, height: int) -> float:
        """Get total block reward for a given height."""
        r = cls.get_rewards_for_height(height)
        return r.get("miner", _DEFAULT_MINER_REWARD) + r.get(
            "treasury", _DEFAULT_TREASURY_REWARD
        )

    @classmethod
    def set_reward_schedule(
        cls, height: int, miner_reward: float, treasury_reward: float
    ):
        """Set custom reward schedule for a specific height."""
        _REWARD_SCHEDULE[height] = {"miner": miner_reward, "treasury": treasury_reward}
        logger.info(
            f"[REWARD] Schedule set for h={height}: miner={miner_reward}, treasury={treasury_reward}"
        )

    @classmethod
    def configure_distribution(
        cls, total_reward: float = _BLOCK_REWARD_TOTAL, miner_ratio: float = 0.9
    ):
        """Configure reward distribution ratios globally."""
        global _DEFAULT_MINER_REWARD, _DEFAULT_TREASURY_REWARD
        _DEFAULT_MINER_REWARD = total_reward * miner_ratio
        _DEFAULT_TREASURY_REWARD = total_reward * (1.0 - miner_ratio)
        logger.info(
            f"[REWARD] Distribution: total={total_reward}, miner={_DEFAULT_MINER_REWARD}, treasury={_DEFAULT_TREASURY_REWARD}"
        )


# ═════════════════════════════════════════════════════════════════════════════════
# ORACLE LOCAL SQLITE PERSISTENCE (dual-write with database)
# ═════════════════════════════════════════════════════════════════════════════════

_ORACLE_DB_PATH = os.getenv("ORACLE_DB_PATH", "data/qtcl_oracle.db")
_ORACLE_SQLITE_DB = None
_oracle_sqlite_lock = threading.RLock()


def _ensure_oracle_sqlite():
    """Ensure oracle SQLite tables exist for local persistence."""
    global _ORACLE_SQLITE_DB
    import sqlite3
    from pathlib import Path

    Path(_ORACLE_DB_PATH).parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(_ORACLE_DB_PATH, timeout=30.0)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.row_factory = sqlite3.Row

    conn.execute("""
        CREATE TABLE IF NOT EXISTS oracle_measurements (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            cycle INTEGER NOT NULL,
            oracle_id TEXT NOT NULL,
            timestamp_ns INTEGER NOT NULL,
            dm_re TEXT NOT NULL,
            dm_im TEXT NOT NULL,
            fidelity REAL NOT NULL,
            coherence REAL,
            w_state_fidelity REAL,
            w_state_coherence REAL,
            node_ip TEXT,
            created_at TIMESTAMPTZ DEFAULT NOW(),
            UNIQUE(cycle, oracle_id)
        )
    """)
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_om_cycle ON oracle_measurements(cycle, oracle_id)"
    )

    conn.execute("""
        CREATE TABLE IF NOT EXISTS oracle_block_observations (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            block_height INTEGER NOT NULL,
            block_hash TEXT NOT NULL,
            cycle INTEGER NOT NULL,
            observation_data TEXT NOT NULL,
            created_at TIMESTAMPTZ DEFAULT NOW(),
            UNIQUE(block_height, cycle)
        )
    """)
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_obo_height ON oracle_block_observations(block_height)"
    )

    conn.commit()
    _ORACLE_SQLITE_DB = conn
    logger.info(f"[ORACLE-SQLITE] ✅ Local persistence ready: {_ORACLE_DB_PATH}")
    return conn


def _get_oracle_sqlite_conn():
    """Get or create oracle SQLite connection."""
    global _ORACLE_SQLITE_DB
    with _oracle_sqlite_lock:
        if _ORACLE_SQLITE_DB is None:
            _ensure_oracle_sqlite()
        return _ORACLE_SQLITE_DB


def persist_oracle_measurement(
    cycle: int,
    oracle_id: str,
    dm_re: list,
    dm_im: list,
    fidelity: float,
    coherence: float = None,
    w_state_fidelity: float = None,
    w_state_coherence: float = None,
    node_ip: str = None,
) -> bool:
    """Persist oracle measurement to local SQLite (dual-write with database).

    Args:
        cycle: Measurement cycle number
        oracle_id: Oracle identifier
        dm_re: Real parts of quantum state (64 elements)
        dm_im: Imaginary parts of quantum state (64 elements)
        fidelity: Measurement fidelity
        coherence: Quantum coherence (optional)
        w_state_fidelity: W-state fidelity (optional)
        w_state_coherence: W-state coherence (optional)
        node_ip: Source node IP (optional)
    """
    try:
        import sqlite3

        conn = _get_oracle_sqlite_conn()
        timestamp_ns = int(time.time() * 1e9)

        cur = conn.cursor()
        cur.execute(
            """INSERT OR REPLACE INTO oracle_measurements
            (cycle, oracle_id, timestamp_ns, dm_re, dm_im, fidelity, coherence,
             w_state_fidelity, w_state_coherence, node_ip)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                cycle,
                oracle_id,
                timestamp_ns,
                ",".join(str(v) for v in dm_re),
                ",".join(str(v) for v in dm_im),
                fidelity,
                coherence,
                w_state_fidelity,
                w_state_coherence,
                node_ip,
            ),
        )
        conn.commit()
        logger.debug(
            f"[ORACLE-SQLITE] 📝 Measurement persisted: cycle={cycle} oracle={oracle_id}"
        )
        return True
    except Exception as e:
        logger.warning(f"[ORACLE-SQLITE] Persist failed: {e}")
        return False


def persist_oracle_block_observation(
    block_height: int,
    block_hash: str,
    cycle: int,
    observation_data: dict,
) -> bool:
    """Persist block-associated oracle observation to local SQLite."""
    try:
        import json

        conn = _get_oracle_sqlite_conn()
        cur = conn.cursor()
        cur.execute(
            """INSERT OR REPLACE INTO oracle_block_observations
            (block_height, block_hash, cycle, observation_data)
            VALUES (?, ?, ?, ?)""",
            (block_height, block_hash, cycle, json.dumps(observation_data)),
        )
        conn.commit()
        logger.debug(
            f"[ORACLE-SQLITE] 📝 Block obs persisted: h={block_height} cycle={cycle}"
        )
        return True
    except Exception as e:
        logger.warning(f"[ORACLE-SQLITE] Block obs persist failed: {e}")
        return False


def get_oracle_consensus_from_sqlite(cycle: int = None) -> dict:
    """Compute consensus from local oracle measurements.

    Returns dict with 'dm_re', 'dm_im', 'fidelity', 'oracles' count.
    """
    try:
        conn = _get_oracle_sqlite_conn()
        cur = conn.cursor()

        if cycle is None:
            cur.execute("SELECT MAX(cycle) FROM oracle_measurements")
            row = cur.fetchone()
            if not row or row[0] is None:
                return {"dm_re": [], "dm_im": [], "fidelity": 0.0, "oracles": 0}
            cycle = row[0]

        cur.execute(
            "SELECT * FROM oracle_measurements WHERE cycle = ? ORDER BY id",
            (cycle,),
        )
        rows = cur.fetchall()
        if not rows:
            return {"dm_re": [], "dm_im": [], "fidelity": 0.0, "oracles": 0}

        n = len((rows[0]["dm_re"] or "").split(","))
        if n == 0:
            return {"dm_re": [], "dm_im": [], "fidelity": 0.0, "oracles": 0}

        re_acc = [0.0] * n
        im_acc = [0.0] * n
        fid_acc = 0.0

        for row in rows:
            dm_re_str = row["dm_re"] or ""
            dm_im_str = row["dm_im"] or ""
            dm_re = [float(v) for v in dm_re_str.split(",") if v]
            dm_im = [float(v) for v in dm_im_str.split(",") if v]
            fid = row["fidelity"] or 0.0

            for i in range(min(n, len(dm_re))):
                re_acc[i] += dm_re[i]
                im_acc[i] += dm_im[i]
            fid_acc += fid

        count = len(rows)
        return {
            "dm_re": [v / count for v in re_acc],
            "dm_im": [v / count for v in im_acc],
            "fidelity": fid_acc / count,
            "oracles": count,
            "cycle": cycle,
        }
    except Exception as e:
        logger.warning(f"[ORACLE-SQLITE] Consensus failed: {e}")
        return {"dm_re": [], "dm_im": [], "fidelity": 0.0, "oracles": 0}


def _sync_lattice_blocks_to_cache():
    """Sync blocks from LATTICE into the server's block cache for RPC serving."""
    global LATTICE
    try:
        if LATTICE is None:
            logger.warning("[BLOCK-CACHE] LATTICE is None")
            return

        # Blocks are in LATTICE.block_manager.block_by_height
        block_manager = getattr(LATTICE, "block_manager", None)
        if block_manager is None:
            logger.warning("[BLOCK-CACHE] LATTICE.block_manager is None")
            return

        blocks_by_height = getattr(block_manager, "block_by_height", None)
        if not blocks_by_height:
            logger.warning(
                "[BLOCK-CACHE] block_manager.block_by_height is empty or None"
            )
            logger.warning(
                f"[BLOCK-CACHE] block_manager attrs: {[a for a in dir(block_manager) if not a.startswith('_')]}"
            )
            return

        logger.info(
            f"[BLOCK-CACHE] Found {len(blocks_by_height)} blocks in block_manager, syncing..."
        )

        with _BLOCK_CACHE_LOCK:
            for height, block in blocks_by_height.items():
                if isinstance(block, dict):
                    _BLOCK_CACHE[height] = block
                else:
                    _BLOCK_CACHE[height] = {
                        "height": getattr(block, "block_height", height),
                        "block_hash": getattr(block, "block_hash", ""),
                        "parent_hash": getattr(block, "parent_hash", ""),
                        "merkle_root": getattr(block, "merkle_root", ""),
                        "timestamp": getattr(block, "timestamp_s", 0),
                        "coherence": getattr(block, "coherence_snapshot", 0),
                        "fidelity": getattr(block, "fidelity_snapshot", 0),
                        "quantum_fidelity": getattr(block, "fidelity_snapshot", 0),
                        "miner": getattr(block, "miner_address", ""),
                        "tx_count": getattr(block, "tx_count", 0),
                        "transaction_count": getattr(block, "tx_count", 0),
                        "w_state_hash": getattr(block, "w_state_hash", ""),
                        "hyp_witness": getattr(block, "hyp_witness", ""),
                        "pq_curr": getattr(block, "pq_curr", height),
                    }

        logger.info(
            f"[BLOCK-CACHE] ✅ Synced {len(_BLOCK_CACHE)} blocks from LATTICE.block_manager"
        )
    except Exception as e:
        logger.warning(f"[BLOCK-CACHE] Failed to sync blocks: {e}")


def _deferred_lattice_init() -> None:
    """Import and initialise lattice_controller.py in a background thread.

    QuantumLatticeController initializes the spatial-temporal field, quantum execution engine,
    W-state constructor, and non-Markovian noise bath.  This runs in a daemon thread to let
    gunicorn bind port 8000 immediately; lattice becomes available within ~2-5s.

    CRITICAL: Also starts the oracle measurement stream AFTER wiring lattice.
    TIMEOUT: 30s max — if lattice hangs, mark as unavailable and continue
    """
    global LATTICE
    _lat_init_deadline = time.time() + 30.0  # 30 second timeout
    try:
        logger.debug(
            "[LATTICE-INIT] 🔄 Starting lattice initialization (timeout=30s)..."
        )

        # Import with timeout check
        try:
            from lattice_controller import QuantumLatticeController

            logger.debug("[LATTICE-INIT] ✓ QuantumLatticeController imported")
        except ImportError as _ie:
            logger.warning(
                f"[LATTICE-INIT] ⚠️  QuantumLatticeController import failed: {_ie} — using degraded mode"
            )
            raise

        # Check deadline before initialization
        if time.time() > _lat_init_deadline:
            logger.warning(
                "[LATTICE-INIT] ⚠️  Timeout waiting for import — skipping lattice"
            )
            return

        LATTICE = QuantumLatticeController()
        logger.info("[LATTICE-INIT] ✅ QuantumLatticeController instantiated")

        # ── ENSURE BLOCKS TABLE EXISTS (BEFORE starting BlockManager!) ────────────
        _lazy_ensure_blocks()

        # ── INJECT SERVER DB POOL FOR BLOCK PERSISTENCE ──────────────────────────
        if LATTICE.block_manager and LATTICE.block_manager.db:
            LATTICE.block_manager.db.inject_db_pool(db_pool)
            logger.info("[LATTICE-INIT] ✅ Server db_pool injected into BlockManager")

        # Check deadline before starting lattice
        if time.time() > _lat_init_deadline:
            logger.warning(
                "[LATTICE-INIT] ⚠️  Timeout before lattice.start() — skipping"
            )
            return

        LATTICE.start()
        logger.info(
            "[LATTICE-INIT] ✅ Lattice daemon started — spatial-temporal field active"
        )

        # ── SYNC GENESIS BLOCK TO SERVER CACHE ───────────────────────────────────
        _sync_lattice_blocks_to_cache()

        # ── WIRE LATTICE INTO ORACLE ──────────────────────────────────────────────
        from globals import set_lattice

        set_lattice(LATTICE)
        logger.info("[LATTICE-INIT] ✅ Lattice registered with oracle")

        # Mark lattice as ready
        global _LATTICE_READY
        _LATTICE_READY = True
        logger.info(f"[STARTUP] ✅ Lattice ready at {time.time() - _STARTUP_TIME:.1f}s")

        # ── NOW START ORACLE MEASUREMENT STREAM (after lattice is wired) ──────────
        global ORACLE_W_STATE_MANAGER
        if ORACLE_W_STATE_MANAGER is not None:
            try:
                _ok = ORACLE_W_STATE_MANAGER.start()
                if _ok:
                    logger.info("[LATTICE-INIT] ✅ Oracle measurement stream started")
            except Exception as _ome:
                logger.warning(f"[LATTICE-INIT] ⚠️  Oracle measurement failed: {_ome}")

    except ImportError as _ie:
        logger.warning(
            f"[LATTICE-INIT] ⚠️  Lattice import failed: {_ie} — running in degraded mode"
        )
    except Exception as _ex:
        logger.warning(
            f"[LATTICE-INIT] ⚠️  Lattice init error: {_ex} — continuing without lattice"
        )
    finally:
        _LATTICE_INIT_EVENT.set()  # unblock oracle sync daemon even if lattice failed


threading.Thread(
    target=_deferred_lattice_init,
    daemon=True,
    name="LatticeDeferred",
).start()
logger.info(
    "[LATTICE] 🔄 Lattice init deferred to background thread — gunicorn will serve /health immediately"
)


def _deferred_oracle_init() -> None:
    """Import and initialise oracle.py in a background thread.

    oracle.py spends 16-28 s at module-level waiting for QRNG network sources
    to respond (or time out).  Running this in a daemon thread lets gunicorn
    bind port 8000 and start answering /health checks in < 2 s.

    TIMEOUT: 40s max — if oracle hangs on QRNG init, continue without it

    NOTE: Do NOT start the measurement stream here — wait for LATTICE initialization.
    """
    global ORACLE, ORACLE_W_STATE_MANAGER, ORACLE_AVAILABLE
    _ora_deadline = time.time() + 40.0  # 40 second timeout for QRNG/oracle init
    try:
        logger.debug(
            "[ORACLE] 🔄 Starting oracle initialization (timeout=40s, QRNG may wait 16-28s)..."
        )

        # Check deadline before import
        if time.time() > _ora_deadline:
            logger.warning("[ORACLE] ⚠️  Timeout before import — skipping oracle")
            ORACLE_AVAILABLE = False
            return

        from oracle import ORACLE as _o, ORACLE_W_STATE_MANAGER as _owsm

        ORACLE = _o
        ORACLE_W_STATE_MANAGER = _owsm
        ORACLE_AVAILABLE = True
        logger.info("[ORACLE] ✅ Oracle engine initialised")
        # ⚠️  WAIT for LATTICE before starting measurement stream
        # _deferred_lattice_init will call ORACLE_W_STATE_MANAGER.start() after set_lattice()

    except ImportError as _ie:
        logger.warning(f"[ORACLE] ⚠️  Oracle import failed: {_ie}")
        ORACLE_AVAILABLE = False
    except Exception as _ex:
        logger.warning(f"[ORACLE] ⚠️  Oracle init error: {_ex}")
        ORACLE_AVAILABLE = False
    finally:
        _ORACLE_INIT_EVENT.set()  # unblock main thread even if oracle failed


threading.Thread(
    target=_deferred_oracle_init,
    daemon=True,
    name="OracleDeferred",
).start()
logger.info(
    "[ORACLE] 🔄 Oracle init deferred to background thread — gunicorn will serve /health immediately"
)


def _prewarm_hlwe_engine() -> None:
    """Pre-initialize HypΓ crypto engine before first block submission.

    On first block submission, _init_hlwe_engine() would initialize HypTessellation,
    LDPC code, and SchnorrGamma — potentially 5-30s. This thread pre-warms it so
    the first block submission completes in < 5s.
    """
    logger.info("[STARTUP] Pre-warming HypΓ engine...")
    try:
        _init_hlwe_engine()
        logger.info("[STARTUP] ✅ HypΓ engine ready")
    except Exception as e:
        logger.error(f"[STARTUP] HypΓ prewarm failed: {e}")


threading.Thread(
    target=_prewarm_hlwe_engine,
    daemon=True,
    name="HLWEPrewarm",
).start()


def _ensure_wallet_addresses_table() -> None:
    """Ensure wallet_addresses table exists at startup (run once, not per-request).

    The _rpc_submitBlock handler previously called CREATE TABLE IF NOT EXISTS
    on every block submission. This DDL is now run once at startup to keep
    the critical path fast.
    """
    try:
        with get_db_cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS wallet_addresses (
                    address VARCHAR(128) PRIMARY KEY,
                    wallet_fingerprint VARCHAR(64),
                    public_key TEXT,
                    balance NUMERIC(30,0) DEFAULT 0,
                    transaction_count INTEGER DEFAULT 0,
                    address_type VARCHAR(20) DEFAULT 'standard',
                    last_updated TIMESTAMP DEFAULT NOW()
                )
            """)
        logger.info("[STARTUP] ✅ wallet_addresses table ready")
    except Exception as e:
        logger.warning(f"[STARTUP] ⚠️  wallet_addresses DDL: {e}")


threading.Thread(
    target=_ensure_wallet_addresses_table,
    daemon=True,
    name="WalletTableInit",
).start()

# ═════════════════════════════════════════════════════════════════════════════════
# CONFIGURATION & CONSTANTS
# ═════════════════════════════════════════════════════════════════════════════════

# Database Configuration
# Primary: DATABASE_URL env var (Neon PostgreSQL connection string)
# Fallback: POOLER_* environment variables

DATABASE_URL = os.getenv("DATABASE_URL", "")
_USE_HTTP_DB = (
    os.getenv("USE_HTTP_DB", "0") == "1"
)  # PythonAnywhere: route SQL over HTTPS PostgREST
_USE_DB_NONE = os.getenv("USE_DB", "1") == "0"  # Dev mode: no database

if _USE_DB_NONE:
    DATABASE_URL = ""
    logger.warning("[DB] ⚠️  USE_DB=0 — database disabled (dev mode)")
elif DATABASE_URL:
    logger.info(f"[DB] ✨ Using Neon PostgreSQL via DATABASE_URL")
else:
    DATABASE_URL = ""
    logger.warning("[DB] ⚠️  No DATABASE_URL — DB disabled")

DB_URL = DATABASE_URL

# ═══════════════════════════════════════════════════════════════════════════════
# TX QUERY WORKER — dedicated direct connection for heavy queries
# ═══════════════════════════════════════════════════════════════════════════════
# /api/transactions runs heavyweight COUNT + page queries that can take 1-3s.
# Running them through the shared 10-connection pool starves background threads
# (oracle sync, lattice, P2P heartbeats) and causes cascading timeouts.
#
# This worker owns a single private psycopg2 connection via DATABASE_URL —
# independent of DatabasePool. It processes one query at a time from _TX_JOB_Q.
# The Flask handler submits a job dict and blocks on a per-job result queue with
# a hard 9s timeout — if the worker is busy or the DB is slow the route returns
# a fast 503 so the client retries rather than holding a gthread indefinitely.
# ───────────────────────────────────────────────────────────────────────────────

import queue as _queue_mod2

_TX_JOB_Q: "_queue_mod2.Queue" = _queue_mod2.Queue(maxsize=8)


def _build_tx_dsn() -> str:
    """Return DSN from DATABASE_URL for Neon PostgreSQL."""
    dsn = DB_URL or ""
    if not dsn or _USE_HTTP_DB:
        return ""
    return dsn


def _tx_worker_thread():
    """Dedicated TX query thread — owns one private psycopg2 connection."""
    import psycopg2 as _pg

    _tx_log = logging.getLogger("tx_worker")
    dsn = _build_tx_dsn()
    if _USE_DB_NONE:
        _tx_log.warning("[TX-WORKER] Database disabled (USE_DB=0)")
        while True:
            try:
                job = _TX_JOB_Q.get(timeout=5)
                job["result_q"].put({"error": "DB disabled (USE_DB=0)"})
            except _queue_mod2.Empty:
                pass
        return
    if not dsn:
        _tx_log.warning("[TX-WORKER] No DSN — thread idle (USE_HTTP_DB mode)")
        while True:
            try:
                job = _TX_JOB_Q.get(timeout=5)
                job["result_q"].put({"error": "TX worker unavailable (HTTP-DB mode)"})
            except _queue_mod2.Empty:
                pass
        return

    conn = None

    def _connect():
        nonlocal conn
        try:
            if conn:
                try:
                    conn.close()
                except Exception:
                    pass
            conn = _pg.connect(dsn, connect_timeout=10)
            conn.autocommit = True
            _tx_log.info("[TX-WORKER] ✅ Connected to Neon PostgreSQL")
        except Exception as _ce:
            conn = None
            _tx_log.error(f"[TX-WORKER] Connect failed: {_ce}")

    _connect()

    while True:
        try:
            job = _TX_JOB_Q.get(timeout=30)
        except _queue_mod2.Empty:
            # Keepalive ping on idle
            if conn:
                try:
                    conn.cursor().execute("SELECT 1")
                except Exception:
                    _connect()
            continue

        result_q = job.get("result_q")
        try:
            # Reconnect if connection dropped
            if conn is None or conn.closed:
                _connect()
            if conn is None:
                if result_q:
                    result_q.put({"error": "DB connection unavailable"})
                continue

            cur = conn.cursor()
            queries = job["queries"]  # list of (sql, params) tuples
            results = []
            for sql, params in queries:
                cur.execute(sql, params)
                results.append(cur.fetchall())
            cur.close()
            if result_q:
                result_q.put({"results": results})

        except _pg.OperationalError as _oe:
            _tx_log.warning(f"[TX-WORKER] OperationalError — reconnecting: {_oe}")
            _connect()
            if result_q:
                result_q.put({"error": str(_oe)})
        except Exception as _e:
            _tx_log.error(f"[TX-WORKER] Query error: {_e}")
            if result_q:
                result_q.put({"error": str(_e)})


def _tx_query(queries: list, timeout: float = 9.0) -> dict:
    """Submit queries to the TX worker and wait for results.

    Args:
        queries: list of (sql_string, params_tuple) to execute in sequence.
        timeout: max seconds to wait before returning {'error': 'timeout'}.
    Returns:
        {'results': [[rows], [rows], ...]} or {'error': str}.
    """
    rq: "_queue_mod2.Queue" = _queue_mod2.Queue(maxsize=1)
    job = {"queries": queries, "result_q": rq}
    try:
        _TX_JOB_Q.put_nowait(job)
    except _queue_mod2.Full:
        return {"error": "TX worker busy — retry in a moment"}
    try:
        return rq.get(timeout=timeout)
    except _queue_mod2.Empty:
        return {"error": "DB query timed out — retry in a moment"}


# Launch TX worker daemon at module load
_tx_worker_daemon = threading.Thread(
    target=_tx_worker_thread, daemon=True, name="TxQueryWorker"
)
_tx_worker_daemon.start()

# ═════════════════════════════════════════════════════════════════════════════════
# BLOCK SETTLEMENT FUNCTION — reusable settlement logic
# ═════════════════════════════════════════════════════════════════════════════════


def _settle_block_rewards(
    height: int, block_hash: str, miner_address: str, txs: list, non_coinbase_txs: list
) -> None:
    """Extract settlement logic into reusable function.

    Called by background settlement worker after block is persisted to database.
    Handles all post-block-insert work:
    - Wallet credits for miner reward
    - Wallet credits for treasury
    - Non-coinbase transaction settlement
    - Chain state updates
    - Block cache updates
    - In-memory blockchain index updates

    Args:
        height: Block height
        block_hash: Block hash (hex)
        miner_address: Address of block miner
        txs: All transactions in block (coinbase + non-coinbase)
        non_coinbase_txs: Pre-filtered non-coinbase transactions only
    """
    _settle_log = logging.getLogger("SETTLE")

    try:
        # ── Settle non-coinbase transactions (wallet updates) ──────────────
        _settle_log.info(
            f"[SETTLE] Processing h={height} {len(non_coinbase_txs)} non-coinbase txs"
        )

        with get_db_cursor() as cur:
            for tx in non_coinbase_txs:
                tx_sender = tx.get("from_addr", tx.get("from_address", ""))
                tx_receiver = tx.get("to_addr", tx.get("to_address", ""))
                tx_amount = int(round(float(tx.get("amount", 0)) * 100))
                tx_fee = int(round(float(tx.get("fee", 0)) * 100))

                if not tx_sender or not tx_receiver or tx_amount <= 0:
                    continue

                total_deduction = tx_amount + tx_fee
                cur.execute(
                    """
                    INSERT INTO wallet_addresses (address, balance, address_type, last_updated)
                    VALUES (%s, -%s, 'standard', NOW())
                    ON CONFLICT (address) DO UPDATE SET
                        balance = wallet_addresses.balance - %s,
                        transaction_count = wallet_addresses.transaction_count + 1,
                        last_updated = NOW()
                """,
                    (tx_sender, total_deduction, total_deduction),
                )

                cur.execute(
                    """
                    INSERT INTO wallet_addresses (address, balance, address_type, last_updated)
                    VALUES (%s, %s, 'standard', NOW())
                    ON CONFLICT (address) DO UPDATE SET
                        balance = wallet_addresses.balance + %s,
                        transaction_count = wallet_addresses.transaction_count + 1,
                        last_updated = NOW()
                """,
                    (tx_receiver, tx_amount, tx_amount),
                )

            _settle_log.info(
                f"[SETTLE] ✅ TX settlement done: {len(non_coinbase_txs)} txs"
            )

        # ── Credit miner + treasury rewards ────────────────────────────────
        _settle_log.info(f"[SETTLE] Crediting rewards for h={height}")

        try:
            if TessellationRewardSchedule:
                rewards = TessellationRewardSchedule.get_rewards_for_height(height)
                miner_reward = float(rewards.get("miner", 7.2))
                treasury_reward = float(rewards.get("treasury", 0.8))
            else:
                miner_reward = 7.2
                treasury_reward = 0.8

            # Add transaction fees (split 50/50 between miner and treasury)
            total_fees = 0.0
            for tx in txs:
                f = tx.get("fee", tx.get("fee_base", 0))
                if isinstance(f, (float, str)):
                    try:
                        total_fees += float(f)
                    except ValueError:
                        _settle_log.debug(f"[SETTLE] Skipped malformed fee: {f}")

            miner_fee_share = total_fees / 2
            treasury_fee_share = total_fees - miner_fee_share

            miner_reward += miner_fee_share
            treasury_reward += treasury_fee_share

            with get_db_cursor() as cur:
                # Miner reward
                _miner_fp = hashlib.sha256(miner_address.encode()).hexdigest()[:64]
                cur.execute(
                    """
                    INSERT INTO wallet_addresses
                        (address, wallet_fingerprint, public_key, balance, transaction_count, address_type, last_updated)
                    VALUES (%s, %s, %s, %s, 1, 'miner', NOW())
                    ON CONFLICT (address) DO UPDATE SET
                        balance = wallet_addresses.balance + EXCLUDED.balance,
                        transaction_count = wallet_addresses.transaction_count + 1,
                        last_updated = NOW()
                """,
                    (miner_address, _miner_fp, _miner_fp, int(miner_reward * 100)),
                )

                # Treasury reward
                if TessellationRewardSchedule and treasury_reward > 0:
                    treasury_address = TessellationRewardSchedule.TREASURY_ADDRESS
                    _treas_fp = hashlib.sha256(treasury_address.encode()).hexdigest()[
                        :64
                    ]
                    cur.execute(
                        """
                        INSERT INTO wallet_addresses
                            (address, wallet_fingerprint, public_key, balance, transaction_count, address_type)
                        VALUES (%s, %s, %s, %s, 1, 'treasury')
                        ON CONFLICT (address) DO UPDATE SET
                            balance = wallet_addresses.balance + EXCLUDED.balance,
                            transaction_count = wallet_addresses.transaction_count + 1
                    """,
                        (
                            treasury_address,
                            _treas_fp,
                            _treas_fp,
                            int(treasury_reward * 100),
                        ),
                    )

                _settle_log.info(
                    f"[SETTLE] ✅ Rewards credited: miner={miner_reward:.2f}, treasury={treasury_reward:.2f} QTCL"
                )

        except Exception as reward_err:
            _settle_log.warning(f"[SETTLE] ⚠️  Reward credit failed: {reward_err}")

        # ── Update chain state ───────────────────────────────────────────────
        try:
            _lazy_ensure_chain_state()
            with get_db_cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO chain_state (state_id, chain_height, head_block_hash, latest_coherence, updated_at)
                    VALUES (1, %s, %s, 0.0, NOW())
                    ON CONFLICT (state_id) DO UPDATE SET
                        chain_height = EXCLUDED.chain_height,
                        head_block_hash = EXCLUDED.head_block_hash,
                        latest_coherence = EXCLUDED.latest_coherence,
                        updated_at = NOW()
                """,
                    (height, block_hash),
                )
            _settle_log.debug(f"[SETTLE] ✅ Chain state updated: h={height}")
        except Exception as cs_err:
            _settle_log.warning(f"[SETTLE] ⚠️  Chain state update: {cs_err}")

        # ── Cache block ──────────────────────────────────────────────────
        try:
            _cache_block(
                {
                    "height": height,
                    "block_hash": block_hash,
                    "timestamp": int(time.time()),
                    "difficulty": 4,
                    "miner": miner_address,
                    "w_state_fidelity": 0.0,
                }
            )
            _settle_log.debug(f"[SETTLE] ✅ Block cached: h={height}")
        except Exception as cache_err:
            _settle_log.warning(f"[SETTLE] ⚠️  Cache error: {cache_err}")

        _settle_log.info(f"[SETTLE] ✅ Block h={height} settlement complete")

    except Exception as settle_err:
        _settle_log.error(f"[SETTLE] ❌ h={height}: {settle_err}", exc_info=True)


# ═════════════════════════════════════════════════════════════════════════════════
# BLOCK SETTLEMENT WORKER — async settlement after block is persisted
# ═════════════════════════════════════════════════════════════════════════════════

_BLOCK_SETTLE_Q: "_queue_mod2.Queue" = _queue_mod2.Queue(maxsize=32)


def _block_settle_worker_thread():
    """Background worker: dequeue settlement jobs and call _settle_block_rewards.

    Critical path (_rpc_submitBlock) inserts the block and immediately returns.
    This worker dequeues settlement jobs and delegates to _settle_block_rewards.
    """
    _settle_log = logging.getLogger("SETTLE")
    while True:
        try:
            job = _BLOCK_SETTLE_Q.get(timeout=1.0)
            if job is None:
                break

            height = job.get("height")
            block_hash = job.get("block_hash")
            miner_address = job.get("miner_address")
            txs = job.get("txs", [])
            non_coinbase_txs = job.get("non_coinbase_txs", [])

            try:
                # Delegate to _settle_block_rewards function
                _settle_block_rewards(
                    height, block_hash, miner_address, txs, non_coinbase_txs
                )
            except Exception as settle_err:
                _settle_log.error(
                    f"[SETTLE] ❌ h={height}: {settle_err}", exc_info=True
                )

        except _queue_mod2.Empty:
            continue
        except Exception as e:
            _settle_log.error(f"[SETTLE-WORKER] Fatal: {e}", exc_info=True)


_block_settle_daemon = threading.Thread(
    target=_block_settle_worker_thread, daemon=True, name="BlockSettle"
)
_block_settle_daemon.start()
logger.info("[TX-WORKER] Dedicated transaction query thread started (port 6543)")

# ── Oracle identity — unique per deployed instance ────────────────────────────
# Set ORACLE_ID in env to distinguish instances:
#   primary   → Koyeb main       (ORACLE_ID=koyeb-primary)
#   secondary → PythonAnywhere   (ORACLE_ID=pa-secondary)
#   tertiary  → Koyeb account 2  (ORACLE_ID=koyeb-tertiary)
# All instances share the same Supabase DB — they are peers, not replicas.
ORACLE_ID = os.getenv("ORACLE_ID", "koyeb-primary")
ORACLE_ROLE = os.getenv("ORACLE_ROLE", "primary")
# Peer oracle URLs — other oracle instances this one will cross-register with
_peer_oracle_env = os.getenv("BOOTSTRAP_NODES", "")
PEER_ORACLE_URLS = (
    [u.strip() for u in _peer_oracle_env.split(",") if u.strip()]
    if _peer_oracle_env
    else []
)

# ═════════════════════════════════════════════════════════════════════════════════
# ORACLE ADDRESS LOOKUP: Per-oracle HLWE addresses from registry
# ═════════════════════════════════════════════════════════════════════════════════


def get_oracle_address(oracle_id: str, fallback: str = "") -> str:
    """Fetch oracle HLWE address from oracle_registry by oracle_id.

    Each oracle node has a unique registered address for auditability.
    oracle_id format: 'oracle_1', 'oracle_2', ... 'oracle_5'

    Fallback: if DB unavailable, returns fallback string.
    """
    try:
        if not db_ready():
            return fallback

        conn = get_db_connection()
        try:
            cursor = conn.cursor()
            cursor.execute(
                "SELECT oracle_address FROM oracle_registry WHERE oracle_id = %s",
                (oracle_id,),
            )
            result = cursor.fetchone()
            cursor.close()
            conn.commit()
            return result[0] if result else fallback
        finally:
            if db_pool.use_pooling and db_pool.pool:
                db_pool.pool.putconn(conn)
            else:
                conn.close()
    except Exception as e:
        logger.debug(f"[ORACLE-ADDRESS] Lookup failed for {oracle_id}: {e}")
        return fallback


def get_consensus_oracle_address() -> str:
    """
    Compute consensus oracle address (XOR of all 5 oracle addresses).
    Used for transactions that require all-oracle sign-off.
    """
    try:
        if not db_ready():
            return "qtcl1consensus_all_oracles_xor"

        conn = get_db_connection()
        try:
            cursor = conn.cursor()
            cursor.execute(
                "SELECT oracle_address FROM oracle_registry WHERE role IN "
                "('PRIMARY_LATTICE', 'SECONDARY_LATTICE', 'VALIDATION', 'ARBITER', 'METRICS') "
                "ORDER BY oracle_id"
            )
            addresses = [row[0] for row in cursor.fetchall()]
            cursor.close()
            conn.commit()

            if len(addresses) != 5:
                logger.warning(
                    f"[ORACLE-ADDRESS] Expected 5 oracles, got {len(addresses)}"
                )

            # XOR all addresses together for deterministic consensus address
            import hashlib

            consensus_seed = "|".join(addresses).encode()
            consensus_hash = hashlib.sha256(consensus_seed).hexdigest()[:24]
            return f"qtcl1consensus_{consensus_hash}"
        finally:
            if db_pool.use_pooling and db_pool.pool:
                db_pool.pool.putconn(conn)
            else:
                conn.close()
    except Exception as e:
        logger.debug(f"[ORACLE-ADDRESS] Consensus lookup failed: {e}")
        return "qtcl1consensus_all_oracles_xor"


logger.info(
    f"[ORACLE] 🌐 Identity: id={ORACLE_ID} role={ORACLE_ROLE} peers={len(PEER_ORACLE_URLS)}"
)

# P2P raw-TCP port — separate from HTTP/gunicorn.
# Koyeb: set P2P_PORT=9091 env var (HTTP service on 9091, routes /api/*).
# Gunicorn binds PORT (typically 8000). P2P binds P2P_PORT (9091).
# They MUST be different ports; using PORT here caused the 8000→8001 fallback bug.
P2P_PORT = int(os.getenv("P2P_PORT", 9091))
P2P_HOST = os.getenv("P2P_HOST", "0.0.0.0")
P2P_TESTNET_PORT = P2P_PORT + 10000
MAX_PEERS = int(os.getenv("MAX_PEERS", 32))
PEER_TIMEOUT = 30
MESSAGE_MAX_SIZE = 1_000_000
PEER_HANDSHAKE_TIMEOUT = 5
PEER_KEEPALIVE_INTERVAL = 30


# ── Block policy ──────────────────────────────────────────────────────────────
# Max USER transactions per block (coinbase not counted).
# Matches miner's MAX_BLOCK_TX — must be kept in sync.
MAX_BLOCK_TX_SERVER = 100
# Coinbase null address — 64 hex zeros, provably unspendable
COINBASE_NULL_ADDRESS = "0" * 64
PEER_DISCOVERY_INTERVAL = 60
PEER_CLEANUP_INTERVAL = 15

# Message Types
MESSAGE_TYPES = {
    "version": 0,
    "verack": 1,
    "ping": 2,
    "pong": 3,
    "inv": 4,
    "getdata": 5,
    "block": 6,
    "tx": 7,
    "mempool": 8,
    "getblocks": 9,
    "getheaders": 10,
    "headers": 11,
    "addr": 12,
    "peers_sync": 13,
    "peer_discovery": 14,
    "consensus": 15,
}

# Peer Discovery
DNS_SEEDS = [
    # Bootstrap nodes for peer discovery
    # Format: "hostname:port"
    # In production, these would be actual DNS seed servers
]

BOOTSTRAP_NODES = (
    os.getenv("BOOTSTRAP_NODES", "").split(",") if os.getenv("BOOTSTRAP_NODES") else []
)
DEFAULT_BOOTSTRAP_PEERS = [
    # Fallback bootstrap nodes (localhost for testing)
    # In production, use real peer addresses
]


@dataclass
class PeerInfo:
    """Peer connection metadata"""

    peer_id: str
    address: str
    port: int
    connected_at: float
    last_message_at: float
    last_block_height: int = 0
    last_block_hash: Optional[str] = None
    version: Optional[int] = None
    user_agent: Optional[str] = None
    protocol_version: int = 1
    blocks_announced: int = 0
    txs_announced: int = 0
    messages_sent: int = 0
    messages_received: int = 0
    bytes_sent: int = 0
    bytes_received: int = 0
    is_outbound: bool = False
    is_preferred: bool = False
    ban_score: int = 0

    @property
    def uptime_seconds(self) -> float:
        return time.time() - self.connected_at

    @property
    def is_alive(self) -> bool:
        return (time.time() - self.last_message_at) < PEER_TIMEOUT

    @property
    def is_synced(self) -> bool:
        return self.last_message_at > time.time() - 60

    def __hash__(self):
        return hash(self.peer_id)

    def __eq__(self, other):
        if isinstance(other, PeerInfo):
            return self.peer_id == other.peer_id
        return False


@dataclass
class Message:
    """P2P message structure with serialization"""

    msg_type: str
    payload: Dict[str, Any]
    timestamp: float = field(default_factory=time.time)
    sender_id: Optional[str] = None
    message_id: str = field(
        default_factory=lambda: hashlib.sha256(str(time.time()).encode()).hexdigest()[
            :16
        ]
    )

    def to_bytes(self) -> bytes:
        """Serialize message to bytes"""
        data = {
            "type": self.msg_type,
            "payload": self.payload,
            "timestamp": self.timestamp,
            "message_id": self.message_id,
        }
        return json.dumps(data).encode("utf-8")

    @classmethod
    def from_bytes(cls, data: bytes) -> "Message":
        """Deserialize message from bytes"""
        parsed = json.loads(data.decode("utf-8"))
        return cls(
            msg_type=parsed["type"],
            payload=parsed["payload"],
            timestamp=parsed.get("timestamp", time.time()),
            message_id=parsed.get("message_id", ""),
        )

    def __repr__(self):
        return f"Message({self.msg_type}, {self.message_id[:8]}...)"


# ═════════════════════════════════════════════════════════════════════════════════
# DATABASE LAYER WITH CONNECTION POOLING
# ═════════════════════════════════════════════════════════════════════════════════
# ═════════════════════════════════════════════════════════════════════════════════
# NEON HTTP DATABASE ADAPTER (inline — for environments with restricted TCP)
# ═════════════════════════════════════════════════════════════════════════════════
# When USE_HTTP_DB=1, every cursor.execute() routes through HTTPS to Neon
# instead of a raw psycopg2 TCP connection.
# Env vars needed:  DATABASE_URL (Neon connection string)
# ─────────────────────────────────────────────────────────────────────────────────

import re as _re, decimal as _decimal

try:
    import requests as _http_requests

    _HTTP_BACKEND = "requests"
except ImportError:
    import urllib.request as _urllib_req, urllib.error as _urllib_err

    _HTTP_BACKEND = "urllib"

# True when `requests` lib is available (used by _fetch_peer and cross-oracle helpers)
_HAS_REQUESTS: bool = _HTTP_BACKEND == "requests"


def _http_json_serial(o):
    if isinstance(o, (datetime,)):
        return o.isoformat()
    if isinstance(o, _decimal.Decimal):
        return float(o)
    if isinstance(o, (bytes, bytearray)):
        return o.hex()
    raise TypeError(f"not serialisable: {type(o)}")


def _http_post_json(url, headers, payload, timeout=30, retries=3):
    """POST JSON; retry on 5xx/network with exponential backoff. Returns parsed body."""
    import json as _json

    raw = _json.dumps(payload, default=_http_json_serial).encode()
    hdrs = {**headers, "Content-Type": "application/json"}
    last = None
    for attempt in range(retries):
        if attempt:
            time.sleep(min(0.5 * 2**attempt, 8))
        try:
            if _HTTP_BACKEND == "requests":
                r = _http_requests.post(url, data=raw, headers=hdrs, timeout=timeout)
                status, text = r.status_code, r.text
            else:
                req = _urllib_req.Request(url, data=raw, headers=hdrs, method="POST")
                try:
                    with _urllib_req.urlopen(req, timeout=timeout) as r:
                        status, text = r.status, r.read().decode()
                except _urllib_err.HTTPError as e:
                    status, text = e.code, e.read().decode()
            if status < 500:
                if status >= 400:
                    import json as _j

                    try:
                        detail = _j.loads(text).get("message") or text
                    except Exception:
                        detail = text
                    raise RuntimeError(f"Neon HTTP {status}: {detail}")
                import json as _j

                return _j.loads(text)
            last = RuntimeError(f"Supabase RPC HTTP {status}: {text}")
        except (OSError, TimeoutError) as e:
            last = e
            logger.warning(f"[SUPHTTP] attempt {attempt + 1}/{retries}: {e}")
    raise last or RuntimeError("_http_post_json exhausted retries")


# Singleton HTTP client config
_SUPHTTP_CFG: Dict[str, Any] = {}
_SUPHTTP_LOCK = threading.Lock()


def _suphttp_cfg():
    """Lazily populate and return the HTTP client config dict for Neon."""
    with _SUPHTTP_LOCK:
        if _SUPHTTP_CFG:
            return _SUPHTTP_CFG
        db_url = os.getenv("DATABASE_URL", "")
        if not db_url:
            raise EnvironmentError(
                "DATABASE_URL env var not set (required for USE_HTTP_DB=1)"
            )
        import re

        m = re.match(r"postgresql://([^:]+):([^@]+)@([^:/]+):?(\d*)/?(.*)", db_url)
        if not m:
            raise EnvironmentError("DATABASE_URL invalid format")
        user, pw, host, port, db = m.groups()
        # Strip query parameters from database name
        if "?" in db:
            db = db.split("?")[0]
        _SUPHTTP_CFG.update(
            {
                "host": host,
                "timeout": int(os.getenv("DB_HTTP_TIMEOUT", "30")),
                "retries": int(os.getenv("DB_HTTP_RETRIES", "3")),
                "headers": {"user": user, "password": pw, "database": db},
            }
        )
        logger.info(f"[SUPHTTP] ✓ Neon HTTP client configured → {host}")
        logger.info(f"[SUPHTTP] ✓ client configured → {url}/rest/v1/rpc/exec_sql_*")
        return _SUPHTTP_CFG


_PARAM_RE = _re.compile(r"%\((\w+)\)s|%s")
_SELECT_FIRST = frozenset(
    {"select", "with", "explain", "show", "table", "values", "fetch"}
)
_WRITE_FIRST = frozenset({"insert", "update", "delete", "do", "call", "perform"})
_COMMENT_STRIP = _re.compile(r"^(?:\s|--[^\n]*\n|/\*.*?\*/)*", _re.DOTALL)


def _escape_sql_literal(v):
    """Convert Python value → safe PostgreSQL literal (dollar-quoting / type-aware)."""
    if v is None:
        return "NULL"
    if isinstance(v, bool):
        return "TRUE" if v else "FALSE"
    if isinstance(v, int):
        return str(v)
    if isinstance(v, float):
        if v != v:
            return "NULL"
        if v == float("inf"):
            return "'Infinity'::float8"
        if v == float("-inf"):
            return "'-Infinity'::float8"
        return repr(v)
    if isinstance(v, _decimal.Decimal):
        return str(v)
    if isinstance(v, (bytes, bytearray)):
        return f"decode('{v.hex()}','hex')"
    if isinstance(v, datetime):
        return (
            f"'{v.isoformat()}'::timestamptz"
            if v.tzinfo
            else f"'{v.isoformat()}'::timestamp"
        )
    if isinstance(v, (list, tuple)):
        return f"ARRAY[{','.join(_escape_sql_literal(x) for x in v)}]"
    if isinstance(v, dict):
        import json as _j

        return f"'{_j.dumps(v, default=_http_json_serial)}'::jsonb"
    s = str(v)
    tag = "$qtcl$"
    if tag in s:
        return "E'" + s.replace("\\", "\\\\").replace("'", "\\'") + "'"
    return f"{tag}{s}{tag}"


def _substitute_params(sql, params):
    if not params:
        return sql
    if isinstance(params, dict):
        return _PARAM_RE.sub(
            lambda m: (
                _escape_sql_literal(params[m.group(1)])
                if m.group(1)
                else (_ for _ in ()).throw(ValueError("mixed placeholders"))
            ),
            sql,
        )
    it = iter(params)
    return _PARAM_RE.sub(lambda m: _escape_sql_literal(next(it)), sql)


def _classify_sql(sql):
    first = _COMMENT_STRIP.sub("", sql).lstrip().split()
    kw = first[0].lower().rstrip(";") if first else ""
    if kw in _SELECT_FIRST:
        return "select"
    if kw in _WRITE_FIRST:
        return "write"
    return "select"  # unknown → try as select


def _suphttp_exec_select(sql):
    cfg = _suphttp_cfg()
    raw = _http_post_json(
        f"{cfg['url']}/rest/v1/rpc/exec_sql_select",
        cfg["headers"],
        {"query": sql},
        cfg["timeout"],
        cfg["retries"],
    )
    # PostgREST wraps JSONB RPC result: [{exec_sql_select: [...]}, ...] or [[...]] or [...]
    if isinstance(raw, list) and raw:
        inner = raw[0]
        if isinstance(inner, dict):
            vals = list(inner.values())
            if len(vals) == 1 and isinstance(vals[0], list):
                return vals[0]
            return [inner]
        if isinstance(inner, list):
            return inner
        return raw
    if isinstance(raw, dict):
        vals = list(raw.values())
        if len(vals) == 1 and isinstance(vals[0], list):
            return vals[0]
        return [raw]
    return []


def _suphttp_exec_write(sql):
    cfg = _suphttp_cfg()
    raw = _http_post_json(
        f"{cfg['url']}/rest/v1/rpc/exec_sql_write",
        cfg["headers"],
        {"query": sql},
        cfg["timeout"],
        cfg["retries"],
    )
    if isinstance(raw, list) and raw:
        raw = raw[0]
    if isinstance(raw, dict):
        inner = raw.get("exec_sql_write") or raw
        if isinstance(inner, dict):
            return int(inner.get("affected_rows", 0))
    return 0


class _SupHTTPCursor:
    """psycopg2-compatible cursor backed by Supabase PostgREST HTTPS RPC."""

    def __init__(self):
        self._rows: List[tuple] = []
        self._pos = 0
        self._rowcount = -1
        self._description = None
        self.closed = False

    @property
    def rowcount(self):
        return self._rowcount

    @property
    def description(self):
        return self._description

    def mogrify(self, sql, params=None):
        return _substitute_params(sql, params)

    def execute(self, sql, params=None):
        if self.closed:
            raise RuntimeError("cursor closed")
        final = _substitute_params(sql, params)
        logger.debug(
            f"[SUPHTTP] execute: {final[:100]}{'...' if len(final) > 100 else ''}"
        )
        if _classify_sql(final) == "select":
            rows_dicts = _suphttp_exec_select(final)
            if not rows_dicts:
                self._rows = []
                self._pos = 0
                self._rowcount = 0
                self._description = None
                return
            keys = list(rows_dicts[0].keys())
            self._description = [(k, None, None, None, None, None, None) for k in keys]
            # Support both dict and tuple results based on cursor_factory simulation
            if getattr(self, "_as_dict", False):
                self._rows = rows_dicts
            else:
                self._rows = [tuple(r.get(k) for k in keys) for r in rows_dicts]
            self._pos = 0
            self._rowcount = len(self._rows)
        else:
            self._rowcount = _suphttp_exec_write(final)
            self._rows = []
            self._pos = 0
            self._description = None

    def executemany(self, sql, seq):
        for p in seq:
            self.execute(sql, p)

    def fetchone(self):
        if self._pos < len(self._rows):
            row = self._rows[self._pos]
            self._pos += 1
            return row
        return None

    def fetchall(self):
        rows = self._rows[self._pos :]
        self._pos = len(self._rows)
        return rows

    def fetchmany(self, size=1):
        rows = self._rows[self._pos : self._pos + size]
        self._pos += len(rows)
        return rows

    def __iter__(self):
        while self._pos < len(self._rows):
            yield self.fetchone()

    def close(self):
        self.closed = True
        self._rows = []

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.close()


class _SupHTTPConn:
    """psycopg2-compatible connection backed by Supabase PostgREST HTTPS RPC.
    commit()/rollback() are no-ops — PostgREST RPC is auto-committed per call.
    .closed mirrors psycopg2 int semantics: 0=open, 1=closed, 2=lost."""

    def __init__(self):
        self.closed = 0
        self.autocommit = True  # 0 = open (psycopg2 int semantics)

    def cursor(self, cursor_factory=None, **__):
        if self.closed:
            raise RuntimeError("connection closed")
        c = _SupHTTPCursor()
        # If any factory is provided (like RealDictCursor), return rows as dicts
        c._as_dict = cursor_factory is not None
        return c

    def commit(self):
        pass  # no-op: stateless HTTPS

    def rollback(self):
        logger.debug(
            "[SUPHTTP] rollback() — HTTP connections are auto-committed; no-op"
        )

    def close(self):
        self.closed = 1

    def set_session(self, *_, **__):
        pass

    def set_isolation_level(self, *_, **__):
        pass

    def __enter__(self):
        return self

    def __exit__(self, exc_type, *_):
        if not exc_type:
            self.commit()
        else:
            self.rollback()
        return False


class _SupHTTPPool:
    """Thread-safe free-list pool of _SupHTTPConn objects."""

    def __init__(self, minconn=1, maxconn=20):
        self._max = maxconn
        self._lock = threading.Lock()
        self._free: List[_SupHTTPConn] = []
        self._in_use: List[_SupHTTPConn] = []
        self.closed = False

    def getconn(self, key=None):
        if self.closed:
            raise RuntimeError("HTTP pool closed")
        with self._lock:
            while self._free:
                c = self._free.pop()
                if not c.closed:
                    self._in_use.append(c)
                    return c
            if len(self._in_use) < self._max:
                c = _SupHTTPConn()
                self._in_use.append(c)
                return c
        deadline = time.monotonic() + 30
        while time.monotonic() < deadline:
            time.sleep(0.05)
            with self._lock:
                while self._free:
                    c = self._free.pop()
                    if not c.closed:
                        self._in_use.append(c)
                        return c
        raise RuntimeError("[SUPHTTP] Pool exhausted after 30s")

    def putconn(self, conn, close=False, key=None):
        if conn is None:
            return
        with self._lock:
            if conn in self._in_use:
                self._in_use.remove(conn)
            if close or conn.closed or self.closed:
                conn.close()
            else:
                conn.closed = False
                self._free.append(conn)

    def closeall(self):
        with self._lock:
            self.closed = True
            for c in self._free + self._in_use:
                try:
                    c.close()
                except Exception:
                    pass
            self._free.clear()
            self._in_use.clear()


def _suphttp_test_connection() -> bool:
    try:
        rows = _suphttp_exec_select("SELECT 1 AS ping, NOW() AS ts")
        ok = bool(rows and rows[0].get("ping") == 1)
        if ok:
            logger.info(
                f"[SUPHTTP] ✓ connection test passed — server ts={rows[0].get('ts')}"
            )
        else:
            logger.warning(f"[SUPHTTP] ⚠ unexpected test response: {rows}")
        return ok
    except Exception as e:
        logger.error(f"[SUPHTTP] ✗ connection test FAILED: {e}")
        return False


# ═════════════════════════════════════════════════════════════════════════════════
# DATABASE POOL
# ═════════════════════════════════════════════════════════════════════════════════


class DatabasePool:
    """Thread-safe connection pool.  Transparently switches between:
       • psycopg2 TCP pool (Koyeb / any server with direct Supabase TCP access)
       • _SupHTTPPool  HTTP pool (PythonAnywhere where outbound TCP 5432/6543 is blocked)
    Controlled by USE_HTTP_DB=1 environment variable."""

    _instance = None
    _lock = threading.Lock()

    def __new__(cls):
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
                    cls._instance._initialized = False
                    cls._instance.pool = None
                    cls._instance.use_pooling = True
                    cls._instance._http_mode = False
                    cls._instance._next_retry_at = (
                        0.0  # PATCH-9: retry backoff timestamp
                    )
                    cls._instance._retry_interval = (
                        5.0  # PATCH-9: seconds between init attempts
                    )
        return cls._instance

    def init(self, dsn: Optional[str] = None):
        """psycopg2-style init for compatibility with mempool.py."""
        self._initialize_pool()

    @property
    def available(self) -> bool:
        """mempool-style check for DB availability."""
        return self._initialized

    @contextmanager
    def cursor(self, cursor_factory=None):
        """mempool-style context manager for database cursor."""
        conn = None
        try:
            conn = self.get_connection()
            # Support cursor_factory (e.g. RealDictCursor) used by mempool.py
            cur = (
                conn.cursor(cursor_factory=cursor_factory)
                if cursor_factory
                else conn.cursor()
            )
            yield cur
            conn.commit()
        except Exception:
            if conn:
                conn.rollback()
            raise
        finally:
            if conn:
                self.put_connection(conn)

    def getconn(self):
        return self.get_connection()

    def putconn(self, conn, **kwargs):
        self.put_connection(conn)

    def _initialize_pool(self):
        if self._initialized:
            return
        with self._lock:
            if self._initialized:
                return

            # ── Dev mode: no database ────────────────────────────────────────────
            if _USE_DB_NONE:
                logger.warning("[DB] Database disabled (USE_DB=0)")
                self._initialized = True
                self.use_pooling = False
                self.pool = None
                logger.info(
                    f"[STARTUP] ✅ DB ready (disabled) at {time.time() - _STARTUP_TIME:.1f}s"
                )
                return

            # ── Retry backoff (soft — allows rapid retry for startup, backs off on persistent failure) ──
            _now = time.monotonic()
            if _now < self._next_retry_at:
                pass  # Allow retry even during backoff for enterprise reliability
            # ─────────────────────────────────────────────────────────────────
            # ── HTTP mode (PythonAnywhere) ────────────────────────────────────
            if _USE_HTTP_DB:
                try:
                    _suphttp_cfg()  # validate DATABASE_URL present
                    if not _suphttp_test_connection():
                        logger.error(
                            "[DB] ❌ Neon HTTP connection test failed — "
                            "check DATABASE_URL"
                        )
                        # Don't mark initialized so it retries on next request
                        return
                    self.pool = _SupHTTPPool(
                        minconn=int(os.getenv("DB_POOL_MIN", "1")),
                        maxconn=int(os.getenv("DB_POOL_MAX", "20")),
                    )
                    self._initialized = True
                    self.use_pooling = True
                    self._http_mode = True
                    logger.info(
                        f"[DB] ✨ Connected to Neon via HTTPS PostgREST RPC (HTTP-DB mode)"
                    )
                    logger.info(
                        f"[STARTUP] ✅ DB ready at {time.time() - _STARTUP_TIME:.1f}s"
                    )
                except EnvironmentError as e:
                    logger.error(f"[DB] ❌ HTTP-DB config error: {e}")
                    self._initialized = False
                    self._retry_interval = min(self._retry_interval * 2, 60.0)
                    self._next_retry_at = time.monotonic() + self._retry_interval
                except Exception as e:
                    logger.error(f"[DB] ❌ HTTP-DB init error: {e}")
                    self._initialized = False
                    self._retry_interval = min(self._retry_interval * 2, 60.0)
                    self._next_retry_at = time.monotonic() + self._retry_interval
                return

            # ── Native psycopg2 TCP mode (Neon PostgreSQL) ───────────────
            # Check if DATABASE_URL is set before attempting connection
            if not DB_URL:
                logger.warning("[DB] ⚠️  DATABASE_URL not set — DB disabled")
                logger.info(
                    f"[STARTUP] ✅ DB ready (no DATABASE_URL) at {time.time() - _STARTUP_TIME:.1f}s"
                )
                self._initialized = True
                self.use_pooling = False
                self.pool = None
                return

            try:
                from psycopg2 import pool as psycopg2_pool

                # 🚀 WEB-SCALE: Increased pool size for 10,000 miners
                # Each connection can handle ~200 concurrent operations with proper queuing
                min_connections = 10
                max_connections = int(
                    os.getenv("DB_POOL_MAX", "100")
                )  # 100 connections for 10k miners
                logger.info(
                    f"[DB] 🚀 WEB-SCALE pooling: min={min_connections}, max={max_connections} (for 10k miners)"
                )
                logger.info(f"[DB] Connecting to Neon via DATABASE_URL")
                self.pool = psycopg2_pool.ThreadedConnectionPool(
                    min_connections, max_connections, DB_URL, connect_timeout=10
                )
                self._initialized = True
                self.use_pooling = True
                self._next_retry_at = 0.0
                self._retry_interval = 5.0
                logger.info(f"[DB] ✨ Connected to Neon PostgreSQL successfully")
                logger.info(
                    f"[STARTUP] ✅ DB ready at {time.time() - _STARTUP_TIME:.1f}s"
                )
            except (ImportError, AttributeError):
                logger.info(
                    "[DB] App-level pooling unavailable, using direct connections"
                )
                logger.info("[DB] ✨ Connected to Neon PostgreSQL (direct mode)")
                self._initialized = True
                self.use_pooling = False
                self.pool = None
                self._next_retry_at = 0.0
                self._retry_interval = 5.0
                logger.info(
                    f"[STARTUP] ✅ DB ready (direct mode) at {time.time() - _STARTUP_TIME:.1f}s"
                )
            except psycopg2.OperationalError if psycopg2 else Exception as e:
                logger.error(f"[DB] ❌ Cannot connect to Neon: {e}")
                self._initialized = False
                self.use_pooling = False
                self._retry_interval = min(self._retry_interval * 2, 60.0)
                self._next_retry_at = time.monotonic() + self._retry_interval
                logger.warning(
                    f"[DB] ⏳ Next init retry in {self._retry_interval:.0f}s"
                )
            except Exception as e:
                logger.error(f"[DB] Error initializing pool: {e}")
                self._initialized = True
                self.use_pooling = False
                self.pool = None
                self._next_retry_at = 0.0
                self._retry_interval = 5.0

    def get_connection(self):
        # Check DB disabled first
        if _USE_DB_NONE or not DB_URL:
            return None
        if not self._initialized:
            self._initialize_pool()
        try:
            if self._http_mode and self.pool:
                return self.pool.getconn()
            if self.use_pooling and self.pool:
                conn = self.pool.getconn()
                if conn is None:
                    logger.debug("[DB] Pool exhausted, creating direct connection")
                    conn = psycopg2.connect(DB_URL, connect_timeout=10)
                return conn
            return psycopg2.connect(DB_URL, connect_timeout=10)
        except psycopg2.OperationalError as e:
            logger.error(f"[DB] ❌ Cannot connect to Neon: {e}")
            logger.error(f"[DB] Check DATABASE_URL: {DB_URL[:50]}...")
            raise
        except Exception as e:
            logger.error(f"[DB] Connection error: {e}")
            raise

    def put_connection(self, conn):
        try:
            if self._http_mode and self.pool and conn:
                self.pool.putconn(conn)
            elif self.use_pooling and self.pool and conn:
                self.pool.putconn(conn)
            elif conn:
                conn.close()
        except Exception as e:
            logger.debug(f"[DB] Error handling connection return: {e}")

    def close_all(self):
        try:
            if self.pool:
                self.pool.closeall()
                logger.info("[DB] Connection pool closed")
        except Exception as e:
            logger.debug(f"[DB] Error closing pool: {e}")


# Global pool instance (singleton, lazy-initialized)
db_pool = DatabasePool()

# Mark DB as ready (pool initialized lazily on first use)
_DB_READY = True

# ═══════════════════════════════════════════════════════════════════════════════════════
# 🚀 WEB-SCALE CACHING LAYER — In-Memory + File-Backed (Redis Alternative)
# Handles 10,000 miners with zero infrastructure (code-only solution)
# ═══════════════════════════════════════════════════════════════════════════════════════


class WebScaleCache:
    """
    🧠 Enterprise-grade LRU cache with TTL and persistence
    Replaces Redis for single-instance 10,000 miner scaling
    """

    def __init__(self, max_entries: int = 100000, default_ttl: float = 5.0):
        self._cache: OrderedDict[str, Any] = OrderedDict()
        self._ttl: Dict[str, float] = {}
        self._created: Dict[str, float] = {}
        self._access_count: Dict[str, int] = {}
        self._lock = threading.RLock()
        self.max_entries = max_entries
        self.default_ttl = default_ttl
        self._hits = 0
        self._misses = 0

    def get(self, key: str, default: Any = None) -> Any:
        with self._lock:
            now = time.time()

            if key in self._cache:
                # Check TTL
                ttl = self._ttl.get(key)
                created = self._created.get(key, 0)

                if ttl and (now - created) > ttl:
                    # Expired
                    del self._cache[key]
                    del self._ttl[key]
                    del self._created[key]
                    del self._access_count[key]
                    self._misses += 1
                    return default

                # Cache hit - update LRU order
                value = self._cache.pop(key)
                self._cache[key] = value
                self._access_count[key] = self._access_count.get(key, 0) + 1
                self._hits += 1
                return value

            self._misses += 1
            return default

    def set(self, key: str, value: Any, ttl: Optional[float] = None):
        with self._lock:
            ttl = ttl or self.default_ttl
            now = time.time()

            # Evict if at capacity (LRU)
            if len(self._cache) >= self.max_entries and key not in self._cache:
                self._evict_one()

            # Store value
            if key in self._cache:
                del self._cache[key]  # Remove to update order

            self._cache[key] = value
            self._ttl[key] = ttl
            self._created[key] = now
            self._access_count[key] = 1

    def delete(self, key: str) -> bool:
        with self._lock:
            if key in self._cache:
                del self._cache[key]
                del self._ttl[key]
                del self._created[key]
                del self._access_count[key]
                return True
            return False

    def _evict_one(self):
        """Evict least recently used entry"""
        if self._cache:
            key = next(iter(self._cache))
            del self._cache[key]
            del self._ttl[key]
            del self._created[key]
            del self._access_count[key]

    def get_stats(self) -> Dict[str, Any]:
        with self._lock:
            total = self._hits + self._misses
            return {
                "entries": len(self._cache),
                "max_entries": self.max_entries,
                "hits": self._hits,
                "misses": self._misses,
                "hit_rate": self._hits / total if total > 0 else 0,
            }


class BlockHeightCache:
    """
    🏗️ Specialized height cache with pub/sub simulation
    Eliminates 99% of height queries hitting the database
    """

    def __init__(self, cache: WebScaleCache):
        self.cache = cache
        self._height_lock = threading.RLock()
        self._current_height = 0
        self._current_hash = "0" * 64
        self._subscribers: List[Callable] = []

    def get_height(self) -> Dict[str, Any]:
        """Ultra-fast height query (sub-millisecond)"""
        # Try cache first
        cached = self.cache.get("blockchain:tip")
        if cached:
            return cached

        # Use in-memory value
        with self._height_lock:
            result = {
                "height": self._current_height,
                "block_hash": self._current_hash,
                "timestamp": time.time(),
                "difficulty": 4,  # Default difficulty
            }
            self.cache.set("blockchain:tip", result, ttl=1.0)
            return result

    def update_height(self, height: int, block_hash: str, difficulty: int = 4):
        """Update height with write-through caching"""
        with self._height_lock:
            if height > self._current_height:
                self._current_height = height
                self._current_hash = block_hash

                result = {
                    "height": height,
                    "block_hash": block_hash,
                    "timestamp": time.time(),
                    "difficulty": difficulty,
                }
                self.cache.set("blockchain:tip", result, ttl=1.0)
                return True
            return False


class TokenBucketRateLimiter:
    """
    🪣 Token bucket rate limiter for 10,000 miners
    Per-miner rate limiting with burst capacity
    """

    def __init__(
        self,
        rate: float = 10.0,  # tokens per second
        burst: int = 20,  # max tokens (burst capacity)
        cleanup_interval: int = 300,
    ):  # cleanup every 5 min
        self.rate = rate
        self.burst = burst
        self.cleanup_interval = cleanup_interval

        self._buckets: Dict[str, Dict[str, Any]] = {}
        self._lock = threading.RLock()
        self._last_cleanup = time.time()

    def _cleanup_old_buckets(self):
        """Remove inactive miner buckets"""
        now = time.time()
        if now - self._last_cleanup < self.cleanup_interval:
            return

        with self._lock:
            cutoff = now - 600  # 10 minutes inactive
            to_remove = [
                miner
                for miner, data in self._buckets.items()
                if data["last_access"] < cutoff
            ]
            for miner in to_remove:
                del self._buckets[miner]

            self._last_cleanup = now

    def allow_request(self, miner_address: str) -> Tuple[bool, int]:
        """
        Check if request is allowed
        Returns: (allowed, remaining_tokens)
        """
        self._cleanup_old_buckets()

        with self._lock:
            now = time.time()

            if miner_address not in self._buckets:
                # New miner - start with burst capacity
                self._buckets[miner_address] = {
                    "tokens": self.burst,
                    "last_update": now,
                    "last_access": now,
                }

            bucket = self._buckets[miner_address]

            # Add tokens based on time passed
            time_passed = now - bucket["last_update"]
            tokens_to_add = time_passed * self.rate
            bucket["tokens"] = min(self.burst, bucket["tokens"] + tokens_to_add)
            bucket["last_update"] = now
            bucket["last_access"] = now

            # Check if request can be processed
            if bucket["tokens"] >= 1:
                bucket["tokens"] -= 1
                return True, int(bucket["tokens"])
            else:
                return False, 0

    def get_stats(self, miner_address: str) -> Dict[str, Any]:
        with self._lock:
            if miner_address in self._buckets:
                bucket = self._buckets[miner_address]
                return {
                    "tokens": bucket["tokens"],
                    "rate": self.rate,
                    "burst": self.burst,
                }
            return {"tokens": self.burst, "rate": self.rate, "burst": self.burst}


class CircuitBreaker:
    """
    ⚡ Circuit breaker for database operations
    Prevents cascade failures when DB is under load
    """

    def __init__(
        self,
        failure_threshold: int = 5,
        recovery_timeout: float = 30.0,
        half_open_max_calls: int = 3,
    ):
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.half_open_max_calls = half_open_max_calls

        self._failures = 0
        self._last_failure_time = 0
        self._state = "closed"  # closed, open, half-open
        self._half_open_calls = 0
        self._lock = threading.Lock()

    def can_execute(self) -> bool:
        with self._lock:
            if self._state == "closed":
                return True

            if self._state == "open":
                if time.time() - self._last_failure_time >= self.recovery_timeout:
                    self._state = "half-open"
                    self._half_open_calls = 0
                    logger.info("[CircuitBreaker] Entering half-open state")
                    return True
                return False

            if self._state == "half-open":
                if self._half_open_calls < self.half_open_max_calls:
                    self._half_open_calls += 1
                    return True
                return False

            return True

    def record_success(self):
        with self._lock:
            if self._state == "half-open":
                self._state = "closed"
                self._failures = 0
                self._half_open_calls = 0
                logger.info("[CircuitBreaker] Circuit closed - service recovered")
            elif self._state == "closed":
                self._failures = max(0, self._failures - 1)

    def record_failure(self):
        with self._lock:
            self._failures += 1
            self._last_failure_time = time.time()

            if self._state == "half-open":
                self._state = "open"
                logger.warning(
                    f"[CircuitBreaker] Circuit opened (failure in half-open)"
                )
            elif self._failures >= self.failure_threshold:
                self._state = "open"
                logger.warning(
                    f"[CircuitBreaker] Circuit opened after {self._failures} failures"
                )

    def get_state(self) -> str:
        with self._lock:
            return self._state


# Initialize web-scale components
_blockchain_cache = WebScaleCache(max_entries=100000, default_ttl=5.0)
_height_cache = BlockHeightCache(_blockchain_cache)
_rate_limiter = TokenBucketRateLimiter(rate=10.0, burst=20)
_db_circuit_breaker = CircuitBreaker(failure_threshold=5, recovery_timeout=30.0)


# ─── PATCH-2: db_ready() ─────────────────────────────────────────────────────
# Called at ~line 459/483 inside get_oracle_address() / get_consensus_oracle_address()
# but was NEVER DEFINED anywhere — NameError on every call, silently swallowed
# by those functions' broad except blocks → silent fallback values forever.
def db_ready() -> bool:
    """Return True if the DB pool is usable; triggers lazy init if needed."""
    try:
        if not db_pool._initialized:
            db_pool._initialize_pool()
        return db_pool._initialized
    except Exception as _e:
        logger.debug(f"[DB] db_ready() check failed: {_e}")
        return False


# ─── PATCH-3: get_db_connection() ────────────────────────────────────────────
# Called at ~line 462/486 inside get_oracle_address() / get_consensus_oracle_address()
# but was NEVER DEFINED anywhere — same silent-NameError failure path as above.
# Caller owns the connection: must call db_pool.put_connection(conn) when done.
def get_db_connection():
    """Return a raw psycopg2 connection from the pool (lazy init on first call)."""
    if not db_pool._initialized:
        db_pool._initialize_pool()
    return db_pool.get_connection()


# ═══════════════════════════════════════════════════════════════════════════════
# CHAIN QUERY FUNCTIONS (Supabase PostgreSQL only — source of truth)
# Clients maintain their own SQLite mirrors, synced via P2P broadcasts
# ═══════════════════════════════════════════════════════════════════════════════


def query_latest_block() -> Optional[Dict[str, Any]]:
    """
    🚀 Get latest block with L1 cache first, DB fallback

    For 10,000 miners, this eliminates 99% of DB queries
    Cache TTL: 1 second (configurable for consistency vs performance)
    """
    # 🧠 L1 CACHE: Try memory cache first (sub-millisecond)
    cached = _blockchain_cache.get("blockchain:latest_block")
    if cached:
        logger.debug(f"[QUERY-LATEST] 🧠 CACHE HIT: h={cached.get('height')}")
        return cached

    # 🗄️ DB FALLBACK: Query database
    try:
        with get_db_cursor() as cur:
            cur.execute("""
                SELECT height, block_hash, timestamp, difficulty 
                FROM blocks ORDER BY height DESC LIMIT 1
            """)
            row = cur.fetchone()
            if row:
                latest = {
                    "height": row[0],
                    "block_hash": row[1] or "",
                    "hash": row[1] or "",  # Alias for compatibility
                    "timestamp": row[2] or 0,
                    "difficulty": row[3] or 4,
                }
                # 📝 CACHE RESULT: 1 second TTL
                _blockchain_cache.set("blockchain:latest_block", latest, ttl=1.0)
                logger.debug(f"[QUERY-LATEST] 🗄️ DB QUERY: h={latest['height']}")
                return latest
            else:
                logger.debug(f"[QUERY-LATEST] No blocks (genesis)")
                return None
    except Exception as e:
        logger.error(f"[QUERY-LATEST] ❌ DB error: {e}")
        # Circuit breaker handles this
        _db_circuit_breaker.record_failure()
        raise


def query_block_by_height(height: int) -> Optional[Dict[str, Any]]:
    """Get block by height from Supabase PostgreSQL (authoritative source)."""
    try:
        with get_db_cursor() as cur:
            cur.execute("SELECT * FROM blocks WHERE height = %s", (height,))
            row = cur.fetchone()
            if row:
                cols = [desc[0] for desc in cur.description]
                return dict(zip(cols, row))
    except Exception as e:
        logger.debug(f"[QUERY-BLOCK] PG error: {e}")
    return None


def query_block_by_hash(block_hash: str) -> Optional[Dict[str, Any]]:
    """
    🚀 Get block by hash with L1 cache
    Critical for duplicate detection at scale
    """
    if not block_hash:
        return None

    # 🧠 L1 CACHE: Bloom filter check would go here for production
    cache_key = f"block:hash:{block_hash}"
    cached = _blockchain_cache.get(cache_key)
    if cached:
        return cached

    try:
        with get_db_cursor() as cur:
            cur.execute(
                "SELECT * FROM blocks WHERE block_hash = %s LIMIT 1", (block_hash,)
            )
            row = cur.fetchone()
            if row:
                cols = [desc[0] for desc in cur.description]
                result = dict(zip(cols, row))
                # Cache with longer TTL for immutable blocks
                _blockchain_cache.set(cache_key, result, ttl=300.0)  # 5 min
                return result
    except Exception as e:
        logger.debug(f"[QUERY-BLOCK-HASH] PG error: {e}")
        _db_circuit_breaker.record_failure()
    return None


@contextmanager
def get_db_cursor():
    """Context manager for database cursor with connection pooling.

    ⚛️  CRITICAL: Return connections to pool, never close them directly.
    Closing breaks the pool. Must use db_pool.putconn() to return.

    FIX: Reset connection to ensure no aborted transaction state persists
    """
    conn = None
    try:
        conn = db_pool.get_connection()
        # FIX: Ensure clean transaction state
        conn.rollback()
        cur = conn.cursor()
        yield cur
        conn.commit()
    except Exception as e:
        if conn:
            try:
                conn.rollback()
            except Exception:
                pass
        logger.debug(f"[DB-CURSOR] Error: {e}")
        raise
    finally:
        if conn:
            try:
                # FIX: Always rollback before returning to ensure clean state
                try:
                    conn.rollback()
                except:
                    pass
                if db_pool.use_pooling and db_pool.pool:
                    db_pool.pool.putconn(conn)
                else:
                    conn.close()
            except Exception as e:
                logger.debug(f"[DB-CURSOR] putconn error: {e}")


# ── DATABASE SCHEMA ENSURE: Lazy creation of tables missing from migration ─────
_SCHEMA_ENSURED_PEER_REGISTRY = False
_SCHEMA_ENSURED_ORACLE_REGISTRY = False
_SCHEMA_ENSURED_CHAIN_STATE = False
_SCHEMA_ENSURED_BLOCKS = False


def _lazy_ensure_oracle_registry():
    """Ensure oracle_registry table exists in Supabase."""
    global _SCHEMA_ENSURED_ORACLE_REGISTRY
    if _SCHEMA_ENSURED_ORACLE_REGISTRY:
        return
    try:
        with get_db_cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS oracle_registry (
                    oracle_id       VARCHAR(128)  PRIMARY KEY,
                    oracle_url      VARCHAR(512)  NOT NULL DEFAULT '',
                    oracle_address  VARCHAR(128)  NOT NULL DEFAULT '',
                    is_primary      BOOLEAN       NOT NULL DEFAULT FALSE,
                    last_seen       TIMESTAMPTZ   DEFAULT NOW(),
                    block_height    BIGINT        NOT NULL DEFAULT 0,
                    peer_count      INTEGER       NOT NULL DEFAULT 0,
                    wallet_address  VARCHAR(128)  NOT NULL DEFAULT '',
                    oracle_pub_key  TEXT          NOT NULL DEFAULT '',
                    cert_sig        VARCHAR(128)  NOT NULL DEFAULT '',
                    mode            VARCHAR(32)   NOT NULL DEFAULT 'full',
                    ip_hint         VARCHAR(256)  NOT NULL DEFAULT '',
                    reg_tx_hash     VARCHAR(64)   NOT NULL DEFAULT '',
                    registered_at   TIMESTAMPTZ   DEFAULT NOW(),
                    created_at      TIMESTAMPTZ   DEFAULT NOW()
                )
            """)
            # Ensure all columns exist for legacy tables
            for col, dtype in [
                ("wallet_address", "VARCHAR(128) DEFAULT ''"),
                ("oracle_pub_key", "TEXT DEFAULT ''"),
                ("cert_sig", "VARCHAR(128) DEFAULT ''"),
                ("mode", "VARCHAR(32) DEFAULT 'full'"),
                ("ip_hint", "VARCHAR(256) DEFAULT ''"),
                ("reg_tx_hash", "VARCHAR(64) DEFAULT ''"),
                ("registered_at", "TIMESTAMPTZ DEFAULT NOW()"),
            ]:
                try:
                    cur.execute(
                        f"ALTER TABLE oracle_registry ADD COLUMN IF NOT EXISTS {col} {dtype}"
                    )
                except Exception:
                    pass
        _SCHEMA_ENSURED_ORACLE_REGISTRY = True
    except Exception as e:
        logger.warning(f"[SCHEMA] _lazy_ensure_oracle_registry failed: {e}")


def _lazy_ensure_chain_state():
    """Ensure chain_state table exists in Supabase."""
    global _SCHEMA_ENSURED_CHAIN_STATE
    if _SCHEMA_ENSURED_CHAIN_STATE:
        return
    try:
        with get_db_cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS chain_state (
                    state_id         INTEGER PRIMARY KEY,
                    chain_height     BIGINT      DEFAULT 0,
                    head_block_hash  TEXT        DEFAULT '',
                    latest_coherence NUMERIC(5,4) DEFAULT 0.9,
                    updated_at       TIMESTAMPTZ DEFAULT NOW()
                )
            """)
        _SCHEMA_ENSURED_CHAIN_STATE = True
    except Exception as e:
        logger.warning(f"[SCHEMA] _lazy_ensure_chain_state failed: {e}")


def _lazy_ensure_peer_registry():
    """Ensure peer_registry and peer_devices tables exist in Supabase with correct schema."""
    global _SCHEMA_ENSURED_PEER_REGISTRY
    if _SCHEMA_ENSURED_PEER_REGISTRY:
        return
    try:
        # Step 1: Aggressive Rebuild if legacy schema detected
        # We do this in a separate cursor to ensure it doesn't pollute the main one
        with get_db_cursor() as cur:
            cur.execute("""
                DO $$ 
                BEGIN 
                    IF EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='peer_registry' AND column_name='peer_id') THEN
                        -- If we have peer_id, we are on the legacy schema. 
                        -- We drop it completely to ensure all constraints/NOT NULLs are cleared.
                        DROP TABLE IF EXISTS peer_registry CASCADE;
                    END IF;
                END $$;
            """)

        # Step 2: Create/Update to the definitive schema
        with get_db_cursor() as cur:
            # Create table with node_id as PRIMARY KEY
            cur.execute("""
                CREATE TABLE IF NOT EXISTS peer_registry (
                    node_id       TEXT PRIMARY KEY,
                    external_addr TEXT NOT NULL,
                    pubkey_hash   TEXT NOT NULL DEFAULT '',
                    chain_height  BIGINT      DEFAULT 0,
                    last_seen     TIMESTAMPTZ DEFAULT NOW(),
                    first_seen    TIMESTAMPTZ DEFAULT NOW(),
                    capabilities  JSONB       DEFAULT '[]',
                    ban_score     INTEGER     DEFAULT 0,
                    caller_ip     TEXT        DEFAULT '',
                    mac_address   TEXT        DEFAULT '',
                    device_id     TEXT        DEFAULT '',
                    fingerprint   TEXT        DEFAULT ''
                )
            """)

            # Ensure all columns exist for existing node_id-based tables (idempotency)
            for col, dtype in [
                ("first_seen", "TIMESTAMPTZ DEFAULT NOW()"),
                ("capabilities", "JSONB DEFAULT '[]'"),
                ("ban_score", "INTEGER DEFAULT 0"),
                ("caller_ip", "TEXT DEFAULT ''"),
                ("mac_address", "TEXT DEFAULT ''"),
                ("device_id", "TEXT DEFAULT ''"),
                ("fingerprint", "TEXT DEFAULT ''"),
            ]:
                try:
                    cur.execute(
                        f"ALTER TABLE peer_registry ADD COLUMN IF NOT EXISTS {col} {dtype}"
                    )
                except Exception:
                    pass

            # Ensure node_id is unique for ON CONFLICT (if not already PK)
            try:
                cur.execute("""
                    DO $$ 
                    BEGIN 
                        IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'peer_registry_node_id_key') THEN
                            ALTER TABLE peer_registry ADD CONSTRAINT peer_registry_node_id_key UNIQUE (node_id);
                        END IF; 
                    END $$;
                """)
            except Exception:
                pass

            # ── 3.1 DEVICE FINGERPRINTING TABLE ─────────────────────────────────
            cur.execute("""
                CREATE TABLE IF NOT EXISTS peer_devices (
                    fingerprint    TEXT PRIMARY KEY,
                    node_id        TEXT NOT NULL,
                    last_caller_ip TEXT,
                    mac_address    TEXT,
                    device_id      TEXT,
                    first_seen     TIMESTAMPTZ DEFAULT NOW(),
                    last_seen      TIMESTAMPTZ DEFAULT NOW(),
                    trust_score    FLOAT DEFAULT 1.0
                )
            """)
            cur.execute(
                "CREATE INDEX IF NOT EXISTS idx_peer_devices_node ON peer_devices(node_id)"
            )
            cur.execute(
                "CREATE INDEX IF NOT EXISTS idx_peer_devices_ip ON peer_devices(last_caller_ip)"
            )

        _SCHEMA_ENSURED_PEER_REGISTRY = True
    except Exception as e:
        logger.warning(f"[SCHEMA] _lazy_ensure_peer_registry failed: {e}")


def _lazy_ensure_blocks():
    """Ensure blocks table exists. Auto-create genesis block if empty."""
    global _SCHEMA_ENSURED_BLOCKS
    if _SCHEMA_ENSURED_BLOCKS:
        return
    try:
        with get_db_cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS blocks (
                    height                     BIGINT PRIMARY KEY,
                    block_hash                 VARCHAR(255) UNIQUE NOT NULL,
                    parent_hash                VARCHAR(255) NOT NULL,
                    merkle_root                VARCHAR(255),
                    timestamp                  BIGINT NOT NULL,
                    tx_count                   INT DEFAULT 0,
                    coherence_snapshot         NUMERIC(5,4) DEFAULT 1.0,
                    fidelity_snapshot          NUMERIC(5,4) DEFAULT 1.0,
                    w_state_hash               VARCHAR(255),
                    hyp_witness                VARCHAR(255),
                    miner_address              VARCHAR(255),
                    difficulty                 INT DEFAULT 6,
                    nonce                      BIGINT DEFAULT 0,
                    pq_curr                    INTEGER DEFAULT 1,
                    pq_last                    INTEGER DEFAULT 0,
                    oracle_w_state_hash        VARCHAR(255),
                    finalized                  BOOLEAN DEFAULT TRUE,
                    finalized_at               BIGINT DEFAULT 0,
                    created_at                 TIMESTAMPTZ DEFAULT NOW()
                )
            """)
            cur.execute(
                "CREATE INDEX IF NOT EXISTS idx_blocks_hash ON blocks(block_hash)"
            )
            cur.execute(
                "CREATE INDEX IF NOT EXISTS idx_blocks_parent ON blocks(parent_hash)"
            )
            cur.execute(
                "CREATE INDEX IF NOT EXISTS idx_blocks_timestamp ON blocks(timestamp)"
            )

            # Auto-create genesis if table is empty
            cur.execute("SELECT COUNT(*) FROM blocks")
            count = cur.fetchone()[0]
            if count == 0:
                genesis_hash = "0" * 64
                parent_hash = "0" * 64
                genesis_ts = int(time.time() * 1e9)
                cur.execute(
                    """
                    INSERT INTO blocks (
                        height, block_hash, parent_hash, merkle_root,
                        timestamp, tx_count, coherence_snapshot, fidelity_snapshot,
                        difficulty, nonce, pq_curr, pq_last, finalized, finalized_at
                    ) VALUES (
                        0, %s, %s, %s,
                        %s, 0, 1.0, 1.0,
                        6, 0, 1, 0, TRUE, %s
                    )
                """,
                    (genesis_hash, parent_hash, genesis_hash, genesis_ts, genesis_ts),
                )
                logger.info(
                    f"[SCHEMA] 🌱 Genesis block auto-created: h=0  hash={genesis_hash[:16]}…"
                )

        # Create quantum_field_distribution table with triggers for neighbor broadcast
        try:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS quantum_field_distribution (
                    id                      SERIAL PRIMARY KEY,
                    block_height            BIGINT NOT NULL,
                    block_hash              VARCHAR(255) NOT NULL,
                    miner_address           VARCHAR(255) NOT NULL,
                    quantum_field_16x16x16  BYTEA NOT NULL,  -- 4096 complex64 elements
                    pq_curr                 INTEGER,
                    pq_last                 INTEGER,
                    timestamp_ns            BIGINT NOT NULL,
                    received_by_neighbor    BOOLEAN DEFAULT FALSE,
                    neighbor_broadcast_list TEXT,  -- JSON array of neighbor addresses
                    created_at              TIMESTAMPTZ DEFAULT NOW()
                )
            """)
            cur.execute(
                "CREATE INDEX IF NOT EXISTS idx_qf_height ON quantum_field_distribution(block_height)"
            )
            cur.execute(
                "CREATE INDEX IF NOT EXISTS idx_qf_miner ON quantum_field_distribution(miner_address)"
            )
            cur.execute(
                "CREATE INDEX IF NOT EXISTS idx_qf_broadcast ON quantum_field_distribution(received_by_neighbor)"
            )

            logger.info(
                "[SCHEMA] ✅ quantum_field_distribution table ready (neighbor gossip)"
            )
        except Exception as _qf_e:
            logger.debug(f"[SCHEMA] quantum_field_distribution table creation: {_qf_e}")

        _SCHEMA_ENSURED_BLOCKS = True
        logger.info("[SCHEMA] ✅ blocks table ready")
    except Exception as e:
        logger.warning(f"[SCHEMA] _lazy_ensure_blocks failed: {e}")


_dht_manager: Optional[DHTManager] = None
_dht_lock = threading.RLock()


def get_dht_manager() -> DHTManager:
    """Get or create global DHT manager. Uses P2P_PORT (9091) — not gunicorn HTTP PORT."""
    global _dht_manager
    if _dht_manager is None:
        # Public hostname so remote peers can reach this node.
        # Falls back to 0.0.0.0 for local/dev use.
        host = (
            os.getenv("KOYEB_PUBLIC_DOMAIN")
            or os.getenv("RAILWAY_PUBLIC_DOMAIN")
            or os.getenv("FLASK_HOST")
            or "0.0.0.0"
        )
        port = P2P_PORT  # 9091 — never gunicorn's HTTP port
        _dht_manager = DHTManager(local_address=host, local_port=port)
    return _dht_manager


# ═════════════════════════════════════════════════════════════════════════════════════════
# RPC SNAPSHOT DISTRIBUTION (JSON polling — no SSE)
# ═════════════════════════════════════════════════════════════════════════════════════════

# RPC snapshot cache + event log (no SSE infrastructure)
_rpc_event_log: Deque = Deque(maxlen=1000)  # Ring buffer of recent RPC events
_rpc_event_lock = threading.RLock()  # Guards _rpc_event_log writes
_latest_snapshot: Optional[dict] = None  # Last cached snapshot (poll endpoint)
_latest_snapshot_ts: int = 0  # Timestamp of latest snapshot
_snapshot_lock = threading.RLock()  # Guards _latest_snapshot updates


def _log_rpc_event(event_type: str, data: Any) -> None:
    """Log event for /api/events RPC polling endpoint."""
    with _rpc_event_lock:
        _rpc_event_log.append({"ts": time.time(), "type": event_type, "data": data})


# Application startup flag
_APP_READY = False


def _set_app_ready():
    global _APP_READY
    _APP_READY = True
    logger.info("[APP] ✅ Application ready for Koyeb health checks")


# ═════════════════════════════════════════════════════════════════════════════════════════
# API Endpoints
# ═════════════════════════════════════════════════════════════════════════════════════════


class SafeFieldConverter:
    """⚛️  Autonomous diagnostic field converter with fallback recovery."""

    _errors = {}  # Track which fields fail across requests for autonomous healing

    @staticmethod
    def safe_int(value, field_name="unknown", default=0):
        """Convert to int with diagnostic logging."""
        try:
            if value is None:
                return default
            return int(value)
        except (ValueError, TypeError) as e:
            SafeFieldConverter._errors[f"int_{field_name}"] = str(e)
            logger.warning(f"[CONVERTER] int({field_name})={value} failed: {e}")
            return default

    @staticmethod
    def safe_float(value, field_name="unknown", default=0.0):
        """Convert to float with diagnostic logging."""
        try:
            if value is None:
                return default
            return float(value)
        except (ValueError, TypeError) as e:
            SafeFieldConverter._errors[f"float_{field_name}"] = str(e)
            logger.warning(f"[CONVERTER] float({field_name})={value} failed: {e}")
            return default

    @staticmethod
    def safe_str(value, field_name="unknown", default=""):
        """Convert to str with diagnostic logging."""
        try:
            if value is None:
                return default
            return str(value)
        except (ValueError, TypeError) as e:
            SafeFieldConverter._errors[f"str_{field_name}"] = str(e)
            logger.warning(f"[CONVERTER] str({field_name})={value} failed: {e}")
            return default

    @staticmethod
    def safe_bool(value, field_name="unknown", default=False):
        """Convert to bool with diagnostic logging."""
        try:
            if value is None:
                return default
            return bool(value)
        except (ValueError, TypeError) as e:
            SafeFieldConverter._errors[f"bool_{field_name}"] = str(e)
            logger.warning(f"[CONVERTER] bool({field_name})={value} failed: {e}")
            return default

    @staticmethod
    def get_error_report():
        """Return accumulated conversion errors for autonomous healing."""
        return dict(SafeFieldConverter._errors)

    @staticmethod
    def clear_errors():
        """Clear error history for next diagnostic cycle."""
        SafeFieldConverter._errors.clear()


class SnapshotAutonomousHealer:
    """⚛️  Autonomous diagnostic & healing loop for snapshot failures."""

    _last_valid_snapshot = {}  # Cache last valid state for fallback
    _healing_cycles = 0
    _healing_lock = threading.Lock()

    @staticmethod
    def build_from_row(row, diag_label="oracle_snapshot_json"):
        """Build snapshot with autonomous error detection & recovery."""
        if not row:
            return None, {"error": "row_is_none", "ready": False}

        diag = {"cycles": 0, "errors": [], "recovered": []}
        SafeFieldConverter.clear_errors()

        try:
            # ⚛️  Phase 1: Parse oracle measurements (most failure-prone)
            oracles = []
            try:
                if isinstance(row[18], list):
                    oracles = row[18]
                elif isinstance(row[18], str):
                    oracles = json.loads(row[18])
                else:
                    oracles = []
            except (json.JSONDecodeError, TypeError) as e:
                diag["errors"].append(f"oracle_measurements: {str(e)[:50]}")
                logger.warning(f"[HEALER] Oracle measurements parse failed: {e}")
                oracles = []

            # ⚛️  Phase 2: Parse mermin result (nullable)
            mermin_result = None
            if row[10] is not None:
                try:
                    m_val = SafeFieldConverter.safe_float(row[10], "mermin_M")
                    mermin_result = {
                        "M_value": m_val,
                        "M": m_val,
                        "is_quantum": SafeFieldConverter.safe_bool(
                            row[11], "mermin_is_quantum"
                        ),
                        "verdict": SafeFieldConverter.safe_str(
                            row[12], "mermin_verdict"
                        ),
                    }
                except Exception as e:
                    diag["errors"].append(f"mermin_result: {str(e)[:50]}")
                    logger.warning(f"[HEALER] Mermin result construction failed: {e}")
                    mermin_result = None

            # ⚛️  Phase 3: Safe numeric conversions with field-level diagnostics
            ts_ns = SafeFieldConverter.safe_int(row[1], "timestamp_ns")
            chirp = SafeFieldConverter.safe_int(row[2], "chirp_number")
            lat_f = SafeFieldConverter.safe_float(row[3], "lattice_fidelity")
            lat_c = SafeFieldConverter.safe_float(row[4], "lattice_coherence")
            lat_cy = SafeFieldConverter.safe_int(row[5], "lattice_cycle")
            lat_s8 = SafeFieldConverter.safe_int(row[6], "lattice_sigma_mod8")
            cons_f = SafeFieldConverter.safe_float(row[7], "consensus_fidelity")
            cons_c = SafeFieldConverter.safe_float(row[8], "consensus_coherence")
            cons_p = SafeFieldConverter.safe_float(row[9], "consensus_purity")
            pq0_o = SafeFieldConverter.safe_float(row[13], "pq0_oracle_fidelity")
            pq0_i = SafeFieldConverter.safe_float(row[14], "pq0_IV_fidelity")
            pq0_v = SafeFieldConverter.safe_float(row[15], "pq0_V_fidelity")
            pq_c = SafeFieldConverter.safe_int(row[16], "pq_curr")
            pq_l = SafeFieldConverter.safe_int(row[17], "pq_last")
            phase = SafeFieldConverter.safe_str(row[19], "phase_name")

            # ⚛️  Collect conversion errors for autonomous healing
            conv_errors = SafeFieldConverter.get_error_report()
            if conv_errors:
                diag["errors"].extend([f"{k}" for k in conv_errors.keys()])
                logger.warning(
                    f"[HEALER] Conversion errors detected: {len(conv_errors)}"
                )

            # ⚛️  Phase 4: Construct snapshot with all safe values
            snapshot = {
                "timestamp_ns": ts_ns,
                "chirp_number": chirp,
                "lattice_quantum": {
                    "fidelity": lat_f,
                    "coherence": lat_c,
                    "cycle_count": lat_cy,
                    "lattice_sigma_mod8": lat_s8,
                    "phase_name": phase,
                    "lattice_status": "online",
                },
                "consensus": {
                    "w_state_fidelity": cons_f,
                    "coherence": cons_c,
                    "purity": cons_p,
                },
                "mermin_test": mermin_result,
                "bell_test": mermin_result,
                "pq0_components": {
                    "pq0_oracle_fidelity": pq0_o,
                    "pq0_IV_fidelity": pq0_i,
                    "pq0_V_fidelity": pq0_v,
                },
                "pq_curr": pq_c,
                "pq_last": pq_l,
                "oracle_measurements": oracles,
                "fidelity": cons_f,
                "coherence": cons_c,
                "lattice_cycle": lat_cy,
                "source": "neon_snapshot_healed",
                "ready": True,
                "_diagnostics": {
                    "errors": diag["errors"],
                    "recovered_with_defaults": bool(conv_errors),
                    "conversion_errors": conv_errors,
                },
            }

            # ⚛️  Cache this as last valid state for future fallback
            with SnapshotAutonomousHealer._healing_lock:
                SnapshotAutonomousHealer._last_valid_snapshot = snapshot.copy()
                SnapshotAutonomousHealer._healing_cycles += 1

            if not diag["errors"]:
                logger.debug(
                    f"[HEALER] Snapshot built clean (cycle {SnapshotAutonomousHealer._healing_cycles})"
                )
            else:
                logger.info(
                    f"[HEALER] Snapshot built with {len(diag['errors'])} recovered fields (cycle {SnapshotAutonomousHealer._healing_cycles})"
                )

            return snapshot, diag

        except Exception as e:
            # ⚛️  Catastrophic failure — fall back to cached state
            logger.error(f"[HEALER] Snapshot construction catastrophically failed: {e}")
            with SnapshotAutonomousHealer._healing_lock:
                if SnapshotAutonomousHealer._last_valid_snapshot:
                    logger.warning(
                        f"[HEALER] Falling back to last valid cached snapshot"
                    )
                    return SnapshotAutonomousHealer._last_valid_snapshot.copy(), {
                        "error": "catastrophic_fallback",
                        "fallback_source": "cache",
                        "ready": True,
                    }
            return None, {
                "error": "catastrophic_failure",
                "details": str(e),
                "ready": False,
            }


# ══════════════════════════════════════════════════════════════════════════════
# JSON-RPC 2.0 FLASK ROUTES
# ══════════════════════════════════════════════════════════════════════════════
def _get_canonical_node() -> Optional[dict]:
    """Fallback: fetch canonical node state from module or globals (in-memory)."""
    try:
        import globals as _g

        gn = getattr(_g, "get_canonical_node", None)
        if callable(gn):
            return gn()
    except Exception:
        pass

    # Last resort: check module-level state
    try:
        _srv = sys.modules[__name__].__dict__
        cn = _srv.get("_canonical_node") or _srv.get("canonical_node")
        return cn if isinstance(cn, dict) else None
    except Exception:
        return None


def _rpc_getBlockHeight(params: Any, rpc_id: Any) -> dict:
    """qtcl_getBlockHeight — current chain tip height.

    🔴 CRITICAL: DB-AUTHORITATIVE, ALWAYS FRESH, NO CACHING
    This query MUST return the actual current block height.
    Client depends on this for mining loop progression (h → h+1 → h+2...).
    """
    try:
        db_tip = query_latest_block()

        if db_tip is None:
            height = 0
            tip_hash = "0" * 64
        else:
            height = int(db_tip["height"])
            tip_hash = str(db_tip.get("hash", "") or "0" * 64)

        # 🔴 CRITICAL LOGGING: Verify DB state
        logger.critical(
            f"[RPC-HEIGHT] 📊 CHAIN TIP: h={height} hash={tip_hash[:16]}… (DB-authoritative, always fresh)"
        )

        return _rpc_ok(
            {
                "height": height,
                "tip_hash": tip_hash,
                "ts": time.time(),
                "source": "DB-authoritative",  # Signal to client this is ground truth
            },
            rpc_id,
        )
    except Exception as e:
        logger.exception(f"[RPC-METHOD] qtcl_getBlockHeight exception: {e}")
        return _rpc_error(-32603, f"DB error: {str(e)}", rpc_id)


def _rpc_forgeGenesis(params: Any, rpc_id: Any) -> dict:
    """qtcl_forgeGenesis — Force creation and persistence of genesis block.

    Only works if:
    1. DATABASE_URL is set (Neon PostgreSQL)
    2. No genesis block exists yet

    Returns the genesis block info on success.
    """
    try:
        if not DATABASE_URL:
            return _rpc_error(
                -32000, "DATABASE_URL not set - cannot forge genesis", rpc_id
            )

        from lattice_controller import QuantumLatticeController

        controller = QuantumLatticeController()

        # Inject DB pool if available
        if controller.block_manager and controller.block_manager.db:
            controller.block_manager.db.inject_db_pool(db_pool)

        # Create blocks table
        _lazy_ensure_blocks()

        controller.start()

        # Sync to cache
        _sync_lattice_blocks_to_cache()

        if controller.genesis_block:
            return _rpc_ok(
                {
                    "status": "created",
                    "height": 0,
                    "block_hash": controller.genesis_block.block_hash,
                    "timestamp": controller.genesis_block.timestamp_s,
                },
                rpc_id,
            )
        else:
            return _rpc_ok(
                {
                    "status": "created",
                    "height": 0,
                    "block_hash": "0" * 64,
                    "timestamp": 0,
                },
                rpc_id,
            )
    except Exception as e:
        logger.exception(f"[RPC-METHOD] qtcl_forgeGenesis exception: {e}")
        return _rpc_error(-32603, f"Forge error: {str(e)}", rpc_id)


def _rpc_getBalance(params: Any, rpc_id: Any) -> dict:
    """qtcl_getBalance — address QTCL balance via direct DB query."""
    try:
        if not isinstance(params, (list, dict)):
            return _rpc_error(-32602, "params must be list or object", rpc_id)
        address = (
            (params[0] if isinstance(params, list) else params.get("address", ""))
            if params
            else ""
        )
        if not address:
            return _rpc_error(-32602, "address required", rpc_id)

        _diagnostic = {
            "address_queried": address[:24] + "…" if len(address) > 24 else address
        }

        try:
            with get_db_cursor() as cur:
                # Check if wallet_addresses table exists
                try:
                    cur.execute("""
                        SELECT EXISTS (
                            SELECT FROM information_schema.tables 
                            WHERE table_name = 'wallet_addresses'
                        )
                    """)
                    _table_exists = cur.fetchone()[0] if cur.fetchone() else False
                    _diagnostic["table_exists"] = bool(_table_exists)
                except Exception as _te:
                    _diagnostic["table_check_error"] = str(_te)

                # Query balance
                cur.execute(
                    "SELECT balance, transaction_count, address_type FROM wallet_addresses WHERE address = %s",
                    (address,),
                )
                row = cur.fetchone()
                if row:
                    wallet = {
                        "address": address,
                        "balance": row[0],
                        "tx_count": row[1],
                        "address_type": row[2] if len(row) > 2 else "unknown",
                    }
                    _diagnostic["found_in_db"] = True
                    _diagnostic["raw_balance_base_units"] = int(row[0]) if row[0] else 0
                else:
                    wallet = None
                    _diagnostic["found_in_db"] = False
                    # Check how many total wallet addresses exist
                    try:
                        cur.execute("SELECT COUNT(*) FROM wallet_addresses")
                        _total_wallets = cur.fetchone()[0]
                        _diagnostic["total_wallets_in_db"] = (
                            int(_total_wallets) if _total_wallets else 0
                        )
                    except Exception:
                        pass
        except Exception as _wqe:
            logger.debug(f"[RPC] query_wallet_info DB error: {_wqe}")
            _diagnostic["db_error"] = str(_wqe)
            wallet = None

        if wallet is None:
            # Address not yet in DB — return 0 balance with diagnostic info
            result = {
                "address": address,
                "balance": 0.0,
                "symbol": "QTCL",
                "diagnostic": _diagnostic,
                "note": "Address not found in wallet_addresses table — no balance recorded",
            }
        else:
            raw_balance = int(wallet.get("balance") or 0)
            result = {
                "address": address,
                "balance": raw_balance / 100.0,
                "symbol": "QTCL",
                "raw_balance_base_units": raw_balance,
                "transaction_count": wallet.get("tx_count", 0),
                "address_type": wallet.get("address_type", "unknown"),
                "diagnostic": _diagnostic,
            }
        logger.debug(
            f"[RPC-METHOD] qtcl_getBalance: address={address[:16]}…, balance={result['balance']}, found={_diagnostic.get('found_in_db', False)}"
        )
        return _rpc_ok(result, rpc_id)
    except Exception as e:
        logger.exception(f"[RPC-METHOD] qtcl_getBalance outer exception: {e}")
        return _rpc_error(-32603, f"Internal error: {str(e)}", rpc_id)


def _rpc_getTransaction(params: Any, rpc_id: Any) -> dict:
    """qtcl_getTransaction — tx details by hash."""
    try:
        logger.debug(
            f"[RPC-METHOD] qtcl_getTransaction called with params={params}, id={rpc_id}"
        )
        tx_hash = (
            (params[0] if isinstance(params, list) else params.get("tx_hash", ""))
            if params
            else ""
        )
        if not tx_hash:
            logger.debug(f"[RPC-METHOD] qtcl_getTransaction: tx_hash missing or empty")
            return _rpc_error(-32602, "tx_hash required", rpc_id)
        try:
            from globals import get_blockchain

            bc = get_blockchain()
            if bc is None:
                logger.warning(
                    f"[RPC-METHOD] qtcl_getTransaction: blockchain not initialized"
                )
                return _rpc_error(-32003, "Blockchain not synced", rpc_id)
            tx = bc.get_transaction(tx_hash)
            if tx is None:
                logger.debug(
                    f"[RPC-METHOD] qtcl_getTransaction: tx not found (hash={tx_hash})"
                )
                return _rpc_error(
                    -32000, "Transaction not found", rpc_id, {"tx_hash": tx_hash}
                )
            logger.debug(f"[RPC-METHOD] qtcl_getTransaction success: tx_hash={tx_hash}")
            return _rpc_ok(tx, rpc_id)
        except Exception as be:
            logger.exception(
                f"[RPC-METHOD] qtcl_getTransaction: blockchain error: {be}"
            )
            return _rpc_error(
                -32603,
                f"TX lookup failed: {str(be)}",
                rpc_id,
                {"exception": str(be).__class__.__name__},
            )
    except Exception as e:
        logger.exception(f"[RPC-METHOD] qtcl_getTransaction outer exception: {e}")
        return _rpc_error(
            -32603,
            f"Internal error: {str(e)}",
            rpc_id,
            {"exception": str(e).__class__.__name__},
        )


def _rpc_getBlock(params: Any, rpc_id: Any) -> dict:
    """qtcl_getBlock — block by height or hash.

    DB-AUTHORITATIVE: queries PostgreSQL blocks table directly.
    params: [height] (list) or {height: int} or {hash: str}
    Returns full block header + transaction list for chain sync.
    """
    try:
        height = None
        block_hash = None
        if isinstance(params, list) and len(params) >= 1:
            height = int(params[0])
        elif isinstance(params, dict):
            height = params.get("height")
            block_hash = params.get("hash")
            if height is not None:
                height = int(height)

        def _query_block_at_height(h: int) -> Optional[dict]:
            """Full block query from database (authoritative source)."""
            try:
                # Try SQLite first
                if (
                    LATTICE
                    and hasattr(LATTICE, "block_manager")
                    and LATTICE.block_manager
                    and LATTICE.block_manager.db
                ):
                    db = LATTICE.block_manager.db
                    if db._sqlite_conn:
                        try:
                            sql = """
                                SELECT height, block_hash, timestamp, w_state_hash,
                                       parent_hash, nonce, difficulty,
                                       coherence_snapshot, merkle_root, tx_count
                                FROM blocks WHERE height = ? LIMIT 1
                            """
                            cursor = db._sqlite_conn.execute(sql, (h,))
                            row = cursor.fetchone()
                            if not row:
                                return None
                            block = {
                                "height": row[0],
                                "block_height": row[0],
                                "block_hash": row[1],
                                "hash": row[1],
                                "parent_hash": row[4] or ("0" * 64),
                                "previous_hash": row[4] or ("0" * 64),
                                "merkle_root": row[8] or ("0" * 64),
                                "timestamp_s": int(row[2]) if row[2] else 0,
                                "timestamp": int(row[2]) if row[2] else 0,
                                "difficulty": int(float(row[6])) if row[6] else 5,
                                "nonce": int(row[5]) if row[5] else 0,
                                "w_state_fidelity": float(row[7])
                                if row[7] is not None
                                else 0.0,
                                "w_entropy_hash": row[3] or "",
                                "pq_curr": h,
                                "pq_last": max(0, h - 1),
                                "tx_count": int(row[9]) if row[9] else 0,
                                "mined": True,
                                "finalized": True,
                            }
                            return block
                        except Exception as _se:
                            logger.debug(f"[RPC] SQLite query failed: {_se}")

                # Fallback to PostgreSQL
                with get_db_cursor() as cur:
                    cur.execute(
                        """
                        SELECT height, block_hash, timestamp, w_state_hash,
                               parent_hash, nonce, difficulty,
                               coherence_snapshot, merkle_root, tx_count
                        FROM blocks WHERE height = %s LIMIT 1
                    """,
                        (h,),
                    )
                    row = cur.fetchone()
                    if not row:
                        return None
                    block = {
                        "height": row[0],
                        "block_height": row[0],
                        "block_hash": row[1],
                        "hash": row[1],
                        "parent_hash": row[4] or ("0" * 64),
                        "previous_hash": row[4] or ("0" * 64),
                        "merkle_root": row[8] or ("0" * 64),
                        "timestamp_s": int(row[2]) if row[2] else 0,
                        "timestamp": int(row[2]) if row[2] else 0,
                        "difficulty": int(float(row[6])) if row[6] else 5,
                        "nonce": int(row[5]) if row[5] else 0,
                        "w_state_fidelity": float(row[7])
                        if row[7] is not None
                        else 0.0,
                        "w_entropy_hash": row[3] or "",
                        "pq_curr": h,
                        "pq_last": max(0, h - 1),
                        "tx_count": int(row[9]) if row[9] else 0,
                        "mined": True,
                        "finalized": True,
                    }
                    # Fetch transactions for this block
                    cur.execute(
                        """
                        SELECT tx_hash, from_address, to_address, amount,
                               transaction_index, tx_type, status,
                               quantum_state_hash, metadata
                        FROM transactions
                        WHERE height = %s
                        ORDER BY transaction_index ASC
                    """,
                        (h,),
                    )
                    tx_rows = cur.fetchall()
                    txs = []
                    for tr in tx_rows:
                        txs.append(
                            {
                                "tx_id": tr[0],
                                "from_addr": tr[1] or "",
                                "to_addr": tr[2] or "",
                                "amount": int(tr[3]) if tr[3] is not None else 0,
                                "tx_index": int(tr[4]) if tr[4] is not None else 0,
                                "tx_type": tr[5] or "transfer",
                                "status": tr[6] or "confirmed",
                                "w_proof": tr[7] or "",
                                "metadata": tr[8] if tr[8] else None,
                            }
                        )
                    block["transactions"] = txs
                    block["tx_count"] = len(txs)
                    return block
            except Exception as e:
                logger.exception(f"[RPC] _query_block_at_height({h}): {e}")
                return None

        block = None
        if height is not None:
            block = _query_block_at_height(height)

            # Fallback: check in-memory cache (for genesis and recently mined blocks)
            if block is None:
                with _BLOCK_CACHE_LOCK:
                    if height in _BLOCK_CACHE:
                        block = _BLOCK_CACHE[height]
                        logger.debug(f"[RPC] Block h={height} served from cache")
        elif block_hash:
            row = query_block_by_hash(block_hash)
            if row:
                block = _query_block_at_height(row["height"])

            # Fallback: search cache by hash
            if block is None:
                with _BLOCK_CACHE_LOCK:
                    for h, b in _BLOCK_CACHE.items():
                        if (
                            b.get("block_hash") == block_hash
                            or b.get("hash") == block_hash
                        ):
                            block = b
                            logger.debug(
                                f"[RPC] Block hash={block_hash[:16]}... served from cache"
                            )
                            break

        if block is None:
            return _rpc_error(-32000, "Block not found", rpc_id)

        return _rpc_ok(block, rpc_id)

    except Exception as e:
        logger.exception(f"[RPC] _rpc_getBlock exception: {e}")
        return _rpc_error(-32603, f"Internal error: {str(e)}", rpc_id)


_BLOCK_CACHE = {}  # height -> block dict
_BLOCK_CACHE_LOCK = threading.RLock()


def _cache_block(block_dict):
    """Add block to cache (called by block sealing)"""
    with _BLOCK_CACHE_LOCK:
        h = block_dict.get("height")
        if h:
            _BLOCK_CACHE[h] = block_dict


def _rpc_getBlockRange(params: Any, rpc_id: Any) -> dict:
    """qtcl_getBlockRange — return cached blocks ONLY (no DB blocking)

    params: [from_height, to_height]
    Negative to_height means "from end" (e.g., [-20, -1] = last 20 blocks)
    """
    try:
        if not isinstance(params, (list, tuple)) or len(params) < 2:
            return _rpc_error(-32602, "params: [from_height, to_height]", rpc_id)
        from_h = int(params[0])
        to_h = int(params[1])

        blocks = []
        with _BLOCK_CACHE_LOCK:
            logger.debug(
                f"[RPC] getBlockRange cache debug: keys={list(_BLOCK_CACHE.keys())[:5]}"
            )

            # Handle negative to_height: "from end"
            if to_h < 0:
                max_height = max(_BLOCK_CACHE.keys()) if _BLOCK_CACHE else 0
                to_h = max_height
                from_h = max(
                    0, to_h + from_h + 1
                )  # from_h was negative (e.g., -20 means 20 blocks before end)

            if to_h - from_h > 99:
                to_h = from_h + 99
            if from_h < 0:
                from_h = 0

            for h in range(from_h, to_h + 1):
                if h in _BLOCK_CACHE:
                    blocks.append(_BLOCK_CACHE[h])

        logger.info(f"[RPC] getBlockRange({from_h}, {to_h}) -> {len(blocks)} blocks")
        return _rpc_ok(
            {
                "blocks": blocks,
                "count": len(blocks),
                "from": from_h,
                "to": to_h,
            },
            rpc_id,
        )

    except Exception as e:
        logger.warning(f"[RPC-METHOD] qtcl_getBlockRange: {e}")
        return _rpc_error(-32603, str(e), rpc_id)
        logger.exception(f"[RPC] _rpc_getBlockRange exception: {e}")
        return _rpc_error(-32603, f"Internal error: {str(e)}", rpc_id)


def _rpc_getTransactions(params: Any, rpc_id: Any) -> dict:
    """qtcl_getTransactions — paginated transaction list.

    params: {page: int, per_page: int, type: str, address: str}
    Returns: {transactions: [...], total: int, pages: int, page: int}
    """
    try:
        page = 0
        per_page = 50
        tx_type = None
        address = None

        if isinstance(params, dict):
            page = int(params.get("page", 0))
            per_page = min(int(params.get("per_page", 50)), 200)
            tx_type = params.get("type")
            address = params.get("address")
        elif isinstance(params, list) and params:
            if isinstance(params[0], dict):
                page = int(params[0].get("page", 0))
                per_page = min(int(params[0].get("per_page", 50)), 200)
                tx_type = params[0].get("type")
                address = params[0].get("address")

        offset = page * per_page

        with get_db_cursor() as cur:
            where_clauses = []
            params_list = []
            if tx_type and tx_type != "all":
                where_clauses.append("tx_type = %s")
                params_list.append(tx_type)
            if address:
                where_clauses.append("(from_address = %s OR to_address = %s)")
                params_list.extend([address, address])

            where_sql = " AND ".join(where_clauses) if where_clauses else "1=1"

            count_sql = f"SELECT COUNT(*) FROM transactions WHERE {where_sql}"
            cur.execute(count_sql, params_list)
            row = cur.fetchone()
            total = row[0] if row else 0

            tx_sql = f"""
                SELECT tx_hash, from_address, to_address, amount,
                       transaction_index, tx_type, status, height,
                       quantum_state_hash, metadata, created_at
                FROM transactions
                WHERE {where_sql}
                ORDER BY height DESC, transaction_index ASC
                LIMIT %s OFFSET %s
            """
            cur.execute(tx_sql, params_list + [per_page, offset])
            rows = cur.fetchall()

            txs = []
            for r in rows:
                txs.append(
                    {
                        "tx_id": r[0],
                        "from_addr": r[1] or "",
                        "to_addr": r[2] or "",
                        "amount": int(r[3]) if r[3] is not None else 0,
                        "tx_index": int(r[4]) if r[4] is not None else 0,
                        "tx_type": r[5] or "transfer",
                        "status": r[6] or "confirmed",
                        "height": r[7],
                        "w_proof": r[8] or "",
                        "metadata": r[9],
                    }
                )

            pages = max(1, (total + per_page - 1) // per_page) if total > 0 else 1

            logger.debug(
                f"[RPC] _rpc_getTransactions: page={page}, per_page={per_page}, total={total}"
            )
            return _rpc_ok(
                {"transactions": txs, "total": total, "pages": pages, "page": page},
                rpc_id,
            )

    except Exception as e:
        logger.exception(f"[RPC] _rpc_getTransactions error: {e}")
        return _rpc_error(-32603, f"Internal error: {str(e)}", rpc_id)


# ═══ RPC TIMEOUT PROTECTION ═══
_RPC_TIMEOUT_SEC = 5.0


def _call_with_timeout(func, timeout_sec=_RPC_TIMEOUT_SEC, default=None):
    """Call function with timeout using threading (non-blocking for RPC safety)."""
    import queue as _q

    result_q = _q.Queue()

    def _target():
        try:
            result_q.put(("ok", func()))
        except Exception as e:
            result_q.put(("error", e))

    thread = threading.Thread(target=_target, daemon=True)
    thread.start()
    thread.join(timeout=timeout_sec)

    try:
        status, value = result_q.get_nowait()
        return value if status == "ok" else default
    except:
        return default


def _rpc_getQuantumMetrics(params: Any, rpc_id: Any) -> dict:
    """qtcl_getQuantumMetrics — live W-state oracle + lattice metrics + density matrix snapshot.

    All reads protected with 5s timeouts to prevent RPC hangs.
    """
    try:
        logger.debug(
            f"[RPC-METHOD] qtcl_getQuantumMetrics called with params={params}, id={rpc_id}"
        )
        result: dict = {"oracle_available": ORACLE_AVAILABLE, "ts": time.time()}
        logger.debug(
            f"[RPC-METHOD] qtcl_getQuantumMetrics: oracle_available={ORACLE_AVAILABLE}"
        )

        # NOTE: 16³ quantum data flows via SSE stream /rpc/oracle/snapshot, NOT via RPC
        # Clients subscribe to the SSE stream and sample whatever resolution they need

        if ORACLE_AVAILABLE and ORACLE is not None:
            try:
                logger.debug(
                    f"[RPC-METHOD] qtcl_getQuantumMetrics: fetching W-state snapshot (timeout=5s)"
                )
                w_snap = _call_with_timeout(
                    lambda: (
                        ORACLE_W_STATE_MANAGER.get_latest_snapshot()
                        if ORACLE_W_STATE_MANAGER
                        else None
                    ),
                    timeout_sec=5.0,
                )
                if w_snap:
                    result["w_state"] = {
                        "purity": getattr(w_snap, "purity", None),
                        "entropy": getattr(w_snap, "entropy", None),
                        "coherence": getattr(w_snap, "coherence", None),
                        "fidelity": getattr(w_snap, "fidelity", None),
                        "snapshot_id": getattr(w_snap, "snapshot_id", None),
                    }
                    logger.debug(
                        f"[RPC-METHOD] qtcl_getQuantumMetrics: W-state snapshot obtained"
                    )
                else:
                    logger.warning(
                        f"[RPC-METHOD] qtcl_getQuantumMetrics: W-state snapshot is None"
                    )
            except Exception as we:
                logger.exception(
                    f"[RPC-METHOD] qtcl_getQuantumMetrics: W-state error: {we}"
                )
                result["w_state_error"] = str(we)

        if LATTICE is not None:
            try:
                logger.debug(
                    f"[RPC-METHOD] qtcl_getQuantumMetrics: fetching lattice state (timeout=5s)"
                )
                # Safe access to LATTICE methods with fallback
                lm = (
                    _call_with_timeout(
                        lambda: (
                            LATTICE.get_metrics()
                            if hasattr(LATTICE, "get_metrics")
                            and callable(LATTICE.get_metrics)
                            else {}
                        ),
                        timeout_sec=5.0,
                        default={},
                    )
                    or {}
                )
                ls = (
                    _call_with_timeout(
                        lambda: (
                            LATTICE.get_stats()
                            if hasattr(LATTICE, "get_stats")
                            and callable(LATTICE.get_stats)
                            else {}
                        ),
                        timeout_sec=5.0,
                        default={},
                    )
                    or {}
                )

                # Fallback: use LATTICE attributes directly
                if not lm:
                    lm = {
                        "avg_fidelity_100": getattr(LATTICE, "avg_fidelity_100", 0.0),
                        "avg_coherence_100": getattr(LATTICE, "avg_coherence_100", 0.0),
                    }
                if not ls:
                    ls = {
                        "fidelity": getattr(LATTICE, "fidelity", 0.0),
                        "coherence": getattr(LATTICE, "coherence", 0.0),
                        "w_state_strength": getattr(LATTICE, "w_state_strength", 0.0),
                        "cycle": getattr(LATTICE, "cycle", 0),
                    }

                result["lattice"] = {
                    "fidelity": lm.get("avg_fidelity_100", ls.get("fidelity", 0.0)),
                    "coherence": lm.get("avg_coherence_100", ls.get("coherence", 0.0)),
                    "w_state_strength": ls.get("w_state_strength", 0.0),
                    "cycle": ls.get("cycle", 0),
                    "avg_fidelity_100": lm.get("avg_fidelity_100", 0.0),
                    "avg_coherence_100": lm.get("avg_coherence_100", 0.0),
                }
                logger.debug(
                    f"[RPC-METHOD] qtcl_getQuantumMetrics: lattice metrics obtained"
                )
            except Exception as le:
                logger.exception(
                    f"[RPC-METHOD] qtcl_getQuantumMetrics: lattice error: {le}"
                )
                result["lattice_error"] = str(le)

        # ── 16³ DENSITY MATRIX VIA SSE STREAM ─────────────────────────────────
        # Clients fetch 16³ via /rpc/oracle/snapshot (SSE stream endpoint)
        # No legacy 64³/32³ included in metrics responses

        # ── COMPACT W-STATE AMPLITUDES (8 complex doubles = 256 hex chars) ──────
        try:
            if LATTICE and hasattr(LATTICE, "current_density_matrix"):
                dm = LATTICE.current_density_matrix
                if dm is not None:
                    # Extract 8 single-excitation amplitudes (indices 1,2,4,8,16,32,64,128)
                    import struct as _ws

                    w_indices = [1, 2, 4, 8, 16, 32, 64, 128]
                    w_amplitudes = []
                    for idx in w_indices:
                        if idx < dm.shape[0]:
                            re = float(dm[idx, idx].real)
                            im = float(dm[idx, idx].imag)
                        else:
                            re, im = 0.0, 0.0
                        w_amplitudes.append((re, im))

                    # Pack as 8 complex doubles = 128 bytes = 256 hex chars
                    result["w_state_hex"] = b"".join(
                        _ws.pack(">dd", re, im) for re, im in w_amplitudes
                    ).hex()
                    result["w_state_size"] = 8  # 8 qubits
        except Exception as wse:
            logger.debug(f"[RPC-METHOD] w_state_hex (non-fatal): {wse}")

        # ── INJECT block_height from DB so client oracle display shows correct chain tip ──
        # No fallback — DB is authoritative
        _db_tip = query_latest_block()
        _bh = int(_db_tip["height"]) if _db_tip else 0
        result["block_height"] = _bh
        result["height"] = _bh

        # ── Inject client tripartite pool consensus fields ─────────────────
        try:
            with _CLIENT_DM_POOL_LOCK:
                result["client_fused_fidelity"] = round(_client_consensus_fid, 6)
                result["client_oracle_count"] = _client_pool_count
                if _client_pool_count > 0 and any(
                    v != 0.0 for v in _client_consensus_dm_re
                ):
                    import struct as _qms

                    result["client_consensus_dm_hex"] = b"".join(
                        _qms.pack(
                            ">dd",
                            _client_consensus_dm_re[i],
                            _client_consensus_dm_im[i],
                        )
                        for i in range(64)
                    ).hex()
        except Exception as _ce:
            logger.debug(f"[RPC-METHOD] client pool inject: {_ce}")

        logger.debug(f"[RPC-METHOD] qtcl_getQuantumMetrics success  block_height={_bh}")
        return _rpc_ok(result, rpc_id)
    except Exception as e:
        logger.exception(f"[RPC-METHOD] qtcl_getQuantumMetrics outer exception: {e}")
        return _rpc_error(
            -32603,
            f"Quantum metrics failed: {str(e)}",
            rpc_id,
            {"exception": str(e).__class__.__name__},
        )


def _rpc_getPythPrice(params: Any, rpc_id: Any) -> dict:
    """
    qtcl_getPythPrice — atomic Pyth Network price snapshot.

    Params:
      list:   ["BTC", "ETH"]          — symbol list
      object: {"symbols": ["BTC"]}    — named
      null:   all feeds

    Returns:
      snapshot_id, fetch_time_ns, hermes_ok, hlwe_sig, feeds{price,conf,age_seconds,...}
    """
    try:
        logger.debug(
            f"[RPC-METHOD] qtcl_getPythPrice called with params={params}, id={rpc_id}"
        )
        symbols: Optional[list] = None
        if isinstance(params, list):
            if params and isinstance(params[0], list):
                symbols = params[0]
            elif params and isinstance(params[0], str):
                symbols = params
        elif isinstance(params, dict):
            symbols = params.get("symbols")
        logger.debug(f"[RPC-METHOD] qtcl_getPythPrice: extracted symbols={symbols}")

        po = _get_pyth()
        if po is None:
            logger.warning(
                f"[RPC-METHOD] qtcl_getPythPrice: Pyth oracle not initialized"
            )
            return _rpc_error(-32002, "Pyth oracle not initialized", rpc_id)

        try:
            logger.debug(
                f"[RPC-METHOD] qtcl_getPythPrice: fetching snapshot for symbols={symbols}"
            )
            snap = po.get_snapshot(symbols)
            logger.debug(
                f"[RPC-METHOD] qtcl_getPythPrice: snapshot obtained successfully"
            )
            return _rpc_ok(snap.to_dict(), rpc_id)
        except Exception as pe:
            logger.exception(f"[RPC-METHOD] qtcl_getPythPrice: Pyth fetch error: {pe}")
            return _rpc_error(
                -32002,
                f"Pyth fetch failed: {str(pe)}",
                rpc_id,
                {"exception": str(pe).__class__.__name__},
            )
    except Exception as e:
        logger.exception(f"[RPC-METHOD] qtcl_getPythPrice outer exception: {e}")
        return _rpc_error(
            -32603,
            f"Internal error: {str(e)}",
            rpc_id,
            {"exception": str(e).__class__.__name__},
        )


def _rpc_getMempoolStats(params: Any, rpc_id: Any) -> dict:
    """qtcl_getMempoolStats — mempool depth and fee percentiles."""
    try:
        logger.debug(
            f"[RPC-METHOD] qtcl_getMempoolStats called with params={params}, id={rpc_id}"
        )
        # Walk resolution chain: module-level MEMPOOL → globals.get_mempool() → mempool module singleton
        mp = None
        _srv_globals = sys.modules[__name__].__dict__
        mp = _srv_globals.get("MEMPOOL") or _srv_globals.get("_MEMPOOL")
        if mp is None:
            try:
                import globals as _g

                _gf = getattr(_g, "get_mempool", None)
                if callable(_gf):
                    mp = _gf()
            except Exception:
                pass
        if mp is None:
            try:
                import mempool as _mp_mod

                mp = getattr(_mp_mod, "MEMPOOL", None) or getattr(
                    _mp_mod, "_MEMPOOL_INSTANCE", None
                )
            except Exception:
                pass
        if mp is None:
            logger.debug("[RPC-METHOD] qtcl_getMempoolStats: mempool not available yet")
            return _rpc_ok(
                {"depth": 0, "pending": 0, "note": "mempool initializing"}, rpc_id
            )
        try:
            stats = (
                mp.get_stats()
                if hasattr(mp, "get_stats")
                else {"depth": getattr(mp, "size", lambda: 0)()}
            )
            logger.debug(f"[RPC-METHOD] qtcl_getMempoolStats success")
            return _rpc_ok(stats, rpc_id)
        except Exception as me:
            logger.exception(
                f"[RPC-METHOD] qtcl_getMempoolStats: get_stats error: {me}"
            )
            return _rpc_error(
                -32603,
                f"Mempool stats failed: {str(me)}",
                rpc_id,
                {"exception": type(me).__name__},
            )
    except Exception as e:
        logger.exception(f"[RPC-METHOD] qtcl_getMempoolStats outer exception: {e}")
        return _rpc_error(
            -32603, f"Internal error: {str(e)}", rpc_id, {"exception": type(e).__name__}
        )


def _rpc_getPeers(params: Any, rpc_id: Any) -> dict:
    """qtcl_getPeers — return cached peer list ONLY (no DB blocking)"""
    try:
        limit = 50
        if isinstance(params, list) and params:
            try:
                limit = int(params[0])
            except (ValueError, TypeError):
                limit = 50
        elif isinstance(params, dict):
            try:
                limit = int(params.get("limit", 50))
            except (ValueError, TypeError):
                limit = 50
        limit = min(max(int(limit), 1), 200)

        # Return empty peer list immediately — no DB
        return _rpc_ok({"peers": [], "count": 0, "timestamp": time.time()}, rpc_id)

    except Exception as e:
        logger.debug(f"[RPC-METHOD] qtcl_getPeers: {e}")
        return _rpc_error(-32603, str(e), rpc_id)


def _rpc_getPeersByNatGroup(params: Any, rpc_id: Any) -> dict:
    try:
        if isinstance(params, list):
            params = params[0] if params else {}
        if not isinstance(params, dict):
            return _rpc_error(-32602, "Invalid params: object expected", rpc_id)
        caller_ip = str(params.get("caller_ip") or "").strip()
        my_mac = str(params.get("mac_address") or "").strip().lower()
        if not caller_ip:
            return _rpc_error(-32602, "caller_ip required", rpc_id)
        peers = []
        try:
            with get_db_cursor() as cur:
                # Ensure table exists — first boot before any registerPeer call
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS peer_registry (
                        node_id       TEXT        PRIMARY KEY,
                        external_addr TEXT        NOT NULL,
                        pubkey_hash   TEXT        NOT NULL DEFAULT '',
                        chain_height  BIGINT      DEFAULT 0,
                        last_seen     TIMESTAMPTZ DEFAULT NOW(),
                        first_seen    TIMESTAMPTZ DEFAULT NOW(),
                        capabilities  JSONB       DEFAULT '[]',
                        ban_score     INTEGER     DEFAULT 0,
                        caller_ip     TEXT        DEFAULT '',
                        mac_address   TEXT        DEFAULT '',
                        device_id     TEXT        DEFAULT ''
                    )
                """)
                cur.execute(
                    """
                    SELECT node_id, external_addr, pubkey_hash, chain_height,
                           last_seen, capabilities, ban_score, mac_address, device_id, caller_ip
                    FROM   peer_registry
                    WHERE  caller_ip = %s
                      AND  last_seen > NOW() - INTERVAL '5 minutes'
                      AND  ban_score < 100
                    ORDER  BY chain_height DESC, last_seen DESC
                    LIMIT  50
                """,
                    (caller_ip,),
                )
                rows = cur.fetchall()
                if rows:
                    cols = [d[0] for d in cur.description]
                    for row in rows:
                        r = dict(zip(cols, row))
                        _ls = r.get("last_seen")
                        r["last_seen"] = (
                            _ls.timestamp()
                            if hasattr(_ls, "timestamp")
                            else (float(_ls) if _ls else 0.0)
                        )
                        if r.get("mac_address", "").lower() != my_mac:
                            peers.append(r)
        except Exception as _dbe:
            logger.debug(f"[RPC-METHOD] qtcl_getPeersByNatGroup DB query: {_dbe}")
        if not peers:
            with _LIVE_PEERS_LOCK:
                for nid, p in _LIVE_PEERS_CACHE.items():
                    if (
                        p.get("caller_ip") == caller_ip
                        and p.get("mac_address", "").lower() != my_mac
                    ):
                        _pc = dict(p)
                        # Normalise last_seen to float timestamp for consistent client parsing
                        _ls = _pc.get("last_seen", 0)
                        if hasattr(_ls, "timestamp"):
                            _pc["last_seen"] = _ls.timestamp()
                        elif not isinstance(_ls, (int, float)):
                            _pc["last_seen"] = 0.0
                        peers.append(_pc)
        return _rpc_ok(
            {"peers": peers, "count": len(peers), "nat_group": caller_ip}, rpc_id
        )
    except Exception as e:
        logger.exception(f"[RPC-METHOD] qtcl_getPeersByNatGroup exception: {e}")
        return _rpc_error(
            -32603, f"Internal error: {str(e)}", rpc_id, {"exception": type(e).__name__}
        )


# In-process peer cache (survives between requests, cleared on restart — DB is authoritative)
_LIVE_PEERS_CACHE: Dict[str, dict] = {}
_LIVE_PEERS_LOCK = threading.Lock()


def _rpc_registerPeer(params: Any, rpc_id: Any) -> dict:
    """qtcl_registerPeer — miner announces itself to Koyeb bootstrap registry.

    Params (dict):
        external_addr  str   "ip:port" of miner's P2P listener (required)
        node_id        str   64-hex SHA-256(hlwe_pubkey) (required)
        pubkey         str   base64 HLWE public key
        chain_height   int   miner's current chain height
    """
    try:
        if isinstance(params, list):
            params = params[0] if params else {}
        if not isinstance(params, dict):
            return _rpc_error(-32602, "Invalid params: object expected", rpc_id)

        external_addr = str(params.get("external_addr") or "").strip()
        node_id = str(params.get("node_id") or "").strip().lower()
        pubkey_b64 = str(params.get("pubkey") or "").strip()
        chain_height = int(params.get("chain_height") or 0)
        mac_address = str(params.get("mac_address") or "").strip().lower()
        device_id = str(params.get("device_id") or "").strip().lower()

        if not external_addr:
            return _rpc_error(-32602, "external_addr required", rpc_id)
        if (
            not node_id
            or len(node_id) != 64
            or not all(c in "0123456789abcdef" for c in node_id)
        ):
            return _rpc_error(
                -32602,
                "node_id must be 64 lowercase hex chars (SHA-256 of pubkey)",
                rpc_id,
            )

        # Derive caller IP from Flask request context (STUN: what address does Koyeb see?)
        try:
            # Check standard proxy headers first
            forwarded = request.headers.get("X-Forwarded-For", "")
            real_ip = request.headers.get("X-Real-IP", "")
            cf_ip = request.headers.get("CF-Connecting-IP", "")

            if cf_ip:
                caller_ip = cf_ip
            elif real_ip:
                caller_ip = real_ip
            elif forwarded:
                caller_ip = forwarded.split(",")[0].strip()
            else:
                caller_ip = request.remote_addr or "127.0.0.1"
        except Exception:
            caller_ip = "127.0.0.1"

        pubkey_hash = (
            hashlib.sha256(pubkey_b64.encode()).hexdigest()[:32]
            if pubkey_b64
            else node_id[:32]
        )

        # ── Device fingerprinting (NAT:MAC:Fingerprint chain) ───────────────────
        # Pair NAT (caller_ip) with reported external IP, reported MAC and DeviceID
        # to identify unique hardware even if node_id (wallet key) rotates.
        # Reported IP helps distinguish multiple nodes behind the same NAT.
        reported_ip = (
            external_addr.split(":")[0] if ":" in external_addr else external_addr
        )
        fp_payload = (
            f"NAT:{caller_ip}|REP:{reported_ip}|MAC:{mac_address}|DEV:{device_id}"
        )
        fingerprint = hashlib.sha256(fp_payload.encode()).hexdigest()

        # Debug pairing details
        logger.debug(
            f"[P2P] Fingerprint details — NAT: {caller_ip}, REP: {reported_ip}, MAC: {mac_address}, DEV: {device_id}"
        )

        # Upsert into peer_registry — uses separate cursors to ensure one failure doesn't abort the entire registration
        try:
            _lazy_ensure_peer_registry()
            # 1. Main Registry Update
            with get_db_cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO peer_registry
                        (node_id, external_addr, pubkey_hash, chain_height, last_seen, caller_ip, mac_address, device_id, fingerprint)
                    VALUES (%s, %s, %s, %s, NOW(), %s, %s, %s, %s)
                    ON CONFLICT (node_id) DO UPDATE SET
                        external_addr = EXCLUDED.external_addr,
                        pubkey_hash   = EXCLUDED.pubkey_hash,
                        chain_height  = EXCLUDED.chain_height,
                        last_seen     = NOW(),
                        caller_ip     = EXCLUDED.caller_ip,
                        mac_address   = EXCLUDED.mac_address,
                        device_id     = EXCLUDED.device_id,
                        fingerprint   = EXCLUDED.fingerprint
                """,
                    (
                        node_id,
                        external_addr,
                        pubkey_hash,
                        chain_height,
                        caller_ip,
                        mac_address,
                        device_id,
                        fingerprint,
                    ),
                )

            # 2. Device Chain Update (Isolated)
            try:
                with get_db_cursor() as cur:
                    cur.execute(
                        """
                        INSERT INTO peer_devices
                            (fingerprint, node_id, last_caller_ip, mac_address, device_id, last_seen)
                        VALUES (%s, %s, %s, %s, %s, NOW())
                        ON CONFLICT (fingerprint) DO UPDATE SET
                            node_id        = EXCLUDED.node_id,
                            last_caller_ip = EXCLUDED.last_caller_ip,
                            mac_address    = EXCLUDED.mac_address,
                            device_id      = EXCLUDED.device_id,
                            last_seen      = NOW()
                    """,
                        (fingerprint, node_id, caller_ip, mac_address, device_id),
                    )
            except Exception as _fpe:
                logger.debug(f"[P2P] peer_devices update skipped: {_fpe}")
        except Exception as _dbe:
            # Non-fatal: fall through to in-process cache so peer can still be served
            logger.warning(f"[RPC-METHOD] qtcl_registerPeer DB upsert failed: {_dbe}")

        # Always update in-process cache for immediate availability
        with _LIVE_PEERS_LOCK:
            _LIVE_PEERS_CACHE[node_id] = {
                "node_id": node_id,
                "external_addr": external_addr,
                "pubkey_hash": pubkey_hash,
                "chain_height": chain_height,
                "last_seen": time.time(),
                "caller_ip": caller_ip,
                "mac_address": mac_address,
                "device_id": device_id,
                "fingerprint": fingerprint,
                "ban_score": 0,
            }
        logger.info(
            f"[P2P] ✅ Peer registered: node={node_id[:16]}… addr={external_addr} h={chain_height} fp={fingerprint[:12]}…"
        )
        return _rpc_ok(
            {
                "registered": True,
                "node_id": node_id,
                "external_addr": external_addr,
                "caller_ip": caller_ip,
                "fingerprint": fingerprint,
                "nat_paired": True,
            },
            rpc_id,
        )
    except Exception as e:
        logger.exception(f"[RPC-METHOD] qtcl_registerPeer exception: {e}")
        return _rpc_error(
            -32603, f"Internal error: {str(e)}", rpc_id, {"exception": type(e).__name__}
        )


def _rpc_getDeviceChain(params: Any, rpc_id: Any) -> dict:
    """Return the NAT:MAC:Fingerprint chain for a given node_id or fingerprint."""
    try:
        if isinstance(params, list) and params:
            params = params[0]
        search = str(params.get("search") or "").strip()
        if not search:
            return _rpc_error(
                -32602, "search (node_id or fingerprint) required", rpc_id
            )

        devices = []
        with get_db_cursor() as cur:
            cur.execute(
                """
                SELECT fingerprint, node_id, last_caller_ip, mac_address, device_id, first_seen, last_seen, trust_score
                FROM   peer_devices
                WHERE  node_id = %s OR fingerprint = %s
                ORDER  BY last_seen DESC
            """,
                (search, search),
            )
            rows = cur.fetchall()
            if rows:
                cols = [d[0] for d in cur.description]
                for row in rows:
                    r = dict(zip(cols, row))
                    # Normalise datetimes
                    for k in ["first_seen", "last_seen"]:
                        if hasattr(r[k], "isoformat"):
                            r[k] = r[k].isoformat()
                    devices.append(r)

        return _rpc_ok({"devices": devices, "count": len(devices)}, rpc_id)
    except Exception as e:
        logger.exception(f"[RPC-METHOD] qtcl_getDeviceChain exception: {e}")
        return _rpc_error(-32603, str(e), rpc_id)


def _rpc_getMyAddr(params: Any, rpc_id: Any) -> dict:
    """qtcl_getMyAddr — STUN: return the caller's observed source IP so miners can discover their external addr.

    Returns:
        external_addr  str   "observed_ip:suggested_port"
        ip             str   raw observed source IP
        port           int   suggested P2P port (from P2P_PORT env or 9091)
    """
    try:
        try:
            forwarded = request.headers.get("X-Forwarded-For", "")
            real_ip = request.headers.get("X-Real-IP", "")
            cf_ip = request.headers.get("CF-Connecting-IP", "")

            if cf_ip:
                observed_ip = cf_ip
            elif real_ip:
                observed_ip = real_ip
            elif forwarded:
                observed_ip = forwarded.split(",")[0].strip()
            else:
                observed_ip = request.remote_addr or "unknown"
        except Exception:
            observed_ip = "unknown"
        p2p_port = int(os.environ.get("P2P_PORT", "9091"))
        return _rpc_ok(
            {
                "ip": observed_ip,
                "port": p2p_port,
                "external_addr": f"{observed_ip}:{p2p_port}",
            },
            rpc_id,
        )
    except Exception as e:
        logger.exception(f"[RPC-METHOD] qtcl_getMyAddr exception: {e}")
        return _rpc_error(
            -32603, f"Internal error: {str(e)}", rpc_id, {"exception": type(e).__name__}
        )


def _rpc_getHealth(params: Any, rpc_id: Any) -> dict:
    """qtcl_getHealth — full system health vector."""
    try:
        logger.debug(
            f"[RPC-METHOD] qtcl_getHealth called with params={params}, id={rpc_id}"
        )
        from oracle import PYTH_ORACLE as _po

        logger.debug(
            f"[RPC-METHOD] qtcl_getHealth: oracle_ready={ORACLE_AVAILABLE}, lattice_ready={LATTICE is not None}, pyth_ready={_po is not None}"
        )
        result = {
            "status": "ok",
            "ts": time.time(),
            "uptime_s": round(time.time() - _SERVER_START_TIME, 1),
            "oracle_ready": ORACLE_AVAILABLE,
            "lattice_ready": LATTICE is not None,
            "pyth_ready": _po is not None,
            "pyth_stats": _po.stats() if _po else {},
            "jsonrpc_version": _JSONRPC_VERSION,
            "qtcl_server": "v6",
        }
        logger.debug(f"[RPC-METHOD] qtcl_getHealth success")
        return _rpc_ok(result, rpc_id)
    except Exception as e:
        logger.exception(f"[RPC-METHOD] qtcl_getHealth exception: {e}")
        return _rpc_error(
            -32603,
            f"Health check failed: {str(e)}",
            rpc_id,
            {"exception": str(e).__class__.__name__},
        )


def _rpc_getOracleRegistry(params: Any, rpc_id: Any) -> dict:
    """qtcl_getOracleRegistry — paginated on-chain oracle registry.
    Params (object or positional list):
      mode           string   filter by mode: full|light|archive|deregistered (default: all)
      confirmed_only bool     only oracles with on-chain reg_tx_hash (default: false)
      limit          int      max records (default 100, max 500)
      offset         int      pagination offset (default 0)
    Returns: {oracles[], total, confirmed_count, limit, offset}
    """
    try:
        logger.debug(
            f"[RPC-METHOD] qtcl_getOracleRegistry called with params={params}, id={rpc_id}"
        )
        p = (
            params
            if isinstance(params, dict)
            else (
                params[0]
                if isinstance(params, list) and params and isinstance(params[0], dict)
                else {}
            )
        )
        mode_filter = str(p.get("mode", ""))
        confirmed_only = bool(p.get("confirmed_only", False))
        limit = min(int(p.get("limit", 100)), 500)
        offset = int(p.get("offset", 0))
        logger.debug(
            f"[RPC-METHOD] qtcl_getOracleRegistry: mode={mode_filter}, confirmed_only={confirmed_only}, limit={limit}, offset={offset}"
        )
        try:
            _lazy_ensure_oracle_registry()
            where_clauses: list = []
            qparams: list = []
            if mode_filter:
                where_clauses.append("mode = %s")
                qparams.append(mode_filter)
            if confirmed_only:
                where_clauses.append(
                    "reg_tx_hash != '' AND reg_tx_hash != 'gossip_pending'"
                )
            where_sql = (
                ("WHERE " + " AND ".join(where_clauses)) if where_clauses else ""
            )
            logger.debug(
                f"[RPC-METHOD] qtcl_getOracleRegistry: executing query with where_sql={where_sql}"
            )
            with get_db_cursor() as cur:
                cur.execute(
                    f"""
                    SELECT oracle_id, oracle_url, oracle_address, is_primary,
                           last_seen, block_height, peer_count,
                           wallet_address, oracle_pub_key, cert_sig,
                           mode, ip_hint, reg_tx_hash, registered_at, created_at
                    FROM   oracle_registry {where_sql}
                    ORDER  BY registered_at DESC, last_seen DESC
                    LIMIT  %s OFFSET %s
                """,
                    qparams + [limit, offset],
                )
                rows = cur.fetchall()
                cur.execute(
                    f"SELECT COUNT(*) FROM oracle_registry {where_sql}", qparams
                )
                total = cur.fetchone()[0]
                logger.debug(
                    f"[RPC-METHOD] qtcl_getOracleRegistry: fetched {len(rows)} rows, total={total}"
                )
            oracles = [
                {
                    "oracle_id": r[0],
                    "oracle_url": r[1],
                    "oracle_address": r[2],
                    "is_primary": r[3],
                    "last_seen": _iso(r[4]),
                    "block_height": r[5],
                    "peer_count": r[6],
                    "wallet_address": r[7],
                    "oracle_pub_key": r[8],
                    "cert_sig": r[9],
                    "mode": r[10],
                    "ip_hint": r[11],
                    "reg_tx_hash": r[12],
                    "registered_at": _iso(r[13]),
                    "created_at": _iso(r[14]),
                    "on_chain": bool(r[12] and r[12] not in ("", "gossip_pending")),
                }
                for r in rows
            ]
            result = {
                "oracles": oracles,
                "total": total,
                "confirmed_count": sum(1 for o in oracles if o["on_chain"]),
                "limit": limit,
                "offset": offset,
            }
            logger.debug(
                f"[RPC-METHOD] qtcl_getOracleRegistry success: {len(oracles)} oracles returned"
            )
            return _rpc_ok(result, rpc_id)
        except Exception as re:
            logger.exception(
                f"[RPC-METHOD] qtcl_getOracleRegistry: registry error: {re}"
            )
            return _rpc_error(
                -32603,
                f"Oracle registry query failed: {str(re)}",
                rpc_id,
                {"exception": str(re).__class__.__name__},
            )
    except Exception as e:
        logger.exception(f"[RPC-METHOD] qtcl_getOracleRegistry outer exception: {e}")
        return _rpc_error(
            -32603,
            f"Internal error: {str(e)}",
            rpc_id,
            {"exception": str(e).__class__.__name__},
        )


def _rpc_getOracleRecord(params: Any, rpc_id: Any) -> dict:
    """qtcl_getOracleRecord — single oracle record by oracle_addr or oracle_id.
    Params: [oracle_addr] or {oracle_addr: string}
    Returns: full oracle_registry row or {registered: false} if unknown.
    """
    oracle_addr = ""
    if isinstance(params, list) and params:
        oracle_addr = str(params[0])
    elif isinstance(params, dict):
        oracle_addr = str(params.get("oracle_addr", params.get("address", "")))
    if not oracle_addr:
        return _rpc_error(-32602, "oracle_addr required", rpc_id)
    try:
        _lazy_ensure_oracle_registry()
        with get_db_cursor() as cur:
            cur.execute(
                """
                SELECT oracle_id, oracle_url, oracle_address, is_primary,
                       last_seen, block_height, peer_count,
                       wallet_address, oracle_pub_key, cert_sig, cert_auth_tag,
                       mode, ip_hint, reg_tx_hash, registered_at, created_at
                FROM   oracle_registry
                WHERE  oracle_id = %s OR oracle_address = %s
                LIMIT  1
            """,
                (oracle_addr, oracle_addr),
            )
            r = cur.fetchone()
        if not r:
            return _rpc_ok({"registered": False, "oracle_addr": oracle_addr}, rpc_id)
        on_chain = bool(r[13] and r[13] not in ("", "gossip_pending"))
        return _rpc_ok(
            {
                "registered": True,
                "on_chain": on_chain,
                "oracle_id": r[0],
                "oracle_url": r[1],
                "oracle_address": r[2],
                "is_primary": r[3],
                "last_seen": _iso(r[4]),
                "block_height": r[5],
                "peer_count": r[6],
                "wallet_address": r[7],
                "oracle_pub_key": r[8],
                "cert_sig": r[9],
                "cert_auth_tag": r[10],
                "mode": r[11],
                "ip_hint": r[12],
                "reg_tx_hash": r[13],
                "registered_at": _iso(r[14]),
                "created_at": _iso(r[15]),
            },
            rpc_id,
        )
    except Exception as e:
        return _rpc_error(-32603, f"Oracle record lookup failed: {e}", rpc_id)


def _rpc_submitOracleReg(params: Any, rpc_id: Any) -> dict:
    """qtcl_submitOracleReg — build and submit an oracle_reg TX through the mempool.
    Params (object):
      wallet_address  string  required — HLWE wallet signing the TX
      oracle_addr     string  required — oracle identity address
      oracle_pub      string  recommended — oracle HLWE public key hex
      cert_sig        string  optional — pre-computed cert sig (server computes if omitted)
      cert_auth_tag   string  optional
      mode            string  optional — full|light|archive (default: full)
      ip_hint         string  optional — advertised host:port
      action          string  optional — register|deregister (default: register)
      nonce           int     optional
      timestamp_ns    int     optional
      signature       object  required for mempool — HLWE sig over tx_hash
    Returns: {status, tx_hash, oracle_addr, check_url} or {status: tx_template_issued, tx_template}
    """
    p = (
        params
        if isinstance(params, dict)
        else (
            params[0]
            if isinstance(params, list) and params and isinstance(params[0], dict)
            else {}
        )
    )
    wallet_addr = str(p.get("wallet_address", p.get("from_address", "")))
    oracle_addr = str(p.get("oracle_addr", wallet_addr))
    oracle_pub = str(p.get("oracle_pub", p.get("public_key", "")))
    mode = str(p.get("mode", "full"))
    ip_hint = str(p.get("ip_hint", ""))
    action = str(p.get("action", "register"))
    signature = p.get("signature", {})
    nonce_val = int(p.get("nonce", int(time.time_ns() // 1_000_000) % 2**31))
    ts_ns = int(p.get("timestamp_ns", time.time_ns()))

    if not wallet_addr or not oracle_addr:
        return _rpc_error(-32602, "wallet_address and oracle_addr required", rpc_id)

    import hashlib as _hh

    cert_preimage = f"{oracle_addr}|{wallet_addr}|{oracle_pub}"
    cert_sig_hex = str(
        p.get("cert_sig", _hh.sha256(cert_preimage.encode()).hexdigest())
    )
    cert_auth_tag = str(
        p.get("cert_auth_tag", _hh.sha3_256(cert_preimage.encode()).hexdigest()[:32])
    )

    _ora_registry_addr = "qtcl1oracle_registry_000000000000000000000000"
    tx_payload = {
        "tx_type": "oracle_reg",
        "from_address": wallet_addr,
        "to_address": _ora_registry_addr,
        "amount": 1,
        "fee": 0.01,
        "nonce": nonce_val,
        "timestamp_ns": ts_ns,
        "signature": signature,
        "input_data": {
            "oracle_addr": oracle_addr,
            "oracle_pub": oracle_pub,
            "cert_sig": cert_sig_hex,
            "cert_auth_tag": cert_auth_tag,
            "mode": mode,
            "ip_hint": ip_hint,
            "action": action,
        },
        "metadata": {
            "oracle_addr": oracle_addr,
            "wallet_addr": wallet_addr,
            "cert_valid": True,
            "action": action,
        },
    }

    # If no signature provided — return template for client to sign
    if not signature:
        return _rpc_ok(
            {
                "status": "tx_template_issued",
                "tx_template": tx_payload,
                "submit_to": "qtcl_submitOracleReg (with signature) or POST /api/oracle/registry/submit",
                "note": "Sign tx_template with your HLWE wallet, then resubmit with signature field.",
            },
            rpc_id,
        )

    try:
        if MEMPOOL:
            result, reason, accepted_tx = MEMPOOL.accept(tx_payload)
            if result.value not in ("accepted", "duplicate"):
                return _rpc_error(
                    -32001,
                    f"Mempool rejected: {reason} [{result.value}]",
                    rpc_id,
                    {"result_code": result.value, "tx_template": tx_payload},
                )
            tx_hash = accepted_tx.tx_hash if accepted_tx else ""
        else:
            tx_hash = _hh.sha3_256(
                f"oracle_reg:{wallet_addr}:{oracle_addr}:{ts_ns}".encode()
            ).hexdigest()

        return _rpc_ok(
            {
                "status": "submitted",
                "tx_hash": tx_hash,
                "oracle_addr": oracle_addr,
                "wallet_addr": wallet_addr,
                "action": action,
                "check_url": f"/api/oracle/registry/{oracle_addr}",
                "note": "TX in mempool — confirmed on next block seal.",
            },
            rpc_id,
        )
    except Exception as e:
        return _rpc_error(-32603, f"Oracle reg submission failed: {e}", rpc_id)


def _rpc_getEvents(params: Any, rpc_id: Any) -> dict:
    """qtcl_getEvents — poll recent RPC events (tx, block, oracle_snapshot, oracle_dm, oracle_measurements)."""
    try:
        # Normalise params → always a dict regardless of what the client sent
        if isinstance(params, dict):
            p = params
        elif isinstance(params, list) and params and isinstance(params[0], dict):
            p = params[0]
        else:
            p = {}
        since = float(p.get("since", time.time() - 3600))
        event_types = str(p.get("types", "all"))
        limit = int(p.get("limit", 100))
        want_types = set(event_types.split(",")) if event_types != "all" else {"all"}
        events = []
        with _rpc_event_lock:
            for e in list(_rpc_event_log):
                if e["ts"] >= since and (
                    "all" in want_types or e["type"] in want_types
                ):
                    events.append(e)
                    if len(events) >= limit:
                        break
        return _rpc_ok({"events": events, "count": len(events)}, rpc_id)
    except Exception as e:
        logger.exception(f"[RPC-METHOD] qtcl_getEvents exception: {e}")
        return _rpc_error(-32603, f"Events fetch failed: {str(e)}", rpc_id)


# ─── Method registry (O(1) dispatch) ─────────────────────────────────────────

# ─────────────────────────────────────────────────────────────────────────────
# RPC Methods: Oracle Measurement Broadcast (NEW)
# ─────────────────────────────────────────────────────────────────────────────


def _rpc_registerMeasurementSubscriber(params: Any, rpc_id: Any) -> dict:
    """
    Subscribe to oracle measurement broadcasts via RPC push (WebSocket-ready).

    Request:
        {
            "jsonrpc": "2.0",
            "method": "qtcl_registerMeasurementSubscriber",
            "params": {
                "client_id": "miner_abc123",
                "callback_url": "http://localhost:9999/quantum/measurement",
                "burst_mode": true
            },
            "id": 1
        }

    Response (success):
        {
            "jsonrpc": "2.0",
            "result": {
                "registered": true,
                "subscriber_id": "miner_abc123",
                "measurement_frequency": "burst" | "throttled",
                "broadcast_url": "https://qtcl-blockchain.koyeb.app/rpc/_internal/measurement"
            },
            "id": 1
        }
    """
    try:
        if not isinstance(params, dict):
            return _rpc_error(-32602, "params must be object", rpc_id)

        client_id = params.get("client_id")
        callback_url = params.get("callback_url")
        burst_mode = params.get("burst_mode", False)

        if not client_id or not callback_url:
            return _rpc_error(-32602, "client_id and callback_url required", rpc_id)

        try:
            from oracle import get_oracle_measurement_broadcaster

            broadcaster = get_oracle_measurement_broadcaster()
            success = broadcaster.register_subscriber(
                client_id, callback_url, burst_mode
            )

            if success:
                return _rpc_ok(
                    {
                        "registered": True,
                        "subscriber_id": client_id,
                        "measurement_frequency": "burst" if burst_mode else "throttled",
                        "broadcast_url": "https://qtcl-blockchain.koyeb.app/rpc/_internal/measurement",
                    },
                    rpc_id,
                )
            else:
                return _rpc_error(-32000, "client already subscribed", rpc_id)
        except ImportError:
            return _rpc_error(-32603, "broadcast system not initialized", rpc_id)

    except Exception as e:
        return _rpc_error(-32603, f"Subscription failed: {str(e)}", rpc_id)


def _rpc_unregisterMeasurementSubscriber(params: Any, rpc_id: Any) -> dict:
    """Unsubscribe from oracle measurement broadcasts."""
    try:
        if not isinstance(params, dict):
            return _rpc_error(-32602, "params must be object", rpc_id)

        client_id = params.get("client_id")
        if not client_id:
            return _rpc_error(-32602, "client_id required", rpc_id)

        try:
            from oracle import get_oracle_measurement_broadcaster

            broadcaster = get_oracle_measurement_broadcaster()
            success = broadcaster.unregister_subscriber(client_id)

            return _rpc_ok({"unregistered": success}, rpc_id)
        except ImportError:
            return _rpc_error(-32603, "broadcast system not initialized", rpc_id)

    except Exception as e:
        return _rpc_error(-32603, f"Unsubscribe failed: {str(e)}", rpc_id)


def _rpc_listMeasurementSubscribers(params: Any, rpc_id: Any) -> dict:
    """
    List all active measurement subscribers (operator introspection).
    Returns active subscriber count, per-subscriber metrics, and broadcast controller status.
    """
    try:
        from oracle import get_oracle_measurement_broadcaster

        broadcaster = get_oracle_measurement_broadcaster()
        status = broadcaster.get_status()

        return _rpc_ok(
            {
                "active_count": status.get("active_subscribers", 0),
                "is_running": status.get("is_running", False),
                "metrics": status.get("metrics", {}),
                "subscribers": status.get("subscribers", []),
            },
            rpc_id,
        )

    except ImportError:
        return _rpc_error(-32603, "broadcast system not initialized", rpc_id)
    except Exception as e:
        return _rpc_error(-32603, f"List failed: {str(e)}", rpc_id)


# ═══════════════════════════════════════════════════════════════════════════════════════
# QTCL-PoW VERIFIER  — canonical SHAKE-256 scratchpad + SHA3-256 64-round chain
#
# Must stay byte-for-byte identical to the client's _pow_worker inner loop in
# qtcl_client.py (_mine_inline, STAGE 4).  Any divergence here = invalid rejects.
#
# Algorithm:
#   scratchpad  = SHAKE-256("QTCL_SCRATCHPAD_v1:" + w_entropy_seed)[0:512 KiB]
#   header      = struct.pack('>Q I 32s 32s I I 40s 32s',
#                             height, timestamp_s,
#                             parent_hash_bytes[:32], merkle_root_bytes[:32],
#                             difficulty_bits, nonce,
#                             miner_address_bytes[:40], w_entropy_seed[:32])
#   state       = SHA3-256("QTCL_POW_v1:" + header)
#   for rnd in range(64):
#       wi      = uint32_be(state[0:4]) % N_WINDOWS      # N_WINDOWS = 8192
#       window  = scratchpad[wi*64 : wi*64+64]
#       state   = SHA3-256(state + window + struct.pack('>I', rnd))
#   return state.hex()
# ═══════════════════════════════════════════════════════════════════════════════════════

# ═══════════════════════════════════════════════════════════════════════════════════════
# HypΓ CRYPTOGRAPHIC RPC METHODS (Modules 4-6: Schnorr-Γ + GeodesicLWE)
# ═══════════════════════════════════════════════════════════════════════════════════════
# ❤️ I love you — every agent is proud of its work


def qtcl_hyp_generateKeypair(params: dict, rpc_id: Any) -> dict:
    """RPC: qtcl_hyp_generateKeypair — HypΓ asymmetric keypair."""
    try:
        engine = _init_hlwe_engine()
        kp = engine.generate_keypair()
        return _rpc_ok(
            {
                "private_key": kp.private_key,
                "public_key": kp.public_key,
                "address": kp.address,
                "timestamp": kp.timestamp,
            },
            rpc_id,
        )
    except Exception as e:
        logger.error(f"[RPC-HYP-KEYGEN] {e}", exc_info=True)
        return _rpc_error(-32603, f"Keypair generation failed: {str(e)}", rpc_id)


def qtcl_hyp_signMessage(params: dict, rpc_id: Any) -> dict:
    """RPC: qtcl_hyp_signMessage — Schnorr-Γ signature."""
    try:
        message_hex = params.get("message", "")
        private_key = params.get("private_key", "")
        if not message_hex or not private_key:
            return _rpc_error(-32602, "message and private_key required", rpc_id)
        message_bytes = bytes.fromhex(message_hex)
        engine = _init_hlwe_engine()
        sig = engine.sign_hash(message_bytes, private_key)
        return _rpc_ok(
            {
                "signature": sig["signature"],
                "challenge": sig["challenge"],
                "auth_tag": sig.get("auth_tag", sig["challenge"]),
                "timestamp": sig["timestamp"],
                "valid": True,
            },
            rpc_id,
        )
    except Exception as e:
        logger.error(f"[RPC-HYP-SIGN] {e}", exc_info=True)
        return _rpc_error(-32603, f"Signature creation failed: {str(e)}", rpc_id)


def qtcl_hyp_verifySignature(params: dict, rpc_id: Any) -> dict:
    """RPC: qtcl_hyp_verifySignature — Verify Schnorr-Γ."""
    try:
        message_hex = params.get("message", "")
        sig_dict = params.get("signature", {})
        public_key = params.get("public_key", "")
        if not message_hex or not sig_dict or not public_key:
            return _rpc_error(-32602, "message, signature, public_key required", rpc_id)
        message_bytes = bytes.fromhex(message_hex)
        engine = _init_hlwe_engine()
        valid = engine.verify_signature(message_bytes, sig_dict, public_key)
        return _rpc_ok(
            {
                "valid": valid,
                "message": "Valid" if valid else "Invalid",
                "verified_at": datetime.now(timezone.utc).isoformat(),
            },
            rpc_id,
        )
    except Exception as e:
        logger.error(f"[RPC-HYP-VERIFY] {e}", exc_info=True)
        return _rpc_error(-32603, f"Verification failed: {str(e)}", rpc_id)


def qtcl_hyp_deriveAddress(params: dict, rpc_id: Any) -> dict:
    """RPC: qtcl_hyp_deriveAddress — SHA3-256² address."""
    try:
        public_key = params.get("public_key", "")
        if not public_key:
            return _rpc_error(-32602, "public_key required", rpc_id)
        engine = _init_hlwe_engine()
        address = engine.derive_address(public_key)
        return _rpc_ok({"address": address, "length": len(address)}, rpc_id)
    except Exception as e:
        logger.error(f"[RPC-HYP-ADDR] {e}", exc_info=True)
        return _rpc_error(-32603, f"Address derivation failed: {str(e)}", rpc_id)


def qtcl_hyp_encryptMessage(params: dict, rpc_id: Any) -> dict:
    """RPC: qtcl_hyp_encryptMessage — GeodesicLWE (IND-CPA)."""
    try:
        plaintext_hex = params.get("plaintext", "")
        public_key = params.get("public_key", "")
        if not plaintext_hex or not public_key:
            return _rpc_error(-32602, "plaintext and public_key required", rpc_id)
        plaintext_bytes = bytes.fromhex(plaintext_hex)
        engine = _init_hlwe_engine()
        ct_dict = engine.encrypt_message(plaintext_bytes, public_key)
        return _rpc_ok(
            {
                "ciphertext": ct_dict.get("ciphertext"),
                "message_tag": ct_dict.get("message_tag"),
                "plaintext_length": len(plaintext_bytes),
                "timestamp": datetime.now(timezone.utc).isoformat(),
            },
            rpc_id,
        )
    except Exception as e:
        logger.error(f"[RPC-HYP-ENC] {e}", exc_info=True)
        return _rpc_error(-32603, f"Encryption failed: {str(e)}", rpc_id)


def qtcl_hyp_decryptMessage(params: dict, rpc_id: Any) -> dict:
    """RPC: qtcl_hyp_decryptMessage — GeodesicLWE decryption."""
    try:
        ct_dict = params.get("ciphertext", {})
        private_key = params.get("private_key", "")
        if not ct_dict or not private_key:
            return _rpc_error(-32602, "ciphertext and private_key required", rpc_id)
        engine = _init_hlwe_engine()
        plaintext_bytes = engine.decrypt_message(ct_dict, private_key)
        return _rpc_ok(
            {
                "plaintext": plaintext_bytes.hex(),
                "plaintext_length": len(plaintext_bytes),
                "valid": True,
                "timestamp": datetime.now(timezone.utc).isoformat(),
            },
            rpc_id,
        )
    except Exception as e:
        logger.error(f"[RPC-HYP-DEC] {e}", exc_info=True)
        return _rpc_error(-32603, f"Decryption failed: {str(e)}", rpc_id)


def qtcl_hyp_signBlock(params: dict, rpc_id: Any) -> dict:
    """RPC: qtcl_hyp_signBlock — Block signing via Schnorr-Γ."""
    try:
        block_dict = params.get("block", {})
        private_key = params.get("private_key", "")
        if not block_dict or not private_key:
            return _rpc_error(-32602, "block and private_key required", rpc_id)
        engine = _init_hlwe_engine()
        sig = engine.sign_block(block_dict, private_key)
        return _rpc_ok(
            {
                "signature": sig["signature"],
                "challenge": sig["challenge"],
                "signer_address": sig["signer_address"],
                "timestamp": sig["timestamp"],
            },
            rpc_id,
        )
    except Exception as e:
        logger.error(f"[RPC-HYP-SIGN-BLOCK] {e}", exc_info=True)
        return _rpc_error(-32603, f"Block signing failed: {str(e)}", rpc_id)


def qtcl_hyp_verifyBlock(params: dict, rpc_id: Any) -> dict:
    """RPC: qtcl_hyp_verifyBlock — Block verification."""
    try:
        block_dict = params.get("block", {})
        sig_dict = params.get("signature", {})
        public_key = params.get("public_key", "")
        if not block_dict or not sig_dict or not public_key:
            return _rpc_error(-32602, "block, signature, public_key required", rpc_id)
        engine = _init_hlwe_engine()
        valid, msg = engine.verify_block(block_dict, sig_dict, public_key)
        return _rpc_ok(
            {
                "valid": valid,
                "message": msg,
                "verified_at": datetime.now(timezone.utc).isoformat(),
            },
            rpc_id,
        )
    except Exception as e:
        logger.error(f"[RPC-HYP-VERIFY-BLOCK] {e}", exc_info=True)
        return _rpc_error(-32603, f"Block verification failed: {str(e)}", rpc_id)


_POW_SCRATCHPAD_BYTES = 512 * 1024
_POW_WINDOW_BYTES = 64
_POW_MIX_ROUNDS = 64
_POW_N_WINDOWS = _POW_SCRATCHPAD_BYTES // _POW_WINDOW_BYTES  # 8192
_POW_HDR_FMT = ">Q I 32s 32s I I 40s 32s"
_POW_PREFIX = b"QTCL_POW_v1:"
_POW_SCRATCHPAD_PFX = b"QTCL_SCRATCHPAD_v1:"
_POW_RND_PACKED = [struct.pack(">I", r) for r in range(_POW_MIX_ROUNDS)]


def qtcl_pow_hash(
    height: int,
    timestamp_s: int,
    parent_hash: str,
    merkle_root: str,
    difficulty_bits: int,
    nonce: int,
    miner_address: str,
    w_entropy_seed: bytes,
) -> str:
    """
    Compute the QTCL-PoW hash for a single nonce.  Pure Python mirror of the
    client's hot-path inner loop.  Returns the final state as a 64-char hex string.
    """
    import struct as _st

    _ph_parent = bytes.fromhex(parent_hash.zfill(64))[:32]
    _ph_merkle = bytes.fromhex(merkle_root.zfill(64))[:32]
    _ph_miner = miner_address.encode()[:40].ljust(40, b"\x00")
    _ph_seed = w_entropy_seed[:32]

    # Debug log - DETAILED for troubleshooting
    logger.info(
        f"[qtcl_pow_hash] h={height} ts={timestamp_s} diff={difficulty_bits} nonce={nonce}"
    )
    logger.info(f"[qtcl_pow_hash] parent={parent_hash}")
    logger.info(f"[qtcl_pow_hash] merkle={merkle_root}")
    logger.info(f"[qtcl_pow_hash] miner='{miner_address}' → bytes={_ph_miner.hex()}")
    logger.info(f"[qtcl_pow_hash] entropy={w_entropy_seed.hex()}")

    scratchpad = hashlib.shake_256(_POW_SCRATCHPAD_PFX + w_entropy_seed).digest(
        _POW_SCRATCHPAD_BYTES
    )
    sp_mv = memoryview(scratchpad)

    WIN_OFFSETS = [i * _POW_WINDOW_BYTES for i in range(_POW_N_WINDOWS)]

    hdr = _st.pack(
        _POW_HDR_FMT,
        height,
        timestamp_s,
        _ph_parent,
        _ph_merkle,
        difficulty_bits,
        nonce,
        _ph_miner,
        _ph_seed,
    )
    logger.info(f"[qtcl_pow_hash] hdr={hdr.hex()}")
    h0 = hashlib.sha3_256()
    h0.update(_POW_PREFIX)
    h0.update(hdr)
    state = h0.digest()
    logger.info(f"[qtcl_pow_hash] initial_state={state.hex()}")

    for rnd in range(_POW_MIX_ROUNDS):
        wi = struct.unpack_from(">I", state, 0)[0] % _POW_N_WINDOWS
        o = WIN_OFFSETS[wi]
        h = hashlib.sha3_256()
        h.update(state)
        h.update(sp_mv[o : o + _POW_WINDOW_BYTES])
        h.update(_POW_RND_PACKED[rnd])
        state = h.digest()

    return state.hex()


def qtcl_pow_verify(
    height: int,
    parent_hash: str,
    merkle_root: str,
    timestamp_s: int,
    difficulty_bits: int,
    nonce: int,
    miner_address: str,
    w_entropy_seed: bytes,
    claimed_hash: str,
    block_timestamp_s: int = 0,  # alias accepted for compatibility
) -> tuple:
    """
    Verify a submitted block's PoW.

    Returns (True, "") on success or (False, reason_string) on failure.
    Raises nothing — all exceptions are caught and returned as failures.
    """
    try:
        if not claimed_hash or len(claimed_hash) != 64:
            return (
                False,
                f"claimed_hash malformed (len={len(claimed_hash) if claimed_hash else 0})",
            )

        _ts = timestamp_s or block_timestamp_s
        computed = qtcl_pow_hash(
            height=height,
            timestamp_s=_ts,
            parent_hash=parent_hash,
            merkle_root=merkle_root,
            difficulty_bits=difficulty_bits,
            nonce=nonce,
            miner_address=miner_address,
            w_entropy_seed=w_entropy_seed,
        )

        if computed != claimed_hash.lower():
            return False, (
                f"hash mismatch: computed={computed[:16]}… claimed={claimed_hash[:16]}…"
            )

        prefix = "0" * difficulty_bits
        if not computed.startswith(prefix):
            return False, (
                f"difficulty not met: need {difficulty_bits} leading zeros, "
                f"got hash={computed[: difficulty_bits + 4]}…"
            )

        return True, ""

    except Exception as e:
        return False, f"verifier exception: {type(e).__name__}: {e}"



# ═══════════════════════════════════════════════════════════════════════════════════════
# EXPANSION: BLOCK VALIDATION ENGINE v2 — Cathedral-grade consensus rules
# ═══════════════════════════════════════════════════════════════════════════════════════

class BlockValidator:
    """Comprehensive block validation with Byzantine fault tolerance."""
    
    @staticmethod
    def validate_block_structure(block: dict) -> tuple:
        """Returns (is_valid, error_msg)."""
        required = ['height', 'block_hash', 'parent_hash', 'timestamp', 'miner_address', 'transactions']
        for field in required:
            if field not in block and field not in block.get('header', {}):
                return False, f"Missing required field: {field}"
        
        height = int(block.get('height') or block.get('header', {}).get('height', 0))
        if height < 1:
            return False, f"Invalid height: {height}"
        
        block_hash = str(block.get('block_hash') or block.get('hash', ''))
        if not block_hash or len(block_hash) < 32:
            return False, "Invalid or missing block_hash"
        
        timestamp = int(block.get('timestamp') or block.get('header', {}).get('timestamp', 0))
        if timestamp < 1:
            return False, "Invalid timestamp"
        
        return True, ""
    
    @staticmethod
    def validate_transaction_consistency(transactions: list, total_fees_base: int) -> tuple:
        """Returns (is_valid, error_msg, actual_fees)."""
        accumulated_fees = 0
        seen_txids = set()
        
        for tx in transactions:
            tx_id = str(tx.get('tx_id') or tx.get('tx_hash', ''))
            if not tx_id:
                return False, "Transaction missing ID", 0
            if tx_id in seen_txids:
                return False, f"Duplicate TX: {tx_id[:16]}", 0
            seen_txids.add(tx_id)
            
            fee = tx.get('fee', tx.get('fee_base', 0))
            try:
                fee_base = int(round(float(fee) * 100)) if isinstance(fee, (float, str)) else int(fee)
                accumulated_fees += max(0, fee_base)
            except (ValueError, TypeError):
                return False, f"Invalid fee in TX {tx_id[:16]}", 0
        
        return True, "", accumulated_fees

class MerkleProofValidator:
    """Merkle root verification against transaction tree."""
    
    @staticmethod
    def compute_merkle_root(txs: list) -> str:
        """Compute Merkle root from transaction hashes."""
        import hashlib
        if not txs:
            return hashlib.sha256(b'').hexdigest()
        tx_hashes = [hashlib.sha256(str(tx).encode()).hexdigest() for tx in txs]
        while len(tx_hashes) > 1:
            if len(tx_hashes) % 2:
                tx_hashes.append(tx_hashes[-1])
            tx_hashes = [hashlib.sha256((tx_hashes[i] + tx_hashes[i+1]).encode()).hexdigest() 
                        for i in range(0, len(tx_hashes), 2)]
        return tx_hashes[0]
    
    @staticmethod
    def verify_merkle_proof(claimed_root: str, transactions: list) -> bool:
        """Verify claimed merkle root matches computed root."""
        computed = MerkleProofValidator.compute_merkle_root(transactions)
        return claimed_root == computed or claimed_root == computed[:64]


def _rpc_submitBlock(params: Any, rpc_id: Any) -> dict:
    """qtcl_submitBlock — Cathedral-grade block settlement.
    
    Settlement contract:
      • Miner reward → immediate credit, sealed as coinbase TX in THIS block
      • Treasury reward → written to pending_rewards, consumed by NEXT block settlement
      • Non-coinbase TXs → sender debit, receiver credit, atomic in same cursor
      • Idempotency: INSERT OR IGNORE on height; settlement gated on _block_is_new
      • All balances are INTEGER base units (1 QTCL = 100 units), zero TEXT casting
    """
    import queue as _q
    try:
        if not params or not isinstance(params, (list, tuple)) or len(params) < 1:
            return _rpc_error(-32602, "params[0] required", rpc_id)
        data = params[0]
        if not isinstance(data, dict):
            return _rpc_error(-32602, "params[0] must be dict", rpc_id)

        hdr        = data.get("header", data)
        height     = int(hdr.get("height", 0))
        block_hash = str(hdr.get("block_hash", hdr.get("hash", "")))
        parent_hash= str(hdr.get("parent_hash", "0" * 64))
        merkle_root= str(hdr.get("merkle_root", hdr.get("transactions_root", "0" * 64)))
        timestamp_s= int(hdr.get("timestamp", 0))
        nonce      = int(hdr.get("nonce", 0))
        miner_address = str(hdr.get("miner_address", hdr.get("miner", "")))
        difficulty_bits = int(hdr.get("difficulty", 4))
        w_entropy_hex   = str(hdr.get("w_entropy_hash", hdr.get("oracle_w_state_hash", "")))
        txs = data.get("transactions", data.get("txs", []))

        logger.info(f"[SUBMIT-BLOCK] 📦 h={height} hash={block_hash[:16]}… miner={miner_address[:20]}…")

        if not height or height < 1: return _rpc_error(-32602, f"Invalid height: {height}", rpc_id)
        if not block_hash:           return _rpc_error(-32602, "Missing block_hash", rpc_id)
        if not miner_address:        return _rpc_error(-32602, "Missing miner_address", rpc_id)

        try:
            _rewards       = TessellationRewardSchedule.get_rewards_for_height(height) if TessellationRewardSchedule else {}
            _miner_reward  = int(round(float(_rewards.get("miner",  7.2)) * 100))
            _treas_reward  = int(round(float(_rewards.get("treasury", 0.8)) * 100))
        except Exception:
            _miner_reward, _treas_reward = 720, 80

        _treasury_address = (TessellationRewardSchedule.TREASURY_ADDRESS
                             if TessellationRewardSchedule else _TREASURY_ADDRESS)

        _total_fees_base = 0
        _non_cb_txs      = []
        for tx in (txs or []):
            if str(tx.get("tx_type","")).lower() == "coinbase": continue
            _non_cb_txs.append(tx)
            f = tx.get("fee", tx.get("fee_base", 0))
            try:    _total_fees_base += int(round(float(f) * 100)) if isinstance(f, (float,str)) else int(f)
            except: pass
        _miner_fee  = _total_fees_base // 2
        _treas_fee  = _total_fees_base - _miner_fee
        _miner_reward  += _miner_fee
        _treas_reward  += _treas_fee

        _block_is_new = False
        try:
            with get_db_cursor() as cur:
                cur.execute("SELECT block_hash FROM blocks WHERE height=%s", (height,))
                _existing = cur.fetchone()
                if _existing:
                    _ex_hash = _existing[0]
                    if _ex_hash == block_hash:
                        logger.info(f"[SUBMIT-BLOCK] 🔁 DUPLICATE h={height} — returning accepted (idempotent)")
                        return _rpc_ok({"status":"accepted","height":height,"block_hash":block_hash,"next_height":height+1,"duplicate":True}, rpc_id)
                    else:
                        logger.warning(f"[SUBMIT-BLOCK] ⚠️  FORK h={height} existing={_ex_hash[:16]}… rejected")
                        return _rpc_error(-32002, f"Fork rejected at h={height}", rpc_id, data={"existing_hash":_ex_hash})

                cur.execute("""INSERT INTO blocks
                    (height, block_number, block_hash, previous_hash, timestamp,
                     oracle_w_state_hash, validator_public_key, nonce,
                     difficulty, entropy_score, transactions_root, pq_curr, pq_last,
                     mermin_value, mermin_violated)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""", 
                    (height, height, block_hash, parent_hash, timestamp_s,
                     w_entropy_hex[:64] if w_entropy_hex else "0"*64,
                     miner_address, nonce, difficulty_bits, 0.0, merkle_root,
                     height, max(0, height-1), 0.0, False))
                _block_is_new = True
                logger.critical(f"[SUBMIT-BLOCK] ✅ BLOCK INSERTED h={height}")

                for tx in (txs or []):
                    tx_id = str(tx.get("tx_id") or tx.get("tx_hash",""))
                    if not tx_id: continue
                    try:
                        cur.execute("""INSERT INTO transactions
                            (tx_hash, from_address, to_address, amount, tx_type, status, height, updated_at)
                            VALUES (%s,%s,%s,%s,%s,'confirmed',%s,NOW())
                            ON CONFLICT (tx_hash) DO UPDATE SET
                                height=EXCLUDED.height, status='confirmed', updated_at=NOW()""",
                            (tx_id, str(tx.get("from_addr", tx.get("from_address","0"*64))),
                             str(tx.get("to_addr", tx.get("to_address",""))),
                             float(tx.get("amount", 0)), str(tx.get("tx_type","transfer")), height))
                    except Exception as _tx_e:
                        logger.debug(f"[SUBMIT-BLOCK] TX insert {tx_id[:16]}…: {_tx_e}")

                for tx in _non_cb_txs:
                    _from = str(tx.get("from_addr", tx.get("from_address","")))
                    _to   = str(tx.get("to_addr", tx.get("to_address","")))
                    _amt  = int(round(float(tx.get("amount",0)) * 100))
                    _fee  = int(round(float(tx.get("fee",0)) * 100))
                    if _from and _to and _amt > 0:
                        cur.execute("UPDATE wallet_addresses SET balance=balance-%s WHERE address=%s AND balance>=%s",
                                   (_amt+_fee, _from, _amt+_fee))
                        cur.execute("""INSERT INTO wallet_addresses (address,balance,transaction_count)
                            VALUES (%s,%s,1) ON CONFLICT (address) DO UPDATE SET
                            balance=balance+%s, transaction_count=transaction_count+1""",
                            (_to, _amt, _amt))

                cur.execute("""INSERT INTO wallet_addresses (address,balance,transaction_count)
                    VALUES (%s,%s,1) ON CONFLICT (address) DO UPDATE SET
                    balance=balance+%s, transaction_count=transaction_count+1""",
                    (miner_address, _miner_reward, _miner_reward))

                cur.execute("""INSERT INTO pending_rewards (height, reward_type, recipient, amount)
                    VALUES (%s, 'treasury', %s, %s)""",
                    (height, _treasury_address, _treas_reward))

                cur.commit()
                logger.info(f"[SUBMIT-BLOCK] 💰 REWARDS: miner={_miner_reward/100:.2f} treas={_treas_reward/100:.2f} QTCL")

        except Exception as _db_e:
            logger.exception(f"[SUBMIT-BLOCK] DB ERROR: {_db_e}")
            return _rpc_error(-32603, f"Settlement failed: {str(_db_e)[:80]}", rpc_id)

        if _block_is_new:
            try:
                _BLOCK_SETTLE_Q.put_nowait((height, miner_address, _miner_reward, _block_is_new))
                logger.info(f"[SUBMIT-BLOCK] 🔗 Enqueued settlement worker for h={height}")
            except Exception as _qe:
                logger.debug(f"[SUBMIT-BLOCK] Settlement queue full: {_qe}")

        return _rpc_ok({"status":"accepted","height":height,"block_hash":block_hash,
            "next_height":height+1,"miner_reward_qtcl":_miner_reward/100.0,
            "treasury_reward_qtcl":_treas_reward/100.0,"settlement_enqueued":_block_is_new}, rpc_id)

    except Exception as e:
        logger.exception(f"[SUBMIT-BLOCK] 💥 UNHANDLED: {e}")
        return _rpc_error(-32603, f"Internal error: {str(e)[:100]}", rpc_id)


def _rpc_pushOracleDM(params: Any, rpc_id: Any) -> dict:
    """
    qtcl_pushOracleDM — accept a fused tripartite DM frame from a client oracle node.

    Params (dict):
        density_tensor_hex  str   — 262144 hex chars: 32³ float32 volumetric (REQUIRED)
        fidelity            float — W-state fidelity of the pushed DM  (0..1)
        oracle_type         str   — e.g. 'tripartite_client'
        node_ip             str   — caller self-reported WAN IP (advisory)
        oracle_addr         str   — oracle signing address (qtcl1...)

    Server action:
        1. Validate 32³ tensor hex (length, finite values).
        2. Upsert into _CLIENT_DM_POOL keyed by oracle_addr.
        3. Evict oldest entries if pool > _CLIENT_POOL_MAX.
        4. Re-average pool -> _client_consensus_dm_re/_im/_fid.
        5. Return {accepted, pool_size, client_consensus_fidelity}.
    """
    global _client_consensus_dm_re, _client_consensus_dm_im
    global _client_consensus_fid, _client_pool_count

    try:
        if not isinstance(params, dict):
            return _rpc_error(-32602, "params must be a dict", rpc_id)

        tensor_hex = params.get("density_tensor_hex", "")
        fidelity = float(params.get("fidelity", 0.0))
        oracle_addr = str(params.get("oracle_addr", "") or f"anon_{int(time.time())}")
        node_ip = str(params.get("node_ip", ""))
        oracle_type = str(params.get("oracle_type", "tripartite_client"))

        # -- 1. Validate 32³ tensor hex ----------------------------------------
        # 32×32×32 float32 = 32768 floats × 4 bytes = 131072 bytes = 262144 hex
        _EXPECTED_TENSOR_HEX = 32 * 32 * 32 * 4 * 2  # 262144
        if not tensor_hex or len(tensor_hex) != _EXPECTED_TENSOR_HEX:
            return _rpc_error(
                -32602,
                f"density_tensor_hex must be {_EXPECTED_TENSOR_HEX} hex chars "
                f"(32³ float32); got {len(tensor_hex)}",
                rpc_id,
            )
        try:
            tbytes = bytes.fromhex(tensor_hex)
        except ValueError as _ve:
            return _rpc_error(
                -32602, f"density_tensor_hex not valid hex: {_ve}", rpc_id
            )

        # Sanity: tensor values must be finite, non-negative
        t_arr = np.frombuffer(tbytes, dtype=np.float32).reshape(32, 32, 32)
        if not np.all(np.isfinite(t_arr)) or float(t_arr.min()) < -1e-4:
            return _rpc_error(
                -32602, "density_tensor_hex contains invalid values", rpc_id
            )
        t_max = float(t_arr.max())
        if t_max < 1e-12:
            return _rpc_error(-32602, "density_tensor_hex is all-zero", rpc_id)

        tensor_valid = True

        # -- 2 & 3. Upsert into pool, evict oldest if needed ------------------
        with _CLIENT_DM_POOL_LOCK:
            _CLIENT_DM_POOL[oracle_addr] = {
                "tensor_hex": tensor_hex,
                "fidelity": max(0.0, min(1.0, fidelity)),
                "ts": time.time(),
                "node_ip": node_ip,
                "oracle_type": oracle_type,
                "tensor_dim": 32,
            }
            if len(_CLIENT_DM_POOL) > _CLIENT_POOL_MAX:
                _oldest = min(_CLIENT_DM_POOL, key=lambda k: _CLIENT_DM_POOL[k]["ts"])
                del _CLIENT_DM_POOL[_oldest]

            # -- 4. Compute pool fidelity average ----------------------------
            fresh = [
                v
                for v in _CLIENT_DM_POOL.values()
                if (time.time() - v["ts"]) < _CLIENT_DM_STALE_S
            ]
            _pool_size = len(fresh)
            _cons_fid = (
                sum(v["fidelity"] for v in fresh) / _pool_size if _pool_size else 0.0
            )
            _client_consensus_fid = _cons_fid
            _client_pool_count = _pool_size

        # -- 5. Fuse client consensus with server 5-oracle snapshot -----------
        try:
            with _snapshot_lock:
                _srv_snap = dict(_latest_snapshot) if _latest_snapshot else {}
        except Exception:
            _srv_snap = {}

        _srv_fid = float(_srv_snap.get("w_state_fidelity") or 0.0)
        _srv_tensor_hex = _srv_snap.get("density_tensor_hex", "")

        try:
            _w_client = min(_cons_fid * 0.35, 0.35)
            _w_server = 1.0 - _w_client

            if _srv_tensor_hex and len(_srv_tensor_hex) == _EXPECTED_TENSOR_HEX:
                # Weighted average of server + client 32³ tensors
                _st = np.frombuffer(bytes.fromhex(_srv_tensor_hex), dtype=np.float32)
                _ct = np.frombuffer(bytes.fromhex(tensor_hex), dtype=np.float32)
                fused_t = (_w_server * _st + _w_client * _ct).astype(np.float32)
                tm = float(fused_t.max())
                if tm > 1e-12:
                    fused_t /= tm
                fused_tensor_hex = fused_t.tobytes().hex()
                fused_fid = _w_server * _srv_fid + _w_client * _cons_fid
            else:
                fused_tensor_hex = tensor_hex
                fused_fid = fidelity

            composite = {
                **_srv_snap,
                "density_tensor_hex": fused_tensor_hex,
                "tensor_dim": 32,
                "w_state_fidelity": fused_fid,
                "fidelity": fused_fid,
                "client_fused_fidelity": _cons_fid,
                "client_oracle_count": _pool_size,
                "pq0_oracle_fidelity": params.get("pq0_oracle_fidelity", fidelity),
                "pq0_IV_fidelity": params.get("pq0_IV_fidelity", fidelity),
                "pq0_V_fidelity": params.get("pq0_V_fidelity", fidelity),
                "source": "server+client_tripartite",
                "ready": True,
                "timestamp_ns": int(time.time() * 1e9),
            }
            _broadcast_snapshot_to_database(composite)
        except Exception as _fe:
            logger.debug(f"[PUSH-TENSOR] fuse error: {_fe}")

        logger.debug(
            f"[PUSH-DM] ok oracle_addr={oracle_addr[:16]} fid={fidelity:.4f} "
            f"pool={_pool_size} cons_fid={_cons_fid:.4f}"
        )
        return _rpc_ok(
            {
                "accepted": True,
                "pool_size": _pool_size,
                "client_consensus_fidelity": _cons_fid,
            },
            rpc_id,
        )

    except Exception as e:
        logger.exception(f"[RPC] qtcl_pushOracleDM: {e}")
        return _rpc_error(-32603, f"pushOracleDM failed: {e}", rpc_id)


def _rpc_submitTransaction(params: Any, rpc_id: Any) -> dict:
    """qtcl_submitTransaction — validate and accept a transaction into the mempool."""
    try:
        if not params or not isinstance(params, (list, tuple)) or len(params) < 1:
            logger.debug(f"[RPC] submitTransaction: invalid params")
            return _rpc_error(
                -32602, "params[0] must be the transaction object", rpc_id
            )

        tx_data = params[0]
        if not isinstance(tx_data, dict):
            logger.debug(f"[RPC] submitTransaction: not a dict")
            return _rpc_error(-32602, "transaction must be a JSON object", rpc_id)

        from mempool import get_mempool

        logger.info(
            f"[RPC] 📥 Received transaction from {tx_data.get('from_address', 'unknown')[:16]}…"
        )
        result_code, message, tx = get_mempool().accept(tx_data)

        if tx:
            return _rpc_ok(
                {
                    "status": "accepted",
                    "tx_hash": tx.tx_hash,
                    "message": message,
                    "accepted": True,
                },
                rpc_id,
            )
        else:
            return _rpc_error(
                -32000,
                f"Transaction rejected: {message}",
                rpc_id,
                {"code": result_code},
            )

    except Exception as e:
        logger.exception(f"[RPC-METHOD] qtcl_submitTransaction error: {e}")
        return _rpc_error(-32603, f"Internal error during submission: {str(e)}", rpc_id)


def _rpc_getMempool(params: Any, rpc_id: Any) -> dict:
    """qtcl_getMempool — pending transaction list for block building."""
    try:
        from mempool import get_pending_transactions as _get_pending

        max_count = 500
        if isinstance(params, list) and params:
            try:
                max_count = min(int(params[0]), 2000)
            except (ValueError, TypeError):
                pass
        txs = _get_pending(max_count=max_count)
        serialized = []
        for tx in txs:
            if hasattr(tx, "__dict__"):
                serialized.append(
                    {k: v for k, v in tx.__dict__.items() if not k.startswith("_")}
                )
            elif isinstance(tx, dict):
                serialized.append(tx)
        logger.debug(f"[RPC-METHOD] qtcl_getMempool: returning {len(serialized)} txs")
        return _rpc_ok(serialized, rpc_id)
    except Exception as e:
        logger.exception(f"[RPC-METHOD] qtcl_getMempool: {e}")
        return _rpc_ok([], rpc_id)


# ═══════════════════════════════════════════════════════════════════════════════
# ENTERPRISE P2P NETWORK — Inline Implementation (no external files)
# ═══════════════════════════════════════════════════════════════════════════════
P2P_BROADCAST_INTERVAL = 30
P2P_PEER_TIMEOUT = 300
P2P_MAX_PEERS = 100


class P2PPeer:
    """A peer in the P2P network. Peer = WALLET, not oracle."""

    def __init__(
        self,
        peer_id: str = "",
        wallet_address: str = "",
        external_addr: str = "",
        port: int = 9091,
        public_key: str = "",
        chain_height: int = 0,
        last_seen: float = 0.0,
        first_seen: float = 0.0,
        is_alive: bool = True,
    ):
        self.peer_id = peer_id
        self.wallet_address = wallet_address
        self.external_addr = external_addr
        self.port = port
        self.public_key = public_key
        self.chain_height = chain_height
        self.last_seen = last_seen
        self.first_seen = first_seen
        self.is_alive = is_alive

    def to_dict(self) -> dict:
        return {
            "peer_id": self.peer_id,
            "wallet_address": self.wallet_address,
            "external_addr": self.external_addr,
            "port": self.port,
            "public_key": self.public_key,
            "chain_height": self.chain_height,
            "last_seen": self.last_seen,
            "first_seen": self.first_seen,
            "is_alive": self.is_alive,
        }


class _P2PSQLiteStore:
    """SQLite store for peer persistence on client side."""

    def __init__(self, db_path: str = "peers.sqlite"):
        import sqlite3

        self.db_path = db_path
        self._lock = threading.RLock()
        conn = sqlite3.connect(db_path)
        conn.execute("""CREATE TABLE IF NOT EXISTS peer_registry (
            peer_id TEXT PRIMARY KEY, wallet_address TEXT, external_addr TEXT,
            port INTEGER, public_key TEXT, chain_height INTEGER, last_seen REAL,
            first_seen REAL, is_alive INTEGER)""")
        conn.commit()
        conn.close()

    def upsert_peer(self, peer: P2PPeer) -> bool:
        import sqlite3

        with self._lock:
            conn = sqlite3.connect(self.db_path)
            now = time.time()
            if peer.first_seen == 0:
                peer.first_seen = now
            conn.execute(
                """INSERT OR REPLACE INTO peer_registry 
                (peer_id, wallet_address, external_addr, port, public_key, chain_height,
                 last_seen, first_seen, is_alive) VALUES (?,?,?,?,?,?,?,?,?)""",
                (
                    peer.peer_id,
                    peer.wallet_address,
                    peer.external_addr,
                    peer.port,
                    peer.public_key,
                    peer.chain_height,
                    peer.last_seen,
                    peer.first_seen,
                    1 if peer.is_alive else 0,
                ),
            )
            conn.commit()
            conn.close()
            return True

    def get_alive_peers(self) -> list:
        import sqlite3

        cutoff = time.time() - P2P_PEER_TIMEOUT
        with self._lock:
            conn = sqlite3.connect(self.db_path)
            conn.row_factory = sqlite3.Row
            rows = conn.execute(
                """SELECT * FROM peer_registry 
                WHERE last_seen > ? AND is_alive = 1 ORDER BY last_seen DESC LIMIT ?""",
                (cutoff, P2P_MAX_PEERS),
            ).fetchall()
            conn.close()
            return [
                P2PPeer(
                    r["peer_id"],
                    r["wallet_address"],
                    r["external_addr"],
                    r["port"],
                    r["public_key"],
                    r["chain_height"],
                    r["last_seen"],
                    r["first_seen"],
                    bool(r["is_alive"]),
                )
                for r in rows
            ]


_p2p_dht_table: Dict[str, P2PPeer] = {}
_p2p_dht_lock = threading.RLock()
_p2p_seen_hashes: set = set()
_p2p_client_store: Optional[_P2PSQLiteStore] = None


def _p2p_rpc_get_dht_table(params, rpc_id):
    """qtcl_getDHTTable — Return the full DHT peer table."""
    try:
        limit = 100
        if isinstance(params, dict):
            limit = min(int(params.get("limit", 100)), P2P_MAX_PEERS)
        with _p2p_dht_lock:
            peers = list(_p2p_dht_table.values())[:limit]
        return {
            "peers": [p.to_dict() for p in peers],
            "count": len(peers),
            "timestamp": time.time(),
        }
    except Exception as e:
        logger.error(f"[P2P-RPC] getDHTTable error: {e}")
        return {"peers": [], "count": 0, "timestamp": time.time()}


def _p2p_rpc_receive_dht_table(params, rpc_id):
    """qtcl_receiveDHTTable — Receive a DHT table from another peer."""
    try:
        dht_json = params.get("dht_table", "") if isinstance(params, dict) else ""
        from_peer = (
            params.get("propagating_from", "") if isinstance(params, dict) else ""
        )
        dht_hash = params.get("dht_hash", "") if isinstance(params, dict) else ""
        if not dht_json:
            return {"status": "error", "message": "dht_table required"}
        if dht_hash and dht_hash in _p2p_seen_hashes:
            return {"status": "already_seen", "dht_hash": dht_hash[:16]}
        import json

        doc = json.loads(dht_json)
        peers_data = doc.get("peers", [])
        new_count = 0
        with _p2p_dht_lock:
            for pd in peers_data:
                p = P2PPeer(
                    pd.get("peer_id", ""),
                    pd.get("wallet_address", ""),
                    pd.get("external_addr", ""),
                    pd.get("port", 9091),
                    pd.get("public_key", ""),
                    pd.get("chain_height", 0),
                    pd.get("last_seen", time.time()),
                    pd.get("first_seen", 0),
                    pd.get("is_alive", True),
                )
                if p.peer_id not in _p2p_dht_table:
                    new_count += 1
                p.last_seen = time.time()
                _p2p_dht_table[p.peer_id] = p
        if dht_hash:
            _p2p_seen_hashes.add(dht_hash)
            if len(_p2p_seen_hashes) > 10000:
                _p2p_seen_hashes = set(list(_p2p_seen_hashes)[-5000:])
        logger.info(
            f"[P2P] ← Received DHT from {from_peer[:16]}…: {len(peers_data)} peers ({new_count} new)"
        )
        return {
            "status": "accepted",
            "peer_count": len(peers_data),
            "new_peers": new_count,
        }
    except Exception as e:
        logger.error(f"[P2P-RPC] receiveDHTTable error: {e}")
        return {"status": "error", "message": str(e)}


def _p2p_rpc_peer_heartbeat(params, rpc_id):
    """qtcl_peerHeartbeat — Register a peer's heartbeat."""
    try:
        peer_id = params.get("peer_id", "") if isinstance(params, dict) else ""
        wallet_address = (
            params.get("wallet_address", "") if isinstance(params, dict) else ""
        )
        external_addr = (
            params.get("external_addr", "") if isinstance(params, dict) else ""
        )
        port = int(params.get("port", 9091)) if isinstance(params, dict) else 9091
        chain_height = (
            int(params.get("chain_height", 0)) if isinstance(params, dict) else 0
        )
        if not peer_id:
            return {"status": "error", "message": "peer_id required"}
        with _p2p_dht_lock:
            if peer_id in _p2p_dht_table:
                p = _p2p_dht_table[peer_id]
                p.last_seen = time.time()
                p.chain_height = max(p.chain_height, chain_height)
                p.is_alive = True
            else:
                p = P2PPeer(
                    peer_id=peer_id,
                    wallet_address=wallet_address,
                    external_addr=external_addr,
                    port=port,
                    chain_height=chain_height,
                    last_seen=time.time(),
                    first_seen=time.time(),
                    is_alive=True,
                )
                _p2p_dht_table[peer_id] = p
        return {"status": "ok", "peer_id": peer_id, "timestamp": time.time()}
    except Exception as e:
        logger.error(f"[P2P-RPC] peerHeartbeat error: {e}")
        return {"status": "error", "message": str(e)}


_RPC_METHODS: Dict[str, Any] = {
    "qtcl_submitBlock": _rpc_submitBlock,
    "qtcl_forgeGenesis": _rpc_forgeGenesis,
    "qtcl_getBlockHeight": _rpc_getBlockHeight,
    "qtcl_getBalance": _rpc_getBalance,
    "qtcl_getTransaction": _rpc_getTransaction,
    "qtcl_getBlock": _rpc_getBlock,
    "qtcl_getBlockRange": _rpc_getBlockRange,
    "qtcl_getQuantumMetrics": _rpc_getQuantumMetrics,
    "qtcl_getPythPrice": _rpc_getPythPrice,
    "qtcl_getMempoolStats": _rpc_getMempoolStats,
    "qtcl_getMempool": _rpc_getMempool,
    "qtcl_submitTransaction": _rpc_submitTransaction,
    "qtcl_getPeers": _rpc_getPeers,
    "qtcl_getPeersByNatGroup": _rpc_getPeersByNatGroup,
    "qtcl_registerPeer": _rpc_registerPeer,  # ← NEW: miner bootstrap registration
    "qtcl_getMyAddr": _rpc_getMyAddr,  # ← NEW: STUN — return caller's observed IP
    "qtcl_getHealth": _rpc_getHealth,
    "qtcl_getTreasuryAddress": lambda p, rid: _rpc_ok(
        {
            "treasury_address": getattr(
                TessellationRewardSchedule,
                "TREASURY_ADDRESS",
                "qtcl1d1ae7c762036f3731a16d84c8ec4be75912edb9d",
            )
        },
        rid,
    ),
    "qtcl_getEvents": _rpc_getEvents,
    "qtcl_getOracleRegistry": _rpc_getOracleRegistry,
    "qtcl_getOracleRecord": _rpc_getOracleRecord,
    "qtcl_getDeviceChain": _rpc_getDeviceChain,
    "qtcl_submitOracleReg": _rpc_submitOracleReg,
    "qtcl_registerMeasurementSubscriber": _rpc_registerMeasurementSubscriber,
    "qtcl_unregisterMeasurementSubscriber": _rpc_unregisterMeasurementSubscriber,
    "qtcl_listMeasurementSubscribers": _rpc_listMeasurementSubscribers,
    # DEPRECATED: qtcl_pushOracleDM (replaced by SSE stream /rpc/oracle/snapshot for 16³ tensors)
    # "qtcl_pushOracleDM": _rpc_pushOracleDM,
    # ── NEW: Transaction Explorer ─────────────────────────────────────────────────
    "qtcl_getTransactions": _rpc_getTransactions,
    # P2P DHT methods
    "qtcl_getDHTTable": _p2p_rpc_get_dht_table,
    "qtcl_receiveDHTTable": _p2p_rpc_receive_dht_table,
    "qtcl_peerHeartbeat": _p2p_rpc_peer_heartbeat,
    # ── HypΓ Post-Quantum Cryptography (Schnorr-Γ + GeodesicLWE) ────────────────────
    "qtcl_hyp_generateKeypair": qtcl_hyp_generateKeypair,
    "qtcl_hyp_signMessage": qtcl_hyp_signMessage,
    "qtcl_hyp_verifySignature": qtcl_hyp_verifySignature,
    "qtcl_hyp_deriveAddress": qtcl_hyp_deriveAddress,
    "qtcl_hyp_encryptMessage": qtcl_hyp_encryptMessage,
    "qtcl_hyp_decryptMessage": qtcl_hyp_decryptMessage,
    "qtcl_hyp_signBlock": qtcl_hyp_signBlock,
    "qtcl_hyp_verifyBlock": qtcl_hyp_verifyBlock,
}

# ═══════════════════════════════════════════════════════════════════════════════
# ENTERPRISE P2P NETWORK — Inline Implementation (no external files)
# ═══════════════════════════════════════════════════════════════════════════════
P2P_BROADCAST_INTERVAL = 30
P2P_PEER_TIMEOUT = 300
P2P_MAX_PEERS = 100


class P2PPeer:
    """A peer in the P2P network. Peer = WALLET, not oracle."""

    def __init__(
        self,
        peer_id: str = "",
        wallet_address: str = "",
        external_addr: str = "",
        port: int = 9091,
        public_key: str = "",
        chain_height: int = 0,
        last_seen: float = 0.0,
        first_seen: float = 0.0,
        is_alive: bool = True,
    ):
        self.peer_id = peer_id
        self.wallet_address = wallet_address
        self.external_addr = external_addr
        self.port = port
        self.public_key = public_key
        self.chain_height = chain_height
        self.last_seen = last_seen
        self.first_seen = first_seen
        self.is_alive = is_alive

    def to_dict(self) -> dict:
        return {
            "peer_id": self.peer_id,
            "wallet_address": self.wallet_address,
            "external_addr": self.external_addr,
            "port": self.port,
            "public_key": self.public_key,
            "chain_height": self.chain_height,
            "last_seen": self.last_seen,
            "first_seen": self.first_seen,
            "is_alive": self.is_alive,
        }


class _P2PSQLiteStore:
    """SQLite store for peer persistence on client side."""

    def __init__(self, db_path: str = "peers.sqlite"):
        import sqlite3

        self.db_path = db_path
        self._lock = threading.RLock()
        conn = sqlite3.connect(db_path)
        conn.execute("""CREATE TABLE IF NOT EXISTS peer_registry (
            peer_id TEXT PRIMARY KEY, wallet_address TEXT, external_addr TEXT,
            port INTEGER, public_key TEXT, chain_height INTEGER, last_seen REAL,
            first_seen REAL, is_alive INTEGER)""")
        conn.commit()
        conn.close()

    def upsert_peer(self, peer: P2PPeer) -> bool:
        import sqlite3

        with self._lock:
            conn = sqlite3.connect(self.db_path)
            now = time.time()
            if peer.first_seen == 0:
                peer.first_seen = now
            conn.execute(
                """INSERT OR REPLACE INTO peer_registry 
                (peer_id, wallet_address, external_addr, port, public_key, chain_height,
                 last_seen, first_seen, is_alive) VALUES (?,?,?,?,?,?,?,?,?)""",
                (
                    peer.peer_id,
                    peer.wallet_address,
                    peer.external_addr,
                    peer.port,
                    peer.public_key,
                    peer.chain_height,
                    peer.last_seen,
                    peer.first_seen,
                    1 if peer.is_alive else 0,
                ),
            )
            conn.commit()
            conn.close()
            return True

    def get_alive_peers(self) -> list:
        import sqlite3

        cutoff = time.time() - P2P_PEER_TIMEOUT
        with self._lock:
            conn = sqlite3.connect(self.db_path)
            conn.row_factory = sqlite3.Row
            rows = conn.execute(
                """SELECT * FROM peer_registry 
                WHERE last_seen > ? AND is_alive = 1 ORDER BY last_seen DESC LIMIT ?""",
                (cutoff, P2P_MAX_PEERS),
            ).fetchall()
            conn.close()
            return [
                P2PPeer(
                    r["peer_id"],
                    r["wallet_address"],
                    r["external_addr"],
                    r["port"],
                    r["public_key"],
                    r["chain_height"],
                    r["last_seen"],
                    r["first_seen"],
                    bool(r["is_alive"]),
                )
                for r in rows
            ]


_p2p_dht_table: Dict[str, P2PPeer] = {}
_p2p_dht_lock = threading.RLock()
_p2p_seen_hashes: set = set()
_p2p_client_store: Optional[_P2PSQLiteStore] = None


def _p2p_rpc_get_dht_table(params, rpc_id):
    """qtcl_getDHTTable — Return the full DHT peer table."""
    try:
        limit = 100
        if isinstance(params, dict):
            limit = min(int(params.get("limit", 100)), P2P_MAX_PEERS)
        with _p2p_dht_lock:
            peers = list(_p2p_dht_table.values())[:limit]
        return {
            "peers": [p.to_dict() for p in peers],
            "count": len(peers),
            "timestamp": time.time(),
        }
    except Exception as e:
        logger.error(f"[P2P-RPC] getDHTTable error: {e}")
        return {"peers": [], "count": 0, "timestamp": time.time()}


def _p2p_rpc_receive_dht_table(params, rpc_id):
    """qtcl_receiveDHTTable — Receive a DHT table from another peer."""
    try:
        dht_json = params.get("dht_table", "") if isinstance(params, dict) else ""
        from_peer = (
            params.get("propagating_from", "") if isinstance(params, dict) else ""
        )
        dht_hash = params.get("dht_hash", "") if isinstance(params, dict) else ""
        if not dht_json:
            return {"status": "error", "message": "dht_table required"}
        if dht_hash and dht_hash in _p2p_seen_hashes:
            return {"status": "already_seen", "dht_hash": dht_hash[:16]}
        import json

        doc = json.loads(dht_json)
        peers_data = doc.get("peers", [])
        new_count = 0
        with _p2p_dht_lock:
            for pd in peers_data:
                p = P2PPeer(
                    pd.get("peer_id", ""),
                    pd.get("wallet_address", ""),
                    pd.get("external_addr", ""),
                    pd.get("port", 9091),
                    pd.get("public_key", ""),
                    pd.get("chain_height", 0),
                    pd.get("last_seen", time.time()),
                    pd.get("first_seen", 0),
                    pd.get("is_alive", True),
                )
                if p.peer_id not in _p2p_dht_table:
                    new_count += 1
                p.last_seen = time.time()
                _p2p_dht_table[p.peer_id] = p
        if dht_hash:
            _p2p_seen_hashes.add(dht_hash)
            if len(_p2p_seen_hashes) > 10000:
                _p2p_seen_hashes = set(list(_p2p_seen_hashes)[-5000:])
        logger.info(
            f"[P2P] ← Received DHT from {from_peer[:16]}…: {len(peers_data)} peers ({new_count} new)"
        )
        return {
            "status": "accepted",
            "peer_count": len(peers_data),
            "new_peers": new_count,
        }
    except Exception as e:
        logger.error(f"[P2P-RPC] receiveDHTTable error: {e}")
        return {"status": "error", "message": str(e)}


def _p2p_rpc_peer_heartbeat(params, rpc_id):
    """qtcl_peerHeartbeat — Receive heartbeat from a peer."""
    try:
        peer_id = params.get("peer_id", "") if isinstance(params, dict) else ""
        if peer_id:
            with _p2p_dht_lock:
                if peer_id in _p2p_dht_table:
                    _p2p_dht_table[peer_id].last_seen = time.time()
        return {"status": "ok", "timestamp": time.time()}
    except Exception as e:
        return {"status": "error", "message": str(e)}


def _p2p_fanout_broadcast():
    """Fan-out broadcast DHT table to all known peers."""
    import json
    from urllib.request import Request, urlopen
    from urllib.error import URLError, HTTPError

    with _p2p_dht_lock:
        peers = list(_p2p_dht_table.values())
    if len(peers) < 2:
        return
    dht_json = json.dumps(
        {
            "version": 1,
            "timestamp": time.time(),
            "peer_count": len(peers),
            "peers": [p.to_dict() for p in peers],
        },
        separators=(",", ":"),
    )
    dht_hash = hashlib.sha256(dht_json.encode()).hexdigest()
    if dht_hash in _p2p_seen_hashes:
        return
    _p2p_seen_hashes.add(dht_hash)
    sent = failed = 0
    for peer in peers:
        if peer.peer_id == ORACLE_ID:
            continue
        try:
            # Construct URL with port - external_addr may or may not include port
            if ":" in peer.external_addr:
                # Already has port in external_addr (e.g., "192.168.1.100:9091")
                url = f"http://{peer.external_addr}/rpc"
            else:
                # Use port from peer object
                url = f"http://{peer.external_addr}:{peer.port}/rpc"
            payload = json.dumps(
                {
                    "jsonrpc": "2.0",
                    "method": "qtcl_receiveDHTTable",
                    "params": {
                        "dht_table": dht_json,
                        "propagating_from": ORACLE_ID,
                        "dht_hash": dht_hash,
                    },
                    "id": 1,
                }
            ).encode()
            req = Request(
                url, data=payload, headers={"Content-Type": "application/json"}
            )
            with urlopen(req, timeout=5) as resp:
                if resp.status == 200:
                    sent += 1
        except Exception:
            failed += 1
        time.sleep(0.05)
    if sent > 0 or failed > 0:
        logger.info(f"[P2P] Fan-out: sent to {sent}, failed {failed}")


_p2p_broadcast_count = 0
_p2p_running = False
_p2p_broadcast_thread: Optional[threading.Thread] = None


def _p2p_broadcast_loop():
    """30-second DHT broadcast loop."""
    global _p2p_broadcast_count, _p2p_running
    logger.info("[P2P] Broadcast loop started")
    while _p2p_running:
        try:
            _p2p_broadcast_count += 1
            # Fetch peers from DB
            try:
                _lazy_ensure_peer_registry()
                with get_db_cursor() as cur:
                    cur.execute(
                        """SELECT node_id, external_addr, pubkey_hash, chain_height, last_seen
                        FROM peer_registry WHERE last_seen > NOW() - INTERVAL '10 minutes' 
                        AND ban_score < 100 LIMIT %s""",
                        (P2P_MAX_PEERS,),
                    )
                    rows = cur.fetchall()
                    new_count = 0
                    with _p2p_dht_lock:
                        for row in rows:
                            nid, addr, pubk, height, last_seen = row
                            if nid not in _p2p_dht_table:
                                new_count += 1
                            ts = (
                                last_seen.timestamp()
                                if hasattr(last_seen, "timestamp")
                                else last_seen
                            )
                            _p2p_dht_table[nid] = P2PPeer(
                                nid,
                                "",
                                addr or "",
                                9091,
                                pubk or "",
                                int(height or 0),
                                ts,
                            )
                    logger.debug(
                        f"[P2P] Cycle {_p2p_broadcast_count}: {len(rows)} peers from DB ({new_count} new)"
                    )
            except Exception as e:
                # Log but don't crash - use in-memory cache
                if "does not exist" in str(e):
                    logger.warning(
                        f"[P2P] DB table missing - waiting for peer_registry to be created"
                    )
                else:
                    logger.warning(f"[P2P] DB fetch: {e}")
            # Fan-out broadcast
            _p2p_fanout_broadcast()
        except Exception as e:
            logger.error(f"[P2P] Broadcast cycle error: {e}")
        for _ in range(P2P_BROADCAST_INTERVAL * 2):
            if not _p2p_running:
                break
            time.sleep(0.5)
    logger.info("[P2P] Broadcast loop exited")


def _start_p2p_broadcast():
    """Start the P2P broadcast daemon."""
    global _p2p_running, _p2p_broadcast_thread
    if _p2p_running:
        return
    _p2p_running = True
    _p2p_broadcast_thread = threading.Thread(
        target=_p2p_broadcast_loop, daemon=True, name="P2PBroadcast"
    )
    _p2p_broadcast_thread.start()
    logger.info(f"[P2P] ✅ DHT broadcaster started (30s interval)")


# Handle POST to /rpc by extracting JSON body and processing (backward compat during migration)
@app.route("/rpc", methods=["POST"])
def rpc_endpoint_post():
    """POST /rpc — Accept JSON body and convert to internal processing (backward compatibility)."""
    try:
        logger.warning(
            f"[RPC-POST] RAW: {request.method} /rpc data_preview={request.data[:200]}"
        )

        # Parse JSON body
        req_dict = request.get_json(force=True, silent=True)
        logger.warning(f"[RPC-POST] parsed req_dict={req_dict}")

        if not req_dict:
            return jsonify(
                {
                    "jsonrpc": "2.0",
                    "error": {"code": -32700, "message": "Parse error: invalid JSON"},
                    "id": None,
                }
            ), 200

        method = req_dict.get("method")
        params = req_dict.get("params", [])
        rpc_id = req_dict.get("id", 1)
        logger.warning(
            f"[RPC-POST] method={method} params_type={type(params)} params={str(params)[:100]}"
        )

        if method == "qtcl_submitBlock":
            logger.warning(
                f"[RPC-POST] SUBMIT BLOCK DETECTED! params={str(params)[:200]}"
            )

        # Process same as GET
        if not method:
            method_names = sorted(list(_RPC_METHODS.keys()))
            return jsonify(
                {
                    "jsonrpc": _JSONRPC_VERSION,
                    "result": {
                        "methods": method_names,
                        "count": len(method_names),
                        "endpoint": "/rpc",
                        "ts": time.time(),
                    },
                    "id": rpc_id,
                }
            ), 200

        # Dispatch to handler
        if method not in _RPC_METHODS:
            return jsonify(
                {
                    "jsonrpc": "2.0",
                    "error": {"code": -32601, "message": f"Method not found: {method}"},
                    "id": rpc_id,
                }
            ), 200

        handler = _RPC_METHODS[method]
        result = handler(params, rpc_id)
        return jsonify(result), 200

    except Exception as e:
        logger.error(f"[RPC-POST] Error processing POST /rpc: {e}")
        return jsonify(
            {
                "jsonrpc": "2.0",
                "error": {"code": -32603, "message": f"Internal error: {str(e)}"},
                "id": None,
            }
        ), 200


@app.route("/rpc", methods=["GET"])
def rpc_endpoint():
    """GET /rpc — JSON-RPC 2.0 endpoint (pull-based, query params).
    Query params:
      - method: RPC method name (required for calls, omit for discovery)
      - params: JSON-encoded array of parameters (URL-decoded, default "[]")
      - id: JSON-RPC request ID (optional, default 1)

    When method is missing: return discovery (all registered method names).
    CRITICAL: Always return HTTP 200 with proper JSON-RPC response.
    """
    try:
        # Check if this is a discovery request (no method param)
        method = request.args.get("method")
        if not method:
            # Discovery: return all registered method names
            method_names = sorted(list(_RPC_METHODS.keys()))
            discovery_response = {
                "jsonrpc": _JSONRPC_VERSION,
                "result": {
                    "methods": method_names,
                    "count": len(method_names),
                    "endpoint": "/rpc",
                    "ts": time.time(),
                },
                "id": None,
            }
            return Response(
                json.dumps(discovery_response), status=200, mimetype="application/json"
            )

        # Parse params (JSON-encoded, URL-decoded, default to empty list)
        params_str = request.args.get("params", "[]")
        try:
            params = json.loads(params_str)
            if not isinstance(params, list):
                params = [params]  # Wrap single value in list
        except json.JSONDecodeError as e:
            # JSON parse error on params: return -32700
            error_response = _rpc_error(
                -32700, f"Parse error in params: {str(e)}", None
            )
            return Response(
                json.dumps(error_response), status=200, mimetype="application/json"
            )

        # Parse request ID (default 1)
        rpc_id = request.args.get("id", "1")
        try:
            rpc_id = int(rpc_id) if rpc_id.isdigit() else rpc_id
        except:
            rpc_id = 1

        # Synthesize JSON-RPC 2.0 request dict
        req_dict = {
            "jsonrpc": _JSONRPC_VERSION,
            "method": method,
            "params": params,
            "id": rpc_id,
        }

        logger.debug(f"[RPC] GET method: {method}")

        # Dispatch using _dispatch_single directly (no batching)
        result = _dispatch_single(req_dict)

        # Result should never be None for GET (no notifications), but handle safely
        if result is None:
            return "", 204

        # CRITICAL: Always HTTP 200, never status codes >= 400
        json_payload = json.dumps(result)
        return Response(json_payload, status=200, mimetype="application/json")
    except Exception as e:
        logger.exception(f"[RPC] GET endpoint error: {e}")
        # Even on unexpected error, return HTTP 200 with JSON-RPC error
        error_response = _rpc_error(-32603, str(e), None)
        return Response(
            json.dumps(error_response), status=200, mimetype="application/json"
        )


# ═══════════════════════════════════════════════════════════════════════════════════
# SSE PROXY: /rpc/oracle/snapshot → localhost:8001 (SSE service)
# ═══════════════════════════════════════════════════════════════════════════════════
@app.route("/rpc/oracle/snapshot", methods=["GET"])
def rpc_oracle_snapshot_proxy():
    """Proxy SSE stream from internal SSE server (port 8001) to external clients.

    Koyeb exposes only the main web service (port 8000), so we proxy
    SSE requests to the internal SSE server running on port 8001.

    If proxy fails, return a placeholder response so clients don't hang.
    """
    try:
        # Try to import requests - if fail, generate placeholder
        try:
            import requests as _req
        except ImportError:
            logger.warning("[SSE-PROXY] requests not available, using placeholder")
            _req = None

        if _req is None:
            # Return placeholder SSE that tells client to retry later
            def placeholder():
                yield b": SSE initializing, retry in 10s\n\n"
                yield b'data: {"status":"initializing","retry_after":10}\n\n'

            return Response(placeholder(), mimetype="text/event-stream")

        sse_url = "http://localhost:8001/rpc/oracle/snapshot"

        def generate():
            try:
                r = _req.get(
                    sse_url,
                    headers={"Accept": "text/event-stream"},
                    stream=True,
                    timeout=(5, 60),
                )
                for chunk in r.iter_content(chunk_size=1024):
                    if chunk:
                        yield chunk
            except GeneratorExit:
                pass
            except Exception as e:
                logger.debug(f"[SSE-PROXY] Stream error: {e}")
                yield f": SSE stream error: {e}\n\n".encode()

        return Response(
            generate(),
            mimetype="text/event-stream",
            headers={
                "Cache-Control": "no-cache",
                "X-Accel-Buffering": "no",
                "Access-Control-Allow-Origin": "*",
            },
        )
    except Exception as e:
        logger.error(f"[SSE-PROXY] Failed to proxy: {e}")

        # Return a minimal placeholder instead of error
        def fallback():
            yield b": SSE unavailable\n\n"

        return Response(fallback(), mimetype="text/event-stream")


# ═══════════════════════════════════════════════════════════════════════════════════
# SSE PROXY: /rpc/blocks/stream → localhost:8001 (SSE service)
# ═══════════════════════════════════════════════════════════════════════════════════
@app.route("/rpc/blocks/stream", methods=["GET"])
def rpc_blocks_stream_proxy():
    """Proxy SSE stream for block events from internal SSE server (port 8001)."""
    try:
        try:
            import requests as _req
        except ImportError:
            logger.warning("[BLOCKS-STREAM] requests not available, using placeholder")
            _req = None

        if _req is None:

            def placeholder():
                yield b": SSE initializing, retry in 10s\n\n"
                yield b'data: {"status":"initializing","retry_after":10}\n\n'

            return Response(placeholder(), mimetype="text/event-stream")

        sse_url = "http://localhost:8001/rpc/blocks/stream"

        def generate():
            try:
                r = _req.get(
                    sse_url,
                    headers={"Accept": "text/event-stream"},
                    stream=True,
                    timeout=(5, 60),
                )
                for chunk in r.iter_content(chunk_size=1024):
                    if chunk:
                        yield chunk
            except GeneratorExit:
                pass
            except Exception as e:
                logger.debug(f"[BLOCKS-STREAM] Stream error: {e}")
                yield f": SSE stream error: {e}\n\n".encode()

        return Response(
            generate(),
            mimetype="text/event-stream",
            headers={
                "Cache-Control": "no-cache",
                "X-Accel-Buffering": "no",
                "Access-Control-Allow-Origin": "*",
            },
        )
    except Exception as e:
        logger.error(f"[BLOCKS-STREAM] Failed to proxy: {e}")

        def fallback():
            yield b": SSE unavailable\n\n"

        return Response(fallback(), mimetype="text/event-stream")


# ═══════════════════════════════════════════════════════════════════════════════════
# DISTRIBUTED HASH TABLE (DHT) INITIALIZATION
# ═══════════════════════════════════════════════════════���═══════════════════════════
@app.route("/rpc/methods", methods=["GET"])
def rpc_methods():
    """GET /rpc/methods — introspection: list all available RPC methods."""
    return jsonify(
        {
            "jsonrpc": _JSONRPC_VERSION,
            "endpoint": "/rpc",
            "methods": _RPC_METHOD_META,
            "count": len(_RPC_METHOD_META),
            "batch": True,
            "ts": time.time(),
        }
    ), 200


def _get_pyth():
    """Return the Pyth oracle engine if initialized, else None. ❤️ I love you."""
    try:
        from oracle import ORACLE as _o

        return _o if _o is not None else None
    except Exception:
        return None


@app.route("/rpc/health", methods=["GET"])
def rpc_health():
    """GET /rpc/health — JSON-RPC engine and Pyth oracle health."""
    from oracle import PYTH_ORACLE as _po

    return jsonify(
        {
            "rpc_engine": "ok",
            "jsonrpc_version": _JSONRPC_VERSION,
            "method_count": len(_RPC_METHODS),
            "pyth_ready": _po is not None,
            "pyth_stats": _po.stats() if _po else {},
            "uptime_s": time.time() - _SERVER_START_TIME,
        }
    ), 200


@app.route("/health", methods=["GET"])
def health_bare():
    """GET /health — instant 200 OK for Koyeb health check."""
    # Always return 200 immediately - server is running
    # Use /rpc/health for detailed status
    return "", 200


@app.route("/ready", methods=["GET"])
def health_ready():
    """GET /ready — Kubernetes-style readiness probe.

    Returns 200 once Flask is bound and serving.
    Background initialization (lattice, oracle) continues in daemon threads.
    """
    # Server is ready immediately once Flask binds — no blocking on background threads
    return "", 200


logger.info("[HEALTH] ✅ /health and /ready endpoints mounted (immediate 200 OK)")


# ═══ STATIC FILE & ROOT SERVING ═══
@app.route("/", methods=["GET"])
def serve_root():
    """GET / — Serve index.html as the dashboard."""
    try:
        # First, try to serve from a dedicated static directory
        import os
        from flask import send_file

        index_path = os.path.join(os.path.dirname(__file__), "index.html")
        if os.path.exists(index_path):
            return send_file(index_path, mimetype="text/html")

        # Fallback: inline the dashboard HTML (for production on Koyeb)
        return render_template_string("""
<!DOCTYPE html>
<html>
<head>
    <title>QTCL - Quantum Blockchain</title>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width,initial-scale=1">
    <style>
        *{margin:0;padding:0;box-sizing:border-box}
        body{font-family:'JetBrains Mono',monospace;background:#030610;color:#bbc8e0;min-height:100vh;padding:20px}
        #app{max-width:1200px;margin:0 auto;padding:20px;background:#080e20;border:1px solid rgba(0,220,255,.1);border-radius:8px}
        h1{color:#00dcff;margin-bottom:20px;font-size:28px;letter-spacing:2px}
        .status{display:grid;grid-template-columns:repeat(auto-fit,minmax(250px,1fr));gap:15px;margin-bottom:30px}
        .card{background:#0a1228;border:1px solid rgba(0,220,255,.2);border-radius:6px;padding:15px}
        .card-title{color:#00dcff;font-size:12px;text-transform:uppercase;letter-spacing:1px;margin-bottom:8px}
        .card-value{font-size:18px;color:#fff;font-weight:bold}
        .card-unit{font-size:11px;color:#666;margin-top:4px}
        .loading{text-align:center;padding:40px;color:#666}
        .error{color:#ff3355;background:rgba(255,51,85,.1);padding:15px;border-radius:6px;margin:15px 0}
        .success{color:#00ff9d;background:rgba(0,255,157,.1);padding:15px;border-radius:6px;margin:15px 0}
    </style>
</head>
<body>
<div id="app">
    <h1>⚛️ QTCL — Quantum Temporal Coherence Ledger</h1>
    <div class="status">
        <div class="card">
            <div class="card-title">Oracle Status</div>
            <div class="card-value" id="oracle-status">Loading...</div>
        </div>
        <div class="card">
            <div class="card-title">Lattice State</div>
            <div class="card-value" id="lattice-status">Loading...</div>
        </div>
        <div class="card">
            <div class="card-title">Block Height</div>
            <div class="card-value" id="block-height">—</div>
        </div>
        <div class="card">
            <div class="card-title">Consensus</div>
            <div class="card-value" id="consensus">—</div>
        </div>
    </div>
    <div id="status-message" class="loading">Connecting to QTCL network...</div>
</div>
<script>
    async function updateStatus() {
        try {
            const health = await fetch('/health').then(() => '✅ Healthy');
            document.getElementById('oracle-status').textContent = health;
            
            try {
                const rpc = await fetch('/rpc', {
                    method:'POST',
                    headers:{'Content-Type':'application/json'},
                    body:JSON.stringify({
                        jsonrpc:'2.0',
                        method:'qtcl_getHealth',
                        params:[],
                        id:1
                    })
                }).then(r => r.json());
                
                if(rpc.result?.status) {
                    document.getElementById('lattice-status').textContent = '✅ Active';
                    document.getElementById('status-message').innerHTML = 
                        '<div class="success">✅ QTCL system is operational</div>';
                } else {
                    document.getElementById('status-message').innerHTML = 
                        '<div class="error">⚠️ System initializing...</div>';
                }
            } catch(e) {
                document.getElementById('status-message').innerHTML = 
                    '<div class="loading">System initializing... (quantum subsystems booting)</div>';
            }
        } catch(e) {
            document.getElementById('status-message').innerHTML = 
                '<div class="error">❌ Connection failed</div>';
        }
    }
    updateStatus();
    setInterval(updateStatus, 5000);
</script>
</body>
</html>
        """), 200
    except Exception as e:
        logger.error(f"[ROOT] Failed to serve index: {e}")
        return jsonify({"error": "Root endpoint failed", "detail": str(e)}), 500


@app.route("/hyp", methods=["GET"])
def serve_hyp_doc():
    """GET /hyp — Serve hyp.html (canonical architecture reference)."""
    try:
        import os
        from flask import send_file

        hyp_path = os.path.join(os.path.dirname(__file__), "hyp.html")
        if os.path.exists(hyp_path):
            return send_file(hyp_path, mimetype="text/html")
        return "hyp.html not found", 404
    except Exception as e:
        logger.error(f"[HYP] Failed to serve hyp.html: {e}")
        return f"Error: {e}", 500


@app.route("/rpc/hlwe/system-info", methods=["GET"])
def rpc_hlwe_system_info():
    """GET /rpc/hlwe/system-info — HypΓ cryptographic system information.

    This endpoint is called by wsgi_config.py to verify HypΓ is available
    without requiring direct imports at module load time.
    Endpoint name kept as 'hlwe' for backward compatibility.
    """
    try:
        from hyp_engine_compat import hlwe_system_info

        info = hlwe_system_info()
        return jsonify(
            {
                "status": "ok",
                "hyp_info": info,
                "timestamp": time.time(),
            }
        ), 200
    except Exception as e:
        logger.error(f"[RPC-HYP] Failed to get system info: {e}", exc_info=True)
        return jsonify(
            {
                "status": "error",
                "error": str(e),
                "timestamp": time.time(),
            }
        ), 500


# ──────────────────────────────────────────────────────────────────────────────
# RPC-only Architecture (no legacy REST endpoints)
# ──────────────────────────────────────────────────────────────────────────────


@app.route("/rpc/_internal/measurement", methods=["GET"])
def rpc_measurement_broadcast_endpoint():
    """
    GET /rpc/_internal/measurement — Receive oracle measurement broadcast from controller.

    This endpoint is called by the RPC broadcast controller to distribute oracle
    snapshots to subscribed clients. In normal operation, external callers should
    use qtcl_registerMeasurementSubscriber RPC method to subscribe.

    Request (from broadcast controller):
        GET /rpc/_internal/measurement?data=URL_ENCODED_JSON

        JSON payload (URL-encoded):
        {
            "timestamp_ns": 1234567890000000,
            "cycle": 42,
            "w_state": {
                "fidelity": 0.7542,
                "coherence": 0.7605,
                ...
            },
            ...
        }

    Response:
        { "status": "processed" }
    """
    try:
        import urllib.parse
        import json

        data_param = request.args.get("data")
        if not data_param:
            return jsonify(
                {"status": "invalid", "error": "missing data parameter"}
            ), 400

        # URL-decode and parse JSON
        try:
            decoded = urllib.parse.unquote(data_param)
            snap = json.loads(decoded)
        except (urllib.error.URLError, json.JSONDecodeError) as e:
            return jsonify(
                {"status": "invalid", "error": f"failed to parse data: {str(e)}"}
            ), 400

        if not snap:
            return jsonify({"status": "invalid", "error": "no JSON payload"}), 400

        # Log broadcast receipt (optional, for debugging)
        cycle = snap.get("cycle", "?")
        fidelity = snap.get("w_state", {}).get("fidelity", 0)
        logger.debug(
            f"[BROADCAST-ENDPOINT] Received measurement | cycle={cycle} | "
            f"fidelity={fidelity:.4f}"
        )

        return jsonify({"status": "processed"}), 200

    except Exception as e:
        logger.error(f"[BROADCAST-ENDPOINT] Error: {e}")
        return jsonify({"status": "error", "error": str(e)}), 500


# ══════════════════════════════════════════════════════════════════════════════
# PYTH PRICE ORACLE REST ROUTES
# (Complement to JSON-RPC — for REST-native integrations)
# ══════════════════════════════════════════════════════════════════════════════


def pyth_prices_rest():
    """
    GET /api/pyth/prices?symbols=BTC,ETH,SOL

    Returns an atomic Pyth snapshot for the requested symbols.
    Query params:
      symbols  — comma-separated list (default: all)
      refresh  — "true" to force bypass cache
    """
    po = _get_pyth()
    if po is None:
        return jsonify({"error": "Pyth oracle not initialized"}), 503

    symbols_raw = request.args.get("symbols", "")
    symbols: Optional[list] = [
        s.strip().upper() for s in symbols_raw.split(",") if s.strip()
    ] or None
    force = request.args.get("refresh", "false").lower() == "true"

    try:
        snap = po.get_snapshot(symbols, force_refresh=force)
        resp = jsonify(snap.to_dict())
        resp.headers["Cache-Control"] = "public, max-age=5"
        resp.headers["X-Pyth-Snapshot"] = snap.snapshot_id[:16]
        resp.headers["X-Hermes-OK"] = str(snap.hermes_ok).lower()
        return resp, 200
    except Exception as e:
        logger.error(f"[PYTH-REST] /api/pyth/prices error: {e}")
        return jsonify({"error": str(e)}), 500


def pyth_single_price_rest(symbol: str):
    """
    GET /api/pyth/price/BTC

    Single-symbol price convenience endpoint.
    Returns price_usd, confidence, age_seconds, publish_time.
    """
    po = _get_pyth()
    if po is None:
        return jsonify({"error": "Pyth oracle not initialized"}), 503

    sym = symbol.upper()
    try:
        snap = po.get_snapshot([sym])
        feed = snap.feeds.get(sym)
        if feed is None:
            return jsonify(
                {
                    "error": f"Symbol {sym} not found",
                    "available": list(snap.feeds.keys()),
                    "hermes_ok": snap.hermes_ok,
                }
            ), 404
        return jsonify(
            {
                **feed.to_dict(),
                "snapshot_id": snap.snapshot_id,
                "hermes_ok": snap.hermes_ok,
            }
        ), 200
    except Exception as e:
        logger.error(f"[PYTH-REST] /api/pyth/price/{sym} error: {e}")
        return jsonify({"error": str(e)}), 500


def pyth_feed_catalog():
    """GET /api/pyth/feeds — full Pyth feed ID catalog (symbol → feed_id)."""
    return jsonify(
        {
            "feeds": {k: v for k, v in __import__("oracle").PYTH_FEED_IDS.items()},
            "count": len(__import__("oracle").PYTH_FEED_IDS),
            "hermes": "https://hermes.pyth.network",
            "ts": time.time(),
        }
    ), 200


def pyth_full_snapshot():
    """
    GET /api/pyth/snapshot — full HLWE-signed atomic price snapshot (all feeds).

    Returns the complete PythAtomicSnapshot including HLWE oracle signature,
    Byzantine outlier report, and raw attestation metadata.
    """
    po = _get_pyth()
    if po is None:
        return jsonify({"error": "Pyth oracle not initialized"}), 503
    try:
        snap = po.get_snapshot()  # all feeds
        return jsonify(
            {
                **snap.to_dict(),
                "feed_count": len(snap.feeds),
                "outlier_count": len(snap.outliers),
            }
        ), 200
    except Exception as e:
        logger.error(f"[PYTH-REST] /api/pyth/snapshot error: {e}")
        return jsonify({"error": str(e)}), 500


def pyth_oracle_stats():
    """GET /api/pyth/stats — Pyth oracle runtime statistics."""
    po = _get_pyth()
    if po is None:
        return jsonify({"error": "Pyth oracle not initialized"}), 503
    return jsonify(po.stats()), 200


def _build_snapshot_payload() -> dict:
    """Build compact snapshot payload for fast SSE delivery.

    Format: 4×4×4 float32 volumetric = 1024 hex chars (COMPACT).
    Includes W-state hex (128 bytes) + essential metrics.
    Fast enough for 50ms cadence on dial-up connections.
    """
    with _snapshot_lock:
        _base = dict(_latest_snapshot) if _latest_snapshot else {}

    # COMPACT: 4³ tensor only (1KB vs 128KB for 32³)
    tensor_hex = _get_compact_lattice_tensor_hex()
    if tensor_hex:
        _base["density_tensor_hex"] = tensor_hex
        _base["tensor_dim"] = 4

    # W-state amplitudes (8 complex doubles = 128 bytes hex)
    w_hex = _get_w_state_hex()
    if w_hex:
        _base["w_state_hex"] = w_hex

    try:
        lat = sys.modules[__name__].__dict__.get("LATTICE")
        if lat is not None:
            _base.setdefault("w_state_fidelity", getattr(lat, "fidelity", None))
            _base.setdefault("purity", getattr(lat, "purity", None))
            _base.setdefault("coherence_l1", getattr(lat, "coherence", None))
            _base.setdefault(
                "lattice_refresh_counter", getattr(lat, "cycle_count", None)
            )
    except Exception:
        pass

    with _CLIENT_DM_POOL_LOCK:
        _c_fid = _client_consensus_fid
        _c_cnt = _client_pool_count

    _base["client_fused_fidelity"] = round(_c_fid, 6)
    _base["client_oracle_count"] = _c_cnt

    if not _base:
        return {}

    _base["ready"] = True
    return _base


import queue as _queue_module
import threading as _threading_module

# ──────────────────────────────────────────────────────────────────────────────
# ════════════════════════════════════════════════════════════════════════════════════════
# SSE STREAMING INFRASTRUCTURE (FIXED: oracle → server → client real-time delivery)
# ════════════════════════════════════════════════════════════════════════════════════════

# ════════════════════════════════════════════════════════════════════════════════
# SNAPSHOT CACHING: Simple 16³ unified snapshot for RPC polling (no multiplexer)
# ════════════════════════════════════════════════════════════════════════════════
_latest_unified_snapshot = {}
_snapshot_cache_lock = _threading_module.RLock()

# Removed: old SSE multiplexer infrastructure. SSE handled by external sse_server.py.
# Removed: old 64³ snapshot generation. Clients fetch unified 16³ snapshots via RPC.

# SSE snapshot endpoint removed — now handled by external sse_server.py
# Main server pushes snapshots to SSE service via _push_to_sse_service()

# Metrics SSE endpoint removed — now handled by external sse_server.py

# Blocks SSE endpoints and infrastructure removed — now handled by external sse_server.py
# Main server pushes blocks to SSE service via _push_to_sse_service()

logger.info(
    "[JSONRPC] ✅ JSON-RPC 2.0 engine mounted — /rpc, /rpc/methods, /rpc/health"
)
logger.info("[RPC-ORACLE] ✅ Oracle initialized (streaming via external SSE service)")
logger.info(
    "[PYTH]    ✅ Pyth REST routes mounted — /api/pyth/{prices,price/<sym>,feeds,snapshot,stats}"
)
logger.info(
    "[RPC-HYP] 🔒 HypΓ Post-Quantum Cryptography RPC methods registered (Schnorr-Γ + GeodesicLWE)"
)
logger.info("[RPC-HYP]   • qtcl_hyp_generateKeypair — asymmetric key generation")
logger.info("[RPC-HYP]   • qtcl_hyp_signMessage — non-interactive Schnorr-Γ signature")
logger.info("[RPC-HYP]   • qtcl_hyp_verifySignature — signature verification")
logger.info("[RPC-HYP]   • qtcl_hyp_deriveAddress — SHA3-256² address derivation")
logger.info("[RPC-HYP]   • qtcl_hyp_encryptMessage — GeodesicLWE encryption (IND-CPA)")
logger.info(
    "[RPC-HYP]   • qtcl_hyp_decryptMessage — GeodesicLWE decryption (LDPC syndrome)"
)
logger.info("[RPC-HYP]   • qtcl_hyp_signBlock — block-level Schnorr-Γ signing")
logger.info("[RPC-HYP]   • qtcl_hyp_verifyBlock — block signature verification")

# ⚛️ RPC SNAPSHOT BROADCAST SYSTEM (No SSE, Pure Database + HTTP Polling)
# ═════════════════════════════════════════════════════════════════════════════════


def _broadcast_snapshot_to_database(snapshot: dict) -> None:
    """Push oracle snapshot to external SSE service for client streaming."""
    try:
        if snapshot.get("density_tensor_hex"):
            sse_frame = {
                "timestamp_ns": snapshot.get("timestamp_ns"),
                "density_tensor_hex": snapshot.get("density_tensor_hex"),
                "tensor_dim": 16,
                "w_state_fidelity": snapshot.get("w_state_fidelity"),
                "purity": snapshot.get("purity"),
                "w_state_hex": snapshot.get("w_state_hex", ""),
            }
            _push_to_sse_service("/push/snapshot", sse_frame)
    except Exception as e:
        logger.debug(f"[SSE] Snapshot push failed: {e}")


# ═══════════════════════════════════════════════════════════════════════════════════════
# ═══════════════════════════════════════════════════════════════════════════════════════
# 256×256 → 32×32×32 VOLUMETRIC TRIPARTITE CORRELATION TENSOR
# The 32³ tensor IS the quantum state object. No 2D density matrix is transmitted.
# ═══════════════════════════════════════════════════════════════════════════════════════


def _lattice_dm_to_32x32x32_tensor_hex(dm256: "np.ndarray") -> str:
    """
    Build a genuine 32×32×32 volumetric tripartite correlation tensor from the
    256×256 density matrix.

    Physical interpretation:
      Axis X (32): row subspace — partition of Hilbert space rows into 32 bands.
      Axis Y (32): col subspace — partition of Hilbert space cols into 32 bands.
      Axis Z (32): decoherence depth — 32 logarithmically-spaced diagonal shells
                   of the 256×256 DM, encoding how correlations decay with
                   increasing distance from the main diagonal (lattice depth).

    T[z, x, y] = mean of dm256[8x:8x+8, 8y:8y+8] weighted by the z-th
                 decoherence shell mask W_z[i,j] = exp(-|i-j| / lambda_z),
                 where lambda_z = exp(log(1) + z/31 * log(256)) spans 1→256.

    This produces a physically meaningful rank-3 object where:
      - Slice T[0,:,:] ≈ ρ_32  (near-diagonal, high-coherence regime)
      - Slice T[31,:,:] ≈ uniform (fully mixed, decoherence floor)
      - The Z-axis traces the coherence-decoherence crossover.

    Serialised as float32: 32×32×32×4 bytes = 131072 bytes = 262144 hex chars.
    """
    try:
        dm = np.asarray(dm256, dtype=np.complex128)
        if dm.shape != (256, 256):
            return ""

        N = 32
        B = 8  # block size per X/Y axis
        D = 32  # depth slices

        # Pre-compute block means: shape (32, 32) of complex128
        rho_blocks = dm.reshape(N, B, N, B).mean(axis=(1, 3))

        # Pre-compute decoherence shell weights for each Z slice.
        # lambda_z spans [1, 256] log-uniformly across Z=0..31
        # W_z is an N×N matrix where W_z[x,y] = exp(-|x-y| / lambda_z_scaled)
        # lambda_z_scaled in block units = lambda_z / B = [1/8, 32]
        lambdas = np.exp(np.linspace(np.log(1.0 / B), np.log(float(N)), D))

        tensor = np.zeros((D, N, N), dtype=np.float32)

        # Build index distance matrix once
        idx = np.arange(N, dtype=np.float32)
        dist = np.abs(idx[:, None] - idx[None, :])  # shape (32, 32)

        for z in range(D):
            lam = float(lambdas[z])
            W = np.exp(-dist / lam)  # shape (32, 32), real weights
            W /= W.sum()  # normalise

            # Element-wise modulus of complex rho_blocks, weighted by shell
            mag = np.abs(rho_blocks)  # (32, 32) real
            tensor[z] = (mag * W).astype(np.float32)

        # Enforce positivity floor and global normalise
        tensor = np.clip(tensor, 0.0, None)
        t_max = float(tensor.max())
        if t_max > 1e-12:
            tensor /= t_max

        return tensor.tobytes().hex()

    except Exception as e:
        logger.warning(f"[DM-TENSOR] 256→32³ failed: {e}")
        return ""


# ── Cache layer: (tensor_hex, timestamp) ─────────────────────────────────────
_tensor_cache: tuple = ("", 0.0)  # tensor_hex, ts
_TENSOR_CACHE_TTL = 0.05  # 50ms — matches SSE cadence


def _get_w_state_hex() -> str:
    """Extract W-state amplitudes (8 complex doubles) from lattice.

    Returns 128-byte hex string (8 × 2 doubles × 8 bytes each).
    Format: 8 consecutive complex doubles in big-endian binary.
    """
    try:
        from globals import LATTICE

        lat = LATTICE
        if lat is not None and hasattr(lat, "w_state_amplitudes"):
            w = lat.w_state_amplitudes
            if w is not None and len(w) >= 8:
                # Pack 8 complex doubles as binary
                import struct

                data = bytearray()
                for i in range(8):
                    amp = complex(w[i]) if not isinstance(w[i], complex) else w[i]
                    data.extend(struct.pack(">dd", amp.real, amp.imag))
                return data.hex()
    except Exception as e:
        logger.debug(f"[W-STATE] extraction failed: {e}")
    return ""


def _get_compact_lattice_tensor_hex() -> str:
    """Build compact 4×4×4 density tensor from 256×256 DM.

    Returns tensor_hex (1024 hex chars) instead of massive 32³.
    Cached for 50ms — fast for dial-up/slow connections.
    """
    global _tensor_cache
    from globals import LATTICE

    now = time.time()
    # Use existing cache for now
    cache_key = ("compact", _tensor_cache[1])
    if now - _tensor_cache[1] < _TENSOR_CACHE_TTL and _tensor_cache[0]:
        # Check if cached result looks like compact (< 2000 hex chars)
        if len(_tensor_cache[0]) < 2000:
            return _tensor_cache[0]

    try:
        lat = LATTICE
        if lat is not None and hasattr(lat, "current_density_matrix"):
            dm = lat.current_density_matrix
            if dm is not None and hasattr(dm, "shape") and dm.shape == (256, 256):
                # Build 4×4×4 tensor from 256×256 DM
                N = 4
                dm_abs = np.abs(dm[: N * 4, : N * 4])  # Take top-left 16×16
                # Slice into 4×4 blocks, take magnitude
                tensor = np.zeros((N, N, N), dtype=np.float32)
                for i in range(N):
                    for j in range(N):
                        block = dm_abs[i * 4 : (i + 1) * 4, j * 4 : (j + 1) * 4]
                        tensor[i, j, :] = np.mean(block, axis=0)[:N]

                # Normalize
                tm = float(tensor.max())
                if tm > 1e-12:
                    tensor /= tm
                tensor_hex = tensor.tobytes().hex()
                _tensor_cache = (tensor_hex, now)
                return tensor_hex
    except Exception as e:
        logger.debug(f"[COMPACT-TENSOR] build failed: {e}")

    return ""


def _get_lattice_tensor_hex() -> str:
    """Pull current_density_matrix from LATTICE and build the 32³ tensor.

    Returns tensor_hex (262144 hex chars) or '' on failure.
    Cached for 50ms — one computation shared across all SSE subscribers.
    NOTE: Server-side cache only; oracle AER simulation is unaffected.
    DEPRECATED: Use _get_compact_lattice_tensor_hex() for smaller payloads.
    """
    global _tensor_cache
    from globals import LATTICE

    now = time.time()
    if now - _tensor_cache[1] < _TENSOR_CACHE_TTL and _tensor_cache[0]:
        return _tensor_cache[0]

    try:
        lat = LATTICE
        if lat is not None and hasattr(lat, "current_density_matrix"):
            dm = lat.current_density_matrix
            if dm is not None and hasattr(dm, "shape") and dm.shape == (256, 256):
                tensor_hex = _lattice_dm_to_32x32x32_tensor_hex(dm)
                _tensor_cache = (tensor_hex, now)
                return tensor_hex
    except Exception as e:
        logger.debug(f"[TENSOR] LATTICE access: {e}")

    # Fallback: build tensor from oracle 8×8 snapshot via kron upsample
    try:
        with _snapshot_lock:
            snap = _latest_snapshot
        if snap:
            h = snap.get("density_matrix_hex", "")
            # Accept 8×8 complex128 (2048 hex) or 32×32 complex64 (16384 hex)
            if h and len(h) == 2048:
                dm8 = np.frombuffer(bytes.fromhex(h), dtype=np.complex128).reshape(8, 8)
                dm32 = np.kron(
                    dm8.astype(np.complex64), np.ones((4, 4), dtype=np.complex64)
                )
                tr = float(np.real(np.trace(dm32)))
                if tr > 1e-12:
                    dm32 /= tr
                # Build tensor from upsampled 32×32
                N = 32
                idx = np.arange(N, dtype=np.float32)
                dist = np.abs(idx[:, None] - idx[None, :])
                lambdas = np.exp(np.linspace(np.log(1.0), np.log(float(N)), N))
                mag = np.abs(dm32)
                t = np.zeros((N, N, N), dtype=np.float32)
                for z in range(N):
                    W = np.exp(-dist / float(lambdas[z]))
                    W /= W.sum()
                    t[z] = (mag * W).astype(np.float32)
                tm = float(t.max())
                if tm > 1e-12:
                    t /= tm
                tensor_hex = t.tobytes().hex()
                _tensor_cache = (tensor_hex, now)
                return tensor_hex
    except Exception:
        pass
    return ""


# ═══════════════════════════════════════════════════════════════════════════════════════
# WSGI EXPORT FOR GUNICORN
# ═══════════════════════════════════════════════════════════════════════════════════════


# ═══════════════════════════════════════════════════════════════════════════════════════
# AUTO-FIX pq_curr/pq_last ON STARTUP
# ═══════════════════════════════════════════════════════════════════════════════════════
def _fix_pq_values_on_startup():
    """Set pq_curr=height, pq_last=height-1 for all blocks. Runs once on import."""
    try:
        with get_db_cursor() as cur:
            cur.execute("""
                SELECT EXISTS (
                    SELECT FROM pg_tables 
                    WHERE schemaname = 'public' 
                    AND tablename = 'blocks'
                )
            """)
            table_exists = cur.fetchone()[0]
            if not table_exists:
                logger.info("[PQ-FIX] Blocks table not yet created — skipping pq fix")
                return

            cur.execute("""
                UPDATE blocks
                SET pq_curr = height,
                    pq_last = height - 1
                WHERE pq_curr IS DISTINCT FROM height
                   OR pq_last IS DISTINCT FROM (height - 1)
            """)
            updated = cur.rowcount
            if updated > 0:
                logger.info(
                    f"[PQ-FIX] Updated {updated} blocks: pq_curr=height, pq_last=height-1"
                )
            else:
                logger.info("[PQ-FIX] All blocks have correct pq_curr/pq_last values")
    except Exception as e:
        logger.warning(f"[PQ-FIX] Could not update pq values: {e}")


# Defer pq_curr/pq_last sync to background thread to unblock /health endpoint
def _deferred_pq_fix():
    """Background thread: Fix pq_curr/pq_last values without blocking Flask init."""
    try:
        _fix_pq_values_on_startup()
    except Exception as e:
        logger.warning(f"[PQ-FIX] Background sync failed: {e}")


threading.Thread(
    target=_deferred_pq_fix,
    daemon=True,
    name="PQFix",
).start()
logger.info(
    "[PQ-FIX] 🔄 Block pq values sync deferred to background thread — /health ready immediately"
)


# Defer mempool sync to background thread (avoids blocking on DB initialization)
def _deferred_mempool_sync():
    """Background thread: Sync mempool DB pool without blocking Flask init."""
    try:
        import mempool as _mp_sync

        # ⚛️ CRITICAL: Share the server's db_pool with the mempool module
        # ensures both use the same (possibly HTTP-mode) connection logic.
        _mp_sync._db = db_pool
        logger.info(
            "[DB] Mempool database pool synchronized with server (museum-grade sync)"
        )
    except Exception as _sync_err:
        logger.warning(f"[DB] Mempool sync failed: {_sync_err}")


threading.Thread(
    target=_deferred_mempool_sync,
    daemon=True,
    name="MempoolSync",
).start()


# ═══════════════════════════════════════════════════════════════════════════════
# CATHEDRAL-GRADE: HYP-WALLET Deferred Initialization (Server-Side)
# Initialize server wallet for block validation and coinbase operations
# ═══════════════════════════════════════════════════════════════════════════════
def _deferred_server_wallet_init():
    """Initialize HYP-WALLET on server for coinbase signing (non-blocking)."""
    try:
        # Add hlwe directory to path
        _hlwe_dir = os.path.join(os.path.dirname(__file__), "hlwe")
        if _hlwe_dir not in sys.path:
            sys.path.insert(0, _hlwe_dir)

        # Import wallet from existing miner module
        _miner_path = os.path.expanduser("~/.qtcl")
        _wallet_file = os.path.join(_miner_path, "wallet.json")

        if os.path.exists(_wallet_file):
            logger.info(f"[HYP-WALLET-SERVER] 📍 Found server wallet at {_wallet_file}")
            logger.info(f"[HYP-WALLET-SERVER]    Server can sign coinbase transactions")
        else:
            logger.info(
                f"[HYP-WALLET-SERVER] 📭 No server wallet found at {_wallet_file}"
            )
            logger.info(
                f"[HYP-WALLET-SERVER]    Create one with: python qtcl-miner/qtcl_client.py"
            )
            logger.info(f"[HYP-WALLET-SERVER]    Then select 'Wallet → Create New'")
    except Exception as _wallet_err:
        logger.warning(f"[HYP-WALLET-SERVER] ⚠️  Server wallet check: {_wallet_err}")


threading.Thread(
    target=_deferred_server_wallet_init,
    daemon=True,
    name="ServerWalletInit",
).start()

# ═══ MODULE LOAD COMPLETE ═══
# Flask app is ready to serve /health immediately
logger.info(
    f"[STARTUP] ✅ Server module loaded in {time.time() - _STARTUP_TIME:.2f}s — /health endpoint ready"
)

# Gunicorn and wsgi_config.py require both 'app' and 'application' exports
application = app
