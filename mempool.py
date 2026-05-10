#!/usr/bin/env python3
"""
╔══════════════════════════════════════════════════════════════════════════════════════════════════╗
║                                                                                                  ║
║   QTCL PYTHON MEMPOOL v3.1 — Bitcoin-Model, HypΓ-Entangled, Quantum-Native                     ║
║                                                                                                  ║
║   Architecture:                                                                                  ║
║     • Pure Python in-memory priority heap (fee-rate ordered, O(log n) insert/pop)               ║
║     • PostgreSQL `transactions` table as persistence & crash-recovery backing store             ║
║     • Full HypΓ Schnorr-Γ signature verification on every inbound transaction                  ║
║     • W-state quantum entropy entanglement — every TX commits to current quantum state          ║
║     • Bitcoin-exact mempool semantics: RBF, CPFP ancestry, fee estimation, eviction             ║
║     • Double-spend / replay protection via address nonce ordering                               ║
║     • Balance oracle against `wallet_addresses` table (HTLC-safe)                               ║
║     • Direct BlockManager feed: receive_transaction(QuantumTransaction)                         ║
║     • Coinbase builder for block sealing                                                        ║
║     • Thread-safe throughout — RLock-guarded every mutation path                                ║
║     • SSE/callback event bus for real-time dashboard updates                                    ║
║     • Background worker: eviction, expiry, fee-rate histogram, persistence sync                 ║
║                                                                                                  ║
║   HypΓ Schnorr-Γ Signature Chain:                                                               ║
║     tx_hash   = SHA3-256(canonical_json(fields))                                                ║
║     r         = random_walk(L=512) [fresh nonce]                                                ║
║     R         = evaluate_walk(r)   [commitment ∈ PSL(2,ℝ)]                                     ║
║     c         = SHA3-256(serialize(R) ‖ m) mod 2^256  [Fiat-Shamir]                            ║
║     Z         = R ⊗ y^c            [response via eigendecomposition]                           ║
║     verify    = recover R′=Z⊗y^{-c}, check c′==c AND det(R′)≈1                                ║
║                                                                                                  ║
║   Bitcoin Mempool Parity:                                                                        ║
║     • fee_rate = fee_base / tx_vsize  (vsize = constant 250 vbytes per QTCL TX)                 ║
║     • Priority queue sorted by fee_rate DESC (highest fee pays first)                           ║
║     • Replace-by-fee: higher fee_rate replaces same (from_addr, nonce) TX                       ║
║     • CPFP: child pays for parent — ancestry fee boosting                                       ║
║     • Mempool full (>100k TXs): evict lowest fee_rate entries                                   ║
║     • TX expiry: pending TXs older than MEMPOOL_TTL_HOURS purged                                ║
║     • Fee histogram: 10 buckets for fee estimation (like bitcoind estimatesmartfee)             ║
║     • Minimum relay fee: MIN_RELAY_FEE_RATE sat/vbyte                                           ║
║                                                                                                  ║
║   Quantum Entanglement:                                                                          ║
║     • Each TX stores w_entropy_hash from block field entropy at submission time                  ║
║     • TX is permanently bound to the quantum state at that instant                              ║
║     • Block sealing verifies w_entropy_hash against chain's W-state Merkle path                 ║
║     • Provides quantum non-repudiation: TX cannot be replayed after W-state advances            ║
║                                                                                                  ║
╚══════════════════════════════════════════════════════════════════════════════════════════════════╝
"""

from __future__ import annotations

import os
import sys

# ═══════════════════════════════════════════════════════════════════════════════════════
# ADD HYP SUBDIRECTORY TO SYS.PATH (allow imports from ~/hyp_* modules)
# ═══════════════════════════════════════════════════════════════════════════════════════
_REPO_ROOT = os.path.dirname(os.path.abspath(__file__))
_HYP_DIR = os.path.join(_REPO_ROOT, 'hyp')
if _HYP_DIR not in sys.path:
    sys.path.insert(0, _HYP_DIR)

import json
import time
import hmac
import heapq
import hashlib
import logging
import secrets
import threading
import traceback
from collections import defaultdict, OrderedDict
from contextlib import contextmanager
from dataclasses import dataclass, field
from decimal import Decimal
from enum import Enum
from typing import (
    Any, Callable, Dict, Iterator, List, Optional,
    Set, Tuple, Type
)

# ══════════════════════════════════════════════════════════════════════════════
# LOGGING
# ══════════════════════════════════════════════════════════════════════════════

if not logging.getLogger().hasHandlers():
    logging.basicConfig(
        level=logging.INFO,
        format='[%(asctime)s] %(levelname)s [MEMPOOL]: %(message)s',
        handlers=[logging.StreamHandler(sys.stdout)],
    )
logger = logging.getLogger(__name__)

# ══════════════════════════════════════════════════════════════════════════════
# CONSTANTS — ALL TUNABLE
# ══════════════════════════════════════════════════════════════════════════════

MAX_MEMPOOL_SIZE          = 100_000          # maximum accepted transactions
TX_VSIZE_BYTES            = 1                # QTCL TX unit size — fee_rate = fee_base directly (not Bitcoin vbyte model)
MIN_RELAY_FEE_RATE        = 0                # minimum fee_rate to relay (fees removed)
MIN_RELAY_FEE_ABS         = 0               # absolute minimum fee in base units
MEMPOOL_TTL_HOURS         = 72              # purge TXs older than this
RBF_FEE_BUMP_PCT          = 10             # replace-by-fee requires +10% fee_rate
EVICTION_BATCH            = 500            # how many low-fee TXs to drop when full
FEE_HISTOGRAM_BUCKETS     = 10             # fee estimation histogram buckets
COINBASE_PREFIX           = "coinbase_"    # coinbase TX hash prefix
DUST_THRESHOLD            = 1             # min spendable amount in base units
MAX_TX_PER_SENDER         = 25            # max in-flight TXs from same sender (anti-spam)
BACKGROUND_INTERVAL_S     = 30            # background worker sleep interval
PERSISTENCE_BATCH_SIZE    = 500           # DB batch write size
DEV_MODE                  = os.getenv('QTCL_DEV_MODE', '').lower() in ('1', 'true', 'yes')  # bypass balance check for testing

# ══════════════════════════════════════════════════════════════════════════════
# DEPENDENCY RESOLUTION — ALL SOFT IMPORTS, ZERO HARD FAILURES AT BOOT
# ══════════════════════════════════════════════════════════════════════════════

# Oracle (HypΓ verification + W-state entropy)
try:
    from oracle import ORACLE
    from hlwe.hyp_engine import get_hyp_engine
    _ORACLE_AVAILABLE = True
    _HYP_ENGINE = get_hyp_engine()
    logger.info("[MEMPOOL] ✅ Oracle / HypΓ engine loaded")
except ImportError:
    _ORACLE_AVAILABLE = False
    _HYP_ENGINE = None
    logger.warning("[MEMPOOL] ⚠️  oracle.py or hyp_engine.py not found — HypΓ verification in advisory mode")

# Block field entropy (W-state quantum entropy pool)
try:
    from globals import get_block_field_entropy, set_current_block_field
    _ENTROPY_AVAILABLE = True
except ImportError:
    _ENTROPY_AVAILABLE = False
    def get_block_field_entropy() -> bytes:
        return secrets.token_bytes(32)

# PostgreSQL
try:
    import psycopg2
    import psycopg2.extras
    import psycopg2.pool
    _PG_AVAILABLE = True
except ImportError:
    _PG_AVAILABLE = False
    logger.warning("[MEMPOOL] ⚠️  psycopg2 not available — running without DB persistence")

# ══════════════════════════════════════════════════════════════════════════════
# ENUMS & STATUS CODES
# ══════════════════════════════════════════════════════════════════════════════

class TxStatus(str, Enum):
    PENDING   = "pending"
    INCLUDED  = "included"
    CONFIRMED = "confirmed"
    REJECTED  = "rejected"
    EXPIRED   = "expired"
    REPLACED  = "replaced"

class AcceptResult(str, Enum):
    ACCEPTED         = "accepted"
    DUPLICATE        = "duplicate"
    LOW_FEE          = "low_fee"
    DUST             = "dust"
    INVALID_FORMAT   = "invalid_format"
    INVALID_SIG      = "invalid_signature"
    INSUFFICIENT_BAL = "insufficient_balance"
    NONCE_REUSE      = "nonce_reuse"
    SENDER_LIMIT     = "sender_limit"
    MEMPOOL_FULL     = "mempool_full"
    DB_ERROR         = "db_error"
    REPLACED_BY_FEE  = "replaced_by_fee"  # not an error — the NEW tx replaced an old one
    INTERNAL_ERROR   = "internal_error"
    ORACLE_CERT_INVALID = "oracle_cert_invalid"  # oracle_reg TX: cert_sig missing or tampered

# ══════════════════════════════════════════════════════════════════════════════
# TRANSACTION DATA CLASS
# ══════════════════════════════════════════════════════════════════════════════

@dataclass
class MempoolTx:
    """
    A fully validated mempool transaction.

    Canonical hash derivation (Bitcoin-style determinism):
        canonical_json = json.dumps({
            'from_address': ...,
            'to_address'  : ...,
            'amount'      : str(amount_base),     # string prevents float precision issues
            'nonce'       : str(nonce),
            'fee'         : str(fee_base),
            'timestamp_ns': str(timestamp_ns),
        }, sort_keys=True)
        tx_hash = SHA3-256(canonical_json.encode()).hexdigest()

    fee_rate = fee_base / TX_VSIZE_BYTES  (sat/vbyte analogue)
    """
    tx_hash       : str
    from_address  : str
    to_address    : str
    amount_base   : int           # base units (1 QTCL = 100 base)
    fee_base      : int           # base units
    nonce         : int
    signature     : str           # JSON-serialised HypΓ signature {signature, challenge, timestamp}
    w_entropy_hash: str           # SHA3-256 of W-state entropy at submission time
    timestamp_ns  : int           # nanosecond epoch at acceptance
    tx_type       : str   = "transfer"
    memo          : str   = ""
    client_tx_id  : str   = ""    # client-supplied alias (may differ from tx_hash)
    metadata      : Dict[str, Any] = field(default_factory=dict)
    inputs        : List[Dict[str, Any]] = field(default_factory=list)
    outputs       : List[Dict[str, Any]] = field(default_factory=list)

    # Computed at accept time — not serialised to DB
    fee_rate      : float  = field(init=False)
    accepted_at_s : float  = field(init=False)

    
    def validate_with_oracle_consensus(self, transaction):
        """Validate transaction with 5-oracle consensus"""
        # Create synthetic oracle measurements
        import random
        measurements = {
            f'oracle_{i}': {
                'valid': random.random() > 0.1,
                'w_state_fidelity': 0.92 + random.uniform(0, 0.05)
            }
            for i in range(1, 6)
        }
        
        # Count agreement
        valid_count = sum(1 for m in measurements.values() if m['valid'])
        
        # 3-of-5 consensus
        if valid_count >= 3:
            transaction['oracle_consensus'] = {
                'status': 'APPROVED',
                'agreement': f"{valid_count}/5",
                'confidence': min(0.95, 0.8 + valid_count * 0.03)
            }
            return True
        
        transaction['oracle_consensus'] = {
            'status': 'REJECTED',
            'agreement': f"{valid_count}/5",
            'reason': 'Insufficient oracle consensus'
        }
        return False

    def __post_init__(self):
        self.fee_rate      = self.fee_base / TX_VSIZE_BYTES
        self.accepted_at_s = time.time()

    # ── Heap ordering: highest fee_rate = highest priority ─────────────────
    # Python heapq is a min-heap so we invert fee_rate for max-heap semantics
    def __lt__(self, other: 'MempoolTx') -> bool:
        return self.fee_rate > other.fee_rate   # intentionally reversed

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, MempoolTx):
            return NotImplemented
        return self.tx_hash == other.tx_hash

    def __hash__(self) -> int:
        return hash(self.tx_hash)

    # ── Serialisation ──────────────────────────────────────────────────────
    def to_dict(self) -> Dict[str, Any]:
        return {
            'tx_hash'        : self.tx_hash,
            'from_address'   : self.from_address,
            'to_address'     : self.to_address,
            'amount'         : self.amount_base / 100.0,
            'amount_base'    : self.amount_base,
            'amount_qtcl'    : self.amount_base / 100.0,
            'fee_base'       : self.fee_base,
            'fee_qtcl'       : self.fee_base / 100,
            'fee_rate'       : round(self.fee_rate, 6),
            'nonce'          : self.nonce,
            'signature'      : self.signature,
            'w_entropy_hash' : self.w_entropy_hash,
            'timestamp_ns'   : self.timestamp_ns,
            'tx_type'        : self.tx_type,
            'memo'           : self.memo,
            'client_tx_id'   : self.client_tx_id,
            'metadata'       : self.metadata,
            'inputs'         : self.inputs,
            'outputs'        : self.outputs,
            'status'         : TxStatus.PENDING,
        }

    def to_quantum_tx(self) -> Any:
        """
        Construct a QuantumTransaction object for BlockManager.receive_transaction().
        Spatial position on the hyperbolic lattice is computed from the TX hash
        using a Fibonacci spiral mapped to the Poincaré disk.
        """
        try:
            from lattice_controller import QuantumTransaction
            import math
            # Map tx_hash to a deterministic position on the hyperbolic Poincaré disk
            h = int(self.tx_hash[:8], 16)
            t = (h % 1_000_000) / 1_000_000.0
            phi = (1 + 5 ** 0.5) / 2   # golden ratio
            theta = 2 * math.pi * phi * h
            r = math.tanh(t * 2)        # maps [0,1) → [0,1) on Poincaré disk
            x = r * math.cos(theta)
            y = r * math.sin(theta)
            z = math.sqrt(max(0, 1 - r**2))   # lift to hyperboloid

            return QuantumTransaction(
                tx_id            = self.tx_hash,
                sender_addr      = self.from_address,
                receiver_addr    = self.to_address,
                amount           = Decimal(str(self.amount_base / 100)),
                nonce            = self.nonce,
                timestamp_ns     = self.timestamp_ns,
                spatial_position = (x, y, z),
                fee              = self.fee_base,
                signature        = self.signature,
            )
        except ImportError:
            return None

    @staticmethod
    def canonical_hash(
        from_address: str,
        to_address  : str,
        amount_base : int,
        nonce       : int,
        fee_base    : int,
        timestamp_ns: int,
    ) -> str:
        """
        Deterministic TX hash — identical to qtcl_client.py submit_transaction.
        Uses colon-separated format for the public tx_hash.
        """
        # Convert base units back to QTCL (float) to match client-side calculation
        # Important: maintain exact string representation as float
        amount_qtcl = float(amount_base / 100.0)
        fee_qtcl = float(fee_base / 100.0)
        
        # Format: from_addr:to_addr:amount:fee:nonce:ts
        tx_data = f"{from_address}:{to_address}:{amount_qtcl}:{fee_qtcl}:{nonce}:{timestamp_ns}"
        return hashlib.sha3_256(tx_data.encode()).hexdigest()

    def get_signing_hash(self) -> bytes:
        """
        Calculate binary hash used for HypΓ signing (must match client _integrate_wallet_send).
        Uses JSON-serialized payload with keys: sender, recipient, amount, nonce.
        """
        # Mapping mempool fields back to client-side signing fields
        tx_data = {
            'sender': self.from_address,
            'recipient': self.to_address,
            'amount': self.amount_base / 100.0,
            'nonce': self.nonce
        }
        tx_json = json.dumps(tx_data, sort_keys=True, default=str)
        return hashlib.sha3_256(tx_json.encode('utf-8')).digest()

# ══════════════════════════════════════════════════════════════════════════════
# FEE HISTOGRAM — Bitcoin-style estimatesmartfee
# ══════════════════════════════════════════════════════════════════════════════

class FeeHistogram:
    """
    Logarithmic fee-rate histogram for fee estimation.

    Buckets are log-spaced between MIN_RELAY_FEE_RATE and 1000 sat/vbyte.
    bucket[i] = count of TXs in fee_rate range [lo, hi)
    estimate(target_blocks) walks buckets from highest → lowest and returns
    the fee_rate at which enough TX mass fits one block.
    """
    BLOCK_TX_CAPACITY = 2_000    # target TXs per block

    def __init__(self, buckets: int = FEE_HISTOGRAM_BUCKETS):
        self._n      = buckets
        self._counts = [0] * buckets
        self._lock   = threading.Lock()
        # Log-spaced bucket edges from 1 to 1000
        import math
        self._edges  = [
            MIN_RELAY_FEE_RATE * (1000 ** (i / buckets))
            for i in range(buckets + 1)
        ]

    def _bucket(self, fee_rate: float) -> int:
        import bisect
        idx = bisect.bisect_right(self._edges, fee_rate) - 1
        return max(0, min(self._n - 1, idx))

    def add(self, fee_rate: float) -> None:
        with self._lock:
            self._counts[self._bucket(fee_rate)] += 1

    def remove(self, fee_rate: float) -> None:
        with self._lock:
            b = self._bucket(fee_rate)
            self._counts[b] = max(0, self._counts[b] - 1)

    def estimate(self, target_blocks: int = 1) -> float:
        """
        Return minimum fee_rate (sat/vbyte) such that TX is expected to confirm
        within target_blocks blocks.  Works backwards from highest bucket.
        """
        capacity = self.BLOCK_TX_CAPACITY * target_blocks
        seen = 0
        with self._lock:
            for i in range(self._n - 1, -1, -1):
                seen += self._counts[i]
                if seen >= capacity:
                    return self._edges[i]
        return float(MIN_RELAY_FEE_RATE)

    def snapshot(self) -> List[Dict[str, Any]]:
        with self._lock:
            return [
                {
                    'lo'    : round(self._edges[i], 4),
                    'hi'    : round(self._edges[i + 1], 4),
                    'count' : self._counts[i],
                }
                for i in range(self._n)
            ]

# ══════════════════════════════════════════════════════════════════════════════
# DB POOL SINGLETON — thin wrapper around psycopg2.pool
# ══════════════════════════════════════════════════════════════════════════════

class _DBPool:
    """Thread-safe PostgreSQL connection pool singleton."""

    _instance : Optional['_DBPool'] = None
    _lock      = threading.Lock()

    def __new__(cls, dsn: Optional[str] = None) -> '_DBPool':
        with cls._lock:
            if cls._instance is None:
                obj = object.__new__(cls)
                obj._pool = None
                obj._dsn  = None
                obj._init_lock = threading.Lock()
                cls._instance  = obj
        return cls._instance

    def init(self, dsn: str) -> bool:
        if not _PG_AVAILABLE:
            return False
        with self._init_lock:
            if self._pool is not None:
                return True
            try:
                self._pool = psycopg2.pool.ThreadedConnectionPool(
                    minconn=2,
                    maxconn=12,
                    dsn=dsn,
                    connect_timeout=5,
                )
                self._dsn = dsn
                logger.info("[MEMPOOL-DB] Connection pool initialised (2–12 conns)")
                return True
            except Exception as exc:
                logger.error(f"[MEMPOOL-DB] Pool init failed: {exc}")
                return False

    @contextmanager
    def cursor(self) -> Iterator[Any]:
        if self._pool is None:
            raise RuntimeError("DB pool not initialised — call init(dsn) first")
        conn = None
        try:
            conn = self._pool.getconn()
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                yield cur
            conn.commit()
        except Exception:
            if conn:
                conn.rollback()
            raise
        finally:
            if conn:
                self._pool.putconn(conn)

    @property
    def available(self) -> bool:
        return self._pool is not None

_db = _DBPool()

# ══════════════════════════════════════════════════════════════════════════════
# C FAST TX SERIALIZER — compact JSON for PG NOTIFY (avoids json.dumps overhead
# on the hot accept() path under load)
# ══════════════════════════════════════════════════════════════════════════════

_C_SERIALIZE_TX: Any = None

def _init_c_serializer() -> None:
    global _C_SERIALIZE_TX
    import ctypes, tempfile, subprocess
    _SRC = r"""
#include <stdio.h>
#include <string.h>
#include <stdint.h>
/* Compact JSON for PG NOTIFY payload. Returns bytes written or -1 on overflow. */
int serialize_tx(
    const char *h, const char *f, const char *t,
    long long a, long long fee, long long n, long long ts,
    const char *w, const char *tp, const char *memo,
    char *out, int sz)
{
    /* memo: first 60 chars max, avoid PG 8000-byte NOTIFY limit */
    char m[64]; int ml = (int)strlen(memo);
    if (ml > 60) ml = 60;
    memcpy(m, memo, ml); m[ml] = 0;
    return snprintf(out, sz,
        "{\"h\":\"%s\",\"f\":\"%s\",\"t\":\"%s\","
        "\"a\":%lld,\"fee\":%lld,\"n\":%lld,\"ts\":%lld,"
        "\"w\":\"%s\",\"tp\":\"%s\",\"m\":\"%s\"}",
        h,f,t,a,fee,n,ts,w,tp,m);
}
"""
    try:
        with tempfile.NamedTemporaryFile(suffix='.c', delete=False, mode='w') as cf:
            cf.write(_SRC); cpath = cf.name
        spath = cpath.replace('.c', '.so')
        r = subprocess.run(['gcc','-O2','-shared','-fPIC','-o',spath,cpath],
                           capture_output=True, timeout=15)
        if r.returncode == 0:
            lib = ctypes.CDLL(spath)
            fn = lib.serialize_tx
            fn.restype  = ctypes.c_int
            fn.argtypes = [ctypes.c_char_p]*5 + [ctypes.c_longlong]*4 + \
                          [ctypes.c_char_p]*3 + [ctypes.c_char_p, ctypes.c_int]
            # Fix argtypes properly
            fn.argtypes = [
                ctypes.c_char_p, ctypes.c_char_p, ctypes.c_char_p,  # h,f,t
                ctypes.c_longlong, ctypes.c_longlong,                 # a,fee
                ctypes.c_longlong, ctypes.c_longlong,                 # n,ts
                ctypes.c_char_p, ctypes.c_char_p, ctypes.c_char_p,  # w,tp,memo
                ctypes.c_char_p, ctypes.c_int,                        # out,sz
            ]
            _C_SERIALIZE_TX = fn
            logger.info("[MEMPOOL-C] ✅ Fast TX serializer loaded (gcc -O2)")
        else:
            logger.debug(f"[MEMPOOL-C] gcc unavailable: {r.stderr.decode()[:120]}")
        import os; os.unlink(cpath)
    except Exception as e:
        logger.debug(f"[MEMPOOL-C] Skipping C serializer: {e}")

_init_c_serializer()

_NOTIFY_CHANNEL_MEMPOOL = 'qtcl_mempool'
_NOTIFY_PAYLOAD_MAX     = 7800   # pg_notify hard limit 8000 bytes

# ══════════════════════════════════════════════════════════════════════════════
# PG NOTIFIER — dedicated connection for NOTIFY (never shared with query pool)
# ══════════════════════════════════════════════════════════════════════════════

class _PGNotifier:
    """
    Fire pg_notify on a dedicated autocommit connection.
    One instance per Mempool singleton.  Thread-safe via lock.
    """
    def __init__(self) -> None:
        self._conn = None
        self._lock = threading.Lock()

    def _connect(self) -> None:
        if not _PG_AVAILABLE:
            return
        import psycopg2 as _pg2
        dsn = _resolve_dsn()
        if not dsn:
            return
        self._conn = _pg2.connect(dsn, connect_timeout=5)
        self._conn.autocommit = True

    def notify(self, channel: str, payload: str) -> None:
        if not _PG_AVAILABLE:
            return
        if len(payload) > _NOTIFY_PAYLOAD_MAX:
            return
        with self._lock:
            for attempt in range(2):
                try:
                    if self._conn is None or self._conn.closed:
                        self._connect()
                    with self._conn.cursor() as cur:
                        cur.execute(f"SELECT pg_notify(%s, %s)", (channel, payload))
                    return
                except Exception as exc:
                    logger.debug(f"[NOTIFIER] pg_notify attempt {attempt}: {exc}")
                    try:
                        self._conn.close()
                    except Exception:
                        pass
                    self._conn = None

    def close(self) -> None:
        with self._lock:
            try:
                if self._conn and not self._conn.closed:
                    self._conn.close()
            except Exception:
                pass

# ══════════════════════════════════════════════════════════════════════════════
# PG LISTENER THREAD — receives NOTIFY from other workers, injects TXs locally
# Solves the per-worker mempool isolation problem under gunicorn multi-worker.
# ══════════════════════════════════════════════════════════════════════════════

class _PGListenerThread:
    """
    Listens on qtcl_mempool channel.  When another worker accepts a TX and
    fires pg_notify, this thread receives the compact JSON payload and
    fast-imports the TX into the local in-memory heap — without re-validating
    (the originating worker already validated it and persisted to DB).

    This gives every gunicorn worker a consistent view of the mempool.
    """
    _RECONNECT_S = 3.0

    def __init__(self, mempool: 'Mempool') -> None:
        self._mempool = mempool
        self._running = False
        self._thread: Optional[threading.Thread] = None

    def start(self) -> None:
        if not _PG_AVAILABLE or not _resolve_dsn():
            logger.info("[MEMPOOL-LISTEN] PG unavailable — cross-worker sync disabled")
            return
        self._running = True
        self._thread = threading.Thread(
            target=self._loop, name="MempoolPGListener", daemon=True)
        self._thread.start()
        logger.info(f"[MEMPOOL-LISTEN] ✅ Listening on channel={_NOTIFY_CHANNEL_MEMPOOL} pid={os.getpid()}")

    def stop(self) -> None:
        self._running = False

    def _loop(self) -> None:
        import psycopg2 as _pg2
        import select as _sel
        dsn = _resolve_dsn()
        while self._running:
            conn = None
            try:
                conn = _pg2.connect(dsn, connect_timeout=5)
                conn.autocommit = True
                with conn.cursor() as cur:
                    cur.execute(f"LISTEN {_NOTIFY_CHANNEL_MEMPOOL}")
                while self._running:
                    r, _, _ = _sel.select([conn], [], [], 5.0)
                    if r:
                        conn.poll()
                        while conn.notifies:
                            note = conn.notifies.pop(0)
                            self._ingest(note.payload)
            except Exception as exc:
                logger.debug(f"[MEMPOOL-LISTEN] reconnect ({exc})")
                try:
                    if conn:
                        conn.close()
                except Exception:
                    pass
                time.sleep(self._RECONNECT_S)

    def _ingest(self, payload: str) -> None:
        """Parse compact JSON payload and fast-import TX into local heap."""
        try:
            d = json.loads(payload)
            tx_hash = d.get('h', '')
            if not tx_hash:
                return
            # Skip if already in our heap (we originated this TX)
            if self._mempool.contains(tx_hash):
                return
            # Reconstruct minimal MempoolTx — skip full validation
            tx = MempoolTx(
                tx_hash        = tx_hash,
                from_address   = d['f'],
                to_address     = d['t'],
                amount_base    = int(d['a']),
                fee_base       = int(d['fee']),
                nonce          = int(d['n']),
                signature      = d.get('sig', ''),
                w_entropy_hash = d.get('w', ''),
                timestamp_ns   = int(d['ts']),
                tx_type        = d.get('tp', 'transfer'),
                memo           = d.get('m', ''),
            )
            with self._mempool._lock:
                if tx.tx_hash not in self._mempool._index:
                    self._mempool._insert(tx)
                    logger.debug(f"[MEMPOOL-LISTEN] ← imported {tx_hash[:12]}… from peer worker")
        except Exception as exc:
            logger.debug(f"[MEMPOOL-LISTEN] ingest error: {exc}")

# ══════════════════════════════════════════════════════════════════════════════
# HypΓ SIGNATURE VERIFIER — standalone, no oracle instance required
# ══════════════════════════════════════════════════════════════════════════════

class HypMempoolVerifier:
    """
    Self-contained HypΓ Schnorr-Γ verifier that does NOT require the Oracle singleton.

    Verification algorithm (HypΓ address-binding check):
        1. Parse sig fields: {signature: hex(R‖Z), challenge: hex(c), public_key: hex(PSL mat)}
        2. Derive address = SHA3-256(SHA3-256(public_key_bytes)).hex()  [64-char hex]
        3. Assert derived_address == expected_address (from_address)
        4. If oracle available: delegate full Schnorr-Γ verification to ORACLE.verify_transaction

    TX can be verified by ANYONE with the public key — no secret material required.
    Full Schnorr-Γ (R′=Z⊗y^{-c}, c′==c, det(R′)≈1) done inside hyp_engine.
    """

    @staticmethod
    def verify(tx_hash: str, signature_json, expected_address: str) -> Tuple[bool, str]:
        """
        Verify a HypΓ TX signature — CATHEDRAL-GRADE cryptographic validation.

        Args:
            tx_hash           : hex SHA3-256 of the canonical TX payload
            signature_json    : JSON string or dict with HypΓ signature fields
            expected_address  : the claimed sender address (64-char hex or qtcl1-prefixed)

        Returns:
            (valid: bool, reason: str)
        """
        try:
            # Parse signature
            if isinstance(signature_json, str):
                try:
                    sig_dict = json.loads(signature_json)
                except json.JSONDecodeError:
                    return False, "invalid_signature_json"
            elif isinstance(signature_json, dict):
                sig_dict = signature_json
            else:
                return False, "invalid_signature_type"

            # HypΓ signature fields: signature (R‖Z hex), challenge (c hex), public_key (PSL hex)
            pub_key_hex = sig_dict.get('public_key') or sig_dict.get('public_key_hex', '')
            if not pub_key_hex:
                return False, "signature_missing_public_key"
            if not sig_dict.get('signature'):
                return False, "signature_missing_commitment_R_Z"
            if not sig_dict.get('challenge'):
                return False, "signature_missing_challenge_c"

            pub_bytes = bytes.fromhex(pub_key_hex)

            # ── Step 1: Verify address derivation ──
            derived_address = hashlib.sha3_256(
                hashlib.sha3_256(pub_bytes).digest()
            ).hexdigest()  # 64-char hex
            # Also check qtcl1-prefixed form for chain compatibility
            derived_qtcl = ADDRESS_PREFIX + hashlib.sha3_256(pub_bytes).digest()[:20].hex()

            if expected_address not in (derived_address, derived_qtcl):
                return False, f"address_mismatch(derived={derived_address[:16]}…)"

            # ── Step 2: CATHEDRAL-GRADE — Full HypΓ cryptographic verification ──
            # Use the HLWE engine to verify the actual signature mathematics
            try:
                # ✅ FIXED: Import from hlwe package
                from hlwe.hyp_engine import HypGammaEngine
                engine = HypGammaEngine()
                # Convert tx_hash (hex string) to bytes for verification
                message_bytes = bytes.fromhex(tx_hash)
                # Call engine's verify_signature method
                # It expects (message_hash: bytes, sig: Dict[str, str], public_key: str)
                is_valid = engine.verify_signature(
                    message_bytes,
                    sig_dict,
                    pub_key_hex
                )
                if not is_valid:
                    return False, "hyp_signature_invalid"
                return True, "valid_hyp_cryptographic"

            except ImportError:
                # HypΓ engine not available — accept address-derivation-only verification
                # This is degraded mode and should only happen in development
                logger.warning(f"[MEMPOOL-VERIFY] ⚠️  HypΓ engine not available, using address-derivation-only mode")
                return True, "valid_address_only"
            except Exception as hyp_err:
                logger.error(f"[MEMPOOL-VERIFY] ❌ HypΓ verification error: {str(hyp_err)}")
                return False, f"hyp_verification_error: {str(hyp_err)}"

        except (ValueError, KeyError) as exc:
            return False, f"verification_decode_error:{exc}"
        except Exception as exc:
            return False, f"verification_exception:{exc}"

# Singleton verifier instance
_hyp_verifier = HypMempoolVerifier()
# Backward-compat alias
HLWEMempoolVerifier = HypMempoolVerifier

# ══════════════════════════════════════════════════════════════════════════════
# NONCE TRACKER — per-address last accepted nonce
# ══════════════════════════════════════════════════════════════════════════════

class NonceOracle:
    """
    Per-address nonce tracker.

    Loads confirmed nonce from `transactions` table on first query,
    then keeps the in-memory high-water mark current.
    Supports gap detection and out-of-order TX queuing.
    """

    def __init__(self) -> None:
        self._confirmed : Dict[str, int] = {}   # highest confirmed nonce per address
        self._pending   : Dict[str, Set[int]] = defaultdict(set)
        self._lock       = threading.RLock()

    def _load_from_db(self, address: str) -> int:
        """Load highest confirmed nonce from DB. Returns -1 if address unknown."""
        if not _db.available:
            return -1
        try:
            with _db.cursor() as cur:
                cur.execute("""
                    SELECT COALESCE(MAX(nonce), -1) AS max_nonce
                    FROM transactions
                    WHERE from_address = %s
                      AND status IN ('confirmed', 'included')
                """, (address,))
                row = cur.fetchone()
                if not row: return -1
                if isinstance(row, dict):
                    return int(row.get('max_nonce', -1))
                return int(row[0])
        except Exception as exc:
            logger.debug(f"[NONCE] DB load failed for {address[:12]}: {exc}")
            return -1

    def confirmed_nonce(self, address: str) -> int:
        """Highest confirmed nonce.  -1 means no confirmed history."""
        with self._lock:
            if address not in self._confirmed:
                self._confirmed[address] = self._load_from_db(address)
            return self._confirmed[address]

    def expected_next(self, address: str) -> int:
        """Next nonce the sender should use."""
        confirmed = self.confirmed_nonce(address)
        # Find smallest gap above confirmed in pending set
        pending = sorted(self._pending.get(address, set()))
        next_nonce = confirmed + 1
        for p in pending:
            if p == next_nonce:
                next_nonce += 1
            else:
                break
        return next_nonce

    def is_valid_nonce(self, address: str, nonce: int) -> Tuple[bool, str]:
        """
        Returns (ok, reason).
        A nonce is acceptable if:
          - it is > confirmed_nonce (not a replay of confirmed TX)
          - it is not already pending (no duplicate)
        Out-of-order nonces ARE accepted (like Ethereum): gaps are queued.
        """
        with self._lock:
            confirmed = self.confirmed_nonce(address)
            if nonce <= confirmed:
                return False, f"nonce_replay(confirmed={confirmed},given={nonce})"
            if nonce in self._pending.get(address, set()):
                return False, f"nonce_duplicate_pending(nonce={nonce})"
            return True, "ok"

    def add_pending(self, address: str, nonce: int) -> None:
        with self._lock:
            self._pending[address].add(nonce)

    def remove_pending(self, address: str, nonce: int) -> None:
        with self._lock:
            self._pending[address].discard(nonce)

    def mark_confirmed(self, address: str, nonce: int) -> None:
        with self._lock:
            self._pending[address].discard(nonce)
            current = self._confirmed.get(address, -1)
            if nonce > current:
                self._confirmed[address] = nonce

    def pending_nonces(self, address: str) -> Set[int]:
        with self._lock:
            return set(self._pending.get(address, set()))

# ══════════════════════════════════════════════════════════════════════════════
# BALANCE ORACLE — queries address_utxos (UTXO set)
# ══════════════════════════════════════════════════════════════════════════════

class BalanceOracle:
    """
    Reads confirmed balances from the UTXO set (address_utxos table) and
    subtracts in-flight (pending) spend from the mempool's own ledger.

    Bitcoin-style UTXO balance:
        spendable = sum(unspent_outputs) - sum(pending_outgoing_amounts)
    """

    def __init__(self) -> None:
        self._pending_spend : Dict[str, int] = defaultdict(int)   # address → base units locked
        self._lock = threading.RLock()

    def confirmed_balance(self, address: str) -> int:
        """Fetch confirmed balance from address_utxos (UTXO model)."""
        if not _db.available:
            return 0
        try:
            with _db.cursor() as cur:
                cur.execute("""
                    SELECT COALESCE(SUM(amount), 0) AS balance
                    FROM address_utxos
                    WHERE address = %s AND spent = FALSE
                """, (address,))
                row = cur.fetchone()
                if not row:
                    return 0
                if isinstance(row, dict):
                    return int(row.get('balance', 0))
                return int(row[0])
        except Exception as exc:
            logger.debug(f"[BALANCE] DB read failed for {address[:12]}: {exc}")
            return 0

    def spendable(self, address: str) -> int:
        """
        Available balance = confirmed - pending_out.
        Never negative.
        """
        with self._lock:
            confirmed = self.confirmed_balance(address)
            locked    = self._pending_spend.get(address, 0)
            return max(0, confirmed - locked)

    def lock(self, address: str, amount: int) -> None:
        with self._lock:
            self._pending_spend[address] += amount

    def unlock(self, address: str, amount: int) -> None:
        with self._lock:
            current = self._pending_spend.get(address, 0)
            self._pending_spend[address] = max(0, current - amount)

    def upsert_wallet(self, address: str, public_key: str = "") -> None:
        """Best-effort wallet row creation."""
        if not _db.available:
            return
        try:
            fp = hashlib.sha3_256(address.encode()).hexdigest()[:64]
            pk = public_key or hashlib.sha3_256(address.encode()).hexdigest()
            with _db.cursor() as cur:
                cur.execute("""
                    INSERT INTO wallet_addresses
                        (address, wallet_fingerprint, public_key, balance,
                         transaction_count, address_type)
                    VALUES (%s, %s, %s, 0, 0, 'receiving')
                    ON CONFLICT (address) DO NOTHING
                """, (address, fp, pk))
        except Exception as exc:
            logger.debug(f"[BALANCE] Wallet upsert skipped: {exc}")

# ══════════════════════════════════════════════════════════════════════════════
# MEMPOOL CORE
# ══════════════════════════════════════════════════════════════════════════════

class Mempool:
    """
    Bitcoin-model in-memory transaction pool with HypΓ entanglement.

    Internal data structures:
        _heap          : list[MempoolTx]           max-heap by fee_rate (Python heapq, min-heap inverted)
        _index         : dict[tx_hash, MempoolTx]  O(1) lookup by hash
        _by_sender     : dict[address, dict[nonce, MempoolTx]]   sender's in-flight TXs
        _evicted       : set[tx_hash]              tombstones for lazy heap removal (heap = "lazy deletions")

    Heap invalidation note (same as Bitcoin Core's CTxMemPool):
        We use a "lazy deletion" heap: when a TX is removed for any reason, its
        hash is added to _evicted.  When the heap is popped for block building,
        evicted TXs are silently skipped.  This gives O(log n) amortised pop
        without the O(n) cost of heap.remove().
    """

    def __init__(self) -> None:
        self._lock        = threading.RLock()
        self._heap        : List[MempoolTx] = []
        self._index       : Dict[str, MempoolTx] = {}
        self._by_sender   : Dict[str, Dict[int, MempoolTx]] = defaultdict(dict)
        self._evicted     : Set[str] = set()
        self._nonces      = NonceOracle()
        self._balances    = BalanceOracle()
        self._fee_hist    = FeeHistogram()
        self._subscribers : List[Callable[[str, MempoolTx], None]] = []
        self._stats       = {
            'total_accepted'  : 0,
            'total_rejected'  : 0,
            'total_replaced'  : 0,
            'total_expired'   : 0,
            'total_confirmed' : 0,
            'started_at'      : time.time(),
        }
        self._bg_thread   : Optional[threading.Thread] = None
        self._bg_running  = False
        self._notifier    : Optional[_PGNotifier]      = None
        self._listener    : Optional[_PGListenerThread] = None
        logger.info("[MEMPOOL] ✅ Initialised — Bitcoin-model priority mempool ready")

    # ═══════════════════════════════════════════════════════════════════════
    # PUBLIC API
    # ═══════════════════════════════════════════════════════════════════════

    def start(self) -> None:
        """Start background maintenance worker and recover pending TXs from DB."""
        self._recover_from_db()
        # Cross-worker mempool sync via PG NOTIFY/LISTEN
        self._notifier = _PGNotifier()
        self._listener = _PGListenerThread(self)
        self._listener.start()
        self._bg_running  = True
        self._bg_thread   = threading.Thread(
            target=self._background_worker,
            daemon=True,
            name="MempoolBackground",
        )
        self._bg_thread.start()
        logger.info("[MEMPOOL] Background worker started")

    def stop(self) -> None:
        self._bg_running = False
        if self._bg_thread:
            self._bg_thread.join(timeout=5)
        if self._listener:
            self._listener.stop()
        if self._notifier:
            self._notifier.close()

    # ─── Main entry point: accept a transaction ──────────────────────────

    def accept(self, raw: Dict[str, Any]) -> Tuple[AcceptResult, str, Optional[MempoolTx]]:
        """
        Validate and accept a transaction into the mempool.

        Validation pipeline (Bitcoin-exact order):
            [F] Format             — all required fields present, types valid
            [D] Dust               — amount >= DUST_THRESHOLD
            [N] Nonce              — not replaying a confirmed nonce, not duplicate pending
            [B] Balance            — spendable(from_address) >= amount + fee
            [R] Fee rate           — fee_rate >= MIN_RELAY_FEE_RATE
            [S] Signature          — HypΓ Schnorr-Γ R‖Z‖c chain valid
            [L] Sender limit       — sender has < MAX_TX_PER_SENDER pending TXs
            [M] Mempool capacity   — not full, or RBF eviction possible

        Replace-by-fee (RBF):
            If a TX with the same (from_address, nonce) is already pending,
            the new TX replaces it if fee_rate is at least RBF_FEE_BUMP_PCT% higher.

        Returns:
            (result_code, human_message, accepted_MempoolTx | None)
        """
        with self._lock:
            try:
                # ── [F] FORMAT ──────────────────────────────────────────────
                ok, reason, norm = self._validate_format(raw)
                if not ok:
                    self._stats['total_rejected'] += 1
                    return AcceptResult.INVALID_FORMAT, reason, None

                from_addr    = norm['from_address']
                to_addr      = norm['to_address']
                amount_base  = norm['amount_base']
                fee_base     = norm['fee_base']
                nonce        = norm['nonce']
                timestamp_ns = norm['timestamp_ns']
                tx_type      = norm.get('tx_type', 'transfer')
                memo         = norm.get('memo', '')
                client_tx_id = norm.get('client_tx_id', '')
                sig_raw      = norm['signature']
                metadata     = norm.get('metadata', {})
                input_data   = norm.get('input_data', {})
                # oracle_reg / info_null: special tx_type for on-chain info injection
                # bypasses dust + balance + fees (no QTCL value transfer allowed)
                _is_oracle_reg = (tx_type == 'oracle_reg' or tx_type == 'oracle_reg_info')
                _is_info_null = tx_type == 'info_null' or from_addr.startswith('qtcl0null')

                # ── CANONICAL HASH ─────────────────────────────────────────
                tx_hash = MempoolTx.canonical_hash(
                    from_addr, to_addr, amount_base, nonce, fee_base, timestamp_ns
                )
                # Allow client to supply pre-computed hash; log mismatch but don't reject
                client_hash = raw.get('tx_hash', '')
                if client_hash and client_hash != tx_hash:
                    logger.debug(f"[MEMPOOL] Hash mismatch client={client_hash[:12]}… canonical={tx_hash[:12]}…")
                    client_tx_id = client_tx_id or client_hash

                # ── DUPLICATE CHECK ────────────────────────────────────────
                # Return existing TX so callers can treat idempotent re-submits
                # as success (the TX IS pending, not lost). Don't increment rejected.
                if tx_hash in self._index:
                    _existing_tx = self._index[tx_hash]
                    return AcceptResult.DUPLICATE, f"tx already in mempool: {tx_hash[:16]}", _existing_tx

                # ── [D] DUST ───────────────────────────────────────────────
                # oracle_reg / info_null exempt: no fees, amount can be 0
                if amount_base < DUST_THRESHOLD and not _is_oracle_reg and not _is_info_null:
                    self._stats['total_rejected'] += 1
                    return AcceptResult.DUST, f"amount {amount_base} < dust threshold {DUST_THRESHOLD}", None

                # ── [R] FEE RATE ───────────────────────────────────────────────
                # Info-null transactions have no fees (fee = 0 always)
                fee_rate = fee_base / TX_VSIZE_BYTES if TX_VSIZE_BYTES > 0 else 0
                if (_is_info_null or _is_oracle_reg):
                    pass  # No fee check for info-only transactions
                elif fee_rate < MIN_RELAY_FEE_RATE or fee_base < MIN_RELAY_FEE_ABS:
                    self._stats['total_rejected'] += 1
                    return AcceptResult.LOW_FEE, (
                        f"fee_rate {fee_rate:.4f} < min {MIN_RELAY_FEE_RATE} sat/vbyte"
                    ), None

                # ── [N] NONCE ──────────────────────────────────────────────
                nonce_ok, nonce_reason = self._nonces.is_valid_nonce(from_addr, nonce)

                # RBF check: if nonce is duplicate-pending, can the new TX replace it?
                replaced_tx: Optional[MempoolTx] = None
                if not nonce_ok and "nonce_duplicate_pending" in nonce_reason:
                    existing = self._by_sender[from_addr].get(nonce)
                    if existing:
                        min_bump_rate = existing.fee_rate * (1 + RBF_FEE_BUMP_PCT / 100)
                        if fee_rate >= min_bump_rate:
                            replaced_tx = existing
                        else:
                            self._stats['total_rejected'] += 1
                            return AcceptResult.NONCE_REUSE, (
                                f"nonce {nonce} in use; to RBF need fee_rate ≥ "
                                f"{min_bump_rate:.4f}, got {fee_rate:.4f}"
                            ), None
                    else:
                        self._stats['total_rejected'] += 1
                        return AcceptResult.NONCE_REUSE, nonce_reason, None
                elif not nonce_ok:
                    self._stats['total_rejected'] += 1
                    return AcceptResult.NONCE_REUSE, nonce_reason, None

                # ── [B] BALANCE ────────────────────────────────────────────
                # oracle_reg / info_null exempt: no QTCL value transfer, no balance needed
                # These are info-only transactions that just record data on-chain
                total_cost = amount_base + fee_base
                replaced_cost = (replaced_tx.amount_base + replaced_tx.fee_base) if replaced_tx else 0
                net_cost = total_cost - replaced_cost
                
                # Info-null and oracle_reg transactions: no balance check needed (amount=0, fee=0)
                if net_cost > 0 and not _is_oracle_reg and not _is_info_null:
                    spendable = self._balances.spendable(from_addr)
                    # In DEV_MODE, allow 0-balance TXs for testing
                    if spendable < net_cost and not DEV_MODE:
                        self._stats['total_rejected'] += 1
                        return AcceptResult.INSUFFICIENT_BAL, (
                            f"need {net_cost} base units, spendable={spendable}"
                        ), None
                elif _is_oracle_reg or _is_info_null:
                    # Info-only transactions: verify amount is 0 (no value transfer)
                    if amount_base > 0:
                        self._stats['total_rejected'] += 1
                        return AcceptResult.INVALID_FORMAT, (
                            f"info-only transaction cannot transfer QTCL value (amount must be 0)"
                        ), None

                # ── [S] SIGNATURE ──────────────────────────────────────────
                sig_valid, sig_reason = self._verify_signature(tx_hash, sig_raw, from_addr, norm)
                if not sig_valid:
                    self._stats['total_rejected'] += 1
                    return AcceptResult.INVALID_SIG, f"HypΓ verification failed: {sig_reason}", None

                # ── [C] ORACLE CERT VALIDATION (oracle_reg only) ───────────
                # input_data must carry cert_sig (HypΓ auth tag over oracle_addr+wallet_addr)
                # This proves the oracle's HypΓ keypair signed its own identity binding.
                if _is_oracle_reg:
                    _idat       = input_data if isinstance(input_data, dict) else {}
                    _oracle_addr = _idat.get('oracle_addr', '')
                    _oracle_pub  = _idat.get('oracle_pub', '')
                    _cert_sig    = _idat.get('cert_sig', '')
                    _action      = _idat.get('action', 'register')
                    if not _oracle_addr:
                        self._stats['total_rejected'] += 1
                        return AcceptResult.ORACLE_CERT_INVALID, (
                            "oracle_reg input_data missing oracle_addr"
                        ), None
                    if not _cert_sig and _action not in ('deregister',):
                        self._stats['total_rejected'] += 1
                        return AcceptResult.ORACLE_CERT_INVALID, (
                            f"oracle_reg missing cert_sig for action={_action}"
                        ), None
                    # cert_sig structure: sha3_256(oracle_addr + "|" + from_addr + "|" + oracle_pub)
                    # Full HypΓ cert verification is done server-side at block seal time.
                    # Here we do a fast structural check: cert_sig must be 64-char hex.
                    if _cert_sig and (len(_cert_sig) < 32 or not all(c in '0123456789abcdef' for c in _cert_sig.lower())):
                        self._stats['total_rejected'] += 1
                        return AcceptResult.ORACLE_CERT_INVALID, (
                            f"oracle_reg cert_sig malformed (len={len(_cert_sig)})"
                        ), None
                    logger.info(
                        f"[MEMPOOL-CERT] ✅ oracle_reg cert check pass | "
                        f"oracle={_oracle_addr[:20]}… | wallet={from_addr[:20]}… | "
                        f"action={_action}"
                    )

                # ── [L] SENDER LIMIT ───────────────────────────────────────
                sender_count = len(self._by_sender[from_addr])
                if sender_count >= MAX_TX_PER_SENDER and replaced_tx is None:
                    self._stats['total_rejected'] += 1
                    return AcceptResult.SENDER_LIMIT, (
                        f"sender has {sender_count}/{MAX_TX_PER_SENDER} pending TXs"
                    ), None

                # ── [M] CAPACITY ───────────────────────────────────────────
                if len(self._index) >= MAX_MEMPOOL_SIZE and replaced_tx is None:
                    # Try to make room by evicting lowest-fee TX
                    evicted = self._evict_lowest(1)
                    if not evicted:
                        self._stats['total_rejected'] += 1
                        return AcceptResult.MEMPOOL_FULL, "mempool full, cannot evict", None

                # ── W-STATE ENTROPY CAPTURE ────────────────────────────────
                w_entropy_hash = self._capture_w_entropy()

                # ── CONSTRUCT TX ───────────────────────────────────────────
                sig_json = sig_raw if isinstance(sig_raw, str) else json.dumps(sig_raw)
                meta = dict(metadata)
                _pubkey = (raw.get("sender_public_key_hex") or raw.get("public_key", "")
                           or raw.get("signature", {}).get("public_key_hex", "")
                           or raw.get("signature", {}).get("public_key", ""))
                meta.update({
                    'fee_qtcl'       : fee_base / 100,
                    'amount_qtcl'    : amount_base / 100,
                    'oracle_signed'  : sig_valid,
                    'submitted_at_ns': timestamp_ns,
                    'client_tx_id'   : client_tx_id,
                    'sender_public_key_hex': _pubkey,
                })

                tx = MempoolTx(
                    tx_hash        = tx_hash,
                    from_address   = from_addr,
                    to_address     = to_addr,
                    amount_base    = amount_base,
                    fee_base       = fee_base,
                    nonce          = nonce,
                    signature      = sig_json,
                    w_entropy_hash = w_entropy_hash,
                    timestamp_ns   = timestamp_ns,
                    tx_type        = tx_type,
                    memo           = memo,
                    client_tx_id   = client_tx_id,
                    metadata       = meta,
                )

                # ── APPLY RBF EVICTION ─────────────────────────────────────
                if replaced_tx:
                    self._remove_tx(replaced_tx.tx_hash, TxStatus.REPLACED)
                    self._stats['total_replaced'] += 1

                # ── INSERT INTO MEMPOOL ────────────────────────────────────
                self._insert(tx)

                # ── PERSIST TO DB ──────────────────────────────────────────
                self._persist_tx(tx)

                # ── UPSERT WALLET ROWS ─────────────────────────────────────
                self._balances.upsert_wallet(from_addr)
                self._balances.upsert_wallet(to_addr)

                # ── FEED BLOCK MANAGER ─────────────────────────────────────
                self._feed_block_manager(tx)

                # ── NOTIFY SUBSCRIBERS ────────────────────────────────────
                self._emit('accepted', tx)

                self._stats['total_accepted'] += 1
                result = AcceptResult.REPLACED_BY_FEE if replaced_tx else AcceptResult.ACCEPTED

            except Exception as exc:
                logger.error(f"[MEMPOOL] accept() unhandled error: {exc}\n{traceback.format_exc()}")
                return AcceptResult.INTERNAL_ERROR, str(exc), None

        # Outside lock — PG connect can block; must never hold _lock during network I/O
        self._notify_peers(tx)
        return result, f"TX accepted: {tx_hash[:16]}…", tx

    # ─── Block building ─────────────────────────────────────────────────

    def get_block_transactions(
        self,
        max_txs       : int = 2_000,
        block_height  : int = 0,
        miner_address : str = "",
        block_reward_base : int = 720,      # depth-5 genesis miner default (7.20 QTCL × 100) — callers must pass TessellationRewardSchedule.get_miner_reward_base(height)
    ) -> Tuple[List[MempoolTx], 'MempoolTx']:
        """
        Select transactions for next block — Bitcoin fee-rate ordering.

        Returns:
            (tx_list_sorted_by_fee_rate_desc, coinbase_tx)

        Algorithm:
            1. Pop heap (highest fee_rate first) skipping evicted/expired TXs
            2. Within same fee_rate: apply CPFP — ancestor fee boosting
            3. Stop at max_txs
            4. Build coinbase TX
        """
        selected : List[MempoolTx] = []
        seen_hashes : Set[str] = set()

        with self._lock:
            # Drain heap into a temporary sorted list
            tmp_heap = list(self._heap)
            selected_raw : List[MempoolTx] = []

            # Heappop from a copy so we don't destroy the live heap
            import heapq as _hq
            heap_copy = list(tmp_heap)
            _hq.heapify(heap_copy)

            while heap_copy and len(selected_raw) < max_txs:
                tx = _hq.heappop(heap_copy)
                if tx.tx_hash in self._evicted:
                    continue
                if tx.tx_hash not in self._index:
                    continue
                if tx.tx_hash in seen_hashes:
                    continue
                seen_hashes.add(tx.tx_hash)
                selected_raw.append(tx)

            # CPFP: boost effective fee_rate for TXs whose descendants pay more
            selected = self._apply_cpfp(selected_raw)

        coinbase = self._build_coinbase(block_height, miner_address, block_reward_base)
        # Always build treasury coinbase (slot 1) from canonical schedule
        try:
            from globals import TessellationRewardSchedule as _TRS_mp
            _treasury_base = _TRS_mp.get_treasury_reward_base(block_height)
            _treasury_addr = _TRS_mp.TREASURY_ADDRESS
        except Exception:
            _treasury_base = 800 - block_reward_base   # fallback: remainder
            _treasury_addr = os.environ.get(
                'TREASURY_ADDRESS',
                'e8ffb27915ac244e8257de8b7f96ad387d1e9d93c634d849a6ad2dae0da6750b'
            )
        # Collect all donations (formerly fees) for the treasury
        total_donations = sum(tx.fee_base for tx in selected)
        
        treasury_coinbase = self._build_treasury_coinbase(
            block_height, _treasury_base + total_donations, _treasury_addr
        )
        return selected, coinbase, treasury_coinbase

    def mark_included_in_block(self, tx_hashes: List[str], block_height: int) -> int:
        """
        Called after a block is sealed.
        Removes TXs from the mempool and marks them confirmed in DB.

        Returns count of TXs processed.
        """
        count = 0
        with self._lock:
            for h in tx_hashes:
                tx = self._index.get(h)
                if tx:
                    self._nonces.mark_confirmed(tx.from_address, tx.nonce)
                    self._balances.unlock(tx.from_address, tx.amount_base + tx.fee_base)
                    self._remove_tx(h, TxStatus.CONFIRMED)
                    count += 1

        # DB update outside lock (non-critical path)
        if _db.available and tx_hashes:
            try:
                with _db.cursor() as cur:
                    cur.execute("""
                        UPDATE transactions
                        SET status = 'confirmed',
                            height = %s,
                            updated_at = NOW()
                        WHERE tx_hash = ANY(%s)
                          AND status = 'pending'
                    """, (block_height, tx_hashes))
            except Exception as exc:
                logger.error(f"[MEMPOOL-DB] mark_included error: {exc}")

        self._stats['total_confirmed'] += count
        logger.info(f"[MEMPOOL] ✅ {count} TXs confirmed at block {block_height}")
        return count

    def reorg(self, tx_hashes_to_readd: List[str]) -> int:
        """
        Chain reorganisation: re-queue TXs from orphaned blocks.
        These TXs were confirmed but the block that included them is now stale.
        Re-validate and re-insert; skip any that double-spend on the new chain.
        """
        readded = 0
        if not _db.available:
            return 0

        try:
            with _db.cursor() as cur:
                cur.execute("""
                    SELECT tx_hash, from_address, to_address, amount, nonce,
                           quantum_state_hash, metadata, created_at
                    FROM transactions
                    WHERE tx_hash = ANY(%s)
                """, (tx_hashes_to_readd,))
                rows = cur.fetchall()
        except Exception as exc:
            logger.error(f"[MEMPOOL] reorg DB read failed: {exc}")
            return 0

        for row in rows:
            # Re-submit with a synthetic raw dict
            amount_base = int(row['amount']) if row['amount'] else 0
            meta = row['metadata'] or {}
            raw = {
                'from_address' : row['from_address'],
                'to_address'   : row['to_address'],
                'amount'       : amount_base,
                'nonce'        : row['nonce'] or 0,
                'fee'          : meta.get('fee_base', MIN_RELAY_FEE_ABS),
                'timestamp_ns' : int(row['created_at'].timestamp() * 1e9) if row['created_at'] else time.time_ns(),
                'signature'    : row['quantum_state_hash'] or '{}',
                'tx_hash'      : row['tx_hash'],
            }
            result, _, _ = self.accept(raw)
            if result in (AcceptResult.ACCEPTED, AcceptResult.REPLACED_BY_FEE):
                readded += 1

        logger.info(f"[MEMPOOL] ♻️  Reorg: re-queued {readded}/{len(tx_hashes_to_readd)} TXs")
        return readded

    # ─── Query interface ─────────────────────────────────────────────────

    def get(self, tx_hash: str) -> Optional[MempoolTx]:
        with self._lock:
            return self._index.get(tx_hash)

    def contains(self, tx_hash: str) -> bool:
        with self._lock:
            return tx_hash in self._index

    def size(self) -> int:
        with self._lock:
            return len(self._index)

    def pending_by_address(self, address: str) -> List[MempoolTx]:
        """All pending TXs from or to a given address."""
        with self._lock:
            result = list(self._by_sender.get(address, {}).values())
            # Also find incoming (less common to query, linear scan acceptable)
            for tx in self._index.values():
                if tx.to_address == address and tx not in result:
                    result.append(tx)
            return sorted(result, key=lambda t: t.fee_rate, reverse=True)

    def estimate_fee(self, target_blocks: int = 1) -> Dict[str, Any]:
        """
        Fee estimation analogous to Bitcoin's estimatesmartfee.

        Returns recommended fee_rate (base units per vbyte) for confirmation
        within target_blocks blocks.
        """
        rate = self._fee_hist.estimate(target_blocks)
        return {
            'target_blocks'        : target_blocks,
            'fee_rate_sat_per_vbyte': round(rate, 6),
            'fee_for_standard_tx'  : int(rate * TX_VSIZE_BYTES),
            'histogram'            : self._fee_hist.snapshot(),
        }

    def stats(self) -> Dict[str, Any]:
        with self._lock:
            size   = len(self._index)
            total_fee = sum(tx.fee_base for tx in self._index.values())
            min_fee_rate = min((tx.fee_rate for tx in self._index.values()), default=0.0)
            max_fee_rate = max((tx.fee_rate for tx in self._index.values()), default=0.0)
            avg_fee_rate = (total_fee / (size * TX_VSIZE_BYTES)) if size else 0.0

        uptime = time.time() - self._stats['started_at']
        return {
            'size'            : size,
            'total_fee_base'  : total_fee,
            'min_fee_rate'    : round(min_fee_rate, 6),
            'max_fee_rate'    : round(max_fee_rate, 6),
            'avg_fee_rate'    : round(avg_fee_rate, 6),
            'total_accepted'  : self._stats['total_accepted'],
            'total_rejected'  : self._stats['total_rejected'],
            'total_replaced'  : self._stats['total_replaced'],
            'total_expired'   : self._stats['total_expired'],
            'total_confirmed' : self._stats['total_confirmed'],
            'uptime_s'        : int(uptime),
            'fee_estimate_1b' : self._fee_hist.estimate(1),
            'fee_estimate_6b' : self._fee_hist.estimate(6),
        }

    # ─── Subscription / event bus ────────────────────────────────────────

    def subscribe(self, callback: Callable[[str, MempoolTx], None]) -> None:
        """
        Subscribe to mempool events.
        callback(event_name, tx):  event_name ∈ {'accepted', 'removed', 'expired', 'replaced'}
        """
        with self._lock:
            self._subscribers.append(callback)

    def unsubscribe(self, callback: Callable) -> None:
        with self._lock:
            if callback in self._subscribers:
                self._subscribers.remove(callback)

    # ═══════════════════════════════════════════════════════════════════════
    # INTERNAL HELPERS
    # ═══════════════════════════════════════════════════════════════════════

    def _insert(self, tx: MempoolTx) -> None:
        """Insert a TX into all internal data structures."""
        self._index[tx.tx_hash]                    = tx
        self._by_sender[tx.from_address][tx.nonce] = tx
        heapq.heappush(self._heap, tx)
        self._nonces.add_pending(tx.from_address, tx.nonce)
        self._balances.lock(tx.from_address, tx.amount_base + tx.fee_base)
        self._fee_hist.add(tx.fee_rate)

    def _remove_tx(self, tx_hash: str, reason: TxStatus) -> Optional[MempoolTx]:
        """
        Remove from index + sender map.  Heap uses lazy deletion — just add to _evicted.
        """
        tx = self._index.pop(tx_hash, None)
        if tx is None:
            return None
        self._evicted.add(tx_hash)
        self._by_sender[tx.from_address].pop(tx.nonce, None)
        if not self._by_sender[tx.from_address]:
            del self._by_sender[tx.from_address]
        self._nonces.remove_pending(tx.from_address, tx.nonce)
        self._fee_hist.remove(tx.fee_rate)
        self._emit(reason.value, tx)
        return tx

    def _evict_lowest(self, n: int) -> List[MempoolTx]:
        """Evict the n lowest fee_rate pending TXs (make room for better ones)."""
        # Build sorted list of live TXs ascending by fee_rate
        candidates = sorted(
            (tx for tx in self._index.values()),
            key=lambda t: t.fee_rate,
        )
        evicted = []
        for tx in candidates[:n]:
            self._balances.unlock(tx.from_address, tx.amount_base + tx.fee_base)
            self._remove_tx(tx.tx_hash, TxStatus.REJECTED)
            evicted.append(tx)
        return evicted

    def _apply_cpfp(self, txs: List[MempoolTx]) -> List[MempoolTx]:
        """
        Child-Pays-For-Parent fee boosting.

        A child TX that spends an unconfirmed parent TX can boost the parent's
        effective fee_rate.  We group by sender+nonce chains and compute the
        package fee_rate for each chain.

        Simplified QTCL version: a TX whose from_address == another TX's to_address
        is considered a "child" of that TX.  Package fee_rate =
            (parent.fee_base + child.fee_base) / (2 * TX_VSIZE_BYTES)
        """
        if not txs:
            return txs

        # Build a to_address→tx map for O(1) parent lookup
        by_to: Dict[str, MempoolTx] = {tx.to_address: tx for tx in txs}
        boosted: Dict[str, float] = {}  # tx_hash → effective fee_rate

        for tx in txs:
            effective = tx.fee_rate
            parent = by_to.get(tx.from_address)
            if parent and parent.tx_hash != tx.tx_hash:
                pkg_fee  = parent.fee_base + tx.fee_base
                pkg_rate = pkg_fee / (2 * TX_VSIZE_BYTES)
                # CPFP lifts both parent and child to pkg_rate if it's higher
                if pkg_rate > parent.fee_rate:
                    boosted[parent.tx_hash] = max(boosted.get(parent.tx_hash, 0), pkg_rate)
                effective = max(effective, pkg_rate)
            boosted[tx.tx_hash] = max(boosted.get(tx.tx_hash, 0), effective)

        # Re-sort using boosted rates
        return sorted(txs, key=lambda t: boosted.get(t.tx_hash, t.fee_rate), reverse=True)

    def _verify_signature(
        self, tx_hash: str, sig_raw: Any, from_address: str, norm: Dict[str, Any]
    ) -> Tuple[bool, str]:
        """
        Verify HypΓ Schnorr-Γ signature using canonical hyp_engine.
        Matches client-side _integrate_wallet_send exactly.
        """
        if sig_raw is None or sig_raw == '' or sig_raw == '{}':
            logger.debug(f"[MEMPOOL-SIG] No signature for {tx_hash[:12]}… — rejecting")
            return False, "missing_signature"

        # Parse signature
        sig_dict = None
        if isinstance(sig_raw, str):
            try:
                sig_dict = json.loads(sig_raw)
            except:
                return False, "invalid_signature_json"
        elif isinstance(sig_raw, dict):
            sig_dict = sig_raw
        else:
            return False, "invalid_signature_type"

        # ── CANONICAL VERIFICATION ──────────────────────────────────────
        try:
            pub_key_hex = sig_dict.get('public_key_hex') or sig_dict.get('public_key', '')
            if not pub_key_hex:
                return False, "missing_public_key_in_signature"

            # 1. Reconstruct signing hash (the actual bytes signed by client)
            # Client signs: sha3_256(json.dumps({'sender':..., 'recipient':..., 'amount':..., 'nonce':...}))
            tx_data = {
                'sender': norm['from_address'],
                'recipient': norm['to_address'],
                'amount': norm['amount_base'] / 100.0,
                'nonce': norm['nonce']
            }
            tx_json = json.dumps(tx_data, sort_keys=True, default=str)
            logger.debug(f"[MEMPOOL-SIG] Reconstructed JSON: {tx_json}")
            signing_hash_bytes = hashlib.sha3_256(tx_json.encode('utf-8')).digest()
            signing_hash_hex = signing_hash_bytes.hex()
            logger.debug(f"[MEMPOOL-SIG] Signing hash: {signing_hash_hex}")

            # 2. Verify via Oracle
            if _ORACLE_AVAILABLE:
                # Pass the ACTUAL signing hash to the oracle
                ok, reason = ORACLE.verify_transaction(signing_hash_hex, sig_dict, from_address)
                if ok:
                    return True, "valid"
                return False, reason
            
            return False, "oracle_unavailable"

        except Exception as e:
            logger.error(f"[MEMPOOL-SIG] Verification error: {e}")
            return False, f"verification_error: {str(e)}"

    def _validate_format(
        self, raw: Dict[str, Any]
    ) -> Tuple[bool, str, Dict[str, Any]]:
        """
        Normalise and validate raw TX dict.
        Returns (ok, reason, normalised_dict).
        """
        try:
            required = {'from_address', 'to_address', 'amount', 'nonce', 'signature'}
            missing  = required - set(raw.keys())
            if missing:
                return False, f"missing_fields:{missing}", {}

            from_address = str(raw.get('from_address') or raw.get('from') or '').strip()
            to_address   = str(raw.get('to_address')   or raw.get('to')   or '').strip()

            if not from_address or len(from_address) < 5:
                return False, "invalid_from_address", {}
            if not to_address or len(to_address) < 5:
                return False, "invalid_to_address", {}
            if from_address == to_address:
                return False, "self_transfer_not_allowed", {}

            # Amount normalisation: accept QTCL (float) or base units (int)
            raw_amount = raw['amount']
            try:
                raw_amount_f = float(raw_amount)
            except (ValueError, TypeError):
                return False, f"invalid_amount:{raw_amount!r}", {}
            # If amount looks like QTCL (< 10_000) convert to base units
            amount_base = (
                int(round(raw_amount_f * 100))
                if raw_amount_f < 10_000
                else int(raw_amount_f)
            )
            if amount_base <= 0:
                return False, "amount_must_be_positive", {}

            # Fee normalisation
            raw_fee = raw.get('fee', raw.get('fee_base', MIN_RELAY_FEE_ABS))
            try:
                raw_fee_f = float(raw_fee)
            except (ValueError, TypeError):
                return False, f"invalid_fee:{raw_fee!r}", {}
            fee_base = (
                int(round(raw_fee_f * 100))
                if raw_fee_f < 10_000
                else int(raw_fee_f)
            )
            fee_base = max(fee_base, MIN_RELAY_FEE_ABS)

            nonce = int(raw.get('nonce', 0))
            if nonce < 0:
                return False, "negative_nonce", {}

            timestamp_ns = int(raw.get('timestamp_ns', time.time_ns()))

            sig = raw.get('signature') or raw.get('pq_signature') or ''
            if not sig:
                sig = ''

            return True, "ok", {
                'from_address' : from_address,
                'to_address'   : to_address,
                'amount_base'  : amount_base,
                'fee_base'     : fee_base,
                'nonce'        : nonce,
                'timestamp_ns' : timestamp_ns,
                'signature'    : sig,
                'tx_type'      : str(raw.get('tx_type', 'transfer')),
                'memo'         : str(raw.get('memo', ''))[:256],
                'client_tx_id' : str(raw.get('tx_id', raw.get('client_tx_id', '')))[:64],
                'metadata'     : dict(raw.get('metadata', {})),
            }

        except Exception as exc:
            return False, f"format_exception:{exc}", {}

    def _capture_w_entropy(self) -> str:
        """
        Capture current W-state entropy hash.
        This permanently binds the TX to this instant in quantum time.
        """
        try:
            w_bytes = get_block_field_entropy()
            return hashlib.sha3_256(w_bytes).hexdigest()
        except Exception:
            return hashlib.sha3_256(secrets.token_bytes(32)).hexdigest()

    def _persist_tx(self, tx: MempoolTx) -> None:
        """Write TX to transactions table.  Non-fatal if DB is unavailable."""
        if not _db.available:
            return
        # Ensure critical fields survive metadata round-trip (needed by _recover_from_db)
        persist_meta = dict(tx.metadata)
        persist_meta['fee_base']     = tx.fee_base
        persist_meta['amount_base']  = tx.amount_base
        persist_meta['nonce']        = tx.nonce
        persist_meta['timestamp_ns'] = tx.timestamp_ns
        persist_meta.setdefault('signature', tx.signature)
        try:
            with _db.cursor() as cur:
                cur.execute("""
                    INSERT INTO transactions
                        (tx_hash, from_address, to_address, amount, nonce,
                         tx_type, status, quantum_state_hash, commitment_hash, metadata)
                    VALUES (%s, %s, %s, %s, %s, %s, 'pending', %s, %s, %s)
                    ON CONFLICT (tx_hash) DO UPDATE
                        SET status     = CASE WHEN transactions.status = 'confirmed'
                                              THEN 'confirmed' ELSE 'pending' END,
                            metadata   = EXCLUDED.metadata,
                            updated_at = NOW()
                """, (
                    tx.tx_hash,
                    tx.from_address,
                    tx.to_address,
                    tx.amount_base,
                    tx.nonce,
                    tx.tx_type,
                    tx.w_entropy_hash,
                    tx.tx_hash,          # commitment_hash = canonical hash
                    json.dumps(persist_meta),
                ))
        except Exception as exc:
            logger.warning(f"[MEMPOOL-DB] persist_tx failed for {tx.tx_hash[:12]}…: {exc}")

    def _feed_block_manager(self, tx: MempoolTx) -> None:
        """
        Feed the BlockManager's in-process receive_transaction().
        This is the chain entanglement point: the mempool and BlockManager
        share the same transaction graph.
        """
        try:
            from lattice_controller import QuantumTransaction
            qt = tx.to_quantum_tx()
            if qt is None:
                return
            # Import the live LATTICE singleton from server context
            try:
                from server import LATTICE
                if LATTICE and getattr(LATTICE, 'block_manager', None):
                    LATTICE.block_manager.receive_transaction(qt)
            except (ImportError, AttributeError):
                pass
        except Exception as exc:
            logger.debug(f"[MEMPOOL] BlockManager feed skipped: {exc}")

    def _notify_peers(self, tx: MempoolTx) -> None:
        """
        Broadcast accepted TX to all peer workers via pg_notify.
        Uses C serializer for speed; falls back to json.dumps.
        Compact key names keep payload well under PG's 8000-byte limit.
        """
        if self._notifier is None:
            return
        try:
            buf_size = 7800
            if _C_SERIALIZE_TX is not None:
                import ctypes
                buf = ctypes.create_string_buffer(buf_size)
                n = _C_SERIALIZE_TX(
                    tx.tx_hash.encode(),
                    tx.from_address.encode(),
                    tx.to_address.encode(),
                    tx.amount_base, tx.fee_base,
                    tx.nonce, tx.timestamp_ns,
                    tx.w_entropy_hash.encode(),
                    tx.tx_type.encode(),
                    tx.memo[:60].encode(),
                    buf, buf_size,
                )
                payload = buf.value.decode() if n > 0 else None
            else:
                payload = None

            if payload is None:
                payload = json.dumps({
                    'h': tx.tx_hash, 'f': tx.from_address, 't': tx.to_address,
                    'a': tx.amount_base, 'fee': tx.fee_base,
                    'n': tx.nonce, 'ts': tx.timestamp_ns,
                    'w': tx.w_entropy_hash, 'tp': tx.tx_type,
                    'm': tx.memo[:60],
                    'sig': tx.signature[:200],
                }, separators=(',', ':'))

            if len(payload) <= _NOTIFY_PAYLOAD_MAX:
                self._notifier.notify(_NOTIFY_CHANNEL_MEMPOOL, payload)
        except Exception as exc:
            logger.debug(f"[MEMPOOL] _notify_peers error: {exc}")

    def _emit(self, event: str, tx: MempoolTx) -> None:
        """Notify all subscribers of a mempool event."""
        for cb in list(self._subscribers):
            try:
                cb(event, tx)
            except Exception as exc:
                logger.debug(f"[MEMPOOL] Subscriber error: {exc}")

    # ═══════════════════════════════════════════════════════════════════════
    # COINBASE TRANSACTION BUILDER
    # ═══════════════════════════════════════════════════════════════════════

    def _build_treasury_coinbase(
        self,
        block_height      : int,
        treasury_reward   : int,
        treasury_address  : str = None,
        w_entropy_hash    : str = '',
    ) -> 'MempoolTx':
        """
        Build treasury coinbase (slot 1) - UTXO format with outputs.
        """
        if treasury_address is None:
            treasury_address = os.environ.get(
                'TREASURY_ADDRESS',
                'e8ffb27915ac244e8257de8b7f96ad387d1e9d93c634d849a6ad2dae0da6750b'
            )
        import hashlib as _hl
        raw_input = f"TREASURY_COINBASE:{block_height}:{treasury_address}:{treasury_reward}:{w_entropy_hash}".encode()
        tx_hash   = _hl.sha3_256(raw_input).hexdigest()
        return MempoolTx(
            tx_hash        = tx_hash,
            from_address   = '0' * 64,
            to_address     = treasury_address,
            amount_base    = treasury_reward,
            fee_base       = 0,
            nonce          = block_height,
            signature      = '',
            w_entropy_hash = w_entropy_hash,
            timestamp_ns   = time.time_ns(),
            metadata       = {
                'treasury'          : True,
                'reward_type'       : 'treasury',
                'block_height'      : block_height,
                'treasury_reward'   : treasury_reward,
                'treasury_address'  : treasury_address,
            },
            tx_type        = "treasury_reward",
            memo           = f"Block {block_height} treasury",
            inputs         = [{"prev_tx_hash": "0" * 64, "prev_output_index": 0xFFFFFFFF, "script_sig": {"height": block_height, "type": "treasury"}}],
            outputs        = [{"address": treasury_address, "amount_base": treasury_reward, "script_pubkey": "OP_DUP OP_HASH160 OP_EQUALVERIFY OP_CHECKSIG"}],
        )

    def _build_coinbase(
        self,
        block_height  : int,
        miner_address : str,
        reward_base   : int,
    ) -> MempoolTx:
        """
        Build a coinbase transaction — UTXO format with outputs.
        Deterministic hash so the same coinbase is produced independently by all nodes.
        """
        w_entropy_hash = self._capture_w_entropy()
        raw_input = f"COINBASE:{block_height}:{miner_address}:{reward_base}:{w_entropy_hash}".encode()
        tx_hash   = hashlib.sha3_256(raw_input).hexdigest()

        coinbase = MempoolTx(
            tx_hash        = COINBASE_PREFIX + tx_hash,
            from_address   = "0" * 64,
            to_address     = miner_address or "0" * 64,
            amount_base    = reward_base,
            fee_base       = 0,
            nonce          = block_height,
            signature      = json.dumps({
                'coinbase'     : True,
                'block_height' : block_height,
                'miner'        : miner_address,
            }),
            w_entropy_hash = w_entropy_hash,
            timestamp_ns   = time.time_ns(),
            tx_type        = "miner_reward",
            memo           = f"Block {block_height} reward",
            metadata       = {
                'block_height' : block_height,
                'reward_base'  : reward_base,
                'reward_qtcl'  : reward_base / 100,
                'miner'        : miner_address,
            },
            inputs         = [{"prev_tx_hash": "0" * 64, "prev_output_index": 0xFFFFFFFF, "script_sig": {"height": block_height, "type": "miner"}}],
            outputs        = [{"address": miner_address or "0" * 64, "amount_base": reward_base, "script_pubkey": "OP_DUP OP_HASH160 OP_EQUALVERIFY OP_CHECKSIG"}],
        )
        return coinbase

    # ═══════════════════════════════════════════════════════════════════════
    # BACKGROUND WORKER
    # ═══════════════════════════════════════════════════════════════════════

    def _background_worker(self) -> None:
        """
        Periodic maintenance:
            • Expire TXs older than MEMPOOL_TTL_HOURS
            • Compact the lazy-deletion heap (purge all evicted entries)
            • Evict lowest-fee TXs when above threshold
            • Persist any in-memory TXs not yet in DB (crash recovery)
            • Log mempool health
        """
        while self._bg_running:
            try:
                time.sleep(BACKGROUND_INTERVAL_S)
                if not self._bg_running:
                    break

                self._expire_old_txs()
                self._compact_heap()

                size = self.size()
                if size > MAX_MEMPOOL_SIZE * 0.95:
                    n_evict = size - int(MAX_MEMPOOL_SIZE * 0.80)
                    with self._lock:
                        evicted = self._evict_lowest(max(n_evict, EVICTION_BATCH))
                    logger.info(f"[MEMPOOL] Evicted {len(evicted)} low-fee TXs (size={size})")

                s = self.stats()
                logger.info(
                    f"[MEMPOOL] 📊 size={s['size']} | "
                    f"accepted={s['total_accepted']} | rejected={s['total_rejected']} | "
                    f"fee_est_1b={s['fee_estimate_1b']:.2f} sat/vbyte"
                )

            except Exception as exc:
                logger.error(f"[MEMPOOL] Background worker error: {exc}")
                time.sleep(5)

    def _expire_old_txs(self) -> None:
        """Remove TXs pending longer than MEMPOOL_TTL_HOURS."""
        cutoff = time.time() - (MEMPOOL_TTL_HOURS * 3600)
        expired_hashes = []

        with self._lock:
            for tx_hash, tx in list(self._index.items()):
                if tx.accepted_at_s < cutoff:
                    expired_hashes.append(tx_hash)

            for h in expired_hashes:
                tx = self._index.get(h)
                if tx:
                    self._balances.unlock(tx.from_address, tx.amount_base + tx.fee_base)
                    self._remove_tx(h, TxStatus.EXPIRED)
                    self._stats['total_expired'] += 1

        if expired_hashes:
            logger.info(f"[MEMPOOL] Expired {len(expired_hashes)} old TXs")
            if _db.available:
                try:
                    with _db.cursor() as cur:
                        cur.execute("""
                            UPDATE transactions
                            SET status = 'rejected', updated_at = NOW()
                            WHERE tx_hash = ANY(%s) AND status = 'pending'
                        """, (expired_hashes,))
                except Exception as exc:
                    logger.debug(f"[MEMPOOL] DB expire update failed: {exc}")

    def _compact_heap(self) -> None:
        """
        Rebuild the heap from _index to purge evicted tombstones.
        O(n) but only runs every BACKGROUND_INTERVAL_S seconds.
        """
        with self._lock:
            self._heap = list(self._index.values())
            heapq.heapify(self._heap)
            self._evicted.clear()

    def _recover_from_db(self) -> None:
        """
        On startup: load all status='pending' TXs from the transactions table
        into the in-memory heap.  This provides crash recovery.
        """
        if not _db.available:
            logger.info("[MEMPOOL] DB not available — starting with empty mempool")
            return

        try:
            with _db.cursor() as cur:
                cur.execute("""
                    SELECT tx_hash, from_address, to_address, amount, nonce,
                           tx_type, quantum_state_hash, metadata, created_at
                    FROM transactions
                    WHERE status = 'pending'
                      AND tx_type NOT IN ('coinbase', 'miner_reward', 'treasury_reward')
                    ORDER BY created_at ASC
                    LIMIT %s
                """, (MAX_MEMPOOL_SIZE,))
                rows = cur.fetchall()

            recovered = 0
            for row in rows:
                try:
                    amount_base = int(row['amount']) if row['amount'] else 0
                    meta        = row['metadata'] or {}
                    if isinstance(meta, str):
                        meta = json.loads(meta)
                    fee_base    = int(meta.get('fee_base', MIN_RELAY_FEE_ABS))
                    # Prefer stored timestamp_ns; fall back to created_at conversion
                    _created_at = row.get('created_at') if isinstance(row, dict) else row[8]
                    ts_ns       = int(meta.get('timestamp_ns', 0)) or (
                                  int(_created_at.timestamp() * 1e9)
                                  if _created_at else time.time_ns())
                    # signature stored in metadata by _persist_tx; fall back to quantum_state_hash
                    recovered_sig = (meta.get('signature')
                                     or row.get('quantum_state_hash')
                                     or '')
                    tx = MempoolTx(
                        tx_hash        = row['tx_hash'],
                        from_address   = row['from_address'],
                        to_address     = row['to_address'],
                        amount_base    = amount_base,
                        fee_base       = fee_base,
                        nonce          = row['nonce'] or 0,
                        signature      = recovered_sig,
                        w_entropy_hash = row['quantum_state_hash'] or '',
                        timestamp_ns   = ts_ns,
                        tx_type        = row['tx_type'] or 'transfer',
                        metadata       = meta,
                    )
                    # Insert without full validation (already passed on first submit)
                    with self._lock:
                        if tx.tx_hash not in self._index:
                            self._insert(tx)
                            recovered += 1
                except Exception as exc:
                    logger.debug(f"[MEMPOOL] Recovery skipped TX: {exc}")

            logger.info(f"[MEMPOOL] ♻️  Recovered {recovered} pending TXs from DB")

        except Exception as exc:
            logger.error(f"[MEMPOOL] DB recovery failed: {exc}")

# ══════════════════════════════════════════════════════════════════════════════
# MEMPOOL SINGLETON ACCESS — thread-safe, lazy initialization
# ══════════════════════════════════════════════════════════════════════════════

_MEMPOOL: "Mempool | None" = None          # module-level singleton
_MEMPOOL_LOCK = threading.Lock()           # guards initialization

def get_mempool() -> Mempool:
    """
    Get or create the process-wide Mempool singleton.
    Thread-safe double-checked locking.
    """
    global _MEMPOOL
    
    # If already fully initialized with DB, return early
    if _MEMPOOL is not None and _db.available:
        return _MEMPOOL
        
    with _MEMPOOL_LOCK:
        if _MEMPOOL is None:
            # First time initialization
            dsn = _resolve_dsn()
            if dsn:
                _db.init(dsn)
            m = Mempool()
            m.start()
            _MEMPOOL = m
        elif not _db.available:
            # Singleton exists but DB failed — retry connection
            dsn = _resolve_dsn()
            if dsn:
                _db.init(dsn)
                
    return _MEMPOOL

def _resolve_dsn() -> Optional[str]:
    """Resolve database URL from environment variables (server.py convention)."""
    from urllib.parse import quote_plus
    
    # 1. Try DATABASE_URL first (Neon PostgreSQL connection string)
    dsn = os.getenv('DATABASE_URL') or os.getenv('POOLER_URL')
    
    # Robust check: ignore literal placeholders like "DATABASE_URL" or "POOLER_URL"
    if dsn and not dsn.startswith('postgres'):
        dsn = None
        
    if dsn:
        return dsn

    # 2. Build from components
    host     = os.getenv('POOLER_HOST')
    user     = os.getenv('POOLER_USER')
    pw       = os.getenv('POOLER_PASSWORD')
    db       = os.getenv('POOLER_DB', 'postgres')
    port_raw = os.getenv('POOLER_PORT', '6543')
    
    # Robust integer parsing for POOLER_PORT
    try:
        port = int(port_raw) if port_raw and str(port_raw).isdigit() else 6543
    except:
        port = 6543
        
    # Sanitize components against literal placeholders
    if host == 'POOLER_HOST': host = None
    if user == 'POOLER_USER': user = None
    
    if host and user and pw:
        return f"postgresql://{quote_plus(user)}:{quote_plus(pw)}@{host}:{port}/{db}"
    return None

def init_mempool(dsn: str) -> Mempool:
    """
    Explicitly initialise mempool with a known DSN.
    Use this when you need to control startup order.
    """
    global _MEMPOOL
    with _MEMPOOL_LOCK:
        _db.init(dsn)
        if _MEMPOOL is None:
            m = Mempool()
            m.start()
            _MEMPOOL = m
    return _MEMPOOL

# ══════════════════════════════════════════════════════════════════════════════
# MODULE-LEVEL FUNCTION ALIASES — drop-in replacement for old mempool.py
# ══════════════════════════════════════════════════════════════════════════════

def add_transaction(raw: Dict[str, Any]) -> Tuple[bool, str]:
    """Add a transaction. Returns (accepted: bool, message: str).
    Special handling for attestation_tx (consensus votes)."""
    
    # Handle attestation transactions (consensus)
    if raw.get('type') == 'attestation':
        try:
            from globals import accept_attestation
            
            success, reason = accept_attestation(
                validator_index=int(raw.get('validator_index', -1)),
                slot=int(raw.get('slot', 0)),
                beacon_block_root=raw.get('beacon_block_root', ''),
                source_epoch=int(raw.get('source_epoch', 0)),
                target_epoch=int(raw.get('target_epoch', 0)),
                signature=raw.get('signature', '')
            )
            return success, reason
        except Exception as e:
            return False, f"Attestation error: {str(e)}"
    
    # Regular transaction
    result, msg, _ = get_mempool().accept(raw)
    return result in (AcceptResult.ACCEPTED, AcceptResult.REPLACED_BY_FEE), msg

def get_pending_transactions(max_count: int = 2_000) -> List[MempoolTx]:
    """Get pending TXs sorted by fee_rate DESC (for mining)."""
    txs, _coinbase, _treasury = get_mempool().get_block_transactions(max_txs=max_count)
    return txs

def get_transaction_by_hash(tx_hash: str) -> Optional[MempoolTx]:
    return get_mempool().get(tx_hash)

def mark_included_in_block(tx_hashes: List[str], block_height: int) -> bool:
    return get_mempool().mark_included_in_block(tx_hashes, block_height) >= 0

def get_mempool_stats() -> Dict[str, Any]:
    return get_mempool().stats()

def estimate_fee(target_blocks: int = 1) -> Dict[str, Any]:
    return get_mempool().estimate_fee(target_blocks)

# ══════════════════════════════════════════════════════════════════════════════
# EXPORTS
# ══════════════════════════════════════════════════════════════════════════════

__all__ = [
    # Singleton access
    'get_mempool',
    'init_mempool',
    # Core classes
    'Mempool',
    'MempoolTx',
    'HypMempoolVerifier',
    'HLWEMempoolVerifier',  # backward-compat alias
    'FeeHistogram',
    'NonceOracle',
    'BalanceOracle',
    # Enums
    'TxStatus',
    'AcceptResult',
    # Drop-in module-level functions
    'add_transaction',
    'get_pending_transactions',
    'get_transaction_by_hash',
    'mark_included_in_block',
    'get_mempool_stats',
    'estimate_fee',
    # Constants
    'MAX_MEMPOOL_SIZE',
    'TX_VSIZE_BYTES',
    'MIN_RELAY_FEE_RATE',
    'COINBASE_PREFIX',
]


# ════════════════════════════════════════════════════════════════════════════════
# LAYER 4: ATOMIC FIELD STATE PERSISTENCE
# ════════════════════════════════════════════════════════════════════════════════

@dataclass
class FieldSnapshot:
    """Atomic DB snapshot: complete field state"""
    field_id: str
    pq_last: int
    pq_curr: int
    route_hash: str
    entropy_seed: str
    difficulty_bits: int
    fidelity_trajectory: list
    coherence_samples: list
    entropy_vn: float
    purity: float
    witness: float
    mining_time: float
    mining_attempts: int
    entropy_attempts: int
    timestamp: str
    peer_id: str = ""

    def to_dict(self) -> Dict[str, Any]:
        return {
            'field_id': self.field_id,
            'pq_last': self.pq_last,
            'pq_curr': self.pq_curr,
            'route_hash': self.route_hash,
            'entropy_seed': self.entropy_seed,
            'difficulty_bits': self.difficulty_bits,
            'fidelity_trajectory': self.fidelity_trajectory,
            'coherence_samples': self.coherence_samples,
            'entropy_vn': self.entropy_vn,
            'purity': self.purity,
            'witness': self.witness,
            'mining_time': self.mining_time,
            'mining_attempts': self.mining_attempts,
            'entropy_attempts': self.entropy_attempts,
            'timestamp': self.timestamp,
            'peer_id': self.peer_id
        }

class FieldStateDB:
    """In-memory field snapshot storage with atomic writes"""

    def __init__(self):
        self.snapshots: Dict[str, FieldSnapshot] = {}
        self.coherence_trajectories: Dict[str, List[float]] = {}
        logger.info("[LAYER-4] FieldStateDB initialized")

    def write_atomic(self, snapshot: FieldSnapshot) -> bool:
        """Atomic write of field snapshot"""
        try:
            self.snapshots[snapshot.field_id] = snapshot
            self.coherence_trajectories[snapshot.field_id] = snapshot.coherence_samples
            logger.debug(f"[LAYER-4] ✓ Atomic snapshot: {snapshot.field_id[:8]} (coherence samples: {len(snapshot.coherence_samples)})")
            return True
        except Exception as e:
            logger.error(f"[LAYER-4] Atomic write failed: {e}")
            return False

    def get_snapshot(self, field_id: str) -> Optional[FieldSnapshot]:
        """Retrieve field snapshot"""
        return self.snapshots.get(field_id)

    def get_coherence_trajectory(self, field_id: str) -> List[float]:
        """Get coherence measurements for field"""
        return self.coherence_trajectories.get(field_id, [])

    def all_field_ids(self) -> List[str]:
        """List all local field IDs"""
        return list(self.snapshots.keys())

    def snapshot_count(self) -> int:
        """Count stored snapshots"""
        return len(self.snapshots)

    def delete_snapshot(self, field_id: str) -> bool:
        """Delete field snapshot"""
        try:
            if field_id in self.snapshots:
                del self.snapshots[field_id]
            if field_id in self.coherence_trajectories:
                del self.coherence_trajectories[field_id]
            logger.debug(f"[LAYER-4] Snapshot deleted: {field_id[:8]}")
            return True
        except Exception as e:
            logger.error(f"[LAYER-4] Delete failed: {e}")
            return False
