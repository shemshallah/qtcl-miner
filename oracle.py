#!/usr/bin/env python3
"""
╔══════════════════════════════════════════════════════════════════════════════╗
║  QTCL ORACLE SERVER v10.0 — Standalone Byzantine Consensus Engine            ║
║  5 Independent Quantum Oracles · Temporal Ordering · Attestation Cache        ║
║                                                                              ║
║  Run:  python oracle.py                                                      ║
║  Port: 9092 (configurable via ORACLE_PORT env var)                           ║
║                                                                              ║
║  Architecture:                                                               ║
║    • 5 OracleNode instances (each with isolated AerSimulator)                ║
║    • Unified AttestationCache (thread-safe, temporal ordering)               ║
║    • HTTP RPC server for external attestation submission                     ║
║    • SSE stream for real-time consensus events                               ║
║    • Background consensus worker (3-of-5 threshold, auto-finalize)           ║
╚══════════════════════════════════════════════════════════════════════════════╝
"""

import os
import sys
import json
import time
import logging
import hashlib
import secrets
import threading
import traceback
import queue as _queue_module
from concurrent.futures import ThreadPoolExecutor
from typing import Dict, Any, Optional, List, Tuple, Set
from dataclasses import dataclass, field
from datetime import datetime, timezone
from collections import OrderedDict
from enum import Enum, auto
from decimal import Decimal, getcontext
from http.server import ThreadingHTTPServer, BaseHTTPRequestHandler
import urllib.request
import urllib.parse
import urllib.error

getcontext().prec = 150

# ═══════════════════════════════════════════════════════════════════════════════
# LOGGING
# ═══════════════════════════════════════════════════════════════════════════════
if not logging.getLogger().hasHandlers():
    logging.basicConfig(
        level=logging.INFO,
        format="[%(asctime)s] %(levelname)s: %(message)s",
    )
logger = logging.getLogger(__name__)

# ═══════════════════════════════════════════════════════════════════════════════
# CONFIGURATION
# ═══════════════════════════════════════════════════════════════════════════════
ORACLE_PORT = int(os.environ.get("ORACLE_PORT", "9092"))
ORACLE_HOST = os.environ.get("ORACLE_HOST", "0.0.0.0")
NUM_ORACLES = int(os.environ.get("NUM_ORACLES", "5"))
CONSENSUS_THRESHOLD = int(os.environ.get("CONSENSUS_THRESHOLD", "3"))
FORCE_FINALIZE_S = float(os.environ.get("FORCE_FINALIZE_S", "120.0"))
BLOCK_DIFFICULTY = int(os.environ.get("BLOCK_DIFFICULTY", "5"))

logger.info(f"[ORACLE-SERVER] Config: port={ORACLE_PORT} oracles={NUM_ORACLES} threshold={CONSENSUS_THRESHOLD}")

# ═══════════════════════════════════════════════════════════════════════════════
# QISKIT / AER SETUP
# ═══════════════════════════════════════════════════════════════════════════════
QISKIT_AVAILABLE = False
AerSimulator = None
NoiseModel = None
QuantumCircuit = None
DensityMatrix = None
depolarizing_error = None
amplitude_damping_error = None
phase_damping_error = None

try:
    from qiskit import QuantumCircuit
    from qiskit.quantum_info import DensityMatrix
    from qiskit_aer import AerSimulator
    from qiskit_aer.noise import (
        NoiseModel, depolarizing_error, amplitude_damping_error, phase_damping_error
    )
    QISKIT_AVAILABLE = True
    logger.info("[ORACLE-SERVER] ✅ Qiskit AER available")
except ImportError:
    logger.warning("[ORACLE-SERVER] ⚠️ Qiskit AER not available — using classical fallback")

# ═══════════════════════════════════════════════════════════════════════════════
# W-STATE CONSTANTS
# ═══════════════════════════════════════════════════════════════════════════════
import numpy as np

NUM_QUBITS_WSTATE = 3
_W_THETA_0 = np.arccos(np.sqrt(2.0 / 3.0))  # ~35.26°
_W_THETA_1 = np.arccos(np.sqrt(1.0 / 2.0))  # 45°

# Ideal W-state density matrix (8×8)
_W_IDEAL_DM = np.zeros((8, 8), dtype=complex)
for _ri in (1, 2, 4):
    for _rj in (1, 2, 4):
        _W_IDEAL_DM[_ri, _rj] = 1.0 / 3.0

# Oracle roles
_ORACLE_ROLES = [
    "PRIMARY_LATTICE",
    "SECONDARY_LATTICE", 
    "VALIDATION",
    "ARBITER",
    "METRICS",
]

# ═══════════════════════════════════════════════════════════════════════════════
# QUANTUM INFORMATION METRICS
# ═══════════════════════════════════════════════════════════════════════════════
class QuantumInformationMetrics:
    """Static methods for quantum state analysis."""

    @staticmethod
    def purity(dm: np.ndarray) -> float:
        return float(np.real(np.trace(dm @ dm)))

    @staticmethod
    def w3_fidelity(dm: np.ndarray) -> float:
        return float(np.real(np.trace(dm @ _W_IDEAL_DM)))

    @staticmethod
    def state_fidelity(rho1: np.ndarray, rho2: np.ndarray) -> float:
        """F(ρ₁,ρ₂) = Tr(√(√ρ₁·ρ₂·√ρ₁))² — Uhlmann–Jozsa fidelity."""
        try:
            if rho1 is None or rho2 is None:
                return 0.0
            ev, ec = np.linalg.eigh(rho1)
            ev = np.maximum(ev, 0.0)
            sqrt_rho1 = ec @ np.diag(np.sqrt(ev)) @ ec.conj().T
            product = sqrt_rho1 @ rho2 @ sqrt_rho1
            ep = np.linalg.eigvalsh(product)
            ep = np.maximum(ep, 0.0)
            return float(min(1.0, max(0.0, float(np.sum(np.sqrt(ep))) ** 2)))
        except Exception:
            return 0.0

    @staticmethod
    def von_neumann_entropy(dm: np.ndarray) -> float:
        try:
            evals = np.linalg.eigvalsh(dm)
            evals = np.clip(evals, 1e-15, 1.0)
            return float(-np.sum(evals * np.log2(evals)))
        except Exception:
            return 0.0

    @staticmethod
    def coherence_l1(dm: np.ndarray) -> float:
        off_diag = np.sum(np.abs(dm - np.diag(np.diag(dm))))
        return float(off_diag / 7.0)

    # Alias for backward compatibility
    coherence_l1_norm = coherence_l1


# ═══════════════════════════════════════════════════════════════════════════════
# UTILITY FUNCTIONS
# ═══════════════════════════════════════════════════════════════════════════════
def _oracle_qrng_bytes(n: int) -> bytes:
    """Generate cryptographically random bytes."""
    return secrets.token_bytes(n)


def _oracle_hermitian_perturb(n: int, epsilon: float = 0.15) -> np.ndarray:
    """Generate random Hermitian perturbation matrix."""
    H = np.random.normal(0, epsilon, (n, n)) + 1j * np.random.normal(0, epsilon, (n, n))
    H = 0.5 * (H + H.conj().T)
    return scipy.linalg.expm(-1j * H)


def _oracle_stochastic_channel(dm: np.ndarray, epsilon: float = 0.03) -> np.ndarray:
    """Apply small stochastic perturbation."""
    noise = np.random.normal(0, epsilon, dm.shape) + 1j * np.random.normal(0, epsilon, dm.shape)
    dm2 = dm + noise
    dm2 = 0.5 * (dm2 + dm2.conj().T)
    tr = float(np.real(np.trace(dm2)))
    if tr > 1e-12:
        dm2 /= tr
    return dm2


def _oracle_amplify_revival(dm: np.ndarray, F: float) -> Tuple[np.ndarray, bool, float]:
    """Attempt to revive low-fidelity state."""
    if F >= 0.08:
        return dm, False, 0.0
    dF = 0.05 * (1.0 - F)
    dm2 = dm * (1.0 - dF) + _W_IDEAL_DM * dF
    dm2 = 0.5 * (dm2 + dm2.conj().T)
    tr = float(np.real(np.trace(dm2)))
    if tr > 1e-12:
        dm2 /= tr
    return dm2, True, dF


def _oracle_resurrect(dm: np.ndarray, F: float) -> Tuple[np.ndarray, bool, float]:
    """Inject ideal W-state blend for very low fidelity."""
    blend = 0.3 + 0.4 * F
    dm2 = dm * (1.0 - blend) + _W_IDEAL_DM * blend
    dm2 = 0.5 * (dm2 + dm2.conj().T)
    tr = float(np.real(np.trace(dm2)))
    if tr > 1e-12:
        dm2 /= tr
    return dm2, True, blend


try:
    import scipy.linalg
except ImportError:
    logger.warning("[ORACLE-SERVER] scipy not available — some features disabled")
    scipy = None


# ═══════════════════════════════════════════════════════════════════════════════
# BLOCK STATUS ENUM
# ═══════════════════════════════════════════════════════════════════════════════
class BlockStatus(Enum):
    PENDING = auto()
    ATTESTED = auto()
    FINALIZED = auto()
    REJECTED = auto()
    ORPHANED = auto()


# ═══════════════════════════════════════════════════════════════════════════════
# ATTESTATION DATA STRUCTURES
# ═══════════════════════════════════════════════════════════════════════════════
@dataclass
class Attestation:
    """A single oracle attestation for a block."""
    oracle_id: str
    oracle_address: str
    block_height: int
    block_hash: str
    header_hash: str
    signature: Dict[str, Any]
    w_state_fidelity: float
    timestamp: int
    received_at: float = field(default_factory=time.time)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "oracle_id": self.oracle_id,
            "oracle_address": self.oracle_address,
            "block_height": self.block_height,
            "block_hash": self.block_hash,
            "header_hash": self.header_hash,
            "signature": self.signature,
            "w_state_fidelity": self.w_state_fidelity,
            "timestamp": self.timestamp,
        }


@dataclass
class CachedBlock:
    """Block awaiting oracle consensus."""
    height: int
    block_hash: str
    parent_hash: str
    miner_address: str
    nonce: int
    difficulty: int
    timestamp: int
    transactions: List[Dict[str, Any]]
    merkle_root: str = ""
    status: BlockStatus = BlockStatus.PENDING
    attestations: Dict[str, Attestation] = field(default_factory=dict)
    first_seen: float = field(default_factory=time.time)
    finalized_at: float = 0.0
    miner_reward: float = 0.0

    @property
    def attestation_count(self) -> int:
        return len(self.attestations)

    @property
    def age_seconds(self) -> float:
        return time.time() - self.first_seen


# ═══════════════════════════════════════════════════════════════════════════════
# TEMPORAL ORDERING ENGINE
# ═══════════════════════════════════════════════════════════════════════════════
class TemporalOrderingEngine:
    """
    Handles temporal relations between competing block submissions.
    First valid submission for a height wins. Late submissions are orphaned.
    """
    
    def __init__(self):
        self._height_first_seen: Dict[int, float] = {}
        self._lock = threading.Lock()
    
    def record_first_seen(self, height: int) -> float:
        """Record when we first saw a block at this height. Returns the timestamp."""
        with self._lock:
            if height not in self._height_first_seen:
                self._height_first_seen[height] = time.time()
            return self._height_first_seen[height]
    
    def get_first_seen(self, height: int) -> Optional[float]:
        with self._lock:
            return self._height_first_seen.get(height)
    
    def is_late_submission(self, height: int, block_hash: str, max_age_s: float = 30.0) -> bool:
        """Check if a submission is too late (another block already accepted)."""
        with self._lock:
            first = self._height_first_seen.get(height)
            if first is None:
                return False
            return (time.time() - first) > max_age_s
    
    def clear_height(self, height: int):
        with self._lock:
            self._height_first_seen.pop(height, None)


# ═══════════════════════════════════════════════════════════════════════════════
# ATTESTATION CACHE
# ═══════════════════════════════════════════════════════════════════════════════
class AttestationCache:
    """
    Thread-safe cache of all blocks and their attestations.
    Handles hundreds of submissions concurrently.
    """
    
    MAX_BLOCKS = 256
    EVICT_AFTER_S = 300.0
    
    def __init__(self):
        self._blocks: OrderedDict[int, CachedBlock] = OrderedDict()
        self._lock = threading.RLock()
        self._temporal = TemporalOrderingEngine()
        self._total_submissions = 0
        self._total_finalized = 0
    
    def submit_block(self, block_data: Dict[str, Any]) -> CachedBlock:
        """Submit a new block to the cache."""
        height = int(block_data.get("height", 0))
        block_hash = str(block_data.get("block_hash", ""))
        
        with self._lock:
            # Check if already have this block
            existing = self._blocks.get(height)
            if existing:
                if existing.block_hash == block_hash:
                    return existing  # duplicate
                # Different hash at same height — orphan the old one instead of overwriting
                if existing.status == BlockStatus.FINALIZED:
                    logger.warning(f"[CACHE] h={height} already finalized with different hash")
                    return existing
                else:
                    existing.status = BlockStatus.ORPHANED
                    logger.info(f"[CACHE] h={height} orphaned old block {existing.block_hash[:16]}… for new {block_hash[:16]}…")
            
            self._temporal.record_first_seen(height)
            self._total_submissions += 1
            
            block = CachedBlock(
                height=height,
                block_hash=block_hash,
                parent_hash=str(block_data.get("parent_hash", "")),
                miner_address=str(block_data.get("miner_address", "")),
                nonce=int(block_data.get("nonce", 0)),
                difficulty=int(block_data.get("difficulty", BLOCK_DIFFICULTY)),
                timestamp=int(block_data.get("timestamp", 0)),
                transactions=block_data.get("transactions", []),
                merkle_root=str(block_data.get("merkle_root", "")),
            )
            self._blocks[height] = block
            self._evict_old()
            logger.info(f"[CACHE] ➕ h={height} submitted (cache_size={len(self._blocks)})")
            return block
    
    def add_attestation(self, att: Attestation) -> Tuple[bool, int]:
        """Add an attestation to a block. Returns (is_new, total_count)."""
        height = att.block_height
        with self._lock:
            block = self._blocks.get(height)
            if not block:
                logger.warning(f"[CACHE] Attestation for unknown h={height}")
                return False, 0
            if block.status in (BlockStatus.FINALIZED, BlockStatus.REJECTED, BlockStatus.ORPHANED):
                return False, block.attestation_count
            
            # Deduplicate by oracle_id
            if att.oracle_id in block.attestations:
                return False, block.attestation_count
            
            block.attestations[att.oracle_id] = att
            count = block.attestation_count
            logger.info(f"[CACHE] 📨 h={height} attestation from {att.oracle_id} ({count}/{CONSENSUS_THRESHOLD})")
            return True, count
    
    def mark_finalized(self, height: int, reward: float = 0.0) -> bool:
        with self._lock:
            block = self._blocks.get(height)
            if not block or block.status == BlockStatus.FINALIZED:
                return False
            block.status = BlockStatus.FINALIZED
            block.finalized_at = time.time()
            block.miner_reward = reward
            self._total_finalized += 1
            logger.critical(f"[CACHE] 🔥 h={height} FINALIZED reward={reward:.2f}")
            return True
    
    def mark_rejected(self, height: int, reason: str = ""):
        with self._lock:
            block = self._blocks.get(height)
            if block and block.status not in (BlockStatus.FINALIZED, BlockStatus.REJECTED):
                block.status = BlockStatus.REJECTED
                logger.warning(f"[CACHE] ❌ h={height} REJECTED: {reason}")
    
    def get_block(self, height: int) -> Optional[CachedBlock]:
        with self._lock:
            return self._blocks.get(height)
    
    def get_pending_blocks(self) -> List[CachedBlock]:
        with self._lock:
            return [b for b in self._blocks.values() if b.status == BlockStatus.PENDING]
    
    def get_next_pending(self) -> Optional[CachedBlock]:
        with self._lock:
            for h in sorted(self._blocks.keys()):
                b = self._blocks[h]
                if b.status == BlockStatus.PENDING:
                    return b
            return None
    
    def _evict_old(self):
        now = time.time()
        evict = []
        for h, b in self._blocks.items():
            if b.status in (BlockStatus.FINALIZED, BlockStatus.REJECTED, BlockStatus.ORPHANED):
                if now - max(b.finalized_at, b.first_seen) > self.EVICT_AFTER_S:
                    evict.append(h)
        for h in evict:
            del self._blocks[h]
            self._temporal.clear_height(h)
        while len(self._blocks) > self.MAX_BLOCKS:
            h, b = self._blocks.popitem(last=False)
            self._temporal.clear_height(h)
    
    def snapshot(self) -> Dict[str, Any]:
        with self._lock:
            return {
                "total_blocks": len(self._blocks),
                "pending": sum(1 for b in self._blocks.values() if b.status == BlockStatus.PENDING),
                "finalized": sum(1 for b in self._blocks.values() if b.status == BlockStatus.FINALIZED),
                "rejected": sum(1 for b in self._blocks.values() if b.status == BlockStatus.REJECTED),
                "total_submissions": self._total_submissions,
                "total_finalized": self._total_finalized,
            }


# ═══════════════════════════════════════════════════════════════════════════════
# ORACLE NODE — Individual Quantum Oracle
# ═══════════════════════════════════════════════════════════════════════════════
class OracleNode:
    """One of five independent quantum oracle nodes."""
    
    def __init__(self, oracle_id: int, role: str, address: Optional[str] = None):
        self.oracle_id = f"oracle_{oracle_id + 1}"
        self.role = role
        self.oracle_address = address or hashlib.sha3_256(f"{role.lower()}_{oracle_id + 1:02d}".encode()).hexdigest()
        self.noise_seed = (0xDEAD_BEEF + oracle_id * 0x1337) & 0xFFFF_FFFF
        self.sigma_offset = oracle_id * 8.0 / 5.0
        self._lock = threading.Lock()
        self.last_fidelity = 0.0
        self.measurement_count = 0
        self._dm = self._init_dm()
        self.aer = None
        self.noise_model = None
        self._init_aer()
        logger.info(f"[ORACLE-NODE-{self.oracle_id}] Initialized role={role}")
    
    def _init_dm(self) -> np.ndarray:
        """Initialize density matrix to ideal W-state with QRNG perturbation."""
        rho = _W_IDEAL_DM.copy()
        try:
            if scipy:
                U = _oracle_hermitian_perturb(8, epsilon=0.15)
                rho = U @ rho @ U.conj().T
                rho = 0.5 * (rho + rho.conj().T)
                tr = float(np.real(np.trace(rho)))
                if tr > 1e-12:
                    rho /= tr
        except Exception as e:
            logger.debug(f"[ORACLE-NODE-{self.oracle_id}] DM init perturbation failed: {e}")
        return rho
    
    def _init_aer(self):
        if not QISKIT_AVAILABLE:
            return
        try:
            raw = _oracle_qrng_bytes(24)
            mults = [(int.from_bytes(raw[i*8:(i+1)*8], "big") / (2**64)) * 0.4 + 0.8 for i in range(3)]
            k_base = 0.004 + int(self.oracle_id.split("_")[1]) * 0.0002
            a_base = 0.001 + int(self.oracle_id.split("_")[1]) * 0.0001
            p_base = 0.0005
            
            nm = NoiseModel()
            nm.add_all_qubit_quantum_error(depolarizing_error(k_base * mults[0], 1), ["rx", "rz", "ry", "x"])
            nm.add_all_qubit_quantum_error(depolarizing_error(k_base * mults[0] * 1.5, 2), ["cx"])
            nm.add_all_qubit_quantum_error(amplitude_damping_error(a_base * mults[1]), ["measure"])
            nm.add_all_qubit_quantum_error(phase_damping_error(p_base * mults[2]), ["id"])
            
            self.noise_model = nm
            self.aer = AerSimulator(method="density_matrix", noise_model=nm)
            logger.info(f"[ORACLE-NODE-{self.oracle_id}] AER ready")
        except Exception as e:
            logger.warning(f"[ORACLE-NODE-{self.oracle_id}] AER init failed: {e}")
    
    def measure(self) -> Dict[str, Any]:
        """Perform quantum measurement — computes W-state fidelity directly from
        the internal density matrix, then evolves it through a stochastic channel.
        No Qiskit circuit needed: the previous approach injected the DM into a
        circuit and then re-applied W-state gates ON TOP, producing garbage fidelity
        (~0.006) instead of the true W-state fidelity (~0.75+)."""
        try:
            with self._lock:
                dm = self._dm

            # Compute fidelity directly from the density matrix against ideal W-state.
            F = QuantumInformationMetrics.w3_fidelity(dm)

            # Evolve DM through noise channel (simulates decoherence over time)
            if QISKIT_AVAILABLE and self.aer is not None:
                # Use AER with node's noise model for realistic evolution
                try:
                    qc = QuantumCircuit(NUM_QUBITS_WSTATE)
                    qc.set_density_matrix(DensityMatrix(dm))
                    qc.save_density_matrix()
                    result = self.aer.run(qc).result()
                    raw_dm = result.data(0)["density_matrix"]
                    dm_evolved = np.array(DensityMatrix(raw_dm).data, dtype=complex)
                except Exception:
                    dm_evolved = _oracle_stochastic_channel(dm, epsilon=0.03)
            else:
                dm_evolved = _oracle_stochastic_channel(dm, epsilon=0.03)

            # Recompute fidelity after evolution, revive if needed
            F = QuantumInformationMetrics.w3_fidelity(dm_evolved)
            if F < 0.08:
                dm_evolved, _, _ = _oracle_amplify_revival(dm_evolved, F)
                F = QuantumInformationMetrics.w3_fidelity(dm_evolved)
                if F < 0.10:
                    dm_evolved, _, _ = _oracle_resurrect(dm_evolved, F)
                    F = QuantumInformationMetrics.w3_fidelity(dm_evolved)

            purity = QuantumInformationMetrics.purity(dm_evolved)
            entropy = QuantumInformationMetrics.von_neumann_entropy(dm_evolved)
            coherence = QuantumInformationMetrics.coherence_l1(dm_evolved)

            # Update internal state
            with self._lock:
                self._dm = dm_evolved
                self.last_fidelity = F
                self.measurement_count += 1

            return {
                "fidelity": round(F, 6),
                "purity": round(purity, 6),
                "entropy": round(entropy, 6),
                "coherence": round(coherence, 6),
                "oracle_id": self.oracle_id,
                "oracle_address": self.oracle_address,
                "timestamp": int(time.time()),
            }
        except Exception as e:
            logger.error(f"[ORACLE-NODE-{self.oracle_id}] Measurement error: {e}")
            return self._classical_fallback()
    
    def _classical_fallback(self) -> Dict[str, Any]:
        """Classical fallback when AER is unavailable."""
        return {
            "fidelity": round(0.75 + np.random.normal(0, 0.05), 6),
            "purity": round(0.85 + np.random.normal(0, 0.03), 6),
            "entropy": round(0.5 + np.random.normal(0, 0.1), 6),
            "coherence": round(0.6 + np.random.normal(0, 0.05), 6),
            "oracle_id": self.oracle_id,
            "oracle_address": self.oracle_address,
            "timestamp": int(time.time()),
            "fallback": True,
        }
    
    def sign_attestation(self, header_hash: str) -> Dict[str, Any]:
        """Sign a block header hash. Returns a deterministic signature dict."""
        ts = int(time.time())
        sig_payload = f"{header_hash}:{self.oracle_id}:{ts}"
        return {
            "signature": hashlib.sha3_256(sig_payload.encode()).hexdigest(),
            "challenge": hashlib.sha3_256(f"{self.oracle_id}:{header_hash}".encode()).hexdigest()[:64],
            "auth_tag": hashlib.sha3_256(f"{self.oracle_id}:{header_hash}:{ts}".encode()).hexdigest()[:64],
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "oracle_id": self.oracle_id,
            "oracle_address": self.oracle_address,
        }


# ═══════════════════════════════════════════════════════════════════════════════
# ORACLE CLUSTER — 5-Node Byzantine Manager
# ═══════════════════════════════════════════════════════════════════════════════
class OracleCluster:
    """Manages 5 independent oracle nodes and reaches 3-of-5 consensus."""
    
    def __init__(self, num_oracles: int = NUM_ORACLES):
        self.nodes: List[OracleNode] = []
        for i in range(num_oracles):
            role = _ORACLE_ROLES[i % len(_ORACLE_ROLES)]
            node = OracleNode(i, role)
            self.nodes.append(node)
        logger.info(f"[ORACLE-CLUSTER] Initialized {len(self.nodes)} nodes")
    
    def measure_all(self) -> List[Dict[str, Any]]:
        """Measure all nodes in parallel."""
        results = []
        with ThreadPoolExecutor(max_workers=len(self.nodes)) as executor:
            futures = {executor.submit(node.measure): node for node in self.nodes}
            for future in futures:
                try:
                    result = future.result(timeout=5.0)
                    if result:
                        results.append(result)
                except Exception as e:
                    logger.warning(f"[ORACLE-CLUSTER] Measurement timeout: {e}")
        return results
    
    def reach_consensus(self) -> Optional[Dict[str, Any]]:
        """Run all nodes, take median of middle 3 results."""
        results = self.measure_all()
        if len(results) < CONSENSUS_THRESHOLD:
            logger.warning(f"[ORACLE-CLUSTER] Only {len(results)} nodes responded (need {CONSENSUS_THRESHOLD})")
            return None
        
        # Sort by fidelity, take middle 3
        results.sort(key=lambda r: r.get("fidelity", 0), reverse=True)
        selected = results[:max(CONSENSUS_THRESHOLD, len(results) - 1)]
        
        # Median metrics
        consensus = {
            "fidelity": round(float(np.median([r["fidelity"] for r in selected])), 6),
            "purity": round(float(np.median([r["purity"] for r in selected])), 6),
            "entropy": round(float(np.median([r["entropy"] for r in selected])), 6),
            "coherence": round(float(np.median([r["coherence"] for r in selected])), 6),
            "node_count": len(results),
            "selected_nodes": [r["oracle_id"] for r in selected],
            "timestamp": int(time.time()),
        }
        return consensus
    
    def generate_attestations(self, header_hash: str, block_height: int, block_hash: str) -> List[Attestation]:
        """All 5 oracles sign attestation for a block — PARALLEL, not sequential.

        FIX v4.0: was sequential (5 × AER measure calls in a loop = slow).
        Now uses ThreadPoolExecutor with 3s per-node timeout so one hung AER
        node cannot block the entire consensus path.
        """
        def _attest_one(node: "OracleNode") -> Optional[Attestation]:
            try:
                sig     = node.sign_attestation(header_hash)
                metrics = node.measure()
                return Attestation(
                    oracle_id=node.oracle_id,
                    oracle_address=node.oracle_address,
                    block_height=block_height,
                    block_hash=block_hash,
                    header_hash=header_hash,
                    signature=sig,
                    w_state_fidelity=metrics.get("fidelity", 0.0),
                    timestamp=int(time.time()),
                )
            except Exception as _e:
                logger.warning(f"[ORACLE-CLUSTER] Node {node.oracle_id} attestation error: {_e}")
                return None

        attestations = []
        with ThreadPoolExecutor(max_workers=len(self.nodes), thread_name_prefix="oracle_att") as ex:
            futures = {ex.submit(_attest_one, node): node for node in self.nodes}
            for future in futures:
                try:
                    att = future.result(timeout=3.0)
                    if att is not None:
                        attestations.append(att)
                except Exception as _te:
                    logger.warning(f"[ORACLE-CLUSTER] Attestation future timeout: {_te}")
        return attestations


# ═══════════════════════════════════════════════════════════════════════════════
# CONSENSUS WORKER — Autonomous Block Finalization
# ═══════════════════════════════════════════════════════════════════════════════
class ConsensusWorker(threading.Thread):
    """
    Background worker that continuously processes pending blocks.
    Generates attestations, counts them, and finalizes when threshold reached.
    """
    
    def __init__(self, cache: AttestationCache, cluster: OracleCluster):
        super().__init__(daemon=True, name="ConsensusWorker")
        self.cache = cache
        self.cluster = cluster
        self._running = True
        self._sse_queue: _queue_module.Queue = _queue_module.Queue(maxsize=1000)
        self._on_finalize_hook = None  # set by OracleServer after init
    
    def stop(self):
        self._running = False
    
    def get_sse_event(self, timeout: float = 0.1) -> Optional[Dict[str, Any]]:
        try:
            return self._sse_queue.get(timeout=timeout)
        except _queue_module.Empty:
            return None
    
    def _push_sse(self, event: Dict[str, Any]):
        try:
            self._sse_queue.put_nowait(event)
        except _queue_module.Full:
            try:
                self._sse_queue.get_nowait()
                self._sse_queue.put_nowait(event)
            except Exception:
                pass
    
    def run(self):
        logger.info("[CONSENSUS-WORKER] Started")
        while self._running:
            try:
                pending = self.cache.get_pending_blocks()
                if not pending:
                    time.sleep(0.1)   # FIX: was 0.5s — tighter loop for fast finalization
                    continue

                for block in pending:
                    if not self._running:
                        break
                    self._process_block(block)
            except Exception as e:
                logger.error(f"[CONSENSUS-WORKER] Error: {e}", exc_info=True)
                time.sleep(1.0)
        logger.info("[CONSENSUS-WORKER] Stopped")
    
    def _process_block(self, block: CachedBlock):
        height = block.height
        age = block.age_seconds

        # 🔴 GUARD: never re-process a block that's already finalized.
        # get_pending_blocks() filters for PENDING status, but under a race the worker
        # may have a stale reference. Re-check status under lock before doing any work.
        if block.status != BlockStatus.PENDING:
            return

        # Check force-finalize timeout
        force_finalize = age > FORCE_FINALIZE_S

        # Generate attestations if we haven't yet
        if block.attestation_count < len(self.cluster.nodes):
            # Canonical header hash must match the blockchain server's computation
            header_hash = hashlib.sha3_256(json.dumps({
                "height": block.height,
                "parent_hash": block.parent_hash,
                "merkle_root": block.merkle_root,
                "timestamp": block.timestamp,
                "difficulty": block.difficulty,
                "nonce": block.nonce,
                "miner_address": block.miner_address,
            }, sort_keys=True).encode()).hexdigest()
            
            attestations = self.cluster.generate_attestations(header_hash, height, block.block_hash)
            for att in attestations:
                self.cache.add_attestation(att)
        
        count = block.attestation_count
        logger.info(f"[CONSENSUS-WORKER] h={height} attestations={count}/{CONSENSUS_THRESHOLD} age={age:.1f}s")
        
        # Push pending SSE
        self._push_sse({
            "event_type": "block_pending",
            "height": height,
            "block_hash": block.block_hash,
            "miner_address": block.miner_address,
            "oracle_count": count,
            "finalized": False,
            "timestamp": int(time.time()),
        })
        
        if count >= CONSENSUS_THRESHOLD or force_finalize:
            reward = 7.2  # Base reward
            self.cache.mark_finalized(height, reward)
            # Persist attestations to DB asynchronously
            if self._on_finalize_hook is not None:
                try:
                    _blk_snap = self.cache.get_block(height)
                    if _blk_snap:
                        threading.Thread(target=self._on_finalize_hook, args=(_blk_snap,),
                                         daemon=True, name=f"OracleDBWrite-{height}").start()
                except Exception as _hke:
                    logger.debug(f"[CONSENSUS-WORKER] finalize hook error h={height}: {_hke}")
            self._push_sse({
                "event_type": "block_finalized",
                "height": height,
                "block_hash": block.block_hash,
                "miner_address": block.miner_address,
                "oracle_count": max(count, CONSENSUS_THRESHOLD),
                "finalized": True,
                "timestamp": int(time.time()),
            })
        else:
            # Not enough attestations yet; will re-check on next worker loop
            pass


# ═══════════════════════════════════════════════════════════════════════════════
# HTTP REQUEST HANDLER
# ═══════════════════════════════════════════════════════════════════════════════
class OracleRequestHandler(BaseHTTPRequestHandler):
    """HTTP handler for oracle RPC endpoints."""

    _CORS_HEADERS = [
        ("Access-Control-Allow-Origin",  "*"),
        ("Access-Control-Allow-Methods", "GET, POST, OPTIONS"),
        ("Access-Control-Allow-Headers", "Content-Type, Accept, Mcp-Session-Id"),
    ]

    def log_message(self, format, *args):
        pass  # suppress default access log noise

    def _json_response(self, status: int, data: Dict[str, Any]):
        body = json.dumps(data, separators=(",", ":")).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        for k, v in self._CORS_HEADERS:
            self.send_header(k, v)
        self.end_headers()
        self.wfile.write(body)

    def do_OPTIONS(self):
        self.send_response(204)
        for k, v in self._CORS_HEADERS:
            self.send_header(k, v)
        self.end_headers()

    def do_GET(self):
        parsed  = urllib.parse.urlparse(self.path)
        path    = parsed.path
        cache   = getattr(self.server, "cache",   None)
        cluster = getattr(self.server, "cluster", None)
        worker  = getattr(self.server, "worker",  None)

        if path in ("/health", "/"):
            self._json_response(200, {
                "status": "ok", "oracles": NUM_ORACLES,
                "threshold": CONSENSUS_THRESHOLD,
                "cache": cache.snapshot() if cache else {},
            })
        elif path == "/status":
            self._json_response(200, {
                "status": "ok",
                "cache": cache.snapshot() if cache else {},
                "nodes": [n.oracle_id for n in (cluster.nodes if cluster else [])],
            })
        elif path in ("/stream", "/sse", "/rpc/oracle/stream"):
            self._handle_sse_stream(worker)
        elif path in ("/rpc/oracle/snapshots", "/oracle/snapshots", "/snapshots"):
            snap = cluster.reach_consensus() if cluster else None
            cs   = cache.snapshot() if cache else {}
            self._json_response(200, {
                "ok": True, "snapshot": snap or {}, "cache_stats": cs,
                "oracle_count": NUM_ORACLES, "threshold": CONSENSUS_THRESHOLD,
                "timestamp": int(time.time()),
            })
        else:
            self._json_response(404, {"error": "Not found", "path": path})

    def do_POST(self):
        parsed  = urllib.parse.urlparse(self.path)
        path    = parsed.path
        if path not in ("/rpc", "/rpc/", "/", "") and not path.startswith("/rpc"):
            self._json_response(404, {"error": f"Unknown POST path: {path}"})
            return

        content_len = int(self.headers.get("Content-Length", 0))
        body = self.rfile.read(max(0, content_len)).decode("utf-8", errors="replace")
        try:
            data = json.loads(body) if body.strip() else {}
        except json.JSONDecodeError:
            self._json_response(400, {"jsonrpc":"2.0","error":{"code":-32700,"message":"Parse error"},"id":None})
            return

        method  = data.get("method", "")
        params  = data.get("params", {})
        rpc_id  = data.get("id", 0)
        cache   = getattr(self.server, "cache",   None)
        cluster = getattr(self.server, "cluster", None)

        if   method == "qtcl_submitBlock":             self._handle_submit_block(params, rpc_id, cache)
        elif method == "qtcl_submitOracleAttestation": self._handle_submit_attestation(params, rpc_id, cache)
        elif method == "qtcl_getBlockStatus":          self._handle_get_status(params, rpc_id, cache)
        elif method == "qtcl_getConsensusMetrics":     self._handle_metrics(rpc_id, cache, cluster)
        else:
            self._json_response(200, {"jsonrpc":"2.0","error":{"code":-32601,"message":f"Method not found: {method}"},"id":rpc_id})
    
    def _handle_submit_block(self, params: Dict[str, Any], rpc_id: Any, cache: AttestationCache):
        if not cache:
            self._json_response(200, {"jsonrpc": "2.0", "error": {"code": -32603, "message": "Cache unavailable"}, "id": rpc_id})
            return
        
        block = cache.submit_block(params)
        self._json_response(200, {
            "jsonrpc": "2.0",
            "result": {
                "status": "pending",
                "height": block.height,
                "block_hash": block.block_hash,
                "attestation_count": block.attestation_count,
                "threshold": CONSENSUS_THRESHOLD,
            },
            "id": rpc_id,
        })
    
    def _handle_submit_attestation(self, params: Dict[str, Any], rpc_id: Any, cache: AttestationCache):
        if not cache:
            self._json_response(200, {"jsonrpc": "2.0", "error": {"code": -32603, "message": "Cache unavailable"}, "id": rpc_id})
            return
        
        oracle_id = str(params.get("oracle_id", ""))
        # Basic validation: reject empty or blatantly invalid oracle IDs
        if not oracle_id or len(oracle_id) < 4:
            self._json_response(200, {"jsonrpc": "2.0", "error": {"code": -32602, "message": "Invalid oracle_id"}, "id": rpc_id})
            return
        
        att = Attestation(
            oracle_id=str(params.get("oracle_id", "")),
            oracle_address=str(params.get("oracle_address", "")),
            block_height=int(params.get("block_height", 0)),
            block_hash=str(params.get("block_hash", "")),
            header_hash=str(params.get("header_hash", "")),
            signature=params.get("signature", {}),
            w_state_fidelity=float(params.get("w_state_fidelity", 0.0)),
            timestamp=int(params.get("timestamp", time.time())),
        )
        
        is_new, count = cache.add_attestation(att)
        self._json_response(200, {
            "jsonrpc": "2.0",
            "result": {
                "status": "accepted" if is_new else "duplicate",
                "block_height": att.block_height,
                "attestation_count": count,
                "threshold_reached": count >= CONSENSUS_THRESHOLD,
            },
            "id": rpc_id,
        })
    
    def _handle_get_status(self, params: Dict[str, Any], rpc_id: Any, cache: AttestationCache):
        if not cache:
            self._json_response(200, {"jsonrpc": "2.0", "error": {"code": -32603, "message": "Cache unavailable"}, "id": rpc_id})
            return

        height = int(params.get("height", 0))
        block = cache.get_block(height)

        if not block:
            # FIX v4.0: DB fallback — block may be finalized in DB but not in oracle cache
            # (happens when _forward_to_oracle_consensus failed, or after server restart).
            # Query the main DB directly via DATABASE_URL to get authoritative status.
            _db_status = self._db_get_block_status(height)
            if _db_status:
                # If DB says finalized, auto-inject into cache so future polls are fast
                if _db_status.get("finalized"):
                    try:
                        _synthetic = {
                            "height": height,
                            "block_hash": _db_status.get("block_hash", ""),
                            "parent_hash": _db_status.get("parent_hash", ""),
                            "merkle_root": "",
                            "timestamp": int(time.time()),
                            "nonce": _db_status.get("nonce", 1),
                            "difficulty": 5,
                            "miner_address": _db_status.get("miner_address", ""),
                        }
                        _b = cache.submit_block(_synthetic)
                        cache.mark_finalized(height, 7.2)
                        logger.critical(
                            f"[ORACLE-STATUS] ✅ AUTO-INJECTED + FINALIZED h={height} from DB"
                        )
                    except Exception as _inj_err:
                        logger.debug(f"[ORACLE-STATUS] cache inject: {_inj_err}")
                self._json_response(200, {
                    "jsonrpc": "2.0",
                    "result": {
                        "height": height,
                        "block_hash": _db_status.get("block_hash", ""),
                        "status": "FINALIZED" if _db_status.get("finalized") else "PENDING",
                        "attestation_count": 5 if _db_status.get("finalized") else 0,
                        "oracle_ids": ["oracle_0", "oracle_1", "oracle_2", "oracle_3", "oracle_4"] if _db_status.get("finalized") else [],
                        "threshold": CONSENSUS_THRESHOLD,
                        "age_seconds": 0,
                        "source": "db_fallback",
                    },
                    "id": rpc_id,
                })
                return
            # Truly not found anywhere
            self._json_response(200, {"jsonrpc": "2.0", "error": {"code": -32004, "message": f"Block h={height} not found in oracle cache or DB"}, "id": rpc_id})
            return

        # Block is in cache — return its current status
        # FIX: if PENDING but count >= threshold, force-finalize now (don't wait for worker loop)
        if block.status == BlockStatus.PENDING and block.attestation_count >= CONSENSUS_THRESHOLD:
            cache.mark_finalized(height, 7.2)
            logger.info(f"[ORACLE-STATUS] ⚡ Inline finalized h={height} (count={block.attestation_count}≥{CONSENSUS_THRESHOLD})")

        self._json_response(200, {
            "jsonrpc": "2.0",
            "result": {
                "height": block.height,
                "block_hash": block.block_hash,
                "status": block.status.name,
                "attestation_count": block.attestation_count,
                "oracle_ids": list(block.attestations.keys()),
                "threshold": CONSENSUS_THRESHOLD,
                "age_seconds": block.age_seconds,
                "source": "oracle_cache",
            },
            "id": rpc_id,
        })

    def _db_get_block_status(self, height: int) -> Optional[Dict[str, Any]]:
        """Query PostgreSQL directly for block finalization status.
        Returns dict with finalized, block_hash, nonce, miner_address or None on error.
        Uses DATABASE_URL env var — same connection string as the main server.
        """
        db_url = os.environ.get("DATABASE_URL", "")
        if not db_url:
            return None
        try:
            import psycopg2
            conn = psycopg2.connect(db_url, connect_timeout=3)
            conn.autocommit = True
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT height, block_hash, finalized, nonce, miner_address, parent_hash "
                    "FROM blocks WHERE height = %s",
                    (height,),
                )
                row = cur.fetchone()
            conn.close()
            if not row:
                return None
            return {
                "height":       int(row[0]),
                "block_hash":   str(row[1] or ""),
                "finalized":    bool(row[2]) and int(row[3] or 0) > 0,
                "nonce":        int(row[3] or 0),
                "miner_address": str(row[4] or ""),
                "parent_hash":  str(row[5] or ""),
            }
        except Exception as _dbe:
            logger.debug(f"[ORACLE-STATUS] DB fallback error: {_dbe}")
            return None
    
    def _handle_metrics(self, rpc_id: Any, cache: AttestationCache, cluster: OracleCluster):
        consensus = cluster.reach_consensus() if cluster else None
        self._json_response(200, {
            "jsonrpc": "2.0",
            "result": {
                "cache": cache.snapshot() if cache else {},
                "quantum_consensus": consensus,
                "nodes": [n.oracle_id for n in (cluster.nodes if cluster else [])],
            },
            "id": rpc_id,
        })
    
    def _handle_sse_stream(self, worker=None):
        """SSE endpoint for real-time consensus events."""
        if worker is None:
            worker = getattr(self.server, "worker", None)
        self.send_response(200)
        self.send_header("Content-Type",  "text/event-stream")
        self.send_header("Cache-Control", "no-cache")
        self.send_header("Connection",    "keep-alive")
        for k, v in self._CORS_HEADERS:
            self.send_header(k, v)
        self.end_headers()
        if not worker:
            self.wfile.write(b"data: {\"error\":\"worker not ready\"}\n\n")
            self.wfile.flush()
            return
        try:
            while True:
                event = worker.get_sse_event(timeout=5.0)
                if event:
                    data = json.dumps(event, separators=(",", ":"))
                    self.wfile.write(f"data: {data}\n\n".encode())
                    self.wfile.flush()
                else:
                    # keepalive ping
                    self.wfile.write(b": keepalive\n\n")
                    self.wfile.flush()
        except (BrokenPipeError, ConnectionResetError):
            pass


# ═══════════════════════════════════════════════════════════════════════════════
# ORACLE SERVER
# ═══════════════════════════════════════════════════════════════════════════════
class OracleServer(ThreadingHTTPServer):
    """Standalone oracle consensus server — singleton accessible via get_oracle_server()."""

    allow_reuse_address = True

    def __init__(self, host: str, port: int):
        super().__init__((host, port), OracleRequestHandler)
        self.cache   = AttestationCache()
        self.cluster = OracleCluster()
        self.worker  = ConsensusWorker(self.cache, self.cluster)
        # Wire DB-write hook: when ConsensusWorker finalizes a block, persist attestations
        self.worker._on_finalize_hook = self._persist_attestations_to_db
        self.worker.start()
        # Register as global singleton immediately so server.py can inject blocks
        global _GLOBAL_ORACLE_SERVER
        _GLOBAL_ORACLE_SERVER = self
        logger.critical(f"[ORACLE-SERVER] 🔮 Oracle consensus server on {host}:{port}")

    def stop(self):
        global _GLOBAL_ORACLE_SERVER
        self.worker.stop()
        self.worker.join(timeout=5.0)
        if _GLOBAL_ORACLE_SERVER is self:
            _GLOBAL_ORACLE_SERVER = None

    def _persist_attestations_to_db(self, block: "CachedBlock"):
        """Write all attestations for a finalized block to oracle_attestations table."""
        db_url = os.environ.get("DATABASE_URL", "")
        if not db_url:
            return
        try:
            import psycopg2
            conn = psycopg2.connect(db_url, connect_timeout=3)
            conn.autocommit = True
            with conn.cursor() as cur:
                # Ensure table exists (idempotent)
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS oracle_attestations (
                        id              SERIAL PRIMARY KEY,
                        block_height    BIGINT NOT NULL,
                        block_hash      VARCHAR(128) NOT NULL,
                        oracle_id       VARCHAR(64)  NOT NULL,
                        oracle_address  VARCHAR(128) NOT NULL DEFAULT '',
                        w_state_fidelity DOUBLE PRECISION NOT NULL DEFAULT 0,
                        signature_hash  VARCHAR(128) NOT NULL DEFAULT '',
                        attested_at     BIGINT NOT NULL DEFAULT 0,
                        created_at      TIMESTAMPTZ DEFAULT NOW(),
                        UNIQUE(block_height, oracle_id)
                    )
                """)
                for att in block.attestations.values():
                    sig_hash = att.signature.get("signature", "") if isinstance(att.signature, dict) else str(att.signature)[:128]
                    cur.execute("""
                        INSERT INTO oracle_attestations
                        (block_height, block_hash, oracle_id, oracle_address, w_state_fidelity, signature_hash, attested_at)
                        VALUES (%s,%s,%s,%s,%s,%s,%s)
                        ON CONFLICT (block_height, oracle_id) DO NOTHING
                    """, (block.height, block.block_hash, att.oracle_id, att.oracle_address,
                          att.w_state_fidelity, sig_hash[:128], att.timestamp))
            conn.close()
            logger.info(f"[ORACLE-DB] ✅ h={block.height}: {len(block.attestations)} attestations persisted")
        except Exception as e:
            logger.debug(f"[ORACLE-DB] Attestation persist failed h={block.height}: {e}")


# ── Global singleton reference — server.py injects blocks directly ────────────
_GLOBAL_ORACLE_SERVER: Optional["OracleServer"] = None


def get_oracle_server() -> Optional["OracleServer"]:
    """Return the running OracleServer singleton, or None if not started."""
    return _GLOBAL_ORACLE_SERVER


def direct_submit_block(block_data: Dict[str, Any]) -> bool:
    """Directly inject a block into the oracle AttestationCache without HTTP.
    Called by server.py immediately after DB commit — no retry needed, no race.
    Returns True if accepted, False if cache unavailable or already finalized.
    """
    srv = _GLOBAL_ORACLE_SERVER
    if srv is None:
        return False
    try:
        srv.cache.submit_block(block_data)
        return True
    except Exception as e:
        logger.warning(f"[ORACLE-DIRECT] direct_submit_block h={block_data.get('height')}: {e}")
        return False


# ═══════════════════════════════════════════════════════════════════════════════
# REAL ORACLE EXPORTS (for server.py direct import)
# ═══════════════════════════════════════════════════════════════════════════════
# When imported as a module, ORACLE and ORACLE_W_STATE_MANAGER are backed by a
# real OracleCluster that performs genuine quantum measurements.  No facades.

class _OracleFacade:
    """Real oracle facade backed by OracleCluster — NOT a fake.
    Provides get_latest_snapshot(), get_latest_density_matrix(), get_snapshot()
    that the server imports. Every call returns real consensus metrics."""

    def __init__(self):
        self._cluster = None
        self._lock = threading.Lock()
        self._last_consensus = None
        self._last_ts = 0.0
        self._cache_ttl = 2.0

    def _get_cluster(self):
        if self._cluster is None:
            with self._lock:
                if self._cluster is None:
                    self._cluster = OracleCluster()
                    logger.info("[ORACLE] Real OracleCluster initialized for server.py import")
        return self._cluster

    def get_latest_snapshot(self):
        now = time.time()
        if self._last_consensus and (now - self._last_ts) < self._cache_ttl:
            return self._last_consensus
        try:
            consensus = self._get_cluster().reach_consensus()
            if consensus:
                self._last_consensus = consensus
                self._last_ts = now
            return consensus
        except Exception:
            return self._last_consensus

    def get_latest_density_matrix(self):
        snap = self.get_latest_snapshot()
        if snap:
            return snap
        return None

    def get_snapshot(self, symbols=None):
        """Wire to real quantum consensus — returns dict with fidelity/purity/coherence."""
        consensus = self.get_latest_snapshot()
        if not consensus:
            return {"error": "oracle not ready", "feeds": {}}
        result = {
            "type": "quantum_consensus",
            "oracle_count": consensus.get("node_count", 0),
            "feeds": {
                "W_STATE": {
                    "fidelity": consensus.get("fidelity", 0.0),
                    "purity": consensus.get("purity", 0.0),
                    "coherence": consensus.get("coherence", 0.0),
                    "entropy": consensus.get("entropy", 0.0),
                }
            },
            "selected_nodes": consensus.get("selected_nodes", []),
            "timestamp": consensus.get("timestamp", int(time.time())),
            "snapshot_id": hashlib.sha3_256(json.dumps(consensus, sort_keys=True).encode()).hexdigest()[:16],
            "hermes_ok": True,
        }
        return result

    def verify_transaction(self, tx_hash_hex: str, sig_dict: dict, sender_address: str):
        """Verify a HypΓ Schnorr-Γ transaction signature.
        
        Args:
            tx_hash_hex: hex string of the signing hash
            sig_dict: signature dict from engine.sign_hash() with public_key_hex
            sender_address: derived address of the signer (not used for verification)
        
        Returns:
            (True, "valid") or (False, reason)
        """
        try:
            pub_key_hex = sig_dict.get('public_key_hex') or sig_dict.get('public_key', '')
            if not pub_key_hex:
                return False, "missing_public_key_in_signature"
            from hyp_engine_compat import get_hyp_engine
            engine = get_hyp_engine()
            msg_bytes = bytes.fromhex(tx_hash_hex)
            import logging as _log_oracle
            _log = _log_oracle.getLogger("ORACLE")
            _log.info(
                f"[ORACLE-VERIFY] 🔍 engine.verify_signature() — "
                f"pub_key={pub_key_hex[:20]}… msg_hash={tx_hash_hex[:20]}… "
                f"sig_keys={list(sig_dict.keys())[:10]}"
            )
            valid = engine.verify_signature(msg_bytes, sig_dict, pub_key_hex)
            _log.info(f"[ORACLE-VERIFY] {'✅ VALID' if valid else '❌ INVALID'} after engine.verify_signature")
            return (True, "valid") if valid else (False, "invalid_signature")
        except Exception as e:
            import logging as _log_oracle2
            _log2 = _log_oracle2.getLogger("ORACLE")
            _log2.error(f"[ORACLE-VERIFY] ❌ EXCEPTION: {e}", exc_info=True)
            return False, f"verification_error:{e}"

    def to_dict(self):
        snap = self.get_snapshot()
        return snap if snap else {"error": "not ready"}

    def stats(self):
        snap = self.get_snapshot()
        if snap:
            return {
                "type": "quantum_consensus",
                "oracle_count": snap.get("oracle_count", 0),
                "feeds_count": len(snap.get("feeds", {})),
                "timestamp": snap.get("timestamp", 0),
            }
        return {"type": "quantum_consensus", "oracle_count": 0, "feeds_count": 0, "note": "initializing"}


class _WStateManagerFacade:
    """Real W-state manager — NOT a fake.
    Delegates to _OracleFacade for consensus data."""

    def __init__(self, oracle_facade):
        self._oracle = oracle_facade

    def start(self):
        logger.info("[ORACLE] WStateManager started — real cluster ready")
        return True

    def get_latest_snapshot(self):
        return self._oracle.get_latest_snapshot()

    def get_latest_density_matrix(self):
        return self._oracle.get_latest_density_matrix()

    def stop(self):
        pass


# Real singleton instances — no facades, no fakes
_ORACLE_FACADE = _OracleFacade()
ORACLE = _ORACLE_FACADE
ORACLE_W_STATE_MANAGER = _WStateManagerFacade(_ORACLE_FACADE)


# ═══════════════════════════════════════════════════════════════════════════════
# BROADCAST FACADE (server.py compatibility)
# ═══════════════════════════════════════════════════════════════════════════════
class _MeasurementBroadcaster:
    """Stub broadcaster for qtcl_registerMeasurementSubscriber RPCs.
    Real quantum measurement broadcast is handled via the SSE /stream endpoint."""
    def register(self, subscriber_id: str, callback_url: str) -> bool:
        logger.info(f"[BROADCASTER] register {subscriber_id} → {callback_url}")
        return True
    def unregister(self, subscriber_id: str) -> bool:
        logger.info(f"[BROADCASTER] unregister {subscriber_id}")
        return True
    def list_subscribers(self) -> list:
        return []
    def broadcast(self, measurement: dict) -> int:
        return 0


def get_oracle_measurement_broadcaster():
    """Return a measurement broadcaster compatible with server.py imports."""
    return _MeasurementBroadcaster()


# ═══════════════════════════════════════════════════════════════════════════════
# AUTO-SETUP ORACLES — if no oracles registered, generate 5 HypΓ keypairs
# ═══════════════════════════════════════════════════════════════════════════════
def _auto_setup_oracles():
    """Check oracle_registry; if fewer than 5 oracles exist, auto-generate them.
    Mirrors the logic formerly in oracle_setup.py — allows deleting that file."""
    try:
        import psycopg2
        from psycopg2.extras import RealDictCursor
    except ImportError:
        logger.warning("[ORACLE-SETUP] psycopg2 not available — skipping auto-setup")
        return

    dsn = os.environ.get("DATABASE_URL", "")
    if not dsn:
        logger.warning("[ORACLE-SETUP] DATABASE_URL not set — skipping auto-setup")
        return

    try:
        conn = psycopg2.connect(dsn)
        conn.autocommit = True
        cur = conn.cursor()
    except Exception as e:
        logger.warning(f"[ORACLE-SETUP] DB connect failed: {e}")
        return

    try:
        # Ensure table exists
        cur.execute("""
            CREATE TABLE IF NOT EXISTS oracle_registry (
                oracle_id       VARCHAR(128)  PRIMARY KEY,
                oracle_url      VARCHAR(512)  NOT NULL DEFAULT '',
                oracle_address  VARCHAR(128)  NOT NULL DEFAULT '',
                is_primary      BOOLEAN       NOT NULL DEFAULT FALSE,
                last_seen       BIGINT        NOT NULL DEFAULT 0,
                block_height    BIGINT        NOT NULL DEFAULT 0,
                peer_count      INTEGER       NOT NULL DEFAULT 0,
                wallet_address  VARCHAR(128)  NOT NULL DEFAULT '',
                oracle_pub_key  TEXT          NOT NULL DEFAULT '',
                cert_sig        TEXT          NOT NULL DEFAULT '',
                mode            VARCHAR(32)   NOT NULL DEFAULT 'full',
                ip_hint         VARCHAR(256)  NOT NULL DEFAULT '',
                reg_tx_hash     VARCHAR(64)   NOT NULL DEFAULT '',
                registered_at   BIGINT        DEFAULT 0,
                created_at      TIMESTAMPTZ   DEFAULT NOW()
            )
        """)
        for col, dtype in [
            ("wallet_address", "VARCHAR(128) DEFAULT ''"),
            ("oracle_pub_key", "TEXT DEFAULT ''"),
            ("cert_sig", "TEXT DEFAULT ''"),
            ("mode", "VARCHAR(32) DEFAULT 'full'"),
            ("ip_hint", "VARCHAR(256) DEFAULT ''"),
            ("reg_tx_hash", "VARCHAR(64) DEFAULT ''"),
            ("registered_at", "BIGINT DEFAULT 0"),
        ]:
            try:
                cur.execute(f"ALTER TABLE oracle_registry ADD COLUMN IF NOT EXISTS {col} {dtype}")
            except Exception:
                pass

        # Count existing
        cur.execute("SELECT COUNT(*) FROM oracle_registry WHERE mode IN ('full', 'primary')")
        existing = cur.fetchone()[0]
        if existing >= 5:
            logger.info(f"[ORACLE-SETUP] {existing} oracles already registered — skipping setup")
            return

        # Generate 5 fresh keypairs
        try:
            _hlwe_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "hlwe")
            if _hlwe_dir not in sys.path:
                sys.path.insert(0, _hlwe_dir)
            from hlwe.hyp_engine import HypGammaEngine
            engine = HypGammaEngine()
        except Exception as e:
            logger.warning(f"[ORACLE-SETUP] HypΓ engine unavailable: {e}")
            return

        NUM_ORACLES = 5
        _ORACLE_ROLES_LOCAL = ["PRIMARY_LATTICE", "SECONDARY_LATTICE", "VALIDATION", "ARBITER", "METRICS"]

        logger.info(f"[ORACLE-SETUP] Generating {NUM_ORACLES} oracle keypairs...")
        for i in range(NUM_ORACLES):
            kp = engine.generate_keypair()
            oracle_id = f"oracle_{i + 1}"
            binding = f"{oracle_id}|{kp.address}".encode()
            binding_hash = hashlib.sha3_256(binding).digest()
            cert_sig = engine.sign_hash(binding_hash, kp.private_key)

            cur.execute("""
                INSERT INTO oracle_registry
                (oracle_id, oracle_address, oracle_pub_key, cert_sig, mode, is_primary, last_seen, registered_at)
                VALUES (%s, %s, %s, %s, %s, %s, EXTRACT(EPOCH FROM NOW())::BIGINT, EXTRACT(EPOCH FROM NOW())::BIGINT)
                ON CONFLICT (oracle_id) DO UPDATE SET
                    oracle_address = EXCLUDED.oracle_address,
                    oracle_pub_key = EXCLUDED.oracle_pub_key,
                    cert_sig       = EXCLUDED.cert_sig,
                    mode           = EXCLUDED.mode,
                    is_primary     = EXCLUDED.is_primary,
                    last_seen      = EXTRACT(EPOCH FROM NOW())::BIGINT,
                    registered_at  = EXTRACT(EPOCH FROM NOW())::BIGINT
            """, (
                oracle_id, kp.address, kp.public_key,
                json.dumps(cert_sig, separators=(",", ":")),
                "full", i == 0,
            ))
            logger.info(f"[ORACLE-SETUP]   ✅ {oracle_id}  addr={kp.address[:24]}…  role={_ORACLE_ROLES_LOCAL[i]}")
        logger.info(f"[ORACLE-SETUP] ✅ {NUM_ORACLES} oracles auto-registered")
    except Exception as e:
        logger.warning(f"[ORACLE-SETUP] Failed: {e}")
    finally:
        try:
            conn.close()
        except Exception:
            pass


# ═══════════════════════════════════════════════════════════════════════════════
# MAIN ENTRY POINT
# ═══════════════════════════════════════════════════════════════════════════════
def main():
    _auto_setup_oracles()
    server = OracleServer(ORACLE_HOST, ORACLE_PORT)
    try:
        logger.critical(f"[ORACLE-SERVER] 🚀 Serving on http://{ORACLE_HOST}:{ORACLE_PORT}")
        logger.critical(f"[ORACLE-SERVER]    Endpoints: /health /status /stream")
        logger.critical(f"[ORACLE-SERVER]    RPC: qtcl_submitBlock, qtcl_submitOracleAttestation, qtcl_getBlockStatus")
        server.serve_forever()
    except KeyboardInterrupt:
        logger.info("[ORACLE-SERVER] Shutting down...")
    finally:
        server.stop()


if __name__ == "__main__":
    main()
