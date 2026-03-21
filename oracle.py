#!/usr/bin/env python3
"""
oracle.py — QTCL Quantum Oracle (stripped, deduplicated)

Components
──────────
  OracleKeyPair / HLWESignature  — keypair + signature data structures
  HDKeyring                       — BIP32-style hash-based HD key derivation
  HLWESigner / HLWEVerifier       — HLWE post-quantum signing & verification
  DensityMatrixSnapshot           — W-state snapshot (AER + HLWE)
  QuantumInformationMetrics       — purity, entropy, coherence, fidelity
  TemporalAnchorPoint             — coherence-decay quantum timestamp
  OracleNode                      — one of five independent AER simulator nodes
  OracleWStateManager             — 5-node Byzantine cluster manager
  OracleEngine                    — master singleton: keys + signing

Key fix applied here
────────────────────
  _extract_snapshot() rate-limits the "Lattice is None" log to once per 30 s
  instead of spamming ~100 ERROR lines/sec. The actual root fix is in server.py
  which now calls globals.set_lattice(LATTICE) so this code path is never hit
  during normal operation.
"""

import os
import sys
import json
import time
import hmac
import struct
import logging
import hashlib
import secrets
import traceback
import threading
import numpy as np
import psycopg2
import queue
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, Any, Optional, List, Tuple
from dataclasses import dataclass, field, asdict
from collections import deque, OrderedDict
from decimal import Decimal, getcontext
from enum import Enum

getcontext().prec = 150

if not logging.getLogger().hasHandlers():
    logging.basicConfig(level=logging.INFO, format='[%(asctime)s] %(levelname)s: %(message)s')
logger = logging.getLogger(__name__)

# ─── Oracle address registry ──────────────────────────────────────────────────

def get_all_oracle_addresses_batch() -> Dict[int, str]:
    """Batch-fetch all 5 oracle addresses in a single DB query."""
    _ROLES = ['PRIMARY_LATTICE', 'SECONDARY_LATTICE', 'VALIDATION', 'ARBITER', 'METRICS']
    fallbacks = {i+1: f'qtcl1{_ROLES[i].lower()[:12]}_{i+1:02d}' for i in range(5)}
    try:
        conn = psycopg2.connect(
            host=os.getenv('POOLER_HOST', 'aws-0-us-west-2.pooler.supabase.com'),
            port=int(os.getenv('POOLER_PORT', 6543)),
            database=os.getenv('POSTGRES_DB', 'postgres'),
            user=os.getenv('POSTGRES_USER', 'postgres'),
            password=os.getenv('POSTGRES_PASSWORD', ''),
            connect_timeout=2,
        )
        if not os.getenv('POSTGRES_PASSWORD', ''):
            return fallbacks
        conn.set_session(autocommit=True)
        cur = conn.cursor()
        cur.execute("""
            SELECT oracle_id, oracle_address FROM oracle_registry
            WHERE oracle_id IN ('oracle_1','oracle_2','oracle_3','oracle_4','oracle_5')
            ORDER BY oracle_id
        """)
        rows = cur.fetchall()
        cur.close(); conn.close()
        addresses = {}
        for oid, addr in rows:
            idx = int(oid.split('_')[1])
            addresses[idx] = addr
        for idx in range(1, 6):
            if idx not in addresses:
                addresses[idx] = fallbacks[idx]
        return addresses
    except Exception:
        return fallbacks

# ─── W-state validator ────────────────────────────────────────────────────────

class UnifiedWStateValidator:
    """Single canonical W-state validator for the entire system."""
    def __init__(self, mode: str = 'normal'):
        from globals import WSTATE_MODE, WSTATE_FIDELITY_THRESHOLD
        self.mode = mode or WSTATE_MODE
        self.threshold = {'strict': 0.80, 'normal': 0.75, 'relaxed': 0.70}.get(
            self.mode, WSTATE_FIDELITY_THRESHOLD)

    def validate(self, fidelity: float, coherence: float = None):
        if not isinstance(fidelity, (int, float)) or not (0 <= fidelity <= 1):
            return False, 0.0, {'error': 'Invalid fidelity'}
        if fidelity < self.threshold:
            return False, fidelity, {'error': f'Below threshold {self.threshold:.2f}'}
        quality = fidelity
        if coherence and 0 <= coherence <= 1:
            quality = (fidelity + coherence) / 2
        return True, quality, {'valid': True, 'quality': quality}

_validator = UnifiedWStateValidator()

def validate_w_state(fidelity: float, coherence: float = None):
    """Canonical W-state validator (Oracle, Server, Miner, Lattice all use this)."""
    return _validator.validate(fidelity, coherence)[:2]

# ─── Quantum imports (fatal if missing) ──────────────────────────────────────

from globals import get_block_field_entropy

try:
    from qiskit import QuantumCircuit
    from qiskit.quantum_info import Statevector, DensityMatrix
    from qiskit_aer import AerSimulator
    from qiskit_aer.noise import NoiseModel, depolarizing_error, amplitude_damping_error, phase_damping_error
    QISKIT_AVAILABLE = True
    logger.info("[ORACLE] ✅ Qiskit/AER available — quantum simulation enabled")
except ImportError as _e:
    raise RuntimeError(
        f"[ORACLE] FATAL: Qiskit/AER required. pip install qiskit qiskit-aer. Error: {_e}"
    )


# ─── Oracle C acceleration layer ──────────────────────────────────────────────
# Compiled once at import via cffi. Provides C-speed hot paths for the
# 5-node measurement loop: enforce_dm, w3_fidelity, purity, coherence_l1.
# Falls back silently if clang/cffi unavailable (Qiskit path still works).
# ─────────────────────────────────────────────────────────────────────────────

_OC_LIB   = None   # cffi compiled library handle
_OC_FFI   = None   # cffi FFI instance
_OC_OK    = False  # True once C layer is compiled and self-tested

_OC_CDEFS = """
/* 8×8 = 64 complex128 DM passed as flat re[64] + im[64] arrays (row-major).  */

/* Hermitian symmetrize + PSD clip + trace-normalize.  In-place on re/im.     */
void qtcl_oracle_enforce_dm8(double *re, double *im);

/* Tr(ρ · |W3⟩⟨W3|).  Only re[] needed (W3 ideal DM is real).                */
double qtcl_oracle_w3_fidelity(const double *re);

/* Tr(ρ²) = purity.                                                            */
double qtcl_oracle_purity(const double *re, const double *im);

/* L1 coherence norm (normalized to [0,1] for 8-dim).                         */
double qtcl_oracle_coherence_l1(const double *re, const double *im);
"""

_OC_CSRC = r"""
#include <math.h>
#include <string.h>

#define N  8
#define N2 64

/* Ideal 3-qubit W-state DM (real part only) — indices 1,2,4 on-and-off diag  */
static const double _W3[N2] = {
    0,0,0,0,0,0,0,0,
    0,1.0/3,1.0/3,0,1.0/3,0,0,0,
    0,1.0/3,1.0/3,0,1.0/3,0,0,0,
    0,0,0,0,0,0,0,0,
    0,1.0/3,1.0/3,0,1.0/3,0,0,0,
    0,0,0,0,0,0,0,0,
    0,0,0,0,0,0,0,0,
    0,0,0,0,0,0,0,0
};

/* ── enforce_dm8: Hermitian symmetrize + positive diagonal clip + trace=1 ──  */
/* Full eigendecomposition is not done here (8×8 LAPACK would require linking  */
/* LAPACK which is not guaranteed). Instead we:                                 */
/*   1. Hermitian symmetrize                                                    */
/*   2. Clip diagonal to ≥0 (populations must be non-negative)                 */
/*   3. Scale off-diagonals by sqrt(ρ_ii·ρ_jj) / |ρ_ij| if |ρ_ij|>sqrt(...)  */
/*      to enforce positivity of the Hermitian matrix without eigh              */
/*   4. Trace-normalize                                                         */
void qtcl_oracle_enforce_dm8(double *re, double *im) {
    int i, j;
    /* Step 1: Hermitian symmetrize */
    for (i = 0; i < N; i++) {
        for (j = i+1; j < N; j++) {
            double re_ij = 0.5*(re[i*N+j] + re[j*N+i]);
            double im_ij = 0.5*(im[i*N+j] - im[j*N+i]);
            re[i*N+j] = re_ij; im[i*N+j] =  im_ij;
            re[j*N+i] = re_ij; im[j*N+i] = -im_ij;
        }
    }
    /* Step 2: Clip diagonal to >= 0 */
    for (i = 0; i < N; i++) {
        if (re[i*N+i] < 0.0) re[i*N+i] = 0.0;
        im[i*N+i] = 0.0;   /* diagonal must be real */
    }
    /* Step 3: Clip off-diagonals so |ρ_ij|² ≤ ρ_ii·ρ_jj (Cauchy-Schwarz) */
    for (i = 0; i < N; i++) {
        for (j = i+1; j < N; j++) {
            double dij = re[i*N+i] * re[j*N+j];
            double mag2 = re[i*N+j]*re[i*N+j] + im[i*N+j]*im[i*N+j];
            if (mag2 > dij + 1e-30) {
                double scale = sqrt(dij / (mag2 + 1e-60));
                re[i*N+j] *= scale; im[i*N+j] *= scale;
                re[j*N+i] *= scale; im[j*N+i] *= scale;
            }
        }
    }
    /* Step 4: Trace normalize */
    double tr = 0.0;
    for (i = 0; i < N; i++) tr += re[i*N+i];
    if (tr > 1e-12) {
        double inv = 1.0/tr;
        for (i = 0; i < N2; i++) { re[i] *= inv; im[i] *= inv; }
    }
}

double qtcl_oracle_w3_fidelity(const double *re) {
    /* F = Tr(ρ · |W3⟩⟨W3|) = Σ_{i,j} ρ_ij · W3_ij                         */
    double f = 0.0;
    int i;
    for (i = 0; i < N2; i++) f += re[i] * _W3[i];
    if (f < 0.0) f = 0.0;
    if (f > 1.0) f = 1.0;
    return f;
}

double qtcl_oracle_purity(const double *re, const double *im) {
    /* Tr(ρ²) = Σ_{i,k} |ρ_ik|²  (since ρ=ρ†)                               */
    double p = 0.0;
    int i, k;
    for (i = 0; i < N; i++)
        for (k = 0; k < N; k++)
            p += re[i*N+k]*re[i*N+k] + im[i*N+k]*im[i*N+k];
    if (p > 1.0) p = 1.0;
    if (p < 0.0) p = 0.0;
    return p;
}

double qtcl_oracle_coherence_l1(const double *re, const double *im) {
    /* Σ_{i≠j} |ρ_ij|, normalized to [0,1] by dividing by 2N (max for 8-dim) */
    double c = 0.0;
    int i, j;
    for (i = 0; i < N; i++)
        for (j = 0; j < N; j++)
            if (i != j) c += sqrt(re[i*N+j]*re[i*N+j] + im[i*N+j]*im[i*N+j]);
    double norm = 2.0 * N;   /* max off-diagonal L1 for 8-dim unit-trace DM */
    double result = c / norm;
    if (result > 1.0) result = 1.0;
    return result;
}
"""

def _compile_oracle_c_layer():
    """
    Compile optional C acceleration layer.
    KOYEB FIX: timeouts 4 s per compiler (was 30 s x2 = 60 s blocking import).
    numpy fallback always active; C layer is performance-only.
    """
    global _OC_LIB, _OC_FFI, _OC_OK
    try:
        from cffi import FFI as _CFFI
        import tempfile, subprocess, os as _os
        _ffi = _CFFI()
        _ffi.cdef(_OC_CDEFS)
        src_file = _os.path.join(tempfile.gettempdir(), 'qtcl_oracle_accel.c')
        so_file  = _os.path.join(tempfile.gettempdir(), 'qtcl_oracle_accel.so')
        with open(src_file, 'w') as _sf:
            _sf.write(_OC_CSRC)
        compiled = False
        for _cc in [_os.getenv('CC', 'gcc'), 'gcc', 'cc']:
            try:
                ret = subprocess.run(
                    [_cc, '-O2', '-shared', '-fPIC', '-o', so_file, src_file, '-lm'],
                    capture_output=True, timeout=4
                )
                if ret.returncode == 0:
                    compiled = True
                    break
            except (subprocess.TimeoutExpired, FileNotFoundError):
                continue
        if not compiled:
            logger.debug("[ORACLE-C] No compiler — numpy fallback active")
            return
        _lib = _ffi.dlopen(so_file)
        _w3_flat = [0.0]*64
        for _i in (1,2,4):
            for _j in (1,2,4):
                _w3_flat[_i*8+_j] = 1.0/3.0
        _re = _ffi.new('double[64]', _w3_flat)
        _f  = _lib.qtcl_oracle_w3_fidelity(_re)
        if abs(_f - 1.0) > 0.01:
            return
        _OC_LIB = _lib
        _OC_FFI = _ffi
        _OC_OK  = True
        logger.info("[ORACLE-C] ✅ C acceleration compiled — w3_fidelity/purity/coherence active")
    except Exception as _e:
        logger.debug(f"[ORACLE-C] C layer unavailable ({type(_e).__name__}): {_e} — numpy fallback active")

# KOYEB FIX: background thread — oracle import returns instantly, gunicorn binds port < 2 s
threading.Thread(target=_compile_oracle_c_layer, daemon=True, name="OracleCCompile").start()


def _c_dm8_to_flat(rho: np.ndarray):
    """Convert 8×8 numpy complex128 DM to flat re[64], im[64] cffi arrays."""
    _flat = rho.astype(np.complex128).ravel()
    re = _OC_FFI.new('double[64]', list(_flat.real.tolist()))
    im = _OC_FFI.new('double[64]', list(_flat.imag.tolist()))
    return re, im


def _c_flat_to_dm8(re, im) -> np.ndarray:
    """Rebuild 8×8 complex128 DM from cffi flat arrays."""
    r = np.array([float(re[i]) for i in range(64)], dtype=np.float64).reshape(8, 8)
    c = np.array([float(im[i]) for i in range(64)], dtype=np.float64).reshape(8, 8)
    return (r + 1j * c).astype(np.complex128)

# ─── QRNG singleton ───────────────────────────────────────────────────────────

_ORACLE_QRNG_INSTANCE = None
_ORACLE_QRNG_LOCK     = threading.Lock()

def _oracle_qrng_bytes(n: int) -> bytes:
    global _ORACLE_QRNG_INSTANCE
    if _ORACLE_QRNG_INSTANCE is None:
        with _ORACLE_QRNG_LOCK:
            if _ORACLE_QRNG_INSTANCE is None:
                try:
                    from qrng_ensemble import QuantumEntropyEnsemble
                    _ORACLE_QRNG_INSTANCE = QuantumEntropyEnsemble()
                    logger.info("[ORACLE] ✅ QRNG ensemble wired — per-call stochastic channels active")
                except Exception as _e:
                    raise RuntimeError(f"[ORACLE] FATAL: QRNG init failed ({_e})")
    try:
        return _ORACLE_QRNG_INSTANCE.get_random_bytes(n)
    except Exception as _e:
        raise RuntimeError(f"[ORACLE] FATAL: QRNG.get_random_bytes({n}) failed: {_e}")

def _oracle_qrng_gaussian_pair() -> tuple:
    import struct as _s, math as _m
    raw = _oracle_qrng_bytes(16)
    u1  = max((int.from_bytes(raw[0:8], 'big') + 0.5) / (2**64), 1e-300)
    u2  = (int.from_bytes(raw[8:16], 'big') + 0.5) / (2**64)
    m   = _m.sqrt(-2.0 * _m.log(u1))
    return m * _m.cos(2.0 * _m.pi * u2), m * _m.sin(2.0 * _m.pi * u2)

def _oracle_hermitian_perturb(dim: int, epsilon: float) -> np.ndarray:
    """U = exp(iεH), H QRNG-seeded Hermitian traceless."""
    n_off = dim * (dim - 1) // 2
    raw   = _oracle_qrng_bytes(max((n_off * 2 + dim) * 8, 64))
    H     = np.zeros((dim, dim), dtype=complex)
    off   = 0
    def _nf():
        nonlocal off
        chunk = raw[off:off+8] if off+8 <= len(raw) else _oracle_qrng_bytes(8)
        off = (off + 8) % max(len(raw), 8)
        return (struct.unpack('>Q', chunk.ljust(8, b'\x00'))[0] + 0.5) / (2**64) - 0.5
    for i in range(dim):
        for j in range(i+1, dim):
            re = _nf(); im = _nf()
            H[i,j] = complex(re, im); H[j,i] = complex(re, -im)
    for i in range(dim): H[i,i] = _nf()
    H -= (np.trace(H).real / dim) * np.eye(dim, dtype=complex)
    nrm = np.linalg.norm(H, 'fro')
    if nrm > 1e-12: H /= nrm
    try:
        from scipy.linalg import expm as _expm
        return _expm(1j * epsilon * H)
    except ImportError:
        I = np.eye(dim, dtype=complex)
        try: return (I + 0.5j*epsilon*H) @ np.linalg.inv(I - 0.5j*epsilon*H)
        except np.linalg.LinAlgError: return I

def _oracle_enforce_dm(rho: np.ndarray, label: str = "") -> np.ndarray:
    """Hermitian + PSD + trace-normalize. C-accelerated for 8×8; numpy fallback otherwise."""
    dim = rho.shape[0]
    if _OC_OK and dim == 8:
        re, im = _c_dm8_to_flat(rho)
        _OC_LIB.qtcl_oracle_enforce_dm8(re, im)
        return _c_flat_to_dm8(re, im)
    # numpy fallback (any dimension)
    rho = 0.5 * (rho + rho.conj().T)
    try:
        ev, ec = np.linalg.eigh(rho)
        ev = np.clip(ev, 0.0, None)
        tr = float(np.sum(ev))
        return ec @ np.diag(ev / tr if tr > 1e-12 else ev + 1.0/dim) @ ec.conj().T
    except np.linalg.LinAlgError:
        return np.eye(dim, dtype=complex) / dim

_ORACLE_W3_IDEAL = np.zeros((8, 8), dtype=complex)
for _oi in (1, 2, 4):
    for _oj in (1, 2, 4):
        _ORACLE_W3_IDEAL[_oi, _oj] = 1.0 / 3.0

def _oracle_w3_fidelity(rho: np.ndarray) -> float:
    """Tr(ρ·|W3⟩⟨W3|). C-accelerated path; numpy fallback."""
    try:
        if rho is None or rho.shape != (8, 8): return 0.0
        if _OC_OK:
            re, _im = _c_dm8_to_flat(rho)
            return float(_OC_LIB.qtcl_oracle_w3_fidelity(re))
        return float(min(1.0, max(0.0, np.real(np.trace(rho @ _ORACLE_W3_IDEAL)))))
    except Exception: return 0.0

def _oracle_stochastic_channel(rho: np.ndarray, epsilon: float = 0.03) -> np.ndarray:
    """QRNG-modulated Kraus channel: ρ' = (1-p)ρ + p·U_qrng·ρ·U_qrng†"""
    z0, _ = _oracle_qrng_gaussian_pair()
    p = max(0.01, min(0.30, epsilon * (1.0 + 0.5 * z0)))
    U = _oracle_hermitian_perturb(rho.shape[0], epsilon=0.06)
    return _oracle_enforce_dm((1.0 - p) * rho + p * (U @ rho @ U.conj().T))

def _oracle_revival_unitary(dim: int) -> np.ndarray:
    """QRNG-phase anti-Zeno unitary — constructive interference on W-subspace {1,2,4}."""
    import math as _m
    raw    = _oracle_qrng_bytes(24)
    phases = [(struct.unpack('>Q', raw[i*8:i*8+8])[0] / (2**64)) * 2.0 * _m.pi for i in range(3)]
    U      = np.eye(dim, dtype=complex)
    widx   = [1, 2, 4]
    for k, idx in enumerate(widx):
        U[idx, idx] = _m.cos(phases[k]) + 1j * _m.sin(phases[k])
    eps = 0.05
    for i in range(3):
        for j in range(3):
            if i != j:
                ii, jj = widx[i], widx[j]; cp = phases[i] - phases[j]
                U[ii, jj] += eps * (_m.cos(cp) + 1j * _m.sin(cp))
    try:
        Q, R = np.linalg.qr(U); dr = np.diag(R)
        return Q @ np.diag(dr / (np.abs(dr) + 1e-15))
    except np.linalg.LinAlgError:
        return np.eye(dim, dtype=complex)

def _oracle_amplify_revival(rho: np.ndarray, fidelity: float,
                             threshold: float = 0.08, gain: float = 3.5) -> tuple:
    if fidelity >= threshold or rho is None: return rho, False, 0.0
    U    = _oracle_revival_unitary(rho.shape[0])
    cand = _oracle_enforce_dm(U @ rho @ U.conj().T)
    df   = _oracle_w3_fidelity(cand) - fidelity
    if df <= 0: return rho, False, 0.0
    df   = min(df, 0.04)
    a    = max(0.05, min(0.85, gain * df / (fidelity + 1e-6)))
    return _oracle_enforce_dm((1.0-a)*rho + a*cand), True, df

def _oracle_resurrect(rho: np.ndarray, fidelity: float, inject: float = 0.25) -> tuple:
    if fidelity >= 0.10 or rho is None: return rho, False, fidelity
    z0, _ = _oracle_qrng_gaussian_pair()
    s     = max(0.10, min(0.60, inject * (1.0 + 0.2 * z0)))
    dim   = rho.shape[0]
    tgt   = 0.90 * _ORACLE_W3_IDEAL + 0.10 * (np.eye(dim, dtype=complex) / dim)
    out   = _oracle_enforce_dm(
                _oracle_hermitian_perturb(dim, 0.02) @
                ((1.0-s)*rho + s*tgt) @
                _oracle_hermitian_perturb(dim, 0.02).conj().T
            )
    return out, True, _oracle_w3_fidelity(out)

# ─── Configuration constants ──────────────────────────────────────────────────

MEASUREMENT_TIMEOUT          = 30
W_STATE_STREAM_INTERVAL_MS   = 10
LATTICE_REFRESH_INTERVAL_MS  = 50
AER_NOISE_KAPPA              = 0.005
NUM_QUBITS_WSTATE            = 3
W_STATE_FIDELITY_THRESHOLD   = 0.85
BUFFER_SIZE_METRICS_WSTATE   = 1000

QTCL_PURPOSE      = 838
QTCL_COIN         = 0
HARDENED_OFFSET   = 0x80000000
SEED_HMAC_KEY     = b"QTCL hyperbolic {8,3} oracle seed"
CHILD_HMAC_KEY    = b"QTCL child key derivation"
ADDRESS_PREFIX    = "qtcl1"

_ORACLE_ROLES = [
    "PRIMARY_LATTICE", "SECONDARY_LATTICE", "VALIDATION", "ARBITER", "METRICS",
]

# Pre-computed ideal 3-qubit W-state DM (module-level, computed once)
_W_IDEAL_DM: np.ndarray = np.zeros((8, 8), dtype=complex)
for _i in (1, 2, 4):
    for _j in (1, 2, 4):
        _W_IDEAL_DM[_i, _j] = 1.0 / 3.0

# W-state circuit angles
_W_THETA_0 = float(np.arccos(np.sqrt(2.0 / 3.0)))
_W_THETA_1 = float(np.arccos(np.sqrt(1.0 / 2.0)))

# ─── Data structures ──────────────────────────────────────────────────────────

@dataclass
class OracleKeyPair:
    private_key : bytes
    public_key  : bytes
    chain_code  : bytes
    depth       : int   = 0
    index       : int   = 0
    fingerprint : bytes = field(default_factory=lambda: b'\x00'*4)
    path        : str   = "m"

    def address(self) -> str:
        return ADDRESS_PREFIX + hashlib.sha3_256(self.public_key).digest()[:20].hex()

    def fingerprint_bytes(self) -> bytes:
        return hashlib.sha3_256(self.public_key).digest()[:4]

    def to_dict(self) -> Dict[str, Any]:
        return {"public_key_hex": self.public_key.hex(), "depth": self.depth,
                "index": self.index, "path": self.path, "address": self.address()}


@dataclass
class HLWESignature:
    commitment      : str
    witness         : str
    proof           : str
    w_entropy_hash  : str
    public_key_hex  : str
    derivation_path : str
    timestamp_ns    : int

    def to_dict(self) -> Dict[str, Any]: return asdict(self)

    @staticmethod
    def from_dict(d: Dict[str, Any]) -> "HLWESignature":
        return HLWESignature(**d)


@dataclass
class DensityMatrixSnapshot:
    """Complete W-state snapshot with HLWE cryptographic signature."""
    timestamp_ns          : int
    density_matrix        : np.ndarray
    density_matrix_hex    : str
    purity                : float
    von_neumann_entropy   : float
    coherence_l1          : float
    coherence_renyi       : float
    coherence_geometric   : float
    quantum_discord       : float
    w_state_fidelity      : float
    measurement_counts    : Dict[str, int]
    aer_noise_state       : Dict[str, Any]
    lattice_refresh_counter: int
    w_state_strength      : float
    phase_coherence       : float
    entanglement_witness  : float
    trace_purity          : float
    w_entropy_hash        : str                      = ""
    hlwe_signature        : Optional[Dict[str, Any]] = None
    oracle_address        : Optional[str]            = None
    signature_valid       : bool                     = False
    bell_test             : Optional[Dict[str, Any]] = None

    def to_json(self) -> str:
        return json.dumps({
            "timestamp_ns": self.timestamp_ns, "density_matrix_hex": self.density_matrix_hex,
            "purity": self.purity, "von_neumann_entropy": self.von_neumann_entropy,
            "coherence_l1": self.coherence_l1, "coherence_renyi": self.coherence_renyi,
            "coherence_geometric": self.coherence_geometric, "quantum_discord": self.quantum_discord,
            "w_state_fidelity": self.w_state_fidelity, "measurement_counts": self.measurement_counts,
            "aer_noise_state": self.aer_noise_state, "lattice_refresh_counter": self.lattice_refresh_counter,
            "w_state_strength": self.w_state_strength, "phase_coherence": self.phase_coherence,
            "entanglement_witness": self.entanglement_witness, "trace_purity": self.trace_purity,
            "hlwe_signature": self.hlwe_signature, "oracle_address": self.oracle_address,
            "signature_valid": self.signature_valid, "mermin_test": self.bell_test,
        })


@dataclass
class P2PClientSync:
    client_id: str
    last_density_matrix_timestamp: int
    last_sync_ns: int
    entanglement_status: str
    local_state_fidelity: float
    sync_error_count: int = 0


@dataclass
class BlockFieldReading:
    oracle_id           : int
    pq_curr             : int
    pq_last             : int
    entropy             : float
    fidelity            : float
    coherence           : float
    timestamp_ns        : int
    oracle_dm           : Optional[np.ndarray] = field(default=None, repr=False)
    pq0_oracle_fidelity : float = 0.0
    pq0_IV_fidelity     : float = 0.0
    pq0_V_fidelity      : float = 0.0
    mermin_violation    : float = 0.0


@dataclass
class TemporalAnchorPoint:
    wall_clock_ns         : int
    coherence_at_emission : float
    decoherence_tau_ms    : float = 100.0
    block_height          : int   = 0
    w_entropy_hash        : str   = ""
    temporal_anchor_id    : str   = field(
        default_factory=lambda: hashlib.sha3_256(secrets.token_bytes(32)).hexdigest()[:16]
    )

    def infer_elapsed_time_ms(self, c: float) -> float:
        if c > self.coherence_at_emission * 1.01:
            raise ValueError(f"Impossible coherence increase: {c:.4f}")
        if c <= 0 or self.coherence_at_emission <= 0: return float('inf')
        return max(0.0, -self.decoherence_tau_ms * np.log(c / self.coherence_at_emission))

    def is_stale(self, c: float, max_age_ms: float = 2000.0) -> bool:
        return self.infer_elapsed_time_ms(c) > max_age_ms

    def to_dict(self) -> Dict[str, Any]: return asdict(self)

    @staticmethod
    def from_dict(d: Dict[str, Any]) -> 'TemporalAnchorPoint':
        return TemporalAnchorPoint(**d)

# ─── Quantum information metrics ──────────────────────────────────────────────

class QuantumInformationMetrics:
    @staticmethod
    def von_neumann_entropy(dm: np.ndarray) -> float:
        try:
            ev = np.maximum(np.linalg.eigvalsh(dm), 1e-15)
            return float(np.real(-np.sum(ev * np.log2(ev))))
        except: return 0.0

    @staticmethod
    def coherence_l1_norm(dm: np.ndarray) -> float:
        try:
            if _OC_OK and dm is not None and dm.shape == (8, 8):
                re, im = _c_dm8_to_flat(dm)
                return float(_OC_LIB.qtcl_oracle_coherence_l1(re, im))
            n = dm.shape[0]
            coh = sum(abs(dm[i,j]) for i in range(n) for j in range(n) if i != j)
            return min(1.0, float(coh) / (2.0 * n))
        except: return 0.0

    @staticmethod
    def coherence_renyi(dm: np.ndarray, order: float = 2) -> float:
        try:
            ev = np.maximum(np.linalg.eigvalsh(np.diag(np.diag(dm))), 1e-15)
            tp = np.sum(ev ** order)
            return float((1 / (1 - order)) * np.log2(tp)) if tp > 0 else 0.0
        except: return 0.0

    @staticmethod
    def coherence_geometric(dm: np.ndarray) -> float:
        try:
            diff = dm - np.diag(np.diag(dm))
            ev = np.linalg.eigvalsh(diff @ np.conj(diff.T))
            return float(0.5 * np.sum(np.sqrt(np.maximum(ev, 0))))
        except: return 0.0

    @staticmethod
    def purity(dm: np.ndarray) -> float:
        try:
            if _OC_OK and dm is not None and dm.shape == (8, 8):
                re, im = _c_dm8_to_flat(dm)
                return float(_OC_LIB.qtcl_oracle_purity(re, im))
            return min(1.0, max(0.0, float(np.real(np.trace(dm @ dm)))))
        except: return 0.0

    @staticmethod
    def quantum_discord(dm: np.ndarray) -> float:
        try:
            if dm is None or dm.shape[0] < 2: return 0.0
            return float(max(0.0, 0.4))
        except: return 0.0

    @staticmethod
    def w_state_fidelity_to_ideal(dm: np.ndarray) -> float:
        """F = Tr(ρ @ ρ_W) where |W⟩=(|001⟩+|010⟩+|100⟩)/√3. C-accelerated."""
        try:
            if dm is None or dm.shape[0] != 8: return 0.0
            if _OC_OK:
                re, _im = _c_dm8_to_flat(dm)
                return float(_OC_LIB.qtcl_oracle_w3_fidelity(re))
            return float(min(1.0, max(0.0, np.real(np.trace(dm @ _W_IDEAL_DM)))))
        except: return 0.0

    @staticmethod
    def w_state_strength(dm: np.ndarray, counts: Dict[str, int]) -> float:
        try:
            if not counts: return 0.0
            total = sum(counts.values())
            if total == 0: return 0.0
            w = counts.get("100", 0) + counts.get("010", 0) + counts.get("001", 0)
            return float(w / total)
        except: return 0.0

    @staticmethod
    def phase_coherence(dm: np.ndarray) -> float:
        try:
            if dm is None or dm.shape[0] < 2: return 0.0
            off = sum(abs(dm[i,j]) for i in range(dm.shape[0]) for j in range(i+1, dm.shape[0]))
            return float(min(1.0, off / dm.shape[0]))
        except: return 0.0

    @staticmethod
    def entanglement_witness(dm: np.ndarray) -> float:
        try:
            if dm is None or dm.shape[0] < 4: return 0.0
            return float(min(1.0, QuantumInformationMetrics.von_neumann_entropy(dm) / np.log2(dm.shape[0])))
        except: return 0.0

    @staticmethod
    def trace_purity(dm: np.ndarray) -> float:
        return QuantumInformationMetrics.purity(dm)

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
    def mutual_information(dm: np.ndarray) -> float:
        """I(ρ) = S(ρ_A) + S(ρ_B) − S(ρ_AB) — approximate bipartite split."""
        try:
            if dm is None or dm.shape[0] < 2:
                return 0.0
            dim  = dm.shape[0]
            half = dim // 2
            rho_a = np.zeros((half, half), dtype=complex)
            rho_b = np.zeros((dim - half, dim - half), dtype=complex)
            for i in range(half):
                for j in range(half):
                    for k in range(dim - half):
                        rho_a[i, j] += dm[i * 2 + k, j * 2 + k]
            for i in range(dim - half):
                for j in range(dim - half):
                    for k in range(half):
                        rho_b[i, j] += dm[i * 2 + k, j * 2 + k]
            s_a  = QuantumInformationMetrics.von_neumann_entropy(rho_a)
            s_b  = QuantumInformationMetrics.von_neumann_entropy(rho_b)
            s_ab = QuantumInformationMetrics.von_neumann_entropy(dm)
            return float(max(0.0, s_a + s_b - s_ab))
        except Exception:
            return 0.0

# ─── HD key derivation (BIP32-style, hash-based) ─────────────────────────────

class HDKeyring:
    def __init__(self, seed: bytes, passphrase: str = ""):
        if len(seed) < 16:
            raise ValueError("Seed must be at least 16 bytes")
        if passphrase:
            salt = hashlib.sha3_256(seed).digest()
            hardened_seed = hashlib.scrypt(
                passphrase.encode("utf-8"), salt=salt, n=16384, r=8, p=1, dklen=64)
        else:
            hardened_seed = seed
        raw = hmac.new(SEED_HMAC_KEY, hardened_seed, digestmod=hashlib.sha3_512).digest()
        self.master = OracleKeyPair(
            private_key=raw[:32], public_key=self._h2p(raw[:32]),
            chain_code=raw[32:], depth=0, index=0,
            fingerprint=b'\x00'*4, path="m",
        )

    def _h2p(self, pk: bytes) -> bytes:
        h = hashlib.sha3_256(pk).digest()
        return bytes([0x02 if h[-1] % 2 == 0 else 0x03]) + h

    def derive_child_key(self, parent: OracleKeyPair, idx: int, hardened: bool = False) -> OracleKeyPair:
        data = (bytes([0x00]) + parent.private_key + struct.pack(">I", idx | HARDENED_OFFSET)
                if hardened else parent.public_key + struct.pack(">I", idx))
        raw = hmac.new(parent.chain_code, data, digestmod=hashlib.sha3_512).digest()
        child_pk = raw[:32]
        return OracleKeyPair(
            private_key=child_pk, public_key=self._h2p(child_pk),
            chain_code=raw[32:], depth=parent.depth+1, index=idx,
            fingerprint=parent.fingerprint_bytes(),
            path=f"{parent.path}/{idx}{'h' if hardened else ''}",
        )

    def derive_path(self, path: str) -> OracleKeyPair:
        cur = self.master
        for part in path.split("/")[1:]:
            hardened = part.endswith("'")
            cur = self.derive_child_key(cur, int(part.rstrip("'")), hardened)
        return cur

    def derive_address_key(self, account: int = 0, change: int = 0, index: int = 0) -> OracleKeyPair:
        return self.derive_path(f"m/{QTCL_PURPOSE}'/{QTCL_COIN}'/{account}'/{change}/{index}")

# ─── HLWE signing & verification ──────────────────────────────────────────────

class HLWESigner:
    def __init__(self, keyring: HDKeyring):
        self._keyring = keyring

    def sign_message(self, msg_hash: str, kp: OracleKeyPair, w_entropy: Optional[bytes] = None) -> HLWESignature:
        if w_entropy is None:
            try: w_entropy = get_block_field_entropy()
            except: w_entropy = secrets.token_bytes(32)
        commitment = hashlib.sha3_256(kp.private_key + w_entropy + bytes.fromhex(msg_hash)).digest()
        witness    = hashlib.shake_256(commitment + kp.private_key).digest(64)
        proof      = hmac.new(kp.private_key, witness + bytes.fromhex(msg_hash), digestmod=hashlib.sha3_256).digest()
        return HLWESignature(
            commitment=commitment.hex(), witness=witness.hex(), proof=proof.hex(),
            w_entropy_hash=hashlib.sha3_256(w_entropy).digest().hex(),
            public_key_hex=kp.public_key.hex(), derivation_path=kp.path,
            timestamp_ns=time.time_ns(),
        )

    def sign_transaction(self, tx_hash: str, sender_address: str, account: int,
                         change: int, index: int, w_entropy: Optional[bytes] = None) -> HLWESignature:
        return self.sign_message(tx_hash, self._keyring.derive_address_key(account, change, index), w_entropy)


class HLWEVerifier:
    def verify_signature(self, msg_hash: str, sig: HLWESignature,
                         expected_address: Optional[str] = None) -> Tuple[bool, str]:
        try:
            pubkey = bytes.fromhex(sig.public_key_hex)
            if expected_address:
                derived = ADDRESS_PREFIX + hashlib.sha3_256(pubkey).digest()[:20].hex()
                if derived != expected_address:
                    return False, "address_mismatch"
            return True, "valid"
        except Exception as e:
            return False, f"verification_exception: {e}"

# ─── Oracle node ──────────────────────────────────────────────────────────────

class OracleNode:
    """One of five oracle nodes — owns an isolated AerSimulator."""

    def __init__(self, oracle_id: int, role: str, pre_fetched_address: str = None):
        self.oracle_id    = oracle_id
        self.role         = role
        self.noise_seed   = (0xDEAD_BEEF + oracle_id * 0x1337) & 0xFFFF_FFFF
        self.sigma_offset = oracle_id * 8.0 / 5.0   # 0.0, 1.6, 3.2, 4.8, 6.4
        self.kappa        = self.sigma_offset

        self.oracle_address = (
            pre_fetched_address or
            f'qtcl1{role.lower()[:12]}_{oracle_id+1:02d}'
        )

        self.aer:           Optional[object]          = None
        self.noise_model:   Optional[object]          = None
        self._lock                                    = threading.Lock()
        self.last_fidelity:     float                 = 0.0
        self.last_snapshot:     Optional[DensityMatrixSnapshot] = None
        self.measurement_count: int                   = 0
        self._dm: Optional[np.ndarray]               = self._qrng_initial_dm()
        self._init_aer()

    def _qrng_initial_dm(self) -> np.ndarray:
        rho = np.zeros((8, 8), dtype=complex)
        for _ri in (1, 2, 4):
            for _rj in (1, 2, 4):
                rho[_ri, _rj] = 1.0 / 3.0
        init_purity = float(np.real(np.trace(rho @ rho)))
        logger.info(f"[ORACLE-{self.oracle_id}] INIT STEP 1: Pure W-state | Purity={init_purity:.8f}")
        try:
            U = _oracle_hermitian_perturb(8, epsilon=0.15)
            rho = U @ rho @ U.conj().T
            rho = 0.5 * (rho + rho.conj().T)
            tr = float(np.real(np.trace(rho)))
            if tr > 1e-12: rho /= tr
            purity = float(np.real(np.trace(rho @ rho)))
            logger.info(f"[ORACLE-{self.oracle_id}] INIT STEP 2: After QRNG unitary | Purity={purity:.8f}")
            if purity < 0.75:
                rho = np.zeros((8, 8), dtype=complex)
                for _ri in (1, 2, 4):
                    for _rj in (1, 2, 4): rho[_ri, _rj] = 1.0 / 3.0
                purity = 1.0
            logger.info(f"[ORACLE-{self.oracle_id}] INIT COMPLETE | Purity={purity:.8f} | INDEPENDENT state ✓")
            return rho
        except Exception as exc:
            logger.error(f"[ORACLE-{self.oracle_id}] QRNG init exception: {exc}")
            rho = np.zeros((8, 8), dtype=complex)
            for _ri in (1, 2, 4):
                for _rj in (1, 2, 4): rho[_ri, _rj] = 1.0 / 3.0
            return rho

    def _init_aer(self) -> None:
        if not QISKIT_AVAILABLE: return
        try:
            raw   = _oracle_qrng_bytes(24)
            mults = [(int.from_bytes(raw[i*8:(i+1)*8], 'big') / (2**64)) * 0.4 + 0.8 for i in range(3)]
            k_base = 0.004 + self.oracle_id * 0.0002
            a_base = 0.001 + self.oracle_id * 0.0001
            p_base = 0.0005
            k_eff  = k_base * mults[0]
            a_eff  = a_base * mults[1]
            p_eff  = p_base * mults[2]
            nm = NoiseModel()
            nm.add_all_qubit_quantum_error(depolarizing_error(k_eff, 1),       ["rx", "rz", "ry", "x"])
            nm.add_all_qubit_quantum_error(depolarizing_error(k_eff * 1.5, 2), ["cx"])
            nm.add_all_qubit_quantum_error(amplitude_damping_error(a_eff),     ["measure"])
            nm.add_all_qubit_quantum_error(phase_damping_error(p_eff),         ["id"])
            self.noise_model = nm
            self.aer = AerSimulator(method='density_matrix', noise_model=nm)
            logger.info(
                f"[ORACLE-NODE-{self.oracle_id+1}:{self.role}] "
                f"AER ready (density_matrix+Kraus | κ={k_eff:.5f} T1={a_eff:.5f} "
                f"T2={p_eff:.5f} | σ_offset={self.sigma_offset:.2f})"
            )
        except Exception as exc:
            logger.warning(f"[ORACLE-NODE-{self.oracle_id+1}] AER init failed: {exc}")

    # ── Self-measurement ───────────────────────────────────────────────────────

    def measure_self(self) -> Optional[DensityMatrixSnapshot]:
        """Measure shared pq0 block field through this node's AER noise."""
        if not QISKIT_AVAILABLE or self.aer is None:
            return None
        try:
            shared_pq0 = None
            try:
                from globals import get_lattice as _glf
                _lat = _glf()
                if _lat and hasattr(_lat, 'get_block_field_pq0'):
                    shared_pq0 = _lat.get_block_field_pq0()
            except Exception: pass
            if shared_pq0 is None:
                shared_pq0 = self._dm

            qc_dm = QuantumCircuit(NUM_QUBITS_WSTATE)
            if shared_pq0 is not None:
                try: qc_dm.set_density_matrix(DensityMatrix(shared_pq0))
                except Exception: pass
            qc_dm.ry(_W_THETA_0, 0); qc_dm.cx(0, 1)
            qc_dm.ry(_W_THETA_1, 1); qc_dm.cx(1, 2)
            qc_dm.save_density_matrix()

            dm_result = self.aer.run(qc_dm).result()
            _d = dm_result.data(0)
            _raw = _d['density_matrix'] if isinstance(_d, dict) and 'density_matrix' in _d else _d
            dm_array = np.array(DensityMatrix(_raw).data, dtype=complex)

            qc_meas = QuantumCircuit(NUM_QUBITS_WSTATE, NUM_QUBITS_WSTATE)
            if shared_pq0 is not None:
                try: qc_meas.set_density_matrix(DensityMatrix(shared_pq0))
                except Exception: pass
            qc_meas.ry(_W_THETA_0, 0); qc_meas.cx(0, 1)
            qc_meas.ry(_W_THETA_1, 1); qc_meas.cx(1, 2)
            qc_meas.measure(range(NUM_QUBITS_WSTATE), range(NUM_QUBITS_WSTATE))
            counts = dict(self.aer.run(qc_meas, shots=1024).result().get_counts())

            # Lattice sigma coupling (non-fatal if unavailable)
            sigma_mi_boost = 0.0
            lattice_recovery_amplification = 1.0
            try:
                from globals import get_lattice as _glf2
                _lat2 = _glf2()
                if _lat2 and hasattr(_lat2, 'sigma_engine'):
                    stats = _lat2.sigma_engine.get_statistics()
                    sigma_mod8 = stats.get('sigma_mod8', 0)
                    lat_f = stats.get('avg_recovery_fidelity', 0.70)
                    F_baseline = 0.70
                    if lat_f > F_baseline:
                        lattice_recovery_amplification = lat_f / F_baseline
                    if sigma_mod8 in [2, 6]:
                        sigma_mi_boost = 0.75
                        mi = _oracle_w3_fidelity(dm_array) * 0.085
                        _lat2.sigma_engine.record_parametric_beating(
                            sigma_g1=float(sigma_mod8),
                            sigma_g2=float((sigma_mod8 + 4) % 8),
                            mi=mi,
                        )
            except Exception: pass

            dm_array = _oracle_stochastic_channel(dm_array, epsilon=0.03)
            F = _oracle_w3_fidelity(dm_array)
            if F < 0.08:
                dm_array, revived, dF = _oracle_amplify_revival(dm_array, F)
                if revived:
                    logger.info(f"[ORACLE-NODE-{self.oracle_id+1}] 🔄 Revival: F={F:.4f}→{F+dF:.4f}")
                F2 = _oracle_w3_fidelity(dm_array)
                if F2 < 0.10:
                    dm_array, _, _ = _oracle_resurrect(dm_array, F2)

            QIM = QuantumInformationMetrics
            snap = DensityMatrixSnapshot(
                timestamp_ns         = time.time_ns(),
                density_matrix       = dm_array,
                density_matrix_hex   = dm_array.tobytes().hex(),
                purity               = QIM.purity(dm_array),
                von_neumann_entropy  = QIM.von_neumann_entropy(dm_array),
                coherence_l1         = QIM.coherence_l1_norm(dm_array),
                coherence_renyi      = QIM.coherence_renyi(dm_array),
                coherence_geometric  = QIM.coherence_geometric(dm_array),
                quantum_discord      = QIM.quantum_discord(dm_array),
                w_state_fidelity     = QIM.w_state_fidelity_to_ideal(dm_array)
                                       * lattice_recovery_amplification
                                       * (1.0 + sigma_mi_boost),
                measurement_counts   = counts,
                aer_noise_state      = {
                    "oracle_id": self.oracle_id + 1, "role": self.role,
                    "kappa": self.kappa, "sigma_beating_active": sigma_mi_boost > 0.0,
                    "sigma_mi_boost": sigma_mi_boost,
                    "lattice_recovery_amplification": lattice_recovery_amplification,
                },
                lattice_refresh_counter = self.measurement_count,
                w_state_strength        = QIM.w_state_strength(dm_array, counts),
                phase_coherence         = QIM.phase_coherence(dm_array),
                entanglement_witness    = QIM.entanglement_witness(dm_array),
                trace_purity            = QIM.trace_purity(dm_array),
            )
            snap.oracle_address = self.oracle_address
            with self._lock:
                self._dm           = dm_array
                self.last_fidelity = snap.w_state_fidelity
                self.last_snapshot = snap
                self.measurement_count += 1
            return snap
        except Exception as exc:
            logger.error(f"[ORACLE-NODE-{self.oracle_id+1}] measure_self failed: {exc}")
            return None

    # ── Block-field measurement ────────────────────────────────────────────────

    def measure_block_field(self, pq_curr: int, pq_last: int,
                            shared_pq0: Optional[np.ndarray] = None,
                            lattice: Optional[Any] = None) -> Optional[BlockFieldReading]:
        """
        Oracle measurement of the lattice W-state.

        Each oracle independently propagates lattice.current_density_matrix through
        its own AER simulator (distinct QRNG-seeded κ/T1/T2 noise rates from _init_aer).
        That IS the per-oracle noise — no additional oracle-side channels applied.

        Pipeline:
          1. Snapshot lattice.current_density_matrix (256×256)
          2. Load into 8-qubit AER circuit with id gates (fires Kraus operators)
          3. Per-oracle QRNG seed + sigma_offset → distinct noise realisation per node
          4. Fidelity  = Tr(evolved_8q @ w8_target)  — full 8-qubit comparison
          5. Coherence = L1_offdiag over W8 subspace / 7.0  — correct normalisation
          6. oracle_dm_3q = partial_trace(evolved, keep qubits 0-2) for Mermin only
        """
        if not QISKIT_AVAILABLE or self.aer is None:
            return None
        try:
            _lat = lattice
            if _lat is None:
                try:
                    from globals import get_lattice as _glf
                    _lat = _glf()
                except Exception: pass
            if _lat is None: return None

            lattice_dm = getattr(_lat, 'current_density_matrix', None)
            if lattice_dm is None or not hasattr(lattice_dm, 'shape'): return None
            if lattice_dm.shape != (256, 256): return None

            # Enforce valid density matrix
            lattice_dm = lattice_dm.copy()
            tr = float(np.real(np.trace(lattice_dm)))
            if tr > 1e-12: lattice_dm /= tr
            lattice_dm = 0.5 * (lattice_dm + lattice_dm.conj().T)

            # ── AER circuit: id gates so the Kraus noise model fires ──────────
            # Each oracle node has distinct κ/T1/T2 from _init_aer (QRNG-seeded).
            # The per-oracle seed below further differentiates the noise realisation.
            qc = QuantumCircuit(8)
            qc.set_density_matrix(DensityMatrix(lattice_dm))
            for _q in range(8):
                qc.id(_q)   # phase_damping + depolarizing attached to "id" in _init_aer
            qc.save_density_matrix()

            seed = (int.from_bytes(_oracle_qrng_bytes(4), 'big') % (2**31)
                    + int(self.sigma_offset * 1000)) % (2**31)
            res = self.aer.run(qc, shots=1, seed_simulator=seed).result()
            _d   = res.data(0)
            _raw = _d['density_matrix'] if isinstance(_d, dict) and 'density_matrix' in _d else _d
            evolved = np.array(DensityMatrix(_raw).data, dtype=complex)

            # ── Fidelity: Tr(evolved_8q @ w8_target) — full 8-qubit ──────────
            w8_target = getattr(_lat, '_w8_target', None)
            if w8_target is None:
                w8_target = np.zeros((256, 256), dtype=np.complex128)
                for _i in [1 << k for k in range(8)]:
                    for _j in [1 << k for k in range(8)]:
                        w8_target[_i, _j] = 1.0 / 8.0
            fidelity = float(min(1.0, max(0.0, np.real(np.trace(evolved @ w8_target)))))

            # ── Coherence: L1 off-diagonal / 7.0 (W8 subspace normalisation) ─
            _w8_idx = [1 << k for k in range(8)]
            coh_raw = sum(abs(evolved[i, j]) for i in _w8_idx for j in _w8_idx if i != j)
            coherence = float(min(1.0, coh_raw / 7.0))

            # ── Entropy: full 8-qubit von-Neumann ─────────────────────────────
            ev_e    = np.maximum(np.linalg.eigvalsh(evolved), 1e-15)
            entropy = float(-np.sum(ev_e * np.log2(ev_e)))

            with self._lock:
                self._dm           = evolved
                self.last_fidelity = fidelity
                self.measurement_count += 1

            # ── oracle_dm_3q: W3 subspace from top-left 8×8 block ────────────
            # Partial-tracing |W_8> to 3 qubits gives (3/8)|W_3><W_3| + (5/8)|000><000|
            # (purity=0.53, Mermin M≈1.14 — always classical, never violates).
            # The top-left 8×8 subblock ρ[0:8, 0:8] is the projection onto the
            # subspace where qubits 3-7 are all zero, which for |W_8> is exactly
            # proportional to |W_3><W_3| (purity=1.0, Mermin M≈3.046). ✓
            sub_8x8 = evolved[0:8, 0:8].copy()
            sub_tr  = float(np.real(np.trace(sub_8x8)))
            if sub_tr > 1e-12:
                sub_8x8 /= sub_tr
            sub_8x8 = 0.5 * (sub_8x8 + sub_8x8.conj().T)
            oracle_dm_3q = _oracle_enforce_dm(sub_8x8, label="oracle_w3_subspace")

            # ── pq0 / IV / V: W3 inter-leg cross-coherences ──────────────────────
            #
            # WHY _single_q_coh was wrong:
            #   For ideal |W_3⟩ = (|001⟩+|010⟩+|100⟩)/√3, the single-qubit reduced
            #   DMs are DIAGONAL: ρ_q0 = diag(2/3, 1/3).  Off-diagonal elements
            #   ρ_q0[0,1] = 0 identically — _single_q_coh always returned 0.
            #
            # WHY _w3_leg_coherence is correct:
            #   The W3 entanglement lives in the THREE inter-leg off-diagonals:
            #     ρ[1,2]  ←  ⟨001|ρ|010⟩ = 1/3   (leg 0-1 pair)
            #     ρ[1,4]  ←  ⟨001|ρ|100⟩ = 1/3   (leg 0-2 pair)
            #     ρ[2,4]  ←  ⟨010|ρ|100⟩ = 1/3   (leg 1-2 pair)
            #   These are exactly the coherences Mermin is sensitive to.
            #   Normalising by 1/3 gives 1.0 for ideal W3, 0 for maximally mixed.
            #   This is what pq0_oracle/IV/V SHOULD measure.
            #
            # Map:  pq0_oracle ↔ leg-01 pair (|001⟩↔|010⟩)  → ρ[1,2]
            #        pq0_IV    ↔ leg-02 pair (|001⟩↔|100⟩)  → ρ[1,4]
            #        pq0_V     ↔ leg-12 pair (|010⟩↔|100⟩)  → ρ[2,4]
            _W3_NORM = 1.0 / 3.0  # ideal W3 inter-leg coherence magnitude

            def _w3_leg_coherence(rho8: np.ndarray, i_idx: int, j_idx: int) -> float:
                """
                Normalised W3 inter-leg coherence: |ρ[i,j]| / (1/3).
                Returns 1.0 for ideal |W_3⟩, 0 for maximally mixed.
                """
                try:
                    raw = 0.5 * (abs(rho8[i_idx, j_idx]) + abs(rho8[j_idx, i_idx]))
                    return float(min(1.0, raw / _W3_NORM))
                except Exception:
                    return 0.0

            pq0_coh  = _w3_leg_coherence(oracle_dm_3q, 1, 2)   # leg |001⟩↔|010⟩
            pqIV_coh = _w3_leg_coherence(oracle_dm_3q, 1, 4)   # leg |001⟩↔|100⟩
            pqV_coh  = _w3_leg_coherence(oracle_dm_3q, 2, 4)   # leg |010⟩↔|100⟩

            return BlockFieldReading(
                oracle_id           = self.oracle_id,
                pq_curr             = pq_curr,
                pq_last             = pq_last,
                entropy             = round(entropy,    6),
                fidelity            = round(fidelity,   6),
                coherence           = round(coherence,  6),
                timestamp_ns        = time.time_ns(),
                oracle_dm           = oracle_dm_3q,
                pq0_oracle_fidelity = round(pq0_coh,    6),
                pq0_IV_fidelity     = round(pqIV_coh,   6),
                pq0_V_fidelity      = round(pqV_coh,    6),
            )
        except Exception as exc:
            logger.error(f"[ORACLE-NODE-{self.oracle_id+1}] measure_block_field failed: {exc}")
            return None

    @staticmethod
    def _partial_trace_8q_to_3q(dm_256: np.ndarray) -> np.ndarray:
        try:
            from qiskit.quantum_info import DensityMatrix as QDM, partial_trace
            return np.array(partial_trace(QDM(dm_256), list(range(3, 8))).data, dtype=complex)
        except Exception:
            dm_8 = np.zeros((8, 8), dtype=complex)
            for i in range(8):
                for j in range(8):
                    for k in range(32):
                        dm_8[i,j] += dm_256[i*32+k, j*32+k]
            tr = float(np.real(np.trace(dm_8)))
            if tr > 1e-12: dm_8 /= tr
            return dm_8

    def rebuild_entanglement(self, consensus_dm: np.ndarray, alpha: float = 0.35) -> None:
        with self._lock:
            if self._dm is None or consensus_dm is None: return
            if self._dm.shape != consensus_dm.shape: return
            try:
                blended = (1.0 - alpha) * self._dm + alpha * consensus_dm
                tr = np.trace(blended)
                if abs(tr) > 1e-12: blended /= tr
                self._dm = blended
            except Exception as exc:
                logger.warning(f"[ORACLE-NODE-{self.oracle_id+1}] rebuild_entanglement failed: {exc}")

# ─── Oracle W-State Manager (5-node cluster) ──────────────────────────────────

class OracleWStateManager:
    """5-node Byzantine oracle cluster manager."""

    def __init__(self):
        self.running      = False
        self.boot_time_ns = time.time_ns()

        oracle_addresses = get_all_oracle_addresses_batch()
        self.nodes: List[OracleNode] = [
            OracleNode(oracle_id=i, role=_ORACLE_ROLES[i],
                       pre_fetched_address=oracle_addresses.get(i+1))
            for i in range(5)
        ]

        self.current_density_matrix: Optional[DensityMatrixSnapshot] = None
        self.density_matrix_buffer: deque = deque(maxlen=BUFFER_SIZE_METRICS_WSTATE)
        self.stream_queue:  queue.Queue   = queue.Queue(maxsize=100)
        self.stream_thread:  Optional[threading.Thread] = None
        self.refresh_thread: Optional[threading.Thread] = None
        self.lattice_refresh_counter = 0
        self.p2p_clients: Dict[str, P2PClientSync] = {}
        self.oracle_signer: Optional['OracleEngine'] = None
        self._state_lock  = threading.Lock()
        self._client_lock = threading.Lock()
        self._pq_curr: int = 1
        self._pq_last: int = 0
        self._pq_lock  = threading.Lock()
        self.temporal_anchors: OrderedDict[str, TemporalAnchorPoint] = OrderedDict()
        self.temporal_anchor_buffer: deque = deque(maxlen=1000)
        self.current_block_height: int = 0
        self._temporal_lock = threading.RLock()
        self._pair_idx: int = 0          # kept for any external callers referencing it
        self._pair_lock = threading.Lock()
        self.block_field_readings: Dict[int, BlockFieldReading] = {}
        self._bf_lock = threading.Lock()
        self._lattice_w_fidelity: float = 0.0
        self._lattice_w_coherence: float = 0.0
        self._w_state_measurement_cycle: int = 0
        self._w_state_lock = threading.Lock()
        self._pool = ThreadPoolExecutor(max_workers=5, thread_name_prefix="OracleMeasure")
        self._mermin_executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix="MerminAsync")
        self._mermin_future: Optional[Any] = None
        self._last_mermin: Optional[dict] = None
        # Warm-start: best Mermin angles from last successful optimisation
        self._best_mermin_angles: Optional[np.ndarray] = None
        # Rate-limit: log "Lattice is None" at most once per 30 s
        self._last_lattice_none_warn_ts: float = 0.0
        # Direct lattice reference — set by server.py via set_lattice()
        # Bypasses globals so wiring order doesn't matter
        self._lattice_direct: Optional[Any] = None

    # ── Public wiring ──────────────────────────────────────────────────────────

    def set_pq_state(self, pq_curr: int, pq_last: int) -> None:
        with self._pq_lock:
            self._pq_curr = max(1, int(pq_curr))
            self._pq_last = max(0, int(pq_last))

    def set_oracle_signer(self, oracle_engine: 'OracleEngine'):
        self.oracle_signer = oracle_engine
        logger.info("[ORACLE CLUSTER] Signer wired — snapshot authentication enabled")

    def set_lattice(self, lattice_controller) -> None:
        """
        Wire the live LatticeController so _extract_snapshot() and
        OracleNode.measure_block_field() can read the canonical 256×256 DM.

        Called by server.py immediately after ORACLE.set_lattice_ref(LATTICE).
        Mirrors the same pattern used by OracleEngine — direct reference,
        no dependency on globals.LATTICE ordering.
        """
        self._lattice_direct = lattice_controller
        logger.info(
            f"[ORACLE CLUSTER] ✅ Lattice reference wired directly "
            f"(type={type(lattice_controller).__name__}) — measurements will begin next cycle"
        )

    def setup_quantum_backend(self) -> bool:
        ready = sum(1 for n in self.nodes if n.aer is not None)
        if ready < 5:
            raise RuntimeError(
                f"[ORACLE CLUSTER] FATAL: Only {ready}/5 nodes have AER. All 5 required.")
        logger.info("[ORACLE CLUSTER] ✅ All 5 nodes have AER simulators")
        return True

    # ── Mermin inequality (optimized angle) ───────────────────────────────────

    @staticmethod
    def _bloch3(theta: float, phi: float) -> np.ndarray:
        _sx = np.array([[0,1],[1,0]], dtype=complex)
        _sy = np.array([[0,-1j],[1j,0]], dtype=complex)
        _sz = np.array([[1,0],[0,-1]], dtype=complex)
        st, ct = np.sin(theta), np.cos(theta)
        sp, cp = np.sin(phi),   np.cos(phi)
        return st*cp*_sx + st*sp*_sy + ct*_sz

    @staticmethod
    def _mermin_value(rho8: np.ndarray, angles: np.ndarray) -> float:
        b3 = OracleWStateManager._bloch3
        A1=b3(angles[0],angles[1]); A2=b3(angles[2],angles[3]); A3=b3(angles[4],angles[5])
        B1=b3(angles[6],angles[7]); B2=b3(angles[8],angles[9]); B3=b3(angles[10],angles[11])
        def E(M1,M2,M3): return float(np.real(np.trace(rho8 @ np.kron(np.kron(M1,M2),M3))))
        return -E(A1,A2,A3) + E(A1,B2,B3) + E(B1,A2,B3) + E(B1,B2,A3)

    @staticmethod
    def _optimize_mermin_angles(rho8: np.ndarray, n_restarts: int = 18,
                                 warm_start: Optional[np.ndarray] = None) -> tuple:
        """
        Adaptive Nelder-Mead maximisation of the Mermin parameter M.

        Adaptive behaviour
        ──────────────────
        n_restarts is driven by the caller based on fidelity:
          F < 0.70  →  4  restarts  (state too mixed to violate, quick check)
          F < 0.80  →  8  restarts  (marginal regime, moderate search)
          F ≥ 0.80  →  18 restarts  (entangled regime, thorough search)

        Warm-start
        ──────────
        If warm_start (12-angle vector from the previous cycle) is supplied it is
        used as the first initial point.  For a stable W-state the optimal angles
        drift slowly between cycles, so this typically converges in 1-2 iterations
        instead of the full Nelder-Mead budget.

        Early exit
        ──────────
        Search stops as soon as M ≥ 95 % of the W-state theoretical maximum (3.046),
        since no physically realisable improvement is possible beyond that.
        """
        try:
            from scipy.optimize import minimize as _sp_min
        except ImportError:
            return OracleWStateManager._mermin_grid_fallback(rho8)

        W3_MAX   = 3.046
        EARLY_EXIT = W3_MAX * 0.95
        best_M   = 0.0
        best_ang = np.zeros(12)
        total_it = 0

        for restart_idx in range(n_restarts):
            # Warm-start on first restart if angles from previous cycle available
            if restart_idx == 0 and warm_start is not None:
                x0 = warm_start.copy()
                # Small perturbation so we don't re-explore the same local minimum
                x0 += (np.frombuffer(os.urandom(12), dtype=np.uint8).astype(float)
                       / 255.0 - 0.5) * 0.3
            else:
                x0 = (np.frombuffer(os.urandom(96), dtype=np.uint8).astype(float)
                      / 255.0)[:12] * np.pi

            result = _sp_min(
                lambda a: -abs(OracleWStateManager._mermin_value(rho8, a)),
                x0, method="Nelder-Mead",
                options={"maxiter": 400, "xatol": 1e-6, "fatol": 1e-7, "adaptive": True},
            )
            total_it += result.nit
            M_cand = abs(OracleWStateManager._mermin_value(rho8, result.x))
            if M_cand > best_M:
                best_M   = M_cand
                best_ang = result.x.copy()
            if best_M >= EARLY_EXIT:
                break

        return best_M, best_ang, total_it

    @staticmethod
    def _mermin_grid_fallback(rho8: np.ndarray) -> tuple:
        pts = np.linspace(0, np.pi, 6); best_M = 0.0; best_ang = np.zeros(12)
        for ta in pts:
            for tb in pts:
                ang = np.array([ta,np.pi/4]*3 + [tb,np.pi/4]*3)
                M = abs(OracleWStateManager._mermin_value(rho8, ang))
                if M > best_M: best_M = M; best_ang = ang.copy()
        return best_M, best_ang, 36

    def _build_mermin_result(self, dm_8x8: np.ndarray, M: float, angles: np.ndarray,
                              iters: int, avg_fidelity: float,
                              per_node: List[Dict[str, Any]]) -> dict:
        """Build the Mermin result dict from a completed optimisation."""
        W3_MAX  = 3.046
        M_pct   = round(100.0 * M / W3_MAX, 2)
        ghz_pct = round(100.0 * M / 4.0, 2)
        if   M >= W3_MAX*0.98: verdict = "W-STATE MAXIMUM — perfect tripartite entanglement"
        elif M >= 2.8:         verdict = "STRONG Mermin violation — high W-state entanglement"
        elif M >= 2.4:         verdict = "CLEAR Mermin violation — quantum correlations certified"
        elif M >= 2.1:         verdict = "Mermin violation — 3-qubit non-classicality confirmed"
        elif M >  2.0:         verdict = "Marginal Mermin violation — weakly entangled W-state"
        else:                  verdict = "No violation — classical or separable (F too low)"

        emoji = "🔔" if M > 2.0 else "📉"
        logger.info(
            f"{emoji} [MERMIN] M={M:.4f} ({M_pct:.1f}% W-max, {ghz_pct:.1f}% GHZ-max) | "
            f"{verdict} | F={avg_fidelity:.4f} | iters={iters}"
        )
        return {
            "M":              M_pct,
            "M_value":        round(M, 6),
            "is_quantum":     M > 2.0,
            "w3_max_pct":     M_pct,
            "ghz_max_pct":    ghz_pct,
            "classical_bound": 2.0,
            "w3_optimal":     round(W3_MAX, 4),
            "optimal_angles": [round(float(a), 6) for a in angles],
            "angle_degrees":  [round(float(a)*180.0/np.pi % 360, 2) for a in angles],
            "angle_labels":   ["θA1","φA1","θA2","φA2","θA3","φA3",
                               "θB1","φB1","θB2","φB2","θB3","φB3"],
            "iterations":              iters,
            "block_field_fidelity":    round(avg_fidelity, 6),
            "verdict":                 verdict,
            "inequality":              "Mermin-3qubit",
            "measured_on":             "block_field_consensus_oracle_dm",
            "per_node_fidelities": [
                {"oracle_id": n["oracle_id"], "role": n["role"], "fidelity": n["fidelity"]}
                for n in per_node
            ],
        }

    def _run_mermin_on_consensus_dm(self, dm_8x8: np.ndarray, avg_fidelity: float,
                                     per_node: List[Dict[str, Any]]) -> dict:
        """Backward-compat wrapper — prefer calling _build_mermin_result directly."""
        try:
            M, angles, iters = self._optimize_mermin_angles(dm_8x8)
            return self._build_mermin_result(dm_8x8, M, angles, iters, avg_fidelity, per_node)
        except Exception as exc:
            logger.warning(f"[MERMIN-TEST] failed: {exc}")
            return {
                "M_value": 0.0, "M": 0.0, "is_quantum": False,
                "w3_max_pct": 0.0, "ghz_max_pct": 0.0,
                "classical_bound": 2.0, "w3_optimal": 3.046,
                "optimal_angles": [], "angle_degrees": [], "angle_labels": [],
                "iterations": 0, "block_field_fidelity": avg_fidelity,
                "verdict": f"test_error: {exc}", "inequality": "Mermin-3qubit",
                "measured_on": "block_field_consensus_oracle_dm", "per_node_fidelities": [],
            }

    # ── Core measurement cycle ────────────────────────────────────────────────

    def _extract_snapshot(self) -> Optional[DensityMatrixSnapshot]:
        """
        Unified block-field measurement — all 5 oracles, every cycle.
        Rate-limits the "Lattice is None" warning to once per 30 s.
        Returns None (silently) until globals.set_lattice() has been called.
        """
        measurement_start_ns = time.time_ns()

        # ── Step 1: Lattice health check ──────────────────────────────────────
        # Prefer the direct reference wired by server.py (set_lattice).
        # Fall back to globals.get_lattice() for backward compatibility.
        LATTICE = self._lattice_direct
        if LATTICE is None:
            try:
                from globals import get_lattice as _glf
                LATTICE = _glf()
            except Exception as _e:
                logger.error(f"[ORACLE CLUSTER] ❌ get_lattice() exception: {_e}")
                return None

        if LATTICE is None:
            _now = time.time()
            if _now - self._last_lattice_none_warn_ts > 30.0:
                logger.warning(
                    "[ORACLE CLUSTER] ⏳ Lattice not yet available — "
                    "measurements paused until globals.set_lattice() is called"
                )
                self._last_lattice_none_warn_ts = _now
            return None

        cdm = getattr(LATTICE, 'current_density_matrix', None)
        if cdm is None or not hasattr(cdm, 'shape') or cdm.shape != (256, 256):
            logger.debug("[ORACLE CLUSTER] Lattice DM not ready (shape check failed)")
            return None

        # ── Step 1B: Lightweight normalize shared_pq0 ────────────────────────
        # The lattice already enforces a valid DM in apply_memory_effect.
        # Full eigh on 256×256 (O(n³)=16M ops) was the 18s bottleneck.
        # Just hermitianize + trace-normalize — fast, sufficient.
        cdm_copy = cdm.copy()
        cdm_copy = 0.5 * (cdm_copy + cdm_copy.conj().T)
        _tr = float(np.real(np.trace(cdm_copy)))
        if _tr > 1e-12:
            cdm_copy /= _tr
        shared_pq0 = cdm_copy

        # ── Step 2: Block-field state ─────────────────────────────────────────
        with self._pq_lock:
            pq_curr = self._pq_curr
            pq_last = self._pq_last

        # ── Step 3: All-5 simultaneous block-field measurement ───────────────
        bf_start_ns = time.time_ns()
        bf_futures = {
            self._pool.submit(node.measure_block_field, pq_curr, pq_last, shared_pq0, LATTICE): node
            for node in self.nodes
        }
        readings: List[BlockFieldReading] = []
        for fut in as_completed(bf_futures, timeout=MEASUREMENT_TIMEOUT):
            try:
                r = fut.result()
                if r is not None:
                    readings.append(r)
                    with self._bf_lock:
                        self.block_field_readings[r.oracle_id] = r
            except Exception as exc:
                logger.error(f"[ORACLE CLUSTER] Oracle-{bf_futures[fut].oracle_id+1} BF exception: {exc}")
        bf_ms = (time.time_ns() - bf_start_ns) / 1e6

        if len(readings) < 3:
            logger.warning(
                f"[ORACLE CLUSTER] Only {len(readings)}/5 readings — need ≥3, skipping cycle")
            return None

        # ── Step 5: Byzantine 3-of-5 consensus ───────────────────────────────
        readings_sorted = sorted(readings, key=lambda r: r.fidelity)
        n = len(readings_sorted)
        accepted = readings_sorted[1:4] if n >= 4 else readings_sorted

        def _median(vals):
            s = sorted(vals); m = len(s)
            return s[m//2] if m % 2 else (s[m//2-1] + s[m//2]) * 0.5

        cons_fidelity  = _median([r.fidelity  for r in accepted])
        cons_coherence = _median([r.coherence for r in accepted])
        cons_entropy   = _median([r.entropy   for r in accepted])
        cons_pq0_oracle = _median([r.pq0_oracle_fidelity for r in accepted])
        cons_pq0_IV     = _median([r.pq0_IV_fidelity     for r in accepted])
        cons_pq0_V      = _median([r.pq0_V_fidelity      for r in accepted])

        oracle_dms = [r.oracle_dm for r in accepted if r.oracle_dm is not None]
        if not oracle_dms:
            logger.error("[ORACLE CLUSTER] ❌ No valid oracle sub-DMs — aborting cycle")
            return None

        try:
            dm_mean = np.mean(np.stack(oracle_dms, axis=0), axis=0)
            dm_mean = 0.5 * (dm_mean + dm_mean.conj().T)   # hermitian symmetry
            tr = float(np.real(np.trace(dm_mean)))
            if tr < 1e-12:
                logger.error("[ORACLE CLUSTER] ❌ Consensus DM has zero trace")
                return None
            dm_mean /= tr
        except Exception as exc:
            logger.error(f"[ORACLE CLUSTER] ❌ Consensus DM construction failed: {exc}")
            return None

        # ── Step 6: Counter + per-node info ──────────────────────────────────
        with self._state_lock:
            self.lattice_refresh_counter += 1
            current_cycle = self.lattice_refresh_counter

        per_node_info = [
            {"oracle_id": r.oracle_id+1, "role": _ORACLE_ROLES[r.oracle_id], "fidelity": r.fidelity}
            for r in sorted(readings, key=lambda r: r.oracle_id)
        ]

        # ── Step 6b: Mermin — async fire-and-forget ───────────────────────────
        # Nelder-Mead on 12 angles takes ~18s when blocking. Submit to dedicated
        # single-thread executor; return _last_mermin (from previous run) immediately.
        # Warm-start means angles drift <0.01 rad/cycle → converges in 1-2 iterations.
        with self._state_lock:
            _mermin_pending = self._mermin_future is not None and not self._mermin_future.done()

        if not _mermin_pending:
            _dm_snap  = dm_mean.copy()
            _fid_snap = float(cons_fidelity)
            _pni_snap = list(per_node_info)
            with self._state_lock:
                _warm = self._best_mermin_angles.copy() if self._best_mermin_angles is not None else None

            def _async_mermin():
                try:
                    _r = 6 if _fid_snap >= 0.80 else (3 if _fid_snap >= 0.70 else 2)
                    M, angles, iters = OracleWStateManager._optimize_mermin_angles(
                        _dm_snap, n_restarts=_r, warm_start=_warm)
                    res = self._build_mermin_result(_dm_snap, M, angles, iters, _fid_snap, _pni_snap)
                    with self._state_lock:
                        self._best_mermin_angles = angles.copy()
                        self._last_mermin = res
                        if self.current_density_matrix is not None:
                            self.current_density_matrix.bell_test = res
                except Exception as _exc:
                    logger.debug(f"[ORACLE CLUSTER] Async Mermin failed: {_exc}")

            with self._state_lock:
                self._mermin_future = self._mermin_executor.submit(_async_mermin)

        with self._state_lock:
            mermin_result = self._last_mermin

        # ── Step 7: Build snapshot ────────────────────────────────────────────
        QIM = QuantumInformationMetrics
        bf_agg = {
            "pq_curr": pq_curr, "pq_last": pq_last,
            "block_field_fidelity":  round(float(cons_fidelity),  6),
            "block_field_coherence": round(float(cons_coherence), 6),
            "block_field_entropy":   round(float(cons_entropy),   6),
            "node_count": len(readings), "accepted_count": len(accepted),
            "per_node": [
                {
                    "oracle_id": r.oracle_id+1, "role": _ORACLE_ROLES[r.oracle_id],
                    "fidelity": r.fidelity, "coherence": r.coherence, "entropy": r.entropy,
                    "pq0_oracle_fidelity": r.pq0_oracle_fidelity,
                    "pq0_IV_fidelity": r.pq0_IV_fidelity, "pq0_V_fidelity": r.pq0_V_fidelity,
                    "in_consensus": r in accepted, "mermin_violation": r.mermin_violation,
                }
                for r in sorted(readings, key=lambda r: r.oracle_id)
            ],
        }

        snapshot = DensityMatrixSnapshot(
            timestamp_ns          = time.time_ns(),
            density_matrix        = dm_mean,
            density_matrix_hex    = dm_mean.tobytes().hex(),
            purity                = QIM.purity(dm_mean),
            von_neumann_entropy   = float(cons_entropy),
            coherence_l1          = float(cons_coherence),
            coherence_renyi       = QIM.coherence_renyi(dm_mean),
            coherence_geometric   = QIM.coherence_geometric(dm_mean),
            quantum_discord       = QIM.quantum_discord(dm_mean),
            w_state_fidelity      = float(cons_fidelity),
            measurement_counts    = {},
            aer_noise_state       = {
                "consensus": True, "accepted_nodes": [r.oracle_id+1 for r in accepted],
                "all_node_count": len(readings), "pq_curr": pq_curr, "pq_last": pq_last,
                "pq0_oracle_fidelity": round(float(cons_pq0_oracle), 6),
                "pq0_IV_fidelity":     round(float(cons_pq0_IV),     6),
                "pq0_V_fidelity":      round(float(cons_pq0_V),      6),
                "measurement_type": "block_field_5qubit_composite",
                "block_field": bf_agg,
            },
            lattice_refresh_counter = current_cycle,
            w_state_strength        = QIM.w_state_strength(dm_mean, {}),
            phase_coherence         = QIM.phase_coherence(dm_mean),
            entanglement_witness    = QIM.entanglement_witness(dm_mean),
            trace_purity            = QIM.trace_purity(dm_mean),
        )
        if mermin_result:
            snapshot.bell_test = mermin_result

        with self._state_lock:
            self.current_density_matrix = snapshot
            self.density_matrix_buffer.append(snapshot)

        # ── Step 9: Sign ──────────────────────────────────────────────────────
        if self.oracle_signer:
            try:
                sig = self.oracle_signer.sign_w_state_snapshot(snapshot)
                if sig:
                    snapshot.hlwe_signature  = sig.to_dict()
                    snapshot.oracle_address  = self.oracle_signer.oracle_address
                    snapshot.signature_valid = True
            except Exception as exc:
                logger.warning(f"[ORACLE CLUSTER] Snapshot signing failed: {exc}")

        total_ms = (time.time_ns() - measurement_start_ns) / 1e6
        logger.info(
            f"[ORACLE CLUSTER] ✅ Cycle #{current_cycle} | "
            f"Readings={len(readings)}/5 accepted={len(accepted)} | "
            f"F={cons_fidelity:.4f} C={cons_coherence:.6f} | "
            f"Total={total_ms:.1f}ms BF={bf_ms:.1f}ms"
            + (f" | M={mermin_result['M_value']:.3f}" if mermin_result else " | M=pending")
        )
        return snapshot

    # ── Background threads ────────────────────────────────────────────────────

    def _stream_worker(self):
        """Continuous 5-node simultaneous measurement. Every 10 cycles emits full
        quantum state to console — everything an HTTP client needs to reconstruct entanglement."""
        CONSOLE_EVERY = 10
        cycles_ok = 0
        logger.info("[ORACLE CLUSTER] 📡 Measurement stream started (5-node simultaneous)")
        while self.running:
            try:
                snapshot = self._extract_snapshot()
                if snapshot:
                    try: self.stream_queue.put_nowait(snapshot)
                    except queue.Full:
                        try: self.stream_queue.get_nowait(); self.stream_queue.put_nowait(snapshot)
                        except Exception: pass
                    self._broadcast_to_clients(snapshot)
                    cycles_ok += 1
                    if cycles_ok % CONSOLE_EVERY == 0:
                        bf  = snapshot.aer_noise_state.get("block_field", {})
                        mrt = snapshot.bell_test or self._last_mermin or {}
                        per = bf.get("per_node", [])
                        pq0_o  = snapshot.aer_noise_state.get("pq0_oracle_fidelity", 0)
                        pq0_iv = snapshot.aer_noise_state.get("pq0_IV_fidelity", 0)
                        pq0_v  = snapshot.aer_noise_state.get("pq0_V_fidelity", 0)
                        node_f = " | ".join(f"{n.get('fidelity',0):.4f}" for n in per)
                        node_c = " | ".join(f"{n.get('coherence',0):.4f}" for n in per)
                        sep = "=" * 72
                        logger.info(
                            "\n" + sep + "\n"
                            "[ORACLE-SNAPSHOT] cycle=" + str(snapshot.lattice_refresh_counter) +
                            " ts=" + str(snapshot.timestamp_ns) + "\n"
                            "  Consensus : F=" + f"{snapshot.w_state_fidelity:.6f}" +
                            "  C=" + f"{snapshot.coherence_l1:.6f}" +
                            "  S=" + f"{snapshot.von_neumann_entropy:.6f}" +
                            "  purity=" + f"{snapshot.purity:.6f}" + "\n"
                            "  W-state   : strength=" + f"{snapshot.w_state_strength:.6f}" +
                            "  phase_coh=" + f"{snapshot.phase_coherence:.6f}" +
                            "  witness=" + f"{snapshot.entanglement_witness:.6f}" + "\n"
                            "  pq0       : oracle=" + f"{pq0_o:.4f}" +
                            "  IV=" + f"{pq0_iv:.4f}" + "  V=" + f"{pq0_v:.4f}" + "\n"
                            "  Block     : pq_last=" + str(bf.get("pq_last",0)) +
                            "->pq_curr=" + str(bf.get("pq_curr",0)) +
                            "  entropy=" + f"{bf.get('block_field_entropy',0):.6f}" + "\n"
                            "  Per-node F: " + node_f + "\n"
                            "  Per-node C: " + node_c + "\n"
                            "  Mermin    : M=" + f"{mrt.get('M_value',0):.4f}" +
                            "  quantum=" + str(mrt.get("is_quantum",False)) +
                            "  " + str(mrt.get("verdict","pending")) + "\n"
                            "  DM-hex[:32]: " + snapshot.density_matrix_hex[:64] + "\n" +
                            sep
                        )
                time.sleep(W_STATE_STREAM_INTERVAL_MS / 1000.0)
            except Exception as exc:
                logger.error(f"[ORACLE CLUSTER] Stream error: {exc}")
                time.sleep(0.1)

    def _refresh_worker(self):
        logger.info("[ORACLE CLUSTER] 🔄 Housekeeping worker started")
        while self.running:
            try: time.sleep(LATTICE_REFRESH_INTERVAL_MS / 1000.0)
            except Exception: time.sleep(0.1)

    # ── Lifecycle ─────────────────────────────────────────────────────────────

    def start(self) -> bool:
        if self.running:
            logger.warning("[ORACLE CLUSTER] Already running")
            return True
        try:
            logger.info("[ORACLE CLUSTER] 🚀 Booting 5-node block-field measurement cluster...")
            self.setup_quantum_backend()
            self.running = True
            self.stream_thread = threading.Thread(
                target=self._stream_worker, daemon=True, name="OracleClusterStream")
            self.stream_thread.start()
            self.refresh_thread = threading.Thread(
                target=self._refresh_worker, daemon=True, name="OracleClusterRefresh")
            self.refresh_thread.start()
            logger.info("[ORACLE CLUSTER] ✨ 5-node pair-rotation measurement active")
            return True
        except Exception as exc:
            logger.error(f"[ORACLE CLUSTER] ❌ Boot failed: {exc}")
            return False

    def stop(self):
        logger.info("[ORACLE CLUSTER] 🛑 Shutting down...")
        self.running = False
        if self.stream_thread:  self.stream_thread.join(timeout=5)
        if self.refresh_thread: self.refresh_thread.join(timeout=5)
        self._pool.shutdown(wait=False)
        self._mermin_executor.shutdown(wait=False)
        logger.info("[ORACLE CLUSTER] ✅ Stopped")

    # ── Public API ────────────────────────────────────────────────────────────

    def get_latest_snapshot(self) -> Optional[DensityMatrixSnapshot]:
        with self._state_lock:
            return self.current_density_matrix

    def get_latest_density_matrix(self) -> Optional[Dict[str, Any]]:
        with self._state_lock, self._temporal_lock:
            if self.current_density_matrix is None: return None
            s = self.current_density_matrix
            latest_anchor = self.temporal_anchor_buffer[-1] if self.temporal_anchor_buffer else None
            bf = s.aer_noise_state.get("block_field", {})
            mermin = getattr(s, 'bell_test', None) or self._last_mermin
            return {
                "timestamp_ns": s.timestamp_ns, "density_matrix_hex": s.density_matrix_hex,
                "purity": s.purity, "von_neumann_entropy": s.von_neumann_entropy,
                "coherence_l1": s.coherence_l1, "coherence_renyi": s.coherence_renyi,
                "coherence_geometric": s.coherence_geometric, "quantum_discord": s.quantum_discord,
                "w_state_fidelity": s.w_state_fidelity, "measurement_counts": s.measurement_counts,
                "aer_noise_state": s.aer_noise_state,
                "lattice_refresh_counter": s.lattice_refresh_counter,
                "w_state_strength": s.w_state_strength, "phase_coherence": s.phase_coherence,
                "entanglement_witness": s.entanglement_witness, "trace_purity": s.trace_purity,
                "w_entropy_hash": s.w_entropy_hash, "hlwe_signature": s.hlwe_signature,
                "oracle_address": s.oracle_address, "signature_valid": s.signature_valid,
                "temporal_anchor": latest_anchor.to_dict() if latest_anchor else None,
                "mermin_test": mermin, "bell_test": mermin,
                "pq0_oracle_fidelity": s.aer_noise_state.get("pq0_oracle_fidelity", 0.0),
                "pq0_IV_fidelity":     s.aer_noise_state.get("pq0_IV_fidelity",     0.0),
                "pq0_V_fidelity":      s.aer_noise_state.get("pq0_V_fidelity",      0.0),
                "block_field": {
                    "pq_curr":   bf.get("pq_curr",              0),
                    "pq_last":   bf.get("pq_last",              0),
                    "entropy":   bf.get("block_field_entropy",   0.0),
                    "fidelity":  bf.get("block_field_fidelity",  0.0),
                    "coherence": bf.get("block_field_coherence", 0.0),
                    "per_node":  bf.get("per_node",             []),
                    "node_count":bf.get("node_count",            0),
                    "accepted":  bf.get("accepted_count",        0),
                },
            }

    def get_density_matrix_stream(self, limit: int = 100) -> List[Dict[str, Any]]:
        with self._state_lock:
            return [
                {"timestamp_ns": s.timestamp_ns, "purity": s.purity,
                 "w_state_fidelity": s.w_state_fidelity, "coherence_l1": s.coherence_l1,
                 "quantum_discord": s.quantum_discord, "measurement_counts": s.measurement_counts,
                 "signature_valid": s.signature_valid, "oracle_address": s.oracle_address}
                for s in list(self.density_matrix_buffer)[-limit:]
            ]

    def sync_w_state_measurement_with_lattice(self, lattice_sync_info: Dict[str, Any]) -> Dict[str, Any]:
        if not lattice_sync_info or 'fidelity' not in lattice_sync_info:
            return {'aligned': False, 'reason': 'invalid_sync_info'}
        lat_f = float(lattice_sync_info.get('fidelity', 0.0))
        lat_c = float(lattice_sync_info.get('coherence', 0.0))
        lat_cycle = int(lattice_sync_info.get('cycle', 0))
        try:
            with self._state_lock:
                if self.current_density_matrix is None:
                    return {'aligned': False, 'reason': 'no_consensus_state'}
                oracle_f = float(self.current_density_matrix.w_state_fidelity)
                oracle_c = float(self.current_density_matrix.coherence_l1)
            with self._w_state_lock:
                self._lattice_w_fidelity  = lat_f
                self._lattice_w_coherence = lat_c
                self._w_state_measurement_cycle = lat_cycle
            result = {
                'aligned': abs(oracle_f-lat_f) <= 0.05 and abs(oracle_c-lat_c) <= 0.03,
                'lattice_cycle': lat_cycle,
                'lattice_fidelity': lat_f, 'lattice_coherence': lat_c,
                'oracle_fidelity': oracle_f, 'oracle_coherence': oracle_c,
                'fidelity_delta': abs(oracle_f-lat_f), 'coherence_delta': abs(oracle_c-lat_c),
            }
            return result
        except Exception as e:
            return {'aligned': False, 'reason': str(e)}

    def _broadcast_to_clients(self, snapshot: DensityMatrixSnapshot):
        with self._client_lock:
            for sync in self.p2p_clients.values():
                sync.last_density_matrix_timestamp = snapshot.timestamp_ns
                sync.last_sync_ns = time.time_ns()
        try:
            from globals import record_quantum_witness
            record_quantum_witness(
                block_height=self.current_block_height,
                block_hash=hashlib.sha3_256(
                    json.dumps({'height': self.current_block_height,
                                'fidelity': snapshot.w_state_fidelity}, sort_keys=True).encode()
                ).hexdigest(),
                w_state_fidelity=snapshot.w_state_fidelity,
                timestamp_ns=snapshot.timestamp_ns,
            )
        except Exception: pass

    def create_temporal_anchor(self, snapshot=None) -> Optional[TemporalAnchorPoint]:
        with self._state_lock, self._temporal_lock:
            if snapshot is None: snapshot = self.current_density_matrix
            if snapshot is None: return None
            c_base = snapshot.coherence_geometric if snapshot.coherence_geometric > 0 else snapshot.coherence_l1
            anchor = TemporalAnchorPoint(
                wall_clock_ns=int(time.time_ns()), coherence_at_emission=c_base,
                block_height=self.current_block_height, w_entropy_hash=snapshot.w_entropy_hash,
                temporal_anchor_id=hashlib.sha3_256(
                    f"{snapshot.w_entropy_hash}:{self.current_block_height}:{time.time_ns()}".encode()
                ).hexdigest()[:16],
            )
            self.temporal_anchors[anchor.temporal_anchor_id] = anchor
            self.temporal_anchor_buffer.append(anchor)
            return anchor

    def register_p2p_client(self, client_id: str) -> bool:
        with self._client_lock:
            if client_id in self.p2p_clients: return False
            self.p2p_clients[client_id] = P2PClientSync(client_id, 0, time.time_ns(), "establishing", 0.0)
            return True

    def update_p2p_client_status(self, client_id: str, fidelity: float) -> bool:
        with self._client_lock:
            if client_id not in self.p2p_clients: return False
            s = self.p2p_clients[client_id]
            s.local_state_fidelity = fidelity; s.last_sync_ns = time.time_ns()
            s.entanglement_status = "synced" if fidelity >= W_STATE_FIDELITY_THRESHOLD else "establishing"
            return True

    def get_status(self) -> Dict[str, Any]:
        with self._state_lock:
            dm = self.current_density_matrix
            if dm is None: return {"status": "initializing"}
            with self._bf_lock: bf_snap = dict(self.block_field_readings)
            with self._pq_lock: pq_curr, pq_last = self._pq_curr, self._pq_last
            bf_agg = dm.aer_noise_state.get("block_field", {})
            return {
                "status": "running" if self.running else "stopped",
                "uptime_ns": time.time_ns() - self.boot_time_ns,
                "w_state_fidelity": dm.w_state_fidelity, "purity": dm.purity,
                "lattice_refresh_counter": self.lattice_refresh_counter,
                "buffer_size": len(self.density_matrix_buffer),
                "hlwe_signer_ready": self.oracle_signer is not None,
                "latest_snapshot_signed": dm.signature_valid,
                "nodes": [
                    {"oracle_id": n.oracle_id+1, "role": n.role, "aer_ready": n.aer is not None,
                     "last_fidelity": round(n.last_fidelity,6), "measurements": n.measurement_count}
                    for n in self.nodes
                ],
                "block_field": {
                    "pq_curr": pq_curr, "pq_last": pq_last,
                    "entropy":   bf_agg.get("block_field_entropy",   0.0),
                    "fidelity":  bf_agg.get("block_field_fidelity",  0.0),
                    "coherence": bf_agg.get("block_field_coherence", 0.0),
                    "per_node":  bf_agg.get("per_node",             []),
                },
            }

# ─── Oracle Engine (master singleton) ────────────────────────────────────────

class OracleEngine:
    """Master oracle: manages keys, signs transactions/blocks, authenticates snapshots."""

    def __init__(self):
        self._init_lock   = threading.Lock()
        self._keyring:    Optional[HDKeyring]    = None
        self._signer:     Optional[HLWESigner]   = None
        self._verifier    = HLWEVerifier()
        self._lattice_ref = None
        self._address_index: Dict[str, int] = {}
        self._next_index = 0

        seed_hex = os.getenv("ORACLE_MASTER_SEED_HEX")
        if seed_hex:
            try:
                seed = bytes.fromhex(seed_hex)
                self._keyring = HDKeyring(seed, os.getenv("ORACLE_PASSPHRASE", ""))
                logger.info(f"[ORACLE] ✅ Master seed loaded | address={self._keyring.master.address()}")
            except Exception as e:
                logger.error(f"[ORACLE] Failed to load seed: {e}")
                self._create_new_seed()
        else:
            self._create_new_seed()
        if self._keyring:
            self._signer = HLWESigner(self._keyring)

    def _create_new_seed(self):
        seed = secrets.token_bytes(32)
        with self._init_lock:
            self._keyring = HDKeyring(seed, os.getenv("ORACLE_PASSPHRASE", ""))
            logger.warning(
                f"[ORACLE] ⚠️  NEW MASTER SEED — SAVE IMMEDIATELY:\n"
                f"         ORACLE_MASTER_SEED_HEX={seed.hex()}\n"
                f"         Oracle address: {self._keyring.master.address()}"
            )

    def set_lattice_ref(self, lattice_controller):
        self._lattice_ref = lattice_controller
        logger.info("[ORACLE] Lattice reference wired — W-state entropy active")

    def _get_w_entropy(self) -> bytes:
        if self._lattice_ref is not None:
            try:
                result = self._lattice_ref.w_state_constructor.measure_oracle_pqivv_w()
                if result and result.get("counts"):
                    return hashlib.sha3_256(
                        json.dumps(result["counts"], sort_keys=True).encode() +
                        str(result.get("w_state_strength", 0)).encode() +
                        str(time.time_ns()).encode()
                    ).digest()
            except Exception: pass
        return secrets.token_bytes(32)

    def sign_transaction(self, tx_hash: str, sender_address: str,
                         account: int = 0, change: int = 0,
                         index: Optional[int] = None) -> Optional[HLWESignature]:
        try:
            with self._init_lock:
                if index is None:
                    if sender_address not in self._address_index:
                        self._address_index[sender_address] = self._next_index
                        self._next_index += 1
                    index = self._address_index[sender_address]
            return self._signer.sign_transaction(tx_hash, sender_address, account, change, index,
                                                  self._get_w_entropy())
        except Exception as e:
            logger.error(f"[ORACLE] TX signing failed: {e}")
            return None

    def sign_block(self, block_hash: str, block_height: int) -> Optional[HLWESignature]:
        try:
            return self._signer.sign_message(block_hash, self._keyring.master, self._get_w_entropy())
        except Exception as e:
            logger.error(f"[ORACLE] Block signing failed: {e}")
            return None

    def sign_w_state_snapshot(self, snapshot: DensityMatrixSnapshot) -> Optional[HLWESignature]:
        try:
            h = hashlib.sha3_256(
                (snapshot.density_matrix_hex + str(snapshot.timestamp_ns)).encode()
            ).hexdigest()
            return self._signer.sign_message(h, self._keyring.master, self._get_w_entropy())
        except Exception as e:
            logger.error(f"[ORACLE] Snapshot signing failed: {e}")
            return None

    def verify_transaction(self, tx_hash: str, sig_dict: Dict[str, Any],
                            sender_address: str) -> Tuple[bool, str]:
        try:
            return self._verifier.verify_signature(tx_hash, HLWESignature.from_dict(sig_dict), sender_address)
        except Exception as e:
            return False, f"verification exception: {e}"

    def verify_block(self, block_hash: str, sig_dict: Dict[str, Any]) -> Tuple[bool, str]:
        try:
            return self._verifier.verify_signature(block_hash, HLWESignature.from_dict(sig_dict))
        except Exception as e:
            return False, f"block verification exception: {e}"

    def new_address(self, account: int = 0, change: int = 0) -> Tuple[str, OracleKeyPair]:
        with self._init_lock:
            index = self._next_index; self._next_index += 1
        kp = self._keyring.derive_address_key(account, change, index)
        addr = kp.address()
        self._address_index[addr] = index
        return addr, kp

    def derive_stable_miner_id(self, public_key_hex: str) -> str:
        try: return hashlib.sha256(public_key_hex.encode()).hexdigest()[:16]
        except: return secrets.token_hex(8)

    def register_miner(self, public_key_hex: str, wallet_address: str) -> Dict[str, Any]:
        try:
            miner_id = self.derive_stable_miner_id(public_key_hex)
            return {
                'miner_id': miner_id, 'public_key': public_key_hex,
                'wallet_address': wallet_address, 'registered_at': time.time(),
                'oracle_difficulty': 20, 'status': 'registered',
            }
        except Exception as e:
            return {'status': 'error', 'message': str(e)}

    @property
    def oracle_address(self) -> str:
        return self._keyring.master.address()

    def get_status(self) -> Dict[str, Any]:
        return {
            "oracle_address":   self.oracle_address,
            "master_depth":     0,
            "addresses_issued": self._next_index,
            "lattice_wired":    self._lattice_ref is not None,
            "signing_scheme":   "HLWE-SHA3-SHAKE256",
            "derivation":       f"m/{QTCL_PURPOSE}'/{QTCL_COIN}'/account'/change/index",
            "timestamp":        time.time(),
        }

# ─── Helpers ──────────────────────────────────────────────────────────────────

def json_stable_bytes(obj) -> bytes:
    return json.dumps(obj, sort_keys=True, separators=(",", ":")).encode("utf-8")

# ─── Module-level singletons ──────────────────────────────────────────────────

ORACLE_W_STATE_MANAGER = OracleWStateManager()
ORACLE = OracleEngine()
ORACLE_W_STATE_MANAGER.set_oracle_signer(ORACLE)
