# ═════════════════════════════════════════════════════════════════════════════════╗
# ║                                                                                ║
# ║   QTCL DATABASE BUILDER V7 - NEONDB + SQLITE DUAL-MODE                       ║
# ║   NeonDB (server) via DATABASE_URL env | SQLite (client) fallback             ║
# ║   Client-side Argon2id key-wrapping (NO external KMS required)               ║
# ║                                                                                ║
# ║   SERVER MODE  → export DATABASE_URL=postgresql://neondb_owner:<pw>@<host>/  ║
# ║   CLIENT MODE  → no DATABASE_URL set → qtcl.db local SQLite                   ║
# ║                                                                                ║
# ║   KEY SECURITY MODEL (replaces Google/AWS KMS):                               ║
# ║     passphrase + device_pepper → Argon2id → KEK (never stored)               ║
# ║     KEK + AES-256-GCM encrypts 32-byte DEK                                    ║
# ║     DEK + AES-256-GCM encrypts BIP39 seed entropy                             ║
# ║     DB contains: salts + ciphertexts + nonces only — zero key material       ║
# ║                                                                                ║
# ╚════════════════════════════════════════════════════════════════════════════════╝

# ─────────────────────────────────────────────────────────────────────────────
# GUARD: This module is NOT meant to be imported. Only run as __main__.
# ─────────────────────────────────────────────────────────────────────────────
import sys as _sys
if __name__ not in ('__main__', '__mp_main__'):
    raise ImportError(
        "qtcl_db_builder.py is NOT a module for import. "
        "Run as standalone script: python qtcl_db_builder.py"
    )

# ─────────────────────────────────────────────────────────────────────────────
# STEP 1: DEPENDENCIES (only if running as main)
# ─────────────────────────────────────────────────────────────────────────────
import subprocess
import os
import sys  # needed early for logging

_db_url = os.environ.get("DATABASE_URL", "")
_pg_mode = _db_url.startswith("postgresql")

_pkgs = ["mpmath", "tqdm"]
if _pg_mode:
    _pkgs.append("psycopg2-binary")
_pkgs.append("argon2-cffi")
_pkgs.append("cryptography")

try:
    subprocess.check_call([_sys.executable, "-m", "pip", "install", "--quiet", "--break-system-packages"] + _pkgs)
except subprocess.CalledProcessError:
    print("Installing packages without --break-system-packages...")
    subprocess.check_call([_sys.executable, "-m", "pip", "install", "--quiet", "--user"] + _pkgs)

# ─────────────────────────────────────────────────────────────────────────────
# STEP 2: IMPORTS & SETUP
# ─────────────────────────────────────────────────────────────────────────────
import time
import json
import math
import hashlib
import logging
import gc
import sqlite3
from datetime import datetime, timezone
from typing import Dict, List, Tuple, Optional, Any, Iterator
from dataclasses import dataclass, field
from contextlib import contextmanager
from urllib.parse import quote_plus

# Progress bar — use standard mode (notebook mode fails without ipywidgets)
try:
    # Try standard tqdm first (works everywhere, no widget dependencies)
    from tqdm import tqdm
except ImportError:
    # Fallback: simple dummy progress bar if tqdm unavailable
    class tqdm:
        def __init__(self, *args, total=None, desc=None, leave=True, **kwargs):
            self.total = total
            self.desc = desc
            self.n = 0
        def update(self, n=1):
            self.n += n
        def close(self):
            pass
        def __enter__(self):
            return self
        def __exit__(self, *args):
            self.close()
        def __iter__(self):
            return iter([])

# Mathematical precision
try:
    from mpmath import (
        mp, mpf, mpc, sqrt, pi, cos, sin, exp, log, tanh, sinh, cosh, acosh,
        atanh, atan2, fabs, re as mre, im as mim, conj, norm, phase,
        matrix, nstr, power, floor, ceil, asin, acos, hypot, fsum
    )
    mp.dps = 150  # 150-bit precision
    MPMATH_AVAILABLE = True
except ImportError:
    MPMATH_AVAILABLE = False
    mp = None
    mpf = float
    print("⚠️ mpmath not available - precision reduced", file=sys.stderr)

# Database
try:
    import psycopg2
    from psycopg2.extras import execute_values, execute_batch, RealDictCursor, Json
    from psycopg2 import sql, errors as psycopg2_errors
    PSYCOPG2_AVAILABLE = True
except ImportError:
    PSYCOPG2_AVAILABLE = False
    print("⚠️ psycopg2 not available", file=sys.stderr)

# Argon2id — client-side KEK derivation (no external KMS)
try:
    from argon2.low_level import hash_secret_raw, Type
    ARGON2_AVAILABLE = True
except ImportError:
    ARGON2_AVAILABLE = False
import hashlib as _hashlib  # always available (used in device_pepper + scrypt fallback)

# AES-256-GCM — stdlib from Python 3.6+
from cryptography.hazmat.primitives.ciphers.aead import AESGCM

# ─────────────────────────────────────────────────────────────────────────────
# STEP 3: LOGGING (Colab-friendly)
# ─────────────────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] %(levelname)s: %(message)s',
    datefmt='%H:%M:%S',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)

# Colors for Colab output
class CLR:
    BOLD = '\033[1m'
    G = '\033[92m'
    R = '\033[91m'
    Y = '\033[93m'
    C = '\033[96m'
    M = '\033[95m'
    E = '\033[0m'
    HEADER = f'{BOLD}{M}'
    OK = f'{BOLD}{G}'
    ERROR = f'{BOLD}{R}'
    WARN = f'{BOLD}{Y}'
    QUANTUM = f'{BOLD}{M}'

# ─────────────────────────────────────────────────────────────────────────────
# STEP 4: CONNECTION MODE — NeonDB (server) or SQLite (client)
# ─────────────────────────────────────────────────────────────────────────────
#
#  SERVER / COLAB:  export DATABASE_URL="postgresql://neondb_owner:<pw>@<host>/neondb?sslmode=require&channel_binding=require"
#  CLIENT (local):  leave DATABASE_URL unset → SQLite file qtcl.db is created
#
# The NeonDB connection string is NEVER hard-coded here.
# Set it as an environment variable or a Colab secret (userdata.get).
# ─────────────────────────────────────────────────────────────────────────────

def _resolve_database_url() -> Tuple[str, str]:
    """
    Returns (db_url, db_mode) where db_mode is 'postgres' or 'sqlite'.
    Priority:
      1. DATABASE_URL environment variable
      2. Colab userdata secret (if running in Colab)
      3. Fall back to SQLite
    """
    # 1. Env var (works everywhere: Koyeb, local, Termux, Colab with os.environ)
    url = os.environ.get("DATABASE_URL", "").strip()
    if url and url.startswith("postgresql"):
        logger.info(f"{CLR.OK}[CONN] PostgreSQL mode via DATABASE_URL env{CLR.E}")
        return url, "postgres"

    # 2. SQLite fallback — local client mode (uses canonical data/ directory)
    repo_root = os.path.dirname(os.path.abspath(__file__))
    data_dir = os.path.join(repo_root, "data")
    os.makedirs(data_dir, exist_ok=True)
    sqlite_path = os.path.join(data_dir, "qtcl.db")
    logger.info(f"{CLR.WARN}[CONN] No DATABASE_URL found → SQLite client mode: {sqlite_path}{CLR.E}")
    return sqlite_path, "sqlite"


# Resolve at import time so builder classes can use it
_DB_URL, _DB_MODE = _resolve_database_url()

logger.info(f"[CONN] Mode: {_DB_MODE.upper()}")
if _DB_MODE == "postgres":
    # Redact password from log
    _log_url = _DB_URL.split("@")[-1] if "@" in _DB_URL else _DB_URL
    logger.info(f"[CONN] Host: {_log_url}")


# ─────────────────────────────────────────────────────────────────────────────
# STEP 4B: CLIENT-SIDE KEY VAULT (replaces Google/AWS KMS entirely)
# ─────────────────────────────────────────────────────────────────────────────
#
#  Security model — three-layer envelope encryption:
#
#   Layer 1  passphrase + device_pepper
#              ↓  Argon2id(m=65536, t=3, p=4) or scrypt fallback
#            KEK  (32 bytes, NEVER stored anywhere)
#
#   Layer 2  KEK + random salt
#              ↓  AES-256-GCM
#            wrapped_dek  (stored in wallet_encrypted_seeds.wrapped_dek_b64)
#
#   Layer 3  DEK (unwrapped in RAM) + random nonce
#              ↓  AES-256-GCM
#            ciphertext_b64  (BIP39 entropy, stored in wallet_encrypted_seeds)
#
#  Attacker with full DB dump + no passphrase = zero decryptable material.
#  Argon2id m=65536 = 64MB RAM cost — brute force on GPU is economically broken.
#
# ─────────────────────────────────────────────────────────────────────────────

import base64 as _b64
import secrets as _secrets
import hmac as _hmac
import struct as _struct

# Argon2id params (OWASP 2024 minimum: m=19456, t=2)
# We use 3× that for wallet keys.
_ARGON2_M_COST   = 65536   # 64 MB RAM
_ARGON2_T_COST   = 3       # iterations
_ARGON2_P_COST   = 4       # parallelism
_ARGON2_HASH_LEN = 32      # 256-bit KEK
_ARGON2_SALT_LEN = 32      # 256-bit salt

# scrypt fallback params (BIP38-compatible strength)
_SCRYPT_N = 1 << 17  # 131072
_SCRYPT_R = 8
_SCRYPT_P = 1


def _device_pepper() -> bytes:
    """
    Derive a stable device-specific pepper from local hardware identifiers.
    This binds encrypted keys to the device without storing the pepper.
    If hardware identifiers are unavailable, returns empty bytes (still secure,
    just not device-bound — passphrase strength alone protects the key).
    """
    import platform, socket
    parts = [
        platform.node(),
        platform.machine(),
        platform.processor(),
        socket.gethostname(),
    ]
    # Android/Termux: try ANDROID_ID
    for env_var in ["ANDROID_ID", "BUILD_FINGERPRINT", "SERIAL"]:
        v = os.environ.get(env_var, "")
        if v:
            parts.append(v)
    combined = "|".join(p for p in parts if p).encode()
    return _hashlib.sha256(combined).digest() if not ARGON2_AVAILABLE else            _hashlib.sha3_256(combined).digest()


def derive_kek(passphrase: str, salt: bytes, device_pepper: bytes = b"") -> bytes:
    """
    Derive Key-Encryption-Key (KEK) from passphrase + salt + device pepper.
    Returns 32 bytes. KEK is NEVER stored — re-derived on demand.

    Uses Argon2id if available, falls back to scrypt.
    """
    # Pepper the passphrase: HMAC(device_pepper, passphrase_bytes) if pepper present
    if device_pepper:
        pw_bytes = _hmac.new(device_pepper, passphrase.encode(), "sha3_256").digest()
    else:
        pw_bytes = passphrase.encode("utf-8")

    if ARGON2_AVAILABLE:
        return hash_secret_raw(
            secret=pw_bytes,
            salt=salt,
            time_cost=_ARGON2_T_COST,
            memory_cost=_ARGON2_M_COST,
            parallelism=_ARGON2_P_COST,
            hash_len=_ARGON2_HASH_LEN,
            type=Type.ID,
        )
    else:
        # scrypt fallback (BIP38-strength)
        return _hashlib.scrypt(
            pw_bytes, salt=salt,
            n=_SCRYPT_N, r=_SCRYPT_R, p=_SCRYPT_P,
            dklen=32
        )


def wrap_dek(dek: bytes, kek: bytes) -> tuple:
    """
    Encrypt DEK with KEK using AES-256-GCM.
    Returns (nonce_b64, ciphertext_b64) — both safe to store in DB.
    """
    nonce = _secrets.token_bytes(12)
    ct = AESGCM(kek).encrypt(nonce, dek, None)
    return (
        _b64.b64encode(nonce).decode(),
        _b64.b64encode(ct).decode(),
    )


def unwrap_dek(nonce_b64: str, ciphertext_b64: str, kek: bytes) -> bytes:
    """Decrypt wrapped DEK. Raises InvalidTag if KEK is wrong (wrong passphrase)."""
    nonce = _b64.b64decode(nonce_b64)
    ct    = _b64.b64decode(ciphertext_b64)
    return AESGCM(kek).decrypt(nonce, ct, None)


def encrypt_seed(seed_entropy: bytes, dek: bytes) -> tuple:
    """
    Encrypt BIP39 seed entropy (16-32 bytes) with DEK using AES-256-GCM.
    Returns (nonce_b64, ciphertext_b64).
    """
    nonce = _secrets.token_bytes(12)
    ct = AESGCM(dek).encrypt(nonce, seed_entropy, None)
    return (
        _b64.b64encode(nonce).decode(),
        _b64.b64encode(ct).decode(),
    )


def decrypt_seed(nonce_b64: str, ciphertext_b64: str, dek: bytes) -> bytes:
    """Decrypt BIP39 seed entropy. Raises InvalidTag on wrong DEK."""
    nonce = _b64.b64decode(nonce_b64)
    ct    = _b64.b64decode(ciphertext_b64)
    return AESGCM(dek).decrypt(nonce, ct, None)


def new_wallet_envelope(passphrase: str) -> dict:
    """
    Generate a complete wallet key envelope ready for DB insertion.
    Returns dict matching wallet_encrypted_seeds columns.
    Call with a fresh random BIP39 entropy (or your existing entropy).

    Example usage:
        import secrets
        entropy = secrets.token_bytes(32)   # 256-bit → 24-word mnemonic
        envelope = new_wallet_envelope("my strong passphrase")
        # then INSERT envelope into wallet_encrypted_seeds + store entropy's
        # ciphertext — the plaintext entropy should be wiped from RAM.
    """
    import secrets as _s
    kek_salt  = _s.token_bytes(_ARGON2_SALT_LEN)
    dek       = _s.token_bytes(32)
    pepper    = _device_pepper()
    kek       = derive_kek(passphrase, kek_salt, pepper)
    w_nonce, wrapped_dek = wrap_dek(dek, kek)

    kdf_type = "argon2id" if ARGON2_AVAILABLE else "scrypt"
    return {
        "kdf_type":          kdf_type,
        "kdf_salt_b64":      _b64.b64encode(kek_salt).decode(),
        "argon2_m_cost":     _ARGON2_M_COST   if ARGON2_AVAILABLE else None,
        "argon2_t_cost":     _ARGON2_T_COST   if ARGON2_AVAILABLE else None,
        "argon2_p_cost":     _ARGON2_P_COST   if ARGON2_AVAILABLE else None,
        "scrypt_n":          _SCRYPT_N         if not ARGON2_AVAILABLE else None,
        "scrypt_r":          _SCRYPT_R         if not ARGON2_AVAILABLE else None,
        "scrypt_p":          _SCRYPT_P         if not ARGON2_AVAILABLE else None,
        "dek_nonce_b64":     w_nonce,
        "wrapped_dek_b64":   wrapped_dek,
        "device_bound":      bool(pepper),
        # caller must fill: wallet_fingerprint, seed_nonce_b64, seed_ciphertext_b64, bip32_xpub
    }

logger.info(f"[KEYVAULT] Client-side KEK: {'Argon2id' if ARGON2_AVAILABLE else 'scrypt (fallback)'}")
logger.info(f"[KEYVAULT] Device-bound pepper: enabled")


# ─────────────────────────────────────────────────────────────────────────────
# STEP 5: MATHEMATICAL STRUCTURES (Optimized for Colab memory)
# ─────────────────────────────────────────────────────────────────────────────

@dataclass(frozen=True, slots=True)
class HyperbolicPoint:
    x: Any
    y: Any
    name: Optional[str] = None
    
    def to_db_tuple(self) -> Tuple[str, str, Optional[str]]:
        return (str(self.x), str(self.y), self.name)
    
    @staticmethod
    def from_db_tuple(x: str, y: str, name: Optional[str]) -> 'HyperbolicPoint':
        return HyperbolicPoint(mpf(x), mpf(y), name)


@dataclass(slots=True)
class HyperbolicTriangle:
    triangle_id: int
    v0: HyperbolicPoint
    v1: HyperbolicPoint
    v2: HyperbolicPoint
    depth: int = 0
    parent_id: Optional[int] = None
    
    def to_db_row(self) -> Tuple:
        return (
            self.triangle_id, self.depth, self.parent_id,
            *self.v0.to_db_tuple(),
            *self.v1.to_db_tuple(),
            *self.v2.to_db_tuple()
        )


@dataclass(slots=True)
class Pseudoqubit:
    pseudoqubit_id: int
    triangle_id: int
    x: Any
    y: Any
    placement_type: str
    phase_theta: Any = field(default_factory=lambda: mpf(0))
    coherence_measure: Any = field(default_factory=lambda: mpf("0.99"))
    
    def to_db_row(self) -> Tuple:
        return (
            self.pseudoqubit_id, self.triangle_id,
            str(self.x), str(self.y), self.placement_type,
            str(self.phase_theta), str(self.coherence_measure)
        )

# ─────────────────────────────────────────────────────────────────────────────
# STEP 6: HYPERBOLIC GEOMETRY (Mathematically Corrected)
# ─────────────────────────────────────────────────────────────────────────────

def _safe_atanh(x: Any) -> Any:
    if not MPMATH_AVAILABLE:
        return 0.5 * log((1 + x) / (1 - x)) if abs(x) < 1 else float('inf')
    x_clamped = max(mpf(-1) + mpf(10)**(-140), min(mpf(1) - mpf(10)**(-140), x))
    return atanh(x_clamped)


def hyperbolic_distance(p1: HyperbolicPoint, p2: HyperbolicPoint) -> Any:
    z1 = mpc(p1.x, p1.y)
    z2 = mpc(p2.x, p2.y)
    numerator = abs(z1 - z2)
    denominator = abs(mpf(1) - conj(z1) * z2)
    if denominator < mpf(10)**(-140):
        return mpf(0) if numerator < mpf(10)**(-140) else mpf('inf')
    ratio = numerator / denominator
    ratio = max(mpf(0), min(mpf(1) - mpf(10)**(-140), ratio))
    return mpf(2) * _safe_atanh(ratio)


def _safe_vertex_name(name: Optional[str], max_len: int = 200) -> Optional[str]:
    """Safely truncate vertex names to prevent database column overflow"""
    if not name:
        return None
    if len(name) <= max_len:
        return name
    # Hash suffix for long names
    import hashlib
    hash_suffix = hashlib.md5(name.encode()).hexdigest()[:8]
    return name[:max_len-9] + "_" + hash_suffix


def poincare_midpoint(p1: HyperbolicPoint, p2: HyperbolicPoint) -> HyperbolicPoint:
    z1 = mpc(p1.x, p1.y)
    z2 = mpc(p2.x, p2.y)
    denominator = mpf(1) + conj(z1) * z2
    if abs(denominator) < mpf(10)**(-140):
        # Use simple naming to avoid length issues
        fallback_name = "mid_fallback_p1" if abs(z1) < abs(z2) else "mid_fallback_p2"
        target_pt = p1 if abs(z1) < abs(z2) else p2
        return HyperbolicPoint(target_pt.x, target_pt.y, name=fallback_name)
    m = (z1 + z2) / denominator
    # Use simple naming: just use a hash of coordinates instead of parent names
    return HyperbolicPoint(mre(m), mim(m), name="midpoint")


def hyperbolic_angle_at_vertex(a: HyperbolicPoint, b: HyperbolicPoint, c: HyperbolicPoint) -> Any:
    a_len = hyperbolic_distance(b, c)
    b_len = hyperbolic_distance(a, c)
    c_len = hyperbolic_distance(a, b)
    cosh_a, cosh_b, cosh_c = cosh(a_len), cosh(b_len), cosh(c_len)
    sinh_a, sinh_c = sinh(a_len), sinh(c_len)
    if sinh_a * sinh_c < mpf(10)**(-140):
        return mpf(0)
    cos_angle = (cosh_b - cosh_a * cosh_c) / (sinh_a * sinh_c)
    cos_angle = max(mpf(-1), min(mpf(1), cos_angle))
    return acos(cos_angle)


def hyperbolic_incenter(tri: HyperbolicTriangle) -> HyperbolicPoint:
    a = hyperbolic_distance(tri.v1, tri.v2)
    b = hyperbolic_distance(tri.v0, tri.v2)
    c = hyperbolic_distance(tri.v0, tri.v1)
    w0 = mpf(1) / sinh(a) if sinh(a) > mpf(10)**(-140) else mpf(1)
    w1 = mpf(1) / sinh(b) if sinh(b) > mpf(10)**(-140) else mpf(1)
    w2 = mpf(1) / sinh(c) if sinh(c) > mpf(10)**(-140) else mpf(1)
    total = w0 + w1 + w2
    if total < mpf(10)**(-140):
        return tri.v0
    x = (w0 * tri.v0.x + w1 * tri.v1.x + w2 * tri.v2.x) / total
    y = (w0 * tri.v0.y + w1 * tri.v1.y + w2 * tri.v2.y) / total
    return HyperbolicPoint(x, y, name=f"incenter_{tri.triangle_id}")


def hyperbolic_circumcenter(tri: HyperbolicTriangle) -> HyperbolicPoint:
    ax, ay = tri.v0.x, tri.v0.y
    bx, by = tri.v1.x, tri.v1.y
    cx, cy = tri.v2.x, tri.v2.y
    d = mpf(2) * (ax * (by - cy) + bx * (cy - ay) + cx * (ay - by))
    if fabs(d) < mpf(10)**(-140):
        x = (ax + bx + cx) / mpf(3)
        y = (ay + by + cy) / mpf(3)
        return HyperbolicPoint(x, y, name=f"circumcenter_degen_{tri.triangle_id}")
    ux = ((ax**2 + ay**2) * (by - cy) + (bx**2 + by**2) * (cy - ay) + (cx**2 + cy**2) * (ay - by)) / d
    uy = ((ax**2 + ay**2) * (cx - bx) + (bx**2 + by**2) * (ax - cx) + (cx**2 + cy**2) * (bx - ax)) / d
    if ux**2 + uy**2 >= mpf(1) - mpf(10)**(-140):
        return hyperbolic_incenter(tri)
    return HyperbolicPoint(ux, uy, name=f"circumcenter_{tri.triangle_id}")


def hyperbolic_orthocenter(tri: HyperbolicTriangle) -> HyperbolicPoint:
    try:
        x = (tri.v0.x + tri.v1.x + tri.v2.x) / mpf(3)
        y = (tri.v0.y + tri.v1.y + tri.v2.y) / mpf(3)
        return HyperbolicPoint(x, y, name=f"orthocenter_{tri.triangle_id}")
    except Exception:
        return hyperbolic_incenter(tri)


def hyperbolic_geodesic_interpolate(p1: HyperbolicPoint, p2: HyperbolicPoint, t: Any) -> HyperbolicPoint:
    z1 = mpc(p1.x, p1.y)
    z2 = mpc(p2.x, p2.y)
    z_t = (mpf(1) - t) * z1 + t * z2
    r2 = abs(z_t)**2
    if r2 >= mpf(1) - mpf(10)**(-140):
        z_t = z_t * (mpf(1) - mpf(10)**(-140)) / sqrt(r2)
    # Keep name short to avoid truncation
    t_str = f"{float(t):.6f}".replace('.', '_')
    return HyperbolicPoint(mre(z_t), mim(z_t), name=_safe_vertex_name(f"geo_t{t_str}"))


def generate_geodesic_grid(tri: HyperbolicTriangle, density: int = 5) -> List[HyperbolicPoint]:
    points = []
    for i in range(1, density):
        for j in range(1, density - i):
            lambda1 = mpf(i) / mpf(density)
            lambda2 = mpf(j) / mpf(density)
            lambda3 = mpf(1) - lambda1 - lambda2
            if lambda1 + lambda2 > 0:
                pt_01 = hyperbolic_geodesic_interpolate(tri.v0, tri.v1, lambda2 / (lambda1 + lambda2))
            else:
                pt_01 = tri.v0
            pt = hyperbolic_geodesic_interpolate(tri.v2, pt_01, lambda3)
            # Use simple indexed name to avoid length issues
            points.append(HyperbolicPoint(pt.x, pt.y, name=f"g_{len(points)}"))
    # Add center point (barycenter) as 7th point
    center_x = (tri.v0.x + tri.v1.x + tri.v2.x) / mpf(3)
    center_y = (tri.v0.y + tri.v1.y + tri.v2.y) / mpf(3)
    points.append(HyperbolicPoint(center_x, center_y, name="g_center"))
    return points[:7]  # Ensure exactly 7 points

# ─────────────────────────────────────────────────────────────────────────────
# STEP 7: COMPLETE SCHEMA (Your exact 58-table schema)
# ─────────────────────────────────────────────────────────────────────────────

COMPLETE_SCHEMA = """
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS "pg_trgm";
CREATE EXTENSION IF NOT EXISTS "btree_gin";
CREATE EXTENSION IF NOT EXISTS "btree_gist";

-- ════════════════════════════════════════════════════════════════════════════════════════════════════════════
-- QUANTUM LATTICE BLOCKCHAIN COMPLETE SCHEMA v2.0 - COMPREHENSIVE PATCH
-- ════════════════════════════════════════════════════════════════════════════════════════════════════════════
--
-- ALL 62 tables from db_builder integrated with blockchain core
-- Lattice geometry + Quantum measurements + Oracle state + Blockchain + Network + Wallet
--
-- Created: 2025
-- License: MIT
--
-- ════════════════════════════════════════════════════════════════════════════════════════════════════════════

-- TABLE: address_balance_history
CREATE TABLE address_balance_history (
    id BIGSERIAL PRIMARY KEY,
    address VARCHAR(255) NOT NULL,
    block_height BIGINT NOT NULL,
    block_hash VARCHAR(255),
    balance NUMERIC(30, 0) NOT NULL,
    delta NUMERIC(30, 0),
    snapshot_timestamp TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    UNIQUE(address, block_height)
);

-- TABLE: address_labels
CREATE TABLE address_labels (
    label_id BIGSERIAL PRIMARY KEY,
    address VARCHAR(255) NOT NULL,
    label VARCHAR(255) NOT NULL,
    description TEXT,
    label_type VARCHAR(50),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- TABLE: address_transactions
CREATE TABLE address_transactions (
    id BIGSERIAL PRIMARY KEY,
    address VARCHAR(255) NOT NULL,
    tx_hash VARCHAR(255) NOT NULL,
    direction VARCHAR(10) NOT NULL,
    from_address VARCHAR(255),
    to_address VARCHAR(255),
    amount NUMERIC(30, 0),
    block_height BIGINT,
    block_hash VARCHAR(255),
    block_timestamp BIGINT,
    tx_status VARCHAR(50) DEFAULT 'pending',
    notes TEXT,
    label VARCHAR(255),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    UNIQUE(address, tx_hash)
);

-- TABLE: address_utxos
CREATE TABLE address_utxos (
    utxo_id BIGSERIAL PRIMARY KEY,
    address VARCHAR(255) NOT NULL,
    tx_hash VARCHAR(255) NOT NULL,
    output_index INT NOT NULL,
    amount NUMERIC(30, 0) NOT NULL,
    spent BOOLEAN DEFAULT FALSE,
    spent_at_height BIGINT,
    spent_in_tx_hash VARCHAR(255),
    created_at_height BIGINT,
    created_at_timestamp BIGINT
);

-- TABLE: audit_logs
CREATE TABLE audit_logs (
    log_id BIGSERIAL PRIMARY KEY,
    event_type VARCHAR(100) NOT NULL,
    actor_peer_id VARCHAR(255),
    action VARCHAR(255),
    resource_type VARCHAR(100),
    resource_id VARCHAR(255),
    changes JSONB,
    result VARCHAR(50),
    error_message TEXT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- TABLE: block_headers_cache
CREATE TABLE block_headers_cache (
    height BIGINT PRIMARY KEY,
    block_hash VARCHAR(255) UNIQUE NOT NULL,
    previous_hash VARCHAR(255) NOT NULL,
    state_root VARCHAR(255),
    transactions_root VARCHAR(255),
    timestamp BIGINT NOT NULL,
    difficulty NUMERIC(20, 10),
    nonce VARCHAR(255),
    quantum_proof VARCHAR(255),
    quantum_state_hash VARCHAR(255),
    temporal_coherence NUMERIC(5, 4),
    pq_signature TEXT,
    pq_key_fingerprint VARCHAR(255),
    received_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- TABLE: blocks
CREATE TABLE blocks (
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
    created_at                 TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_blocks_hash ON blocks(block_hash);
CREATE INDEX IF NOT EXISTS idx_blocks_parent ON blocks(parent_hash);
CREATE INDEX IF NOT EXISTS idx_blocks_timestamp ON blocks(timestamp);

-- TABLE: chain_reorganizations
CREATE TABLE chain_reorganizations (
    reorg_id BIGSERIAL PRIMARY KEY,
    timestamp BIGINT NOT NULL,
    reorg_depth INT NOT NULL,
    old_head_height BIGINT,
    new_head_height BIGINT,
    old_head_hash VARCHAR(255),
    new_head_hash VARCHAR(255),
    reorg_point_hash VARCHAR(255),
    transactions_reverted INT,
    transactions_reinserted INT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- TABLE: client_block_sync
CREATE TABLE client_block_sync (
    sync_id BIGSERIAL PRIMARY KEY,
    peer_id VARCHAR(255) NOT NULL,
    blocks_downloaded INT,
    blocks_requested INT,
    blocks_total INT,
    sync_started_at TIMESTAMP WITH TIME ZONE,
    sync_completed_at TIMESTAMP WITH TIME ZONE,
    sync_status VARCHAR(50),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- TABLE: client_network_metrics
CREATE TABLE client_network_metrics (
    metric_id BIGSERIAL PRIMARY KEY,
    peer_id VARCHAR(255) NOT NULL,
    timestamp BIGINT NOT NULL,
    latency_ms NUMERIC(10, 2),
    bandwidth_in_kbps NUMERIC(15, 2),
    bandwidth_out_kbps NUMERIC(15, 2),
    packet_loss_rate NUMERIC(5, 4),
    blocks_per_second NUMERIC(10, 2),
    avg_sync_time_ms NUMERIC(15, 2),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- TABLE: client_oracle_sync
CREATE TABLE client_oracle_sync (
    sync_id BIGSERIAL PRIMARY KEY,
    peer_id VARCHAR(255) NOT NULL UNIQUE,
    block_height_local BIGINT,
    block_height_oracle BIGINT,
    w_state_hash_local VARCHAR(255),
    w_state_hash_oracle VARCHAR(255),
    density_matrix_hash_local VARCHAR(255),
    density_matrix_hash_oracle VARCHAR(255),
    density_matrix_sync_status VARCHAR(50) DEFAULT 'pending',
    entropy_hash_local VARCHAR(255),
    entropy_hash_oracle VARCHAR(255),
    coherence_measure_local NUMERIC(5, 4),
    coherence_measure_oracle NUMERIC(5, 4),
    coherence_aligned BOOLEAN DEFAULT FALSE,
    lattice_sync_quality NUMERIC(5, 4),
    tessellation_in_sync BOOLEAN DEFAULT FALSE,
    last_lattice_update TIMESTAMP WITH TIME ZONE,
    sync_status VARCHAR(50) DEFAULT 'initializing',
    sync_confidence NUMERIC(5, 4) DEFAULT 0.0,
    last_sync_attempt TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    last_successful_sync TIMESTAMP WITH TIME ZONE,
    sync_error_message TEXT,
    sync_attempt_count INT DEFAULT 0,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- TABLE: client_sync_events
CREATE TABLE client_sync_events (
    event_id BIGSERIAL PRIMARY KEY,
    peer_id VARCHAR(255) NOT NULL,
    timestamp BIGINT NOT NULL,
    event_type VARCHAR(50),
    event_description TEXT,
    details JSONB,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- TABLE: consensus_events
CREATE TABLE consensus_events (
    event_id BIGSERIAL PRIMARY KEY,
    block_height BIGINT,
    timestamp BIGINT NOT NULL,
    event_type VARCHAR(100),
    event_description TEXT,
    severity VARCHAR(20),
    details JSONB,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- TABLE: database_metadata
CREATE TABLE database_metadata (
    metadata_id BIGSERIAL PRIMARY KEY,
    schema_version VARCHAR(50),
    build_timestamp TIMESTAMP WITH TIME ZONE,
    build_info JSONB,
    tables_created INT,
    indexes_created INT,
    constraints_created INT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- TABLE: encrypted_private_keys
CREATE TABLE encrypted_private_keys (
    key_id BIGSERIAL PRIMARY KEY,
    address VARCHAR(255) NOT NULL UNIQUE,
    algorithm VARCHAR(50) DEFAULT 'AES-256-GCM',
    kdf_algorithm VARCHAR(50) DEFAULT 'PBKDF2-SHA3-512',
    kdf_iterations INT DEFAULT 16384,
    nonce_hex VARCHAR(255) NOT NULL,
    salt_hex VARCHAR(255) NOT NULL,
    ciphertext_hex TEXT NOT NULL,
    auth_tag_hex VARCHAR(255),
    key_fingerprint VARCHAR(255),
    derivation_path VARCHAR(100),
    is_locked BOOLEAN DEFAULT FALSE,
    lock_reason TEXT,
    last_used_for_signing TIMESTAMP WITH TIME ZONE,
    signing_count INT DEFAULT 0,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- TABLE: entanglement_records
CREATE TABLE entanglement_records (
    entanglement_id BIGSERIAL PRIMARY KEY,
    block_height BIGINT NOT NULL,
    timestamp BIGINT NOT NULL,
    peer_id VARCHAR(255) NOT NULL,
    peer_public_key VARCHAR(255),
    entanglement_type VARCHAR(50),
    entanglement_measure NUMERIC(5, 4),
    bell_parameter NUMERIC(5, 4),
    oracle_entanglement_measure NUMERIC(5, 4),
    entanglement_match_score NUMERIC(5, 4),
    in_sync_with_oracle BOOLEAN DEFAULT FALSE,
    local_w_state_hash VARCHAR(255),
    local_density_matrix_hash VARCHAR(255),
    verification_proof TEXT,
    verified BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- TABLE: entropy_quality_log
CREATE TABLE entropy_quality_log (
    log_id BIGSERIAL PRIMARY KEY,
    timestamp BIGINT NOT NULL,
    anu_qrng_quality NUMERIC(5, 4),
    random_org_quality NUMERIC(5, 4),
    qbick_quality NUMERIC(5, 4),
    outshift_quality NUMERIC(5, 4),
    hotbits_quality NUMERIC(5, 4),
    ensemble_min_entropy NUMERIC(5, 4),
    ensemble_shannon_entropy NUMERIC(5, 4),
    passed_diehard BOOLEAN,
    passed_nist BOOLEAN,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- TABLE: epoch_validators
CREATE TABLE epoch_validators (
    membership_id BIGSERIAL PRIMARY KEY,
    epoch_id BIGINT NOT NULL,
    validator_id BIGINT NOT NULL,
    stake NUMERIC(30, 0) NOT NULL,
    blocks_proposed INT DEFAULT 0,
    blocks_attested INT DEFAULT 0,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    UNIQUE(epoch_id, validator_id)
);

-- TABLE: epochs
CREATE TABLE epochs (
    epoch_id BIGSERIAL PRIMARY KEY,
    epoch_number BIGINT UNIQUE NOT NULL,
    start_block_height BIGINT NOT NULL,
    end_block_height BIGINT,
    validator_count INT,
    total_stake NUMERIC(30, 0),
    finalized BOOLEAN DEFAULT FALSE,
    finalized_at TIMESTAMP WITH TIME ZONE,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- TABLE: finality_records
CREATE TABLE finality_records (
    finality_id BIGSERIAL PRIMARY KEY,
    block_height BIGINT NOT NULL UNIQUE,
    block_hash VARCHAR(255) NOT NULL,
    finalized BOOLEAN DEFAULT FALSE,
    finalized_at TIMESTAMP WITH TIME ZONE,
    finality_epoch BIGINT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- TABLE: hyperbolic_triangles
CREATE TABLE hyperbolic_triangles (
    triangle_id BIGINT PRIMARY KEY,
    depth INT NOT NULL,
    parent_id BIGINT,
    v0_x NUMERIC(250, 210) NOT NULL,
    v0_y NUMERIC(250, 210) NOT NULL,
    v0_name TEXT,
    v1_x NUMERIC(250, 210) NOT NULL,
    v1_y NUMERIC(250, 210) NOT NULL,
    v1_name TEXT,
    v2_x NUMERIC(250, 210) NOT NULL,
    v2_y NUMERIC(250, 210) NOT NULL,
    v2_name TEXT,
    area NUMERIC(250, 210),
    perimeter NUMERIC(250, 210),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- TABLE: lattice_sync_state
CREATE TABLE lattice_sync_state (
    state_id BIGSERIAL PRIMARY KEY,
    peer_id VARCHAR(255) NOT NULL,
    block_height BIGINT NOT NULL,
    timestamp BIGINT NOT NULL,
    hyperbolic_coordinates_synced JSONB,
    poincare_disk_coverage NUMERIC(5, 4),
    vertex_synchronization_count INT,
    edge_synchronization_count INT,
    pseudoqubit_positions_hash VARCHAR(255),
    pseudoqubit_lattice_sync_quality NUMERIC(5, 4),
    lattice_coherence_measure NUMERIC(5, 4),
    critical_points_coherence JSONB,
    geodesic_paths_synchronized BOOLEAN DEFAULT FALSE,
    updates_since_last_sync INT DEFAULT 0,
    bytes_synchronized BIGINT DEFAULT 0,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- TABLE: merkle_proofs
CREATE TABLE merkle_proofs (
    proof_id BIGSERIAL PRIMARY KEY,
    transaction_hash VARCHAR(255) NOT NULL,
    height BIGINT,
    block_hash VARCHAR(255) NOT NULL,
    proof_path TEXT NOT NULL,
    proof_index INT NOT NULL,
    verified BOOLEAN DEFAULT FALSE,
    verified_at TIMESTAMP WITH TIME ZONE,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    UNIQUE(transaction_hash, block_hash)
);

-- TABLE: network_bandwidth_usage
CREATE TABLE network_bandwidth_usage (
    usage_id BIGSERIAL PRIMARY KEY,
    peer_id VARCHAR(255),
    timestamp BIGINT NOT NULL,
    bandwidth_in_kbps NUMERIC(15, 2),
    bandwidth_out_kbps NUMERIC(15, 2),
    total_bandwidth_kbps NUMERIC(15, 2),
    bytes_in INT,
    bytes_out INT,
    congestion_level NUMERIC(5, 4),
    packet_loss_rate NUMERIC(5, 4),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- TABLE: network_events
CREATE TABLE network_events (
    event_id BIGSERIAL PRIMARY KEY,
    timestamp BIGINT NOT NULL,
    event_type VARCHAR(100) NOT NULL,
    event_description TEXT,
    affected_peers INT,
    details JSONB,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- TABLE: network_partition_events
CREATE TABLE network_partition_events (
    event_id BIGSERIAL PRIMARY KEY,
    timestamp BIGINT NOT NULL,
    partition_detected BOOLEAN DEFAULT TRUE,
    peers_in_partition_1 INT,
    peers_in_partition_2 INT,
    partition_healed BOOLEAN DEFAULT FALSE,
    healing_timestamp BIGINT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- TABLE: oracle_registry
CREATE TABLE oracle_registry (
    oracle_id       VARCHAR(128)  PRIMARY KEY,
    oracle_url      VARCHAR(512)  NOT NULL DEFAULT '',
    oracle_address  VARCHAR(128)  NOT NULL DEFAULT '',
    is_primary      BOOLEAN       NOT NULL DEFAULT FALSE,
    last_seen       BIGINT        NOT NULL DEFAULT 0,
    block_height    BIGINT        NOT NULL DEFAULT 0,
    peer_count      INTEGER       NOT NULL DEFAULT 0,
    gossip_url      JSONB         NOT NULL DEFAULT '{}'::JSONB,
    created_at      TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    -- ── ON-CHAIN IDENTITY COLUMNS (oracle_reg TX pipeline) ─────────────────
    wallet_address  VARCHAR(128)  NOT NULL DEFAULT '',
    oracle_pub_key  TEXT          NOT NULL DEFAULT '',
    cert_sig        VARCHAR(128)  NOT NULL DEFAULT '',
    cert_auth_tag   VARCHAR(128)  NOT NULL DEFAULT '',
    mode            VARCHAR(32)   NOT NULL DEFAULT 'full',
    ip_hint         VARCHAR(256)  NOT NULL DEFAULT '',
    reg_tx_hash     VARCHAR(64)   NOT NULL DEFAULT '',
    registered_at   BIGINT        NOT NULL DEFAULT 0
);
CREATE INDEX IF NOT EXISTS idx_oracle_registry_last_seen     ON oracle_registry (last_seen DESC);
CREATE INDEX IF NOT EXISTS idx_oracle_registry_primary       ON oracle_registry (is_primary) WHERE is_primary = TRUE;
CREATE INDEX IF NOT EXISTS idx_oracle_registry_wallet        ON oracle_registry (wallet_address);
CREATE INDEX IF NOT EXISTS idx_oracle_registry_reg_tx        ON oracle_registry (reg_tx_hash) WHERE reg_tx_hash != '';
CREATE INDEX IF NOT EXISTS idx_oracle_registry_registered_at ON oracle_registry (registered_at DESC);

-- TABLE: oracle_coherence_metrics
CREATE TABLE oracle_coherence_metrics (
    metric_id BIGSERIAL PRIMARY KEY,
    block_height BIGINT NOT NULL,
    timestamp BIGINT NOT NULL,
    system_coherence_measure NUMERIC(5, 4),
    lattice_coherence_score NUMERIC(5, 4),
    tessellation_synchronization_quality NUMERIC(5, 4),
    pseudoqubit_coherence_array JSONB,
    min_coherence NUMERIC(5, 4),
    max_coherence NUMERIC(5, 4),
    avg_coherence NUMERIC(5, 4),
    phase_drift_radians NUMERIC(200, 150),
    phase_correction_applied BOOLEAN,
    validator_agreement_score NUMERIC(5, 4),
    network_partition_detected BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- TABLE: oracle_consensus_state
CREATE TABLE oracle_consensus_state (
    consensus_id BIGSERIAL PRIMARY KEY,
    block_height BIGINT NOT NULL,
    timestamp BIGINT NOT NULL,
    oracle_consensus_reached BOOLEAN DEFAULT FALSE,
    validator_agreement_count INT,
    total_validators INT,
    consensus_threshold NUMERIC(5, 4),
    w_state_hash_agreement BOOLEAN,
    density_matrix_hash_agreement BOOLEAN,
    entropy_hash_agreement BOOLEAN,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    UNIQUE(block_height)
);

-- TABLE: oracle_density_matrix_stream
CREATE TABLE oracle_density_matrix_stream (
    stream_id BIGSERIAL PRIMARY KEY,
    block_height BIGINT NOT NULL,
    timestamp BIGINT NOT NULL,
    density_matrix_json JSONB NOT NULL,
    density_matrix_hash VARCHAR(255) UNIQUE NOT NULL,
    trace_value NUMERIC(5, 4),
    purity NUMERIC(5, 4),
    von_neumann_entropy NUMERIC(5, 4),
    eigenvalues JSONB,
    live_metrics_json JSONB,
    sensor_timestamps JSONB,
    update_sequence_number BIGINT,
    time_since_last_update_ms NUMERIC(15, 2),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- TABLE: oracle_distribution_log
CREATE TABLE oracle_distribution_log (
    log_id BIGSERIAL PRIMARY KEY,
    block_height BIGINT NOT NULL,
    timestamp BIGINT NOT NULL,
    peer_id VARCHAR(255) NOT NULL,
    data_type VARCHAR(50),
    data_hash VARCHAR(255),
    distribution_successful BOOLEAN DEFAULT TRUE,
    distribution_latency_ms NUMERIC(15, 2),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- TABLE: oracle_entanglement_records
CREATE TABLE oracle_entanglement_records (
    entanglement_id BIGSERIAL PRIMARY KEY,
    block_height BIGINT NOT NULL,
    timestamp BIGINT NOT NULL,
    peer_id VARCHAR(255) NOT NULL,
    peer_public_key VARCHAR(255),
    entanglement_type VARCHAR(50),
    entanglement_measure NUMERIC(5, 4),
    bell_parameter NUMERIC(5, 4),
    oracle_entanglement_measure NUMERIC(5, 4),
    entanglement_match_score NUMERIC(5, 4),
    in_sync_with_oracle BOOLEAN DEFAULT FALSE,
    verification_proof TEXT,
    verified BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- TABLE: oracle_entropy_feeds
CREATE TABLE oracle_entropy_feeds (
    feed_id BIGSERIAL PRIMARY KEY,
    block_height BIGINT NOT NULL,
    timestamp BIGINT NOT NULL,
    anu_qrng_entropy TEXT,
    random_org_entropy TEXT,
    qbick_entropy TEXT,
    outshift_entropy TEXT,
    hotbits_entropy TEXT,
    xor_combined_seed TEXT,
    entropy_hash VARCHAR(255) UNIQUE NOT NULL,
    min_entropy_estimate NUMERIC(5, 4),
    shannon_entropy_estimate NUMERIC(5, 4),
    source_agreement_score NUMERIC(5, 4),
    distributed_to_peers INT DEFAULT 0,
    distribution_complete BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- TABLE: oracle_pq0_state
CREATE TABLE oracle_pq0_state (
    state_id BIGSERIAL PRIMARY KEY,
    block_height BIGINT NOT NULL,
    timestamp BIGINT NOT NULL,
    oracle_pq_id BIGINT,
    oracle_position_x NUMERIC(200, 150),
    oracle_position_y NUMERIC(200, 150),
    pq_inverse_virtual_id BIGINT,
    pq_virtual_id BIGINT,
    quantum_state_json JSONB,
    phase_theta NUMERIC(200, 150),
    coherence_measure NUMERIC(5, 4),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- TABLE: oracle_w_state_snapshots
CREATE TABLE oracle_w_state_snapshots (
    snapshot_id BIGSERIAL PRIMARY KEY,
    block_height BIGINT NOT NULL,
    block_hash VARCHAR(255) NOT NULL,
    timestamp BIGINT NOT NULL,
    w_state_serialized TEXT NOT NULL,
    w_state_hash VARCHAR(255) UNIQUE NOT NULL,
    entanglement_measure NUMERIC(5, 4),
    coherence_time_us NUMERIC(15, 2),
    fidelity_estimate NUMERIC(5, 4),
    quantum_proof_data TEXT,
    quantum_proof_hash VARCHAR(255),
    shannon_entropy NUMERIC(5, 4),
    entropy_source_quality JSONB,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    UNIQUE(block_height, block_hash)
);

-- TABLE: pq0_entanglement_log — Tripartite pq0 entanglement chain log
CREATE TABLE pq0_entanglement_log (
    id                      BIGSERIAL PRIMARY KEY,
    epoch                   BIGINT NOT NULL DEFAULT 0,
    block_height            BIGINT NOT NULL DEFAULT 0,
    pq0                     BIGINT NOT NULL DEFAULT 0,
    oracle_ids              TEXT NOT NULL DEFAULT '',
    entanglement_matrix_hex TEXT NOT NULL DEFAULT '',
    created_at              TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_pq0_height ON pq0_entanglement_log(block_height);
CREATE INDEX IF NOT EXISTS idx_pq0_pq0 ON pq0_entanglement_log(pq0);

-- TABLE: wstate_consensus_log — W-state BFT consensus log
CREATE TABLE wstate_consensus_log (
    id               BIGSERIAL PRIMARY KEY,
    block_height     BIGINT NOT NULL,
    median_fidelity  NUMERIC(5,4) NOT NULL,
    agreement_score  NUMERIC(5,4) NOT NULL,
    peer_count       INT NOT NULL,
    quorum_hash_hex  TEXT,
    pow_seed         TEXT,
    created_at       TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_wscl_height ON wstate_consensus_log(block_height);
CREATE INDEX IF NOT EXISTS idx_wscl_fidelity ON wstate_consensus_log(median_fidelity);

-- TABLE: orphan_blocks
CREATE TABLE orphan_blocks (
    block_hash VARCHAR(255) PRIMARY KEY,
    parent_hash VARCHAR(255) NOT NULL,
    block_height BIGINT,
    timestamp BIGINT,
    block_data_compressed BYTEA,
    block_size_bytes INT,
    received_from_peer VARCHAR(255),
    received_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    expires_at TIMESTAMP WITH TIME ZONE,
    resolution_status VARCHAR(50) DEFAULT 'awaiting_parent',
    resolution_attempt_count INT DEFAULT 0
);

-- TABLE: peer_connections
CREATE TABLE peer_connections (
    connection_id BIGSERIAL PRIMARY KEY,
    peer_id VARCHAR(255) NOT NULL,
    connection_state VARCHAR(50) DEFAULT 'disconnected',
    established_at TIMESTAMP WITH TIME ZONE,
    disconnected_at TIMESTAMP WITH TIME ZONE,
    latency_ms NUMERIC(10, 2),
    bandwidth_in_kbps NUMERIC(15, 2),
    bandwidth_out_kbps NUMERIC(15, 2),
    packet_loss_rate NUMERIC(5, 4),
    blocks_sync_height BIGINT,
    last_message_at TIMESTAMP WITH TIME ZONE,
    messages_sent INT DEFAULT 0,
    messages_received INT DEFAULT 0,
    bytes_sent BIGINT DEFAULT 0,
    bytes_received BIGINT DEFAULT 0,
    oracle_state_shared BOOLEAN DEFAULT FALSE,
    density_matrix_shared BOOLEAN DEFAULT FALSE,
    entropy_shared BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- TABLE: peer_registry
CREATE TABLE peer_registry (
    peer_id VARCHAR(255) PRIMARY KEY,
    public_key VARCHAR(255) UNIQUE NOT NULL,
    ip_address VARCHAR(45),
    port INTEGER,
    peer_type VARCHAR(50) DEFAULT 'full',
    capabilities TEXT[],
    block_height BIGINT DEFAULT 0,
    chain_head_hash VARCHAR(255),
    network_version VARCHAR(50),
    reputation_score NUMERIC(10, 4) DEFAULT 1.0,
    blocks_validated INT DEFAULT 0,
    blocks_rejected INT DEFAULT 0,
    last_seen TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    last_handshake TIMESTAMP WITH TIME ZONE,
    connection_attempts INT DEFAULT 0,
    failed_attempts INT DEFAULT 0,
    oracle_entanglement_ready BOOLEAN DEFAULT FALSE,
    oracle_density_matrix_version BIGINT DEFAULT 0,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- TABLE: peer_reputation
CREATE TABLE peer_reputation (
    reputation_id BIGSERIAL PRIMARY KEY,
    peer_id VARCHAR(255) NOT NULL,
    timestamp BIGINT NOT NULL,
    score NUMERIC(10, 4) NOT NULL,
    factors JSONB,
    event_type VARCHAR(50),
    event_description TEXT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- TABLE: pseudoqubits
CREATE TABLE pseudoqubits (
    pq_id BIGINT PRIMARY KEY,
    triangle_id BIGINT NOT NULL,
    x NUMERIC(200, 150) NOT NULL,
    y NUMERIC(200, 150) NOT NULL,
    placement_type VARCHAR(50) NOT NULL,
    phase_theta NUMERIC(200, 150) DEFAULT 0,
    coherence_measure NUMERIC(5, 4) DEFAULT 0.99,
    coherence_time_us NUMERIC(15, 2) DEFAULT 100000,
    entanglement_with_oracle NUMERIC(5, 4) DEFAULT 0,
    entanglement_measure_neighbors JSONB DEFAULT '{}'::jsonb,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    last_measured_at TIMESTAMP WITH TIME ZONE
);

-- TABLE: quantum_circuit_execution
CREATE TABLE quantum_circuit_execution (
    circuit_id BIGSERIAL PRIMARY KEY,
    block_height BIGINT NOT NULL,
    timestamp BIGINT NOT NULL,
    circuit_depth INT,
    circuit_size INT,
    num_qubits INT,
    num_gates INT,
    execution_successful BOOLEAN DEFAULT TRUE,
    execution_time_ms NUMERIC(15, 2),
    ghz_fidelity NUMERIC(5, 4),
    w_state_fidelity NUMERIC(5, 4),
    circuit_data JSONB,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- TABLE: quantum_coherence_snapshots
CREATE TABLE quantum_coherence_snapshots (
    snapshot_id BIGSERIAL PRIMARY KEY,
    block_height BIGINT NOT NULL,
    timestamp BIGINT NOT NULL,
    global_coherence NUMERIC(5, 4) NOT NULL,
    average_coherence NUMERIC(5, 4),
    min_coherence NUMERIC(5, 4),
    max_coherence NUMERIC(5, 4),
    coherence_histogram JSONB,
    phase_drift_radians NUMERIC(200, 150),
    phase_correction_applied BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- TABLE: quantum_density_matrix_global
CREATE TABLE quantum_density_matrix_global (
    state_id BIGSERIAL PRIMARY KEY,
    block_height BIGINT NOT NULL,
    timestamp BIGINT NOT NULL,
    density_matrix_json JSONB NOT NULL,
    density_matrix_hash VARCHAR(255) UNIQUE NOT NULL,
    trace_value NUMERIC(5, 4),
    purity NUMERIC(5, 4),
    von_neumann_entropy NUMERIC(5, 4),
    eigenvalues JSONB,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- TABLE: quantum_error_correction
CREATE TABLE quantum_error_correction (
    correction_id BIGSERIAL PRIMARY KEY,
    pq_id BIGINT NOT NULL,
    block_height BIGINT NOT NULL,
    timestamp BIGINT NOT NULL,
    error_detected BOOLEAN NOT NULL,
    error_type VARCHAR(50),
    error_location_code VARCHAR(255),
    correction_applied BOOLEAN DEFAULT FALSE,
    correction_method VARCHAR(50),
    correction_strength NUMERIC(5, 4),
    correction_verified BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- TABLE: quantum_lattice_metadata
CREATE TABLE quantum_lattice_metadata (
    metadata_id BIGSERIAL PRIMARY KEY,
    tessellation_depth INT NOT NULL,
    total_triangles BIGINT NOT NULL,
    total_pseudoqubits BIGINT NOT NULL,
    precision_bits INT DEFAULT 150,
    hyperbolicity_constant NUMERIC(5, 4) DEFAULT -1.0,
    poincare_radius NUMERIC(5, 4) DEFAULT 1.0,
    status VARCHAR(50) DEFAULT 'constructing',
    construction_started_at TIMESTAMP WITH TIME ZONE,
    construction_completed_at TIMESTAMP WITH TIME ZONE,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- TABLE: quantum_measurements
CREATE TABLE quantum_measurements (
    measurement_id BIGSERIAL PRIMARY KEY,
    pq_id BIGINT NOT NULL,
    block_height BIGINT NOT NULL,
    timestamp BIGINT NOT NULL,
    outcome INT CHECK (outcome IN (0, 1)),
    basis VARCHAR(10),
    expectation_value NUMERIC(5, 4),
    variance NUMERIC(5, 4),
    post_measurement_state JSONB,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- TABLE: quantum_phase_evolution
CREATE TABLE quantum_phase_evolution (
    phase_id BIGSERIAL PRIMARY KEY,
    pq_id BIGINT NOT NULL,
    block_height BIGINT NOT NULL,
    timestamp BIGINT NOT NULL,
    phase_theta NUMERIC(200, 150) NOT NULL,
    phase_derivative NUMERIC(200, 150),
    coherence_measure NUMERIC(5, 4),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- TABLE: quantum_shadow_tomography
CREATE TABLE quantum_shadow_tomography (
    shadow_id BIGSERIAL PRIMARY KEY,
    block_height BIGINT NOT NULL,
    timestamp BIGINT NOT NULL,
    shadow_snapshots JSONB NOT NULL,
    shadow_measurement_bases JSONB,
    reconstruction_fidelity NUMERIC(5, 4),
    num_snapshots INT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- TABLE: quantum_supremacy_proofs
CREATE TABLE quantum_supremacy_proofs (
    proof_id BIGSERIAL PRIMARY KEY,
    block_height BIGINT NOT NULL,
    timestamp BIGINT NOT NULL,
    circuit_depth INT,
    success_probability NUMERIC(5, 4),
    classical_simulation_complexity VARCHAR(255),
    quantum_result_hash VARCHAR(255),
    classical_hardness_assumption TEXT,
    verified BOOLEAN DEFAULT FALSE,
    verification_method VARCHAR(100),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- TABLE: state_root_updates
CREATE TABLE state_root_updates (
    update_id BIGSERIAL PRIMARY KEY,
    block_height BIGINT NOT NULL,
    new_state_root VARCHAR(255) NOT NULL,
    previous_state_root VARCHAR(255),
    timestamp BIGINT NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    UNIQUE(block_height)
);

-- TABLE: system_metrics
CREATE TABLE system_metrics (
    metric_id BIGSERIAL PRIMARY KEY,
    timestamp BIGINT NOT NULL,
    db_size_mb NUMERIC(15, 2),
    active_connections INT,
    active_peers INT,
    total_peers INT,
    avg_latency_ms NUMERIC(10, 2),
    blocks_per_minute NUMERIC(10, 2),
    transactions_per_second NUMERIC(10, 2),
    avg_coherence NUMERIC(5, 4),
    oracle_sync_quality NUMERIC(5, 4),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- TABLE: transaction_inputs
CREATE TABLE transaction_inputs (
    input_id BIGSERIAL PRIMARY KEY,
    tx_id BIGINT NOT NULL,
    previous_tx_hash VARCHAR(255),
    previous_output_index INT,
    script_sig TEXT,
    script_pubkey TEXT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- TABLE: transaction_outputs
CREATE TABLE transaction_outputs (
    output_id BIGSERIAL PRIMARY KEY,
    tx_id BIGINT NOT NULL,
    output_index INT NOT NULL,
    address VARCHAR(255) NOT NULL,
    amount NUMERIC(30, 0) NOT NULL,
    script_pubkey TEXT,
    spent BOOLEAN DEFAULT FALSE,
    spent_in_tx_id BIGINT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    UNIQUE(tx_id, output_index)
);

-- TABLE: transaction_receipts
CREATE TABLE transaction_receipts (
    receipt_id BIGSERIAL PRIMARY KEY,
    tx_id BIGINT NOT NULL,
    height BIGINT,
    status INT,
    logs_json JSONB,
    bloom_filter TEXT,
    quantum_proof TEXT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- TABLE: transactions
CREATE TABLE transactions (
    id BIGSERIAL PRIMARY KEY,
    tx_hash VARCHAR(255) UNIQUE NOT NULL,
    from_address VARCHAR(255) NOT NULL,
    to_address VARCHAR(255) NOT NULL,
    amount NUMERIC(30, 0) NOT NULL,
    nonce BIGINT,
    height BIGINT,
    block_hash VARCHAR(255),
    transaction_index INT,
    tx_type VARCHAR(50) DEFAULT 'transfer',
    status VARCHAR(50) DEFAULT 'pending',
    pq_signature TEXT,
    pq_signer_key_fp VARCHAR(255),
    pq_verified BOOLEAN DEFAULT FALSE,
    pq_verified_at TIMESTAMP WITH TIME ZONE,
    quantum_state_hash VARCHAR(255),
    commitment_hash VARCHAR(255),
    entropy_score NUMERIC(5, 4),
    input_data TEXT,
    metadata JSONB,
    error_message TEXT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    finalized_at TIMESTAMP WITH TIME ZONE
);

-- TABLE: validator_stakes
CREATE TABLE validator_stakes (
    stake_id BIGSERIAL PRIMARY KEY,
    validator_id BIGINT NOT NULL,
    amount NUMERIC(30, 0) NOT NULL,
    staker_address VARCHAR(255),
    active BOOLEAN DEFAULT TRUE,
    delegated BOOLEAN DEFAULT FALSE,
    stake_at_height BIGINT,
    unstake_at_height BIGINT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- TABLE: validators
CREATE TABLE validators (
    validator_id BIGSERIAL PRIMARY KEY,
    public_key VARCHAR(255) UNIQUE NOT NULL,
    peer_id VARCHAR(255),
    stake NUMERIC(30, 0) DEFAULT 0,
    commission_rate NUMERIC(5, 4),
    slashing_rate NUMERIC(5, 4) DEFAULT 0.0,
    blocks_proposed INT DEFAULT 0,
    blocks_validated INT DEFAULT 0,
    blocks_missed INT DEFAULT 0,
    status VARCHAR(50) DEFAULT 'active',
    is_slashed BOOLEAN DEFAULT FALSE,
    slashed_at TIMESTAMP WITH TIME ZONE,
    oracle_participation_score NUMERIC(5, 4) DEFAULT 0.0,
    w_state_sync_quality NUMERIC(5, 4) DEFAULT 0.0,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- TABLE: w_state_snapshots
CREATE TABLE w_state_snapshots (
    snapshot_id BIGSERIAL PRIMARY KEY,
    block_height BIGINT NOT NULL,
    timestamp BIGINT NOT NULL,
    w_state_serialized TEXT NOT NULL,
    w_state_hash VARCHAR(255) UNIQUE NOT NULL,
    entanglement_measure NUMERIC(5, 4),
    coherence_time_us NUMERIC(15, 2),
    fidelity_estimate NUMERIC(5, 4),
    pq_addresses TEXT[],
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    UNIQUE(block_height)
);

-- TABLE: w_state_validator_states
CREATE TABLE w_state_validator_states (
    state_id BIGSERIAL PRIMARY KEY,
    block_height BIGINT NOT NULL,
    validator_public_key VARCHAR(255) NOT NULL,
    timestamp BIGINT NOT NULL,
    w_state_serialized TEXT,
    w_state_hash VARCHAR(255),
    coherence_with_oracle NUMERIC(5, 4),
    phase_alignment_radians NUMERIC(200, 150),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- TABLE: wallet_addresses
CREATE TABLE wallet_addresses (
    address VARCHAR(255) PRIMARY KEY,
    wallet_fingerprint VARCHAR(64) NOT NULL,
    derivation_path VARCHAR(100),
    account_index INT,
    change_index INT,
    address_index INT,
    public_key VARCHAR(255) NOT NULL,
    address_type VARCHAR(50) DEFAULT 'receiving',
    is_watching_only BOOLEAN DEFAULT FALSE,
    is_cold_storage BOOLEAN DEFAULT FALSE,
    balance NUMERIC(30, 0) DEFAULT 0,
    balance_updated_at TIMESTAMP WITH TIME ZONE,
    balance_at_height BIGINT,
    first_used_at TIMESTAMP WITH TIME ZONE,
    last_used_at TIMESTAMP WITH TIME ZONE,
    transaction_count INT DEFAULT 0,
    label VARCHAR(255),
    notes TEXT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    UNIQUE(wallet_fingerprint, derivation_path)
);

-- TABLE: wallet_key_rotation_history
CREATE TABLE wallet_key_rotation_history (
    rotation_id BIGSERIAL PRIMARY KEY,
    wallet_fingerprint VARCHAR(64) NOT NULL,
    old_key_id VARCHAR(255),
    new_key_id VARCHAR(255),
    rotation_reason TEXT,
    rotation_timestamp TIMESTAMP WITH TIME ZONE,
    ratchet_material TEXT,
    next_rotation_material TEXT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- TABLE: wallet_seed_backup_status
CREATE TABLE wallet_seed_backup_status (
    backup_id BIGSERIAL PRIMARY KEY,
    wallet_fingerprint VARCHAR(64) NOT NULL UNIQUE,
    seed_phrase_backed_up BOOLEAN DEFAULT FALSE,
    backup_confirmed_at TIMESTAMP WITH TIME ZONE,
    seed_hint VARCHAR(50),
    seed_hash VARCHAR(255),
    backup_required BOOLEAN DEFAULT TRUE,
    days_since_creation_without_backup INT,
    email_notifications_sent INT DEFAULT 0,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);



-- V5 SEQUENTIAL ROUTING
CREATE TABLE pq_sequential (
    pq_id BIGINT PRIMARY KEY,
    next_pq_id BIGINT,
    prev_pq_id BIGINT,
    sequence_order BIGINT UNIQUE NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE INDEX idx_pq_next ON pq_sequential(next_pq_id);
CREATE INDEX idx_pq_prev ON pq_sequential(prev_pq_id);
CREATE INDEX idx_sequence_order ON pq_sequential(sequence_order);

-- ════════════════════════════════════════════════════════
-- SECURITY: CLIENT-SIDE KEY VAULT (no external KMS required)
-- ════════════════════════════════════════════════════════
--
--  Envelope encryption model (3 layers, all client-side):
--    passphrase + device_pepper → Argon2id → KEK  (never stored)
--    KEK → AES-256-GCM → wrapped_dek_b64          (stored here)
--    DEK → AES-256-GCM → BIP39 entropy ciphertext (stored here)
--
--  Full DB scrape exposes: salts, nonces, ciphertexts, KDF params.
--  None of these are decryptable without the passphrase.
--  Argon2id m=65536 makes GPU brute-force economically infeasible.
--  No external service, no API keys, no cloud dependency.

CREATE TABLE IF NOT EXISTS wallet_encrypted_seeds (
    seed_id              BIGSERIAL PRIMARY KEY,
    wallet_fingerprint   VARCHAR(64)   NOT NULL UNIQUE,
    -- KDF params (stored so we can re-derive KEK on any device)
    kdf_type             VARCHAR(16)   NOT NULL DEFAULT 'argon2id',
    kdf_salt_b64         TEXT          NOT NULL,  -- 32-byte random salt, base64
    argon2_m_cost        INTEGER       DEFAULT 65536,
    argon2_t_cost        INTEGER       DEFAULT 3,
    argon2_p_cost        INTEGER       DEFAULT 4,
    scrypt_n             INTEGER,                  -- populated only if kdf_type=scrypt
    scrypt_r             INTEGER,
    scrypt_p             INTEGER,
    -- Wrapped DEK: AES-256-GCM(KEK, random_dek)
    dek_nonce_b64        TEXT          NOT NULL,   -- 12-byte GCM nonce
    wrapped_dek_b64      TEXT          NOT NULL,   -- ciphertext+tag (useless without KEK)
    -- Seed ciphertext: AES-256-GCM(DEK, bip39_entropy)
    seed_nonce_b64       TEXT          NOT NULL,   -- 12-byte GCM nonce
    seed_ciphertext_b64  TEXT          NOT NULL,   -- encrypted BIP39 entropy
    -- Safe-to-expose public identity
    bip32_xpub           TEXT,
    derivation_scheme    VARCHAR(32)   DEFAULT 'BIP44',
    coin_type            INTEGER       DEFAULT 60,
    mnemonic_word_count  SMALLINT      DEFAULT 24,
    is_passphrase_protected BOOLEAN    DEFAULT FALSE,
    device_bound         BOOLEAN       DEFAULT FALSE,  -- was device pepper used?
    -- Usage tracking
    last_decrypted_at    TIMESTAMP WITH TIME ZONE,
    decrypt_count        INTEGER       DEFAULT 0,
    created_at           TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at           TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- key_audit_log: append-only compliance log of every decrypt/sign event.
CREATE TABLE IF NOT EXISTS key_audit_log (
    audit_id             BIGSERIAL PRIMARY KEY,
    event_type           VARCHAR(64)  NOT NULL,
    wallet_fingerprint   VARCHAR(64),
    address              VARCHAR(255),
    kms_key_id           BIGINT,
    actor_peer_id        VARCHAR(255),
    tx_hash              VARCHAR(255),
    block_height         BIGINT,
    success              BOOLEAN      NOT NULL DEFAULT TRUE,
    failure_reason       TEXT,
    duration_ms          NUMERIC(10,2),
    created_at           TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_key_audit_wallet  ON key_audit_log (wallet_fingerprint, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_key_audit_event   ON key_audit_log (event_type, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_key_audit_fail    ON key_audit_log (success) WHERE success = FALSE;

-- nonce_ledger: replay-attack prevention. Every signing nonce recorded here.
CREATE TABLE IF NOT EXISTS nonce_ledger (
    nonce_id             BIGSERIAL PRIMARY KEY,
    nonce_hex            VARCHAR(128) NOT NULL UNIQUE,
    address              VARCHAR(255) NOT NULL,
    used_in_type         VARCHAR(50)  NOT NULL,
    used_in_hash         VARCHAR(255),
    created_at           TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    expires_at           TIMESTAMP WITH TIME ZONE
);
CREATE INDEX IF NOT EXISTS idx_nonce_address ON nonce_ledger (address, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_nonce_expiry  ON nonce_ledger (expires_at) WHERE expires_at IS NOT NULL;
"""

# ─────────────────────────────────────────────────────────────────────────────
# STEP 8: OPTIMIZED DATABASE BUILDER CLASS (Colab-tuned)
# ─────────────────────────────────────────────────────────────────────────────

# ─────────────────────────────────────────────────────────────────────────────
# STEP 8: DUAL-MODE DATABASE BUILDER CLASS (NeonDB / SQLite)
# ─────────────────────────────────────────────────────────────────────────────

class QuantumTemporalCoherenceLedgerServer:
    """
    Dual-mode QTCL database builder.
      postgres mode → NeonDB via DATABASE_URL (server / Colab)
      sqlite mode   → qtcl.db local file (client / Termux)

    Same schema, same logic, same call surface.
    """

    TRIANGLE_BATCH_SIZE = 2000
    QUBIT_BATCH_SIZE = 10000
    PROGRESS_INTERVAL_TRI = 500
    PROGRESS_INTERVAL_QUB = 5000

    def __init__(self, db_url: str = _DB_URL, db_mode: str = _DB_MODE, tessellation_depth: int = 5):
        self.db_url = db_url
        self.db_mode = db_mode
        self.tessellation_depth = tessellation_depth
        self.conn = None
        self.cursor = None
        self._start_time = None

    # ── internal helpers ──────────────────────────────────────────────────────

    def _exec(self, sql: str, params=None):
        """Execute a statement, adapting %s → ? for SQLite."""
        if self.db_mode == "sqlite":
            sql = sql.replace("%s", "?")
        if params:
            self.cursor.execute(sql, params)
        else:
            self.cursor.execute(sql)

    def _commit(self):
        self.conn.commit()

    def _execute_values_compat(self, sql_template: str, rows: list, page_size: int = 200):
        """
        Batch insert: uses psycopg2.extras.execute_values for postgres,
        falls back to executemany with ? placeholders for sqlite.
        """
        if self.db_mode == "postgres":
            execute_values(self.cursor, sql_template, rows, page_size=page_size)
        else:
            # Convert VALUES %s template to INSERT ... VALUES (?,?,...) for sqlite
            import re
            # Count placeholders from first row
            n_cols = len(rows[0]) if rows else 0
            placeholders = "(" + ",".join(["?"] * n_cols) + ")"
            # Replace everything after VALUES with placeholder
            base = re.split(r'\bVALUES\b', sql_template, flags=re.IGNORECASE)[0]
            sqlite_sql = base.strip() + " VALUES " + placeholders
            self.cursor.executemany(sqlite_sql, rows)

    # ── connection ────────────────────────────────────────────────────────────

    def connect(self):
        if self.db_mode == "postgres":
            logger.info(f"{CLR.QUANTUM}[DB] Connecting to NeonDB (PostgreSQL)...{CLR.E}")
            self.conn = psycopg2.connect(self.db_url)
            self.cursor = self.conn.cursor()
            self.cursor.execute("SET statement_timeout = '600s';")
            self.cursor.execute("SET application_name = 'qtcl_v6';")
            self.cursor.execute("SET work_mem = '128MB';")
            self.cursor.execute("SET maintenance_work_mem = '256MB';")
            self.cursor.execute("SET synchronous_commit = off;")
            logger.info(f"{CLR.OK}[DB] NeonDB connected{CLR.E}")
        else:
            logger.info(f"{CLR.QUANTUM}[DB] Opening SQLite: {self.db_url}{CLR.E}")
            self.conn = sqlite3.connect(self.db_url, isolation_level=None)
            self.conn.execute("PRAGMA journal_mode=WAL;")
            self.conn.execute("PRAGMA synchronous=NORMAL;")
            self.conn.execute("PRAGMA cache_size=-131072;")  # 128MB
            self.conn.execute("PRAGMA temp_store=MEMORY;")
            self.cursor = self.conn.cursor()
            # SQLite manual transaction for bulk ops
            self.conn.execute("BEGIN;")
            logger.info(f"{CLR.OK}[DB] SQLite opened in WAL mode{CLR.E}")

    def drop_all_tables(self):
        logger.info(f"{CLR.ERROR}[DROP] Dropping ALL existing tables...{CLR.E}")
        try:
            if self.db_mode == "postgres":
                self.cursor.execute("SELECT tablename FROM pg_tables WHERE schemaname = 'public';")
                tables = [r[0] for r in self.cursor.fetchall()]
                for tname in tables:
                    self.cursor.execute(f"DROP TABLE IF EXISTS {tname} CASCADE;")
                    self._commit()
            else:
                # SQLite: skip internal tables like sqlite_sequence
                self.cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%';")
                tables = [r[0] for r in self.cursor.fetchall()]
                for tname in tables:
                    self.cursor.execute(f"DROP TABLE IF EXISTS {tname};")
            self._commit()
            logger.info(f"{CLR.OK}[DROP] {len(tables)} tables dropped{CLR.E}")
        except Exception as e:
            logger.error(f"{CLR.ERROR}[DROP] Failed: {e}{CLR.E}")
            self.conn.rollback()
            raise

    def create_schema(self):
        logger.info(f"{CLR.QUANTUM}[SCHEMA] Creating schema ({self.db_mode})...{CLR.E}")
        try:
            schema = COMPLETE_SCHEMA
            if self.db_mode == "sqlite":
                # SQLite compatibility: strip PG-only DDL
                import re
                schema = re.sub(r'CREATE EXTENSION[^;]+;', '', schema)
                schema = re.sub(r'BIGSERIAL', 'INTEGER', schema)
                schema = re.sub(r'BIGINT\b', 'INTEGER', schema)  # SQLite uses INTEGER for big ints
                schema = re.sub(r'BOOLEAN', 'INTEGER', schema)
                schema = re.sub(r'TIMESTAMP WITH TIME ZONE', 'TEXT', schema)
                schema = re.sub(r'NUMERIC\([^)]+\)', 'TEXT', schema)  # High precision numbers stored as TEXT
                schema = re.sub(r'JSONB', 'TEXT', schema)
                schema = re.sub(r'BYTEA', 'BLOB', schema)
                schema = re.sub(r'\bINET\b', 'TEXT', schema)  # IP addresses as TEXT
                schema = re.sub(r'TEXT\[\]', 'TEXT', schema)
                schema = re.sub(r'VARCHAR\(\d+\)', 'TEXT', schema)
                schema = re.sub(r'VARCHAR\b', 'TEXT', schema)  # Any VARCHAR -> TEXT
                schema = re.sub(r'SMALLINT', 'INTEGER', schema)
                schema = re.sub(r'INT\b', 'INTEGER', schema)  # INT -> INTEGER
                schema = re.sub(r"DEFAULT '.*?'::JSONB", "DEFAULT '{}'", schema)
                schema = re.sub(r"::\w+", "", schema)  # Remove ALL PostgreSQL type casts (::TEXT, ::jsonb, etc.)
                schema = re.sub(r'REFERENCES \w+\(\w+\)', '', schema)  # SQLite FK optional
                schema = re.sub(r"DEFAULT NOW\(\)", "DEFAULT (strftime('%s', 'now'))", schema)  # NOW() not valid in SQLite
            
            logger.info(f"[SCHEMA] Raw schema length: {len(schema)}")
            
            # Execute statement by statement
            skipped = 0
            tables_created = 0
            stmts = schema.split(";")
            logger.info(f"[SCHEMA] Total statements: {len(stmts)}")
            
            for i, stmt in enumerate(stmts):
                # Remove leading comment-only lines but keep the actual SQL
                lines = stmt.strip().split('\n')
                sql_lines = [l for l in lines if not l.strip().startswith('--')]
                s = '\n'.join(sql_lines).strip()
                
                if s:  # Only execute if there's actual SQL content
                    try:
                        self.cursor.execute(s)
                        if 'CREATE TABLE' in s.upper():
                            tables_created += 1
                            logger.info(f"[SCHEMA] ✓ Created: {s[:60]}...")
                    except Exception as e:
                        # Log CREATE TABLE failures as errors (not just warnings)
                        if 'CREATE TABLE' in s.upper():
                            logger.error(f"[SCHEMA] CREATE TABLE failed: {s[:60]}... → {e}")
                            raise
                        else:
                            logger.info(f"[SCHEMA] Skipped: {s[:60]}... → {e}")
                            skipped += 1
            
            self._commit()
            logger.info(f"[SCHEMA] Created {tables_created} tables, {skipped} skipped")
            if skipped > 0:
                logger.warning(f"[SCHEMA] {skipped} statements skipped")
            
            # Verify tables exist
            if self.db_mode == "sqlite":
                self.cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
                tables = [r[0] for r in self.cursor.fetchall()]
                logger.info(f"[SCHEMA] Tables in DB: {len(tables)}")
                if 'hyperbolic_triangles' in tables:
                    logger.info(f"[SCHEMA] ✓ hyperbolic_triangles table verified")
                else:
                    logger.error(f"[SCHEMA] ❌ hyperbolic_triangles NOT in DB! Tables: {tables[:10]}...")
            
            logger.info(f"{CLR.OK}[SCHEMA] Schema created{CLR.E}")
        except Exception as e:
            logger.error(f"{CLR.ERROR}[SCHEMA] Failed: {e}{CLR.E}")
            self.conn.rollback()
            raise

    def _migrate_oracle_registry_onchain(self):
        """Idempotent migration — adds on-chain identity columns if missing."""
        if self.db_mode == "sqlite":
            migrations = [
                "ALTER TABLE oracle_registry ADD COLUMN wallet_address  TEXT NOT NULL DEFAULT ''",
                "ALTER TABLE oracle_registry ADD COLUMN oracle_pub_key  TEXT NOT NULL DEFAULT ''",
                "ALTER TABLE oracle_registry ADD COLUMN cert_sig        TEXT NOT NULL DEFAULT ''",
                "ALTER TABLE oracle_registry ADD COLUMN cert_auth_tag   TEXT NOT NULL DEFAULT ''",
                "ALTER TABLE oracle_registry ADD COLUMN mode            TEXT NOT NULL DEFAULT 'full'",
                "ALTER TABLE oracle_registry ADD COLUMN ip_hint         TEXT NOT NULL DEFAULT ''",
                "ALTER TABLE oracle_registry ADD COLUMN reg_tx_hash     TEXT NOT NULL DEFAULT ''",
                "ALTER TABLE oracle_registry ADD COLUMN registered_at   INTEGER NOT NULL DEFAULT 0",
            ]
        else:
            migrations = [
                "ALTER TABLE oracle_registry ADD COLUMN IF NOT EXISTS wallet_address  VARCHAR(128) NOT NULL DEFAULT ''",
                "ALTER TABLE oracle_registry ADD COLUMN IF NOT EXISTS oracle_pub_key  TEXT         NOT NULL DEFAULT ''",
                "ALTER TABLE oracle_registry ADD COLUMN IF NOT EXISTS cert_sig        VARCHAR(128) NOT NULL DEFAULT ''",
                "ALTER TABLE oracle_registry ADD COLUMN IF NOT EXISTS cert_auth_tag   VARCHAR(128) NOT NULL DEFAULT ''",
                "ALTER TABLE oracle_registry ADD COLUMN IF NOT EXISTS mode            VARCHAR(32)  NOT NULL DEFAULT 'full'",
                "ALTER TABLE oracle_registry ADD COLUMN IF NOT EXISTS ip_hint         VARCHAR(256) NOT NULL DEFAULT ''",
                "ALTER TABLE oracle_registry ADD COLUMN IF NOT EXISTS reg_tx_hash     VARCHAR(64)  NOT NULL DEFAULT ''",
                "ALTER TABLE oracle_registry ADD COLUMN IF NOT EXISTS registered_at   BIGINT       NOT NULL DEFAULT 0",
                "CREATE INDEX IF NOT EXISTS idx_oracle_registry_wallet        ON oracle_registry (wallet_address)",
                "CREATE INDEX IF NOT EXISTS idx_oracle_registry_reg_tx        ON oracle_registry (reg_tx_hash) WHERE reg_tx_hash != ''",
                "CREATE INDEX IF NOT EXISTS idx_oracle_registry_registered_at ON oracle_registry (registered_at DESC)",
            ]
        ok = 0
        for ddl in migrations:
            try:
                self.cursor.execute(ddl)
                self._commit()
                ok += 1
            except Exception as e:
                try: self.conn.rollback()
                except: pass
                logger.debug(f"[MIGRATE] Skipped ({ddl[:50]}): {e}")
        logger.info(f"{CLR.OK}[MIGRATE] oracle_registry migration: {ok}/{len(migrations)} OK{CLR.E}")

    def _insert_triangles_batched(self, triangles: Dict[int, 'HyperbolicTriangle']):
        logger.info(f"{CLR.C}[TRI] Inserting triangles...{CLR.E}")
        triangle_list = [t for t in triangles.values() if t.depth == self.tessellation_depth]
        total = len(triangle_list)
        pbar = tqdm(total=total, desc="Triangles", leave=True)
        for batch_start in range(0, total, self.TRIANGLE_BATCH_SIZE):
            batch = triangle_list[batch_start:batch_start + self.TRIANGLE_BATCH_SIZE]
            rows = [(
                tri.triangle_id, tri.depth, None,
                str(tri.v0.x), str(tri.v0.y), tri.v0.name,
                str(tri.v1.x), str(tri.v1.y), tri.v1.name,
                str(tri.v2.x), str(tri.v2.y), tri.v2.name
            ) for tri in batch]
            self._execute_values_compat(
                """
                INSERT INTO hyperbolic_triangles (
                    triangle_id, depth, parent_id,
                    v0_x, v0_y, v0_name,
                    v1_x, v1_y, v1_name,
                    v2_x, v2_y, v2_name
                ) VALUES %s
                """,
                rows, page_size=100
            )
            if batch_start % (self.TRIANGLE_BATCH_SIZE * 5) == 0 and batch_start > 0:
                self._commit()
            pbar.update(len(batch))
        pbar.close()
        self._commit()
        logger.info(f"{CLR.OK}[TRI] {total} triangles inserted{CLR.E}")

    def _insert_pseudoqubits_batched(self, qubits: Dict[int, 'Pseudoqubit'], triangle_ids: set):
        qubit_list = [q for q in qubits.values() if q.triangle_id in triangle_ids]
        total = len(qubit_list)
        logger.info(f"{CLR.C}[QUB] Inserting {total} pseudoqubits...{CLR.E}")
        pbar = tqdm(total=total, desc="Pseudoqubits", leave=True)
        for batch_start in range(0, total, self.QUBIT_BATCH_SIZE):
            batch = qubit_list[batch_start:batch_start + self.QUBIT_BATCH_SIZE]
            rows = [q.to_db_row() for q in batch]
            self._execute_values_compat(
                """
                INSERT INTO pseudoqubits (
                    pq_id, triangle_id, x, y,
                    placement_type, phase_theta, coherence_measure
                ) VALUES %s
                """,
                rows, page_size=200
            )
            if batch_start % (self.QUBIT_BATCH_SIZE * 3) == 0 and batch_start > 0:
                self._commit()
            pbar.update(len(batch))
        pbar.close()
        self._commit()
        logger.info(f"{CLR.OK}[QUB] {total} pseudoqubits inserted{CLR.E}")

    def _build_tessellation_inline(self) -> Tuple[Dict[int, 'HyperbolicTriangle'], Dict[int, 'Pseudoqubit']]:
        """Inline tessellation builder — memory efficient"""
        triangles: Dict[int, HyperbolicTriangle] = {}
        qubits: Dict[int, Pseudoqubit] = {}
        triangle_id_counter = [0]
        qubit_id_counter = [0]
        
        def build_octagon_decomposition() -> List[HyperbolicTriangle]:
            logger.info(f"{CLR.QUANTUM}[OCTAGON] Constructing 8 fundamental octagons{CLR.E}")
            triangles_list = []
            octagon_radius = mpf("0.4")
            octagon_vertices = []
            for i in range(8):
                angle = mpf(2) * pi * mpf(i) / mpf(8)
                x = octagon_radius * cos(angle)
                y = octagon_radius * sin(angle)
                vertex = HyperbolicPoint(x, y, name=f"oct_v{i}")
                octagon_vertices.append(vertex)
            center = HyperbolicPoint(mpf(0), mpf(0), name="oct_center")
            for i in range(8):
                v0 = center
                v1 = octagon_vertices[i]
                v2 = octagon_vertices[(i + 1) % 8]
                triangle = HyperbolicTriangle(
                    triangle_id=triangle_id_counter[0],
                    v0=v0, v1=v1, v2=v2,
                    depth=0,
                    parent_id=None
                )
                triangle_id_counter[0] += 1
                triangles_list.append(triangle)
            logger.info(f"{CLR.OK}[OCTAGON] Created {len(triangles_list)} fundamental triangles{CLR.E}")
            return triangles_list
        
        def subdivide_triangle(parent: HyperbolicTriangle) -> List[HyperbolicTriangle]:
            m01 = poincare_midpoint(parent.v0, parent.v1)
            m12 = poincare_midpoint(parent.v1, parent.v2)
            m20 = poincare_midpoint(parent.v2, parent.v0)
            children = [
                HyperbolicTriangle(triangle_id=triangle_id_counter[0], v0=parent.v0, v1=m01, v2=m20, depth=parent.depth + 1, parent_id=parent.triangle_id),
                HyperbolicTriangle(triangle_id=triangle_id_counter[0] + 1, v0=parent.v1, v1=m12, v2=m01, depth=parent.depth + 1, parent_id=parent.triangle_id),
                HyperbolicTriangle(triangle_id=triangle_id_counter[0] + 2, v0=parent.v2, v1=m20, v2=m12, depth=parent.depth + 1, parent_id=parent.triangle_id),
                HyperbolicTriangle(triangle_id=triangle_id_counter[0] + 3, v0=m01, v1=m12, v2=m20, depth=parent.depth + 1, parent_id=parent.triangle_id)
            ]
            triangle_id_counter[0] += 4
            return children
        
        def build_recursive_tessellation():
            logger.info(f"{CLR.QUANTUM}[RECURSIVE] Building tessellation depth {self.tessellation_depth}{CLR.E}")
            current_level = build_octagon_decomposition()
            for tri in current_level:
                triangles[tri.triangle_id] = tri
            total_levels = self.tessellation_depth
            for level in range(1, total_levels + 1):
                next_level = []
                for parent_tri in current_level:
                    children = subdivide_triangle(parent_tri)
                    for child in children:
                        triangles[child.triangle_id] = child
                        next_level.append(child)
                current_level = next_level
                pct = (level / total_levels) * 100
                bar_len = 40
                filled = int(bar_len * level / total_levels)
                bar = "█" * filled + "░" * (bar_len - filled)
                logger.info(f"{CLR.C}[Tessellation] [{bar}] {pct:5.1f}% | Level {level}/{total_levels} | {len(triangles):,} triangles{CLR.E}")
            logger.info(f"{CLR.OK}[RECURSIVE] ✅ Complete: {len(triangles):,} triangles{CLR.E}")
        
        def place_pseudoqubits():
            logger.info(f"{CLR.QUANTUM}[QUBITS] Placing pseudoqubits...{CLR.E}")
            qubit_id = 0
            total_triangles = len(triangles)
            log_interval = max(1, total_triangles // 20)
            for idx, (tri_id, triangle) in enumerate(triangles.items()):
                for i, vertex in enumerate([triangle.v0, triangle.v1, triangle.v2]):
                    qubit = Pseudoqubit(pseudoqubit_id=qubit_id, triangle_id=tri_id, x=vertex.x, y=vertex.y, placement_type="vertex")
                    qubits[qubit_id] = qubit
                    qubit_id += 1
                inc = hyperbolic_incenter(triangle)
                qubit = Pseudoqubit(pseudoqubit_id=qubit_id, triangle_id=tri_id, x=inc.x, y=inc.y, placement_type="incenter")
                qubits[qubit_id] = qubit
                qubit_id += 1
                circ = hyperbolic_circumcenter(triangle)
                qubit = Pseudoqubit(pseudoqubit_id=qubit_id, triangle_id=tri_id, x=circ.x, y=circ.y, placement_type="circumcenter")
                qubits[qubit_id] = qubit
                qubit_id += 1
                orth = hyperbolic_orthocenter(triangle)
                qubit = Pseudoqubit(pseudoqubit_id=qubit_id, triangle_id=tri_id, x=orth.x, y=orth.y, placement_type="orthocenter")
                qubits[qubit_id] = qubit
                qubit_id += 1
                grid_points = generate_geodesic_grid(triangle)
                for gp in grid_points[:7]:
                    qubit = Pseudoqubit(pseudoqubit_id=qubit_id, triangle_id=tri_id, x=gp.x, y=gp.y, placement_type="geodesic")
                    qubits[qubit_id] = qubit
                    qubit_id += 1
                if (idx + 1) % log_interval == 0 or idx == total_triangles - 1:
                    pct = ((idx + 1) / total_triangles) * 100
                    bar_len = 40
                    filled = int(bar_len * (idx + 1) / total_triangles)
                    bar = "█" * filled + "░" * (bar_len - filled)
                    logger.info(f"{CLR.C}[Geometry IDs] [{bar}] {pct:5.1f}% | {idx+1:,}/{total_triangles:,} triangles | {qubit_id:,} IDs placed{CLR.E}")
                if tri_id % 1000 == 0:
                    gc.collect()
            logger.info(f"{CLR.OK}[QUBITS] ✅ All {qubit_id:,} pseudoqubits placed{CLR.E}")
        
        # ✅ ACTUALLY CALL THE FUNCTIONS
        build_recursive_tessellation()
        place_pseudoqubits()
        gc.collect()
        return triangles, qubits
    
    def populate_tessellation(self):
        logger.info(f"{CLR.QUANTUM}[POPULATE] Building and inserting tessellation...{CLR.E}")
        self._start_time = time.time()
        try:
            triangles, qubits = self._build_tessellation_inline()
            final_depth_triangle_ids = set(
                t.triangle_id for t in triangles.values()
                if t.depth == self.tessellation_depth
            )
            logger.info(f"{CLR.C}[FILTER] Final-depth triangles: {len(final_depth_triangle_ids)}{CLR.E}")
            self._insert_triangles_batched(triangles)
            self._insert_pseudoqubits_batched(qubits, final_depth_triangle_ids)

            ts_now = datetime.now(timezone.utc).isoformat()
            n_tris = len(final_depth_triangle_ids)
            n_qubs = len([q for q in qubits.values() if q.triangle_id in final_depth_triangle_ids])

            if self.db_mode == "postgres":
                self.cursor.execute("""
                    INSERT INTO quantum_lattice_metadata (
                        tessellation_depth, total_triangles, total_pseudoqubits,
                        precision_bits, hyperbolicity_constant, poincare_radius,
                        status, construction_started_at, construction_completed_at
                    ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
                """, (self.tessellation_depth, n_tris, n_qubs, 150, -1.0, 1.0,
                      'complete', ts_now, ts_now))
                self.cursor.execute("""
                    INSERT INTO database_metadata (schema_version, build_timestamp, tables_created)
                    VALUES (%s,%s,%s)
                """, ('6.0.0-neon', ts_now, 62))
            else:
                self.cursor.execute(
                    "INSERT INTO quantum_lattice_metadata "
                    "(tessellation_depth, total_triangles, total_pseudoqubits, "
                    "precision_bits, hyperbolicity_constant, poincare_radius, "
                    "status, construction_started_at, construction_completed_at) "
                    "VALUES (?,?,?,?,?,?,?,?,?)",
                    (self.tessellation_depth, n_tris, n_qubs, 150, -1.0, 1.0,
                     'complete', ts_now, ts_now)
                )
                self.cursor.execute(
                    "INSERT INTO database_metadata (schema_version, build_timestamp, tables_created) "
                    "VALUES (?,?,?)", ('6.0.0-sqlite', ts_now, 62)
                )

            self._commit()
            elapsed = time.time() - self._start_time
            logger.info(f"{CLR.OK}[POPULATE] Complete in {elapsed:.1f}s{CLR.E}")
        except Exception as e:
            logger.error(f"{CLR.ERROR}[POPULATE] Failed: {e}{CLR.E}")
            try: self.conn.rollback()
            except: pass
            raise

    def close(self):
        try:
            if self.cursor:
                self.cursor.close()
            if self.conn:
                self._commit()
                self.conn.close()
            logger.info(f"{CLR.OK}[DB] Connection closed{CLR.E}")
        except Exception as e:
            logger.warning(f"[DB] Close warning: {e}")

    def rebuild_complete(self):
        logger.info(f"{CLR.HEADER}{'='*80}{CLR.E}")
        logger.info(f"{CLR.HEADER}QTCL DATABASE BUILDER V6 — MODE: {self.db_mode.upper()}{CLR.E}")
        logger.info(f"{CLR.HEADER}{'='*80}{CLR.E}")
        total_start = time.time()
        try:
            self.connect()
            self.drop_all_tables()
            self.create_schema()
            self._migrate_oracle_registry_onchain()
            self.populate_tessellation()
            total_elapsed = time.time() - total_start
            logger.info(f"{CLR.HEADER}{'='*80}{CLR.E}")
            logger.info(f"{CLR.OK}✓ BUILD COMPLETE — {total_elapsed/60:.1f} min — {self.db_mode.upper()}{CLR.E}")
            logger.info(f"{CLR.OK}  Tessellation depth: {self.tessellation_depth}{CLR.E}")
            logger.info(f"{CLR.OK}  Security tables: kms_key_references, wallet_encrypted_seeds, key_audit_log, nonce_ledger{CLR.E}")
            logger.info(f"{CLR.HEADER}{'='*80}{CLR.E}")
        except Exception as e:
            logger.error(f"{CLR.ERROR}Build failed: {e}{CLR.E}")
            raise
        finally:
            self.close()

# ─────────────────────────────────────────────────────────────────────────────
# STEP 9: RUN THE BUILD (Colab entry point)
# ─────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    logger.info(f"\n{'='*80}")
    logger.info(f"QTCL DATABASE BUILDER V7 — MODE: {_DB_MODE.upper()}")
    logger.info(f"KEK: {'Argon2id' if ARGON2_AVAILABLE else 'scrypt'} (client-side, no external KMS)")
    logger.info(f"{'='*80}\n")

    builder = QuantumTemporalCoherenceLedgerServer(tessellation_depth=5)
    builder.rebuild_complete()

    logger.info(f"\n✓ QTCL V7 database ready ({_DB_MODE})\n")

    if _DB_MODE == "postgres":
        print("\n💡 Verify:")
        print("  SELECT COUNT(*) FROM hyperbolic_triangles;")
        print("  SELECT COUNT(*) FROM wallet_encrypted_seeds;")
        print("  SELECT COUNT(*) FROM key_audit_log;")
        print("  SELECT COUNT(*) FROM nonce_ledger;")
    else:
        print(f"\n💡 SQLite written to: {_DB_URL}")
        print("   Client nodes: run without DATABASE_URL env var.")
        print("   Wallet seeds: use new_wallet_envelope(passphrase) → INSERT into wallet_encrypted_seeds.")
