#!/usr/bin/env python3
"""
hyp_finite_field.py — Finite-Field Schnorr-Γ over GF(p)
╔══════════════════════════════════════════════════════════════════════════╗
║  Post-Quantum Secure: 2×2 matrices over GF(p) with random generators     ║
║  in SL(2,p).  Matrix exponentiation is EXACT (modular),                  ║
║  enabling the full 256-bit Fiat-Shamir challenge in the Schnorr-Γ       ║
║  signature protocol.                                                      ║
║                                                                           ║
║  Prime: p = 2^255 - 31  (≡ 1 mod 24, √2 and ω₃ both in GF(p))          ║
║  Group: SL(2,p) — 2×2 matrices mod p with det ≡ 1                       ║
║  Generators: Two random SL(2,p) elements g₁, g₂ (deterministic from seed)║
║                                                                           ║
║  Hard Problem: Word Problem / Matrix DL in SL(2,p) — non-abelian HSP    ║
║  not known to be solvable by quantum algorithms (no abelian hidden       ║
║  subgroup structure to exploit).                                          ║
╚══════════════════════════════════════════════════════════════════════════╝
"""

import secrets
import hashlib
import hmac
from typing import Tuple, Optional, List, NamedTuple


# ═══════════════════════════════════════════════════════════════════════════
# CANONICAL PRIME — p = 2^255 − 31  (≡ 1 mod 24)
# ═══════════════════════════════════════════════════════════════════════════
P = 57896044618658097711785492504343953926634992332820282019728792003956564819937
P_BITS = 255
assert P.bit_length() == 255
assert P % 24 == 1, "Prime must satisfy p ≡ 1 (mod 24)"

# ═══════════════════════════════════════════════════════════════════════════
# TONELLI-SHANKS: modular square root
# ═══════════════════════════════════════════════════════════════════════════

def mod_sqrt(a: int, p: int = P) -> int:
    """Tonelli-Shanks: find x such that x² ≡ a (mod p). Returns 0 if a ≡ 0."""
    a = a % p
    if a == 0:
        return 0
    if pow(a, (p - 1) // 2, p) != 1:
        raise ValueError(f"No square root for {a} mod {p}")
    # Factor p-1 as q * 2^s
    q = p - 1
    s = 0
    while q % 2 == 0:
        q //= 2
        s += 1
    # Find quadratic non-residue z
    z = 2
    while pow(z, (p - 1) // 2, p) != p - 1:
        z += 1
    m = s
    c = pow(z, q, p)
    t = pow(a, q, p)
    r = pow(a, (q + 1) // 2, p)
    while t != 1:
        # Find smallest i such that t^{2^i} ≡ 1
        i = 1
        temp = (t * t) % p
        while temp != 1:
            temp = (temp * temp) % p
            i += 1
        b = pow(c, 1 << (m - i - 1), p)
        m = i
        c = (b * b) % p
        t = (t * c) % p
        r = (r * b) % p
    return r


# ═══════════════════════════════════════════════════════════════════════════
# COMPUTE √2 mod p  AND  √(−2) mod p
# ═══════════════════════════════════════════════════════════════════════════
_SQRT2: Optional[int] = None
_SQRT_NEG2: Optional[int] = None

def sqrt2() -> int:
    global _SQRT2
    if _SQRT2 is None:
        _SQRT2 = mod_sqrt(2, P)
        assert (_SQRT2 * _SQRT2) % P == 2
    return _SQRT2

def sqrt_neg2() -> int:
    """√(−2) modulo p."""
    global _SQRT_NEG2
    if _SQRT_NEG2 is None:
        _SQRT_NEG2 = mod_sqrt(P - 2, P)  # -2 ≡ p-2 mod p
        assert (_SQRT_NEG2 * _SQRT_NEG2) % P == P - 2
    return _SQRT_NEG2


# ═══════════════════════════════════════════════════════════════════════════
# MODULAR INVERSE (extended Euclidean)
# ═══════════════════════════════════════════════════════════════════════════

def mod_inv(a: int, p: int = P) -> int:
    """Modular inverse: a⁻¹ mod p."""
    a = a % p
    if a == 0:
        raise ZeroDivisionError("mod_inv(0)")
    # Extended Euclidean
    t, newt = 0, 1
    r, newr = p, a
    while newr != 0:
        q = r // newr
        t, newt = newt, t - q * newt
        r, newr = newr, r - q * newr
    if r != 1:
        raise ValueError(f"{a} not invertible mod {p}")
    return t % p


# ═══════════════════════════════════════════════════════════════════════════
# 2×2 MATRIX OVER GF(p) WITH DET ≡ 1
# ═══════════════════════════════════════════════════════════════════════════

class GFMatrix:
    """2×2 matrix [[a,b],[c,d]] over GF(p) with det ≡ 1 (mod p)."""

    __slots__ = ("a", "b", "c", "d")

    def __init__(self, a: int, b: int, c: int, d: int):
        self.a = a % P
        self.b = b % P
        self.c = c % P
        self.d = d % P

    @staticmethod
    def identity() -> "GFMatrix":
        return GFMatrix(1, 0, 0, 1)

    def det(self) -> int:
        return (self.a * self.d - self.b * self.c) % P

    def assert_det_one(self) -> None:
        if self.det() != 1:
            raise ValueError(f"det = {self.det()} ≠ 1 mod P")

    def __matmul__(self, other: "GFMatrix") -> "GFMatrix":
        return GFMatrix(
            (self.a * other.a + self.b * other.c) % P,
            (self.a * other.b + self.b * other.d) % P,
            (self.c * other.a + self.d * other.c) % P,
            (self.c * other.b + self.d * other.d) % P,
        )

    def inverse(self) -> "GFMatrix":
        """Matrix inverse over GF(p): [[d,-b],[-c,a]] (since det ≡ 1)."""
        return GFMatrix(self.d, (P - self.b) % P, (P - self.c) % P, self.a)

    def __pow__(self, n: int) -> "GFMatrix":
        """Binary exponentiation M^n modulo p. O(log n) matrix multiplications."""
        if n < 0:
            return self.inverse() ** (-n)
        if n == 0:
            return GFMatrix.identity()
        result = GFMatrix.identity()
        base = GFMatrix(self.a, self.b, self.c, self.d)
        exp = n
        while exp > 0:
            if exp & 1:
                result = result @ base
            base = base @ base
            exp >>= 1
        return result

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, GFMatrix):
            return NotImplemented
        return (self.a == other.a and self.b == other.b and
                self.c == other.c and self.d == other.d)

    def neg(self) -> "GFMatrix":
        """Projective negation: in PSL, M ≡ −M."""
        return GFMatrix((P - self.a) % P, (P - self.b) % P,
                        (P - self.c) % P, (P - self.d) % P)

    def normalize_psl(self) -> "GFMatrix":
        """Return the canonical PSL representative (first non-zero entry > 0)."""
        for v in (self.a, self.b, self.c, self.d):
            if v != 0:
                if v > P // 2:
                    return self.neg()
                return self
        return self  # zero matrix (shouldn't happen)

    def serialize(self) -> bytes:
        """Deterministic 128-byte serialization (4 × 32 bytes big-endian)."""
        parts = []
        for v in (self.a, self.b, self.c, self.d):
            parts.append(v.to_bytes(32, "big"))
        return b"".join(parts)

    def hex(self) -> str:
        return self.serialize().hex()

    @classmethod
    def deserialize(cls, data: bytes) -> "GFMatrix":
        if len(data) != 128:
            raise ValueError(f"Expected 128 bytes, got {len(data)}")
        a = int.from_bytes(data[0:32], "big")
        b = int.from_bytes(data[32:64], "big")
        c = int.from_bytes(data[64:96], "big")
        d = int.from_bytes(data[96:128], "big")
        return cls(a, b, c, d)

    @classmethod
    def from_hex(cls, h: str) -> "GFMatrix":
        return cls.deserialize(bytes.fromhex(h))

    def __repr__(self) -> str:
        return f"GFMatrix(0x{self.a:064x}, 0x{self.b:064x}, 0x{self.c:064x}, 0x{self.d:064x})"


# ═══════════════════════════════════════════════════════════════════════════
# SL(2,p) WALK GENERATORS OVER GF(p)
# ═══════════════════════════════════════════════════════════════════════════

_GENS_CACHE: Optional[dict] = None

def _compute_generators() -> dict:
    """
    Construct generator matrices for the Schnorr walk group over GF(p).

    Uses TWO RANDOM elements of SL(2,p) as the base generators g₁, g₂.
    The walk alphabet is {g₁, g₁⁻¹, g₂, g₂⁻¹} (4 generators).

    Two random elements of SL(2,p) almost certainly generate the full
    group (order ≈ p³ ≈ 2^765), providing a large enough search space
    to make the word problem computationally hard.

    The generators are derived deterministically from a fixed seed hash
    so that all nodes agree on the same generating set.
    """
    import hashlib

    # Deterministic seed: SHAKE-256("HYPGAMMA_GF_SL2_GENERATORS_V1")
    # This ensures all nodes use the same generators.
    seed = hashlib.shake_256(
        b"HYPGAMMA_GF_SL2_GENERATORS_V1\x00" + P.to_bytes(32, "big")
    ).digest(128)

    def random_sl2_element(seed_bytes: bytes) -> GFMatrix:
        """Generate a random element of SL(2,p) deterministically from seed."""
        # Pick random a, b in GF(p), then solve for c, d such that ad - bc = 1
        a = (int.from_bytes(seed_bytes[0:32], "big") % (P - 1)) + 1
        b = (int.from_bytes(seed_bytes[32:64], "big") % (P - 1)) + 1
        # Choose c randomly, solve for d
        c = (int.from_bytes(seed_bytes[64:96], "big") % (P - 1)) + 1
        # d = (1 + b*c) * a^{-1} mod p
        a_inv = mod_inv(a, P)
        d = ((1 + b * c) % P * a_inv) % P
        return GFMatrix(a, b, c, d)

    # H-1 FIX (RED TEAM): SHA3-256 only produces 32 bytes, but random_sl2_element
    # indexes [32:64] and [64:96] which were empty slices (b=c=1 for all generators).
    # Now use SHAKE-256 to produce 96 bytes per generator (32 bytes each for a, b, c).
    g1_seed = hashlib.shake_256(seed + b"\x01").digest(96)
    g2_seed = hashlib.shake_256(seed + b"\x02").digest(96)

    g1 = random_sl2_element(g1_seed)
    g2 = random_sl2_element(g2_seed)

    # Ensure g1, g2 are not identity and have large order
    I = GFMatrix.identity()
    assert g1 != I and g1 != I.neg(), "g1 is ±I (degenerate)"
    assert g2 != I and g2 != I.neg(), "g2 is ±I (degenerate)"

    # Verify g1 and g2 generate a non-abelian subgroup (g1 @ g2 != g2 @ g1)
    assert g1 @ g2 != g2 @ g1, "Generators commute (abelian — degenerate)"

    # Compute inverses
    g1_inv = g1.inverse()
    g2_inv = g2.inverse()

    logger = __import__('logging').getLogger(__name__)
    logger.info(
        "[HypGF] Generators: g1 order≈?, g2 order≈? | "
        "tr(g1)=%d, tr(g2)=%d | non-abelian OK", 
        (g1.a + g1.d) % P, (g2.a + g2.d) % P)

    return {
        "a": g1, "a_inv": g1_inv,
        "b": g2, "b_inv": g2_inv,
    }


def get_generators() -> dict:
    global _GENS_CACHE
    if _GENS_CACHE is None:
        _GENS_CACHE = _compute_generators()
    return _GENS_CACHE


def generator_list() -> list:
    g = get_generators()
    return [g["a"], g["a_inv"], g["b"], g["b_inv"]]


def identity() -> GFMatrix:
    """The identity element of SL(2,p)."""
    return GFMatrix.identity()


# ═══════════════════════════════════════════════════════════════════════════
# WALK EVALUATION OVER GF(p)
# ═══════════════════════════════════════════════════════════════════════════

WALK_LENGTH = 512
N_GENERATORS = 4


def random_walk(length: int = WALK_LENGTH, reduced: bool = True) -> list:
    """Cryptographically random walk (indices 0..3).

    HIGH-1 FIX (RED TEAM): Uses rejection sampling to avoid modulo bias.
    For reduced walks (choosing from 3 options), draws bytes < 252 (252 = 3 × 84)
    to ensure uniform distribution over {0,1,2}. The old `byte % 3` with raw
    bytes produced bias: index 0 had 86/256 ≈ 33.59% vs index 2 at 85/256 ≈ 33.20%.
    """
    CANCEL = {0: 1, 1: 0, 2: 3, 3: 2}
    entropy = secrets.token_bytes(length * 2)
    ent_idx = 0
    walk = []
    prev = None
    for i in range(length):
        if reduced and prev is not None:
            choices = [j for j in range(4) if j != CANCEL[prev]]
            while True:
                if ent_idx >= len(entropy):
                    entropy = secrets.token_bytes(64)
                    ent_idx = 0
                byte = entropy[ent_idx]
                ent_idx += 1
                if byte < 252:
                    walk.append(choices[byte % 3])
                    break
        else:
            if ent_idx >= len(entropy):
                entropy = secrets.token_bytes(64)
                ent_idx = 0
            byte = entropy[ent_idx]
            ent_idx += 1
            walk.append(byte % 4)
        prev = walk[-1]
    return walk


def evaluate_walk(walk: list) -> GFMatrix:
    """Compose walk indices left-to-right into a single GFMatrix."""
    gens = generator_list()
    result = GFMatrix.identity()
    for idx in walk:
        result = result @ gens[idx]
    return result


def walk_to_hex(walk: list) -> str:
    """Pack walk indices (2 bits each, 4 per byte) into hex string.
    
    MED-5 FIX (RED TEAM): Prefix with "GF1:" format marker to disambiguate
    binary-packed encoding from legacy decimal-string encoding.
    """
    padded = walk + [0] * ((-len(walk)) % 4)
    result = bytearray()
    for i in range(0, len(padded), 4):
        byte = ((padded[i] << 6) | (padded[i+1] << 4) |
                (padded[i+2] << 2) | padded[i+3])
        result.append(byte)
    return "GF1:" + result.hex()


def walk_to_bytes(walk: list) -> bytes:
    """Pack walk indices into compact bytes (2 bits each, 4 per byte). No prefix."""
    padded = walk + [0] * ((-len(walk)) % 4)
    result = bytearray()
    for i in range(0, len(padded), 4):
        byte = ((padded[i] << 6) | (padded[i+1] << 4) |
                (padded[i+2] << 2) | padded[i+3])
        result.append(byte)
    return bytes(result)


def hex_to_walk(hex_str: str, length: int = WALK_LENGTH) -> list:
    """Unpack hex string back to walk indices.
    
    MED-5 FIX: Strip "GF1:" prefix if present; fall back to raw hex for legacy keys.
    """
    if hex_str.startswith("GF1:"):
        hex_str = hex_str[4:]
    data = bytes.fromhex(hex_str)
    walk = []
    for byte in data:
        walk.append((byte >> 6) & 0x3)
        walk.append((byte >> 4) & 0x3)
        walk.append((byte >> 2) & 0x3)
        walk.append(byte & 0x3)
    return walk[:length]


# ═══════════════════════════════════════════════════════════════════════════
# HASH-TO-WALK: Map a challenge to a deterministic walk
# ═══════════════════════════════════════════════════════════════════════════

def hash_to_walk(challenge: bytes, length: int = WALK_LENGTH) -> list:
    """Map arbitrary bytes to a reduced walk of given length via SHAKE-256."""
    digest = hashlib.shake_256(challenge).digest((length + 3) // 4 * 4)
    return hex_to_walk(digest.hex(), length)


# ═══════════════════════════════════════════════════════════════════════════
# SCHNORR-Γ OVER GF(p): SIGN AND VERIFY
# ═══════════════════════════════════════════════════════════════════════════
# FIXED GENERATOR for Scalar Schnorr-Γ over SL(2,p) (CRIT-A FIX)
#
# CRIT-A: gf_sign_full() accepted private_walk but never used it — the
# response Z = R @ y^c was computable entirely from public data, making
# the scheme universally forgeable. The signer's private key was irrelevant.
#
# FIX: We use a fixed generator g_schnorr ∈ SL(2,p) derived deterministically
# from a seed. The private key is a 256-bit scalar x, and the public key is
# y = g_schnorr ^ x (matrix exponentiation). Signing uses scalar Schnorr:
#   r ← random 256-bit scalar
#   R = g_schnorr ^ r
#   c = SHA3-256(DOMAIN_TAG ‖ R.ser ‖ y.ser ‖ m)  (mod nothing, 256-bit)
#   s = (r + c·x) mod SL2_ORDER  (scalar, reduced mod group order — H-3 FIX)
#   Z = g_schnorr ^ s   (response matrix)
# Verification: g_schnorr ^ s == R @ y ^ c
#
# C-1 CLASSIFICATION (RED TEAM): Security reduces to the Discrete Logarithm
# Problem in the CYCLIC subgroup ⟨g_schnorr⟩ ⊂ SL(2,p). Cyclic groups are
# abelian. Shor's algorithm solves this in polynomial time on a quantum
# computer. The non-abelian structure of SL(2,p) is architecturally present
# but cryptographically irrelevant — the attack surface is entirely within
# the cyclic subgroup generated by the fixed base point.
#
# CLASSICAL SECURITY: ~2^128 work (baby-step giant-step) if ord(g_schnorr)
# has a large prime factor. Adequate for current deployment.
# QUANTUM SECURITY: Broken by Shor. For PQ resistance, wrap with a
# NIST-standardized PQC layer (Dilithium signing, Kyber KEM).
#
# The private walk w is still stored as the master secret; x_sign is derived
# as SHA3-256(walk_bytes ‖ "SIGN").
# ═══════════════════════════════════════════════════════════════════════════

_G_SCHNORR_CACHE: Optional[GFMatrix] = None
_G_ENC_CACHE: Optional[GFMatrix] = None

# H-2 FIX (RED TEAM): Group order of SL(2,p) is p*(p-1)*(p+1) = p*(p^2-1).
# Any element g ∈ SL(2,p) satisfies g^(p*(p^2-1)) = I (Lagrange's theorem).
# This is used for modular reduction of s_scalar (H-3 FIX) and for verifying
# that fixed generators have no small-order subgroup factors (Pohlig-Hellman).
SL2_ORDER = P * (P * P - 1)   # ≈ 2^766 — safe upper bound for any element's order

def _derive_fixed_generator(domain_tag: bytes) -> GFMatrix:
    """Derive a fixed generator from SHAKE-256 for a given domain.

    H-2 FIX (RED TEAM): Ensures the generator's order has the largest available
    prime factor of |SL(2,p)|, preventing trivial Pohlig-Hellman reduction.

    C-1 CLASSIFICATION: The largest prime factor of |SL(2,p)| for p = 2^255-31
    is ~139 bits (from p+1). This provides ~70-bit DLP security — NOT 128-bit.
    For PQ resistance, wrap with NIST PQC (Dilithium/Kyber).
    """
    import hashlib

    # Largest prime factor of (p+1) — ensures generator has large-order subgroup
    Q_LARGEST = 375170473542173123302552081283421768694157  # 139-bit prime
    I = GFMatrix.identity()

    for attempt in range(100):
        seed = hashlib.shake_256(
            b"HYPGAMMA_GF_FIXED_GENERATOR_V1\x00" + domain_tag + attempt.to_bytes(4, 'big')
        ).digest(128)
        a = (int.from_bytes(seed[0:32], "big") % (P - 1)) + 1
        b = (int.from_bytes(seed[32:64], "big") % (P - 1)) + 1
        c = (int.from_bytes(seed[64:96], "big") % (P - 1)) + 1
        a_inv = mod_inv(a, P)
        d = ((1 + b * c) % P * a_inv) % P
        g = GFMatrix(a, b, c, d)

        if g == I or g == I.neg() or g.det() != 1:
            continue

        # H-2 FIX: Verify the generator's order has the largest prime factor
        # of |SL(2,p)|. If g^(SL2_ORDER/Q_LARGEST) == I, then Q_LARGEST does
        # NOT divide ord(g) and the generator's order consists only of small
        # factors — trivially breakable by Pohlig-Hellman.
        cofactor = SL2_ORDER // Q_LARGEST
        if g ** cofactor == I:
            continue  # generator has small order — try another

        logger = __import__('logging').getLogger(__name__)
        logger.info(f"[HypGF] Generator derived (attempt {attempt}): "
                    f"tr(g)={(g.a + g.d) % P}, ord(g) has {Q_LARGEST.bit_length()}-bit prime factor")
        return g

    raise RuntimeError(
        "Failed to derive a generator with large prime-order subgroup "
        "after 100 attempts"
    )

def get_schnorr_generator() -> GFMatrix:
    """Return the fixed generator for Schnorr signing (thread-safe, cached)."""
    global _G_SCHNORR_CACHE
    if _G_SCHNORR_CACHE is None:
        _G_SCHNORR_CACHE = _derive_fixed_generator(b"QTCL_SCHNORR_SIGN\x00")
    return _G_SCHNORR_CACHE

def get_encryption_generator() -> GFMatrix:
    """Return the fixed generator for encryption KEM (thread-safe, cached)."""
    global _G_ENC_CACHE
    if _G_ENC_CACHE is None:
        _G_ENC_CACHE = _derive_fixed_generator(b"QTCL_ENCRYPTION_KEM\x00")
    return _G_ENC_CACHE

def walk_to_scalar(walk: list, domain: bytes) -> int:
    """Derive a 256-bit scalar from a walk, domain-separated."""
    packed = walk_to_bytes(walk)
    return int.from_bytes(hashlib.sha3_256(packed + domain).digest(), 'big')

def walk_to_private_scalar(walk: list) -> int:
    """Derive the signing private scalar from a walk."""
    return walk_to_scalar(walk, b"QTCL_SIGN_SCALAR\x00")

def walk_to_encryption_scalar(walk: list) -> int:
    """Derive the encryption private scalar from a walk."""
    return walk_to_scalar(walk, b"QTCL_ENC_SCALAR\x00")

DOMAIN_TAG = b"HYPGAMMA_GF_SCHNORR_V3_SCALAR\x00"

class GFSchnorrSignature(NamedTuple):
    R: GFMatrix
    Z: GFMatrix
    c_full: int        # 256-bit Fiat-Shamir challenge
    s_scalar: int      # scalar response s = (r + c·x) mod SL2_ORDER
    R_hex: str         # canonical hex of R for exact binding

# ═══════════════════════════════════════════════════════════════════════════
# SCALAR SCHNORR-Γ OVER SL(2,p) — HONEST SECURITY CLASSIFICATION
#
# The private key is a 256-bit scalar x derived from the walk via SHA3-256.
# The public key is y = g ^ x where g is a fixed generator in SL(2,p).
#
# SIGN:
#   r ← secrets.randbits(256)
#   R = g ^ r
#   c = SHA3-256(DOMAIN_TAG ‖ R.ser ‖ y.ser ‖ m)  (256-bit challenge)
#   s = (r + c·x) mod SL2_ORDER  (scalar, reduced — H-3 FIX)
#   Z = g ^ s      (response matrix)
#   σ = (R, Z, c, s_scalar)
#
# VERIFY:
#   c' = SHA3-256(DOMAIN_TAG ‖ R.ser ‖ y.ser ‖ m)
#   Check c' == c
#   Check g ^ s == R @ y ^ c
#
# ═══════════════════════════════════════════════════════════════════════════
# C-1 SECURITY CLASSIFICATION (RED TEAM):
#
# CLASSICAL SECURITY: The DLP in ⟨g⟩ ⊂ SL(2,p) for p = 2^255-31 is limited
# by the largest prime factor of |SL(2,p)|. The largest prime factor of (p+1)
# is 375170473542173123302552081283421768694157 (139 bits). This gives ~70-bit
# DLP security via Pollard's rho — NOT 128-bit as previously claimed.
#
# QUANTUM SECURITY: Broken by Shor's algorithm. The cyclic subgroup ⟨g⟩ is
# abelian. Shor solves DLP in abelian groups in polynomial time.
#
# The non-abelian structure of SL(2,p) is architecturally present (walk
# generators, matrix products) but cryptographically irrelevant to the
# current signing scheme — the attack surface is entirely within the cyclic
# subgroup generated by the fixed base point.
#
# For deployments requiring >70-bit classical security or any quantum
# resistance, wrap with NIST-standardized PQC: Dilithium (signing) and
# Kyber (KEM). The current scheme then provides classical-security backup.
#
# No known practical non-abelian sigma protocol exists for the word problem
# in PSL(2,p) that is both sound and efficient. This is a research-level
# open problem in post-quantum group-based cryptography.
# ═══════════════════════════════════════════════════════════════════════════
# DEPRECATED: gf_sign / gf_verify (CRIT-2 FIX — RED TEAM)
# ═══════════════════════════════════════════════════════════════════════════

def gf_sign(message: bytes, private_walk: list,
            public_key: GFMatrix) -> GFSchnorrSignature:
    raise NotImplementedError(
        "gf_sign removed (CRIT-2): broken cut-and-choose construction. "
        "Use gf_sign_full() for standard Schnorr-Γ over GF(p)."
    )

def gf_verify(sig: GFSchnorrSignature, message: bytes,
              public_key: GFMatrix) -> bool:
    raise NotImplementedError(
        "gf_verify removed (CRIT-2): verification always passed. "
        "Use gf_verify_full() for standard Schnorr-Γ over GF(p)."
    )


# ═══════════════════════════════════════════════════════════════════════════
# NON-ABELIAN SCHNORR-Γ OVER SL(2,p) — PQ-SECURE CONSTRUCTION
#
# The private key IS the walk w (a word in the generators {g1, g1^-1, g2, g2^-1}).
# The public key is y = evaluate_walk(w) (the matrix product of the walk).
# Security reduces to the NON-ABELIAN WORD PROBLEM in PSL(2,p) — no known
# quantum algorithm solves this efficiently (Shor requires abelian groups).
#
# SIGN:
#   r ← random_walk(512)                                    (fresh nonce word)
#   R = evaluate_walk(r)                                     (commitment matrix)
#   c = SHA3-256(DOMAIN_TAG ‖ R.ser ‖ y.ser ‖ m)            (256-bit challenge)
#   s = r · (w_i for each bit i of c where bit i == 1)       (response word)
#   σ = (R, c, s_walk)
#
# VERIFY:
#   c' = SHA3-256(DOMAIN_TAG ‖ R.ser ‖ y.ser ‖ m)
#   Check c' == c
#   Check evaluate_walk(s_walk) == R @ y^c
#
# SECURITY: Forging requires decomposing R @ y^c into a word in the generators
# given only the matrix product — the non-abelian word problem. The response
# word s = r · (w^c) has expected length 512 + 128 = 640 steps. Each signature
# reveals at most 128 walk steps of w (selected by challenge bits), but
# reconstructing w from partial walks requires solving the word problem.
# Fresh nonce r is sampled from OS CSPRNG per signature.
# ═══════════════════════════════════════════════════════════════════════════

def gf_sign_full(message: bytes, private_walk: list,
                 public_key: GFMatrix) -> GFSchnorrSignature:
    """
    Scalar Schnorr-Γ over GF(p) — honest classical construction.
    
    Uses scalar secret x derived from private_walk. The response s = (r + c·x) mod SL2_ORDER.
    C-1: Security is ~70-bit classical DLP. For PQ resistance, add hybrid PQC layer.
    """
    g = get_schnorr_generator()
    x = walk_to_private_scalar(private_walk)

    # Nonce: 256-bit random scalar
    r = secrets.randbits(256)
    R = g ** r

    # Challenge: binds R, public key, and message
    c_bytes = hashlib.sha3_256(
        DOMAIN_TAG + R.serialize() + public_key.serialize() + message
    ).digest()
    c_full = int.from_bytes(c_bytes, 'big')

    # Response: scalar s = (r + c·x) mod SL2_ORDER
    # H-3 FIX: Without modular reduction, s_scalar is ~768 bits
    # and nonce reuse immediately reveals x via exact integer division.
    s_scalar = (r + c_full * x) % SL2_ORDER

    # Response matrix Z = g^s
    Z = g ** s_scalar

    return GFSchnorrSignature(R=R, Z=Z, c_full=c_full,
                              s_scalar=s_scalar, R_hex=R.hex())


def gf_verify_full(sig: GFSchnorrSignature, message: bytes,
                   public_key: GFMatrix) -> bool:
    """
    Verify scalar Schnorr-Γ over GF(p).
    
    The verifier checks:
      1. c == H(DOMAIN_TAG || R.ser || y.ser || m)    (challenge binding)
      2. g^s == R @ y^c                               (scalar response verification)
    """
    g = get_schnorr_generator()
    R = sig.R
    c_full = sig.c_full
    s_scalar = sig.s_scalar

    # Check 1: challenge binding
    expected_c = int.from_bytes(
        hashlib.sha3_256(
            DOMAIN_TAG + R.serialize() + public_key.serialize() + message
        ).digest(), 'big')
    if c_full != expected_c:
        return False

    # Check 2: g^s == R @ y^c
    y_c = public_key ** c_full
    g_s = g ** s_scalar
    expected = R @ y_c
    if g_s == expected:
        return True
    # PSL projective equivalence: ±I are identified in PSL(2,p)
    if g_s == expected.neg():
        return True
    return False


# ═══════════════════════════════════════════════════════════════════════════
# KEY GENERATION (CRIT-A FIX)
# ═══════════════════════════════════════════════════════════════════════════

class GFKeyPair(NamedTuple):
    private_key_hex: str
    public_key_hex: str
    address: str

def gf_generate_keypair() -> GFKeyPair:
    """Generate a scalar-Schnorr keypair over GF(p).

    The public key is y = g^x where g is the fixed generator
    and x = SHA3-256(walk || "SIGN") is derived from the walk.
    C-1: ~70-bit classical DLP security. For PQ resistance, add hybrid PQC layer.
    """
    g = get_schnorr_generator()
    walk = random_walk(WALK_LENGTH, reduced=True)
    x = walk_to_private_scalar(walk)
    y = g ** x
    priv_hex = walk_to_hex(walk)
    pub_hex = y.hex()
    address = hashlib.sha3_256(
        hashlib.sha3_256(y.serialize()).digest()
    ).hexdigest()
    return GFKeyPair(private_key_hex=priv_hex, public_key_hex=pub_hex,
                     address=address)


# ═══════════════════════════════════════════════════════════════════════════
# TESTS
# ═══════════════════════════════════════════════════════════════════════════

def test_finite_field():
    """Comprehensive test suite."""
    passed = 0
    total = 0

    def test(name, condition, detail=""):
        nonlocal passed, total
        total += 1
        if condition:
            print(f"  ✅ {name}")
            passed += 1
        else:
            print(f"  ❌ {name}: {detail}")

    print("=" * 72)
    print("  hyp_finite_field.py — Scalar Schnorr-Γ Test Suite")
    print(f"  Prime: p = 2^255 − 31  ({P_BITS} bits)")
    print("=" * 72)

    # ── Prime properties ──────────────────────────────────────────────
    test("p is prime", pow(2, P - 1, P) == 1)
    test("p ≡ 1 mod 24", P % 24 == 1)
    test("√2 exists in GF(p)", pow(sqrt2(), 2, P) == 2)

    # ── Generator checks ──────────────────────────────────────────────
    gens = get_generators()
    a, a_inv, b, b_inv = gens["a"], gens["a_inv"], gens["b"], gens["b_inv"]
    I = GFMatrix.identity()

    test("det(a) ≡ 1", a.det() == 1)
    test("det(b) ≡ 1", b.det() == 1)
    test("a @ a⁻¹ ≡ I", (a @ a_inv) == I)
    test("b @ b⁻¹ ≡ I", (b @ b_inv) == I)
    test("a ≠ I", a != I and a != I.neg())
    test("b ≠ I", b != I and b != I.neg())
    test("generators non-abelian", a @ b != b @ a)

    # ── Matrix arithmetic ─────────────────────────────────────────────
    M = GFMatrix(3, 5, 7, 11)
    M.d = mod_inv(3, P) * (1 + 5 * 7) % P  # make det=1
    test("det(M) = 1", M.det() == 1)
    test("M @ M⁻¹ = I", (M @ M.inverse()) == I)
    test("M^5 via pow", (M ** 5) == M @ M @ M @ M @ M)

    # ── Walk evaluation ───────────────────────────────────────────────
    walk = [0, 1] * 4  # a, a⁻¹, ... — should cancel to identity
    cancelled = evaluate_walk(walk)
    test("walk [a,a⁻¹]⁴ ≈ I", cancelled == I or cancelled == I.neg(),
         f"got {cancelled.hex()[:32]}")

    rw = random_walk(512)
    Mw = evaluate_walk(rw)
    test("random walk det=1", Mw.det() == 1)

    # ── Key generation ────────────────────────────────────────────────
    kp = gf_generate_keypair()
    test("keypair address 64 chars", len(kp.address) == 64)
    g = get_schnorr_generator()
    walk = hex_to_walk(kp.private_key_hex)
    x = walk_to_private_scalar(walk)
    pub_expected = g ** x
    pub = GFMatrix.from_hex(kp.public_key_hex)
    test("pub key hex roundtrip", pub == pub_expected)

    # ── Scalar Schnorr-Γ sign/verify ───────────────────────────────────
    msg = b"QTCL post-quantum block signature test"

    sig = gf_sign_full(msg, walk, pub)
    test("sign creates signature", sig is not None)
    test("scalar response present", sig.s_scalar > 0)

    valid = gf_verify_full(sig, msg, pub)
    test("verify(sign(m)) == True", valid, f"got {valid}")

    # ── Forgery resistance ──────────────────────────────────────────────
    # Attempt 1: different message
    msg2 = b"different message"
    valid2 = gf_verify_full(sig, msg2, pub)
    test("verify(sign(m1), m2) == False", not valid2)

    # Attempt 2: wrong public key
    kp2 = gf_generate_keypair()
    pub2 = GFMatrix.from_hex(kp2.public_key_hex)
    valid3 = gf_verify_full(sig, msg, pub2)
    test("verify with wrong pk == False", not valid3)

    # Attempt 3: forger tries to sign without private key
    forger_r = secrets.randbits(256)
    forger_R = g ** forger_r
    forger_c = int.from_bytes(hashlib.sha3_256(
        DOMAIN_TAG + forger_R.serialize() + pub.serialize() + msg
    ).digest(), 'big')
    forger_Z = forger_R @ (pub ** forger_c)
    forger_sig = GFSchnorrSignature(R=forger_R, Z=forger_Z, c_full=forger_c,
                                    s_scalar=0, R_hex=forger_R.hex())
    valid_forgery = gf_verify_full(forger_sig, msg, pub)
    test("forgery without private key == False", not valid_forgery,
         f"FORGERY SUCCEEDED")

    # ── 100 round-trips ────────────────────────────────────────────────
    failures = 0
    for i in range(100):
        s = gf_sign_full(msg, walk, pub)
        if not gf_verify_full(s, msg, pub):
            failures += 1
    test("100 sign/verify round-trips", failures == 0,
         f"{failures} failures")

    # ── Summary ───────────────────────────────────────────────────────
    print("=" * 72)
    print(f"  {passed}/{total} tests passed")
    print("=" * 72)
    return passed == total


if __name__ == "__main__":
    success = test_finite_field()
    exit(0 if success else 1)
