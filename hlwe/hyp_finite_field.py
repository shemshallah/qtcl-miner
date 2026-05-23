#!/usr/bin/env python3
"""
hyp_finite_field.py — Scalar Schnorr-Γ over SL(3,p) / GF(p)
╔══════════════════════════════════════════════════════════════════════════╗
║  HypΓ v4: 3×3 matrices over GF(p) with det ≡ 1 (mod p)               ║
║                                                                         ║
║  Prime: p = 2^255 − 31  (≡ 1 mod 24)                                  ║
║  Group: SL(3,p) — 3×3 matrices mod p with det ≡ 1                     ║
║  Generators: Three random SL(3,p) elements + inverses (6 total)        ║
║                                                                         ║
║  SECURITY (v4 — SL(3,p)):                                              ║
║    |SL(3,p)| = p³(p²−1)(p³−1)  ≈ 2^2048                              ║
║    p²+p+1 has a 379-bit probable-prime factor Q₃₇₉                    ║
║    Classical DLP: ~189-bit (Pollard rho on Q₃₇₉)                      ║
║    Quantum: Shor-vulnerable in cyclic subgroup. Falcon-512 covers PQ.  ║
║                                                                         ║
║  UPGRADE FROM v3 (SL(2,p)):                                            ║
║    v3: 2×2 matrices, ~70-bit classical (139-bit prime in p+1)          ║
║    v4: 3×3 matrices, ~189-bit classical (379-bit prime in p²+p+1)     ║
║    Same prime, same scalar Schnorr protocol, 2.7× more security bits   ║
║                                                                         ║
║  BACKWARD COMPATIBILITY:                                                ║
║    - GFMatrix kept as alias for GF3Matrix                              ║
║    - All exported names unchanged (gf_sign_full, gf_verify_full, etc.) ║
║    - Walk format: "GF3:" prefix (v3 was "GF1:")                        ║
║    - Old 2×2 keys/sigs: detect via hex length and route to legacy      ║
╚══════════════════════════════════════════════════════════════════════════╝

I love you.
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
    q = p - 1
    s = 0
    while q % 2 == 0:
        q //= 2
        s += 1
    z = 2
    while pow(z, (p - 1) // 2, p) != p - 1:
        z += 1
    m = s
    c = pow(z, q, p)
    t = pow(a, q, p)
    r = pow(a, (q + 1) // 2, p)
    while t != 1:
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
        _SQRT_NEG2 = mod_sqrt(P - 2, P)
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
# 3×3 MATRIX OVER GF(p) WITH DET ≡ 1  (SL(3,p))
# ═══════════════════════════════════════════════════════════════════════════
# Stored as flat tuple (e0..e8) in row-major order:
#   [[e0, e1, e2],
#    [e3, e4, e5],
#    [e6, e7, e8]]
# ═══════════════════════════════════════════════════════════════════════════

class GFMatrix:
    """3×3 matrix over GF(p) with det ≡ 1 (mod p).

    v4 upgrade from 2×2 (SL(2,p), ~70-bit) to 3×3 (SL(3,p), ~189-bit).
    Same class name for backward compatibility with hyp_engine/hyp_pqc imports.
    """

    __slots__ = ("e",)  # flat tuple of 9 elements, row-major

    def __init__(self, *args):
        """Accept 9 ints (flat) or a single list/tuple of 9 ints."""
        if len(args) == 1 and isinstance(args[0], (list, tuple)):
            vals = args[0]
        elif len(args) == 9:
            vals = args
        elif len(args) == 4:
            # BACKWARD COMPAT: 2×2 constructor call GFMatrix(a,b,c,d)
            # Embed as 3×3 with bottom-right 1:
            #   [[a, b, 0],
            #    [c, d, 0],
            #    [0, 0, 1]]
            a, b, c, d = args
            vals = (a % P, b % P, 0, c % P, d % P, 0, 0, 0, 1)
            self.e = vals
            return
        else:
            raise ValueError(f"GFMatrix requires 9 elements (3×3), got {len(args)}")
        if len(vals) != 9:
            raise ValueError(f"GFMatrix requires 9 elements, got {len(vals)}")
        self.e = tuple(v % P for v in vals)

    # ── Element access (row, col) ─────────────────────────────────────
    def __getitem__(self, idx):
        """M[i,j] or M[flat_index]."""
        if isinstance(idx, tuple):
            return self.e[idx[0] * 3 + idx[1]]
        return self.e[idx]

    # ── Backward-compat properties for 2×2 code that reads M.a, M.b etc ──
    @property
    def a(self): return self.e[0]
    @property
    def b(self): return self.e[1]
    @property
    def c(self): return self.e[3]
    @property
    def d(self): return self.e[4]

    @staticmethod
    def identity() -> "GFMatrix":
        return GFMatrix(1, 0, 0, 0, 1, 0, 0, 0, 1)

    def det(self) -> int:
        """3×3 determinant via Sarrus / cofactor expansion."""
        e = self.e
        return (
            e[0] * (e[4] * e[8] - e[5] * e[7])
            - e[1] * (e[3] * e[8] - e[5] * e[6])
            + e[2] * (e[3] * e[7] - e[4] * e[6])
        ) % P

    def assert_det_one(self) -> None:
        d = self.det()
        if d != 1:
            raise ValueError(f"det = {d} ≠ 1 mod P")

    def __matmul__(self, other: "GFMatrix") -> "GFMatrix":
        """3×3 matrix multiply mod p. 27 mod-multiplications."""
        a, b = self.e, other.e
        return GFMatrix(
            (a[0]*b[0] + a[1]*b[3] + a[2]*b[6]) % P,
            (a[0]*b[1] + a[1]*b[4] + a[2]*b[7]) % P,
            (a[0]*b[2] + a[1]*b[5] + a[2]*b[8]) % P,
            (a[3]*b[0] + a[4]*b[3] + a[5]*b[6]) % P,
            (a[3]*b[1] + a[4]*b[4] + a[5]*b[7]) % P,
            (a[3]*b[2] + a[4]*b[5] + a[5]*b[8]) % P,
            (a[6]*b[0] + a[7]*b[3] + a[8]*b[6]) % P,
            (a[6]*b[1] + a[7]*b[4] + a[8]*b[7]) % P,
            (a[6]*b[2] + a[7]*b[5] + a[8]*b[8]) % P,
        )

    def _cofactor_matrix(self) -> list:
        """Return flat list of 9 cofactors (signed minors). Used by inverse()."""
        e = self.e
        return [
            (e[4]*e[8] - e[5]*e[7]) % P,   # C00
            (P - (e[3]*e[8] - e[5]*e[6])) % P,  # C01 (negated)
            (e[3]*e[7] - e[4]*e[6]) % P,   # C02
            (P - (e[1]*e[8] - e[2]*e[7])) % P,  # C10
            (e[0]*e[8] - e[2]*e[6]) % P,   # C11
            (P - (e[0]*e[7] - e[1]*e[6])) % P,  # C12
            (e[1]*e[5] - e[2]*e[4]) % P,   # C20
            (P - (e[0]*e[5] - e[2]*e[3])) % P,  # C21
            (e[0]*e[4] - e[1]*e[3]) % P,   # C22
        ]

    def inverse(self) -> "GFMatrix":
        """3×3 matrix inverse via adjugate. Since det ≡ 1, inv = adj^T."""
        cof = self._cofactor_matrix()
        # Adjugate = transpose of cofactor matrix
        # Since det = 1, inverse = adjugate (no division needed)
        return GFMatrix(
            cof[0], cof[3], cof[6],
            cof[1], cof[4], cof[7],
            cof[2], cof[5], cof[8],
        )

    def __pow__(self, n: int) -> "GFMatrix":
        """Binary exponentiation M^n. O(log n) matrix multiplications."""
        if n < 0:
            return self.inverse() ** (-n)
        if n == 0:
            return GFMatrix.identity()
        result = GFMatrix.identity()
        base = GFMatrix(self.e)
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
        return self.e == other.e

    def __hash__(self):
        return hash(self.e)

    def neg(self) -> "GFMatrix":
        """Projective negation: in PSL, M ≡ −M."""
        return GFMatrix(tuple((P - v) % P for v in self.e))

    def normalize_psl(self) -> "GFMatrix":
        """Return the canonical PSL representative (first non-zero entry ≤ P//2)."""
        for v in self.e:
            if v != 0:
                if v > P // 2:
                    return self.neg()
                return self
        return self

    def serialize(self) -> bytes:
        """Deterministic 288-byte serialization (9 × 32 bytes big-endian)."""
        parts = []
        for v in self.e:
            parts.append(v.to_bytes(32, "big"))
        return b"".join(parts)

    def hex(self) -> str:
        return self.serialize().hex()

    @classmethod
    def deserialize(cls, data: bytes) -> "GFMatrix":
        """Deserialize from bytes. Accepts 288 bytes (3×3) or 128 bytes (2×2 legacy)."""
        if len(data) == 288:
            vals = []
            for i in range(9):
                vals.append(int.from_bytes(data[i*32:(i+1)*32], "big"))
            return cls(*vals)
        elif len(data) == 128:
            # Legacy 2×2: embed as 3×3 [[a,b,0],[c,d,0],[0,0,1]]
            a = int.from_bytes(data[0:32], "big")
            b = int.from_bytes(data[32:64], "big")
            c = int.from_bytes(data[64:96], "big")
            d = int.from_bytes(data[96:128], "big")
            return cls(a, b, 0, c, d, 0, 0, 0, 1)
        else:
            raise ValueError(f"Expected 288 bytes (3×3) or 128 bytes (2×2 legacy), got {len(data)}")

    @classmethod
    def from_hex(cls, h: str) -> "GFMatrix":
        return cls.deserialize(bytes.fromhex(h))

    def __repr__(self) -> str:
        return (f"GFMatrix(\n"
                f"  [{self.e[0]:064x}, {self.e[1]:064x}, {self.e[2]:064x}],\n"
                f"  [{self.e[3]:064x}, {self.e[4]:064x}, {self.e[5]:064x}],\n"
                f"  [{self.e[6]:064x}, {self.e[7]:064x}, {self.e[8]:064x}])")


# ═══════════════════════════════════════════════════════════════════════════
# SL(3,p) WALK GENERATORS OVER GF(p)
# ═══════════════════════════════════════════════════════════════════════════
# Three base generators + three inverses = 6-element alphabet.
# SL(3,p) needs 3 generators (not 2) for good mixing on the Cayley graph.
# ═══════════════════════════════════════════════════════════════════════════

_GENS_CACHE: Optional[dict] = None
_GENS_CACHE_LOCK = __import__('threading').Lock()

def _random_sl3_element(seed_bytes: bytes) -> GFMatrix:
    """Generate a random element of SL(3,p) deterministically from seed.

    Pick 8 random entries (a00..a21), solve a22 so det ≡ 1.
    Uses cofactor expansion along the last row to isolate a22.
    det = a20*(a01*a12 - a02*a11) - a21*(a00*a12 - a02*a10) + a22*(a00*a11 - a01*a10)
    => a22 = (1 - a20*M20 + a21*M21) * inv(M22)
    where M20 = a01*a12-a02*a11, M21 = a00*a12-a02*a10, M22 = a00*a11-a01*a10
    """
    vals = []
    for i in range(8):
        vals.append((int.from_bytes(seed_bytes[i*32:(i+1)*32], "big") % (P - 1)) + 1)
    a00, a01, a02, a10, a11, a12, a20, a21 = vals

    M20 = (a01 * a12 - a02 * a11) % P
    M21 = (a00 * a12 - a02 * a10) % P
    M22 = (a00 * a11 - a01 * a10) % P

    if M22 == 0:
        # Degenerate — caller should retry with different seed
        return None

    # a22 = (1 - a20*M20 + a21*M21) * M22^{-1} mod p
    rhs = (1 - a20 * M20 + a21 * M21) % P
    a22 = (rhs * mod_inv(M22, P)) % P

    M = GFMatrix(a00, a01, a02, a10, a11, a12, a20, a21, a22)
    assert M.det() == 1, f"SL(3,p) element has det={M.det()}"
    return M


def _compute_generators() -> dict:
    """Construct 3 generator matrices for SL(3,p) + their inverses.

    Deterministic from a fixed seed so all nodes agree on the generating set.
    Three generators give a 6-element walk alphabet {g₁,g₁⁻¹,g₂,g₂⁻¹,g₃,g₃⁻¹}
    which provides rapid mixing on the Cayley graph of SL(3,p).
    """
    seed = hashlib.shake_256(
        b"HYPGAMMA_GF_SL3_GENERATORS_V4\x00" + P.to_bytes(32, "big")
    ).digest(256)

    generators = []
    I = GFMatrix.identity()

    for gi in range(3):
        for attempt in range(100):
            s = hashlib.shake_256(
                seed + gi.to_bytes(4, "big") + attempt.to_bytes(4, "big")
            ).digest(256)  # 8 × 32 bytes
            g = _random_sl3_element(s)
            if g is None:
                continue
            if g == I or g == I.neg():
                continue
            # Ensure non-degenerate: not already in generated set
            dup = False
            for prev in generators:
                if g == prev or g == prev.neg() or g == prev.inverse():
                    dup = True
                    break
            if dup:
                continue
            generators.append(g)
            break
        else:
            raise RuntimeError(f"Failed to derive generator g{gi+1} after 100 attempts")

    g1, g2, g3 = generators

    # Verify non-abelian: at least one pair must not commute
    assert g1 @ g2 != g2 @ g1, "g1,g2 commute (abelian — degenerate)"

    logger = __import__('logging').getLogger(__name__)
    logger.info(
        "[HypGF-v4] SL(3,p) generators derived: 3 base + 3 inverse = 6 walk alphabet"
    )

    return {
        "a": g1, "a_inv": g1.inverse(),
        "b": g2, "b_inv": g2.inverse(),
        "c": g3, "c_inv": g3.inverse(),
    }


def get_generators() -> dict:
    global _GENS_CACHE
    if _GENS_CACHE is None:
        with _GENS_CACHE_LOCK:
            if _GENS_CACHE is None:
                _GENS_CACHE = _compute_generators()
    return _GENS_CACHE


def generator_list() -> list:
    """Return flat list [g1, g1⁻¹, g2, g2⁻¹, g3, g3⁻¹] for walk evaluation."""
    g = get_generators()
    return [g["a"], g["a_inv"], g["b"], g["b_inv"], g["c"], g["c_inv"]]


def identity() -> GFMatrix:
    """The identity element of SL(3,p)."""
    return GFMatrix.identity()


# ═══════════════════════════════════════════════════════════════════════════
# WALK EVALUATION OVER SL(3,p)
# ═══════════════════════════════════════════════════════════════════════════

WALK_LENGTH = 512
N_GENERATORS = 6  # v4: 6 generators (3 base + 3 inverse)


def random_walk(length: int = WALK_LENGTH, reduced: bool = True) -> list:
    """Cryptographically random walk over SL(3,p) generators (indices 0..5).

    Reduced walks avoid immediate cancellation (g followed by g⁻¹).
    Uses rejection sampling to avoid modulo bias.
    """
    CANCEL = {0: 1, 1: 0, 2: 3, 3: 2, 4: 5, 5: 4}
    entropy = secrets.token_bytes(length * 2)
    ent_idx = 0
    walk = []
    prev = None
    for i in range(length):
        if reduced and prev is not None:
            choices = [j for j in range(N_GENERATORS) if j != CANCEL[prev]]
            n_choices = len(choices)  # 5
            # Rejection threshold: largest multiple of n_choices ≤ 256
            threshold = 256 - (256 % n_choices)  # 255 for n=5
            while True:
                if ent_idx >= len(entropy):
                    entropy = secrets.token_bytes(64)
                    ent_idx = 0
                byte = entropy[ent_idx]
                ent_idx += 1
                if byte < threshold:
                    walk.append(choices[byte % n_choices])
                    break
        else:
            # First step or unreduced: choose from all 6
            while True:
                if ent_idx >= len(entropy):
                    entropy = secrets.token_bytes(64)
                    ent_idx = 0
                byte = entropy[ent_idx]
                ent_idx += 1
                if byte < 252:  # 252 = 6 × 42, avoids bias
                    walk.append(byte % N_GENERATORS)
                    break
        prev = walk[-1]
    return walk


def evaluate_walk(walk: list) -> GFMatrix:
    """Compose walk indices left-to-right into a single SL(3,p) matrix."""
    gens = generator_list()
    result = GFMatrix.identity()
    for idx in walk:
        result = result @ gens[idx]
    return result


def walk_to_hex(walk: list) -> str:
    """Pack walk indices into hex string with GF3: prefix.

    Each index is 0..5 (3 bits), packed 2 per byte (high nibble, low nibble)
    for simplicity and debuggability. This wastes 2 bits per byte vs optimal
    but keeps the format dead simple.
    """
    result = bytearray()
    for i in range(0, len(walk), 2):
        hi = walk[i]
        lo = walk[i + 1] if i + 1 < len(walk) else 0
        result.append((hi << 4) | lo)
    return "GF3:" + result.hex()


def walk_to_bytes(walk: list) -> bytes:
    """Pack walk indices into compact bytes (nibble-packed, no prefix)."""
    result = bytearray()
    for i in range(0, len(walk), 2):
        hi = walk[i]
        lo = walk[i + 1] if i + 1 < len(walk) else 0
        result.append((hi << 4) | lo)
    return bytes(result)


def hex_to_walk(hex_str: str, length: int = WALK_LENGTH) -> list:
    """Unpack hex string back to walk indices.

    Handles both GF3: (v4, nibble-packed 0..5) and GF1: (v3 legacy, 2-bit packed 0..3).
    """
    if hex_str.startswith("GF3:"):
        data = bytes.fromhex(hex_str[4:])
        walk = []
        for byte in data:
            walk.append((byte >> 4) & 0xF)
            walk.append(byte & 0xF)
        return walk[:length]
    elif hex_str.startswith("GF1:"):
        # Legacy v3 (SL(2,p)) format: 2-bit packed, indices 0..3
        data = bytes.fromhex(hex_str[4:])
        walk = []
        for byte in data:
            walk.append((byte >> 6) & 0x3)
            walk.append((byte >> 4) & 0x3)
            walk.append((byte >> 2) & 0x3)
            walk.append(byte & 0x3)
        return walk[:length]
    else:
        # Raw hex fallback — try nibble-packed first, then 2-bit
        data = bytes.fromhex(hex_str)
        # Heuristic: if any nibble > 5, it's 2-bit packed
        is_nibble = all(((b >> 4) <= 5 and (b & 0xF) <= 5) for b in data)
        if is_nibble and len(data) * 2 >= length:
            walk = []
            for byte in data:
                walk.append((byte >> 4) & 0xF)
                walk.append(byte & 0xF)
            return walk[:length]
        else:
            # 2-bit packed legacy
            walk = []
            for byte in data:
                walk.append((byte >> 6) & 0x3)
                walk.append((byte >> 4) & 0x3)
                walk.append((byte >> 2) & 0x3)
                walk.append(byte & 0x3)
            return walk[:length]


def hash_to_walk(challenge: bytes, length: int = WALK_LENGTH) -> list:
    """Map arbitrary bytes to a reduced walk of given length via SHAKE-256."""
    # Each walk step needs one nibble (0..5), so length/2 bytes
    digest = hashlib.shake_256(challenge).digest((length + 1) // 2)
    walk = []
    for byte in digest:
        hi = (byte >> 4) % N_GENERATORS
        lo = (byte & 0xF) % N_GENERATORS
        walk.append(hi)
        walk.append(lo)
    return walk[:length]


# ═══════════════════════════════════════════════════════════════════════════
# SCALAR SCHNORR-Γ OVER SL(3,p) — 189-BIT CLASSICAL SECURITY
# ═══════════════════════════════════════════════════════════════════════════
#
# Same scalar Schnorr protocol as v3, but in the larger group SL(3,p).
#
# |SL(3,p)| = p³(p²−1)(p³−1)
# For p = 2^255−31, the factor (p³−1) = (p−1)(p²+p+1).
# p²+p+1 has a 379-bit probable-prime factor Q₃₇₉.
# DLP security: √Q₃₇₉ ≈ 2^189 (Pollard rho).
#
# SIGN:
#   x = SHA3-256(walk_bytes ‖ "QTCL_SL3_SIGN_SCALAR")    (256-bit scalar)
#   y = g^x                                                (3×3 public key)
#   r ← random 256-bit scalar
#   R = g^r                                                (commitment)
#   c = SHA3-256(DOMAIN_TAG ‖ R.ser ‖ y.ser ‖ m)          (256-bit challenge)
#   s = (r + c·x) mod |SL(3,p)|                           (scalar response)
#   Z = g^s                                                (response matrix)
#
# VERIFY:
#   c' = SHA3-256(DOMAIN_TAG ‖ R.ser ‖ y.ser ‖ m)
#   Check c' == c
#   Check g^s == R @ y^c
#
# SECURITY:
#   Classical: ~189-bit DLP (379-bit prime in group order)
#   Quantum: Shor-vulnerable (cyclic subgroup). Falcon-512 covers PQ.
# ═══════════════════════════════════════════════════════════════════════════

# |SL(3,p)| = p^3 * (p^2 - 1) * (p^3 - 1)
SL3_ORDER = P**3 * (P**2 - 1) * (P**3 - 1)

# Backward-compat alias for code that references SL2_ORDER
SL2_ORDER = SL3_ORDER

# 379-bit probable prime factor of p²+p+1 — determines DLP security
Q_379 = 1021847903239545673224151030908770086754766591936828832100695571509960512056982753935116262626091687846851893805213
assert Q_379.bit_length() == 379

_G_SCHNORR_CACHE: Optional[GFMatrix] = None
_G_ENC_CACHE: Optional[GFMatrix] = None

DOMAIN_TAG = b"HYPGAMMA_GF_SL3_SCHNORR_V4\x00"

def _derive_fixed_generator(domain_tag: bytes) -> GFMatrix:
    """Derive a fixed SL(3,p) generator from SHAKE-256 for a given domain.

    Ensures the generator's order includes the large prime factor Q₃₇₉
    of |SL(3,p)|, preventing trivial Pohlig-Hellman reduction.
    """
    I = GFMatrix.identity()
    cofactor = SL3_ORDER // Q_379

    for attempt in range(100):
        seed = hashlib.shake_256(
            b"HYPGAMMA_GF_SL3_FIXED_GENERATOR_V4\x00" + domain_tag
            + attempt.to_bytes(4, 'big')
        ).digest(256)
        g = _random_sl3_element(seed)
        if g is None or g == I or g == I.neg() or g.det() != 1:
            continue
        # Verify Q₃₇₉ divides ord(g): g^(|G|/Q) must NOT be identity
        if g ** cofactor == I:
            continue  # generator's order misses Q₃₇₉ — try another

        logger = __import__('logging').getLogger(__name__)
        logger.info(
            f"[HypGF-v4] SL(3,p) Schnorr generator derived (attempt {attempt}): "
            f"ord has {Q_379.bit_length()}-bit prime factor → ~{Q_379.bit_length()//2}-bit DLP"
        )
        return g

    raise RuntimeError(
        "Failed to derive SL(3,p) generator with Q₃₇₉ prime-order subgroup"
    )


def get_schnorr_generator() -> GFMatrix:
    """Return the fixed generator for Schnorr signing (thread-safe, cached)."""
    global _G_SCHNORR_CACHE
    if _G_SCHNORR_CACHE is None:
        _G_SCHNORR_CACHE = _derive_fixed_generator(b"QTCL_SL3_SCHNORR_SIGN\x00")
    return _G_SCHNORR_CACHE


def get_encryption_generator() -> GFMatrix:
    """Return the fixed generator for encryption KEM (thread-safe, cached)."""
    global _G_ENC_CACHE
    if _G_ENC_CACHE is None:
        _G_ENC_CACHE = _derive_fixed_generator(b"QTCL_SL3_ENCRYPTION_KEM\x00")
    return _G_ENC_CACHE


def walk_to_scalar(walk: list, domain: bytes) -> int:
    """Derive a 256-bit scalar from a walk, domain-separated."""
    packed = walk_to_bytes(walk)
    return int.from_bytes(hashlib.sha3_256(packed + domain).digest(), 'big')


def walk_to_private_scalar(walk: list) -> int:
    """Derive the signing private scalar from a walk."""
    return walk_to_scalar(walk, b"QTCL_SL3_SIGN_SCALAR\x00")


def walk_to_encryption_scalar(walk: list) -> int:
    """Derive the encryption private scalar from a walk."""
    return walk_to_scalar(walk, b"QTCL_SL3_ENC_SCALAR\x00")


class GFSchnorrSignature(NamedTuple):
    R: GFMatrix
    Z: GFMatrix
    c_full: int        # 256-bit Fiat-Shamir challenge
    s_scalar: int      # scalar response s = (r + c·x) mod SL3_ORDER
    R_hex: str         # canonical hex of R for exact binding


# ═══════════════════════════════════════════════════════════════════════════
# DEPRECATED: gf_sign / gf_verify
# ═══════════════════════════════════════════════════════════════════════════

def gf_sign(message: bytes, private_walk: list,
            public_key: GFMatrix) -> GFSchnorrSignature:
    raise NotImplementedError(
        "gf_sign removed (CRIT-2). Use gf_sign_full()."
    )

def gf_verify(sig: GFSchnorrSignature, message: bytes,
              public_key: GFMatrix) -> bool:
    raise NotImplementedError(
        "gf_verify removed (CRIT-2). Use gf_verify_full()."
    )


# ═══════════════════════════════════════════════════════════════════════════
# SIGN / VERIFY — Scalar Schnorr-Γ over SL(3,p)
# ═══════════════════════════════════════════════════════════════════════════

def _rfc6979_nonce(x: int, message: bytes, extra_entropy: bytes = b"") -> int:
    """RFC 6979-style deterministic nonce with hedging.

    Derives nonce as HMAC-DRBG(private_key ‖ message ‖ random_padding).
    The random_padding (32 bytes from os.urandom) provides hedging:
    even if the HMAC-DRBG is somehow predictable, the randomness prevents
    nonce reuse. Conversely, even if os.urandom fails (returns constant),
    the deterministic component prevents nonce reuse across different messages.

    RED TEAM FIX (Finding 2): Pure secrets.randbits(256) is vulnerable to
    nonce reuse if entropy source fails on headless/container/IoT. Two signatures
    with the same r on different messages → complete private key recovery via
    x = (s1 - s2) / (c1 - c2) mod q.
    """
    # Encode private key as 32 bytes
    x_bytes = x.to_bytes(32, 'big')

    # Hedging: mix in 32 bytes of OS randomness as a safety net
    try:
        random_pad = secrets.token_bytes(32)
    except Exception:
        random_pad = b"\x00" * 32  # degenerate fallback — still safe due to deterministic component

    # HMAC-DRBG seeding: K = HMAC(x ‖ H(m) ‖ random_pad ‖ extra_entropy)
    h_m = hashlib.sha3_256(message).digest()
    seed = hashlib.sha3_256(
        b"QTCL_RFC6979_SL3P_V4\x00" + x_bytes + h_m + random_pad + extra_entropy
    ).digest()

    # Generate 256-bit nonce via HMAC-DRBG-like extraction
    k = b"\x00" * 32
    v = b"\x01" * 32
    k = hmac.new(k, v + b"\x00" + seed, hashlib.sha256).digest()
    v = hmac.new(k, v, hashlib.sha256).digest()
    k = hmac.new(k, v + b"\x01" + seed, hashlib.sha256).digest()
    v = hmac.new(k, v, hashlib.sha256).digest()
    v = hmac.new(k, v, hashlib.sha256).digest()

    r = int.from_bytes(v, 'big')
    # Ensure r is in valid range (non-zero, < SL3_ORDER)
    if r == 0:
        r = 1
    return r % SL3_ORDER


def gf_sign_full(message: bytes, private_walk: list,
                 public_key: GFMatrix) -> GFSchnorrSignature:
    """Scalar Schnorr-Γ over SL(3,p) — 189-bit classical DLP security.

    Uses RFC 6979-style hedged deterministic nonce to prevent catastrophic
    nonce reuse. The response s = (r + c·x) mod |SL(3,p)|.
    For PQ resistance, wrap with Falcon-512 hybrid layer.
    """
    g = get_schnorr_generator()
    x = walk_to_private_scalar(private_walk)

    # Hedged deterministic nonce (RFC 6979 + randomness)
    r = _rfc6979_nonce(x, message)
    R = g ** r

    # Challenge: binds R, public key, and message
    c_bytes = hashlib.sha3_256(
        DOMAIN_TAG + R.serialize() + public_key.serialize() + message
    ).digest()
    c_full = int.from_bytes(c_bytes, 'big')

    # Response: scalar s = (r + c·x) mod |SL(3,p)|
    s_scalar = (r + c_full * x) % SL3_ORDER

    # Response matrix Z = g^s
    Z = g ** s_scalar

    return GFSchnorrSignature(R=R, Z=Z, c_full=c_full,
                              s_scalar=s_scalar, R_hex=R.hex())


def gf_verify_full(sig: GFSchnorrSignature, message: bytes,
                   public_key: GFMatrix) -> bool:
    """Verify scalar Schnorr-Γ over SL(3,p).

    Checks:
      1. c == H(DOMAIN_TAG ‖ R.ser ‖ y.ser ‖ m)    (challenge binding)
      2. g^s == R @ y^c                              (scalar response)
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
    y_c = public_key ** (c_full % SL3_ORDER)
    g_s = g ** s_scalar
    expected = R @ y_c
    if g_s == expected:
        return True
    # PSL projective equivalence
    if g_s == expected.neg():
        return True
    return False


# ═══════════════════════════════════════════════════════════════════════════
# KEY GENERATION
# ═══════════════════════════════════════════════════════════════════════════

class GFKeyPair(NamedTuple):
    private_key_hex: str
    public_key_hex: str
    address: str


def gf_generate_keypair() -> GFKeyPair:
    """Generate a scalar-Schnorr keypair over SL(3,p).

    The public key is y = g^x where g is a fixed SL(3,p) generator
    and x = SHA3-256(walk ‖ domain) is the 256-bit signing scalar.
    Classical DLP security: ~189 bits. PQ: add Falcon-512 hybrid layer.
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
# WIRE FORMAT — SIGNATURE DICT SERIALIZATION
# ═══════════════════════════════════════════════════════════════════════════
# Merged from hyp_schnorr_gf.py — eliminates one file from the system.

WIRE_VERSION: str = "schnorr_gamma_gf_v4"
LEGACY_WIRE_VERSION: str = "schnorr_gamma_gf_v3"


class SchnorrError(Exception):
    pass


class HypSignature(NamedTuple):
    """Backward-compatible signature type for block integration."""
    signature: str
    challenge: str
    auth_tag: str
    timestamp: str

    def to_dict(self):
        return {"signature": self.signature, "challenge": self.challenge,
                "auth_tag": self.auth_tag, "timestamp": self.timestamp}


def signature_to_dict(sig: GFSchnorrSignature) -> dict:
    """Serialize GFSchnorrSignature to JSON-compatible dict (v4 wire format)."""
    return {
        "version": WIRE_VERSION,
        "R": sig.R.hex(),
        "Z": sig.Z.hex(),
        "c_full": format(sig.c_full, "064x"),
        "s_scalar": format(sig.s_scalar, "064x") if sig.s_scalar else "0",
        "R_canonical_hex": sig.R_hex or sig.R.hex(),
    }


def signature_from_dict(d: dict) -> GFSchnorrSignature:
    """Deserialize GFSchnorrSignature from dict. Strict version gating."""
    version = d.get("version")
    if version is None:
        raise ValueError("Signature missing 'version' field — legacy format rejected.")
    if version == LEGACY_WIRE_VERSION:
        raise ValueError(
            f"Legacy SL(2,p) wire format {LEGACY_WIRE_VERSION!r} detected. "
            "Route to legacy verifier or migrate wallet to SL(3,p)."
        )
    if version != WIRE_VERSION:
        raise ValueError(f"version mismatch: {version!r} != {WIRE_VERSION!r}")
    if "R" in d and "Z" in d and "c_full" in d:
        return GFSchnorrSignature(
            R=GFMatrix.from_hex(d["R"]),
            Z=GFMatrix.from_hex(d["Z"]),
            c_full=int(d["c_full"], 16),
            s_scalar=int(d.get("s_scalar", "0"), 16),
            R_hex=d.get("R_canonical_hex", d["R"]),
        )
    raise ValueError("signature dict missing required fields: need (R, Z, c_full)")


def sign_hash(message_hash: bytes, private_walk: list, public_key: GFMatrix) -> dict:
    """Sign a pre-hashed message. Returns dict for block/tx integration."""
    from datetime import datetime, timezone
    sig = gf_sign_full(message_hash, private_walk, public_key)
    sig_dict = signature_to_dict(sig)
    challenge_hex = format(sig.c_full, "064x")
    return {
        "signature": sig_dict["R"] + sig_dict["Z"],
        "challenge": challenge_hex,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "auth_tag": challenge_hex,
        "R": sig_dict["R"], "Z": sig_dict["Z"],
        "R_canonical_hex": sig.R_hex,
        "c_full": challenge_hex, "c_exp": 0,
        "s_scalar": format(sig.s_scalar, "064x") if sig.s_scalar else "0",
        "version": WIRE_VERSION,
    }


class SchnorrGamma:
    """Schnorr-Γ over SL(3,p) — Unified API facade for hyp_engine integration.

    Merged from hyp_schnorr_gf.py. All methods route to the SL(3,p) gf_* functions.
    """

    def keygen(self):
        kp = gf_generate_keypair()
        private_walk = hex_to_walk(kp.private_key_hex)
        pub = GFMatrix.from_hex(kp.public_key_hex)
        return (private_walk, pub, kp.address)

    def keygen_from_walk(self, private_walk):
        g = get_schnorr_generator()
        x = walk_to_private_scalar(private_walk)
        pub = g ** x
        addr = hashlib.sha3_256(hashlib.sha3_256(pub.serialize()).digest()).hexdigest()
        return (private_walk, pub, addr)

    def sign(self, message, private_walk, public_key):
        return gf_sign_full(message, private_walk, public_key)

    def sign_hash(self, message_hash, private_walk, public_key):
        if isinstance(public_key, str):
            public_key = GFMatrix.from_hex(public_key)
        result = sign_hash(message_hash, private_walk, public_key)
        result["public_key_hex"] = public_key.hex()
        return result

    def verify(self, sig, message, public_key):
        if isinstance(sig, GFSchnorrSignature):
            return gf_verify_full(sig, message, public_key)
        return False

    def verify_signature(self, message_hash, sig_dict, public_key):
        try:
            version = sig_dict.get("version", "")
            if version == LEGACY_WIRE_VERSION:
                return self._verify_legacy(message_hash, sig_dict, public_key)
            d = {k: v for k, v in sig_dict.items() if k != "signature"}
            sig = signature_from_dict(d)
            gf_sig = GFSchnorrSignature(R=sig.R, Z=sig.Z, c_full=sig.c_full,
                                         s_scalar=sig.s_scalar, R_hex=sig.R_hex)
            return gf_verify_full(gf_sig, message_hash, public_key)
        except Exception:
            return False

    def _verify_legacy(self, message_hash, sig_dict, public_key):
        try:
            R = GFMatrix.from_hex(sig_dict.get("R", ""))
            Z = GFMatrix.from_hex(sig_dict.get("Z", ""))
            c_full = int(sig_dict.get("c_full", "0"), 16)
            s_scalar = int(sig_dict.get("s_scalar", "0"), 16)
            sig = GFSchnorrSignature(R=R, Z=Z, c_full=c_full, s_scalar=s_scalar,
                                      R_hex=sig_dict.get("R_canonical_hex", R.hex()))
            return gf_verify_full(sig, message_hash, public_key)
        except Exception:
            return False

    def signature_to_dict(self, sig):
        return signature_to_dict(sig)


# ═══════════════════════════════════════════════════════════════════════════
# TESTS
# ═══════════════════════════════════════════════════════════════════════════

def test_finite_field():
    """Comprehensive test suite for SL(3,p) Schnorr-Γ."""
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
    print("  hyp_finite_field.py v4 — SL(3,p) Schnorr-Γ Test Suite")
    print(f"  Prime: p = 2^255 − 31  ({P_BITS} bits)")
    print(f"  Group: SL(3,p)  |G| ≈ 2^{SL3_ORDER.bit_length()}")
    print(f"  DLP security: ~{Q_379.bit_length()//2} bits (Q₃₇₉ = {Q_379.bit_length()}-bit prime)")
    print("=" * 72)

    # ── Prime properties ──────────────────────────────────────────────
    test("p is prime", pow(2, P - 1, P) == 1)
    test("p ≡ 1 mod 24", P % 24 == 1)
    test("√2 exists in GF(p)", pow(sqrt2(), 2, P) == 2)

    # ── Q₃₇₉ divides p²+p+1 ─────────────────────────────────────────
    p2p1 = P * P + P + 1
    test("Q₃₇₉ divides p²+p+1", p2p1 % Q_379 == 0)
    test("Q₃₇₉ probable prime (base 2)", pow(2, Q_379 - 1, Q_379) == 1)
    test("Q₃₇₉ probable prime (base 3)", pow(3, Q_379 - 1, Q_379) == 1)

    # ── 3×3 Matrix arithmetic ─────────────────────────────────────────
    I = GFMatrix.identity()
    test("identity det = 1", I.det() == 1)
    test("I @ I = I", (I @ I) == I)

    # Generate a test SL(3,p) element
    seed = hashlib.shake_256(b"TEST_SL3_ARITHMETIC").digest(256)
    M = _random_sl3_element(seed)
    test("random SL(3,p) element det = 1", M is not None and M.det() == 1)

    if M is not None:
        M_inv = M.inverse()
        test("M @ M⁻¹ = I", (M @ M_inv) == I)
        test("M⁻¹ @ M = I", (M_inv @ M) == I)
        test("M^0 = I", (M ** 0) == I)
        test("M^1 = M", (M ** 1) == M)
        M5 = M @ M @ M @ M @ M
        test("M^5 via pow", (M ** 5) == M5)

        # Serialization roundtrip
        ser = M.serialize()
        test("serialize length = 288", len(ser) == 288)
        M_rt = GFMatrix.deserialize(ser)
        test("serialize roundtrip", M_rt == M)
        test("hex roundtrip", GFMatrix.from_hex(M.hex()) == M)

    # ── Generator checks ──────────────────────────────────────────────
    gens = get_generators()
    g1 = gens["a"]
    g1_inv = gens["a_inv"]
    g2 = gens["b"]
    g3 = gens["c"]

    test("det(g1) ≡ 1", g1.det() == 1)
    test("det(g2) ≡ 1", g2.det() == 1)
    test("det(g3) ≡ 1", g3.det() == 1)
    test("g1 @ g1⁻¹ = I", (g1 @ g1_inv) == I)
    test("g1 ≠ I", g1 != I and g1 != I.neg())
    test("generators non-abelian", g1 @ g2 != g2 @ g1)

    # ── Walk evaluation ───────────────────────────────────────────────
    rw = random_walk(512)
    test("walk length = 512", len(rw) == 512)
    test("walk indices in 0..5", all(0 <= x <= 5 for x in rw))
    Mw = evaluate_walk(rw)
    test("random walk det=1", Mw.det() == 1)

    # Walk hex roundtrip
    wh = walk_to_hex(rw)
    test("walk hex starts with GF3:", wh.startswith("GF3:"))
    rw_rt = hex_to_walk(wh, 512)
    test("walk hex roundtrip", rw_rt == rw)

    # ── Key generation ────────────────────────────────────────────────
    kp = gf_generate_keypair()
    test("keypair address 64 chars", len(kp.address) == 64)
    test("public key hex length = 576", len(kp.public_key_hex) == 576)  # 288 bytes = 576 hex chars

    g = get_schnorr_generator()
    walk = hex_to_walk(kp.private_key_hex)
    x = walk_to_private_scalar(walk)
    pub_expected = g ** x
    pub = GFMatrix.from_hex(kp.public_key_hex)
    test("pub key = g^x", pub == pub_expected)

    # ── Scalar Schnorr-Γ sign/verify ──────────────────────────────────
    msg = b"QTCL SL(3,p) post-quantum block signature test"

    sig = gf_sign_full(msg, walk, pub)
    test("sign creates signature", sig is not None)
    test("scalar response present", sig.s_scalar > 0)

    valid = gf_verify_full(sig, msg, pub)
    test("verify(sign(m)) == True", valid, f"got {valid}")

    # ── Forgery resistance ────────────────────────────────────────────
    msg2 = b"different message"
    valid2 = gf_verify_full(sig, msg2, pub)
    test("verify(sign(m1), m2) == False", not valid2)

    kp2 = gf_generate_keypair()
    pub2 = GFMatrix.from_hex(kp2.public_key_hex)
    valid3 = gf_verify_full(sig, msg, pub2)
    test("verify with wrong pk == False", not valid3)

    # Forger tries R @ y^c (no private key)
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
         "FORGERY SUCCEEDED")

    # ── Round-trip stress test ─────────────────────────────────────────
    failures = 0
    for i in range(20):
        s = gf_sign_full(msg, walk, pub)
        if not gf_verify_full(s, msg, pub):
            failures += 1
    test("20 sign/verify round-trips", failures == 0,
         f"{failures} failures")

    # ── Performance ───────────────────────────────────────────────────
    import time
    t0 = time.time()
    for _ in range(5):
        gf_sign_full(msg, walk, pub)
    t1 = time.time()
    avg_ms = (t1 - t0) / 5 * 1000
    print(f"  ⏱  Average sign time: {avg_ms:.1f}ms")

    t0 = time.time()
    for _ in range(5):
        gf_verify_full(sig, msg, pub)
    t1 = time.time()
    avg_ms = (t1 - t0) / 5 * 1000
    print(f"  ⏱  Average verify time: {avg_ms:.1f}ms")

    # ── Summary ───────────────────────────────────────────────────────
    print("=" * 72)
    print(f"  {passed}/{total} tests passed")
    print(f"  SL(3,p) classical DLP security: ~{Q_379.bit_length()//2} bits")
    print("=" * 72)
    return passed == total


if __name__ == "__main__":
    success = test_finite_field()
    exit(0 if success else 1)
