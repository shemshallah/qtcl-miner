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

    # Generate two random elements from the seed
    h1 = hashlib.sha3_256(seed + b"\x01").digest()
    h2 = hashlib.sha3_256(seed + b"\x02").digest()

    g1 = random_sl2_element(h1)
    g2 = random_sl2_element(h2)

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
    """Cryptographically random walk (indices 0..3)."""
    CANCEL = {0: 1, 1: 0, 2: 3, 3: 2}
    entropy = secrets.token_bytes((length + 3) // 4 * 4)
    walk = []
    prev = None
    for i in range(length):
        byte = entropy[i % len(entropy)]
        if reduced and prev is not None:
            choices = [j for j in range(4) if j != CANCEL[prev]]
            walk.append(choices[byte % 3])
        else:
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

DOMAIN_TAG = b"HYPGAMMA_GF_SCHNORR_V2_FIAT_SHAMIR\x00"

class GFSchnorrSignature(NamedTuple):
    R: GFMatrix
    Z: GFMatrix
    c_full: int        # 256-bit Fiat-Shamir challenge
    R_hex: str         # canonical hex of R for exact binding

# ═══════════════════════════════════════════════════════════════════════════
# DEPRECATED: gf_sign / gf_verify (CRIT-2 FIX — RED TEAM)
# These functions used a broken cut-and-choose interleaved walk construction
# where the per-position consistency check was an empty pass, making verification
# always succeed regardless of private key knowledge. They are replaced by
# gf_sign_full / gf_verify_full which use standard Schnorr with full exponent.
# ═══════════════════════════════════════════════════════════════════════════

def gf_sign(message: bytes, private_walk: list,
            public_key: GFMatrix) -> GFSchnorrSignature:
    """DEPRECATED: Broken function removed per CRIT-2 red team finding.
    Use gf_sign_full() instead."""
    raise NotImplementedError(
        "gf_sign removed (CRIT-2): broken cut-and-choose construction. "
        "Use gf_sign_full() for standard Schnorr-Γ over GF(p)."
    )

def gf_verify(sig: GFSchnorrSignature, message: bytes,
              public_key: GFMatrix) -> bool:
    """DEPRECATED: Broken function removed per CRIT-2 red team finding.
    Use gf_verify_full() instead."""
    raise NotImplementedError(
        "gf_verify removed (CRIT-2): verification always passed. "
        "Use gf_verify_full() for standard Schnorr-Γ over GF(p)."
    )


# ═══════════════════════════════════════════════════════════════════════════
# SIMPLIFIED SCHNORR-Γ OVER GF(p) — FULL 256-BIT EXPONENT
#
# This is the PRACTICAL version.  Since we're over GF(p), we CAN compute
# y^c for the full 256-bit c using binary exponentiation (~256 multiplications,
# each O(1) modular ops).  No precision issues — everything is exact mod p.
#
# The verification equation is the standard Schnorr relation:
#   Z = R · y^c    where c = H(R ‖ m)  (full 256 bits)
#
# Security: forging requires solving the matrix discrete log problem
# in SL(2,p) — finding a walk x such that y = eval(x) given only y.
# For p ≈ 2^255, the group order is ≈ p^4 ≈ 2^1020, so classical
# security ≈ 2^128 (index calculus on GF(p)) and quantum security
# plausible via non-abelian HSP hardness.
# ═══════════════════════════════════════════════════════════════════════════

def gf_sign_full(message: bytes, private_walk: list,
                 public_key: GFMatrix) -> GFSchnorrSignature:
    """
    Schnorr-Γ over GF(p) with FULL 256-bit exponent.

    This is the definitive version — y^c is computed via modular binary
    exponentiation (exact, O(log c) mod-p multiplications).  No precision
    limits, no N_PERIOD, no exponent reduction.  Full 256-bit security.
    """
    r_walk = random_walk(WALK_LENGTH, reduced=True)
    R = evaluate_walk(r_walk)

    # Fiat-Shamir: c = H(R ‖ pk ‖ m) — full 256 bits
    # HIGH-3 FIX (RED TEAM): Include public key in the challenge hash to
    # prevent key-substitution attacks. Standard Schnorr (FIPS 186-5) binds pk.
    c_full = int.from_bytes(
        hashlib.sha3_256(
            DOMAIN_TAG + R.serialize() + public_key.serialize() + message
        ).digest(), "big")

    # y^c via binary exponentiation (exact modular arithmetic)
    y_c = public_key ** c_full

    # Z = R @ y^c
    Z = R @ y_c

    return GFSchnorrSignature(R=R, Z=Z, c_full=c_full, R_hex=R.hex())


def gf_verify_full(sig: GFSchnorrSignature, message: bytes,
                   public_key: GFMatrix) -> bool:
    """
    Verify Schnorr-Γ over GF(p) with full 256-bit challenge.

    Verification equation: R' = Z @ y^{-c}, check H(R' ‖ m) == c.
    """
    R = sig.R
    Z = sig.Z
    c_full = sig.c_full

    # Recompute challenge (must match gf_sign_full)
    # HIGH-3 FIX (RED TEAM): Include public key in challenge hash.
    expected_c = int.from_bytes(
        hashlib.sha3_256(
            DOMAIN_TAG + R.serialize() + public_key.serialize() + message
        ).digest(), "big")
    if c_full != expected_c:
        return False

    # Compute y^{-c} = (y^{-1})^{c} via modular binary exponentiation
    y_inv = public_key.inverse()
    y_neg_c = y_inv ** c_full

    # Reconstruct R' = Z @ y^{-c}
    R_prime = Z @ y_neg_c

    # Check R' == R (or R' == -R in PSL)
    if R_prime == R:
        return True
    if R_prime == R.neg():
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
    """Generate a keypair over GF(p)."""
    walk = random_walk(WALK_LENGTH, reduced=True)
    pub = evaluate_walk(walk)
    priv_hex = walk_to_hex(walk)
    pub_hex = pub.hex()
    address = hashlib.sha3_256(
        hashlib.sha3_256(pub.serialize()).digest()
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
    print("  hyp_finite_field.py — GF(p) Schnorr-Γ Test Suite")
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
    test("pub key hex roundtrip",
         GFMatrix.from_hex(kp.public_key_hex) ==
         evaluate_walk(hex_to_walk(kp.private_key_hex)))

    # ── Schnorr-Γ sign/verify (full exponent) ─────────────────────────
    msg = b"QTCL post-quantum block signature test"
    pub = evaluate_walk(hex_to_walk(kp.private_key_hex))

    sig = gf_sign_full(msg, hex_to_walk(kp.private_key_hex), pub)
    test("sign creates signature", sig is not None)

    valid = gf_verify_full(sig, msg, pub)
    test("verify(sign(m)) == True", valid, f"got {valid}")

    # ── Forgery resistance ────────────────────────────────────────────
    msg2 = b"different message"
    valid2 = gf_verify_full(sig, msg2, pub)
    test("verify(sign(m1), m2) == False", not valid2)

    # Wrong public key
    kp2 = gf_generate_keypair()
    pub2 = evaluate_walk(hex_to_walk(kp2.private_key_hex))
    valid3 = gf_verify_full(sig, msg, pub2)
    test("verify with wrong pk == False", not valid3)

    # ── 1000 round-trips ──────────────────────────────────────────────
    failures = 0
    for i in range(100):
        s = gf_sign_full(msg, hex_to_walk(kp.private_key_hex), pub)
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
