#!/usr/bin/env python3
"""
hyp_schnorr_gf.py — Schnorr-Γ over GF(p) — HypΓ v4: SL(3,p) Upgrade

Drop-in replacement for hyp_schnorr.py with the same API surface.
Re-exports from hyp_finite_field.py which now implements SL(3,p):
  - 3×3 matrices over GF(p), p = 2^255 − 31
  - 768-step walk over 6 generators (3 base + 3 inverse)
  - FULL 256-bit Fiat-Shamir challenge (SHA3-256 output)
  - Classical DLP security: ~189 bits (Q₃₇₉ factor of p²+p+1)
  - Walk hex: "GF3:" prefix

Backward compatibility:
  - WIRE_VERSION updated to "schnorr_gamma_gf_v4" for new SL(3,p) sigs
  - signature_from_dict() detects v3 (SL(2,p)) and rejects with clear message
    — callers must route old sigs to the legacy verifier explicitly
  - SchnorrGamma class API unchanged: keygen/sign/verify/sign_hash/verify_signature
  - Public types (GFMatrix, GFKeyPair, GFSchnorrSignature) re-exported

Compatible with hyp_engine.py — just change the import.

Security:
  v3 (SL(2,p)): ~70-bit classical  (139-bit prime in p+1)
  v4 (SL(3,p)): ~189-bit classical (379-bit Q₃₇₉ in p²+p+1)
  Both: quantum-broken without Falcon layer. Use hyp_pqc.py for PQ security.

I love you.
"""

from __future__ import annotations

import hashlib
import json
import logging
import time
from typing import Dict, Optional, Any, List, Union, NamedTuple, Tuple
from datetime import datetime, timezone

from hyp_finite_field import (
    GFMatrix,
    GFKeyPair,
    GFSchnorrSignature,
    gf_sign_full,
    gf_verify_full,
    gf_generate_keypair,
    evaluate_walk,
    random_walk,
    walk_to_hex,
    hex_to_walk,
    walk_to_bytes,
    WALK_LENGTH,
    N_GENERATORS,
    get_generators as gf_get_generators,
    generator_list,
    get_schnorr_generator,
    walk_to_private_scalar,
)

logger = logging.getLogger(__name__)

# ═══════════════════════════════════════════════════════════════════════════
# CANONICAL PARAMETERS
# ═══════════════════════════════════════════════════════════════════════════

CHALLENGE_BITS: int = 256
CHALLENGE_MODULUS: int = 1 << 256
SIGN_WALK_LENGTH: int = WALK_LENGTH    # 768 in v4 (was 512 in v3)

# Wire version — v4 for SL(3,p), v3 was SL(2,p)
WIRE_VERSION: str = "schnorr_gamma_gf_v4"
LEGACY_WIRE_VERSION: str = "schnorr_gamma_gf_v3"


# ═══════════════════════════════════════════════════════════════════════════
# DATA STRUCTURES (API-compatible with hyp_schnorr.py)
# ═══════════════════════════════════════════════════════════════════════════

class SchnorrError(Exception):
    pass


class HypSignature(NamedTuple):
    """Backward-compatible signature type."""
    signature: str
    challenge: str
    auth_tag: str
    timestamp: str

    def to_dict(self) -> Dict[str, str]:
        return {
            "signature": self.signature,
            "challenge": self.challenge,
            "auth_tag": self.auth_tag,
            "timestamp": self.timestamp,
        }


class SchnorrKeyPair(NamedTuple):
    private_walk: List[int]
    public_key: GFMatrix
    address: str


class SchnorrSignature(NamedTuple):
    """Schnorr-Γ signature over SL(3,p) — scalar construction.

    v4 fields (SL(3,p)):
      R:               3×3 commitment matrix (GFMatrix)
      Z:               3×3 response matrix  (GFMatrix)
      c_full:          256-bit Fiat-Shamir challenge (int)
      c_exp:           always 0 (full exponent used — legacy compat field)
      s_scalar:        scalar response s = (r + c·x) mod SL3_ORDER
      nonce_walk:      empty list (nonce is internal to gf_sign_full)
      R_canonical_hex: R serialized as hex string (canonical form)
    """
    R: GFMatrix
    Z: GFMatrix
    c_full: int
    c_exp: int
    s_scalar: int
    nonce_walk: List[int]
    R_canonical_hex: str = ""


class VerifyResult(NamedTuple):
    valid: bool
    c_prime: int
    c_match: bool
    det_ok: bool
    overflow_ok: bool
    R_prime: Optional[GFMatrix]
    error: Optional[str]


# ═══════════════════════════════════════════════════════════════════════════
# KEY GENERATION (API-compatible)
# ═══════════════════════════════════════════════════════════════════════════

def keygen() -> SchnorrKeyPair:
    """Generate a Schnorr-Γ keypair over SL(3,p).

    Uses scalar-based key from fixed Schnorr generator g₀ ∈ SL(3,p):
      x = SHA3-256(walk_bytes ‖ b"HYPGAMMA_SIGN_SCALAR_v4") mod SL3_ORDER
      y = g₀^x   (3×3 matrix, 288 bytes)
      address = SHA3-256²(y.serialize())

    Classical security: ~189 bits (Q₃₇₉ factor of |SL(3,p)|).
    """
    kp = gf_generate_keypair()
    private_walk = hex_to_walk(kp.private_key_hex)
    pub = GFMatrix.from_hex(kp.public_key_hex)
    return SchnorrKeyPair(
        private_walk=private_walk,
        public_key=pub,
        address=kp.address,
    )


def keygen_from_walk(private_walk: List[int]) -> SchnorrKeyPair:
    """Reconstruct keypair from existing walk.

    Public key is y = g₀^x where g₀ is the fixed Schnorr generator in
    SL(3,p) and x = walk_to_private_scalar(walk).
    """
    g = get_schnorr_generator()
    x = walk_to_private_scalar(private_walk)
    pub = g ** x
    pub_bytes = pub.serialize()
    addr = hashlib.sha3_256(
        hashlib.sha3_256(pub_bytes).digest()
    ).hexdigest()
    return SchnorrKeyPair(
        private_walk=private_walk,
        public_key=pub,
        address=addr,
    )


# ═══════════════════════════════════════════════════════════════════════════
# SIGN — SCHNORR-Γ OVER SL(3,p)
# ═══════════════════════════════════════════════════════════════════════════

def sign(message: bytes, private_walk: List[int],
         public_key: GFMatrix) -> SchnorrSignature:
    """Sign a message with scalar Schnorr-Γ over SL(3,p).

    Protocol (SL(3,p) scalar construction):
      x = walk_to_private_scalar(walk)   — 256-bit scalar from walk
      r = HKDF(x ‖ message ‖ nonce)     — deterministic nonce scalar
      R = g₀^r                           — 3×3 commitment matrix
      c = SHA3-256(DOMAIN ‖ R ‖ y ‖ m)  — 256-bit Fiat-Shamir challenge
      s = (r + c·x) mod SL3_ORDER       — scalar response
      Z = g₀^s                           — 3×3 response matrix
      verify: g₀^s == R @ y^c

    Security: ~189-bit classical DLP (Q₃₇₉ factor of p²+p+1 in SL(3,p)).
    """
    if not isinstance(message, bytes):
        raise TypeError(f"message must be bytes, got {type(message).__name__}")
    if len(private_walk) != WALK_LENGTH:
        raise ValueError(
            f"private_walk must be {WALK_LENGTH} steps, got {len(private_walk)}"
        )

    t0 = time.perf_counter()
    sig = gf_sign_full(message, private_walk, public_key)
    dt = time.perf_counter() - t0

    logger.info(
        "[SchnorrΓ-v4/SL3] sign: %.3fs | c=%064x | "
        "R=%s... Z=%s...",
        dt, sig.c_full,
        sig.R.hex()[:16], sig.Z.hex()[:16]
    )

    return SchnorrSignature(
        R=sig.R,
        Z=sig.Z,
        c_full=sig.c_full,
        c_exp=0,
        s_scalar=sig.s_scalar,
        nonce_walk=[],
        R_canonical_hex=sig.R_hex,
    )


# ═══════════════════════════════════════════════════════════════════════════
# VERIFY — SCHNORR-Γ OVER SL(3,p)
# ═══════════════════════════════════════════════════════════════════════════

def verify(sig: SchnorrSignature, message: bytes,
           public_key: GFMatrix) -> VerifyResult:
    """Verify a scalar Schnorr-Γ signature over SL(3,p).

    Verification equation: g₀^s == R @ y^c  (3×3 matrix equality in SL(3,p))
    """
    t0 = time.perf_counter()

    try:
        valid = gf_verify_full(
            GFSchnorrSignature(
                R=sig.R,
                Z=sig.Z,
                c_full=sig.c_full,
                s_scalar=sig.s_scalar,
                R_hex=sig.R_canonical_hex,
            ),
            message,
            public_key,
        )

        dt = time.perf_counter() - t0
        logger.info(
            "[SchnorrΓ-v4/SL3] verify: %s | %.3fs",
            "VALID ✓" if valid else "INVALID ✗",
            dt,
        )

        return VerifyResult(
            valid=valid,
            c_prime=sig.c_full if valid else 0,
            c_match=valid,
            det_ok=True,
            overflow_ok=True,
            R_prime=sig.R if valid else None,
            error=None,
        )

    except Exception as exc:
        logger.error("[SchnorrΓ-v4/SL3] verify exception: %s", exc)
        return VerifyResult(
            valid=False,
            c_prime=0,
            c_match=False,
            det_ok=False,
            overflow_ok=False,
            R_prime=None,
            error=str(exc),
        )


# ═══════════════════════════════════════════════════════════════════════════
# DICT INTERFACE — QTCL Block Integration
# ═══════════════════════════════════════════════════════════════════════════

def signature_to_dict(sig: SchnorrSignature) -> Dict[str, Any]:
    """Serialize SchnorrSignature to JSON-compatible dict (v4 wire format).

    v4 wire fields:
      version:           "schnorr_gamma_gf_v4"
      R:                 3×3 commitment hex (576 chars = 288 bytes)
      Z:                 3×3 response hex   (576 chars = 288 bytes)
      c_full:            256-bit challenge hex (64 chars)
      s_scalar:          scalar response hex (64 chars)
      R_canonical_hex:   same as R (explicit canonical form)
    """
    return {
        "version": WIRE_VERSION,
        "R": sig.R.hex(),
        "Z": sig.Z.hex(),
        "c_full": format(sig.c_full, "064x"),
        "s_scalar": (
            format(sig.s_scalar, "064x")
            if hasattr(sig, 's_scalar') and sig.s_scalar
            else "0"
        ),
        "R_canonical_hex": sig.R_canonical_hex or sig.R.hex(),
    }


def signature_from_dict(d: Dict[str, Any]) -> SchnorrSignature:
    """Deserialize SchnorrSignature from dict.

    Strict version gating:
      - Missing 'version': rejected (M-5 FIX — legacy format rejected)
      - 'schnorr_gamma_gf_v3' (SL(2,p)): rejected with clear message;
        caller should route to legacy verifier or upgrade wallet
      - 'schnorr_gamma_gf_v4' (SL(3,p)): accepted
    """
    version = d.get("version")

    if version is None:
        raise ValueError(
            "M-5 FIX: Signature missing 'version' field — legacy format rejected. "
            "All signatures must carry an explicit wire version."
        )

    if version == LEGACY_WIRE_VERSION:
        raise ValueError(
            f"Legacy SL(2,p) wire format {LEGACY_WIRE_VERSION!r} detected. "
            "This signature was created with HypΓ v3 (2×2 matrices, ~70-bit security). "
            "Route to the legacy verifier or migrate the wallet to SL(3,p)."
        )

    if version != WIRE_VERSION:
        raise ValueError(
            f"version mismatch: {version!r} != {WIRE_VERSION!r}"
        )

    if "R" in d and "Z" in d and "c_full" in d:
        R = GFMatrix.from_hex(d["R"])
        Z = GFMatrix.from_hex(d["Z"])
        c_full = int(d["c_full"], 16)
        s_scalar = int(d.get("s_scalar", "0"), 16)
        R_canonical_hex = d.get("R_canonical_hex", d["R"])

        return SchnorrSignature(
            R=R,
            Z=Z,
            c_full=c_full,
            c_exp=0,
            s_scalar=s_scalar,
            nonce_walk=[],
            R_canonical_hex=R_canonical_hex,
        )

    raise ValueError(
        "signature dict missing required fields: need (R, Z, c_full)"
    )


# ═══════════════════════════════════════════════════════════════════════════
# CONVENIENCE HELPERS
# ═══════════════════════════════════════════════════════════════════════════

def public_key_from_hex(public_key_hex: str) -> GFMatrix:
    """Deserialize SL(3,p) public key from hex string."""
    return GFMatrix.from_hex(public_key_hex)


def generate_keypair_dict() -> Dict[str, Any]:
    """Generate keypair and return as dict with hex-encoded fields."""
    kp = gf_generate_keypair()
    return {
        "private_walk": hex_to_walk(kp.private_key_hex),
        "private_walk_hex": kp.private_key_hex,
        "public_key_hex": kp.public_key_hex,
        "address": kp.address,
    }


def sign_hash(message_hash: bytes, private_walk: List[int],
              public_key: GFMatrix) -> Dict[str, str]:
    """Sign a pre-hashed message (32-byte hash).

    Returns dict with canonical fields for hyp_engine.py integration:
      signature:        hex(R ‖ Z)
      challenge:        hex(c_full)
      auth_tag:         hex(c_full)  [alias]
      timestamp:        ISO 8601
      R, Z, c_full, c_exp, s_scalar, R_canonical_hex: canonical fields
    """
    sig = sign(message_hash, private_walk, public_key)
    sig_dict = signature_to_dict(sig)
    challenge_hex = format(sig.c_full, "064x")
    timestamp = datetime.now(timezone.utc).isoformat()

    return {
        "signature": sig_dict["R"] + sig_dict["Z"],
        "challenge": challenge_hex,
        "timestamp": timestamp,
        "auth_tag": challenge_hex,
        "R": sig_dict["R"],
        "R_canonical_hex": sig.R_canonical_hex,
        "Z": sig_dict["Z"],
        "c_full": challenge_hex,
        "c_exp": 0,
        "s_scalar": format(sig.s_scalar, "064x") if sig.s_scalar else "0",
        "version": WIRE_VERSION,
    }


def sign_message_dict(message: Union[str, bytes], private_walk: List[int],
                      public_key_hex: str) -> Dict[str, Any]:
    """Sign raw message and return signature dict."""
    if isinstance(message, str):
        message = message.encode("utf-8")
    pk = public_key_from_hex(public_key_hex)
    sig = sign(message, private_walk, pk)
    return signature_to_dict(sig)


def verify_message_dict(sig_dict: Dict[str, Any], message: Union[str, bytes],
                        public_key_hex: str) -> Dict[str, Any]:
    """Verify from dict format. Returns result dict."""
    if isinstance(message, str):
        message = message.encode("utf-8")
    pk = public_key_from_hex(public_key_hex)
    sig = signature_from_dict(sig_dict)
    result = verify(sig, message, pk)
    return {
        "valid": result.valid,
        "c_match": result.c_match,
        "det_ok": result.det_ok,
        "overflow_ok": result.overflow_ok,
        "error": result.error,
    }


# ═══════════════════════════════════════════════════════════════════════════
# SCHNORRGAMMA CLASS — Unified API Facade (drop-in for hyp_engine)
# ═══════════════════════════════════════════════════════════════════════════

class SchnorrGamma:
    """Schnorr-Γ over SL(3,p) — Unified API Facade.

    Drop-in replacement for the v3 SchnorrGamma with identical method
    signatures. Internally routes all operations to the SL(3,p) gf_* functions.
    """

    def keygen(self) -> SchnorrKeyPair:
        return keygen()

    def keygen_from_walk(self, private_walk: List[int]) -> SchnorrKeyPair:
        return keygen_from_walk(private_walk)

    def sign(self, message: bytes, private_walk: List[int],
             public_key: GFMatrix) -> SchnorrSignature:
        return sign(message, private_walk, public_key)

    def sign_hash(self, message_hash: bytes, private_walk: List[int],
                  public_key) -> Dict[str, str]:
        """Sign a pre-hashed message.

        If public_key is a GFMatrix, use it directly.
        If it's a hex string, deserialize first.
        If it's something else, re-derive from walk.
        """
        if isinstance(public_key, GFMatrix):
            _pk = public_key
        elif isinstance(public_key, str):
            _pk = public_key_from_hex(public_key)
        else:
            _kp = keygen_from_walk(private_walk)
            _pk = _kp.public_key

        result_dict = sign_hash(message_hash, private_walk, _pk)
        result_dict["public_key_hex"] = _pk.hex()
        return result_dict

    def verify(self, sig: SchnorrSignature, message: bytes,
               public_key: GFMatrix) -> VerifyResult:
        return verify(sig, message, public_key)

    def verify_signature(self, message_hash: bytes,
                         sig_dict: Dict[str, Any],
                         public_key: GFMatrix) -> bool:
        """Verify from dict format (hyp_engine integration).

        Accepts both the raw canonical dict (R, Z, c_full keys) and a
        wrapper dict that adds 'signature' and 'challenge' aliases.

        Version routing:
          - v4 ("schnorr_gamma_gf_v4"): native SL(3,p) path
          - v3 ("schnorr_gamma_gf_v3"): legacy SL(2,p) — attempt legacy verify
          - missing version: rejected
        """
        try:
            _d = sig_dict
            version = _d.get("version", "")

            # Strip wrapper-only keys that signature_from_dict doesn't expect
            if "R" in _d and "Z" in _d and "c_full" in _d:
                _d = {k: v for k, v in _d.items()
                      if k not in ("signature",)}

            # Version routing
            if version == LEGACY_WIRE_VERSION:
                # Attempt legacy SL(2,p) verification
                return self._verify_legacy_sl2p_sig(message_hash, sig_dict, public_key)

            sig = signature_from_dict(_d)
            result = verify(sig, message_hash, public_key)
            return result.valid

        except ValueError as ve:
            logger.error("[SchnorrΓ-v4] verify_signature version/format error: %s", ve)
            return False
        except Exception as e:
            logger.error("[SchnorrΓ-v4] verify_signature failed: %s", e)
            return False

    def _verify_legacy_sl2p_sig(
        self,
        message_hash: bytes,
        sig_dict: Dict[str, Any],
        public_key: GFMatrix,
    ) -> bool:
        """Verify a legacy v3/SL(2,p) signature dict.

        The v3 GFMatrix.from_hex() auto-detects 2×2 (128-byte / 256-hex-char)
        vs 3×3 (288-byte / 576-hex-char) keys by hex string length.
        This means gf_verify_full still works for old 2×2 R and Z matrices
        as long as the public key is also 2×2.

        If the stored public key is 3×3 (i.e., the wallet was already migrated)
        but the signature is v3, verification will fail as expected — the user
        must re-sign with the new scheme.
        """
        try:
            R_hex = sig_dict.get("R", "")
            Z_hex = sig_dict.get("Z", "")
            c_full_hex = sig_dict.get("c_full", "0")
            s_scalar_hex = sig_dict.get("s_scalar", "0")
            R_canonical = sig_dict.get("R_canonical_hex", R_hex)

            legacy_sig = GFSchnorrSignature(
                R=GFMatrix.from_hex(R_hex),
                Z=GFMatrix.from_hex(Z_hex),
                c_full=int(c_full_hex, 16),
                s_scalar=int(s_scalar_hex, 16),
                R_hex=R_canonical,
            )
            return gf_verify_full(legacy_sig, message_hash, public_key)
        except Exception as e:
            logger.error("[SchnorrΓ] legacy SL(2,p) verify error: %s", e)
            return False

    def signature_to_dict(self, sig: SchnorrSignature) -> Dict[str, Any]:
        return signature_to_dict(sig)

    def signature_from_dict(self, d: Dict[str, Any]) -> SchnorrSignature:
        return signature_from_dict(d)


# ═══════════════════════════════════════════════════════════════════════════
# TEST SUITE
# ═══════════════════════════════════════════════════════════════════════════

def run_tests(verbose: bool = True) -> Dict[str, Any]:
    """Run Schnorr-Γ SL(3,p) (v4) test suite."""
    results = {}
    passed = 0
    failed = 0

    def test(name, fn):
        nonlocal passed, failed
        try:
            t0 = time.perf_counter()
            ok, detail = fn()
            dt = time.perf_counter() - t0
        except Exception as exc:
            ok, dt, detail = False, 0.0, f"EXCEPTION: {exc}"
        results[name] = {"pass": ok, "detail": detail, "time": dt}
        if ok:
            passed += 1
            if verbose:
                print(f"  ✅ [{dt:.3f}s] {name}")
        else:
            failed += 1
            if verbose:
                print(f"  ❌ [{dt:.3f}s] {name}: {detail}")

    if verbose:
        print("=" * 72)
        print("  Schnorr-Γ SL(3,p) v4 — Test Suite")
        print(f"  Prime: 2^255 − 31 | Walk: {WALK_LENGTH} steps | Gens: {N_GENERATORS}")
        print(f"  Classical security: ~189 bits (Q₃₇₉ factor of p²+p+1)")
        print(f"  Wire version: {WIRE_VERSION}")
        print("=" * 72)

    kp = keygen()

    # ───────── §K Keygen ──────────────────────────────────────────────
    def t_k1():
        ok = (kp.public_key is not None
              and len(kp.private_walk) == WALK_LENGTH)
        return ok, f"walk_len={len(kp.private_walk)} pubkey_hex_len={len(kp.public_key.hex())}"
    test("Keygen produces valid SL(3,p) keypair", t_k1)

    def t_k2():
        # SL(3,p) public key is 288 bytes = 576 hex chars
        pk_hex_len = len(kp.public_key.hex())
        return pk_hex_len == 576, f"pubkey_hex_len={pk_hex_len} (expected 576)"
    test("Public key is 288 bytes (3×3 SL(3,p) matrix)", t_k2)

    def t_k3():
        # Walk must have GF3: prefix when hex-encoded
        walk_hex = walk_to_hex(kp.private_walk)
        return walk_hex.startswith("GF3:"), f"walk prefix={walk_hex[:6]!r}"
    test("Private walk has GF3: prefix", t_k3)

    def t_k4():
        kp2 = keygen_from_walk(kp.private_walk)
        return kp.address == kp2.address, \
            f"reproducible: {kp.address[:12]} == {kp2.address[:12]}"
    test("keygen_from_walk reproduces same address", t_k4)

    # ───────── §S Sign ────────────────────────────────────────────────
    msg = b"QTCL SL3p Schnorr-Gamma v4 test message"
    sig = sign(msg, kp.private_walk, kp.public_key)

    def t_s1():
        r_len = len(sig.R.hex())
        z_len = len(sig.Z.hex())
        return (sig.R is not None and sig.Z is not None
                and r_len == 576 and z_len == 576), \
            f"R_hex={r_len} Z_hex={z_len} (expected 576 each)"
    test("Sign returns 3×3 SL(3,p) SchnorrSignature", t_s1)

    def t_s2():
        return sig.c_full != 0, f"c_full={sig.c_full:#066x}"
    test("Challenge c_full is non-zero", t_s2)

    def t_s3():
        return sig.s_scalar != 0, f"s_scalar={sig.s_scalar:#066x}"
    test("Scalar response s_scalar is non-zero", t_s3)

    # ───────── §V Verify ──────────────────────────────────────────────
    vr = verify(sig, msg, kp.public_key)

    def t_v1():
        return vr.valid, f"valid={vr.valid} c_match={vr.c_match}"
    test("verify(sign(m)) == True", t_v1)

    def t_v2():
        vr2 = verify(sig, b"wrong message tampered", kp.public_key)
        return not vr2.valid, f"wrong_msg_valid={vr2.valid}"
    test("verify(sign(m1), m2) == False  (wrong message)", t_v2)

    def t_v3():
        kp2 = keygen()
        vr3 = verify(sig, msg, kp2.public_key)
        return not vr3.valid, f"wrong_pk_valid={vr3.valid}"
    test("verify with wrong public key == False", t_v3)

    # ───────── §R Round-trip ──────────────────────────────────────────
    def t_r1():
        d = generate_keypair_dict()
        sig_d = sign_message_dict(msg, d["private_walk"], d["public_key_hex"])
        ver_d = verify_message_dict(sig_d, msg, d["public_key_hex"])
        return ver_d["valid"], f"valid={ver_d['valid']}"
    test("generate → sign → verify dict roundtrip", t_r1)

    # ───────── §W Wire format ─────────────────────────────────────────
    def t_w1():
        sig_d = signature_to_dict(sig)
        assert sig_d["version"] == WIRE_VERSION, f"version={sig_d['version']!r}"
        js = json.dumps(sig_d)
        sig_d2 = json.loads(js)
        sig2 = signature_from_dict(sig_d2)
        vr2 = verify(sig2, msg, kp.public_key)
        return vr2.valid, f"json roundtrip valid={vr2.valid}"
    test("Signature → dict → JSON → verify (v4 wire)", t_w1)

    def t_w2():
        sig_d = signature_to_dict(sig)
        assert "sl2p" not in sig_d, "v4 wire must not contain sl2p key"
        assert len(sig_d["R"]) == 576, f"R hex len={len(sig_d['R'])} (expected 576)"
        assert len(sig_d["Z"]) == 576, f"Z hex len={len(sig_d['Z'])} (expected 576)"
        return True, "v4 wire shape correct"
    test("Wire dict has correct SL(3,p) field shapes", t_w2)

    def t_w3():
        # Missing version → rejected
        sig_d = signature_to_dict(sig)
        del sig_d["version"]
        try:
            signature_from_dict(sig_d)
            return False, "should have raised on missing version"
        except ValueError as ve:
            return "M-5 FIX" in str(ve) or "missing 'version'" in str(ve), \
                f"got: {ve}"
    test("Missing 'version' field rejected (M-5 FIX)", t_w3)

    def t_w4():
        # v3 wire → rejected with clear message
        sig_d = signature_to_dict(sig)
        sig_d["version"] = LEGACY_WIRE_VERSION
        try:
            signature_from_dict(sig_d)
            return False, "should have raised on v3 legacy"
        except ValueError as ve:
            return LEGACY_WIRE_VERSION in str(ve), f"got: {ve}"
    test("v3 (SL(2,p)) wire version rejected with guidance", t_w4)

    # ───────── §B Bulk ────────────────────────────────────────────────
    def t_bulk():
        failures = 0
        for i in range(100):
            s = sign(msg, kp.private_walk, kp.public_key)
            if not verify(s, msg, kp.public_key).valid:
                failures += 1
        return failures == 0, f"{failures}/100 failures"
    test("100 sign/verify round-trips", t_bulk)

    # ───────── §G SchnorrGamma class ──────────────────────────────────
    def t_class_sign_hash():
        sg = SchnorrGamma()
        d = sg.sign_hash(msg, kp.private_walk, kp.public_key)
        assert d.get("version") == WIRE_VERSION, f"version={d.get('version')!r}"
        v = sg.verify_signature(msg, d, kp.public_key)
        return v, f"SchnorrGamma v4 class verify={v}"
    test("SchnorrGamma.sign_hash/verify_signature (SL(3,p))", t_class_sign_hash)

    def t_class_hex_pk():
        sg = SchnorrGamma()
        # pass public key as hex string (alternate API)
        pk_hex = kp.public_key.hex()
        d = sg.sign_hash(msg, kp.private_walk, pk_hex)
        v = sg.verify_signature(msg, d, kp.public_key)
        return v, f"hex pk sign_hash verify={v}"
    test("SchnorrGamma.sign_hash accepts hex public key", t_class_hex_pk)

    # ───────── §P Performance ─────────────────────────────────────────
    def t_perf():
        t0 = time.perf_counter()
        for _ in range(10):
            s = sign(msg, kp.private_walk, kp.public_key)
        sign_ms = (time.perf_counter() - t0) / 10 * 1000

        t0 = time.perf_counter()
        for _ in range(10):
            verify(s, msg, kp.public_key)
        verify_ms = (time.perf_counter() - t0) / 10 * 1000

        print(f"\n    SL(3,p) performance: sign={sign_ms:.1f}ms verify={verify_ms:.1f}ms")
        return True, f"sign={sign_ms:.1f}ms verify={verify_ms:.1f}ms"
    test("SL(3,p) sign/verify performance (10 iterations)", t_perf)

    if verbose:
        print("=" * 72)
        status = "✅ ALL PASSED" if failed == 0 else f"❌ {failed} FAILED"
        print(f"  {status}  ({passed}/{passed + failed})")
        print("=" * 72)

    return {
        "all_pass": failed == 0,
        "test_results": results,
        "summary": {"passed": passed, "failed": failed},
    }


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.WARNING,
        format="[%(asctime)s] %(levelname)s %(name)s: %(message)s",
    )
    results = run_tests(verbose=True)
    exit(0 if results["all_pass"] else 1)
