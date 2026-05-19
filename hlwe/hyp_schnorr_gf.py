#!/usr/bin/env python3
"""
hyp_schnorr_gf.py — Schnorr-Γ over GF(p) — Post-Quantum Signature Module

Drop-in replacement for hyp_schnorr.py with the same API.  Uses the
finite-field implementation (hyp_finite_field.py) for exact modular
matrix exponentiation with the FULL 256-bit Fiat-Shamir challenge.

Compatible with hyp_engine.py — just change the import.
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
    WALK_LENGTH,
    N_GENERATORS,
    get_generators as gf_get_generators,
    generator_list,
)

logger = logging.getLogger(__name__)

# ═══════════════════════════════════════════════════════════════════════════
# CANONICAL PARAMETERS
# ═══════════════════════════════════════════════════════════════════════════

CHALLENGE_BITS: int = 256
CHALLENGE_MODULUS: int = 1 << 256
SIGN_WALK_LENGTH: int = WALK_LENGTH

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
    """Schnorr-Γ signature over GF(p)."""
    R: GFMatrix
    Z: GFMatrix
    c_full: int
    c_exp: int           # always 0 in GF(p) version (full exponent used)
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
    """Generate a Schnorr-Γ keypair over GF(p)."""
    kp = gf_generate_keypair()
    private_walk = hex_to_walk(kp.private_key_hex)
    pub = evaluate_walk(private_walk)
    return SchnorrKeyPair(private_walk=private_walk, public_key=pub,
                          address=kp.address)


def keygen_from_walk(private_walk: List[int]) -> SchnorrKeyPair:
    """Reconstruct keypair from existing walk."""
    pub = evaluate_walk(private_walk)
    pub_bytes = pub.serialize()
    addr = hashlib.sha3_256(
        hashlib.sha3_256(pub_bytes).digest()
    ).hexdigest()
    return SchnorrKeyPair(private_walk=private_walk, public_key=pub,
                          address=addr)


# ═══════════════════════════════════════════════════════════════════════════
# SIGN — SCHNORR-Γ OVER GF(p)
# ═══════════════════════════════════════════════════════════════════════════

def sign(message: bytes, private_walk: List[int],
         public_key: GFMatrix) -> SchnorrSignature:
    """
    Sign a message with Schnorr-Γ over GF(p).

    Protocol:
        r_walk = random_walk(512)              — fresh nonce
        R = evaluate_walk(r_walk)              — commitment
        c = H(R ‖ m)                            — Fiat-Shamir (256-bit)
        y_c = public_key ** c                   — exact via binary exponentiation
        Z = R @ y_c                             — response
        σ = (R, Z, c)

    Uses the FULL 256-bit challenge — no exponent reduction needed because
    matrix exponentiation over GF(p) is exact and O(log c) cost.
    """
    if not isinstance(message, bytes):
        raise TypeError(f"message must be bytes, got {type(message).__name__}")
    if len(private_walk) != WALK_LENGTH:
        raise ValueError(f"private_walk must be {WALK_LENGTH} steps, got {len(private_walk)}")

    t0 = time.perf_counter()

    sig = gf_sign_full(message, private_walk, public_key)

    dt = time.perf_counter() - t0
    logger.info("[SchnorrΓ-GF] sign: done in %.3fs | c_full=%064x", dt, sig.c_full)

    return SchnorrSignature(
        R=sig.R, Z=sig.Z, c_full=sig.c_full, c_exp=0,
        nonce_walk=[],  # not needed for wire format
        R_canonical_hex=sig.R_hex)


# ═══════════════════════════════════════════════════════════════════════════
# VERIFY — SCHNORR-Γ OVER GF(p)
# ═══════════════════════════════════════════════════════════════════════════

def verify(sig: SchnorrSignature, message: bytes,
           public_key: GFMatrix) -> VerifyResult:
    """Verify a Schnorr-Γ signature over GF(p)."""
    t0 = time.perf_counter()

    try:
        valid = gf_verify_full(
            GFSchnorrSignature(R=sig.R, Z=sig.Z, c_full=sig.c_full,
                              R_hex=sig.R_canonical_hex),
            message, public_key)

        dt = time.perf_counter() - t0
        logger.info("[SchnorrΓ-GF] verify: %s | %.3fs",
                     "VALID ✓" if valid else "INVALID ✗", dt)

        return VerifyResult(
            valid=valid, c_prime=sig.c_full if valid else 0,
            c_match=valid, det_ok=True, overflow_ok=True,
            R_prime=sig.R if valid else None, error=None)

    except Exception as exc:
        logger.error("[SchnorrΓ-GF] verify: exception: %s", exc)
        return VerifyResult(valid=False, c_prime=0, c_match=False,
                           det_ok=False, overflow_ok=False,
                           R_prime=None, error=str(exc))


# ═══════════════════════════════════════════════════════════════════════════
# DICT INTERFACE — QTCL Block Integration
# ═══════════════════════════════════════════════════════════════════════════

WIRE_VERSION: str = "schnorr_gamma_gf_v1"


def signature_to_dict(sig: SchnorrSignature) -> Dict[str, Any]:
    """Serialize signature to JSON-compatible dict."""
    return {
        "version": WIRE_VERSION,
        "R": sig.R.hex(),
        "Z": sig.Z.hex(),
        "c_full": format(sig.c_full, "064x"),
        "R_canonical_hex": sig.R_canonical_hex or sig.R.hex(),
    }


def signature_from_dict(d: Dict[str, Any]) -> SchnorrSignature:
    """Deserialize signature from dict."""
    version = d.get("version")
    if version is not None and version != WIRE_VERSION:
        raise ValueError(f"version mismatch: {version!r} != {WIRE_VERSION!r}")

    if "R" in d and "Z" in d and "c_full" in d:
        R = GFMatrix.from_hex(d["R"])
        Z = GFMatrix.from_hex(d["Z"])
        c_full = int(d["c_full"], 16)
        r_hex = d.get("R_canonical_hex", d["R"])
        return SchnorrSignature(R=R, Z=Z, c_full=c_full, c_exp=0,
                                nonce_walk=[], R_canonical_hex=r_hex)

    raise ValueError(f"Unrecognized signature format. Keys: {list(d.keys())}")


def public_key_to_hex(pk: GFMatrix) -> str:
    return pk.hex()


def public_key_from_hex(hex_str: str) -> GFMatrix:
    return GFMatrix.from_hex(hex_str)


# ═══════════════════════════════════════════════════════════════════════════
# MODULE-LEVEL CONVENIENCE (API-compatible)
# ═══════════════════════════════════════════════════════════════════════════

def generate_keypair_dict() -> Dict[str, Any]:
    kp = keygen()
    return {
        "private_walk": kp.private_walk,
        "public_key_hex": public_key_to_hex(kp.public_key),
        "address": kp.address,
        "walk_length": WALK_LENGTH,
    }


def sign_hash(message_hash: bytes, private_walk: List[int],
              public_key: GFMatrix) -> Dict[str, str]:
    """Sign a pre-hashed message (32-byte hash)."""
    sig = sign(message_hash, private_walk, public_key)
    sig_dict = signature_to_dict(sig)
    challenge_hex = format(sig.c_full, "064x")
    timestamp = datetime.now(timezone.utc).isoformat()

    return {
        "signature": sig_dict["R"],
        "challenge": challenge_hex,
        "timestamp": timestamp,
        "auth_tag": challenge_hex,
        "R": sig_dict["R"],
        "R_canonical_hex": sig.R_canonical_hex,
        "Z": sig_dict["Z"],
        "c_full": challenge_hex,
        "c_exp": 0,
    }


def sign_message_dict(message: Union[str, bytes], private_walk: List[int],
                      public_key_hex: str) -> Dict[str, Any]:
    if isinstance(message, str):
        message = message.encode("utf-8")
    pk = public_key_from_hex(public_key_hex)
    sig = sign(message, private_walk, pk)
    return signature_to_dict(sig)


def verify_message_dict(sig_dict: Dict[str, Any], message: Union[str, bytes],
                        public_key_hex: str) -> Dict[str, Any]:
    if isinstance(message, str):
        message = message.encode("utf-8")
    pk = public_key_from_hex(public_key_hex)
    sig = signature_from_dict(sig_dict)
    result = verify(sig, message, pk)
    return {"valid": result.valid, "c_match": result.c_match,
            "det_ok": result.det_ok, "overflow_ok": result.overflow_ok,
            "error": result.error}


# ═══════════════════════════════════════════════════════════════════════════
# SCHNORRGAMMA CLASS — Unified API Facade (drop-in for hyp_engine)
# ═══════════════════════════════════════════════════════════════════════════

class SchnorrGamma:
    """Schnorr-Γ over GF(p) — Unified API Facade."""

    def keygen(self) -> SchnorrKeyPair:
        return keygen()

    def keygen_from_walk(self, private_walk: List[int]) -> SchnorrKeyPair:
        return keygen_from_walk(private_walk)

    def sign(self, message: bytes, private_walk: List[int],
             public_key: GFMatrix) -> SchnorrSignature:
        return sign(message, private_walk, public_key)

    def sign_hash(self, message_hash: bytes, private_walk: List[int],
                  public_key) -> Dict[str, str]:
        """Sign a pre-hashed message. If public_key is a GFMatrix, use it
        directly. Otherwise derive from walk."""
        if isinstance(public_key, GFMatrix):
            _pk = public_key
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
        """Verify from dict format (hyp_engine integration)."""
        try:
            _d = sig_dict
            if "R" in _d and "Z" in _d and "c_full" in _d:
                _d = {k: v for k, v in _d.items() if k != "signature"}
            sig = signature_from_dict(_d)
            result = verify(sig, message_hash, public_key)
            return result.valid
        except Exception as e:
            logger.error(f"[SchnorrΓ-GF] verify_signature failed: {e}")
            return False

    def signature_to_dict(self, sig: SchnorrSignature) -> Dict[str, Any]:
        return signature_to_dict(sig)

    def signature_from_dict(self, d: Dict[str, Any]) -> SchnorrSignature:
        return signature_from_dict(d)


# ═══════════════════════════════════════════════════════════════════════════
# TEST SUITE
# ═══════════════════════════════════════════════════════════════════════════

def run_tests(verbose: bool = True) -> Dict[str, Any]:
    """Run Schnorr-Γ GF(p) test suite."""
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
        print("  Schnorr-Γ GF(p) — Test Suite")
        print(f"  Prime: 2^255 − 31 | FULL 256-bit Fiat-Shamir")
        print("=" * 72)

    kp = keygen()

    # ───────── §K Keygen ──────────────────────────────────────────────
    def t_k1():
        return kp.public_key is not None and len(kp.private_walk) == WALK_LENGTH, \
            f"walk_len={len(kp.private_walk)}"
    test("Keygen produces valid keypair", t_k1)

    def t_k2():
        kp2 = keygen_from_walk(kp.private_walk)
        return kp.address == kp2.address, \
            f"reproducible: {kp.address[:8]} == {kp2.address[:8]}"
    test("Keygen_from_walk reproduces same key", t_k2)

    # ───────── §S Sign ───────────────────────────────────────────────
    msg = b"QTCL-GF Schnorr test message"
    sig = sign(msg, kp.private_walk, kp.public_key)

    def t_s1():
        return sig.R is not None and sig.Z is not None, \
            f"R={sig.R is not None} Z={sig.Z is not None}"
    test("Sign returns SchnorrSignature", t_s1)

    # ───────── §V Verify ─────────────────────────────────────────────
    vr = verify(sig, msg, kp.public_key)

    def t_v1():
        return vr.valid, f"valid={vr.valid}"
    test("verify(sign(m)) == True", t_v1)

    def t_v2():
        vr2 = verify(sig, b"wrong message", kp.public_key)
        return not vr2.valid, f"wrong msg valid={vr2.valid}"
    test("verify(sign(m1), m2) == False", t_v2)

    def t_v3():
        kp2 = keygen()
        vr3 = verify(sig, msg, kp2.public_key)
        return not vr3.valid, f"wrong pk valid={vr3.valid}"
    test("verify with wrong pk == False", t_v3)

    # ───────── §R Round-trip ─────────────────────────────────────────
    def t_r1():
        d = generate_keypair_dict()
        sig_d = sign_message_dict(msg, d["private_walk"], d["public_key_hex"])
        ver_d = verify_message_dict(sig_d, msg, d["public_key_hex"])
        return ver_d["valid"], f"valid={ver_d['valid']}"
    test("generate → sign → verify roundtrip", t_r1)

    # ───────── §W Wire format ────────────────────────────────────────
    def t_w1():
        sig_d = signature_to_dict(sig)
        js = json.dumps(sig_d)
        sig_d2 = json.loads(js)
        sig2 = signature_from_dict(sig_d2)
        vr2 = verify(sig2, msg, kp.public_key)
        return vr2.valid, f"json roundtrip valid={vr2.valid}"
    test("Signature → dict → JSON → verify", t_w1)

    # ───────── §B Bulk ───────────────────────────────────────────────
    def t_bulk():
        failures = 0
        for i in range(100):
            s = sign(msg, kp.private_walk, kp.public_key)
            if not verify(s, msg, kp.public_key).valid:
                failures += 1
        return failures == 0, f"{failures}/100 failures"
    test("100 sign/verify round-trips", t_bulk)

    # ───────── §S SchnorrGamma class ─────────────────────────────────
    def t_class():
        sg = SchnorrGamma()
        d = sg.sign_hash(msg, kp.private_walk, kp.public_key)
        v = sg.verify_signature(msg, d, kp.public_key)
        return v, f"SchnorrGamma class verify={v}"
    test("SchnorrGamma.sign_hash/verify_signature", t_class)

    if verbose:
        print("=" * 72)
        status = "✅ ALL PASSED" if failed == 0 else f"❌ {failed} FAILED"
        print(f"  {status}  ({passed}/{passed+failed})")
        print("=" * 72)

    return {"all_pass": failed == 0, "test_results": results,
            "summary": {"passed": passed, "failed": failed}}


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO,
                        format="[%(asctime)s] %(levelname)s %(name)s: %(message)s")
    results = run_tests(verbose=True)
    exit(0 if results["all_pass"] else 1)
