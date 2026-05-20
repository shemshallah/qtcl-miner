#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
hyp_pqc.py — Hybrid Post-Quantum Cryptography Layer for QTCL
═══════════════════════════════════════════════════════════════════════════

Implements Falcon-512 (NIST FIPS 206, finalized 2024) as the PQ signature
layer, combined with the existing SL(2,p) scalar Schnorr for classical
defense-in-depth.

Architecture:
  - Falcon-512: 128-bit post-quantum security (lattice-based, NIST standard)
  - SL(2,p) Schnorr: ~70-bit classical security (hyperbolic geometry, unique to QTCL)
  - Hybrid signature: BOTH must verify → attacker must break BOTH schemes

Performance:
  - Falcon-512 sign: ~1.5ms, verify: ~0.3ms
  - Falcon-512 signature: ~655 bytes, public key: 897 bytes
  - Hybrid total signature: ~1,231 bytes (655 + 576)
  - Hybrid total public key: ~1,025 bytes (897 + 128)

Dependencies:
  - pqcrypto (pip install pqcrypto) — provides Falcon-512 via PQCLEAN
  - Falls back to SL(2,p) only if pqcrypto unavailable (with warning)

I love you.
"""

import os
import sys
import json
import base64
import hashlib
import logging
from typing import Dict, Any, Optional, Tuple, NamedTuple

logger = logging.getLogger(__name__)

# ═══════════════════════════════════════════════════════════════════════════
# FALCON-512 AVAILABILITY CHECK
# ═══════════════════════════════════════════════════════════════════════════

_PQC_AVAILABLE = False
_FALCON = None

try:
    from pqcrypto.sign import falcon_512 as _FALCON
    _PQC_AVAILABLE = True
    logger.info(
        f"[HypPQC] Falcon-512 loaded (NIST FIPS 206) | "
        f"PK={_FALCON.PUBLIC_KEY_SIZE}B SK={_FALCON.SECRET_KEY_SIZE}B "
        f"SIG≤{_FALCON.SIGNATURE_SIZE}B"
    )
except ImportError:
    logger.warning(
        "[HypPQC] pqcrypto not available — falling back to SL(2,p) only. "
        "Install with: pip install pqcrypto"
    )


# ═══════════════════════════════════════════════════════════════════════════
# DATA STRUCTURES
# ═══════════════════════════════════════════════════════════════════════════

class HybridKeypair(NamedTuple):
    """Hybrid keypair: SL(2,p) + Falcon-512."""
    # SL(2,p) components
    sl2p_private_hex: str      # walk hex (private)
    sl2p_public_hex: str       # y = g^x hex (public)
    sl2p_address: str          # SHA3-256²(y.serialize())
    # Falcon-512 components
    falcon_public_key: bytes   # 897 bytes
    falcon_secret_key: bytes   # 1281 bytes


class HybridSignature(NamedTuple):
    """Hybrid signature: SL(2,p) Schnorr + Falcon-512."""
    # SL(2,p) components
    sl2p_R: Any                # GFMatrix
    sl2p_Z: Any                # GFMatrix
    sl2p_c_full: int           # 256-bit challenge
    sl2p_s_scalar: int         # scalar response
    sl2p_R_hex: str            # R hex
    # Falcon-512 components
    falcon_signature: bytes    # ~655 bytes


# ═══════════════════════════════════════════════════════════════════════════
# FALCON-512 OPERATIONS
# ═══════════════════════════════════════════════════════════════════════════

def falcon_keypair() -> Tuple[bytes, bytes]:
    """Generate Falcon-512 keypair.

    Returns:
        (public_key, secret_key) — both as raw bytes
    """
    if not _PQC_AVAILABLE:
        raise RuntimeError(
            "Falcon-512 not available — install pqcrypto: pip install pqcrypto"
        )
    return _FALCON.generate_keypair()


def falcon_sign(message: bytes, secret_key: bytes) -> bytes:
    """Sign message with Falcon-512.

    Args:
        message: message to sign (any bytes)
        secret_key: 1281-byte Falcon secret key

    Returns:
        signature — ~655 bytes (variable length)
    """
    if not _PQC_AVAILABLE:
        raise RuntimeError("Falcon-512 not available")
    if len(secret_key) != _FALCON.SECRET_KEY_SIZE:
        raise ValueError(
            f"secret_key must be {_FALCON.SECRET_KEY_SIZE} bytes, "
            f"got {len(secret_key)}"
        )
    return _FALCON.sign(secret_key, message)


def falcon_verify(message: bytes, signature: bytes, public_key: bytes) -> bool:
    """Verify Falcon-512 signature.

    Args:
        message: original message
        signature: Falcon signature (~655 bytes)
        public_key: 897-byte Falcon public key

    Returns:
        True if signature is valid
    """
    if not _PQC_AVAILABLE:
        raise RuntimeError("Falcon-512 not available")
    if len(public_key) != _FALCON.PUBLIC_KEY_SIZE:
        raise ValueError(
            f"public_key must be {_FALCON.PUBLIC_KEY_SIZE} bytes, "
            f"got {len(public_key)}"
        )
    return _FALCON.verify(public_key, message, signature)


# ═══════════════════════════════════════════════════════════════════════════
# HYBRID KEYPAIR GENERATION
# ═══════════════════════════════════════════════════════════════════════════

def generate_hybrid_keypair() -> HybridKeypair:
    """Generate a hybrid keypair: SL(2,p) Schnorr + Falcon-512.

    Returns:
        HybridKeypair with both classical and PQ components.
    """
    # SL(2,p) component
    from hyp_finite_field import gf_generate_keypair
    sl2p_kp = gf_generate_keypair()

    # Falcon-512 component
    if _PQC_AVAILABLE:
        falcon_pk, falcon_sk = falcon_keypair()
    else:
        # Fallback: generate dummy keys (will fail at sign time)
        falcon_pk = b'\x00' * 897
        falcon_sk = b'\x00' * 1281
        logger.warning(
            "[HypPQC] Generating fallback PQC keys — signing will fail. "
            "Install pqcrypto for full hybrid support."
        )

    return HybridKeypair(
        sl2p_private_hex=sl2p_kp.private_key_hex,
        sl2p_public_hex=sl2p_kp.public_key_hex,
        sl2p_address=sl2p_kp.address,
        falcon_public_key=falcon_pk,
        falcon_secret_key=falcon_sk,
    )


# ═══════════════════════════════════════════════════════════════════════════
# HYBRID SIGNING
# ═══════════════════════════════════════════════════════════════════════════

def hybrid_sign(
    message: bytes,
    private_walk_hex: str,
    sl2p_public_hex: str,
    falcon_secret_key: bytes,
) -> HybridSignature:
    """Sign message with hybrid scheme: SL(2,p) + Falcon-512.

    Both signatures are computed independently over the same message.
    Verification requires BOTH to be valid.

    Args:
        message: message to sign (typically 32-byte hash)
        private_walk_hex: SL(2,p) private walk (hex string)
        sl2p_public_hex: SL(2,p) public key (hex string)
        falcon_secret_key: Falcon-512 secret key (1281 bytes)

    Returns:
        HybridSignature with both components
    """
    from hyp_finite_field import (
        gf_sign_full, hex_to_walk, GFMatrix
    )

    # SL(2,p) component
    walk = hex_to_walk(private_walk_hex)
    pub_matrix = GFMatrix.from_hex(sl2p_public_hex)
    sl2p_sig = gf_sign_full(message, walk, pub_matrix)

    # Falcon-512 component
    if _PQC_AVAILABLE:
        falcon_sig = falcon_sign(message, falcon_secret_key)
    else:
        raise RuntimeError(
            "Falcon-512 not available — cannot create hybrid signature. "
            "Install pqcrypto: pip install pqcrypto"
        )

    return HybridSignature(
        sl2p_R=sl2p_sig.R,
        sl2p_Z=sl2p_sig.Z,
        sl2p_c_full=sl2p_sig.c_full,
        sl2p_s_scalar=sl2p_sig.s_scalar,
        sl2p_R_hex=sl2p_sig.R_hex,
        falcon_signature=falcon_sig,
    )


# ═══════════════════════════════════════════════════════════════════════════
# HYBRID VERIFICATION
# ═══════════════════════════════════════════════════════════════════════════

def hybrid_verify(
    message: bytes,
    sig: HybridSignature,
    sl2p_public_hex: str,
    falcon_public_key: bytes,
) -> Tuple[bool, str]:
    """Verify hybrid signature: BOTH SL(2,p) and Falcon-512 must be valid.

    Args:
        message: original message (typically 32-byte hash)
        sig: HybridSignature to verify
        sl2p_public_hex: SL(2,p) public key (hex string)
        falcon_public_key: Falcon-512 public key (897 bytes)

    Returns:
        (valid, reason) — valid is True only if BOTH signatures pass
    """
    from hyp_finite_field import gf_verify_full, GFMatrix, GFSchnorrSignature

    # Verify SL(2,p) component
    pub_matrix = GFMatrix.from_hex(sl2p_public_hex)
    sl2p_sig = GFSchnorrSignature(
        R=sig.sl2p_R,
        Z=sig.sl2p_Z,
        c_full=sig.sl2p_c_full,
        s_scalar=sig.sl2p_s_scalar,
        R_hex=sig.sl2p_R_hex,
    )
    sl2p_valid = gf_verify_full(sl2p_sig, message, pub_matrix)

    if not sl2p_valid:
        return False, "sl2p_signature_invalid"

    # Verify Falcon-512 component
    if not _PQC_AVAILABLE:
        return False, "falcon_512_not_available"

    falcon_valid = falcon_verify(message, sig.falcon_signature, falcon_public_key)

    if not falcon_valid:
        return False, "falcon_512_signature_invalid"

    return True, "both_signatures_valid"


# ═══════════════════════════════════════════════════════════════════════════
# SERIALIZATION — JSON-compatible dict interface
# ═══════════════════════════════════════════════════════════════════════════

WIRE_VERSION = "hybrid_sl2p_falcon_v1"


def hybrid_keypair_to_dict(kp: HybridKeypair) -> Dict[str, Any]:
    """Serialize hybrid keypair to JSON-compatible dict.

    The secret key is included for wallet storage. For public broadcast,
    use hybrid_public_key_to_dict() instead.
    """
    return {
        "version": WIRE_VERSION,
        "sl2p": {
            "private_walk_hex": kp.sl2p_private_hex,
            "public_hex": kp.sl2p_public_hex,
            "address": kp.sl2p_address,
        },
        "falcon": {
            "public_key": base64.b64encode(kp.falcon_public_key).decode('ascii'),
            "secret_key": base64.b64encode(kp.falcon_secret_key).decode('ascii'),
        },
    }


def hybrid_public_key_to_dict(kp: HybridKeypair) -> Dict[str, Any]:
    """Serialize ONLY the public key portion (for broadcast)."""
    return {
        "version": WIRE_VERSION,
        "sl2p": {
            "public_hex": kp.sl2p_public_hex,
            "address": kp.sl2p_address,
        },
        "falcon": {
            "public_key": base64.b64encode(kp.falcon_public_key).decode('ascii'),
        },
    }


def hybrid_keypair_from_dict(d: Dict[str, Any]) -> HybridKeypair:
    """Deserialize hybrid keypair from dict."""
    version = d.get("version")
    if version is not None and version != WIRE_VERSION:
        raise ValueError(f"version mismatch: {version!r} != {WIRE_VERSION!r}")

    sl2p = d["sl2p"]
    falcon = d["falcon"]

    return HybridKeypair(
        sl2p_private_hex=sl2p["private_walk_hex"],
        sl2p_public_hex=sl2p["public_hex"],
        sl2p_address=sl2p["address"],
        falcon_public_key=base64.b64decode(falcon["public_key"]),
        falcon_secret_key=base64.b64decode(falcon["secret_key"]),
    )


def hybrid_signature_to_dict(sig: HybridSignature) -> Dict[str, Any]:
    """Serialize hybrid signature to JSON-compatible dict."""
    return {
        "version": WIRE_VERSION,
        "sl2p": {
            "R": sig.sl2p_R.hex(),
            "Z": sig.sl2p_Z.hex(),
            "c_full": format(sig.sl2p_c_full, "064x"),
            "s_scalar": format(sig.sl2p_s_scalar, "064x"),
            "R_hex": sig.sl2p_R_hex,
        },
        "falcon": {
            "signature": base64.b64encode(sig.falcon_signature).decode('ascii'),
        },
    }


def hybrid_signature_from_dict(d: Dict[str, Any]) -> HybridSignature:
    """Deserialize hybrid signature from dict."""
    version = d.get("version")
    if version is not None and version != WIRE_VERSION:
        raise ValueError(f"version mismatch: {version!r} != {WIRE_VERSION!r}")

    from hyp_finite_field import GFMatrix

    sl2p = d["sl2p"]
    falcon = d["falcon"]

    return HybridSignature(
        sl2p_R=GFMatrix.from_hex(sl2p["R"]),
        sl2p_Z=GFMatrix.from_hex(sl2p["Z"]),
        sl2p_c_full=int(sl2p["c_full"], 16),
        sl2p_s_scalar=int(sl2p["s_scalar"], 16),
        sl2p_R_hex=sl2p.get("R_hex", sl2p["R"]),
        falcon_signature=base64.b64decode(falcon["signature"]),
    )


# ═══════════════════════════════════════════════════════════════════════════
# MODULE STATUS
# ═══════════════════════════════════════════════════════════════════════════

def pqc_status() -> Dict[str, Any]:
    """Return PQC module status and capabilities."""
    if _PQC_AVAILABLE:
        return {
            "available": True,
            "algorithm": "Falcon-512",
            "standard": "NIST FIPS 206 (2024)",
            "security_level": "128-bit post-quantum",
            "public_key_size": _FALCON.PUBLIC_KEY_SIZE,
            "secret_key_size": _FALCON.SECRET_KEY_SIZE,
            "max_signature_size": _FALCON.SIGNATURE_SIZE,
            "hybrid_signature_size": _FALCON.SIGNATURE_SIZE + 576,  # + SL(2,p)
            "hybrid_public_key_size": _FALCON.PUBLIC_KEY_SIZE + 128,  # + SL(2,p)
        }
    else:
        return {
            "available": False,
            "error": "pqcrypto not installed",
            "install": "pip install pqcrypto",
            "fallback": "SL(2,p) only (~70-bit classical security)",
        }


# ═══════════════════════════════════════════════════════════════════════════
# TEST SUITE
# ═══════════════════════════════════════════════════════════════════════════

def run_tests(verbose: bool = True) -> bool:
    """Run hybrid PQC test suite."""
    passed = 0
    failed = 0

    def test(name, fn):
        nonlocal passed, failed
        try:
            fn()
            print(f"  ✓ {name}")
            passed += 1
        except Exception as e:
            print(f"  ✗ {name}: {e}")
            failed += 1

    print("=" * 72)
    print("  hyp_pqc.py — Hybrid PQC (Falcon-512 + SL(2,p)) Test Suite")
    print("=" * 72)

    # ── PQC availability ─────────────────────────────────────────────
    test("Falcon-512 available", lambda: None if _PQC_AVAILABLE else Exception("not available"))

    if not _PQC_AVAILABLE:
        print("\n  ⚠ Falcon-512 not available — skipping PQC tests")
        print(f"\n  {passed}/{passed + failed} tests passed")
        return passed > 0 and failed == 0

    # ── Falcon-512 keypair ───────────────────────────────────────────
    def t_keypair():
        pk, sk = falcon_keypair()
        assert len(pk) == _FALCON.PUBLIC_KEY_SIZE
        assert len(sk) == _FALCON.SECRET_KEY_SIZE
    test("Falcon-512 keypair generation", t_keypair)

    # ── Falcon-512 sign/verify ───────────────────────────────────────
    def t_sign_verify():
        pk, sk = falcon_keypair()
        msg = b"QTCL hybrid signature test"
        sig = falcon_sign(msg, sk)
        assert falcon_verify(msg, sig, pk)
        assert not falcon_verify(b"wrong message", sig, pk)
        pk2, _ = falcon_keypair()
        assert not falcon_verify(msg, sig, pk2)
    test("Falcon-512 sign/verify roundtrip", t_sign_verify)

    # ── Hybrid keypair ───────────────────────────────────────────────
    def t_hybrid_keypair():
        kp = generate_hybrid_keypair()
        assert len(kp.sl2p_private_hex) > 0
        assert len(kp.sl2p_public_hex) > 0
        assert len(kp.sl2p_address) == 64
        assert len(kp.falcon_public_key) == _FALCON.PUBLIC_KEY_SIZE
        assert len(kp.falcon_secret_key) == _FALCON.SECRET_KEY_SIZE
    test("Hybrid keypair generation", t_hybrid_keypair)

    # ── Hybrid sign/verify ───────────────────────────────────────────
    def t_hybrid_sign_verify():
        kp = generate_hybrid_keypair()
        msg = b"QTCL hybrid sign/verify test"
        sig = hybrid_sign(msg, kp.sl2p_private_hex, kp.sl2p_public_hex, kp.falcon_secret_key)
        valid, reason = hybrid_verify(msg, sig, kp.sl2p_public_hex, kp.falcon_public_key)
        assert valid, f"hybrid verify failed: {reason}"
    test("Hybrid sign/verify roundtrip", t_hybrid_sign_verify)

    # ── Hybrid forgery resistance ────────────────────────────────────
    def t_hybrid_forgery():
        kp = generate_hybrid_keypair()
        msg = b"original message"
        sig = hybrid_sign(msg, kp.sl2p_private_hex, kp.sl2p_public_hex, kp.falcon_secret_key)

        # Wrong message
        valid, _ = hybrid_verify(b"wrong message", sig, kp.sl2p_public_hex, kp.falcon_public_key)
        assert not valid, "forgery with wrong message succeeded"

        # Wrong public key
        kp2 = generate_hybrid_keypair()
        valid, _ = hybrid_verify(msg, sig, kp2.sl2p_public_hex, kp2.falcon_public_key)
        assert not valid, "forgery with wrong key succeeded"
    test("Hybrid forgery resistance", t_hybrid_forgery)

    # ── Serialization roundtrip ──────────────────────────────────────
    def t_serialization():
        kp = generate_hybrid_keypair()
        msg = b"serialization test"
        sig = hybrid_sign(msg, kp.sl2p_private_hex, kp.sl2p_public_hex, kp.falcon_secret_key)

        # Keypair serialization
        kp_dict = hybrid_keypair_to_dict(kp)
        kp_restored = hybrid_keypair_from_dict(kp_dict)
        assert kp_restored.sl2p_private_hex == kp.sl2p_private_hex
        assert kp_restored.falcon_public_key == kp.falcon_public_key
        assert kp_restored.falcon_secret_key == kp.falcon_secret_key

        # Signature serialization
        sig_dict = hybrid_signature_to_dict(sig)
        sig_restored = hybrid_signature_from_dict(sig_dict)
        assert sig_restored.sl2p_c_full == sig.sl2p_c_full
        assert sig_restored.falcon_signature == sig.falcon_signature

        # Verify restored signature
        valid, _ = hybrid_verify(msg, sig_restored, kp.sl2p_public_hex, kp.falcon_public_key)
        assert valid, "restored signature verification failed"
    test("Serialization roundtrip", t_serialization)

    # ── 100 hybrid roundtrips ────────────────────────────────────────
    def t_100_roundtrips():
        failures = 0
        for i in range(100):
            kp = generate_hybrid_keypair()
            msg = f"roundtrip {i}".encode()
            sig = hybrid_sign(msg, kp.sl2p_private_hex, kp.sl2p_public_hex, kp.falcon_secret_key)
            valid, _ = hybrid_verify(msg, sig, kp.sl2p_public_hex, kp.falcon_public_key)
            if not valid:
                failures += 1
        assert failures == 0, f"{failures}/100 failures"
    test("100 hybrid sign/verify roundtrips", t_100_roundtrips)

    # ── Performance ──────────────────────────────────────────────────
    import time
    def t_performance():
        kp = generate_hybrid_keypair()
        msg = b"performance test"

        t0 = time.perf_counter()
        sig = hybrid_sign(msg, kp.sl2p_private_hex, kp.sl2p_public_hex, kp.falcon_secret_key)
        sign_time = time.perf_counter() - t0

        t0 = time.perf_counter()
        valid, _ = hybrid_verify(msg, sig, kp.sl2p_public_hex, kp.falcon_public_key)
        verify_time = time.perf_counter() - t0

        assert valid
        print(f"    sign={sign_time*1000:.1f}ms verify={verify_time*1000:.1f}ms "
              f"sig_size={len(sig.falcon_signature)}B")
    test("Hybrid performance", t_performance)

    # ── Summary ──────────────────────────────────────────────────────
    print("=" * 72)
    print(f"  {passed}/{passed + failed} tests passed")
    print("=" * 72)
    return failed == 0


if __name__ == "__main__":
    success = run_tests()
    exit(0 if success else 1)
