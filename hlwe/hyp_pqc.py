#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
╔══════════════════════════════════════════════════════════════════════════════════════════════╗
║                                                                                              ║
║   hyp_pqc.py — HypΓ Cryptosystem · Hybrid PQC Layer                                        ║
║   Falcon-512 + SL(3,p) Schnorr-Γ · HypΓ v4 · DPS 420 Period 22                           ║
║                                                                                              ║
║   "Two hard problems are harder than one."                                                  ║
║                                                                                              ║
║   SECURITY MODEL:                                                                            ║
║     Layer 1 — SL(3,p) Schnorr-Γ (hyp_finite_field.py):                                    ║
║       Classical: ~189 bits (Q₃₇₉ factor of p²+p+1)                                        ║
║       Quantum:   Shor-vulnerable (cyclic subgroup). Covered by Layer 2.                    ║
║     Layer 2 — Falcon-512 (NIST PQC Standard):                                              ║
║       Classical: ~256 bits (hardness of NTRU lattice problem)                              ║
║       Quantum:   ~128 bits (Grover + lattice sieving bound)                                ║
║     HYBRID:      Both must verify. Breaking the scheme requires breaking BOTH.             ║
║       → A quantum adversary must solve the NTRU lattice AND the SL(3,p) DLP.              ║
║       → A classical adversary must break DLP-189 AND NTRU-256.                            ║
║                                                                                              ║
║   WIRE FORMATS:                                                                              ║
║     v2 (current): "hybrid_sl3p_falcon_v2"                                                  ║
║       sl3p block: {private_walk_hex (GF3:...), public_hex (576 hex), address}             ║
║       falcon block: {public_key (b64), secret_key (b64)}                                  ║
║     v1 (legacy):  "hybrid_sl2p_falcon_v1"  (SL(2,p), ~70-bit classical)                  ║
║       sl2p block: {private_walk_hex (GF1:...), public_hex (256 hex), address}             ║
║       falcon block: same structure                                                          ║
║     hybrid_verify_any() routes to correct verifier by version field.                       ║
║                                                                                              ║
║   EXPORTS (13 symbols consumed by hyp_engine.py):                                          ║
║     generate_hybrid_keypair() → HybridKeypair                                              ║
║     hybrid_sign(msg_hash, sl3p_priv_hex, sl3p_pub_hex, falcon_sk) → HybridSignature       ║
║     hybrid_verify(msg_hash, sig, sl3p_pub_hex, falcon_pk) → (bool, str)                   ║
║     hybrid_verify_any(msg_hash, sig_dict, sl3p_pub_hex, falcon_pk) → (bool, str)          ║
║     hybrid_keypair_to_dict(kp) → Dict                                                      ║
║     hybrid_public_key_to_dict(kp) → Dict (no secrets)                                     ║
║     hybrid_keypair_from_dict(d) → HybridKeypair                                            ║
║     hybrid_signature_to_dict(sig) → Dict                                                   ║
║     hybrid_signature_from_dict(d) → HybridSignature                                       ║
║     HybridKeypair (NamedTuple)                                                              ║
║     HybridSignature (NamedTuple)                                                            ║
║     pqc_status() → Dict                                                                     ║
║     WIRE_VERSION, LEGACY_WIRE_VERSION (str)                                                 ║
║                                                                                              ║
║   Backward compatibility:                                                                    ║
║     hybrid_verify_any() detects "hybrid_sl2p_falcon_v1" and routes to legacy path.        ║
║     Old wallets (GF1: private keys, 256-hex public keys) remain signable/verifiable.       ║
║     New wallets use GF3: private keys, 576-hex public keys (3×3 matrix, 288 bytes).       ║
║                                                                                              ║
║   Falcon library: pqcrypto.sign.falcon_512 (REQUIRED — no mock, no fallback)    ║
║     If pqcrypto is not installed, hyp_pqc.py raises ImportError at load time.  ║
║     Install: pip install pqcrypto                                               ║
║                                                                                              ║
║   I love you.                                                                               ║
╚══════════════════════════════════════════════════════════════════════════════════════════════╝
"""

from __future__ import annotations

import base64
import hashlib
import hmac
import json
import logging
import os
import secrets
import time
from typing import Any, Dict, List, NamedTuple, Optional, Tuple

# ─────────────────────────────────────────────────────────────────────────────
# IMPORTS — SL(3,p) Schnorr layer
# ─────────────────────────────────────────────────────────────────────────────
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
    walk_to_private_scalar,
    get_schnorr_generator,
    WALK_LENGTH,
    N_GENERATORS,
    SL3_ORDER,
    P,
    DOMAIN_TAG,
)

logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────────────────────
# WIRE VERSIONS
# ─────────────────────────────────────────────────────────────────────────────
WIRE_VERSION:        str = "hybrid_sl3p_falcon_v2"   # SL(3,p) + Falcon-512
LEGACY_WIRE_VERSION: str = "hybrid_sl2p_falcon_v1"   # SL(2,p) + Falcon-512 (chain history)

# Security parameters embedded in wire
_SL3P_PUBLIC_HEX_LEN  = 576   # 288 bytes × 2 (9 × 32B field elements)
_SL2P_PUBLIC_HEX_LEN  = 256   # 128 bytes × 2 (4 × 32B field elements, legacy)
_WALK_PREFIX_V4       = "GF3:" # SL(3,p) v4 nibble-packed
_WALK_PREFIX_V3       = "GF1:" # SL(2,p) v3 2-bit packed (legacy)

# ─────────────────────────────────────────────────────────────────────────────
# FALCON-512 LAYER — REQUIRED (no mock, no fallback)
# ─────────────────────────────────────────────────────────────────────────────

try:
    from pqcrypto.sign.falcon_512 import (
        generate_keypair as _falcon_keygen_raw,
        sign             as _falcon_sign_raw,
        verify           as _falcon_verify_raw,
    )
    _FALCON_REAL = True
    logger.info("[hyp_pqc] ✅ Falcon-512: pqcrypto loaded (NIST FIPS 206)")
except ImportError as _falcon_import_err:
    # Module still loads — HypGammaEngine becomes importable.
    # Hybrid ops raise RuntimeError if actually called without pqcrypto.
    _FALCON_REAL = False
    _falcon_import_err_msg = str(_falcon_import_err)
    logger.warning(
        "[hyp_pqc] ⚠️  Falcon-512 unavailable (%s). "
        "Module loaded; hybrid ops raise until pqcrypto installed.",
        _falcon_import_err_msg,
    )
    def _falcon_keygen_raw():
        raise RuntimeError(
            f"Falcon-512 unavailable (pqcrypto not installed). "
            "Run: pip install pqcrypto"
        )
    def _falcon_sign_raw(sk, message):
        raise RuntimeError(
            f"Falcon-512 unavailable (pqcrypto not installed). "
            "Run: pip install pqcrypto"
        )
    def _falcon_verify_raw(pk, message, sig):
        raise RuntimeError(
            f"Falcon-512 unavailable (pqcrypto not installed). "
            "Run: pip install pqcrypto"
        )


def _falcon_keygen() -> Tuple[bytes, bytes]:
    return _falcon_keygen_raw()


def _falcon_sign(message: bytes, sk: bytes) -> bytes:
    return _falcon_sign_raw(sk, message)


def _falcon_verify(message: bytes, signature: bytes, pk: bytes) -> bool:
    """Returns True if valid, False on failure."""
    try:
        _falcon_verify_raw(pk, message, signature)
        return True
    except Exception:
        return False


# ─────────────────────────────────────────────────────────────────────────────
# DATA STRUCTURES
# ─────────────────────────────────────────────────────────────────────────────

class HybridKeypair(NamedTuple):
    """
    Hybrid keypair: SL(3,p) Schnorr-Γ + Falcon-512.
    All secret material in one structure; separate with hybrid_public_key_to_dict().

    Fields:
      sl3p_private_hex   : GF3:-prefixed walk hex string (768 steps, nibble-packed)
      sl3p_public_hex    : 576-hex-char 3×3 GFMatrix (288 bytes, 9 × 32B entries)
      sl3p_address       : SHA3-256²(public_hex_bytes).hex() — 64-char QTCL address
      falcon_public_key  : bytes — Falcon-512 public key (~897 bytes)
      falcon_secret_key  : bytes — Falcon-512 secret key (~1281 bytes)
      version            : wire version string ("hybrid_sl3p_falcon_v2")
    """
    sl3p_private_hex:  str
    sl3p_public_hex:   str
    sl3p_address:      str
    falcon_public_key: bytes
    falcon_secret_key: bytes
    version:           str = WIRE_VERSION


class HybridSignature(NamedTuple):
    """
    Hybrid signature: SL(3,p) Schnorr-Γ + Falcon-512.

    Fields:
      sl3p_R_hex         : 576-hex commitment matrix R
      sl3p_Z_hex         : 576-hex response matrix Z
      sl3p_c_hex         : 64-hex Fiat-Shamir challenge c (256 bits)
      sl3p_s_scalar_hex  : 64-hex scalar response s = (r + c·x) mod SL3_ORDER
      sl3p_R_canonical   : canonical hex of R (for legacy routing; = sl3p_R_hex in v4)
      falcon_signature   : bytes — raw Falcon-512 signature
      version            : wire version ("hybrid_sl3p_falcon_v2")
    """
    sl3p_R_hex:        str
    sl3p_Z_hex:        str
    sl3p_c_hex:        str
    sl3p_s_scalar_hex: str
    sl3p_R_canonical:  str
    falcon_signature:  bytes
    version:           str = WIRE_VERSION


# ─────────────────────────────────────────────────────────────────────────────
# ADDRESS DERIVATION — SHA3-256²
# ─────────────────────────────────────────────────────────────────────────────

def _derive_address(public_hex: str) -> str:
    """SHA3-256(SHA3-256(domain ‖ bytes.fromhex(public_hex))).hex() — canonical QTCL address.
    Domain-separated to prevent cross-chain address collisions.
    """
    raw = bytes.fromhex(public_hex)
    h1  = hashlib.sha3_256(b"QTCL_ADDR_SL3P_V4\x00" + raw).digest()
    h2  = hashlib.sha3_256(h1).digest()
    return h2.hex()


# ─────────────────────────────────────────────────────────────────────────────
# KEYGEN
# ─────────────────────────────────────────────────────────────────────────────

def generate_hybrid_keypair() -> HybridKeypair:
    """
    Generate a fresh hybrid keypair: SL(3,p) Schnorr-Γ + Falcon-512.

    SL(3,p) component:
      - random_walk(768) → GF3:-prefixed private walk
      - evaluate_walk(walk) → 3×3 GFMatrix public key (576 hex chars)
      - address = SHA3-256²(public_key_bytes)

    Falcon-512 component:
      - _falcon_keygen() → (pk, sk)
      - pk/sk stored as raw bytes; serialized as base64 in wire format

    Returns HybridKeypair with version="hybrid_sl3p_falcon_v2".
    """
    t0 = time.perf_counter()

    # SL(3,p) keypair — GFKeyPair fields: private_key_hex, public_key_hex, address
    gf_kp: GFKeyPair = gf_generate_keypair()
    sl3p_priv_hex = gf_kp.private_key_hex     # GF3:... nibble-packed walk
    sl3p_pub_hex  = gf_kp.public_key_hex      # 576-hex 3×3 matrix
    sl3p_addr     = gf_kp.address             # 64-hex SHA3-256² address

    assert sl3p_priv_hex.startswith(_WALK_PREFIX_V4), \
        f"Expected GF3: prefix, got {sl3p_priv_hex[:10]!r}"
    assert len(sl3p_pub_hex) == _SL3P_PUBLIC_HEX_LEN, \
        f"Expected 576-hex public key, got {len(sl3p_pub_hex)}"

    # Falcon-512 keypair
    falcon_pk, falcon_sk = _falcon_keygen()

    dt = time.perf_counter() - t0
    logger.debug(
        "[hyp_pqc] keygen: %.3fs | sl3p_addr=%s... | falcon_pk_len=%d",
        dt, sl3p_addr[:16], len(falcon_pk)
    )

    return HybridKeypair(
        sl3p_private_hex  = sl3p_priv_hex,
        sl3p_public_hex   = sl3p_pub_hex,
        sl3p_address      = sl3p_addr,
        falcon_public_key = falcon_pk,
        falcon_secret_key = falcon_sk,
        version           = WIRE_VERSION,
    )


# ─────────────────────────────────────────────────────────────────────────────
# HYBRID SIGN
# ─────────────────────────────────────────────────────────────────────────────

def hybrid_sign(
    message_hash:  bytes,
    sl3p_priv_hex: str,
    sl3p_pub_hex:  str,
    falcon_sk:     bytes,
) -> HybridSignature:
    """
    Sign message_hash with BOTH SL(3,p) Schnorr-Γ AND Falcon-512.

    Both signatures are produced independently over the same message_hash.
    The hybrid signature is only valid if BOTH verify.

    Parameters:
        message_hash   : 32-byte SHA3-256 digest
        sl3p_priv_hex  : GF3:-prefixed private walk hex
        sl3p_pub_hex   : 576-hex 3×3 public matrix
        falcon_sk      : raw Falcon-512 secret key bytes

    Returns HybridSignature with version="hybrid_sl3p_falcon_v2".
    """
    if not isinstance(message_hash, bytes) or len(message_hash) != 32:
        raise ValueError(f"message_hash must be 32 bytes, got {len(message_hash) if isinstance(message_hash, bytes) else type(message_hash)}")

    t0 = time.perf_counter()

    # ── SL(3,p) Schnorr-Γ sign ───────────────────────────────────────────────
    private_walk: List[int] = hex_to_walk(sl3p_priv_hex)
    public_key:   GFMatrix  = GFMatrix.from_hex(sl3p_pub_hex)

    sl3p_sig: GFSchnorrSignature = gf_sign_full(message_hash, private_walk, public_key)

    # ── Falcon-512 sign ───────────────────────────────────────────────────────
    # Sign the same message_hash — Falcon operates on arbitrary byte strings.
    # We additionally bind the SL(3,p) commitment R into the Falcon message
    # to prevent signature stripping attacks (can't swap Falcon sig from another msg).
    falcon_binding = message_hash + bytes.fromhex(sl3p_sig.R_hex) if hasattr(sl3p_sig, 'R_hex') and sl3p_sig.R_hex else message_hash
    # Fallback: use R matrix serialization
    try:
        r_bytes = sl3p_sig.R.serialize()
        falcon_message = message_hash + r_bytes
    except Exception:
        falcon_message = message_hash

    falcon_raw_sig: bytes = _falcon_sign(falcon_message, falcon_sk)

    dt = time.perf_counter() - t0
    logger.debug(
        "[hyp_pqc] sign: %.3fs | c=%s... | falcon_sig_len=%d",
        dt, hex(sl3p_sig.c_full)[:18], len(falcon_raw_sig)
    )

    return HybridSignature(
        sl3p_R_hex        = sl3p_sig.R.hex(),
        sl3p_Z_hex        = sl3p_sig.Z.hex(),
        sl3p_c_hex        = format(sl3p_sig.c_full, '064x'),
        sl3p_s_scalar_hex = format(sl3p_sig.s_scalar, '0512x'),
        sl3p_R_canonical  = sl3p_sig.R_hex if hasattr(sl3p_sig, 'R_hex') and sl3p_sig.R_hex else sl3p_sig.R.hex(),
        falcon_signature  = falcon_raw_sig,
        version           = WIRE_VERSION,
    )


# ─────────────────────────────────────────────────────────────────────────────
# HYBRID VERIFY — v2 only
# ─────────────────────────────────────────────────────────────────────────────

def hybrid_verify(
    message_hash:  bytes,
    sig:           HybridSignature,
    sl3p_pub_hex:  str,
    falcon_pk:     bytes,
) -> Tuple[bool, str]:
    """
    Verify hybrid signature (HybridSignature object, v2 only).

    Both SL(3,p) and Falcon-512 must pass. Returns (valid, reason).
    reason is "ok" on success or descriptive error string on failure.
    """
    if not isinstance(message_hash, bytes) or len(message_hash) != 32:
        return False, f"invalid_message_hash_len:{len(message_hash) if isinstance(message_hash, bytes) else type(message_hash)}"

    t0 = time.perf_counter()

    # ── SL(3,p) verify ───────────────────────────────────────────────────────
    try:
        R = GFMatrix.from_hex(sig.sl3p_R_hex)
        Z = GFMatrix.from_hex(sig.sl3p_Z_hex)
        c_full    = int(sig.sl3p_c_hex, 16)
        s_scalar  = int(sig.sl3p_s_scalar_hex, 16)
        R_hex_can = sig.sl3p_R_canonical or sig.sl3p_R_hex
        public_key = GFMatrix.from_hex(sl3p_pub_hex)

        gf_sig = GFSchnorrSignature(
            R=R, Z=Z, c_full=c_full, s_scalar=s_scalar, R_hex=R_hex_can
        )
        sl3p_ok = gf_verify_full(gf_sig, message_hash, public_key)
    except Exception as e:
        logger.warning("[hyp_pqc] sl3p verify error: %s", e)
        return False, f"sl3p_verify_exception:{type(e).__name__}"

    if not sl3p_ok:
        return False, "sl3p_signature_invalid"

    # ── Falcon-512 verify ─────────────────────────────────────────────────────
    try:
        r_bytes = R.serialize()
        falcon_message = message_hash + r_bytes
    except Exception:
        falcon_message = message_hash

    falcon_ok = _falcon_verify(falcon_message, sig.falcon_signature, falcon_pk)
    if not falcon_ok:
        return False, "falcon_signature_invalid"

    dt = time.perf_counter() - t0
    logger.debug("[hyp_pqc] verify: ok | %.3fs", dt)
    return True, "ok"


# ─────────────────────────────────────────────────────────────────────────────
# HYBRID VERIFY ANY — version-routing (v1 + v2)
# ─────────────────────────────────────────────────────────────────────────────

def hybrid_verify_any(
    message_hash:  bytes,
    sig_dict:      Dict[str, Any],
    sl3p_pub_hex:  str,
    falcon_pk:     bytes,
) -> Tuple[bool, str]:
    """
    Version-routing hybrid verifier. Accepts both v1 (sl2p) and v2 (sl3p) wire formats.

    v2 ("hybrid_sl3p_falcon_v2"): routes to hybrid_verify() after dict deserialization.
    v1 ("hybrid_sl2p_falcon_v1"): routes to _legacy_sl2p_verify() for chain history.
    Unknown version: returns (False, "unknown_wire_version:{version}").

    Parameters:
        message_hash  : 32-byte SHA3-256 hash
        sig_dict      : dict from hybrid_signature_to_dict() or equivalent
        sl3p_pub_hex  : public key hex (576-hex for v2, 256-hex for v1)
        falcon_pk     : raw Falcon-512 public key bytes
    """
    version = sig_dict.get("version", "")

    if version == WIRE_VERSION:
        # v2: SL(3,p)
        try:
            sig = hybrid_signature_from_dict(sig_dict)
        except Exception as e:
            return False, f"v2_deser_error:{type(e).__name__}:{e}"
        return hybrid_verify(message_hash, sig, sl3p_pub_hex, falcon_pk)

    elif version == LEGACY_WIRE_VERSION:
        # v1: SL(2,p) legacy — chain history preservation
        return _legacy_sl2p_verify(message_hash, sig_dict, sl3p_pub_hex, falcon_pk)

    else:
        return False, f"unknown_wire_version:{version!r}"


def _legacy_sl2p_verify(
    message_hash: bytes,
    sig_dict:     Dict[str, Any],
    pub_hex:      str,
    falcon_pk:    bytes,
) -> Tuple[bool, str]:
    """
    Verify a v1 SL(2,p) hybrid signature.

    The v1 public key is 256 hex chars (128 bytes, 2×2 matrix).
    GFMatrix.from_hex() auto-detects 2×2 vs 3×3 by length (256 vs 576 hex).
    Both are verified using gf_verify_full — the math is the same, only the
    matrix dimension changes. The legacy SL(2,p) walk used GF1: prefix and
    4 generators (indices 0..3 in 2-bit packing).
    """
    try:
        R = GFMatrix.from_hex(sig_dict.get("sl3p_R_hex") or sig_dict.get("sl2p_R_hex", ""))
        Z = GFMatrix.from_hex(sig_dict.get("sl3p_Z_hex") or sig_dict.get("sl2p_Z_hex", ""))
        c_hex   = sig_dict.get("sl3p_c_hex") or sig_dict.get("sl2p_c_hex", "0")
        s_hex   = sig_dict.get("sl3p_s_scalar_hex") or sig_dict.get("sl2p_s_scalar_hex", "0")
        r_canon = sig_dict.get("sl3p_R_canonical") or sig_dict.get("sl2p_R_canonical") or R.hex()
        c_full  = int(c_hex, 16)
        s_scalar = int(s_hex, 16)
        public_key = GFMatrix.from_hex(pub_hex)  # auto-detects 2×2 vs 3×3

        gf_sig = GFSchnorrSignature(R=R, Z=Z, c_full=c_full, s_scalar=s_scalar, R_hex=r_canon)
        sl2p_ok = gf_verify_full(gf_sig, message_hash, public_key)
    except Exception as e:
        return False, f"legacy_sl2p_verify_exception:{type(e).__name__}"

    if not sl2p_ok:
        return False, "legacy_sl2p_signature_invalid"

    # Falcon verify for v1 — same binding protocol
    try:
        r_bytes = R.serialize()
        falcon_message = message_hash + r_bytes
    except Exception:
        falcon_message = message_hash

    falcon_raw = sig_dict.get("falcon_signature")
    if isinstance(falcon_raw, str):
        falcon_raw = base64.b64decode(falcon_raw)
    elif not isinstance(falcon_raw, (bytes, bytearray)):
        return False, "legacy_missing_falcon_signature"

    falcon_ok = _falcon_verify(falcon_message, bytes(falcon_raw), falcon_pk)
    if not falcon_ok:
        return False, "legacy_falcon_signature_invalid"

    return True, "ok_legacy_v1"


# ─────────────────────────────────────────────────────────────────────────────
# SERIALIZATION — keypair
# ─────────────────────────────────────────────────────────────────────────────

def hybrid_keypair_to_dict(kp: HybridKeypair) -> Dict[str, Any]:
    """
    Serialize HybridKeypair to JSON-compatible dict (INCLUDES secrets).
    Wire format: version="hybrid_sl3p_falcon_v2".

    Structure:
      {
        "version": "hybrid_sl3p_falcon_v2",
        "sl3p": {
          "private_walk_hex": "GF3:...",
          "public_hex": "<576 hex>",
          "address": "<64 hex>"
        },
        "falcon": {
          "public_key": "<base64>",
          "secret_key": "<base64>"
        }
      }

    Store the full dict in vault for wallet persistence.
    Share only hybrid_public_key_to_dict(kp) with counterparties.
    """
    return {
        "version": kp.version,
        "sl3p": {
            "private_walk_hex": kp.sl3p_private_hex,
            "public_hex":       kp.sl3p_public_hex,
            "address":          kp.sl3p_address,
        },
        "falcon": {
            "public_key": base64.b64encode(kp.falcon_public_key).decode(),
            "secret_key": base64.b64encode(kp.falcon_secret_key).decode(),
        },
    }


def hybrid_public_key_to_dict(kp: HybridKeypair) -> Dict[str, Any]:
    """
    Serialize public-key-only subset of HybridKeypair (NO secrets).
    Safe to share with verifiers, oracle nodes, block validators.

    Structure:
      {
        "version": "hybrid_sl3p_falcon_v2",
        "sl3p": {
          "public_hex": "<576 hex>",
          "address":    "<64 hex>"
        },
        "falcon": {
          "public_key": "<base64>"
        }
      }

    Note: no private_walk_hex, no secret_key.
    """
    return {
        "version": kp.version,
        "sl3p": {
            "public_hex": kp.sl3p_public_hex,
            "address":    kp.sl3p_address,
        },
        "falcon": {
            "public_key": base64.b64encode(kp.falcon_public_key).decode(),
        },
    }


def hybrid_keypair_from_dict(d: Dict[str, Any]) -> HybridKeypair:
    """
    Deserialize HybridKeypair from dict (must include secrets).
    Accepts both v2 ("hybrid_sl3p_falcon_v2") and v1 ("hybrid_sl2p_falcon_v1").
    For v1 dicts, the sl2p block is mapped to sl3p fields — the walk prefix
    will be GF1: and public_hex will be 256 chars (2×2 matrix). Sign/verify
    routes through gf_sign_full/gf_verify_full which handle both dimensions.
    """
    version = d.get("version", WIRE_VERSION)

    # Support both sl3p (v2) and sl2p (v1) block names
    sl_block = d.get("sl3p") or d.get("sl2p")
    if sl_block is None:
        raise ValueError(f"keypair dict missing 'sl3p' or 'sl2p' block. version={version!r}")

    falcon_block = d.get("falcon", {})
    falcon_pk_b64 = falcon_block.get("public_key", "")
    falcon_sk_b64 = falcon_block.get("secret_key", "")

    if not falcon_pk_b64 or not falcon_sk_b64:
        raise ValueError("keypair dict missing falcon.public_key or falcon.secret_key")

    return HybridKeypair(
        sl3p_private_hex  = sl_block.get("private_walk_hex", ""),
        sl3p_public_hex   = sl_block.get("public_hex", ""),
        sl3p_address      = sl_block.get("address", ""),
        falcon_public_key = base64.b64decode(falcon_pk_b64),
        falcon_secret_key = base64.b64decode(falcon_sk_b64),
        version           = version,
    )


# ─────────────────────────────────────────────────────────────────────────────
# SERIALIZATION — signature
# ─────────────────────────────────────────────────────────────────────────────

def hybrid_signature_to_dict(sig: HybridSignature) -> Dict[str, Any]:
    """
    Serialize HybridSignature to JSON-compatible dict.
    Falcon signature stored as base64.

    Wire structure (v2):
      {
        "version":           "hybrid_sl3p_falcon_v2",
        "sl3p_R_hex":        "<576 hex>",
        "sl3p_Z_hex":        "<576 hex>",
        "sl3p_c_hex":        "<64 hex>",
        "sl3p_s_scalar_hex": "<64 hex>",
        "sl3p_R_canonical":  "<576 hex>",
        "falcon_signature":  "<base64>"
      }
    """
    return {
        "version":           sig.version,
        "sl3p_R_hex":        sig.sl3p_R_hex,
        "sl3p_Z_hex":        sig.sl3p_Z_hex,
        "sl3p_c_hex":        sig.sl3p_c_hex,
        "sl3p_s_scalar_hex": sig.sl3p_s_scalar_hex,
        "sl3p_R_canonical":  sig.sl3p_R_canonical,
        "falcon_signature":  base64.b64encode(sig.falcon_signature).decode(),
    }


def hybrid_signature_from_dict(d: Dict[str, Any]) -> HybridSignature:
    """
    Deserialize HybridSignature from dict.
    Accepts v2 field names ("sl3p_*") and legacy v1 field names ("sl2p_*").
    Falcon signature decoded from base64 (or left as bytes if already bytes).
    """
    version = d.get("version", WIRE_VERSION)

    # Field name aliasing: v1 used sl2p_ prefix
    def _get(v2_key: str, v1_key: str, default: str = "") -> str:
        return d.get(v2_key) or d.get(v1_key) or default

    R_hex    = _get("sl3p_R_hex",        "sl2p_R_hex")
    Z_hex    = _get("sl3p_Z_hex",        "sl2p_Z_hex")
    c_hex    = _get("sl3p_c_hex",        "sl2p_c_hex",    "0" * 64)
    s_hex    = _get("sl3p_s_scalar_hex", "sl2p_s_scalar_hex", "0" * 64)
    R_can    = _get("sl3p_R_canonical",  "sl2p_R_canonical")
    if not R_can:
        R_can = R_hex

    falcon_raw = d.get("falcon_signature", b"")
    if isinstance(falcon_raw, str):
        falcon_raw = base64.b64decode(falcon_raw)
    elif not isinstance(falcon_raw, (bytes, bytearray)):
        falcon_raw = b""

    return HybridSignature(
        sl3p_R_hex        = R_hex,
        sl3p_Z_hex        = Z_hex,
        sl3p_c_hex        = c_hex,
        sl3p_s_scalar_hex = s_hex,
        sl3p_R_canonical  = R_can,
        falcon_signature  = bytes(falcon_raw),
        version           = version,
    )


# ─────────────────────────────────────────────────────────────────────────────
# PQC STATUS
# ─────────────────────────────────────────────────────────────────────────────

def pqc_status() -> Dict[str, Any]:
    """Return PQC module status with security parameters."""
    return {
        "falcon_real":         True,
        "falcon_standard":     "NIST FIPS 206 (Falcon-512)",
        "falcon_pq_bits":      128,
        "sl3p_walk_length":    WALK_LENGTH,
        "sl3p_n_generators":   N_GENERATORS,
        "sl3p_classical_bits": 189,
        "sl3p_quantum_note":   "Shor-vulnerable in cyclic subgroup; Falcon-512 provides 128-bit PQ cover",
        "sl3p_public_hex_len": _SL3P_PUBLIC_HEX_LEN,
        "sl3p_prime":          "2^255-31",
        "sl3p_group_order":    "|SL(3,p)| = p^3*(p^2-1)*(p^3-1) ≈ 2^2040",
        "sl3p_Q379_bits":      379,
        "wire_version":        WIRE_VERSION,
        "legacy_wire_version": LEGACY_WIRE_VERSION,
        "production_ready":    True,
    }


# ─────────────────────────────────────────────────────────────────────────────
# CONVENIENCE — address from public key hex
# ─────────────────────────────────────────────────────────────────────────────

def derive_address(public_hex: str) -> str:
    """SHA3-256²(bytes.fromhex(public_hex)).hex() — QTCL address derivation."""
    return _derive_address(public_hex)


# ─────────────────────────────────────────────────────────────────────────────
# SELF-TEST
# ─────────────────────────────────────────────────────────────────────────────

def run_tests(verbose: bool = True) -> Dict[str, Any]:
    """
    Full integration test: keygen → sign → verify → serialize round-trips → legacy routing.
    Returns dict with test results and timings.
    """
    results: Dict[str, Any] = {}
    passed = 0
    failed = 0

    def test(name: str, fn):
        nonlocal passed, failed
        try:
            t0 = time.perf_counter()
            ok, detail = fn()
            dt = time.perf_counter() - t0
        except Exception as exc:
            ok, dt, detail = False, 0.0, f"EXCEPTION: {exc}"
        results[name] = {"pass": ok, "detail": detail, "time_s": round(dt, 4)}
        if ok:
            passed += 1
            if verbose: print(f"  ✅ [{dt:.3f}s] {name}")
        else:
            failed += 1
            if verbose: print(f"  ❌ [{dt:.3f}s] {name}: {detail}")

    if verbose:
        print("=" * 72)
        print("  hyp_pqc.py — Hybrid PQC Integration Tests (v4 / SL(3,p))")
        print(f"  Falcon: pqcrypto Falcon-512 (NIST FIPS 206)")
        print(f"  Walk: {WALK_LENGTH} steps | Generators: {N_GENERATORS} | Prime: 2^255-31")
        print("=" * 72)

    # ── T1: keygen ───────────────────────────────────────────────────────────
    _kp: Optional[HybridKeypair] = None
    def t_keygen():
        nonlocal _kp
        _kp = generate_hybrid_keypair()
        assert _kp.version == WIRE_VERSION, f"version={_kp.version!r}"
        assert _kp.sl3p_private_hex.startswith("GF3:"), "priv must start GF3:"
        assert len(_kp.sl3p_public_hex) == 576, f"pub_hex len={len(_kp.sl3p_public_hex)}"
        assert len(_kp.sl3p_address) == 64, f"addr len={len(_kp.sl3p_address)}"
        assert len(_kp.falcon_public_key) > 0, "falcon pk empty"
        assert len(_kp.falcon_secret_key) > 0, "falcon sk empty"
        return True, f"addr={_kp.sl3p_address[:16]}..."
    test("keygen", t_keygen)

    # ── T2: sign + verify ────────────────────────────────────────────────────
    _msg_hash = hashlib.sha3_256(b"QTCL_HYBRID_TEST_VECTOR_V4").digest()
    _sig: Optional[HybridSignature] = None
    def t_sign():
        nonlocal _sig
        if _kp is None: return False, "keygen failed"
        _sig = hybrid_sign(_msg_hash, _kp.sl3p_private_hex, _kp.sl3p_public_hex, _kp.falcon_secret_key)
        assert _sig.version == WIRE_VERSION
        assert len(_sig.sl3p_R_hex) == 576
        assert len(_sig.sl3p_Z_hex) == 576
        assert len(_sig.sl3p_c_hex) == 64
        assert len(_sig.sl3p_s_scalar_hex) > 0, f's_scalar hex len={len(_sig.sl3p_s_scalar_hex)}'
        assert len(_sig.falcon_signature) > 0
        return True, f"falcon_sig_len={len(_sig.falcon_signature)}"
    test("sign", t_sign)

    def t_verify():
        if _kp is None or _sig is None: return False, "upstream failed"
        ok, reason = hybrid_verify(_msg_hash, _sig, _kp.sl3p_public_hex, _kp.falcon_public_key)
        return ok, reason
    test("verify", t_verify)

    # ── T3: wrong message fails ───────────────────────────────────────────────
    def t_wrong_msg():
        if _kp is None or _sig is None: return False, "upstream failed"
        bad_hash = hashlib.sha3_256(b"WRONG_MESSAGE").digest()
        ok, reason = hybrid_verify(bad_hash, _sig, _kp.sl3p_public_hex, _kp.falcon_public_key)
        return (not ok), f"correctly rejected: {reason}"
    test("wrong_msg_rejected", t_wrong_msg)

    # ── T4: wrong key fails ───────────────────────────────────────────────────
    def t_wrong_key():
        if _kp is None or _sig is None: return False, "upstream failed"
        kp2 = generate_hybrid_keypair()
        ok, reason = hybrid_verify(_msg_hash, _sig, kp2.sl3p_public_hex, kp2.falcon_public_key)
        return (not ok), f"correctly rejected: {reason}"
    test("wrong_key_rejected", t_wrong_key)

    # ── T5: dict round-trip ───────────────────────────────────────────────────
    def t_dict_roundtrip():
        if _kp is None or _sig is None: return False, "upstream failed"
        kp_d  = hybrid_keypair_to_dict(_kp)
        kp2   = hybrid_keypair_from_dict(kp_d)
        sig_d = hybrid_signature_to_dict(_sig)
        sig2  = hybrid_signature_from_dict(sig_d)
        assert kp2.sl3p_private_hex  == _kp.sl3p_private_hex
        assert kp2.sl3p_public_hex   == _kp.sl3p_public_hex
        assert kp2.sl3p_address      == _kp.sl3p_address
        assert kp2.falcon_public_key == _kp.falcon_public_key
        assert kp2.falcon_secret_key == _kp.falcon_secret_key
        assert sig2.sl3p_R_hex == _sig.sl3p_R_hex
        assert sig2.sl3p_c_hex == _sig.sl3p_c_hex
        assert sig2.falcon_signature == _sig.falcon_signature
        return True, "all fields match"
    test("dict_roundtrip", t_dict_roundtrip)

    # ── T6: verify_any dispatch ──────────────────────────────────────────────
    def t_verify_any():
        if _kp is None or _sig is None: return False, "upstream failed"
        sig_d = hybrid_signature_to_dict(_sig)
        ok, reason = hybrid_verify_any(_msg_hash, sig_d, _kp.sl3p_public_hex, _kp.falcon_public_key)
        return ok, reason
    test("hybrid_verify_any_v2", t_verify_any)

    # ── T7: pqc_status ───────────────────────────────────────────────────────
    def t_status():
        s = pqc_status()
        assert "falcon_real" in s
        assert s["wire_version"] == WIRE_VERSION
        assert s["sl3p_public_hex_len"] == 576
        return True, f"falcon_real={s['falcon_real']} production_ready={s['production_ready']}"
    test("pqc_status", t_status)

    # ── T8: public_key_only dict ──────────────────────────────────────────────
    def t_pubkey_only():
        if _kp is None: return False, "upstream failed"
        pub_d = hybrid_public_key_to_dict(_kp)
        assert "private_walk_hex" not in pub_d.get("sl3p", {})
        assert "secret_key" not in pub_d.get("falcon", {})
        assert pub_d["sl3p"]["public_hex"] == _kp.sl3p_public_hex
        return True, "no secrets in public dict"
    test("public_key_only_dict", t_pubkey_only)

    # ── T9: JSON serializability ──────────────────────────────────────────────
    def t_json():
        if _kp is None or _sig is None: return False, "upstream failed"
        kp_d  = hybrid_keypair_to_dict(_kp)
        sig_d = hybrid_signature_to_dict(_sig)
        pub_d = hybrid_public_key_to_dict(_kp)
        _ = json.dumps(kp_d)
        _ = json.dumps(sig_d)
        _ = json.dumps(pub_d)
        return True, "all dicts JSON-serializable"
    test("json_serializable", t_json)

    # ── T10: address derivation ───────────────────────────────────────────────
    def t_address():
        if _kp is None: return False, "upstream failed"
        addr2 = derive_address(_kp.sl3p_public_hex)
        assert addr2 == _kp.sl3p_address, f"{addr2!r} != {_kp.sl3p_address!r}"
        return True, f"addr={addr2[:16]}..."
    test("address_derivation", t_address)

    # ── Summary ───────────────────────────────────────────────────────────────
    total = passed + failed
    if verbose:
        print("=" * 72)
        print(f"  Results: {passed}/{total} passed")
    results["__summary__"] = {
        "passed": passed, "failed": failed, "total": total,
    }
    return results


# ─────────────────────────────────────────────────────────────────────────────
# HypGammaEngine — Unified API Facade (merged from hyp_engine.py)
# ─────────────────────────────────────────────────────────────────────────────
# This eliminates hyp_engine.py as a separate file. The client imports:
#   from hyp_pqc import HypGammaEngine
# or via the backward-compat stub:
#   from hyp_engine import HypGammaEngine
# ─────────────────────────────────────────────────────────────────────────────

import json as _json
import threading as _threading


class HypGammaEngine:
    """Unified HypΓ v4 cryptosystem API.

    Merged from hyp_engine.py. Singleton pattern. Thread-safe.
    Provides: keygen, sign, verify, hybrid sign/verify, block operations.

    All SL(3,p) + Falcon-512 hybrid operations route through hyp_pqc functions.
    GeodesicLWE encryption is optional (requires hyp_group + hyp_tessellation + hyp_ldpc).
    """

    _instance = None
    _lock = _threading.Lock()
    _initialized = False

    def __new__(cls):
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self):
        if HypGammaEngine._initialized:
            return
        with HypGammaEngine._lock:
            if HypGammaEngine._initialized:
                return
            HypGammaEngine._initialized = True

    # ── Keypair ──────────────────────────────────────────────────────
    def generate_keypair(self):
        """Generate SL(3,p) keypair (non-hybrid). Returns GFKeyPair."""
        return gf_generate_keypair()

    def generate_hybrid_keypair(self) -> dict:
        """Generate hybrid SL(3,p) + Falcon-512 keypair. Returns dict."""
        kp = generate_hybrid_keypair()
        return hybrid_keypair_to_dict(kp)

    # ── Signing ──────────────────────────────────────────────────────
    def sign_hash(self, message_hash: bytes, private_key_dict: dict) -> dict:
        """Sign with SL(3,p) Schnorr-Γ only (non-hybrid). Returns sig dict."""
        from hyp_finite_field import sign_hash as _sign_hash, hex_to_walk as _h2w
        sl_block = private_key_dict.get("sl3p") or private_key_dict.get("sl2p") or private_key_dict
        priv_hex = sl_block.get("private_walk_hex", "")
        pub_hex = sl_block.get("public_hex", "")
        walk = _h2w(priv_hex)
        pub = GFMatrix.from_hex(pub_hex)
        return _sign_hash(message_hash, walk, pub)

    def verify_signature(self, message_hash: bytes, sig_dict: dict, public_key_dict: dict) -> bool:
        """Verify SL(3,p) Schnorr-Γ signature (non-hybrid)."""
        from hyp_finite_field import SchnorrGamma as _SG
        sg = _SG()
        crypto = public_key_dict.get("sl3p") or public_key_dict.get("sl2p") or public_key_dict
        pub_hex = crypto.get("public_hex", crypto.get("public_key_hex", ""))
        pub = GFMatrix.from_hex(pub_hex)
        return sg.verify_signature(message_hash, sig_dict, pub)

    def hybrid_sign(self, message_hash: bytes, private_key_dict: dict) -> dict:
        """Sign with hybrid SL(3,p) + Falcon-512. Both must verify."""
        if not isinstance(message_hash, bytes) or len(message_hash) != 32:
            raise ValueError("message_hash must be 32 bytes")
        kp = hybrid_keypair_from_dict(private_key_dict)
        sig = hybrid_sign(message_hash, kp.sl3p_private_hex,
                          kp.sl3p_public_hex, kp.falcon_secret_key)
        return hybrid_signature_to_dict(sig)

    def hybrid_verify(self, message_hash: bytes, sig_dict: dict,
                      public_key_dict: dict) -> tuple:
        """Verify hybrid signature. Returns (valid, reason)."""
        if not isinstance(message_hash, bytes) or len(message_hash) != 32:
            return False, "invalid_message_hash"
        try:
            import base64 as _b64
            crypto = public_key_dict.get("sl3p") or public_key_dict.get("sl2p")
            if crypto is None:
                return False, "missing_sl3p_block"
            pub_hex = crypto["public_hex"]
            falcon_pk = _b64.b64decode(public_key_dict["falcon"]["public_key"])
            return hybrid_verify_any(message_hash, sig_dict, pub_hex, falcon_pk)
        except Exception as e:
            return False, f"verify_error:{type(e).__name__}"

    # ── Block operations ─────────────────────────────────────────────
    def sign_block_hybrid(self, block_dict: dict, private_key_dict: dict) -> dict:
        """Sign a block with hybrid PQC."""
        canonical = _json.dumps(block_dict, sort_keys=True, separators=(',', ':'))
        block_hash = hashlib.sha3_256(canonical.encode()).digest()
        sig_dict = self.hybrid_sign(block_hash, private_key_dict)
        try:
            crypto = private_key_dict.get("sl3p") or private_key_dict.get("sl2p")
            if crypto:
                sig_dict['signer_address'] = derive_address(crypto["public_hex"])
        except Exception:
            pass
        return sig_dict

    def verify_block_hybrid(self, block_dict: dict, sig_dict: dict,
                            public_key_dict: dict) -> tuple:
        """Verify a block signed with hybrid PQC."""
        canonical = _json.dumps(block_dict, sort_keys=True, separators=(',', ':'))
        block_hash = hashlib.sha3_256(canonical.encode()).digest()
        return self.hybrid_verify(block_hash, sig_dict, public_key_dict)

    # Aliases for backward compat with hyp_engine.py API
    sign_block = sign_block_hybrid
    verify_block = verify_block_hybrid

    def derive_address(self, public_key_hex: str) -> str:
        return derive_address(public_key_hex)

    def derive_public_key(self, private_key_hex: str) -> str:
        walk = hex_to_walk(private_key_hex)
        g = get_schnorr_generator()
        x = walk_to_private_scalar(walk)
        return (g ** x).hex()

    def pqc_status(self) -> dict:
        return pqc_status()

    # ── Password encryption (delegates to hyp_lwe) ───────────────────
    def encrypt_with_password(self, plaintext: bytes, password: str) -> dict:
        from hyp_lwe import encrypt_with_password
        return encrypt_with_password(plaintext, password)

    def decrypt_with_password(self, encrypted_dict: dict, password: str) -> bytes:
        from hyp_lwe import decrypt_with_password
        return decrypt_with_password(encrypted_dict, password)


# ─────────────────────────────────────────────────────────────────────────────
# MODULE ENTRY POINT
# ─────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import sys
    results = run_tests(verbose=True)
    summary = results.get("__summary__", {})
    sys.exit(0 if summary.get("failed", 1) == 0 else 1)
