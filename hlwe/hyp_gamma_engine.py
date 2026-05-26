#!/usr/bin/env python3
"""
hyp_gamma_engine.py — HypGammaEngine standalone shim.

Exists because hyp_pqc.py on the deployed server may be a cached version
that predates the HypGammaEngine class. This file provides the class
independently so server.py can do:

    try:
        from hyp_pqc import HypGammaEngine
    except ImportError:
        from hyp_gamma_engine import HypGammaEngine

Or server.py can just import from here directly.
"""
from __future__ import annotations
import hashlib, json as _json, threading as _threading, logging
from hyp_pqc import (
    generate_hybrid_keypair, hybrid_sign, hybrid_verify, hybrid_verify_any,
    hybrid_keypair_to_dict, hybrid_public_key_to_dict, hybrid_keypair_from_dict,
    hybrid_signature_to_dict, pqc_status, derive_address,
    GFMatrix, GFKeyPair, gf_generate_keypair, hex_to_walk,
    walk_to_private_scalar, get_schnorr_generator,
)

logger = logging.getLogger(__name__)

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
