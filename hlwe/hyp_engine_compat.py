#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
╔══════════════════════════════════════════════════════════════════════════════╗
║                       hyp_engine_compat.py                                   ║
║         Compatibility Layer: HypΓ → Server, Mempool, Oracle                  ║
║                                                                              ║
║  Drop-in replacement for hlwe_engine. Wraps hyp_engine and fixes import    ║
║  mismatches. Single responsibility: make HypΓ crypto available to QTCL.     ║
╚══════════════════════════════════════════════════════════════════════════════╝
"""

from __future__ import annotations

import os
import sys
import json
import hashlib
import secrets
import logging
import threading
import time
from typing import Dict, Optional, Any, NamedTuple, Tuple
from datetime import datetime, timezone

# ═══════════════════════════════════════════════════════════════════════════
# §0 ADD HYP SUBDIRECTORY TO SYS.PATH (allow imports from ~/hlwe/hyp_* modules)
# ═══════════════════════════════════════════════════════════════════════════

_REPO_ROOT = os.path.dirname(os.path.abspath(__file__))
_HYP_DIR = os.path.join(_REPO_ROOT, 'hlwe')
if _HYP_DIR not in sys.path:
    sys.path.insert(0, _HYP_DIR)

logger = logging.getLogger(__name__)

# ═══════════════════════════════════════════════════════════════════════════
# §1 FIX HYPER_GROUP EXPORTS — Add missing serialize_matrix function
# ═══════════════════════════════════════════════════════════════════════════

def _patch_hyp_group():
    """Inject serialize_matrix() into hyp_group module if missing."""
    try:
        import hyp_group
        if not hasattr(hyp_group, 'serialize_matrix'):
            def serialize_matrix(matrix: 'hyp_group.PSLMatrix') -> bytes:
                """Serialize PSLMatrix to bytes using serialize_canonical method."""
                return matrix.serialize_canonical()
            hyp_group.serialize_matrix = staticmethod(serialize_matrix)
            logger.debug("[HYP-COMPAT] Injected serialize_matrix shim into hyp_group")
    except Exception as e:
        logger.warning(f"[HYP-COMPAT] Could not patch hyp_group: {e}")

# ═══════════════════════════════════════════════════════════════════════════
# §2 FIX HYPER_TESSELLATION EXPORTS — Add missing error class
# ═══════════════════════════════════════════════════════════════════════════

def _patch_hyp_tessellation():
    """Inject TessellationError into hyp_tessellation module if missing."""
    try:
        import hyp_tessellation
        if not hasattr(hyp_tessellation, 'TessellationError'):
            class TessellationError(Exception):
                """Base error for tessellation operations."""
                pass
            hyp_tessellation.TessellationError = TessellationError
            logger.debug("[HYP-COMPAT] Injected TessellationError into hyp_tessellation")
    except Exception as e:
        logger.warning(f"[HYP-COMPAT] Could not patch hyp_tessellation: {e}")

# ═══════════════════════════════════════════════════════════════════════════
# §3 FIX HYP_LDPC EXPORTS — Add missing error class and get_ldpc_code
# ═══════════════════════════════════════════════════════════════════════════

def _patch_hyp_ldpc():
    """Inject HLSD_Error and get_ldpc_code into hyp_ldpc module if missing."""
    try:
        import hyp_ldpc
        if not hasattr(hyp_ldpc, 'HLSD_Error'):
            class HLSD_Error(Exception):
                """LDPC decoding error."""
                pass
            hyp_ldpc.HLSD_Error = HLSD_Error
            logger.debug("[HYP-COMPAT] Injected HLSD_Error into hyp_ldpc")
        
        if not hasattr(hyp_ldpc, 'get_ldpc_code'):
            def get_ldpc_code() -> 'hyp_ldpc.LDPCCode':
                """Lazy-load canonical LDPC code."""
                if not hasattr(get_ldpc_code, '_instance'):
                    get_ldpc_code._instance = hyp_ldpc.LDPCCode()
                return get_ldpc_code._instance
            hyp_ldpc.get_ldpc_code = get_ldpc_code
            logger.debug("[HYP-COMPAT] Injected get_ldpc_code shim into hyp_ldpc")
    except Exception as e:
        logger.warning(f"[HYP-COMPAT] Could not patch hyp_ldpc: {e}")

# ═══════════════════════════════════════════════════════════════════════════
# §4 FIX HYP_SCHNORR EXPORTS — Map SchnorrSignature → HypSignature
# ═══════════════════════════════════════════════════════════════════════════

def _patch_hyp_schnorr():
    """Inject HypSignature alias into hyp_schnorr module if missing."""
    try:
        import hyp_schnorr
        if not hasattr(hyp_schnorr, 'HypSignature'):
            # Alias: HypSignature = SchnorrSignature if it exists
            if hasattr(hyp_schnorr, 'SchnorrSignature'):
                hyp_schnorr.HypSignature = hyp_schnorr.SchnorrSignature
                logger.debug("[HYP-COMPAT] Aliased HypSignature → SchnorrSignature")
            else:
                # Fallback: create minimal stub
                from typing import NamedTuple
                class HypSignature(NamedTuple):
                    signature: str
                    challenge: str
                    auth_tag: str
                    timestamp: str
                hyp_schnorr.HypSignature = HypSignature
                logger.debug("[HYP-COMPAT] Created HypSignature stub")
        
        if not hasattr(hyp_schnorr, 'SchnorrError'):
            class SchnorrError(Exception):
                """Schnorr-Γ operation error."""
                pass
            hyp_schnorr.SchnorrError = SchnorrError
            logger.debug("[HYP-COMPAT] Injected SchnorrError into hyp_schnorr")
    except Exception as e:
        logger.warning(f"[HYP-COMPAT] Could not patch hyp_schnorr: {e}")

# ═══════════════════════════════════════════════════════════════════════════
# §5 FIX HYP_LWE EXPORTS — Add missing error class
# ═══════════════════════════════════════════════════════════════════════════

def _patch_hyp_lwe():
    """Inject LWEError into hyp_lwe module if missing."""
    try:
        import hyp_lwe
        if not hasattr(hyp_lwe, 'LWEError'):
            class LWEError(Exception):
                """GeodesicLWE encryption/decryption error."""
                pass
            hyp_lwe.LWEError = LWEError
            logger.debug("[HYP-COMPAT] Injected LWEError into hyp_lwe")
    except Exception as e:
        logger.warning(f"[HYP-COMPAT] Could not patch hyp_lwe: {e}")

# ═══════════════════════════════════════════════════════════════════════════
# §6 APPLY ALL PATCHES AT MODULE LOAD TIME
# ═══════════════════════════════════════════════════════════════════════════

_patch_hyp_group()
_patch_hyp_tessellation()
_patch_hyp_ldpc()
_patch_hyp_schnorr()
_patch_hyp_lwe()

# ═══════════════════════════════════════════════════════════════════════════
# §7 LOAD HYP_ENGINE (now with fixed imports)
# ═══════════════════════════════════════════════════════════════════════════

try:
    from hyp_engine import (
        HypGammaEngine, HypKeyPair, HypSignature as HypEngineSignature,
        HypEngineError
    )
    _HYP_ENGINE_AVAILABLE = True
    logger.info("[HYP-COMPAT] ✅ HypΓ engine loaded with fixed exports")
except ImportError as e:
    _HYP_ENGINE_AVAILABLE = False
    logger.error(f"[HYP-COMPAT] ❌ Cannot import hyp_engine: {e}")
    HypGammaEngine = None
    HypKeyPair = None
    HypEngineSignature = None
    HypEngineError = None

# ═══════════════════════════════════════════════════════════════════════════
# §8 BACKWARD-COMPAT ALIASES (for existing code)
# ═══════════════════════════════════════════════════════════════════════════

# Alias for old code that refers to HLWEEngine
HLWEEngine = HypGammaEngine

# Re-export HypKeyPair under old names if needed
if HypKeyPair:
    HLWEKeyPair = HypKeyPair

# ═══════════════════════════════════════════════════════════════════════════
# §9 SYSTEM INFO STUB (for /rpc/hlwe/system-info endpoint)
# ═══════════════════════════════════════════════════════════════════════════

def hlwe_system_info() -> Dict[str, Any]:
    """Return system info (backward compat for wsgi_config.py)."""
    return {
        "crypto_system": "HypΓ (hyp_engine)",
        "engine_type": "HypGammaEngine",
        "available": _HYP_ENGINE_AVAILABLE,
        "version": "1.0",
        "timestamp": time.time(),
    }

# ═══════════════════════════════════════════════════════════════════════════
# §10 LAZY SINGLETON INSTANCE (thread-safe)
# ═══════════════════════════════════════════════════════════════════════════

_ENGINE_INSTANCE: Optional[HypGammaEngine] = None
_ENGINE_LOCK = threading.Lock()

def get_hyp_engine() -> HypGammaEngine:
    """Get or create the HypΓ engine singleton."""
    global _ENGINE_INSTANCE
    if _ENGINE_INSTANCE is not None:
        return _ENGINE_INSTANCE
    with _ENGINE_LOCK:
        if _ENGINE_INSTANCE is not None:
            return _ENGINE_INSTANCE
        if not _HYP_ENGINE_AVAILABLE:
            raise RuntimeError("HypΓ engine not available (import failed)")
        _ENGINE_INSTANCE = HypGammaEngine()
        logger.info("[HYP-COMPAT] HypΓ engine singleton instantiated")
        return _ENGINE_INSTANCE

# Alias for old code
def get_hlwe_engine() -> HypGammaEngine:
    """Backward-compat alias for get_hyp_engine()."""
    return get_hyp_engine()
