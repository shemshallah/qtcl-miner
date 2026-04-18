#!/usr/bin/env python3
"""
hlwe package — HypΓ cryptosystem modules

Exposes hyp_engine and all supporting modules for top-level imports:
  from hyp_engine import HypGammaEngine
  from hyp_tessellation import ...
"""

# Re-export from hyp_engine so "from hyp_engine import X" works
try:
    from hlwe.hyp_engine import (
        HypGammaEngine,
        HypKeyPair,
        HypEngineError,
    )
    __all__ = ['HypGammaEngine', 'HypKeyPair', 'HypEngineError']
except ImportError as e:
    raise ImportError(f"Failed to import HypΓ engine components: {e}") from e
