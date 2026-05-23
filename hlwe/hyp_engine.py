"""hyp_engine.py — Backward-compat stub. All code merged into hyp_pqc.py."""
from hyp_pqc import HypGammaEngine, pqc_status
from hyp_finite_field import SchnorrGamma, SchnorrError, HypSignature

class HypEngineError(Exception):
    """Backward-compat exception type."""
    pass
