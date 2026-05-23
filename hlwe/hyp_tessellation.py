"""hyp_tessellation.py — REMOVED (Red Team Finding 10).

The tessellation was only used by GeodesicLWE encryption, which was
ElGamal in disguise (not HCVP-based). Removed to eliminate security theater.

QTCL signing uses hyp_finite_field.py (SL(3,p) exact arithmetic).
QTCL encryption uses Falcon-512 (hyp_pqc.py).
"""
raise ImportError(
    "hyp_tessellation.py removed — GeodesicLWE was ElGamal in disguise. "
    "Use hyp_pqc.py for post-quantum signatures."
)
