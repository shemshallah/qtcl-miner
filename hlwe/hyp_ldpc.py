"""hyp_ldpc.py — REMOVED (Red Team Finding 9).

LDPC codes were not used in any security-critical way. The error sampler
could not produce low-weight codewords as claimed. Removed to eliminate
security theater and reduce attack surface.

QTCL signing uses hyp_finite_field.py (SL(3,p) exact arithmetic).
QTCL encryption uses Falcon-512 (hyp_pqc.py).
"""
raise ImportError(
    "hyp_ldpc.py removed — LDPC constraint was never enforced in encryption. "
    "Use hyp_pqc.py for post-quantum signatures."
)
