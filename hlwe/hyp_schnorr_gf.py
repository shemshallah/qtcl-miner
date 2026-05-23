"""hyp_schnorr_gf.py — Backward-compat stub. All code merged into hyp_finite_field.py."""
from hyp_finite_field import (
    GFMatrix, GFKeyPair, GFSchnorrSignature, gf_sign_full, gf_verify_full,
    gf_generate_keypair, evaluate_walk, random_walk, walk_to_hex, hex_to_walk,
    walk_to_bytes, WALK_LENGTH, N_GENERATORS, get_schnorr_generator,
    walk_to_private_scalar, SchnorrGamma, SchnorrError, HypSignature,
    signature_to_dict, signature_from_dict, sign_hash,
    WIRE_VERSION, LEGACY_WIRE_VERSION,
)
# Re-export generator_list for any code that imports it from here
from hyp_finite_field import generator_list
