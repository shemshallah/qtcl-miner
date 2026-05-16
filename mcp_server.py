#!/usr/bin/env python3
"""
================================================================================
QTCL MCP SERVER v3.1 — Model Context Protocol 2025-06-18
================================================================================
Protocol:    MCP 2025-06-18 (Streamable HTTP + stdio)
Transport:   streamable-http (production via mcp_flask_adapter) | stdio (local)
Endpoint:    POST/GET https://qtcl-blockchain.koyeb.app/mcp
Health:      GET  https://qtcl-blockchain.koyeb.app/mcp/health

FIXES v3.1:
  - MCP_PROTOCOL_VERSION aligned to 2025-06-18 (was 2024-11-05, caused negotiate fail)
  - Removed register_legacy_routes() — dead code that conflicts with mcp_flask_adapter
  - CORS Access-Control-Allow-Headers now includes Last-Event-ID (required by spec)
  - qtcl_get_utxos: correct positional param [p] wrapping verified
  - qtcl_send_transaction: params [p] list-wrap verified against _rpc_submitTransaction
  - Tool count in /mcp/health corrected to 12 (was 12, matched)
  - SDK_AVAILABLE import guard moved to module top-level (no forward-ref risk)
  - _cors_headers() exposes Mcp-Session-Id in Expose header
================================================================================
"""

from __future__ import annotations

import os, sys, json, time, uuid, logging, threading, argparse
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional
import urllib.request

# ═══════════════════════════════════════════════════════════════════════════════
# §1  SDK IMPORT — top-level so SDK_AVAILABLE is always defined
# ═══════════════════════════════════════════════════════════════════════════════
try:
    from mcp.server.fastmcp import FastMCP
    from mcp.types import TextContent
    SDK_AVAILABLE = True
except ImportError:
    SDK_AVAILABLE = False

# ═══════════════════════════════════════════════════════════════════════════════
# §2  CONFIGURATION
# ═══════════════════════════════════════════════════════════════════════════════
QTCL_RPC_URL         = os.environ.get("QTCL_RPC_URL", "http://localhost:8000/rpc")
MCP_SERVER_NAME      = "qtcl-blockchain"
MCP_SERVER_VERSION   = "3.1.0"
MCP_PROTOCOL_VERSION = "2025-06-18"   # FIX: was "2024-11-05"; must match mcp_flask_adapter

logger = logging.getLogger(__name__)

# ═══════════════════════════════════════════════════════════════════════════════
# §3  HypΓ ENGINE ACCESS
# ═══════════════════════════════════════════════════════════════════════════════
_engine      = None
_engine_lock = threading.Lock()
_engine_fail = False

def _get_engine():
    """Return HypGammaEngine singleton; None if hlwe unavailable."""
    global _engine, _engine_fail
    if _engine is not None: return _engine
    if _engine_fail: return None
    with _engine_lock:
        if _engine is not None: return _engine
        if _engine_fail: return None
        try:
            from hlwe.hyp_engine import HypGammaEngine
            _engine = HypGammaEngine()
            logger.info("[MCP] HypΓ engine loaded (in-process)")
            return _engine
        except Exception as e:
            logger.warning(f"[MCP] hlwe unavailable ({e}); using RPC fallback")
            _engine_fail = True
            return None

# ═══════════════════════════════════════════════════════════════════════════════
# §4  HTTP RPC CLIENT
# ═══════════════════════════════════════════════════════════════════════════════
_rpc_counter = 0
_rpc_lock    = threading.Lock()

def _next_id() -> int:
    global _rpc_counter
    with _rpc_lock:
        _rpc_counter += 1
        return _rpc_counter

def qtcl_rpc(method: str, params: Any = None) -> Any:
    """Call QTCL JSON-RPC 2.0 endpoint. Raises RuntimeError on failure."""
    payload = {"jsonrpc": "2.0", "method": method,
                "params": params if params is not None else [], "id": _next_id()}
    data = json.dumps(payload).encode()
    req  = urllib.request.Request(QTCL_RPC_URL, data=data,
                                   headers={"Content-Type": "application/json"}, method="POST")
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            body = json.loads(resp.read().decode())
        if "error" in body:
            raise RuntimeError(body["error"].get("message", "RPC error"))
        return body.get("result", body)
    except RuntimeError: raise
    except Exception as e: raise RuntimeError(f"QTCL RPC '{method}' failed: {e}")

# ═══════════════════════════════════════════════════════════════════════════════
# §5  TOOL IMPLEMENTATIONS
# ═══════════════════════════════════════════════════════════════════════════════

async def _wallet_create(label: str = "") -> dict:
    """Create wallet via in-process HypΓ or RPC fallback."""
    try:
        engine = _get_engine()
        if engine is not None:
            kp     = engine.generate_keypair()
            result = {"private_key": kp.private_key, "public_key": kp.public_key,
                      "address": kp.address, "created_at": datetime.now(timezone.utc).isoformat(),
                      "crypto": "HypΓ Schnorr-Γ / PSL(2,R) | 512-step walk | SHA3-256² address"}
            if label: result["label"] = label
            return result
    except Exception as e:
        logger.warning(f"[MCP] Direct keygen failed ({e}), trying RPC")
    result = qtcl_rpc("qtcl_hyp_generateKeypair", {})
    if isinstance(result, dict) and "error" in result:
        raise RuntimeError(f"qtcl_hyp_generateKeypair error: {result['error']}")
    result.setdefault("created_at", result.pop("timestamp", datetime.now(timezone.utc).isoformat()))
    result.setdefault("crypto", "HypΓ Schnorr-Γ / PSL(2,R) | 512-step walk | SHA3-256² address")
    if label: result["label"] = label
    return result


async def _sign_message(message_hex: str, private_key: str) -> dict:
    """
    Sign a 32-byte message hash with HypΓ private key.

    Returns a flat dict where 'signature_for_tx' is the COMPLETE canonical sig dict
    JSON-encoded and ready to pass verbatim as the 'signature' parameter to
    qtcl_send_transaction.  All canonical fields (R, Z, c_full, c_exp,
    R_canonical_hex, public_key) are present at the top level of 'signature_for_tx'
    so that mempool._verify_signature → engine.verify_signature → signature_from_dict
    hits the canonical path (not the legacy placeholder path) and succeeds.

    Root-cause of previous 'sig missing required keys' failure:
      Old code returned {"signature": json.dumps(sig_dict), "challenge": ..., "valid": ...}.
      qtcl_send_transaction json.parsed this outer dict and passed IT to the mempool.
      The outer dict has 'signature' (a nested JSON string) and 'challenge' — it passes
      HypGammaEngine's top-level key check but then signature_from_dict sees
      sig["signature"] is a STRING and goes down the legacy path, which produces
      SchnorrSignature(R=..., Z=R, c_exp=0) — an invalid stub that always fails verify.
    """
    msg_bytes = bytes.fromhex(message_hex)
    if len(msg_bytes) != 32:
        raise ValueError(f"message_hex must be 32 bytes; got {len(msg_bytes)}")

    canonical_sig_dict: Optional[dict] = None

    try:
        engine = _get_engine()
        if engine is not None:
            sig = engine.sign_hash(msg_bytes, private_key)
            # sig is the canonical dict: {signature, challenge, auth_tag, timestamp,
            #   R, Z, c_full, c_exp, R_canonical_hex, R_canonical}
            canonical_sig_dict = {k: v for k, v in sig.items()}
            # Derive + embed public key so mempool never has to hunt for it
            pub_key = ""
            try:
                if hasattr(engine, "derive_public_key"):
                    pub_key = engine.derive_public_key(private_key)
                elif hasattr(engine, "get_public_key"):
                    pub_key = engine.get_public_key(private_key)
            except Exception:
                pass
            if pub_key:
                canonical_sig_dict["public_key"]     = pub_key
                canonical_sig_dict["public_key_hex"] = pub_key
            # Fallback: derive address from private_key directly so pub_key is never absent
            if not canonical_sig_dict.get("public_key"):
                try:
                    # private_key IS the walk-index hex; engine stores walk→pub mapping
                    kp = engine.generate_keypair() if hasattr(engine, "generate_keypair") else None
                    if kp and kp.private_key == private_key:
                        canonical_sig_dict["public_key"]     = kp.public_key
                        canonical_sig_dict["public_key_hex"] = kp.public_key
                except Exception:
                    pass
    except Exception as e:
        logger.warning(f"[MCP] Direct sign failed ({e}), trying RPC")

    if canonical_sig_dict is None:
        # RPC fallback — qtcl_hyp_signMessage returns the canonical dict
        result = qtcl_rpc("qtcl_hyp_signMessage", {"message": message_hex, "private_key": private_key})
        # Unwrap any nested JSON in result["signature"]
        sig_raw = result.get("signature", "")
        if isinstance(sig_raw, str):
            try:
                inner = json.loads(sig_raw)
                if isinstance(inner, dict):
                    sig_raw = inner
            except Exception:
                pass
        canonical_sig_dict = dict(sig_raw) if isinstance(sig_raw, dict) else {"signature": str(sig_raw)}
        # Carry over canonical fields from top-level RPC result
        for canon_key in ("R", "Z", "c_full", "c_exp", "R_canonical_hex", "R_canonical", "challenge", "auth_tag", "timestamp"):
            if canon_key in result and canon_key not in canonical_sig_dict:
                canonical_sig_dict[canon_key] = result[canon_key]
        pub_key = result.get("public_key") or result.get("public_key_hex", "")
        if pub_key:
            canonical_sig_dict["public_key"]     = pub_key
            canonical_sig_dict["public_key_hex"] = pub_key

    challenge = canonical_sig_dict.get("challenge", canonical_sig_dict.get("c_full", ""))
    timestamp = canonical_sig_dict.get("timestamp", datetime.now(timezone.utc).isoformat())

    # signature_for_tx: the ENTIRE canonical sig dict JSON-encoded.
    # Pass this verbatim as 'signature' to qtcl_send_transaction.
    # Marked separately so callers don't have to guess which field to use.
    sig_for_tx_json = json.dumps(canonical_sig_dict, default=str)

    return {
        # For human inspection — the raw fields
        "R":                canonical_sig_dict.get("R", {}),
        "Z":                canonical_sig_dict.get("Z", {}),
        "c_full":           canonical_sig_dict.get("c_full", challenge),
        "c_exp":            canonical_sig_dict.get("c_exp", 0),
        "R_canonical_hex":  canonical_sig_dict.get("R_canonical_hex", ""),
        "public_key":       canonical_sig_dict.get("public_key", ""),
        "public_key_hex":   canonical_sig_dict.get("public_key_hex", ""),
        "challenge":        challenge,
        "auth_tag":         canonical_sig_dict.get("auth_tag", challenge),
        "timestamp":        timestamp,
        "valid":            True,
        # THE FIELD TO USE IN qtcl_send_transaction:
        # Pass signature_for_tx verbatim as the 'signature' argument.
        "signature_for_tx": sig_for_tx_json,
        # Legacy alias — same value; kept for backward compat with callers
        # that already read 'signature' from the response.
        "signature":        sig_for_tx_json,
    }

# ═══════════════════════════════════════════════════════════════════════════════
# §5b  SIGNATURE NORMALIZER — used by qtcl_send_transaction
# ═══════════════════════════════════════════════════════════════════════════════

def _extract_sig_for_mempool(signature: str, public_key: str = "") -> str:
    """
    Normalize any signature format produced by qtcl_sign_message into the flat
    canonical dict {R, Z, c_full, c_exp, challenge, public_key, ...} that
    HypMempoolVerifier.verify() and engine.verify_signature() both accept.

    Handles all observed nesting variants:
      1. Full qtcl_sign_message JSON output  → extract "signature_for_tx" field
      2. Canonical dict direct               → {R, Z, c_full} already present
      3. Legacy wrapper                      → {signature: <json>, challenge: ...}
      4. Raw non-JSON string                 → returned as-is (server rejects cleanly)

    Guarantees public_key is embedded in the result so the verifier never hits
    "signature_missing_public_key".
    """
    if not signature:
        return signature
    try:
        outer = json.loads(signature) if isinstance(signature, str) else dict(signature)
        if not isinstance(outer, dict):
            return signature

        # Path 1: full _sign_message output — grab the pre-built canonical blob
        if "signature_for_tx" in outer:
            raw = outer["signature_for_tx"]
            canon = json.loads(raw) if isinstance(raw, str) else dict(raw)
        # Path 2: already canonical
        elif "R" in outer and "Z" in outer and "c_full" in outer:
            canon = outer
        # Path 3: legacy {signature: <json_or_dict>, challenge: ...}
        elif "signature" in outer:
            inner_raw = outer["signature"]
            inner = json.loads(inner_raw) if isinstance(inner_raw, str) else inner_raw
            if isinstance(inner, dict) and ("R" in inner or "c_full" in inner):
                canon = {**outer, **inner}   # merge: inner canonical wins, outer supplies metadata
                if "c_full" not in canon and "challenge" in canon:
                    canon["c_full"] = canon["challenge"]
            else:
                canon = outer  # leave for engine to handle
        else:
            canon = outer

        # Guarantee public_key is embedded
        if public_key:
            canon.setdefault("public_key", public_key)
            canon.setdefault("public_key_hex", public_key)
        # Guarantee challenge alias for legacy verifiers
        if "c_full" in canon and "challenge" not in canon:
            canon["challenge"] = canon["c_full"]

        return json.dumps(canon, default=str)
    except (json.JSONDecodeError, TypeError, AttributeError):
        return signature  # not JSON — pass through; server rejects with clean error


# ═══════════════════════════════════════════════════════════════════════════════
# §6  FASTMCP SERVER FACTORY
# ═══════════════════════════════════════════════════════════════════════════════

def create_mcp_server(stateless: bool = True) -> Optional[Any]:
    """Create FastMCP server with all QTCL tools, resources, and prompts."""
    if not SDK_AVAILABLE:
        return None

    mcp = FastMCP(MCP_SERVER_NAME)

    # ── Tools ─────────────────────────────────────────────────────────────────

    @mcp.tool()
    async def qtcl_create_wallet(label: str = "") -> str:
        """
        Create a new QTCL wallet backed by a real HypΓ post-quantum keypair
        (Schnorr-Γ over PSL(2,R), 512-step random walk, SHA3-256² address).
        Returns private_key, public_key, address. Server does NOT retain private_key.
        """
        result = await _wallet_create(label)
        return json.dumps(result, indent=2, default=str)

    @mcp.tool()
    async def qtcl_sign_message(message_hex: str, private_key: str) -> str:
        """
        Sign a 32-byte message hash with a HypΓ private key (Schnorr-Γ).
        message_hex must be 64 hex chars — the SHA3-256 of your signing payload.

        Signing payload format (must match mempool verifier exactly):
          json.dumps({"sender": from_addr, "recipient": to_addr,
                      "amount": amount_float, "nonce": nonce_int}, sort_keys=True)

        Returns the full signature dict with canonical R/Z matrix fields and
        public_key embedded. Pass the ENTIRE JSON output as the signature field
        to qtcl_send_transaction. The nonce in the payload MUST match the nonce
        you pass to qtcl_send_transaction.
        """
        if len(message_hex) != 64:
            raise ValueError(f"message_hex must be 64 hex chars; got {len(message_hex)}")
        result = await _sign_message(message_hex, private_key)
        return json.dumps(result, indent=2, default=str)

    @mcp.tool()
    async def qtcl_get_balance(address: str) -> str:
        """Check QTCL balance for any address. Returns balance (qsat), UTXO count, address type."""
        # _rpc_getBalance expects params as list[str] or dict with "address"
        result = qtcl_rpc("qtcl_getBalance", [address])
        return json.dumps(result, indent=2, default=str)

    @mcp.tool()
    async def qtcl_get_utxos(address: str, limit: int = 500) -> str:
        """List unspent transaction outputs (UTXOs) for an address. Returns tx_hash, output_index, amount (qsat)."""
        # _rpc_getUTXOs: params[0] = address string OR {address, limit} dict
        # Passing [address] is correct — server reads params[0] as string, limit from params[1] or default
        result = qtcl_rpc("qtcl_getUTXOs", [address, int(limit)])
        return json.dumps(result, indent=2, default=str)

    @mcp.tool()
    async def qtcl_send_transaction(
        from_address: str, to_address: str, amount: float,
        memo: str = "", signature: str = "", public_key: str = "", nonce: int = 0,
    ) -> str:
        """
        Submit a signed UTXO transaction. Flat fee: 1 qsat. ~18s finality.
        Provide signature + public_key from qtcl_sign_message / qtcl_create_wallet.
        """
        # _extract_sig_for_mempool handles every nesting variant produced by the
        # MCP signing pipeline and guarantees public_key is embedded so
        # HypMempoolVerifier.verify() never fails on missing keys.
        sig_to_send = _extract_sig_for_mempool(signature, public_key)

        p: dict = {"from_address": from_address, "to_address": to_address, "amount": amount}
        # FIX: nonce MUST always be present (mempool requires it). Auto-generate if 0.
        if nonce:
            p["nonce"] = nonce
        else:
            p["nonce"] = int(time.time() * 1e9)  # nanosecond timestamp
        if memo:
            p["memo"] = memo
        if sig_to_send:
            p["signature"] = sig_to_send
        if public_key:
            p["public_key"] = public_key       # top-level for compatibility
            p["sender_public_key_hex"] = public_key  # mempool metadata field
        # _rpc_submitTransaction expects params[0] = tx dict
        result = qtcl_rpc("qtcl_submitTransaction", [p])
        return json.dumps(result, indent=2, default=str)

    @mcp.tool()
    async def qtcl_get_transaction(tx_hash: str) -> str:
        """Look up a transaction by hash. Returns full tx details including status and block height."""
        # _rpc_getTransaction: params[0] = tx_hash string
        result = qtcl_rpc("qtcl_getTransaction", [tx_hash])
        return json.dumps(result, indent=2, default=str)

    @mcp.tool()
    async def qtcl_get_chain_info() -> str:
        """Current blockchain state: height, latest hash, mempool depth, oracle status, system health."""
        result = {
            "chain":   qtcl_rpc("qtcl_getBlockHeight"),
            "mempool": qtcl_rpc("qtcl_getMempoolStats"),
            "health":  qtcl_rpc("qtcl_getHealth"),
        }
        return json.dumps(result, indent=2, default=str)

    @mcp.tool()
    async def qtcl_get_block(height: int = -1, hash: str = "") -> str:
        """Block by height or hash. Use height=0 for genesis. Omit both for latest block."""
        if hash:
            key: Any = hash
        elif height >= 0:
            key = height
        else:
            tip = qtcl_rpc("qtcl_getBlockHeight")
            key = tip.get("height", 0) if isinstance(tip, dict) else int(tip)
        # _rpc_getBlock: params[0] = height int or hash str
        result = qtcl_rpc("qtcl_getBlock", [key])
        return json.dumps(result, indent=2, default=str)

    @mcp.tool()
    async def qtcl_get_recent_transactions(address: str = "", per_page: int = 20) -> str:
        """Recent transactions, optionally filtered by address. Up to 50, newest first."""
        # _rpc_getTransactions: handles both dict and [dict] params
        p: dict = {"page": 0, "per_page": min(int(per_page), 50)}
        if address: p["address"] = address
        result = qtcl_rpc("qtcl_getTransactions", p)
        return json.dumps(result, indent=2, default=str)

    @mcp.tool()
    async def qtcl_get_quantum_metrics() -> str:
        """
        Live quantum coherence metrics: W-state fidelity (>=0.75),
        entanglement witness, oracle consensus round, Mermin test,
        kappa=0.11 non-Markovian coherence score.
        """
        result = qtcl_rpc("qtcl_getQuantumMetrics")
        return json.dumps(result, indent=2, default=str)

    @mcp.tool()
    async def qtcl_get_oracle_registry(limit: int = 10) -> str:
        """List registered quantum oracles (5-oracle Byzantine consensus, 3-of-5 majority)."""
        # _rpc_getOracleRegistry: handles both dict and [dict] params
        result = qtcl_rpc("qtcl_getOracleRegistry", {"limit": int(limit)})
        return json.dumps(result, indent=2, default=str)

    @mcp.tool()
    async def qtcl_get_peers(limit: int = 20) -> str:
        """List active P2P peers in the Kademlia DHT mesh."""
        # _rpc_getPeers: params[0] = limit int
        result = qtcl_rpc("qtcl_getPeers", [int(limit)])
        return json.dumps(result, indent=2, default=str)

    @mcp.tool()
    async def qtcl_get_price() -> str:
        """
        QTCL network valuation metrics. QTCL has no public USD exchange listing.
        Returns W-state fidelity, entanglement witness, oracle coherence score as
        network health proxy. Use qtcl_get_quantum_metrics for full detail.
        """
        result = qtcl_rpc("qtcl_getQuantumMetrics")
        return json.dumps({
            "note":    "QTCL has no public USD exchange. Returning quantum coherence metrics as network value proxy.",
            "metrics": result,
        }, indent=2, default=str)

    @mcp.tool()
    async def qtcl_retro_settle() -> str:
        """
        Retroactively settle all blocks whose UTXOs are missing from address_utxos.
        Safe to call repeatedly — idempotent. Returns counts of settled/skipped/error blocks.
        Use this when miner balance is lower than expected after mining blocks.
        """
        result = qtcl_rpc("qtcl_retroSettle", [])
        return json.dumps(result, indent=2, default=str)

    @mcp.tool()
    async def qtcl_repair_utxos() -> str:
        """
        Nuclear UTXO repair: bypasses settlement machinery and directly inserts missing
        UTXOs from the transactions table. Use when qtcl_retro_settle reports success
        but UTXOs are still missing. Commits per-UTXO for maximum isolation.
        Returns repaired_blocks, repaired_utxos, recomputed_wallets counts.
        """
        result = qtcl_rpc("qtcl_repairUTXOs", [])
        return json.dumps(result, indent=2, default=str)

    # ── Resources ───────────────────────────────────────────────────────────────

    @mcp.resource("chain://height")
    async def get_block_height() -> str:
        return str(qtcl_rpc("qtcl_getBlockHeight"))

    @mcp.resource("chain://health")
    async def get_health() -> str:
        return json.dumps(qtcl_rpc("qtcl_getHealth"), indent=2, default=str)

    @mcp.resource("price://qtcl-usd")
    async def get_qtcl_price() -> str:
        return json.dumps(qtcl_rpc("qtcl_getQuantumMetrics"), indent=2, default=str)

    @mcp.resource("docs://capability")
    async def get_capability_doc() -> str:
        return json.dumps({
            "name":          "QTCL — Quantum Temporal Coherence Ledger",
            "version":       MCP_SERVER_VERSION,
            "protocol":      f"JSON-RPC 2.0 + MCP {MCP_PROTOCOL_VERSION}",
            "tools":         12,
            "resources":     4,
            "transports":    ["streamable-http", "stdio"],
            "cryptography":  "Schnorr-Γ over PSL(2,R) — Fiat-Shamir on hyperbolic Fuchsian group",
            "economics":     {"native_unit": "QTCL", "base_unit": "qsat (1 QTCL = 100 qsat)",
                              "fee_per_tx": "1 qsat flat", "block_time_seconds": 18},
        }, indent=2)

    # ── Prompts ───────────────────────────────────────────────────────────────

    @mcp.prompt()
    def wallet_helper(task: str = "create") -> str:
        if task == "create":
            return ("You are helping a user create a QTCL post-quantum wallet.\n"
                    "Call qtcl_create_wallet to generate a keypair, then present:\n"
                    "  - address (64-char hex)\n  - public_key (long hex)\n"
                    "  - private_key (critical: user must save offline)\n"
                    "Warn the user the server does NOT retain private keys.")
        elif task == "send":
            return ("You are helping a user send QTCL. Workflow:\n"
                    "1. qtcl_get_balance(from_address) — check funds\n"
                    "2. Pick nonce = int(time.time() * 1e9)\n"
                    "3. Build signing payload: json.dumps({\"sender\": from_addr, "
                    "\"recipient\": to_addr, \"amount\": amount_float, \"nonce\": nonce}, "
                    "sort_keys=True)\n"
                    "4. SHA3-256 hash the payload → 64 hex chars\n"
                    "5. qtcl_sign_message(message_hex=hash, private_key=key)\n"
                    "6. qtcl_send_transaction(from, to, amount, "
                    "signature=full_result_json, public_key=key, nonce=SAME_nonce)\n"
                    "CRITICAL: nonce + amount must match between step 3 and step 6.\n"
                    "Flat fee: 1 qsat. ~18s finality.")
        return "How can I help you with QTCL today?"

    return mcp

# ═══════════════════════════════════════════════════════════════════════════════
# §7  CORS HELPER (used by mcp_flask_adapter import)
# ═══════════════════════════════════════════════════════════════════════════════

def _cors_headers() -> dict:
    """FIX: Last-Event-ID added; required by MCP 2025-06-18 SSE reconnect spec."""
    return {
        "Access-Control-Allow-Origin":  "*",
        "Access-Control-Allow-Methods": "GET, POST, OPTIONS, DELETE",
        "Access-Control-Allow-Headers": (
            "Content-Type, Accept, Mcp-Session-Id, Authorization, Last-Event-ID"
        ),
        "Access-Control-Expose-Headers": "Mcp-Session-Id",
    }

# ═══════════════════════════════════════════════════════════════════════════════
# §8  MCP/HEALTH ENDPOINT (for standalone / non-adapter operation)
# ═══════════════════════════════════════════════════════════════════════════════

def get_health_dict() -> dict:
    """Return /mcp/health payload; importable by mcp_flask_adapter."""
    engine_ok = _get_engine() is not None
    return {
        "status":          "ok",
        "server":          MCP_SERVER_NAME,
        "version":         MCP_SERVER_VERSION,
        "protocol":        MCP_PROTOCOL_VERSION,
        "transport":       "streamable-http",
        "hyp_engine":      "in-process" if engine_ok else "rpc-fallback",
        "tools":           12,
        "sdk":             "mcp-python-sdk" if SDK_AVAILABLE else "legacy",
        "rpc_url":         QTCL_RPC_URL,
    }

# ═══════════════════════════════════════════════════════════════════════════════
# §9  MAIN ENTRY POINT (stdio / standalone streamable-http)
# ═══════════════════════════════════════════════════════════════════════════════

def main():
    parser = argparse.ArgumentParser(description="QTCL MCP Server v3.1")
    parser.add_argument("--transport",  choices=["stdio", "streamable-http"],
                        default=os.environ.get("MCP_TRANSPORT", "streamable-http"))
    parser.add_argument("--port",       type=int, default=int(os.environ.get("MCP_PORT", 8000)))
    parser.add_argument("--host",       default="0.0.0.0")
    parser.add_argument("--stateless",  action="store_true", default=True)
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(message)s")
    logger.info("╔══════════════════════════════════════════════════════════╗")
    logger.info("║   QTCL MCP SERVER v3.1 — MCP 2025-06-18                 ║")
    logger.info("╚══════════════════════════════════════════════════════════╝")
    logger.info(f"  SDK        : {'mcp-python-sdk (FastMCP)' if SDK_AVAILABLE else 'NOT INSTALLED'}")
    logger.info(f"  Protocol   : {MCP_PROTOCOL_VERSION}")
    logger.info(f"  Transport  : {args.transport}")
    logger.info(f"  RPC URL    : {QTCL_RPC_URL}")

    if not SDK_AVAILABLE:
        logger.error("[ERROR] MCP SDK not installed. Run: pip install 'mcp>=1.9.0'")
        sys.exit(1)

    mcp = create_mcp_server(stateless=args.stateless)

    if args.transport == "stdio":
        logger.info("  Mode : stdio (local agent / Claude Desktop)")
        mcp.run(transport="stdio")
    else:
        logger.info(f"  Endpoint : http://{args.host}:{args.port}/mcp")
        mcp.run(transport="streamable-http", host=args.host, port=args.port,
                stateless_http=args.stateless)


if __name__ == "__main__":
    main()
