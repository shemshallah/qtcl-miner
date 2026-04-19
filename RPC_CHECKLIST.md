# QTCL RPC System - Implementation Checklist

**Status:** Critical Infrastructure  
**Deadline:** Must be fixed before mining can proceed  
**Date:** 2026-04-19

---

## Quick Diagnosis: Why You're Getting HTTP 503

**The Problem:**
```
[2026-04-19 12:21:00,471] ERROR: [SNAPSHOT-RPC] 💥 HTTP 503
RuntimeError: RPC endpoint error: HTTP 503
```

**Root Cause:** One of three issues:
1. ❌ Server `/rpc` endpoint doesn't exist
2. ❌ Server is crashing before returning response
3. ❌ Middleware/proxy is returning 503 instead of forwarding to `/rpc` handler

**Solution:** Follow the checklists below to ensure both sides match exactly.

---

## CLIENT-SIDE VERIFICATION (qtcl_client.py)

### ✓ What the Client is Sending (VERIFIED)

**Request Format:**
```json
POST {ENTROPY_SERVER_URL}/rpc
Content-Type: application/json

{
    "jsonrpc": "2.0",
    "method": "qtcl_getHealth",
    "params": [],
    "id": 1
}
```

**This is CORRECT.** The client follows JSON-RPC 2.0 spec.

### ✓ What the Client Expects Back (VERIFIED)

**Success Response (HTTP 200):**
```json
{
    "jsonrpc": "2.0",
    "result": {
        "status": "healthy",
        "uptime_seconds": 86400,
        "peer_count": 5,
        "synced": true
    },
    "id": 1
}
```

**Error Response (HTTP 200, NOT 503):**
```json
{
    "jsonrpc": "2.0",
    "error": {
        "code": -32601,
        "message": "Method not found"
    },
    "id": 1
}
```

**Critical Rules:**
- ✓ Always HTTP 200 (even for errors)
- ✓ Always include `jsonrpc: "2.0"`
- ✓ Always include matching `id`
- ✓ Either `result` OR `error`, never both

---

## SERVER-SIDE REQUIREMENTS

### ❌ What Your Server MUST Implement

The server at `$ENTROPY_SERVER_URL` must have a `/rpc` endpoint.

**Required Methods:**
- [ ] `qtcl_getHealth` - Returns server health status
- [ ] `qtcl_getBlockHeight` - Returns current block height
- [ ] `qtcl_getBlock` - Returns block data
- [ ] `qtcl_getPeers` - Returns peer list
- [ ] `qtcl_getLatestDMSnapshot` - Returns oracle snapshot
- [ ] `qtcl_getMyAddr` - Returns address/wallet
- [ ] `qtcl_getMempool` - Returns pending transactions

### ❌ Required Endpoint Configuration

**Endpoint Path:**
```
POST /rpc
```

**Request Handling:**
```python
@app.post("/rpc")
def handle_json_rpc(request):
    """Handle JSON-RPC 2.0 requests."""
    
    try:
        # Parse request
        payload = await request.json()
        method = payload.get("method")
        params = payload.get("params", [])
        request_id = payload.get("id", 1)
        
        # Validate format
        if payload.get("jsonrpc") != "2.0":
            return {
                "jsonrpc": "2.0",
                "error": {
                    "code": -32600,
                    "message": "Invalid Request"
                },
                "id": request_id
            }, 200  # HTTP 200!
        
        # Execute method
        if method == "qtcl_getHealth":
            result = handle_get_health()
        elif method == "qtcl_getBlockHeight":
            result = handle_get_block_height(*params)
        # ... more methods ...
        else:
            return {
                "jsonrpc": "2.0",
                "error": {
                    "code": -32601,
                    "message": f"Method not found: {method}"
                },
                "id": request_id
            }, 200  # HTTP 200 even for unknown method!
        
        # Return success
        return {
            "jsonrpc": "2.0",
            "result": result,
            "id": request_id
        }, 200
    
    except Exception as e:
        # Even on exception, return HTTP 200 with error
        return {
            "jsonrpc": "2.0",
            "error": {
                "code": -32603,
                "message": str(e)
            },
            "id": payload.get("id", 1)
        }, 200
```

**Critical Points:**
- [ ] **NEVER return HTTP 503**
- [ ] **ALWAYS return HTTP 200 for /rpc requests**
- [ ] Set `Content-Type: application/json`
- [ ] Include all required JSON-RPC fields
- [ ] Handle exceptions gracefully with error responses

---

## DEBUGGING STEPS

### Step 1: Verify Server is Reachable

```bash
# Try to connect
curl -X POST http://$ENTROPY_SERVER_URL/rpc \
  -H "Content-Type: application/json" \
  -d '{"jsonrpc":"2.0","method":"qtcl_getHealth","params":[],"id":1}'
```

**Expected Output (HTTP 200):**
```json
{
    "jsonrpc": "2.0",
    "result": {
        "status": "healthy"
    },
    "id": 1
}
```

**If you get HTTP 503:** The `/rpc` endpoint is not configured.

---

### Step 2: Check Server Logs

**Look for:**
- Is `/rpc` route registered?
- Is POST handler defined?
- Are there exceptions being thrown?

**Example log entry (you should see this):**
```
[INFO] POST /rpc - 200 OK
[DEBUG] Method: qtcl_getHealth, Result: {"status": "healthy"}
```

**Bad log entry (fix this):**
```
[ERROR] POST /rpc - 503 Service Unavailable
[ERROR] Unhandled exception in /rpc handler
```

---

### Step 3: Test Each Method

```bash
# Test qtcl_getHealth
curl -X POST http://$ENTROPY_SERVER_URL/rpc \
  -H "Content-Type: application/json" \
  -d '{"jsonrpc":"2.0","method":"qtcl_getHealth","params":[],"id":1}'

# Test qtcl_getBlockHeight
curl -X POST http://$ENTROPY_SERVER_URL/rpc \
  -H "Content-Type: application/json" \
  -d '{"jsonrpc":"2.0","method":"qtcl_getBlockHeight","params":[],"id":1}'

# Test invalid method (should return JSON error, not HTTP 503)
curl -X POST http://$ENTROPY_SERVER_URL/rpc \
  -H "Content-Type: application/json" \
  -d '{"jsonrpc":"2.0","method":"invalid_method","params":[],"id":1}'
```

**All should return HTTP 200.**

---

### Step 4: Use the Verification Script

```bash
cd /home/shemshallah/qtcl-miner
python3 verify_rpc_spec.py --server http://$ENTROPY_SERVER_URL
```

This will test all compliance requirements.

---

## COMMON MISTAKES (Fix These)

### ❌ Mistake 1: Returning HTTP 503 for Unknown Methods

**WRONG:**
```
POST /rpc → HTTP 503 Service Unavailable
<HTML error page>
```

**RIGHT:**
```
POST /rpc → HTTP 200 OK
{
    "jsonrpc": "2.0",
    "error": {
        "code": -32601,
        "message": "Method not found"
    },
    "id": 1
}
```

### ❌ Mistake 2: Not Including jsonrpc Field

**WRONG:**
```json
{
    "result": {"status": "healthy"}
}
```

**RIGHT:**
```json
{
    "jsonrpc": "2.0",
    "result": {"status": "healthy"},
    "id": 1
}
```

### ❌ Mistake 3: Returning Both result and error

**WRONG:**
```json
{
    "jsonrpc": "2.0",
    "result": {"status": "healthy"},
    "error": {"code": -1, "message": "Something went wrong"},
    "id": 1
}
```

**RIGHT:** (pick one)
```json
{
    "jsonrpc": "2.0",
    "result": {"status": "healthy"},
    "id": 1
}
```

### ❌ Mistake 4: Not Setting Content-Type Header

**WRONG:**
```
HTTP/1.1 200 OK
Content-Length: 45

{"jsonrpc":"2.0","result":{...},"id":1}
```

**RIGHT:**
```
HTTP/1.1 200 OK
Content-Type: application/json
Content-Length: 45

{"jsonrpc":"2.0","result":{...},"id":1}
```

---

## ENVIRONMENT VARIABLES

Make sure these are set correctly:

```bash
# The server URL must have the /rpc endpoint
export ENTROPY_SERVER_URL="http://your-server.com:8000"
export ORACLE_URL="http://your-server.com:8000"

# Verify in client
python3 -c "from qtcl_client import ENTROPY_SERVER_URL; print(f'Using: {ENTROPY_SERVER_URL}/rpc')"
```

---

## IMPLEMENTATION TEMPLATE (Python/Flask)

```python
from flask import Flask, request, jsonify

app = Flask(__name__)

# Registry of RPC methods
RPC_METHODS = {
    "qtcl_getHealth": lambda: {
        "status": "healthy",
        "uptime_seconds": 86400
    },
    "qtcl_getBlockHeight": lambda: {
        "height": 12345,
        "hash": "0x...",
        "difficulty": "0x..."
    },
    # Add all required methods...
}

@app.post("/rpc")
def handle_rpc():
    """JSON-RPC 2.0 handler."""
    try:
        # Parse request
        payload = request.get_json() or {}
        method = payload.get("method")
        params = payload.get("params", [])
        request_id = payload.get("id", 1)
        
        # Validate JSON-RPC format
        if payload.get("jsonrpc") != "2.0":
            return jsonify({
                "jsonrpc": "2.0",
                "error": {
                    "code": -32600,
                    "message": "Invalid Request"
                },
                "id": request_id
            }), 200
        
        # Check if method exists
        if method not in RPC_METHODS:
            return jsonify({
                "jsonrpc": "2.0",
                "error": {
                    "code": -32601,
                    "message": f"Method not found: {method}"
                },
                "id": request_id
            }), 200
        
        # Execute method
        try:
            result = RPC_METHODS[method](*params)
            return jsonify({
                "jsonrpc": "2.0",
                "result": result,
                "id": request_id
            }), 200
        except TypeError as e:
            return jsonify({
                "jsonrpc": "2.0",
                "error": {
                    "code": -32602,
                    "message": f"Invalid params: {str(e)}"
                },
                "id": request_id
            }), 200
    
    except Exception as e:
        return jsonify({
            "jsonrpc": "2.0",
            "error": {
                "code": -32603,
                "message": f"Internal error: {str(e)}"
            },
            "id": 1
        }), 200  # ALWAYS HTTP 200

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8000)
```

---

## VALIDATION CHECKLIST

Before declaring RPC "fixed", verify all:

- [ ] Server has `/rpc` endpoint
- [ ] Endpoint accepts POST requests
- [ ] Endpoint returns HTTP 200 for all requests
- [ ] All responses include `jsonrpc: "2.0"`
- [ ] All responses include matching `id`
- [ ] All responses have either `result` OR `error`
- [ ] Invalid methods return JSON error (not HTTP 503)
- [ ] Server exceptions return JSON error (not HTTP 503)
- [ ] Content-Type is `application/json`
- [ ] All required methods are implemented
- [ ] curl test passes
- [ ] verify_rpc_spec.py passes
- [ ] Client mining starts without RPC errors

---

## NEXT STEPS

1. **Run the verification script:**
   ```bash
   python3 verify_rpc_spec.py --server http://$ENTROPY_SERVER_URL
   ```

2. **Fix any failures** according to the checklist above

3. **Test with client:**
   ```bash
   python3 qtcl_client.py
   ```

4. **Monitor logs:**
   - Client should show `[INFO] RPC initialized`
   - No HTTP 503 errors
   - Blocks should start getting mined

---

## Support

For RPC issues, check:
1. `RPC_SPECIFICATION.md` - Full protocol spec
2. `verify_rpc_spec.py` - Automated validator
3. This document - Implementation checklist

All three files are in `/home/shemshallah/qtcl-miner/`

