# RPC System - Immediate Action Plan

**Status:** BLOCKING  
**Issue:** HTTP 503 when mining  
**Root Cause:** Server `/rpc` endpoint not properly configured  
**Time to Fix:** 30 minutes (if server code is available)

---

## The Problem (What You're Seeing)

```
[2026-04-19 12:21:00,471] ERROR: [SNAPSHOT-RPC] 💥 HTTP 503
RuntimeError: RPC endpoint error: HTTP 503
```

## The Root Cause

The client is correctly sending:
```
POST {ENTROPY_SERVER_URL}/rpc HTTP/1.1
Content-Type: application/json

{"jsonrpc":"2.0","method":"qtcl_getHealth","params":[],"id":1}
```

But the server is responding with:
```
HTTP 503 Service Unavailable
<html>...</html>
```

**Instead of:**
```
HTTP 200 OK
Content-Type: application/json

{"jsonrpc":"2.0","result":{"status":"healthy"},"id":1}
```

---

## What You Need to Fix

### Step 1: Locate Your Server Code

Find where the server is implemented:
```bash
# If it's in this repo:
find . -name "*.py" -o -name "*.go" -o -name "*.rs" | xargs grep -l "app\.post.*rpc\|@app\.post\|def.*rpc"

# If it's on a remote server:
ssh your-server 'find . -name "*rpc*" -o -name "*server*"'
```

### Step 2: Add the /rpc Endpoint

**If you don't have a /rpc handler**, add one. Example for Python Flask:

```python
@app.post("/rpc")
def handle_rpc():
    """JSON-RPC 2.0 handler."""
    try:
        payload = request.get_json() or {}
        method = payload.get("method")
        request_id = payload.get("id", 1)
        
        # Validate format
        if payload.get("jsonrpc") != "2.0":
            return jsonify({
                "jsonrpc": "2.0",
                "error": {"code": -32600, "message": "Invalid Request"},
                "id": request_id
            }), 200  # Always HTTP 200!
        
        # Implement required methods
        if method == "qtcl_getHealth":
            return jsonify({
                "jsonrpc": "2.0",
                "result": {"status": "healthy", "uptime_seconds": 86400},
                "id": request_id
            }), 200
        
        else:
            # Unknown method
            return jsonify({
                "jsonrpc": "2.0",
                "error": {"code": -32601, "message": f"Method not found"},
                "id": request_id
            }), 200  # Still HTTP 200!
    
    except Exception as e:
        # Even on crash, return JSON error
        return jsonify({
            "jsonrpc": "2.0",
            "error": {"code": -32603, "message": str(e)},
            "id": 1
        }), 200  # ALWAYS HTTP 200!
```

### Step 3: Implement All 7 Required Methods

See `RPC_SPECIFICATION.md` section 3 for exact request/response formats:

- [ ] `qtcl_getHealth` - Server health
- [ ] `qtcl_getBlockHeight` - Current block
- [ ] `qtcl_getBlock` - Block data
- [ ] `qtcl_getPeers` - Peer list
- [ ] `qtcl_getLatestDMSnapshot` - Oracle snapshot
- [ ] `qtcl_getMyAddr` - Your address
- [ ] `qtcl_getMempool` - Pending transactions

### Step 4: Test with curl

```bash
curl -X POST http://your-server:8000/rpc \
  -H "Content-Type: application/json" \
  -d '{"jsonrpc":"2.0","method":"qtcl_getHealth","params":[],"id":1}'
```

**You should see:**
```
HTTP 200 OK
Content-Type: application/json

{"jsonrpc":"2.0","result":{"status":"healthy",...},"id":1}
```

**You should NOT see:**
```
HTTP 503
HTTP 404
<html>error</html>
```

### Step 5: Test with Verification Script

```bash
python3 /home/shemshallah/qtcl-miner/verify_rpc_spec.py --server http://your-server:8000
```

All tests should pass (green checkmarks).

### Step 6: Restart Client

```bash
python3 qtcl_client.py
```

Should now connect without HTTP 503 errors.

---

## The Three Critical Rules

### Rule #1: Always Return HTTP 200

```
❌ WRONG: HTTP 503 Service Unavailable
❌ WRONG: HTTP 404 Not Found
❌ WRONG: HTTP 500 Internal Server Error

✅ RIGHT: HTTP 200 OK
```

Even if the method doesn't exist, return HTTP 200 with a JSON-RPC error:

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

### Rule #2: Always Include jsonrpc and id

```json
{
    "jsonrpc": "2.0",           ← REQUIRED
    "result": {...} or "error": {...},
    "id": 1                      ← REQUIRED (matches request)
}
```

### Rule #3: Either result OR error, Never Both

```json
✅ {"jsonrpc":"2.0","result":{},"id":1}
✅ {"jsonrpc":"2.0","error":{"code":-1,"message":""},"id":1}

❌ {"jsonrpc":"2.0","result":{},"error":{},"id":1}
```

---

## Files You Have

**Documentation:**
- `RPC_SPECIFICATION.md` - Complete protocol spec (must read)
- `RPC_CHECKLIST.md` - Implementation guide with examples
- `RPC_QUICK_REFERENCE.txt` - One-page cheat sheet
- `RPC_ACTION_PLAN.md` - This file

**Tools:**
- `verify_rpc_spec.py` - Automated validator

---

## If You Get Stuck

### Symptom: Still Getting HTTP 503

**Diagnose:**
```bash
# Test server is running
curl http://your-server:8000/

# Test /rpc endpoint exists
curl http://your-server:8000/rpc

# Check server logs
ssh your-server 'tail -f /var/log/your-app.log'
```

**Fix:** Make sure the /rpc route is registered in your server code.

### Symptom: /rpc returns HTML instead of JSON

**Cause:** Middleware is catching the error before reaching your handler.

**Fix:** Make sure /rpc is configured BEFORE any error-handling middleware.

### Symptom: Methods return wrong format

**Compare** your response to `RPC_SPECIFICATION.md` section 3.

Each method has an exact format defined. Your response must match exactly.

---

## Validation Checklist

Before declaring "RPC is fixed":

- [ ] `curl` to /rpc returns HTTP 200 (not 503)
- [ ] Response includes `"jsonrpc":"2.0"`
- [ ] Response includes `"id"` matching request
- [ ] Response has either `"result"` OR `"error"`
- [ ] Invalid method returns JSON error (not HTML)
- [ ] All 7 methods implemented
- [ ] `verify_rpc_spec.py` shows all ✓ (not ✗)
- [ ] Client starts mining without RPC errors

---

## Timeline

**30 minutes (if you have server code):**
1. Add /rpc endpoint (5 min)
2. Implement 7 methods (15 min)
3. Test with curl (5 min)
4. Run verify_rpc_spec.py (2 min)
5. Restart client (3 min)

**If server is remote and managed by someone else:**
1. Send them this action plan
2. Have them implement /rpc per RPC_SPECIFICATION.md
3. Run verify_rpc_spec.py to confirm
4. Restart client

---

## What Gets Fixed

✅ HTTP 503 errors disappear  
✅ Client can bootstrap oracle  
✅ Mining starts normally  
✅ Signature verification has proper data  

---

## Contact Points

If the server team needs help, send them:
1. `RPC_SPECIFICATION.md` - What they must implement
2. Python Flask template in `RPC_CHECKLIST.md` - How to implement
3. `verify_rpc_spec.py` - How to validate

Everything is self-contained and unambiguous.

---

**Go fix the RPC. This is blocking everything.**

