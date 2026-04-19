# QTCL Standardized RPC Specification

**Version:** 1.0  
**Protocol:** JSON-RPC 2.0  
**Transport:** HTTP POST  
**Last Updated:** 2026-04-19

---

## 1. ENDPOINT CONFIGURATION

### Server Side (What Server Must Implement)

```
POST {BASE_URL}/rpc
Content-Type: application/json
```

### Client Side (What Client Sends)

```python
payload = {
    "jsonrpc": "2.0",
    "method": "<METHOD_NAME>",
    "params": [<PARAMS>],    # array of parameters
    "id": 1
}
```

---

## 2. REQUIRED RESPONSE FORMAT (Both Success and Error)

### Success Response (HTTP 200)
```json
{
    "jsonrpc": "2.0",
    "result": {
        // Method-specific result data
    },
    "id": 1
}
```

### Error Response (HTTP 200 with error field)
```json
{
    "jsonrpc": "2.0",
    "error": {
        "code": <INTEGER>,
        "message": "<ERROR_MESSAGE>",
        "data": {}
    },
    "id": 1
}
```

### Critical Requirements:
- **ALWAYS return HTTP 200** for valid JSON-RPC requests (even if result is an error)
- **NEVER return HTTP 503** for a valid endpoint
- **MUST include `id` field** matching the request
- **MUST include `jsonrpc: "2.0"`** field
- Return EITHER `"result"` OR `"error"`, never both

---

## 3. REQUIRED RPC METHODS

### 3.1 Chain Methods

#### `qtcl_getBlockHeight`
**Client Call:**
```json
{
    "jsonrpc": "2.0",
    "method": "qtcl_getBlockHeight",
    "params": [],
    "id": 1
}
```

**Server Response:**
```json
{
    "jsonrpc": "2.0",
    "result": {
        "height": 12345,
        "hash": "0x...",
        "difficulty": "0x..."
    },
    "id": 1
}
```

---

#### `qtcl_getBlock`
**Client Call:**
```json
{
    "jsonrpc": "2.0",
    "method": "qtcl_getBlock",
    "params": [12345],
    "id": 1
}
```

**Server Response:**
```json
{
    "jsonrpc": "2.0",
    "result": {
        "height": 12345,
        "hash": "0x...",
        "timestamp": 1234567890,
        "transactions": [...],
        "miner": "0x...",
        "nonce": "0x..."
    },
    "id": 1
}
```

---

### 3.2 Health & Status Methods

#### `qtcl_getHealth`
**Client Call:**
```json
{
    "jsonrpc": "2.0",
    "method": "qtcl_getHealth",
    "params": [],
    "id": 1
}
```

**Server Response:**
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

---

#### `qtcl_getPeers`
**Client Call:**
```json
{
    "jsonrpc": "2.0",
    "method": "qtcl_getPeers",
    "params": [{"limit": 50}],
    "id": 1
}
```

**Server Response:**
```json
{
    "jsonrpc": "2.0",
    "result": {
        "peers": [
            {
                "id": "node_id_1",
                "address": "192.168.1.1:9101",
                "version": "1.0",
                "last_seen": 1234567890
            }
        ]
    },
    "id": 1
}
```

---

### 3.3 Oracle Methods

#### `qtcl_getLatestDMSnapshot`
**Client Call:**
```json
{
    "jsonrpc": "2.0",
    "method": "qtcl_getLatestDMSnapshot",
    "params": [],
    "id": 1
}
```

**Server Response:**
```json
{
    "jsonrpc": "2.0",
    "result": {
        "snapshot_id": "snap_123",
        "timestamp": 1234567890,
        "oracle_data": {
            "w_state": "...",
            "pq0": "...",
            "pq_last": "..."
        }
    },
    "id": 1
}
```

---

#### `qtcl_getMyAddr`
**Client Call:**
```json
{
    "jsonrpc": "2.0",
    "method": "qtcl_getMyAddr",
    "params": [],
    "id": 1
}
```

**Server Response:**
```json
{
    "jsonrpc": "2.0",
    "result": {
        "address": "qtcl1f4a0af297d85473389b14835fbde2eda48d2e8a4",
        "wallet": "2c3a5dbef1926448a82c961cabaa7a185791a5d346fe98453fd54fb7e337365e"
    },
    "id": 1
}
```

---

### 3.4 Mempool Methods

#### `qtcl_getMempool`
**Client Call:**
```json
{
    "jsonrpc": "2.0",
    "method": "qtcl_getMempool",
    "params": [100],
    "id": 1
}
```

**Server Response:**
```json
{
    "jsonrpc": "2.0",
    "result": {
        "pending_transactions": [
            {
                "txid": "0x...",
                "from": "0x...",
                "to": "0x...",
                "amount": "1000000",
                "timestamp": 1234567890,
                "signature": "0x..."
            }
        ]
    },
    "id": 1
}
```

---

## 4. ERROR CODES (JSON-RPC Standard)

| Code | Message | Meaning |
|------|---------|---------|
| -32700 | Parse error | Invalid JSON sent |
| -32600 | Invalid Request | Request format incorrect |
| -32601 | Method not found | Method name doesn't exist |
| -32602 | Invalid params | Parameters don't match method signature |
| -32603 | Internal error | Server-side error |
| -32000 | Application error | Custom application error |

---

## 5. CLIENT IMPLEMENTATION CHECKLIST

### When Calling RPC:
- [ ] Construct JSON-RPC 2.0 payload with `jsonrpc: "2.0"`
- [ ] Include method name in `method` field
- [ ] Pass parameters as array in `params` field
- [ ] Include `id: 1` (or incrementing ID for multiple calls)
- [ ] POST to `{BASE_URL}/rpc` with `Content-Type: application/json`
- [ ] Set timeout to 5-10 seconds
- [ ] Handle both `result` and `error` fields in response
- [ ] Retry on network errors (not on HTTP 4xx errors)
- [ ] Log both request and response for debugging

### When Parsing Response:
- [ ] Check `jsonrpc == "2.0"`
- [ ] Check `id` matches request
- [ ] If `result` field exists, extract it
- [ ] If `error` field exists, handle error code and message
- [ ] Never assume HTTP 200 means success (check JSON-RPC fields)

---

## 6. SERVER IMPLEMENTATION CHECKLIST

### When Receiving Request:
- [ ] Parse JSON body
- [ ] Verify `jsonrpc == "2.0"`
- [ ] Extract `method`, `params`, and `id`
- [ ] Validate method name exists
- [ ] Validate parameter count/types

### When Sending Response:
- [ ] **ALWAYS return HTTP 200** for valid JSON-RPC requests
- [ ] Build response with `jsonrpc: "2.0"` and matching `id`
- [ ] On success: return `{"result": <DATA>}`
- [ ] On error: return `{"error": {"code": <CODE>, "message": "<MSG>"}}`
- [ ] Never mix `result` and `error` in same response
- [ ] Set `Content-Type: application/json`
- [ ] Include all required fields

---

## 7. CRITICAL FIXES FOR CURRENT ISSUES

### Issue: HTTP 503 on Valid Endpoint

**Root Cause:** Server returning 503 instead of JSON-RPC error response

**Fix:** Server MUST:
1. Return HTTP 200 (not 503) for all `/rpc` requests
2. Return JSON-RPC error object if endpoint not found
3. Never return HTML error pages from `/rpc` endpoint

**Example (Server-side pseudocode):**
```python
@app.post("/rpc")
def handle_rpc(request):
    try:
        payload = request.json()
        method = payload.get("method")
        params = payload.get("params", [])
        
        if method not in REGISTERED_METHODS:
            # Return HTTP 200 with JSON-RPC error
            return {
                "jsonrpc": "2.0",
                "error": {
                    "code": -32601,
                    "message": f"Method '{method}' not found"
                },
                "id": payload.get("id", 1)
            }, 200  # HTTP 200, not 503!
        
        result = REGISTERED_METHODS[method](*params)
        return {
            "jsonrpc": "2.0",
            "result": result,
            "id": payload.get("id", 1)
        }, 200
    
    except Exception as e:
        return {
            "jsonrpc": "2.0",
            "error": {
                "code": -32603,
                "message": str(e)
            },
            "id": payload.get("id", 1)
        }, 200  # Still HTTP 200!
```

---

## 8. DEBUGGING CHECKLIST

When RPC calls fail:

1. **Log the full request:**
   ```
   POST /rpc
   Body: {"jsonrpc":"2.0","method":"qtcl_getHealth","params":[],"id":1}
   ```

2. **Log the full response:**
   ```
   HTTP 200
   Body: {"jsonrpc":"2.0","result":{"status":"healthy"},"id":1}
   ```

3. **Verify:**
   - HTTP status code is 200
   - Response is valid JSON
   - Has `jsonrpc: "2.0"` field
   - Has `id` matching request
   - Has either `result` or `error`, not both

4. **On HTTP 503:**
   - The endpoint `/rpc` is not configured on server
   - Server crashed or not responding
   - Middleware is returning error before reaching handler
   - Check server logs for actual error

---

## 9. EXAMPLE: Full Client Call Flow

```python
import json
from urllib.request import Request, urlopen

# 1. Build request
payload = {
    "jsonrpc": "2.0",
    "method": "qtcl_getHealth",
    "params": [],
    "id": 1
}

# 2. Send request
url = "http://localhost:8000/rpc"
body = json.dumps(payload).encode()
req = Request(url, data=body, method="POST")
req.add_header("Content-Type", "application/json")

# 3. Get response
with urlopen(req, timeout=10) as resp:
    data = json.loads(resp.read().decode("utf-8"))

# 4. Parse response
if "result" in data:
    print("Success:", data["result"])
elif "error" in data:
    print("Error:", data["error"]["message"])
else:
    print("Invalid response:", data)
```

---

## 10. VERSION HISTORY

| Version | Date | Changes |
|---------|------|---------|
| 1.0 | 2026-04-19 | Initial specification |

