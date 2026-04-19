#!/usr/bin/env python3
"""
RPC Specification Compliance Verifier
Validates that client and server RPC implementations match the standardized spec.
"""

import json
import sys
from urllib.request import Request, urlopen
from urllib.error import HTTPError, URLError
from typing import Dict, List, Tuple

# ANSI colors
GREEN = "\033[92m"
RED = "\033[91m"
YELLOW = "\033[93m"
BLUE = "\033[94m"
RESET = "\033[0m"
BOLD = "\033[1m"

class RPCSpecValidator:
    """Validates RPC spec compliance."""

    def __init__(self, server_url: str = "http://localhost:8000"):
        self.server_url = server_url.rstrip("/")
        self.base_rpc_url = f"{self.server_url}/rpc"
        self.results = []

    def test(self, name: str, passed: bool, message: str = ""):
        """Record test result."""
        status = f"{GREEN}✓ PASS{RESET}" if passed else f"{RED}✗ FAIL{RESET}"
        self.results.append((name, passed, message))
        print(f"  {status} {name}")
        if message:
            print(f"      {message}")

    def validate_response_structure(self, response: Dict) -> Tuple[bool, str]:
        """Check if response matches JSON-RPC 2.0 spec."""

        # Must have jsonrpc field
        if response.get("jsonrpc") != "2.0":
            return False, f"Missing or invalid 'jsonrpc' field (got {response.get('jsonrpc')})"

        # Must have id field
        if "id" not in response:
            return False, "Missing 'id' field"

        # Must have either result or error, not both
        has_result = "result" in response
        has_error = "error" in response

        if not (has_result or has_error):
            return False, "Missing both 'result' and 'error' fields"

        if has_result and has_error:
            return False, "Both 'result' and 'error' present (should be only one)"

        # If error, check structure
        if has_error:
            error = response["error"]
            if not isinstance(error, dict):
                return False, "Error field must be an object"
            if "code" not in error or "message" not in error:
                return False, "Error must have 'code' and 'message' fields"

        return True, "Valid JSON-RPC 2.0 response"

    def test_connection(self) -> bool:
        """Test basic connectivity to server."""
        print(f"\n{BLUE}Testing Connection:{RESET}")

        try:
            payload = {"jsonrpc": "2.0", "method": "qtcl_getHealth", "params": [], "id": 1}
            body = json.dumps(payload).encode()
            req = Request(self.base_rpc_url, data=body, method="POST")
            req.add_header("Content-Type", "application/json")

            with urlopen(req, timeout=5) as resp:
                status_code = resp.status
                data = json.loads(resp.read().decode("utf-8"))

                self.test("HTTP Connection", status_code == 200,
                         f"Got HTTP {status_code}")

                valid, msg = self.validate_response_structure(data)
                self.test("JSON-RPC 2.0 Response Format", valid, msg)

                return valid and status_code == 200

        except HTTPError as e:
            self.test("HTTP Connection", False, f"HTTP {e.code}: {e.reason}")
            try:
                error_body = e.read().decode("utf-8")
                print(f"      Response body: {error_body[:200]}")
            except:
                pass
            return False
        except URLError as e:
            self.test("HTTP Connection", False, f"Connection failed: {e.reason}")
            return False
        except Exception as e:
            self.test("HTTP Connection", False, f"Unexpected error: {e}")
            return False

    def test_request_format(self) -> bool:
        """Verify client sends correct request format."""
        print(f"\n{BLUE}Testing Request Format:{RESET}")

        try:
            # Valid request
            payload = {
                "jsonrpc": "2.0",
                "method": "qtcl_getHealth",
                "params": [],
                "id": 1
            }

            has_jsonrpc = "jsonrpc" in payload and payload["jsonrpc"] == "2.0"
            self.test("Request has 'jsonrpc: 2.0'", has_jsonrpc)

            has_method = "method" in payload
            self.test("Request has 'method' field", has_method)

            has_params = "params" in payload and isinstance(payload["params"], list)
            self.test("Request has 'params' as array", has_params)

            has_id = "id" in payload
            self.test("Request has 'id' field", has_id)

            return has_jsonrpc and has_method and has_params and has_id

        except Exception as e:
            self.test("Request Format Validation", False, str(e))
            return False

    def test_error_handling(self) -> bool:
        """Test how server handles invalid requests."""
        print(f"\n{BLUE}Testing Error Handling:{RESET}")

        try:
            # Test 1: Invalid method
            payload = {"jsonrpc": "2.0", "method": "nonexistent_method_xyz", "params": [], "id": 1}
            body = json.dumps(payload).encode()
            req = Request(self.base_rpc_url, data=body, method="POST")
            req.add_header("Content-Type", "application/json")

            with urlopen(req, timeout=5) as resp:
                data = json.loads(resp.read().decode("utf-8"))

                # Must return HTTP 200, not 503
                http_ok = resp.status == 200
                self.test("Invalid method returns HTTP 200", http_ok,
                         f"Got HTTP {resp.status}")

                # Must have error field with -32601 code
                has_error = "error" in data
                self.test("Invalid method has 'error' field", has_error)

                if has_error:
                    error = data["error"]
                    has_code = error.get("code") == -32601
                    self.test("Error code is -32601 (Method not found)", has_code,
                             f"Got code {error.get('code')}")

                return http_ok and has_error

        except HTTPError as e:
            if e.code == 503:
                self.test("Invalid method handling", False,
                         "Returns HTTP 503 (should be HTTP 200 with JSON-RPC error)")
            else:
                self.test("Invalid method handling", False, f"Got HTTP {e.code}")
            return False
        except Exception as e:
            self.test("Error handling", False, str(e))
            return False

    def test_response_fields(self) -> bool:
        """Verify all response fields are present."""
        print(f"\n{BLUE}Testing Response Fields:{RESET}")

        try:
            payload = {"jsonrpc": "2.0", "method": "qtcl_getHealth", "params": [], "id": 1}
            body = json.dumps(payload).encode()
            req = Request(self.base_rpc_url, data=body, method="POST")
            req.add_header("Content-Type", "application/json")

            with urlopen(req, timeout=5) as resp:
                data = json.loads(resp.read().decode("utf-8"))

                has_jsonrpc = "jsonrpc" in data and data["jsonrpc"] == "2.0"
                self.test("Response has 'jsonrpc: 2.0'", has_jsonrpc)

                id_matches = data.get("id") == 1
                self.test("Response 'id' matches request", id_matches)

                has_content = "result" in data or "error" in data
                self.test("Response has 'result' or 'error'", has_content)

                return has_jsonrpc and id_matches and has_content

        except Exception as e:
            self.test("Response field validation", False, str(e))
            return False

    def test_content_type(self) -> bool:
        """Verify Content-Type header is correct."""
        print(f"\n{BLUE}Testing Content-Type:{RESET}")

        try:
            payload = {"jsonrpc": "2.0", "method": "qtcl_getHealth", "params": [], "id": 1}
            body = json.dumps(payload).encode()
            req = Request(self.base_rpc_url, data=body, method="POST")
            req.add_header("Content-Type", "application/json")

            with urlopen(req, timeout=5) as resp:
                content_type = resp.headers.get("Content-Type", "")
                is_json = "application/json" in content_type.lower()
                self.test("Response Content-Type is application/json", is_json,
                         f"Got: {content_type}")

                return is_json

        except Exception as e:
            self.test("Content-Type validation", False, str(e))
            return False

    def print_summary(self):
        """Print test summary."""
        passed = sum(1 for _, p, _ in self.results if p)
        total = len(self.results)

        print(f"\n{BOLD}{'='*60}{RESET}")
        print(f"{BOLD}Test Summary{RESET}")
        print(f"{BOLD}{'='*60}{RESET}")
        print(f"Passed: {GREEN}{passed}/{total}{RESET}")

        if passed == total:
            print(f"{GREEN}✓ All tests passed! RPC spec is compliant.{RESET}")
            return True
        else:
            failed = total - passed
            print(f"{RED}✗ {failed} test(s) failed.{RESET}")
            print(f"\n{BOLD}Failed tests:{RESET}")
            for name, passed, msg in self.results:
                if not passed:
                    print(f"  {RED}✗{RESET} {name}")
                    if msg:
                        print(f"    {msg}")
            return False

    def run_all_tests(self) -> bool:
        """Run all validation tests."""
        print(f"\n{BOLD}{BLUE}QTCL RPC Specification Validator{RESET}")
        print(f"Target: {BLUE}{self.base_rpc_url}{RESET}\n")

        if not self.test_connection():
            print(f"\n{RED}Cannot connect to server. Stopping tests.{RESET}")
            return False

        self.test_request_format()
        self.test_error_handling()
        self.test_response_fields()
        self.test_content_type()

        return self.print_summary()


def main():
    import argparse

    parser = argparse.ArgumentParser(description="Validate RPC spec compliance")
    parser.add_argument("--server", default="http://localhost:8000",
                       help="Server URL (default: http://localhost:8000)")
    args = parser.parse_args()

    validator = RPCSpecValidator(args.server)
    success = validator.run_all_tests()

    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
