import json
import unittest

import httpx

from Tepilora import TepiloraClient
from Tepilora.errors import TepiloraAPIError
from Tepilora.models import V3BinaryResponse


class TestTepiloraClientSync(unittest.TestCase):
    def test_call_sends_action_and_header_key(self) -> None:
        def handler(request: httpx.Request) -> httpx.Response:
            self.assertEqual(request.method, "POST")
            self.assertEqual(request.url.host, "testserver")
            self.assertEqual(request.url.path, "/T-Api/v3")
            self.assertEqual(request.headers.get("X-API-Key"), "k")
            payload = json.loads(request.content.decode("utf-8"))
            self.assertEqual(payload["action"], "securities.search")
            self.assertEqual(payload["params"]["query"], "MSCI ETF")
            return httpx.Response(
                200,
                json={
                    "success": True,
                    "action": "securities.search",
                    "data": {"items": []},
                    "meta": {"request_id": "r1", "execution_time_ms": 1, "timestamp": "t"},
                },
            )

        transport = httpx.MockTransport(handler)
        client = TepiloraClient(api_key="k", base_url="http://testserver", transport=transport)
        resp = client.call("securities.search", params={"query": "MSCI ETF"})
        self.assertTrue(resp.success)
        self.assertEqual(resp.action, "securities.search")

    def test_http_error_raises(self) -> None:
        def handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(401, json={"message": "unauthorized"})

        transport = httpx.MockTransport(handler)
        client = TepiloraClient(api_key="k", base_url="http://testserver", transport=transport)
        with self.assertRaises(TepiloraAPIError) as ctx:
            client.health()
        self.assertEqual(ctx.exception.status_code, 401)

    def test_call_arrow_returns_binary_response_and_sets_headers(self) -> None:
        def handler(request: httpx.Request) -> httpx.Response:
            self.assertEqual(request.method, "POST")
            self.assertEqual(request.url.path, "/T-Api/v3")
            self.assertEqual(request.url.params.get("format"), "arrow")
            self.assertEqual(request.headers.get("Accept"), "application/vnd.apache.arrow.stream")
            return httpx.Response(
                200,
                headers={
                    "Content-Type": "application/vnd.apache.arrow.stream",
                    "X-Tepilora-Request-Id": "r1",
                    "X-Tepilora-Execution-Time-Ms": "12",
                    "X-Tepilora-Total-Count": "123",
                    "X-Tepilora-Row-Count": "10",
                },
                content=b"ARROWSTREAM",
            )

        transport = httpx.MockTransport(handler)
        client = TepiloraClient(api_key="k", base_url="http://testserver", transport=transport)
        resp = client.call("securities.search", params={"query": "x"}, response_format="arrow")
        self.assertIsInstance(resp, V3BinaryResponse)
        self.assertEqual(resp.content, b"ARROWSTREAM")
        self.assertEqual(resp.meta.request_id, "r1")
        self.assertEqual(resp.meta.execution_time_ms, 12)
        self.assertEqual(resp.meta.total_count, 123)
        self.assertEqual(resp.meta.row_count, 10)

    def test_securities_search_calls_unified_endpoint(self) -> None:
        def handler(request: httpx.Request) -> httpx.Response:
            self.assertEqual(request.method, "POST")
            self.assertEqual(request.url.path, "/T-Api/v3")
            payload = json.loads(request.content.decode("utf-8"))
            self.assertEqual(payload["action"], "securities.search")
            self.assertEqual(payload["params"]["query"], "MSCI ETF")
            self.assertEqual(payload["params"]["limit"], 2)
            return httpx.Response(
                200,
                json={
                    "success": True,
                    "action": "securities.search",
                    "data": {"securities": [], "totalCount": 0},
                    "meta": {},
                },
            )

        transport = httpx.MockTransport(handler)
        client = TepiloraClient(api_key="k", base_url="http://testserver", transport=transport)
        data = client.securities.search(query="MSCI ETF", limit=2)
        self.assertIsInstance(data, dict)

    def test_securities_lookup(self) -> None:
        """Test that securities.lookup is its own action (not alias to search)."""
        def handler(request: httpx.Request) -> httpx.Response:
            self.assertEqual(request.method, "POST")
            self.assertEqual(request.url.path, "/T-Api/v3")
            payload = json.loads(request.content.decode("utf-8"))
            self.assertEqual(payload["action"], "securities.lookup")
            self.assertEqual(payload["params"]["identifier"], "IE00B4L5Y983")
            return httpx.Response(
                200,
                json={
                    "success": True,
                    "action": "securities.lookup",
                    "data": {"identifier": "IE00B4L5Y983", "name": "Test Security"},
                    "meta": {},
                },
            )

        transport = httpx.MockTransport(handler)
        client = TepiloraClient(api_key="k", base_url="http://testserver", transport=transport)
        data = client.securities.lookup(identifier="IE00B4L5Y983")
        self.assertEqual(data["identifier"], "IE00B4L5Y983")

    def test_typed_endpoints_unwrap_envelope(self) -> None:
        def handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(
                200,
                json={
                    "success": True,
                    "action": "securities.search",
                    "data": {"securities": [], "totalCount": 0, "hasMore": False, "searchMode": "x", "includesInactive": False},
                    "meta": {"request_id": "r1"},
                },
            )

        transport = httpx.MockTransport(handler)
        client = TepiloraClient(api_key="k", base_url="http://testserver", transport=transport)
        data = client.securities.search(query="MSCI ETF", limit=2)
        self.assertIn("securities", data)
