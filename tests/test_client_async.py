import json
import unittest

import httpx

from Tepilora import AsyncTepiloraClient
from Tepilora.models import V3BinaryResponse


class TestTepiloraClientAsync(unittest.IsolatedAsyncioTestCase):
    async def test_call_async(self) -> None:
        async def handler(request: httpx.Request) -> httpx.Response:
            self.assertEqual(request.method, "POST")
            self.assertEqual(request.url.host, "testserver")
            self.assertEqual(request.url.path, "/T-Api/v3")
            payload = json.loads(request.content.decode("utf-8"))
            self.assertEqual(payload["action"], "securities.details")
            return httpx.Response(
                200,
                json={
                    "success": True,
                    "action": "securities.details",
                    "data": {"id": "x"},
                    "meta": {"request_id": "r1", "execution_time_ms": 1, "timestamp": "t"},
                },
            )

        transport = httpx.MockTransport(handler)
        async with AsyncTepiloraClient(api_key="k", base_url="http://testserver", transport=transport) as client:
            resp = await client.call("securities.details", params={"identifier": "x"})
            self.assertTrue(resp.success)

    async def test_call_arrow_async_returns_binary(self) -> None:
        async def handler(request: httpx.Request) -> httpx.Response:
            self.assertEqual(request.method, "POST")
            self.assertEqual(request.url.path, "/T-Api/v3")
            self.assertEqual(request.url.params.get("format"), "arrow")
            self.assertEqual(request.headers.get("Accept"), "application/vnd.apache.arrow.stream")
            return httpx.Response(
                200,
                headers={"Content-Type": "application/vnd.apache.arrow.stream", "X-Tepilora-Request-Id": "r1"},
                content=b"ARROWSTREAM",
            )

        transport = httpx.MockTransport(handler)
        async with AsyncTepiloraClient(api_key="k", base_url="http://testserver", transport=transport) as client:
            resp = await client.call("securities.search", params={"query": "x"}, response_format="arrow")
            self.assertIsInstance(resp, V3BinaryResponse)
            self.assertEqual(resp.content, b"ARROWSTREAM")

    async def test_securities_search_calls_unified_endpoint(self) -> None:
        async def handler(request: httpx.Request) -> httpx.Response:
            self.assertEqual(request.method, "POST")
            self.assertEqual(request.url.path, "/T-Api/v3")
            payload = json.loads(request.content.decode("utf-8"))
            self.assertEqual(payload["action"], "securities.search")
            self.assertEqual(payload["params"]["query"], "MSCI ETF")
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
        async with AsyncTepiloraClient(api_key="k", base_url="http://testserver", transport=transport) as client:
            data = await client.securities.search(query="MSCI ETF", limit=1)
            self.assertIsInstance(data, dict)

    async def test_securities_lookup(self) -> None:
        """Test that securities.lookup is its own action (not alias to search)."""
        async def handler(request: httpx.Request) -> httpx.Response:
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
        async with AsyncTepiloraClient(api_key="k", base_url="http://testserver", transport=transport) as client:
            data = await client.securities.lookup(identifier="IE00B4L5Y983")
            self.assertEqual(data["identifier"], "IE00B4L5Y983")

    async def test_typed_endpoints_unwrap_envelope(self) -> None:
        async def handler(request: httpx.Request) -> httpx.Response:
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
        async with AsyncTepiloraClient(api_key="k", base_url="http://testserver", transport=transport) as client:
            data = await client.securities.search(query="MSCI ETF", limit=1)
            self.assertIn("securities", data)
