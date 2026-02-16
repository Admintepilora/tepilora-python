import json
import unittest

import httpx

from Tepilora import TepiloraClient, AsyncTepiloraClient


class TestIdempotencySync(unittest.TestCase):
    def test_idempotency_key_sent_as_header(self) -> None:
        def handler(request: httpx.Request) -> httpx.Response:
            self.assertEqual(request.headers.get("X-Idempotency-Key"), "abc")
            payload = json.loads(request.content.decode("utf-8"))
            return httpx.Response(
                200,
                json={
                    "success": True,
                    "action": payload.get("action"),
                    "data": {},
                    "meta": {},
                },
            )

        transport = httpx.MockTransport(handler)
        client = TepiloraClient(api_key="k", base_url="http://testserver", transport=transport)
        client.call("analytics.test", idempotency_key="abc")

    def test_no_idempotency_header_by_default(self) -> None:
        def handler(request: httpx.Request) -> httpx.Response:
            self.assertIsNone(request.headers.get("X-Idempotency-Key"))
            payload = json.loads(request.content.decode("utf-8"))
            return httpx.Response(
                200,
                json={
                    "success": True,
                    "action": payload.get("action"),
                    "data": {},
                    "meta": {},
                },
            )

        transport = httpx.MockTransport(handler)
        client = TepiloraClient(api_key="k", base_url="http://testserver", transport=transport)
        client.call("analytics.test")


class TestIdempotencyAsync(unittest.IsolatedAsyncioTestCase):
    async def test_async_idempotency_key(self) -> None:
        async def handler(request: httpx.Request) -> httpx.Response:
            self.assertEqual(request.headers.get("X-Idempotency-Key"), "abc")
            payload = json.loads(request.content.decode("utf-8"))
            return httpx.Response(
                200,
                json={
                    "success": True,
                    "action": payload.get("action"),
                    "data": {},
                    "meta": {},
                },
            )

        transport = httpx.MockTransport(handler)
        async with AsyncTepiloraClient(api_key="k", base_url="http://testserver", transport=transport) as client:
            await client.call("analytics.test", idempotency_key="abc")
