import json
import unittest

import httpx

from Tepilora import TepiloraClient, AsyncTepiloraClient
from Tepilora.errors import TepiloraAPIError


class TestCreditsSync(unittest.TestCase):
    def test_credits_initial_state(self) -> None:
        client = TepiloraClient(api_key="k", base_url="http://testserver")
        self.assertIsNone(client.credits_remaining)
        self.assertEqual(client.credits_used, 0)
        client.close()

    def test_credits_remaining_from_header(self) -> None:
        def handler(request: httpx.Request) -> httpx.Response:
            payload = json.loads(request.content.decode("utf-8"))
            return httpx.Response(
                200,
                headers={"X-Tepilora-Credits-Remaining": "950", "X-Tepilora-Credits-Used": "1"},
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
        self.assertEqual(client.credits_remaining, 950)
        self.assertEqual(client.credits_used, 1)

    def test_credits_used_accumulates(self) -> None:
        def handler(request: httpx.Request) -> httpx.Response:
            payload = json.loads(request.content.decode("utf-8"))
            return httpx.Response(
                200,
                headers={"X-Tepilora-Credits-Used": "1"},
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
        client.call("analytics.test")
        client.call("analytics.test")
        self.assertEqual(client.credits_used, 3)

    def test_credits_tracked_even_on_error(self) -> None:
        def handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(
                500,
                headers={"X-Tepilora-Credits-Remaining": "900", "X-Tepilora-Credits-Used": "2"},
                json={"error": "server"},
            )

        transport = httpx.MockTransport(handler)
        client = TepiloraClient(api_key="k", base_url="http://testserver", transport=transport)
        with self.assertRaises(TepiloraAPIError):
            client.call("analytics.test")
        self.assertEqual(client.credits_remaining, 900)
        self.assertEqual(client.credits_used, 2)


class TestCreditsAsync(unittest.IsolatedAsyncioTestCase):
    async def test_async_credits(self) -> None:
        async def handler(request: httpx.Request) -> httpx.Response:
            payload = json.loads(request.content.decode("utf-8"))
            return httpx.Response(
                200,
                headers={"X-Tepilora-Credits-Remaining": "950", "X-Tepilora-Credits-Used": "1"},
                json={
                    "success": True,
                    "action": payload.get("action"),
                    "data": {},
                    "meta": {},
                },
            )

        transport = httpx.MockTransport(handler)
        async with AsyncTepiloraClient(api_key="k", base_url="http://testserver", transport=transport) as client:
            await client.call("analytics.test")
            self.assertEqual(client.credits_remaining, 950)
            self.assertEqual(client.credits_used, 1)
