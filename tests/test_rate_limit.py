import asyncio
import json
import unittest

import httpx

from Tepilora import AsyncTepiloraClient


class TestRateLimitAsync(unittest.IsolatedAsyncioTestCase):
    async def _wait_for_in_flight(self, getter, expected: int, timeout: float = 1.0) -> None:
        start = asyncio.get_running_loop().time()
        while getter() < expected:
            if asyncio.get_running_loop().time() - start > timeout:
                break
            await asyncio.sleep(0)
        self.assertGreaterEqual(getter(), expected)

    async def test_max_concurrent_limits_parallel_requests(self) -> None:
        in_flight = 0
        max_in_flight = 0
        release_event = asyncio.Event()

        async def handler(request: httpx.Request) -> httpx.Response:
            nonlocal in_flight, max_in_flight
            payload = json.loads(request.content.decode("utf-8")) if request.content else {}
            if request.url.path != "/T-Api/v3/health":
                raise AssertionError(f"Unexpected path: {request.url.path}")
            in_flight += 1
            max_in_flight = max(max_in_flight, in_flight)
            await release_event.wait()
            in_flight -= 1
            return httpx.Response(200, json={"ok": True, "payload": payload})

        transport = httpx.MockTransport(handler)
        async with AsyncTepiloraClient(
            api_key="k",
            base_url="http://testserver",
            transport=transport,
            max_concurrent=3,
        ) as client:
            tasks = [asyncio.create_task(client.health()) for _ in range(10)]
            await self._wait_for_in_flight(lambda: max_in_flight, 3)
            release_event.set()
            await asyncio.gather(*tasks)

        self.assertEqual(max_in_flight, 3)

    async def test_no_rate_limit_by_default(self) -> None:
        in_flight = 0
        max_in_flight = 0
        release_event = asyncio.Event()

        async def handler(request: httpx.Request) -> httpx.Response:
            nonlocal in_flight, max_in_flight
            if request.url.path != "/T-Api/v3/health":
                raise AssertionError(f"Unexpected path: {request.url.path}")
            in_flight += 1
            max_in_flight = max(max_in_flight, in_flight)
            await release_event.wait()
            in_flight -= 1
            return httpx.Response(200, json={"ok": True})

        transport = httpx.MockTransport(handler)
        async with AsyncTepiloraClient(
            api_key="k",
            base_url="http://testserver",
            transport=transport,
        ) as client:
            tasks = [asyncio.create_task(client.health()) for _ in range(10)]
            await self._wait_for_in_flight(lambda: max_in_flight, 10)
            release_event.set()
            await asyncio.gather(*tasks)

        self.assertEqual(max_in_flight, 10)
