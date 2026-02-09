import json
import unittest

import httpx

from Tepilora import AsyncTepiloraClient


class TestAnalyticsAsync(unittest.IsolatedAsyncioTestCase):
    async def test_analytics_dynamic_call_async(self) -> None:
        async def handler(request: httpx.Request) -> httpx.Response:
            self.assertEqual(request.method, "POST")
            self.assertEqual(request.url.path, "/T-Api/v3")
            payload = json.loads(request.content.decode("utf-8"))
            self.assertEqual(payload["action"], "analytics.rolling_beta")
            self.assertEqual(payload["params"]["identifiers"], ["A", "B"])
            return httpx.Response(
                200,
                json={
                    "success": True,
                    "action": "analytics.rolling_beta",
                    "data": {"ok": True},
                    "meta": {"request_id": "r1", "execution_time_ms": 1, "timestamp": "t"},
                },
            )

        transport = httpx.MockTransport(handler)
        async with AsyncTepiloraClient(api_key="k", base_url="http://testserver", transport=transport) as client:
            data = await client.analytics.rolling_beta(identifiers=["A", "B"], Period=252)
            self.assertEqual(data["ok"], True)

