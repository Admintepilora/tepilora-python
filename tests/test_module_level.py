import json
import unittest

import httpx

import Tepilora as T
from Tepilora._default_client import close_default_client


class TestModuleLevel(unittest.TestCase):
    def tearDown(self) -> None:
        close_default_client()

    def test_configure_affects_module_level_analytics(self) -> None:
        def handler(request: httpx.Request) -> httpx.Response:
            self.assertEqual(request.url.path, "/T-Api/v3")
            payload = json.loads(request.content.decode("utf-8"))
            self.assertEqual(payload["action"], "analytics.rolling_volatility")
            self.assertEqual(request.headers.get("X-API-Key"), "k")
            return httpx.Response(
                200,
                json={"success": True, "action": payload["action"], "data": {"ok": True}, "meta": {}},
            )

        transport = httpx.MockTransport(handler)
        T.configure(api_key="k", base_url="http://testserver", transport=transport)
        data = T.analytics.rolling_volatility(identifiers="X", Period=10)
        self.assertTrue(data["ok"])

