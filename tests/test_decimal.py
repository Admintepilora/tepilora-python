import json
import unittest
from datetime import datetime
from decimal import Decimal

import httpx

from Tepilora import TepiloraClient


class TestDecimalSerialization(unittest.TestCase):
    def test_decimal_param_serialized(self) -> None:
        def handler(request: httpx.Request) -> httpx.Response:
            payload = json.loads(request.content.decode("utf-8"))
            value = payload["params"]["price"]
            self.assertEqual(value, 99.95)
            self.assertIsInstance(value, float)
            return httpx.Response(200, json={"success": True, "action": payload["action"], "data": {}, "meta": {}})

        transport = httpx.MockTransport(handler)
        client = TepiloraClient(api_key="k", base_url="http://testserver", transport=transport)
        client.call("analytics.test", params={"price": Decimal("99.95")})

    def test_decimal_in_nested_dict(self) -> None:
        def handler(request: httpx.Request) -> httpx.Response:
            payload = json.loads(request.content.decode("utf-8"))
            value = payload["params"]["filters"]["min_price"]
            self.assertEqual(value, 10.5)
            self.assertIsInstance(value, float)
            return httpx.Response(200, json={"success": True, "action": payload["action"], "data": {}, "meta": {}})

        transport = httpx.MockTransport(handler)
        client = TepiloraClient(api_key="k", base_url="http://testserver", transport=transport)
        client.call("analytics.test", params={"filters": {"min_price": Decimal("10.5")}})

    def test_decimal_in_list(self) -> None:
        def handler(request: httpx.Request) -> httpx.Response:
            payload = json.loads(request.content.decode("utf-8"))
            values = payload["params"]["values"]
            self.assertEqual(values, [1.1, 2.2])
            self.assertIsInstance(values[0], float)
            self.assertIsInstance(values[1], float)
            return httpx.Response(200, json={"success": True, "action": payload["action"], "data": {}, "meta": {}})

        transport = httpx.MockTransport(handler)
        client = TepiloraClient(api_key="k", base_url="http://testserver", transport=transport)
        client.call("analytics.test", params={"values": [Decimal("1.1"), Decimal("2.2")]})

    def test_datetime_param_coerced(self) -> None:
        def handler(request: httpx.Request) -> httpx.Response:
            payload = json.loads(request.content.decode("utf-8"))
            value = payload["params"]["timestamp"]
            self.assertEqual(value, "2024-01-15T08:30:00")
            return httpx.Response(200, json={"success": True, "action": payload["action"], "data": {}, "meta": {}})

        transport = httpx.MockTransport(handler)
        client = TepiloraClient(api_key="k", base_url="http://testserver", transport=transport)
        client.call("analytics.test", params={"timestamp": datetime(2024, 1, 15, 8, 30, 0)})
