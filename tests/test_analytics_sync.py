import json
import importlib.util
import unittest

import httpx

from Tepilora import TepiloraClient

_HAS_PYARROW = importlib.util.find_spec("pyarrow") is not None


class TestAnalyticsSync(unittest.TestCase):
    def test_analytics_list_and_info(self) -> None:
        def handler(request: httpx.Request) -> httpx.Response:
            if request.url.path == "/T-Api/v3/analytics/list":
                payload = json.loads(request.content.decode("utf-8"))
                self.assertEqual(payload, {})
                return httpx.Response(
                    200,
                    json={
                        "success": True,
                        "action": "analytics.list",
                        "data": {"functions": ["rolling_volatility"], "count": 1, "categories": ["single", "multi"]},
                        "meta": {"request_id": "r1", "execution_time_ms": 1, "timestamp": "t"},
                    },
                )

            if request.url.path == "/T-Api/v3/analytics/info":
                payload = json.loads(request.content.decode("utf-8"))
                self.assertEqual(payload["function"], "rolling_volatility")
                return httpx.Response(
                    200,
                    json={
                        "success": True,
                        "action": "analytics.info",
                        "data": {
                            "name": "rolling_volatility",
                            "category": "single",
                            "description": "Calculate rolling volatility.",
                            "docstring": "Docstring here",
                            "module": "analytics.single.volatility",
                            "parameters": {
                                "common": [{"name": "identifiers", "required": False, "oneOf": [{"type": "string"}, {"type": "array"}]}],
                                "specific": [{"name": "Period", "type": "integer", "required": False, "default": 265}],
                            },
                        },
                        "meta": {"request_id": "r1", "execution_time_ms": 1, "timestamp": "t"},
                    },
                )

            raise AssertionError(f"unexpected path {request.url.path}")

        transport = httpx.MockTransport(handler)
        client = TepiloraClient(api_key="k", base_url="http://testserver", transport=transport)

        listing = client.analytics.list()
        self.assertEqual(listing["count"], 1)
        info = client.analytics.info("rolling_volatility")
        self.assertEqual(info["name"], "rolling_volatility")
        help_text = client.analytics.help("rolling_volatility")
        self.assertIn("Period", help_text)

    def test_analytics_dynamic_call_uses_unified_endpoint(self) -> None:
        def handler(request: httpx.Request) -> httpx.Response:
            self.assertEqual(request.method, "POST")
            self.assertEqual(request.url.path, "/T-Api/v3")
            payload = json.loads(request.content.decode("utf-8"))
            self.assertEqual(payload["action"], "analytics.rolling_volatility")
            self.assertEqual(payload["params"]["identifiers"], "IE00B4L5Y983EURXMIL")
            self.assertEqual(payload["params"]["Period"], 252)
            return httpx.Response(
                200,
                json={
                    "success": True,
                    "action": "analytics.rolling_volatility",
                    "data": {"ok": True},
                    "meta": {"request_id": "r1", "execution_time_ms": 1, "timestamp": "t"},
                },
            )

        transport = httpx.MockTransport(handler)
        client = TepiloraClient(api_key="k", base_url="http://testserver", transport=transport)
        data = client.analytics.rolling_volatility(identifiers="IE00B4L5Y983EURXMIL", Period=252)
        self.assertEqual(data["ok"], True)

    def test_analytics_strict_fills_defaults_and_rejects_unknown(self) -> None:
        calls = {"info": 0, "call": 0}

        def handler(request: httpx.Request) -> httpx.Response:
            if request.url.path == "/T-Api/v3/analytics/info":
                calls["info"] += 1
                return httpx.Response(
                    200,
                    json={
                        "success": True,
                        "action": "analytics.info",
                        "data": {
                            "name": "rolling_volatility",
                            "category": "single",
                            "description": "Calculate rolling volatility.",
                            "docstring": None,
                            "module": "x",
                            "parameters": {
                                "common": [{"name": "identifiers", "required": True, "oneOf": [{"type": "string"}, {"type": "array"}]}],
                                "specific": [{"name": "Period", "type": "integer", "required": False, "default": 265}],
                            },
                        },
                        "meta": {"request_id": "r1", "execution_time_ms": 1, "timestamp": "t"},
                    },
                )

            if request.url.path == "/T-Api/v3":
                calls["call"] += 1
                payload = json.loads(request.content.decode("utf-8"))
                self.assertEqual(payload["params"]["Period"], 265)
                return httpx.Response(
                    200,
                    json={
                        "success": True,
                        "action": "analytics.rolling_volatility",
                        "data": {"ok": True},
                        "meta": {"request_id": "r1", "execution_time_ms": 1, "timestamp": "t"},
                    },
                )

            raise AssertionError(f"unexpected path {request.url.path}")

        transport = httpx.MockTransport(handler)
        client = TepiloraClient(api_key="k", base_url="http://testserver", transport=transport)

        data = client.analytics.rolling_volatility(identifiers="X", strict=True)
        self.assertTrue(data["ok"])

        with self.assertRaises(ValueError):
            client.analytics.rolling_volatility(identifiers="X", strict=True, NotAParam=1)

        self.assertGreaterEqual(calls["info"], 1)
        self.assertEqual(calls["call"], 1)

    @unittest.skipUnless(_HAS_PYARROW, "pyarrow not installed")
    def test_analytics_as_table_from_json_result(self) -> None:
        def handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(
                200,
                json={
                    "success": True,
                    "action": "analytics.rolling_volatility",
                    "data": {
                        "category": "single",
                        "function": "rolling_volatility",
                        "result": [{"D": "2025-01-01", "X": 1.0}, {"D": "2025-01-02", "X": 2.0}],
                    },
                    "meta": {"request_id": "r1", "execution_time_ms": 1, "timestamp": "t"},
                },
            )

        transport = httpx.MockTransport(handler)
        client = TepiloraClient(api_key="k", base_url="http://testserver", transport=transport)
        table = client.analytics.rolling_volatility(identifiers="X", as_table="pyarrow")
        self.assertEqual(type(table).__name__, "Table")
        self.assertEqual(getattr(table, "num_rows", None), 2)
