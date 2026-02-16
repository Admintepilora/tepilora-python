import json
import unittest

import httpx

from Tepilora import AsyncTepiloraClient, TepiloraClient
from Tepilora.capabilities import capabilities
from Tepilora.client import _format_to_accept, _raise_for_error_response
from Tepilora.errors import TepiloraAPIError
from Tepilora.models import V3BinaryResponse, V3Meta


class TestClientHelpersCoverage(unittest.TestCase):
    def test_format_to_accept_passthrough_mime(self) -> None:
        self.assertEqual(_format_to_accept(" application/x-custom "), "application/x-custom")

    def test_format_to_accept_unknown_raises(self) -> None:
        with self.assertRaises(ValueError):
            _format_to_accept("made_up_format")

    def test_raise_for_error_response_json_parse_failure_falls_back_to_text(self) -> None:
        response = httpx.Response(
            502,
            headers={"Content-Type": "application/json"},
            content=b"not valid json",
        )
        with self.assertRaises(TepiloraAPIError) as ctx:
            _raise_for_error_response(response)
        self.assertEqual(ctx.exception.status_code, 502)
        self.assertEqual(ctx.exception.response_text, "not valid json")
        self.assertIsNone(ctx.exception.error_data)

    def test_raise_for_error_response_with_string_error_field(self) -> None:
        response = httpx.Response(
            400,
            headers={"Content-Type": "application/json"},
            json={"error": "some string"},
        )
        with self.assertRaises(TepiloraAPIError) as ctx:
            _raise_for_error_response(response)
        self.assertEqual(ctx.exception.status_code, 400)
        self.assertEqual(ctx.exception.message, "some string")

    def test_capabilities_dict_output_returns_copy(self) -> None:
        schema = capabilities(format="dict")
        action = next(iter(schema["operations"]))
        original_summary = schema["operations"][action].get("summary")

        schema["operations"][action]["summary"] = "mutated"
        fresh = capabilities(format="dict")

        self.assertEqual(fresh["operations"][action].get("summary"), original_summary)

    def test_v3meta_cache_hit_parses_string_false(self) -> None:
        meta = V3Meta.from_dict({"cache_hit": "false"})
        self.assertIs(meta.cache_hit, False)


class TestTepiloraClientCoverageSync(unittest.TestCase):
    def test_health_uses_legacy_apikey_query(self) -> None:
        def handler(request: httpx.Request) -> httpx.Response:
            self.assertEqual(request.url.path, "/T-Api/v3/health")
            self.assertEqual(request.url.params.get("apikey"), "legacy-key")
            return httpx.Response(200, json={"ok": True})

        transport = httpx.MockTransport(handler)
        client = TepiloraClient(
            api_key="legacy-key",
            base_url="http://testserver",
            send_legacy_query_key=True,
            transport=transport,
        )
        self.assertEqual(client.health(), {"ok": True})

    def test_init_with_custom_client_sets_no_ownership(self) -> None:
        def handler(request: httpx.Request) -> httpx.Response:
            self.assertEqual(request.url.path, "/T-Api/v3/health")
            return httpx.Response(200, json={"ok": True})

        transport = httpx.MockTransport(handler)
        custom_client = httpx.Client(base_url="http://testserver", transport=transport)
        sdk_client = TepiloraClient(api_key="k", client=custom_client)
        self.assertIs(sdk_client._client, custom_client)
        self.assertFalse(sdk_client._owns_client)

        sdk_client.close()
        response = custom_client.get("/T-Api/v3/health")
        self.assertEqual(response.status_code, 200)
        custom_client.close()

    def test_exit_closes_owned_client(self) -> None:
        transport = httpx.MockTransport(lambda request: httpx.Response(200, json={"ok": True}))
        client = TepiloraClient(api_key="k", base_url="http://testserver", transport=transport)
        self.assertFalse(client._client.is_closed)
        ret = client.__exit__(None, None, None)
        self.assertIsNone(ret)
        self.assertTrue(client._client.is_closed)

    def test_request_returns_text_for_non_json(self) -> None:
        def handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(200, headers={"Content-Type": "text/plain"}, content=b"plain text")

        transport = httpx.MockTransport(handler)
        client = TepiloraClient(api_key="k", base_url="http://testserver", transport=transport)
        result = client._request("GET", "/T-Api/v3/health")
        self.assertEqual(result, "plain text")

    def test_pricing_and_logs_status_hit_expected_paths(self) -> None:
        seen_paths = []

        def handler(request: httpx.Request) -> httpx.Response:
            seen_paths.append(request.url.path)
            return httpx.Response(200, json={"path": request.url.path})

        transport = httpx.MockTransport(handler)
        client = TepiloraClient(api_key="k", base_url="http://testserver", transport=transport)
        pricing = client.pricing()
        logs = client.logs_status()
        self.assertEqual(pricing["path"], "/T-Api/v3/pricing")
        self.assertEqual(logs["path"], "/T-Api/v3/logs/status")
        self.assertEqual(seen_paths, ["/T-Api/v3/pricing", "/T-Api/v3/logs/status"])

    def test_call_raises_on_non_object_json(self) -> None:
        def handler(request: httpx.Request) -> httpx.Response:
            self.assertEqual(request.url.path, "/T-Api/v3")
            return httpx.Response(200, json=[1, 2, 3])

        transport = httpx.MockTransport(handler)
        client = TepiloraClient(api_key="k", base_url="http://testserver", transport=transport)
        with self.assertRaises(TepiloraAPIError) as ctx:
            client.call("analytics.test")
        self.assertIn("Unexpected non-object JSON response", str(ctx.exception))

    def test_call_data_raises_when_success_false(self) -> None:
        def handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(
                200,
                json={
                    "success": False,
                    "action": "analytics.test",
                    "data": {"reason": "nope"},
                    "meta": {"request_id": "r1"},
                },
            )

        transport = httpx.MockTransport(handler)
        client = TepiloraClient(api_key="k", base_url="http://testserver", transport=transport)
        with self.assertRaises(TepiloraAPIError) as ctx:
            client.call_data("analytics.test")
        self.assertIn("success=false", str(ctx.exception))

    def test_call_arrow_ipc_stream_raises_if_json_returned(self) -> None:
        def handler(request: httpx.Request) -> httpx.Response:
            self.assertEqual(request.url.path, "/T-Api/v3")
            self.assertEqual(request.url.params.get("format"), "arrow")
            self.assertEqual(request.headers.get("Accept"), "application/vnd.apache.arrow.stream")
            payload = json.loads(request.content.decode("utf-8"))
            self.assertEqual(payload["action"], "analytics.test")
            return httpx.Response(
                200,
                json={
                    "success": True,
                    "action": "analytics.test",
                    "data": {"value": 1},
                    "meta": {"request_id": "r1"},
                },
            )

        transport = httpx.MockTransport(handler)
        client = TepiloraClient(api_key="k", base_url="http://testserver", transport=transport)
        with self.assertRaises(TepiloraAPIError) as ctx:
            client.call_arrow_ipc_stream("analytics.test")
        self.assertIn("Expected Arrow IPC stream response, got JSON", str(ctx.exception))

    def test_exports_namespace_is_attached(self) -> None:
        transport = httpx.MockTransport(lambda request: httpx.Response(200, json={"ok": True}))
        client = TepiloraClient(api_key="k", base_url="http://testserver", transport=transport)
        self.assertTrue(hasattr(client, "exports"))


class TestTepiloraClientCoverageAsync(unittest.IsolatedAsyncioTestCase):
    async def test_init_with_custom_async_client_sets_no_ownership(self) -> None:
        async def handler(request: httpx.Request) -> httpx.Response:
            self.assertEqual(request.url.path, "/T-Api/v3/health")
            return httpx.Response(200, json={"ok": True})

        transport = httpx.MockTransport(handler)
        custom_client = httpx.AsyncClient(base_url="http://testserver", transport=transport)
        sdk_client = AsyncTepiloraClient(api_key="k", client=custom_client)
        self.assertIs(sdk_client._client, custom_client)
        self.assertFalse(sdk_client._owns_client)

        await sdk_client.aclose()
        response = await custom_client.get("/T-Api/v3/health")
        self.assertEqual(response.status_code, 200)
        await custom_client.aclose()

    async def test_aexit_closes_owned_client(self) -> None:
        transport = httpx.MockTransport(lambda request: httpx.Response(200, json={"ok": True}))
        client = AsyncTepiloraClient(api_key="k", base_url="http://testserver", transport=transport)
        self.assertFalse(client._client.is_closed)
        ret = await client.__aexit__(None, None, None)
        self.assertIsNone(ret)
        self.assertTrue(client._client.is_closed)

    async def test_health_non_json_and_pricing_and_logs_status(self) -> None:
        async def handler(request: httpx.Request) -> httpx.Response:
            self.assertEqual(request.url.params.get("apikey"), "legacy-key")
            if request.url.path == "/T-Api/v3/health":
                return httpx.Response(200, headers={"Content-Type": "text/plain"}, content=b"async-ok")
            if request.url.path == "/T-Api/v3/pricing":
                return httpx.Response(200, json={"pricing": True})
            if request.url.path == "/T-Api/v3/logs/status":
                return httpx.Response(200, json={"status": "ready"})
            raise AssertionError(f"Unexpected path: {request.url.path}")

        transport = httpx.MockTransport(handler)
        async with AsyncTepiloraClient(
            api_key="legacy-key",
            base_url="http://testserver",
            send_legacy_query_key=True,
            transport=transport,
        ) as client:
            health = await client.health()
            pricing = await client.pricing()
            logs = await client.logs_status()
            self.assertEqual(health, "async-ok")
            self.assertEqual(pricing, {"pricing": True})
            self.assertEqual(logs, {"status": "ready"})

    async def test_call_raises_on_non_object_json(self) -> None:
        async def handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(200, json=[{"not": "dict root"}])

        transport = httpx.MockTransport(handler)
        async with AsyncTepiloraClient(api_key="k", base_url="http://testserver", transport=transport) as client:
            with self.assertRaises(TepiloraAPIError) as ctx:
                await client.call("analytics.test")
            self.assertIn("Unexpected non-object JSON response", str(ctx.exception))

    async def test_call_data_returns_binary_content(self) -> None:
        async def handler(request: httpx.Request) -> httpx.Response:
            self.assertEqual(request.url.params.get("format"), "arrow")
            return httpx.Response(
                200,
                headers={"Content-Type": "application/vnd.apache.arrow.stream"},
                content=b"ARROW-BYTES",
            )

        transport = httpx.MockTransport(handler)
        async with AsyncTepiloraClient(api_key="k", base_url="http://testserver", transport=transport) as client:
            result = await client.call_data("analytics.test", response_format="arrow")
            self.assertEqual(result, b"ARROW-BYTES")

    async def test_call_data_raises_when_success_false(self) -> None:
        async def handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(
                200,
                json={
                    "success": False,
                    "action": "analytics.test",
                    "data": {"reason": "nope"},
                    "meta": {"request_id": "r1"},
                },
            )

        transport = httpx.MockTransport(handler)
        async with AsyncTepiloraClient(api_key="k", base_url="http://testserver", transport=transport) as client:
            with self.assertRaises(TepiloraAPIError) as ctx:
                await client.call_data("analytics.test")
            self.assertIn("success=false", str(ctx.exception))

    async def test_call_arrow_ipc_stream_raises_if_json_returned(self) -> None:
        async def handler(request: httpx.Request) -> httpx.Response:
            self.assertEqual(request.url.params.get("format"), "arrow")
            self.assertEqual(request.headers.get("Accept"), "application/vnd.apache.arrow.stream")
            return httpx.Response(
                200,
                json={
                    "success": True,
                    "action": "analytics.test",
                    "data": {"value": 1},
                    "meta": {"request_id": "r1"},
                },
            )

        transport = httpx.MockTransport(handler)
        async with AsyncTepiloraClient(api_key="k", base_url="http://testserver", transport=transport) as client:
            with self.assertRaises(TepiloraAPIError) as ctx:
                await client.call_arrow_ipc_stream("analytics.test")
            self.assertIn("Expected Arrow IPC stream response, got JSON", str(ctx.exception))

    async def test_call_arrow_ipc_stream_returns_binary_response(self) -> None:
        async def handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(
                200,
                headers={"Content-Type": "application/vnd.apache.arrow.stream"},
                content=b"ARROWSTREAM",
            )

        transport = httpx.MockTransport(handler)
        async with AsyncTepiloraClient(api_key="k", base_url="http://testserver", transport=transport) as client:
            result = await client.call_arrow_ipc_stream("analytics.test")
            self.assertIsInstance(result, V3BinaryResponse)

    async def test_exports_namespace_is_attached(self) -> None:
        transport = httpx.MockTransport(lambda request: httpx.Response(200, json={"ok": True}))
        async with AsyncTepiloraClient(api_key="k", base_url="http://testserver", transport=transport) as client:
            self.assertTrue(hasattr(client, "exports"))
