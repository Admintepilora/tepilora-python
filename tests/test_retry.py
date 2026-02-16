import json
import unittest
from typing import List, Tuple
from unittest.mock import AsyncMock, call, patch

import httpx

from Tepilora import TepiloraClient, AsyncTepiloraClient
from Tepilora.errors import TepiloraAPIError


def _make_health_handler(test_case: unittest.TestCase, responses: List[Tuple[int, dict, dict]]):
    calls = {"count": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        test_case.assertEqual(request.url.path, "/T-Api/v3/health")
        idx = calls["count"]
        calls["count"] += 1
        status, payload, headers = responses[idx]
        return httpx.Response(status, json=payload, headers=headers)

    return handler, calls


def _make_call_handler(test_case: unittest.TestCase, responses: List[Tuple[int, dict, dict]]):
    calls = {"count": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        test_case.assertEqual(request.url.path, "/T-Api/v3")
        payload = json.loads(request.content.decode("utf-8"))
        test_case.assertEqual(payload["action"], "analytics.test")
        idx = calls["count"]
        calls["count"] += 1
        status, body, headers = responses[idx]
        return httpx.Response(status, json=body, headers=headers)

    return handler, calls


class TestRetrySync(unittest.TestCase):
    def test_no_retry_by_default(self) -> None:
        handler, calls = _make_health_handler(
            self,
            [
                (503, {"error": "nope"}, {}),
            ],
        )
        transport = httpx.MockTransport(handler)
        client = TepiloraClient(api_key="k", base_url="http://testserver", transport=transport)
        with patch("Tepilora.client.time.sleep") as sleep_mock:
            with self.assertRaises(TepiloraAPIError) as ctx:
                client.health()
            self.assertEqual(ctx.exception.status_code, 503)
            self.assertEqual(calls["count"], 1)
            sleep_mock.assert_not_called()

    def test_retry_on_503(self) -> None:
        handler, calls = _make_health_handler(
            self,
            [
                (503, {"error": "nope"}, {}),
                (503, {"error": "still nope"}, {}),
                (200, {"ok": True}, {}),
            ],
        )
        transport = httpx.MockTransport(handler)
        client = TepiloraClient(api_key="k", base_url="http://testserver", transport=transport, max_retries=3)
        with patch("Tepilora.client.random.uniform", return_value=1.0):
            with patch("Tepilora.client.time.sleep") as sleep_mock:
                resp = client.health()
        self.assertEqual(resp, {"ok": True})
        self.assertEqual(calls["count"], 3)
        self.assertEqual(sleep_mock.call_args_list, [call(0.5), call(1.0)])

    def test_retry_on_429_with_retry_after(self) -> None:
        handler, calls = _make_health_handler(
            self,
            [
                (429, {"error": "slow down"}, {"Retry-After": "1.5"}),
                (200, {"ok": True}, {}),
            ],
        )
        transport = httpx.MockTransport(handler)
        client = TepiloraClient(api_key="k", base_url="http://testserver", transport=transport, max_retries=3)
        with patch("Tepilora.client.time.sleep") as sleep_mock:
            resp = client.health()
        self.assertEqual(resp, {"ok": True})
        self.assertEqual(calls["count"], 2)
        sleep_mock.assert_called_once_with(1.5)

    def test_retry_exhausted_raises(self) -> None:
        handler, calls = _make_health_handler(
            self,
            [
                (503, {"error": "nope"}, {}),
                (503, {"error": "nope"}, {}),
                (503, {"error": "nope"}, {}),
            ],
        )
        transport = httpx.MockTransport(handler)
        client = TepiloraClient(api_key="k", base_url="http://testserver", transport=transport, max_retries=2)
        with patch("Tepilora.client.time.sleep") as sleep_mock:
            with self.assertRaises(TepiloraAPIError) as ctx:
                client.health()
        self.assertEqual(ctx.exception.status_code, 503)
        self.assertEqual(calls["count"], 3)
        self.assertEqual(sleep_mock.call_count, 2)

    def test_no_retry_on_400(self) -> None:
        handler, calls = _make_health_handler(
            self,
            [
                (400, {"error": "bad"}, {}),
                (200, {"ok": True}, {}),
            ],
        )
        transport = httpx.MockTransport(handler)
        client = TepiloraClient(api_key="k", base_url="http://testserver", transport=transport, max_retries=3)
        with patch("Tepilora.client.time.sleep") as sleep_mock:
            with self.assertRaises(TepiloraAPIError) as ctx:
                client.health()
        self.assertEqual(ctx.exception.status_code, 400)
        self.assertEqual(calls["count"], 1)
        sleep_mock.assert_not_called()

    def test_retry_on_call_method(self) -> None:
        scenarios = [
            (
                "retry_on_503",
                3,
                [
                    (503, {"error": "nope"}, {}),
                    (503, {"error": "still nope"}, {}),
                    (200, {"success": True, "action": "analytics.test", "data": {}, "meta": {}}, {}),
                ],
                [0.5, 1.0],
                True,
                None,
            ),
            (
                "retry_on_429",
                3,
                [
                    (429, {"error": "slow down"}, {"Retry-After": "2"}),
                    (200, {"success": True, "action": "analytics.test", "data": {}, "meta": {}}, {}),
                ],
                [2.0],
                True,
                None,
            ),
            (
                "exhausted",
                2,
                [
                    (503, {"error": "nope"}, {}),
                    (503, {"error": "nope"}, {}),
                    (503, {"error": "nope"}, {}),
                ],
                [0.5, 1.0],
                False,
                503,
            ),
            (
                "no_retry_on_400",
                3,
                [
                    (400, {"error": "bad"}, {}),
                    (200, {"success": True, "action": "analytics.test", "data": {}, "meta": {}}, {}),
                ],
                [],
                False,
                400,
            ),
        ]

        for name, max_retries, responses, expected_sleeps, should_succeed, error_status in scenarios:
            with self.subTest(name=name):
                handler, calls = _make_call_handler(self, responses)
                transport = httpx.MockTransport(handler)
                client = TepiloraClient(
                    api_key="k",
                    base_url="http://testserver",
                    transport=transport,
                    max_retries=max_retries,
                )
                with patch("Tepilora.client.random.uniform", return_value=1.0):
                    with patch("Tepilora.client.time.sleep") as sleep_mock:
                        if should_succeed:
                            resp = client.call("analytics.test")
                            self.assertTrue(resp.success)
                        else:
                            with self.assertRaises(TepiloraAPIError) as ctx:
                                client.call("analytics.test")
                            self.assertEqual(ctx.exception.status_code, error_status)
                if expected_sleeps:
                    self.assertEqual([c.args[0] for c in sleep_mock.call_args_list], expected_sleeps)
                else:
                    sleep_mock.assert_not_called()


class TestRetryAsync(unittest.IsolatedAsyncioTestCase):
    async def test_async_retry(self) -> None:
        async def make_health_handler(responses: List[Tuple[int, dict, dict]]):
            calls = {"count": 0}

            async def handler(request: httpx.Request) -> httpx.Response:
                self.assertEqual(request.url.path, "/T-Api/v3/health")
                idx = calls["count"]
                calls["count"] += 1
                status, payload, headers = responses[idx]
                return httpx.Response(status, json=payload, headers=headers)

            return handler, calls

        async def make_call_handler(responses: List[Tuple[int, dict, dict]]):
            calls = {"count": 0}

            async def handler(request: httpx.Request) -> httpx.Response:
                self.assertEqual(request.url.path, "/T-Api/v3")
                payload = json.loads(request.content.decode("utf-8"))
                self.assertEqual(payload["action"], "analytics.test")
                idx = calls["count"]
                calls["count"] += 1
                status, body, headers = responses[idx]
                return httpx.Response(status, json=body, headers=headers)

            return handler, calls

        # _request retries
        handler, calls = await make_health_handler(
            [
                (503, {"error": "nope"}, {}),
                (503, {"error": "still nope"}, {}),
                (200, {"ok": True}, {}),
            ]
        )
        transport = httpx.MockTransport(handler)
        async with AsyncTepiloraClient(
            api_key="k",
            base_url="http://testserver",
            transport=transport,
            max_retries=3,
        ) as client:
            with patch("Tepilora.client.random.uniform", return_value=1.0):
                with patch("asyncio.sleep", new=AsyncMock()) as sleep_mock:
                    resp = await client.health()
        self.assertEqual(resp, {"ok": True})
        self.assertEqual(calls["count"], 3)
        self.assertEqual([c.args[0] for c in sleep_mock.call_args_list], [0.5, 1.0])

        # call retries
        handler, calls = await make_call_handler(
            [
                (429, {"error": "slow down"}, {"Retry-After": "1"}),
                (200, {"success": True, "action": "analytics.test", "data": {}, "meta": {}}, {}),
            ]
        )
        transport = httpx.MockTransport(handler)
        async with AsyncTepiloraClient(
            api_key="k",
            base_url="http://testserver",
            transport=transport,
            max_retries=3,
        ) as client:
            with patch("asyncio.sleep", new=AsyncMock()) as sleep_mock:
                resp = await client.call("analytics.test")
        self.assertTrue(resp.success)
        self.assertEqual(calls["count"], 2)
        sleep_mock.assert_awaited_once_with(1.0)

        # no retry on 400
        handler, calls = await make_call_handler(
            [
                (400, {"error": "bad"}, {}),
                (200, {"success": True, "action": "analytics.test", "data": {}, "meta": {}}, {}),
            ]
        )
        transport = httpx.MockTransport(handler)
        async with AsyncTepiloraClient(
            api_key="k",
            base_url="http://testserver",
            transport=transport,
            max_retries=3,
        ) as client:
            with patch("asyncio.sleep", new=AsyncMock()) as sleep_mock:
                with self.assertRaises(TepiloraAPIError) as ctx:
                    await client.call("analytics.test")
        self.assertEqual(ctx.exception.status_code, 400)
        self.assertEqual(calls["count"], 1)
        sleep_mock.assert_not_awaited()
