import json
import unittest

import httpx

from Tepilora import AsyncTepiloraClient, TepiloraClient


def create_sync_handler(test_case: unittest.TestCase, expected_action: str, expected_params: dict):
    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path != "/T-Api/v3":
            raise AssertionError(f"Expected unified endpoint /T-Api/v3, got {request.url.path}")

        payload = json.loads(request.content.decode("utf-8"))
        test_case.assertEqual(payload.get("action"), expected_action)
        params = payload.get("params", {})
        for key, value in expected_params.items():
            test_case.assertEqual(params.get(key), value)

        return httpx.Response(
            200,
            json={"success": True, "action": expected_action, "data": {"securities": []}, "meta": {}},
        )

    return handler


def create_async_handler(test_case: unittest.TestCase, expected_action: str, expected_params: dict):
    async def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path != "/T-Api/v3":
            raise AssertionError(f"Expected unified endpoint /T-Api/v3, got {request.url.path}")

        payload = json.loads(request.content.decode("utf-8"))
        test_case.assertEqual(payload.get("action"), expected_action)
        params = payload.get("params", {})
        for key, value in expected_params.items():
            test_case.assertEqual(params.get(key), value)

        return httpx.Response(
            200,
            json={"success": True, "action": expected_action, "data": {"securities": []}, "meta": {}},
        )

    return handler


class TestSecuritiesGroupingSync(unittest.TestCase):
    def test_filter_group_by_param(self) -> None:
        transport = httpx.MockTransport(
            create_sync_handler(
                self,
                "securities.filter",
                {"filters": {"Currency": "EUR"}, "group_by": "TepiloraParentId"},
            )
        )
        client = TepiloraClient(api_key="k", base_url="http://testserver", transport=transport)
        client.securities.filter(filters={"Currency": "EUR"}, group_by="TepiloraParentId")

    def test_filter_preferred_currency_param(self) -> None:
        transport = httpx.MockTransport(
            create_sync_handler(
                self,
                "securities.filter",
                {"filters": {"Currency": "EUR"}, "preferred_currency": "USD"},
            )
        )
        client = TepiloraClient(api_key="k", base_url="http://testserver", transport=transport)
        client.securities.filter(filters={"Currency": "EUR"}, preferred_currency="USD")

    def test_filter_group_by_and_preferred_currency(self) -> None:
        transport = httpx.MockTransport(
            create_sync_handler(
                self,
                "securities.filter",
                {
                    "filters": {"Currency": "EUR"},
                    "group_by": "TepiloraParentId",
                    "preferred_currency": "EUR",
                },
            )
        )
        client = TepiloraClient(api_key="k", base_url="http://testserver", transport=transport)
        client.securities.filter(filters={"Currency": "EUR"}, group_by="TepiloraParentId", preferred_currency="EUR")

    def test_search_group_by_param(self) -> None:
        transport = httpx.MockTransport(
            create_sync_handler(
                self,
                "securities.search",
                {"query": "msci", "group_by": "TepiloraParentId"},
            )
        )
        client = TepiloraClient(api_key="k", base_url="http://testserver", transport=transport)
        client.securities.search(query="msci", group_by="TepiloraParentId")

    def test_search_preferred_currency_param(self) -> None:
        transport = httpx.MockTransport(
            create_sync_handler(
                self,
                "securities.search",
                {"query": "msci", "preferred_currency": "USD"},
            )
        )
        client = TepiloraClient(api_key="k", base_url="http://testserver", transport=transport)
        client.securities.search(query="msci", preferred_currency="USD")

    def test_search_group_by_and_preferred_currency(self) -> None:
        transport = httpx.MockTransport(
            create_sync_handler(
                self,
                "securities.search",
                {
                    "query": "msci",
                    "group_by": "TepiloraParentId",
                    "preferred_currency": "EUR",
                },
            )
        )
        client = TepiloraClient(api_key="k", base_url="http://testserver", transport=transport)
        client.securities.search(query="msci", group_by="TepiloraParentId", preferred_currency="EUR")

    def test_filter_sort_order_group_by(self) -> None:
        transport = httpx.MockTransport(
            create_sync_handler(
                self,
                "securities.filter",
                {
                    "filters": {"Currency": "EUR"},
                    "sort": "AUM",
                    "order": "desc",
                    "group_by": "TepiloraParentId",
                },
            )
        )
        client = TepiloraClient(api_key="k", base_url="http://testserver", transport=transport)
        client.securities.filter(filters={"Currency": "EUR"}, sort="AUM", order="desc", group_by="TepiloraParentId")

    def test_filter_array_values(self) -> None:
        transport = httpx.MockTransport(
            create_sync_handler(
                self,
                "securities.filter",
                {"filters": {"TepiloraType": ["Fund", "ETF"]}},
            )
        )
        client = TepiloraClient(api_key="k", base_url="http://testserver", transport=transport)
        client.securities.filter(filters={"TepiloraType": ["Fund", "ETF"]})


class TestSecuritiesGroupingAsync(unittest.IsolatedAsyncioTestCase):
    async def test_filter_group_by_param_async(self) -> None:
        transport = httpx.MockTransport(
            create_async_handler(
                self,
                "securities.filter",
                {"filters": {"Currency": "EUR"}, "group_by": "TepiloraParentId"},
            )
        )
        async with AsyncTepiloraClient(api_key="k", base_url="http://testserver", transport=transport) as client:
            await client.securities.filter(filters={"Currency": "EUR"}, group_by="TepiloraParentId")

    async def test_search_group_by_param_async(self) -> None:
        transport = httpx.MockTransport(
            create_async_handler(
                self,
                "securities.search",
                {"query": "msci", "group_by": "TepiloraParentId"},
            )
        )
        async with AsyncTepiloraClient(api_key="k", base_url="http://testserver", transport=transport) as client:
            await client.securities.search(query="msci", group_by="TepiloraParentId")
