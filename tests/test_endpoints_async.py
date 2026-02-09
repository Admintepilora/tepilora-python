import json
import unittest

import httpx

from Tepilora import AsyncTepiloraClient


def create_async_v3_handler(test_case: unittest.TestCase):
    """Create an async handler for unified V3 endpoint requests."""

    async def handler(request: httpx.Request) -> httpx.Response:
        # All requests should go to the unified endpoint
        if request.url.path != "/T-Api/v3":
            raise AssertionError(f"Expected unified endpoint /T-Api/v3, got {request.url.path}")

        payload = json.loads(request.content.decode("utf-8"))
        action = payload.get("action")
        params = payload.get("params", {})

        # Securities namespace
        if action == "securities.description":
            test_case.assertEqual(params["identifier"], "X")
            return httpx.Response(
                200,
                json={"success": True, "action": action, "data": {"identifier": "X"}, "meta": {}},
            )

        if action == "securities.details":
            # Alias for description
            test_case.assertEqual(params["identifier"], "X")
            return httpx.Response(
                200,
                json={"success": True, "action": action, "data": {"identifier": "X"}, "meta": {}},
            )

        # News namespace
        if action == "news.search":
            test_case.assertEqual(params["query"], "bitcoin")
            return httpx.Response(200, json={"success": True, "action": action, "data": {"articles": []}, "meta": {}})

        # Publications namespace
        if action == "publications.search":
            test_case.assertEqual(params["query"], "x")
            return httpx.Response(200, json={"success": True, "action": action, "data": {"publications": []}, "meta": {}})

        # Queries namespace
        if action == "queries.list":
            test_case.assertEqual(params["limit"], 1)
            return httpx.Response(200, json={"success": True, "action": action, "data": {"queries": []}, "meta": {}})

        # Search namespace
        if action == "search.global":
            test_case.assertEqual(params["query"], "x")
            return httpx.Response(200, json={"success": True, "action": action, "data": {"results": {}}, "meta": {}})

        # Portfolio namespace
        if action == "portfolio.list":
            return httpx.Response(200, json={"success": True, "action": action, "data": {"portfolios": []}, "meta": {}})

        raise AssertionError(f"Unexpected action: {action}")

    return handler


class TestTypedEndpointsAsync(unittest.IsolatedAsyncioTestCase):
    async def test_securities_details_alias_async(self) -> None:
        transport = httpx.MockTransport(create_async_v3_handler(self))
        async with AsyncTepiloraClient(api_key="k", base_url="http://testserver", transport=transport) as client:
            data = await client.securities.details(identifier="X")
            self.assertEqual(data["identifier"], "X")

    async def test_news_endpoints_async(self) -> None:
        transport = httpx.MockTransport(create_async_v3_handler(self))
        async with AsyncTepiloraClient(api_key="k", base_url="http://testserver", transport=transport) as client:
            data = await client.news.search(query="bitcoin")
            self.assertEqual(data["articles"], [])

    async def test_publications_search_async(self) -> None:
        transport = httpx.MockTransport(create_async_v3_handler(self))
        async with AsyncTepiloraClient(api_key="k", base_url="http://testserver", transport=transport) as client:
            data = await client.publications.search(query="x")
            self.assertEqual(data["publications"], [])

    async def test_queries_and_search_async(self) -> None:
        transport = httpx.MockTransport(create_async_v3_handler(self))
        async with AsyncTepiloraClient(api_key="k", base_url="http://testserver", transport=transport) as client:
            queries = await client.queries.list(limit=1)
            self.assertEqual(queries["queries"], [])
            res = await client.search.global_search(query="x")
            self.assertEqual(res["results"], {})

    async def test_new_namespaces_async(self) -> None:
        """Test the new async namespace APIs are accessible."""
        transport = httpx.MockTransport(create_async_v3_handler(self))
        async with AsyncTepiloraClient(api_key="k", base_url="http://testserver", transport=transport) as client:
            # Portfolio namespace
            portfolios = await client.portfolio.list()
            self.assertEqual(portfolios["portfolios"], [])

            # Verify all namespaces are accessible
            self.assertIsNotNone(client.portfolio)
            self.assertIsNotNone(client.macro)
            self.assertIsNotNone(client.alerts)
            self.assertIsNotNone(client.stocks)
            self.assertIsNotNone(client.bonds)
            self.assertIsNotNone(client.options)
            self.assertIsNotNone(client.esg)
            self.assertIsNotNone(client.factors)
            self.assertIsNotNone(client.fh)
            self.assertIsNotNone(client.data)
            self.assertIsNotNone(client.clients)
            self.assertIsNotNone(client.profiling)
            self.assertIsNotNone(client.billing)
            self.assertIsNotNone(client.documents)
            self.assertIsNotNone(client.alternatives)
