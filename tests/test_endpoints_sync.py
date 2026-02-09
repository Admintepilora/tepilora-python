import json
import unittest

import httpx

from Tepilora import TepiloraClient


def create_v3_handler(test_case: unittest.TestCase):
    """Create a handler for unified V3 endpoint requests."""

    def handler(request: httpx.Request) -> httpx.Response:
        # All requests should go to the unified endpoint
        if request.url.path != "/T-Api/v3":
            raise AssertionError(f"Expected unified endpoint /T-Api/v3, got {request.url.path}")

        payload = json.loads(request.content.decode("utf-8"))
        action = payload.get("action")
        params = payload.get("params", {})

        # Securities namespace
        if action == "securities.description":
            test_case.assertEqual(params["identifier"], "FR0010655712EURXPAR")
            return httpx.Response(
                200,
                json={"success": True, "action": action, "data": {"identifier": params["identifier"]}, "meta": {}},
            )

        if action == "securities.details":
            # Alias for description
            test_case.assertEqual(params["identifier"], "FR0010655712EURXPAR")
            return httpx.Response(
                200,
                json={"success": True, "action": action, "data": {"identifier": params["identifier"]}, "meta": {}},
            )

        if action == "securities.facets":
            test_case.assertEqual(params["fields"], ["Currency"])
            return httpx.Response(200, json={"success": True, "action": action, "data": {"facets": {}}, "meta": {}})

        if action == "securities.history":
            test_case.assertEqual(params["identifiers"], "X")
            test_case.assertEqual(params["limit"], 10)
            return httpx.Response(200, json={"success": True, "action": action, "data": {"rows": []}, "meta": {}})

        if action == "securities.filter":
            test_case.assertEqual(params["filters"]["Currency"], "EUR")
            return httpx.Response(200, json={"success": True, "action": action, "data": {"securities": []}, "meta": {}})

        # News namespace
        if action == "news.search":
            test_case.assertEqual(params["query"], "bitcoin")
            return httpx.Response(200, json={"success": True, "action": action, "data": {"articles": []}, "meta": {}})

        if action == "news.latest":
            test_case.assertEqual(params["limit"], 2)
            return httpx.Response(200, json={"success": True, "action": action, "data": {"articles": []}, "meta": {}})

        if action == "news.facets":
            # news.facets only has filters param (optional)
            return httpx.Response(200, json={"success": True, "action": action, "data": {"facets": {}}, "meta": {}})

        if action == "news.details":
            test_case.assertEqual(params["url"], "https://example.com/a")
            return httpx.Response(200, json={"success": True, "action": action, "data": {"url": params["url"]}, "meta": {}})

        # Publications namespace
        if action == "publications.search":
            test_case.assertEqual(params["query"], "bitcoin")
            return httpx.Response(200, json={"success": True, "action": action, "data": {"publications": []}, "meta": {}})

        if action == "publications.latest":
            test_case.assertEqual(params["limit"], 1)
            return httpx.Response(200, json={"success": True, "action": action, "data": {"publications": []}, "meta": {}})

        if action == "publications.facets":
            # publications.facets only has filters param (optional)
            return httpx.Response(200, json={"success": True, "action": action, "data": {"facets": {}}, "meta": {}})

        if action == "publications.details":
            test_case.assertEqual(params["doc_id"], "d1")
            return httpx.Response(200, json={"success": True, "action": action, "data": {"doc_id": "d1"}, "meta": {}})

        if action == "publications.by_source":
            test_case.assertEqual(params["source_news_id"], "n1")
            return httpx.Response(200, json={"success": True, "action": action, "data": {"publications": []}, "meta": {}})

        # Queries namespace
        if action == "queries.list":
            test_case.assertEqual(params["limit"], 3)
            return httpx.Response(200, json={"success": True, "action": action, "data": {"queries": []}, "meta": {}})

        if action == "queries.get":
            test_case.assertEqual(params["name"], "q1")
            test_case.assertEqual(params["category"], "securities")
            return httpx.Response(200, json={"success": True, "action": action, "data": {"name": "q1"}, "meta": {}})

        if action == "queries.save":
            test_case.assertEqual(params["name"], "q2")
            test_case.assertEqual(params["category"], "securities")
            return httpx.Response(200, json={"success": True, "action": action, "data": {"ok": True}, "meta": {}})

        if action == "queries.edit":
            test_case.assertEqual(params["name"], "q2")
            test_case.assertEqual(params["category"], "securities")
            return httpx.Response(200, json={"success": True, "action": action, "data": {"ok": True}, "meta": {}})

        if action == "queries.copy":
            test_case.assertEqual(params["name"], "q2")
            test_case.assertEqual(params["new_name"], "q3")
            return httpx.Response(200, json={"success": True, "action": action, "data": {"name": "q3"}, "meta": {}})

        if action == "queries.delete":
            test_case.assertEqual(params["name"], "q3")
            test_case.assertEqual(params["category"], "securities")
            return httpx.Response(200, json={"success": True, "action": action, "data": {"ok": True}, "meta": {}})

        # Search namespace
        if action == "search.global":
            test_case.assertEqual(params["query"], "msci")
            test_case.assertEqual(params["limit"], 2)
            return httpx.Response(200, json={"success": True, "action": action, "data": {"results": {}}, "meta": {}})

        # Portfolio namespace
        if action == "portfolio.list":
            return httpx.Response(200, json={"success": True, "action": action, "data": {"portfolios": []}, "meta": {}})

        if action == "portfolio.create":
            test_case.assertEqual(params["name"], "Test Portfolio")
            test_case.assertEqual(params["input_type"], "fixed_weights")
            return httpx.Response(200, json={"success": True, "action": action, "data": {"id": "p1", "name": "Test Portfolio"}, "meta": {}})

        # Macro namespace
        if action == "macro.indicators":
            test_case.assertEqual(params["country"], "Italy")
            return httpx.Response(200, json={"success": True, "action": action, "data": {"indicators": []}, "meta": {}})

        raise AssertionError(f"Unexpected action: {action}")

    return handler


class TestTypedEndpointsSync(unittest.TestCase):
    def test_securities_endpoints(self) -> None:
        transport = httpx.MockTransport(create_v3_handler(self))
        client = TepiloraClient(api_key="k", base_url="http://testserver", transport=transport)

        self.assertEqual(client.securities.description(identifier="FR0010655712EURXPAR")["identifier"], "FR0010655712EURXPAR")
        self.assertEqual(client.securities.details(identifier="FR0010655712EURXPAR")["identifier"], "FR0010655712EURXPAR")
        self.assertEqual(client.securities.facets(fields=["Currency"])["facets"], {})
        self.assertEqual(client.securities.history(identifiers="X", limit=10)["rows"], [])
        self.assertEqual(client.securities.filter(filters={"Currency": "EUR"})["securities"], [])

    def test_news_endpoints(self) -> None:
        transport = httpx.MockTransport(create_v3_handler(self))
        client = TepiloraClient(api_key="k", base_url="http://testserver", transport=transport)

        self.assertEqual(client.news.search(query="bitcoin")["articles"], [])
        self.assertEqual(client.news.latest(limit=2)["articles"], [])
        # news.facets uses filters, not fields
        self.assertEqual(client.news.facets()["facets"], {})
        self.assertEqual(client.news.details(url="https://example.com/a")["url"], "https://example.com/a")

    def test_publications_endpoints(self) -> None:
        transport = httpx.MockTransport(create_v3_handler(self))
        client = TepiloraClient(api_key="k", base_url="http://testserver", transport=transport)

        self.assertEqual(client.publications.search(query="bitcoin")["publications"], [])
        self.assertEqual(client.publications.latest(limit=1)["publications"], [])
        # publications.facets uses filters, not fields
        self.assertEqual(client.publications.facets()["facets"], {})
        self.assertEqual(client.publications.details(doc_id="d1")["doc_id"], "d1")
        self.assertEqual(client.publications.by_source(source_news_id="n1")["publications"], [])

    def test_queries_and_global_search_endpoints(self) -> None:
        transport = httpx.MockTransport(create_v3_handler(self))
        client = TepiloraClient(api_key="k", base_url="http://testserver", transport=transport)

        self.assertEqual(client.queries.list(limit=3)["queries"], [])
        self.assertEqual(client.queries.get(name="q1", category="securities")["name"], "q1")
        self.assertTrue(client.queries.save(name="q2", category="securities")["ok"])
        self.assertTrue(client.queries.edit(name="q2", category="securities")["ok"])
        self.assertEqual(client.queries.copy(name="q2", category="securities", new_name="q3")["name"], "q3")
        self.assertTrue(client.queries.delete(name="q3", category="securities")["ok"])
        self.assertEqual(client.search.global_search(query="msci", limit=2)["results"], {})

    def test_new_namespaces(self) -> None:
        """Test the new namespace APIs are accessible."""
        transport = httpx.MockTransport(create_v3_handler(self))
        client = TepiloraClient(api_key="k", base_url="http://testserver", transport=transport)

        # Portfolio namespace
        self.assertEqual(client.portfolio.list()["portfolios"], [])
        self.assertEqual(client.portfolio.create(name="Test Portfolio", input_type="fixed_weights")["name"], "Test Portfolio")

        # Macro namespace
        self.assertEqual(client.macro.indicators(country="Italy")["indicators"], [])

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


class TestImportCompatibility(unittest.TestCase):
    def test_import_from_endpoints_module(self) -> None:
        """Test backward compatibility: imports from Tepilora.endpoints still work."""
        from Tepilora.endpoints import SecuritiesAPI, NewsAPI, PublicationsAPI, QueriesAPI, SearchAPI
        from Tepilora.endpoints import AsyncSecuritiesAPI, AsyncNewsAPI

        self.assertIsNotNone(SecuritiesAPI)
        self.assertIsNotNone(NewsAPI)
        self.assertIsNotNone(PublicationsAPI)
        self.assertIsNotNone(QueriesAPI)
        self.assertIsNotNone(SearchAPI)
        self.assertIsNotNone(AsyncSecuritiesAPI)
        self.assertIsNotNone(AsyncNewsAPI)

    def test_import_new_namespaces(self) -> None:
        """Test new namespace classes can be imported."""
        from Tepilora.endpoints import (
            PortfolioAPI, MacroAPI, AlertsAPI,
            StocksAPI, BondsAPI, OptionsAPI, EsgAPI, FactorsAPI, FhAPI, DataAPI,
            ClientsAPI, ProfilingAPI, BillingAPI, DocumentsAPI, AlternativesAPI,
        )

        self.assertIsNotNone(PortfolioAPI)
        self.assertIsNotNone(MacroAPI)
        self.assertIsNotNone(AlertsAPI)
        self.assertIsNotNone(StocksAPI)
        self.assertIsNotNone(BondsAPI)
        self.assertIsNotNone(OptionsAPI)
        self.assertIsNotNone(EsgAPI)
        self.assertIsNotNone(FactorsAPI)
        self.assertIsNotNone(FhAPI)
        self.assertIsNotNone(DataAPI)
        self.assertIsNotNone(ClientsAPI)
        self.assertIsNotNone(ProfilingAPI)
        self.assertIsNotNone(BillingAPI)
        self.assertIsNotNone(DocumentsAPI)
        self.assertIsNotNone(AlternativesAPI)
