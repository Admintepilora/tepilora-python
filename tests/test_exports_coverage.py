import json
import unittest

import httpx

from Tepilora import TepiloraClient, AsyncTepiloraClient


class TestExportsCoverageSync(unittest.TestCase):
    def test_exports_export_includes_optional_params(self) -> None:
        def handler(request: httpx.Request) -> httpx.Response:
            self.assertEqual(request.url.path, "/T-Api/v3")
            payload = json.loads(request.content.decode("utf-8"))
            self.assertEqual(payload["action"], "exports.export")
            params = payload.get("params")
            self.assertEqual(params["source"], "securities.search")
            self.assertEqual(params["source_params"], {"query": "msci"})
            self.assertEqual(params["filename"], "out.csv")
            self.assertEqual(params["include_metadata"], False)
            self.assertEqual(params["compression"], "gzip")
            self.assertEqual(payload.get("options"), {"format": "csv"})
            self.assertEqual(payload.get("context"), {"trace_id": "t1"})
            return httpx.Response(
                200,
                json={
                    "success": True,
                    "action": "exports.export",
                    "data": {"job_id": "j1"},
                    "meta": {},
                },
            )

        transport = httpx.MockTransport(handler)
        client = TepiloraClient(api_key="k", base_url="http://testserver", transport=transport)
        data = client.exports.export(
            source="securities.search",
            source_params={"query": "msci"},
            filename="out.csv",
            include_metadata=False,
            compression="gzip",
            options={"format": "csv"},
            context={"trace_id": "t1"},
        )
        self.assertEqual(data["job_id"], "j1")

    def test_exports_export_omits_none_params(self) -> None:
        def handler(request: httpx.Request) -> httpx.Response:
            payload = json.loads(request.content.decode("utf-8"))
            params = payload.get("params")
            self.assertEqual(params["source"], "securities.search")
            self.assertNotIn("source_params", params)
            self.assertNotIn("filename", params)
            self.assertNotIn("include_metadata", params)
            self.assertNotIn("compression", params)
            return httpx.Response(
                200,
                json={
                    "success": True,
                    "action": "exports.export",
                    "data": {"job_id": "j2"},
                    "meta": {},
                },
            )

        transport = httpx.MockTransport(handler)
        client = TepiloraClient(api_key="k", base_url="http://testserver", transport=transport)
        data = client.exports.export(
            source="securities.search",
            include_metadata=None,
        )
        self.assertEqual(data["job_id"], "j2")

    def test_exports_formats(self) -> None:
        def handler(request: httpx.Request) -> httpx.Response:
            payload = json.loads(request.content.decode("utf-8"))
            self.assertEqual(payload["action"], "exports.formats")
            self.assertEqual(payload.get("params"), {})
            return httpx.Response(
                200,
                json={
                    "success": True,
                    "action": "exports.formats",
                    "data": {"formats": ["csv", "parquet"]},
                    "meta": {},
                },
            )

        transport = httpx.MockTransport(handler)
        client = TepiloraClient(api_key="k", base_url="http://testserver", transport=transport)
        data = client.exports.formats()
        self.assertEqual(data["formats"], ["csv", "parquet"])


class TestExportsCoverageAsync(unittest.IsolatedAsyncioTestCase):
    async def test_exports_export_includes_optional_params(self) -> None:
        async def handler(request: httpx.Request) -> httpx.Response:
            self.assertEqual(request.url.path, "/T-Api/v3")
            payload = json.loads(request.content.decode("utf-8"))
            self.assertEqual(payload["action"], "exports.export")
            params = payload.get("params")
            self.assertEqual(params["source"], "securities.search")
            self.assertEqual(params["source_params"], {"query": "msci"})
            self.assertEqual(params["filename"], "out.csv")
            self.assertEqual(params["include_metadata"], False)
            self.assertEqual(params["compression"], "gzip")
            self.assertEqual(payload.get("options"), {"format": "csv"})
            self.assertEqual(payload.get("context"), {"trace_id": "t1"})
            return httpx.Response(
                200,
                json={
                    "success": True,
                    "action": "exports.export",
                    "data": {"job_id": "j1"},
                    "meta": {},
                },
            )

        transport = httpx.MockTransport(handler)
        async with AsyncTepiloraClient(api_key="k", base_url="http://testserver", transport=transport) as client:
            data = await client.exports.export(
                source="securities.search",
                source_params={"query": "msci"},
                filename="out.csv",
                include_metadata=False,
                compression="gzip",
                options={"format": "csv"},
                context={"trace_id": "t1"},
            )
            self.assertEqual(data["job_id"], "j1")

    async def test_exports_export_omits_none_params(self) -> None:
        async def handler(request: httpx.Request) -> httpx.Response:
            payload = json.loads(request.content.decode("utf-8"))
            params = payload.get("params")
            self.assertEqual(params["source"], "securities.search")
            self.assertNotIn("source_params", params)
            self.assertNotIn("filename", params)
            self.assertNotIn("include_metadata", params)
            self.assertNotIn("compression", params)
            return httpx.Response(
                200,
                json={
                    "success": True,
                    "action": "exports.export",
                    "data": {"job_id": "j2"},
                    "meta": {},
                },
            )

        transport = httpx.MockTransport(handler)
        async with AsyncTepiloraClient(api_key="k", base_url="http://testserver", transport=transport) as client:
            data = await client.exports.export(
                source="securities.search",
                include_metadata=None,
            )
            self.assertEqual(data["job_id"], "j2")

    async def test_exports_formats(self) -> None:
        async def handler(request: httpx.Request) -> httpx.Response:
            payload = json.loads(request.content.decode("utf-8"))
            self.assertEqual(payload["action"], "exports.formats")
            self.assertEqual(payload.get("params"), {})
            return httpx.Response(
                200,
                json={
                    "success": True,
                    "action": "exports.formats",
                    "data": {"formats": ["csv", "parquet"]},
                    "meta": {},
                },
            )

        transport = httpx.MockTransport(handler)
        async with AsyncTepiloraClient(api_key="k", base_url="http://testserver", transport=transport) as client:
            data = await client.exports.formats()
            self.assertEqual(data["formats"], ["csv", "parquet"])
