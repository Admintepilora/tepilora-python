import io
import json
import os
import tempfile
import unittest

import httpx

from Tepilora import TepiloraClient
from Tepilora.errors import TepiloraAPIError
from Tepilora.models import V3BinaryResponse


class TestTepiloraClientSync(unittest.TestCase):
    def test_call_sends_action_and_header_key(self) -> None:
        def handler(request: httpx.Request) -> httpx.Response:
            self.assertEqual(request.method, "POST")
            self.assertEqual(request.url.host, "testserver")
            self.assertEqual(request.url.path, "/T-Api/v3")
            self.assertEqual(request.headers.get("X-API-Key"), "k")
            payload = json.loads(request.content.decode("utf-8"))
            self.assertEqual(payload["action"], "securities.search")
            self.assertEqual(payload["params"]["query"], "MSCI ETF")
            return httpx.Response(
                200,
                json={
                    "success": True,
                    "action": "securities.search",
                    "data": {"items": []},
                    "meta": {"request_id": "r1", "execution_time_ms": 1, "timestamp": "t"},
                },
            )

        transport = httpx.MockTransport(handler)
        client = TepiloraClient(api_key="k", base_url="http://testserver", transport=transport)
        resp = client.call("securities.search", params={"query": "MSCI ETF"})
        self.assertTrue(resp.success)
        self.assertEqual(resp.action, "securities.search")

    def test_http_error_raises(self) -> None:
        def handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(401, json={"message": "unauthorized"})

        transport = httpx.MockTransport(handler)
        client = TepiloraClient(api_key="k", base_url="http://testserver", transport=transport)
        with self.assertRaises(TepiloraAPIError) as ctx:
            client.health()
        self.assertEqual(ctx.exception.status_code, 401)

    def test_call_arrow_returns_binary_response_and_sets_headers(self) -> None:
        def handler(request: httpx.Request) -> httpx.Response:
            self.assertEqual(request.method, "POST")
            self.assertEqual(request.url.path, "/T-Api/v3")
            self.assertEqual(request.url.params.get("format"), "arrow")
            self.assertEqual(request.headers.get("Accept"), "application/vnd.apache.arrow.stream")
            return httpx.Response(
                200,
                headers={
                    "Content-Type": "application/vnd.apache.arrow.stream",
                    "X-Tepilora-Request-Id": "r1",
                    "X-Tepilora-Execution-Time-Ms": "12",
                    "X-Tepilora-Total-Count": "123",
                    "X-Tepilora-Row-Count": "10",
                },
                content=b"ARROWSTREAM",
            )

        transport = httpx.MockTransport(handler)
        client = TepiloraClient(api_key="k", base_url="http://testserver", transport=transport)
        resp = client.call("securities.search", params={"query": "x"}, response_format="arrow")
        self.assertIsInstance(resp, V3BinaryResponse)
        self.assertEqual(resp.content, b"ARROWSTREAM")
        self.assertEqual(resp.meta.request_id, "r1")
        self.assertEqual(resp.meta.execution_time_ms, 12)
        self.assertEqual(resp.meta.total_count, 123)
        self.assertEqual(resp.meta.row_count, 10)

    def test_securities_search_calls_unified_endpoint(self) -> None:
        def handler(request: httpx.Request) -> httpx.Response:
            self.assertEqual(request.method, "POST")
            self.assertEqual(request.url.path, "/T-Api/v3")
            payload = json.loads(request.content.decode("utf-8"))
            self.assertEqual(payload["action"], "securities.search")
            self.assertEqual(payload["params"]["query"], "MSCI ETF")
            self.assertEqual(payload["params"]["limit"], 2)
            return httpx.Response(
                200,
                json={
                    "success": True,
                    "action": "securities.search",
                    "data": {"securities": [], "totalCount": 0},
                    "meta": {},
                },
            )

        transport = httpx.MockTransport(handler)
        client = TepiloraClient(api_key="k", base_url="http://testserver", transport=transport)
        data = client.securities.search(query="MSCI ETF", limit=2)
        self.assertIsInstance(data, dict)

    def test_securities_lookup(self) -> None:
        """Test that securities.lookup is its own action (not alias to search)."""
        def handler(request: httpx.Request) -> httpx.Response:
            self.assertEqual(request.method, "POST")
            self.assertEqual(request.url.path, "/T-Api/v3")
            payload = json.loads(request.content.decode("utf-8"))
            self.assertEqual(payload["action"], "securities.lookup")
            self.assertEqual(payload["params"]["identifier"], "IE00B4L5Y983")
            return httpx.Response(
                200,
                json={
                    "success": True,
                    "action": "securities.lookup",
                    "data": {"identifier": "IE00B4L5Y983", "name": "Test Security"},
                    "meta": {},
                },
            )

        transport = httpx.MockTransport(handler)
        client = TepiloraClient(api_key="k", base_url="http://testserver", transport=transport)
        data = client.securities.lookup(identifier="IE00B4L5Y983")
        self.assertEqual(data["identifier"], "IE00B4L5Y983")

    def test_typed_endpoints_unwrap_envelope(self) -> None:
        def handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(
                200,
                json={
                    "success": True,
                    "action": "securities.search",
                    "data": {"securities": [], "totalCount": 0, "hasMore": False, "searchMode": "x", "includesInactive": False},
                    "meta": {"request_id": "r1"},
                },
            )

        transport = httpx.MockTransport(handler)
        client = TepiloraClient(api_key="k", base_url="http://testserver", transport=transport)
        data = client.securities.search(query="MSCI ETF", limit=2)
        self.assertIn("securities", data)

    def test_attachments_upload_uses_multipart_for_file_inputs(self) -> None:
        expected_payloads = [b"bytes payload", b"path payload", b"stream payload"]

        def handler(request: httpx.Request) -> httpx.Response:
            expected = expected_payloads.pop(0)
            self.assertEqual(request.method, "POST")
            self.assertEqual(request.url.path, "/T-Api/v3/attachments/upload")
            self.assertIn("multipart/form-data", request.headers.get("Content-Type", ""))
            self.assertNotIn("application/json", request.headers.get("Content-Type", ""))
            body = request.content
            self.assertIn(b'name="file"; filename="notes.txt"', body)
            self.assertIn(b'name="filename"', body)
            self.assertIn(b"notes.txt", body)
            self.assertIn(b'name="source"', body)
            self.assertIn(b"pytest", body)
            self.assertIn(expected, body)
            return httpx.Response(
                200,
                json={
                    "success": True,
                    "action": "attachments.upload",
                    "data": {"attachment": {"fileId": "att_1"}},
                    "meta": {},
                },
            )

        transport = httpx.MockTransport(handler)
        client = TepiloraClient(api_key="k", base_url="http://testserver", transport=transport)

        data = client.attachments.upload(
            file=b"bytes payload",
            filename="notes.txt",
            mime_type="text/plain",
            source="pytest",
        )
        self.assertEqual(data["attachment"]["fileId"], "att_1")

        tmp_name = ""
        try:
            with tempfile.NamedTemporaryFile(delete=False) as tmp:
                tmp.write(b"path payload")
                tmp_name = tmp.name
            client.attachments.upload(
                file=tmp_name,
                filename="notes.txt",
                mime_type="text/plain",
                source="pytest",
            )
        finally:
            if tmp_name:
                os.unlink(tmp_name)

        client.attachments.upload(
            file=io.BytesIO(b"stream payload"),
            filename="notes.txt",
            mime_type="text/plain",
            source="pytest",
        )
        self.assertEqual(expected_payloads, [])

    def test_attachments_companions_remain_json(self) -> None:
        expected_actions = [
            "attachments.list",
            "attachments.info",
            "attachments.rename",
            "attachments.read",
            "attachments.delete",
        ]

        def handler(request: httpx.Request) -> httpx.Response:
            action = expected_actions.pop(0)
            self.assertEqual(request.method, "POST")
            self.assertEqual(request.url.path, "/T-Api/v3")
            self.assertEqual(request.headers.get("Content-Type"), "application/json")
            payload = json.loads(request.content.decode("utf-8"))
            self.assertEqual(payload["action"], action)
            self.assertNotIn(b"multipart/form-data", request.content)
            return httpx.Response(
                200,
                json={
                    "success": True,
                    "action": action,
                    "data": {"ok": True},
                    "meta": {},
                },
            )

        transport = httpx.MockTransport(handler)
        client = TepiloraClient(api_key="k", base_url="http://testserver", transport=transport)
        self.assertEqual(client.attachments.list()["ok"], True)
        self.assertEqual(client.attachments.info(file_id="att_1")["ok"], True)
        self.assertEqual(client.attachments.rename(file_id="att_1", name="renamed.txt")["ok"], True)
        self.assertEqual(client.attachments.read(file_id="att_1", max_chars=100)["ok"], True)
        self.assertEqual(client.attachments.delete(file_id="att_1")["ok"], True)
        self.assertEqual(expected_actions, [])

    def test_attachments_download_returns_bytes_from_binary_endpoint(self) -> None:
        def handler(request: httpx.Request) -> httpx.Response:
            self.assertEqual(request.method, "POST")
            self.assertEqual(request.url.path, "/T-Api/v3/attachments/download")
            self.assertEqual(request.headers.get("Accept"), "application/octet-stream")
            payload = json.loads(request.content.decode("utf-8"))
            self.assertEqual(payload, {"file_id": "att_1"})
            return httpx.Response(
                200,
                headers={
                    "Content-Type": "application/pdf",
                    "Content-Disposition": 'attachment; filename="statement.pdf"',
                    "X-Tepilora-Request-Id": "r1",
                },
                content=b"%PDF-1.4\n",
            )

        transport = httpx.MockTransport(handler)
        client = TepiloraClient(api_key="k", base_url="http://testserver", transport=transport)

        data = client.attachments.download(file_id="att_1")

        self.assertEqual(data, b"%PDF-1.4\n")
