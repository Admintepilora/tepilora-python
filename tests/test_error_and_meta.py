import unittest

import httpx

from Tepilora import TepiloraClient
from Tepilora.errors import TepiloraAPIError
from Tepilora.models import V3Meta, V3Response


class TestErrorAndMeta(unittest.TestCase):
    def test_error_text_response_captured(self) -> None:
        def handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(500, headers={"Content-Type": "text/plain"}, content=b"boom")

        transport = httpx.MockTransport(handler)
        client = TepiloraClient(api_key="k", base_url="http://testserver", transport=transport)
        with self.assertRaises(TepiloraAPIError) as ctx:
            client.health()
        self.assertEqual(ctx.exception.status_code, 500)
        self.assertEqual(ctx.exception.response_text, "boom")

    def test_meta_parsing_tolerates_missing_fields(self) -> None:
        meta = V3Meta.from_dict({"anything": 1})
        self.assertIsNone(meta.request_id)
        self.assertEqual(meta.extra.get("anything"), 1)

        resp = V3Response.from_dict({"data": {"x": 1}})
        self.assertTrue(resp.success)
        self.assertEqual(resp.data, {"x": 1})

