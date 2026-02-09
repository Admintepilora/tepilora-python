import importlib.util
import json
import unittest

import httpx

from Tepilora import TepiloraClient


@unittest.skipUnless(importlib.util.find_spec("pyarrow") is not None, "pyarrow not installed")
class TestArrowBytesDecode(unittest.TestCase):
    def test_as_table_decodes_arrow_bytes(self) -> None:
        import pyarrow as pa
        import pyarrow.ipc as ipc

        table = pa.Table.from_pylist([{"D": "2025-01-01", "X": 1.0}, {"D": "2025-01-02", "X": 2.0}])
        sink = pa.BufferOutputStream()
        with ipc.new_stream(sink, table.schema) as writer:
            writer.write_table(table)
        arrow_bytes = sink.getvalue().to_pybytes()

        def handler(request: httpx.Request) -> httpx.Response:
            self.assertEqual(request.url.path, "/T-Api/v3")
            payload = json.loads(request.content.decode("utf-8"))
            self.assertEqual(payload["action"], "analytics.rolling_volatility")
            # must request arrow
            self.assertEqual(request.url.params.get("format"), "arrow")
            self.assertEqual(request.headers.get("Accept"), "application/vnd.apache.arrow.stream")
            return httpx.Response(
                200,
                headers={"Content-Type": "application/vnd.apache.arrow.stream"},
                content=arrow_bytes,
            )

        transport = httpx.MockTransport(handler)
        client = TepiloraClient(api_key="k", base_url="http://testserver", transport=transport)
        decoded = client.analytics.rolling_volatility(identifiers="X", as_table="pyarrow")
        self.assertEqual(getattr(decoded, "num_rows", None), 2)

