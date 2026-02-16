import sys
import types
import unittest
from unittest.mock import AsyncMock, Mock, patch

from Tepilora.analytics import (
    AnalyticsAPI,
    AnalyticsFunction,
    AsyncAnalyticsAPI,
    AsyncAnalyticsFunction,
    _decode_table,
    _decode_table_from_json,
    _format_param,
)
from Tepilora.arrow import read_ipc_stream
from Tepilora.errors import TepiloraAPIError

# Get the real analytics module (not the _ModuleAnalyticsProxy)
_analytics_module = sys.modules["Tepilora.analytics"]


class TestAnalyticsHelpersCoverage(unittest.TestCase):
    def test_format_param_oneof_string_and_nullable(self) -> None:
        formatted = _format_param(
            {
                "name": "value",
                "oneOf": [{"type": "integer"}, "string"],
                "nullable": True,
                "required": False,
            }
        )
        self.assertIn("integer | string", formatted)
        self.assertIn("(nullable)", formatted)
        self.assertIn("[optional]", formatted)

    def test_decode_table_pandas_uses_to_pandas(self) -> None:
        class TableWithPandas:
            def to_pandas(self) -> dict:
                return {"converted": True}

        with patch.object(_analytics_module, "read_ipc_stream", return_value=TableWithPandas()):
            result = _decode_table(b"ignored", "pandas")
        self.assertEqual(result, {"converted": True})

    def test_decode_table_pandas_raises_if_to_pandas_missing(self) -> None:
        with patch.object(_analytics_module, "read_ipc_stream", return_value=object()):
            with self.assertRaises(TepiloraAPIError) as ctx:
                _decode_table(b"ignored", "pandas")
        self.assertIn("to_pandas", str(ctx.exception))

    def test_decode_table_from_json_polars_and_pandas(self) -> None:
        fake_polars = types.SimpleNamespace(DataFrame=lambda data: {"engine": "polars", "data": data})
        fake_pandas = types.SimpleNamespace(DataFrame=lambda data: {"engine": "pandas", "data": data})
        rows = [{"x": 1}, {"x": 2}]

        with patch.dict(sys.modules, {"polars": fake_polars, "pandas": fake_pandas}, clear=False):
            polars_df = _decode_table_from_json(rows, "polars")
            pandas_df = _decode_table_from_json(rows, "pandas")

        self.assertEqual(polars_df, {"engine": "polars", "data": rows})
        self.assertEqual(pandas_df, {"engine": "pandas", "data": rows})

    def test_decode_table_from_json_invalid_mode(self) -> None:
        with self.assertRaises(ValueError):
            _decode_table_from_json([{"x": 1}], "spark")


class _SyncRequestStub:
    def __init__(self) -> None:
        self.listing = {"functions": ["rolling_volatility", "rolling_beta"], "count": 2, "categories": ["single"]}
        self.info_map = {
            "rolling_volatility": {
                "description": "Rolling volatility function",
                "module": "analytics.single.volatility",
                "docstring": "Compute rolling volatility.",
                "parameters": {
                    "common": [{"name": "identifiers", "required": True, "type": "string"}],
                    "specific": [{"name": "Period", "required": False, "default": 265, "type": "integer"}],
                },
            },
            "empty_params": {"parameters": {"common": [], "specific": []}},
            "no_dict_params": {"parameters": []},
        }

    def _request(self, method: str, path: str, *, json_body=None):
        if path == "/T-Api/v3/analytics/list":
            return {"success": True, "action": "analytics.list", "data": self.listing, "meta": {"request_id": "r1"}}
        if path == "/T-Api/v3/analytics/info":
            function = json_body["function"]
            return {
                "success": True,
                "action": "analytics.info",
                "data": self.info_map[function],
                "meta": {"request_id": "r1"},
            }
        raise AssertionError(f"Unexpected path: {path}")


class TestAnalyticsApiCoverageSync(unittest.TestCase):
    def test_analytics_function_info_and_help_proxy_methods(self) -> None:
        api = Mock()
        api.info.return_value = {"name": "rolling_volatility"}
        api.help.return_value = "help text"

        fn = AnalyticsFunction(api, "rolling_volatility")
        info = fn.info(refresh=True)
        help_text = fn.help()

        self.assertEqual(info, {"name": "rolling_volatility"})
        self.assertEqual(help_text, "help text")
        api.info.assert_called_once_with("rolling_volatility", refresh=True)
        api.help.assert_called_once_with("rolling_volatility")

    def test_help_overview_without_function(self) -> None:
        stub = _SyncRequestStub()
        api = AnalyticsAPI(stub)
        text = api.help()
        self.assertIn("Analytics functions: 2", text)
        self.assertIn("Examples: rolling_volatility, rolling_beta", text)
        self.assertIn("client.analytics.<function>", text)

    def test_help_overview_fallback_when_list_fails(self) -> None:
        api = AnalyticsAPI(Mock())
        api.list = Mock(side_effect=RuntimeError("boom"))  # type: ignore[method-assign]
        text = api.help()
        self.assertIn("client.analytics.<function>(...)", text)

    def test_help_function_no_common_and_no_specific_params(self) -> None:
        stub = _SyncRequestStub()
        api = AnalyticsAPI(stub)
        text = api.help("empty_params")
        self.assertEqual(text.count("- (none)"), 2)

    def test_search_with_query_and_empty_query(self) -> None:
        stub = _SyncRequestStub()
        api = AnalyticsAPI(stub)
        self.assertEqual(api.search("vol"), ["rolling_volatility"])
        self.assertEqual(api.search("   "), ["rolling_volatility", "rolling_beta"])

    def test_schema_dict_and_non_dict(self) -> None:
        stub = _SyncRequestStub()
        api = AnalyticsAPI(stub)
        schema = api.schema("rolling_volatility")
        self.assertIn("common", schema)
        self.assertEqual(api.schema("no_dict_params"), {})

    def test_example_renders_python_and_curl_snippets(self) -> None:
        stub = _SyncRequestStub()
        api = AnalyticsAPI(stub)
        example = api.example("rolling_volatility", identifiers="ABC", Period=123)
        self.assertIn("client.analytics.rolling_volatility(", example)
        self.assertIn("identifiers='ABC'", example)
        self.assertIn("Period=123", example)
        self.assertIn('"action":"analytics.rolling_volatility"', example)

    def test_getattr_underscore_rejected_and_dir_includes_cached_functions(self) -> None:
        api = AnalyticsAPI(Mock())
        with self.assertRaises(AttributeError):
            _ = api.__getattr__("_private")

        api._list_cache = {"functions": ["f1", 123, "f2"]}  # type: ignore[assignment]
        names = api.__dir__()
        self.assertIn("f1", names)
        self.assertIn("f2", names)


class _AsyncRequestStub:
    def __init__(self) -> None:
        self.calls = []
        self.listing = {"functions": ["rolling_beta", "rolling_volatility"], "count": 2, "categories": ["single"]}
        self.info_map = {
            "rolling_beta": {
                "description": "Rolling beta",
                "module": "analytics.single.beta",
                "docstring": "Doc for rolling beta",
                "parameters": {
                    "common": [{"name": "identifiers", "required": True, "type": "string"}],
                    "specific": [{"name": "Period", "required": False, "default": 252, "type": "integer"}],
                },
            },
            "empty": {"parameters": {"common": [], "specific": []}},
            "bad_schema": {"parameters": ["bad"]},
        }

    async def _request(self, method: str, path: str, *, json_body=None):
        self.calls.append((method, path, json_body))
        if path == "/T-Api/v3/analytics/list":
            return {"success": True, "action": "analytics.list", "data": self.listing, "meta": {"request_id": "r1"}}
        if path == "/T-Api/v3/analytics/info":
            fn = json_body["function"]
            return {"success": True, "action": "analytics.info", "data": self.info_map[fn], "meta": {"request_id": "r1"}}
        raise AssertionError(f"Unexpected path: {path}")


class _AsyncCallDataClient:
    def __init__(self, result):
        self.result = result
        self.calls = []

    async def call_data(self, action: str, *, params, options, context, response_format):
        self.calls.append((action, params, options, context, response_format))
        return self.result


class _AsyncApiForFunction:
    def __init__(self, result, info_payload):
        self._client = _AsyncCallDataClient(result)
        self._info_payload = info_payload
        self.info_calls = []
        self.help_calls = []

    async def info(self, name: str, *, refresh: bool = False):
        self.info_calls.append((name, refresh))
        return self._info_payload

    async def help(self, name: str):
        self.help_calls.append(name)
        return f"help:{name}"


class TestAnalyticsApiCoverageAsync(unittest.IsolatedAsyncioTestCase):
    async def test_async_analytics_function_strict_and_as_table_paths(self) -> None:
        info_payload = {
            "parameters": {
                "common": [{"name": "identifiers", "required": True, "type": "string"}],
                "specific": [{"name": "Period", "required": False, "default": 252, "type": "integer"}],
            }
        }

        api_bytes = _AsyncApiForFunction(b"ARROW", info_payload)
        fn_bytes = AsyncAnalyticsFunction(api_bytes, "rolling_beta")
        with patch.object(_analytics_module, "_decode_table", return_value={"decoded": "bytes"}) as decode_table:
            out_bytes = await fn_bytes(identifiers="X", strict=True, as_table="pyarrow")
        self.assertEqual(out_bytes, {"decoded": "bytes"})
        decode_table.assert_called_once_with(b"ARROW", "pyarrow")
        self.assertEqual(api_bytes.info_calls, [("rolling_beta", False)])

        api_json = _AsyncApiForFunction({"result": [{"x": 1}]}, info_payload)
        fn_json = AsyncAnalyticsFunction(api_json, "rolling_beta")
        with patch.object(_analytics_module, "_decode_table_from_json", return_value={"decoded": "json"}) as decode_json:
            out_json = await fn_json(identifiers="X", as_table="pandas")
        self.assertEqual(out_json, {"decoded": "json"})
        decode_json.assert_called_once_with([{"x": 1}], "pandas")

        api_bad = _AsyncApiForFunction({"value": 10}, info_payload)
        fn_bad = AsyncAnalyticsFunction(api_bad, "rolling_beta")
        with self.assertRaises(TepiloraAPIError):
            await fn_bad(identifiers="X", as_table="pandas")

    async def test_async_analytics_function_info_and_help_proxy_methods(self) -> None:
        info_payload = {"parameters": {"common": [], "specific": []}}
        api = _AsyncApiForFunction({"ok": True}, info_payload)
        fn = AsyncAnalyticsFunction(api, "rolling_beta")

        info = await fn.info(refresh=True)
        help_text = await fn.help()
        self.assertEqual(info, info_payload)
        self.assertEqual(help_text, "help:rolling_beta")
        self.assertEqual(api.info_calls, [("rolling_beta", True)])
        self.assertEqual(api.help_calls, ["rolling_beta"])

    async def test_async_api_list_info_help_search_schema_and_getattr(self) -> None:
        stub = _AsyncRequestStub()
        api = AsyncAnalyticsAPI(stub)

        listing1 = await api.list()
        listing2 = await api.list()
        listing_by_category = await api.list(category="single")
        self.assertIs(listing1, listing2)
        self.assertEqual(listing_by_category["count"], 2)

        info1 = await api.info("rolling_beta")
        info2 = await api.info("rolling_beta")
        self.assertIs(info1, info2)

        overview = await api.help()
        self.assertIn("Analytics functions: 2", overview)

        detailed = await api.help("rolling_beta")
        self.assertIn("analytics.rolling_beta", detailed)
        self.assertIn("Docstring:", detailed)

        empty_sections = await api.help("empty")
        self.assertEqual(empty_sections.count("- (none)"), 2)

        filtered = await api.search("vol")
        all_names = await api.search("  ")
        self.assertEqual(filtered, ["rolling_volatility"])
        self.assertEqual(all_names, ["rolling_beta", "rolling_volatility"])

        good_schema = await api.schema("rolling_beta")
        bad_schema = await api.schema("bad_schema")
        self.assertIn("common", good_schema)
        self.assertEqual(bad_schema, {})

        with self.assertRaises(AttributeError):
            _ = api.__getattr__("_private")
        fn = api.__getattr__("rolling_beta")
        self.assertIsInstance(fn, AsyncAnalyticsFunction)

    async def test_async_help_overview_fallback_when_list_fails(self) -> None:
        api = AsyncAnalyticsAPI(Mock())
        api.list = AsyncMock(side_effect=RuntimeError("boom"))  # type: ignore[method-assign]
        text = await api.help()
        self.assertIn("client.analytics.info", text)


class TestArrowDecoderBranches(unittest.TestCase):
    def _install_fake_pyarrow(self, ipc_module: types.ModuleType) -> None:
        pyarrow_module = types.ModuleType("pyarrow")
        pyarrow_module.__path__ = []  # type: ignore[attr-defined]
        pyarrow_module.py_buffer = lambda b: ("buffer", b)
        pyarrow_module.ipc = ipc_module
        self._patcher = patch.dict(
            sys.modules,
            {
                "pyarrow": pyarrow_module,
                "pyarrow.ipc": ipc_module,
            },
            clear=False,
        )
        self._patcher.start()

    def tearDown(self) -> None:
        patcher = getattr(self, "_patcher", None)
        if patcher is not None:
            patcher.stop()

    def test_read_ipc_stream_uses_read_ipc_stream_read_all(self) -> None:
        class Reader:
            def read_all(self):
                return {"rows": 2}

        ipc_module = types.ModuleType("pyarrow.ipc")
        ipc_module.read_ipc_stream = lambda source: Reader()
        self._install_fake_pyarrow(ipc_module)

        out = read_ipc_stream(b"bytes")
        self.assertEqual(out, {"rows": 2})

    def test_read_ipc_stream_returns_result_without_read_all(self) -> None:
        sentinel = {"already": "table"}
        ipc_module = types.ModuleType("pyarrow.ipc")
        ipc_module.read_ipc_stream = lambda source: sentinel
        self._install_fake_pyarrow(ipc_module)

        out = read_ipc_stream(b"bytes")
        self.assertIs(out, sentinel)

    def test_read_ipc_stream_falls_back_to_open_stream(self) -> None:
        class Reader:
            def read_all(self):
                return {"rows": 3}

        ipc_module = types.ModuleType("pyarrow.ipc")
        ipc_module.open_stream = lambda source: Reader()
        self._install_fake_pyarrow(ipc_module)

        out = read_ipc_stream(b"bytes")
        self.assertEqual(out, {"rows": 3})

