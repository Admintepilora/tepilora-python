from __future__ import annotations

import logging
from dataclasses import dataclass
from textwrap import indent
from typing import Any, Dict, List, Mapping, Optional

from .endpoints.analytics import _AnalyticsMethodsMixin, _AsyncAnalyticsMethodsMixin
from .errors import TepiloraAPIError

logger = logging.getLogger("Tepilora.analytics")
from .arrow import read_ipc_stream, read_ipc_stream_polars


def _unwrap_envelope(obj: Any) -> Any:
    if isinstance(obj, dict) and "data" in obj and any(k in obj for k in ("success", "action", "meta")):
        return obj.get("data")
    return obj


def _format_param(p: Mapping[str, Any]) -> str:
    name = p.get("name", "?")
    required = bool(p.get("required", False))
    default = p.get("default", None)
    nullable = bool(p.get("nullable", False))

    type_label = p.get("type")
    one_of = p.get("oneOf")
    if isinstance(one_of, list) and one_of:
        types: List[str] = []
        for entry in one_of:
            if isinstance(entry, dict) and "type" in entry:
                types.append(str(entry["type"]))
            elif isinstance(entry, str):
                types.append(entry)
        if types:
            type_label = " | ".join(types)

    bits = [str(name)]
    if type_label:
        bits.append(f": {type_label}")
    if nullable:
        bits.append(" (nullable)")
    bits.append(" [required]" if required else " [optional]")
    if not required and "default" in p:
        bits.append(f" default={default!r}")
    return "".join(bits)


def _extract_param_specs(info: Mapping[str, Any]) -> List[Mapping[str, Any]]:
    params = info.get("parameters", {}) if isinstance(info, dict) else {}
    if not isinstance(params, dict):
        return []
    out: List[Mapping[str, Any]] = []
    for group_name in ("common", "specific"):
        group = params.get(group_name, [])
        if isinstance(group, list):
            out.extend([p for p in group if isinstance(p, dict)])
    return out


def _validate_and_fill_params(info: Mapping[str, Any], provided: Dict[str, Any]) -> Dict[str, Any]:
    specs = _extract_param_specs(info)
    allowed = {p.get("name") for p in specs if isinstance(p.get("name"), str)}
    unknown = sorted([k for k in provided.keys() if k not in allowed])
    if unknown:
        raise ValueError(f"Unknown parameters: {unknown}")

    filled = dict(provided)
    for p in specs:
        name = p.get("name")
        if not isinstance(name, str) or not name:
            continue
        required = bool(p.get("required", False))
        has_default = "default" in p
        if name not in filled:
            if has_default:
                filled[name] = p.get("default")
            elif required:
                raise ValueError(f"Missing required parameter: {name}")
    return filled


def _normalize_param_names(info: Mapping[str, Any], provided: Dict[str, Any]) -> Dict[str, Any]:
    specs = _extract_param_specs(info)
    allowed = {p.get("name") for p in specs if isinstance(p.get("name"), str)}
    if not allowed:
        return dict(provided)

    lower_map: Dict[str, Optional[str]] = {}
    for name in allowed:
        lower = name.lower()
        if lower in lower_map and lower_map[lower] != name:
            lower_map[lower] = None
        else:
            lower_map[lower] = name

    normalized: Dict[str, Any] = {}
    for key, value in provided.items():
        if key in allowed:
            if key in normalized:
                raise ValueError(f"Duplicate parameter: {key}")
            normalized[key] = value
            continue
        mapped = lower_map.get(key.lower())
        if mapped:
            if mapped in normalized:
                raise ValueError(f"Duplicate parameter: {key}")
            normalized[mapped] = value
            continue
        normalized[key] = value

    return normalized


def _decode_table(content: bytes, as_table: str) -> Any:
    mode = as_table.strip().lower()
    if mode == "pyarrow":
        return read_ipc_stream(content)
    if mode == "polars":
        return read_ipc_stream_polars(content)
    if mode == "pandas":
        table = read_ipc_stream(content)
        if hasattr(table, "to_pandas"):
            return table.to_pandas()
        raise TepiloraAPIError(message="pyarrow Table does not support to_pandas()")
    raise ValueError("as_table must be one of: 'pyarrow', 'polars', 'pandas'")


def _coerce_tabular_json(result: Any) -> Any:
    """
    Best-effort extraction of a tabular payload from common analytics JSON shapes.

    Supported:
    - [{'col': ...}, ...]
    - {'result': [{'col': ...}, ...]}
    """
    if isinstance(result, dict) and "result" in result:
        result = result.get("result")
    if isinstance(result, list):
        return result
    return None


def _decode_table_from_json(tabular: Any, as_table: str) -> Any:
    mode = as_table.strip().lower()
    if mode == "polars":
        try:
            import polars as pl  # type: ignore
        except Exception as e:  # pragma: no cover
            raise TepiloraAPIError(message="polars is required for as_table='polars'") from e
        return pl.DataFrame(tabular)

    if mode == "pandas":
        try:
            import pandas as pd  # type: ignore
        except Exception as e:  # pragma: no cover
            raise TepiloraAPIError(message="pandas is required for as_table='pandas'") from e
        return pd.DataFrame(tabular)

    if mode == "pyarrow":
        try:
            import pyarrow as pa  # type: ignore
        except Exception as e:  # pragma: no cover
            raise TepiloraAPIError(message="pyarrow is required for as_table='pyarrow'") from e
        return pa.Table.from_pylist(tabular)

    raise ValueError("as_table must be one of: 'pyarrow', 'polars', 'pandas'")


@dataclass(frozen=True)
class AnalyticsFunction:
    """
    Callable analytics function bound to a client, with introspection helpers.

    Example:
        client.analytics.rolling_volatility(identifiers="IE00...", Period=252)
        client.analytics.rolling_volatility.help()
    """

    _api: "AnalyticsAPI"
    name: str

    def __call__(
        self,
        *,
        options: Optional[Dict[str, Any]] = None,
        context: Optional[Dict[str, Any]] = None,
        response_format: Optional[str] = None,
        as_table: Optional[str] = None,
        strict: bool = False,
        **params: Any,
    ) -> Any:
        call = getattr(self._api, "_call_analytics", None)
        if callable(call):
            return call(
                self.name,
                params,
                options=options,
                context=context,
                response_format=response_format,
                as_table=as_table,
                strict=strict,
            )

        action = f"analytics.{self.name}"
        payload = dict(params)
        if strict:
            info = self._api.info(self.name)
            payload = _normalize_param_names(info, payload)
            payload = _validate_and_fill_params(info, payload)
        effective_format = "arrow" if as_table else response_format
        result = self._api._client.call_data(
            action,
            params=payload,
            options=options,
            context=context,
            response_format=effective_format,
        )
        if as_table:
            if isinstance(result, (bytes, bytearray)):
                return _decode_table(bytes(result), as_table)
            tabular = _coerce_tabular_json(result)
            if tabular is None:
                raise TepiloraAPIError(message="Expected Arrow bytes or tabular JSON for as_table")
            return _decode_table_from_json(tabular, as_table)
        return result

    def info(self, *, refresh: bool = False) -> Dict[str, Any]:
        return self._api.info(self.name, refresh=refresh)

    def help(self) -> str:
        return self._api.help(self.name)


@dataclass(frozen=True)
class AsyncAnalyticsFunction:
    _api: "AsyncAnalyticsAPI"
    name: str

    async def __call__(
        self,
        *,
        options: Optional[Dict[str, Any]] = None,
        context: Optional[Dict[str, Any]] = None,
        response_format: Optional[str] = None,
        as_table: Optional[str] = None,
        strict: bool = False,
        **params: Any,
    ) -> Any:
        call = getattr(self._api, "_call_analytics", None)
        if callable(call):
            return await call(
                self.name,
                params,
                options=options,
                context=context,
                response_format=response_format,
                as_table=as_table,
                strict=strict,
            )

        action = f"analytics.{self.name}"
        payload = dict(params)
        if strict:
            info = await self._api.info(self.name)
            payload = _normalize_param_names(info, payload)
            payload = _validate_and_fill_params(info, payload)
        effective_format = "arrow" if as_table else response_format
        result = await self._api._client.call_data(
            action,
            params=payload,
            options=options,
            context=context,
            response_format=effective_format,
        )
        if as_table:
            if isinstance(result, (bytes, bytearray)):
                return _decode_table(bytes(result), as_table)
            tabular = _coerce_tabular_json(result)
            if tabular is None:
                raise TepiloraAPIError(message="Expected Arrow bytes or tabular JSON for as_table")
            return _decode_table_from_json(tabular, as_table)
        return result

    async def info(self, *, refresh: bool = False) -> Dict[str, Any]:
        return await self._api.info(self.name, refresh=refresh)

    async def help(self) -> str:
        return await self._api.help(self.name)


class AnalyticsAPI(_AnalyticsMethodsMixin):
    def __init__(self, client: Any) -> None:
        self._client = client
        self._list_cache: Optional[Dict[str, Any]] = None
        self._info_cache: Dict[str, Dict[str, Any]] = {}

    def _call_analytics(
        self,
        name: str,
        params: Dict[str, Any],
        *,
        options: Optional[Dict[str, Any]] = None,
        context: Optional[Dict[str, Any]] = None,
        response_format: Optional[str] = None,
        as_table: Optional[str] = None,
        strict: bool = False,
    ) -> Any:
        payload = dict(params)
        action = f"analytics.{name}"
        if strict:
            info = self.info(name)
            payload = _normalize_param_names(info, payload)
            payload = _validate_and_fill_params(info, payload)
        effective_format = "arrow" if as_table else response_format
        result = self._client.call_data(
            action,
            params=payload,
            options=options,
            context=context,
            response_format=effective_format,
        )
        if as_table:
            if isinstance(result, (bytes, bytearray)):
                return _decode_table(bytes(result), as_table)
            tabular = _coerce_tabular_json(result)
            if tabular is None:
                raise TepiloraAPIError(message="Expected Arrow bytes or tabular JSON for as_table")
            return _decode_table_from_json(tabular, as_table)
        return result

    def list(self, *, category: Optional[str] = None, refresh: bool = False) -> Dict[str, Any]:
        if self._list_cache is not None and not refresh and category is None:
            return self._list_cache

        payload: Dict[str, Any] = {}
        if category is not None:
            payload["category"] = category

        raw = self._client._request("POST", "/T-Api/v3/analytics/list", json_body=payload)
        data = _unwrap_envelope(raw)
        if not isinstance(data, dict):
            raise TepiloraAPIError(message="Unexpected analytics.list response")

        if category is None:
            self._list_cache = data
        return data

    def info(self, function: str, *, refresh: bool = False) -> Dict[str, Any]:
        if not refresh and function in self._info_cache:
            return self._info_cache[function]

        raw = self._client._request("POST", "/T-Api/v3/analytics/info", json_body={"function": function})
        data = _unwrap_envelope(raw)
        if not isinstance(data, dict):
            raise TepiloraAPIError(message="Unexpected analytics.info response")
        self._info_cache[function] = data
        return data

    def help(self, function: Optional[str] = None) -> str:
        if function is None:
            try:
                listing = self.list()
                funcs = listing.get("functions", [])
                count = listing.get("count", len(funcs))
                cats = listing.get("categories", [])
                out = [f"Analytics functions: {count} (categories: {cats})"]
                if isinstance(funcs, list) and funcs:
                    preview = ", ".join(funcs[:20])
                    out.append(f"Examples: {preview}{', ...' if len(funcs) > 20 else ''}")
                out.append("Call: client.analytics.<function>(identifiers=..., Period=..., ...)")
                return "\n".join(out)
            except Exception:
                logger.debug("Failed to fetch analytics list for help()", exc_info=True)
                return "Call: client.analytics.<function>(...) or client.analytics.info('<function>')"

        info = self.info(function)
        params = info.get("parameters", {}) if isinstance(info, dict) else {}
        common = params.get("common", []) if isinstance(params, dict) else []
        specific = params.get("specific", []) if isinstance(params, dict) else []

        lines: List[str] = []
        lines.append(f"analytics.{function}")
        if info.get("description"):
            lines.append(str(info.get("description")))
        if info.get("module"):
            lines.append(f"module: {info.get('module')}")
        lines.append("")
        lines.append("Parameters (common):")
        if isinstance(common, list) and common:
            lines.extend([f"- {_format_param(p)}" for p in common if isinstance(p, dict)])
        else:
            lines.append("- (none)")
        lines.append("")
        lines.append("Parameters (specific):")
        if isinstance(specific, list) and specific:
            lines.extend([f"- {_format_param(p)}" for p in specific if isinstance(p, dict)])
        else:
            lines.append("- (none)")

        doc = info.get("docstring")
        if doc:
            lines.append("")
            lines.append("Docstring:")
            lines.append(indent(str(doc).strip(), "  "))

        return "\n".join(lines)

    def search(self, text: str, *, category: Optional[str] = None) -> List[str]:
        """
        Search function names by substring.
        """
        listing = self.list(category=category)
        funcs = listing.get("functions", []) if isinstance(listing, dict) else []
        if not isinstance(funcs, list):
            return []
        q = text.strip().lower()
        if not q:
            return [f for f in funcs if isinstance(f, str)]
        return [f for f in funcs if isinstance(f, str) and q in f.lower()]

    def schema(self, function: str) -> Dict[str, Any]:
        """
        Return the structured parameter schema for a function (from analytics.info).
        """
        info = self.info(function)
        params = info.get("parameters", {})
        if isinstance(params, dict):
            return params
        return {}

    def example(
        self,
        function: str,
        *,
        identifiers: Optional[Any] = "IE00B4L5Y983EURXMIL",
        **overrides: Any,
    ) -> str:
        """
        Return example snippets (python + curl) for calling an analytics function.
        """
        info = self.info(function)
        params = _validate_and_fill_params(info, {"identifiers": identifiers, **overrides})
        python_lines = [
            "import Tepilora as T",
            "",
            "client = T.TepiloraClient(api_key=\"YOUR_API_KEY\", base_url=\"https://api.tepiloradata.com\")",
            f"result = client.analytics.{function}(",
        ]
        for k, v in params.items():
            python_lines.append(f"    {k}={v!r},")
        python_lines.append(")")
        python_lines.append("print(result)")

        curl = (
            "curl -sS 'https://api.tepiloradata.com/T-Api/v3' \\\n"
            "  -H 'Content-Type: application/json' \\\n"
            "  -H 'X-API-Key: YOUR_API_KEY' \\\n"
            f"  -d '{{\"action\":\"analytics.{function}\",\"params\":{params}}}'"
        )
        return "\n".join(python_lines) + "\n\n" + curl

    def __getattr__(self, name: str) -> AnalyticsFunction:
        if name.startswith("_"):
            raise AttributeError(name)
        return AnalyticsFunction(self, name)

    def __dir__(self) -> List[str]:
        base = set(super().__dir__())
        listing = self._list_cache
        if isinstance(listing, dict):
            funcs = listing.get("functions", [])
            if isinstance(funcs, list):
                base.update([f for f in funcs if isinstance(f, str)])
        return sorted(base)


class AsyncAnalyticsAPI(_AsyncAnalyticsMethodsMixin):
    def __init__(self, client: Any) -> None:
        self._client = client
        self._list_cache: Optional[Dict[str, Any]] = None
        self._info_cache: Dict[str, Dict[str, Any]] = {}

    async def _call_analytics(
        self,
        name: str,
        params: Dict[str, Any],
        *,
        options: Optional[Dict[str, Any]] = None,
        context: Optional[Dict[str, Any]] = None,
        response_format: Optional[str] = None,
        as_table: Optional[str] = None,
        strict: bool = False,
    ) -> Any:
        payload = dict(params)
        action = f"analytics.{name}"
        if strict:
            info = await self.info(name)
            payload = _normalize_param_names(info, payload)
            payload = _validate_and_fill_params(info, payload)
        effective_format = "arrow" if as_table else response_format
        result = await self._client.call_data(
            action,
            params=payload,
            options=options,
            context=context,
            response_format=effective_format,
        )
        if as_table:
            if isinstance(result, (bytes, bytearray)):
                return _decode_table(bytes(result), as_table)
            tabular = _coerce_tabular_json(result)
            if tabular is None:
                raise TepiloraAPIError(message="Expected Arrow bytes or tabular JSON for as_table")
            return _decode_table_from_json(tabular, as_table)
        return result

    async def list(self, *, category: Optional[str] = None, refresh: bool = False) -> Dict[str, Any]:
        if self._list_cache is not None and not refresh and category is None:
            return self._list_cache

        payload: Dict[str, Any] = {}
        if category is not None:
            payload["category"] = category

        raw = await self._client._request("POST", "/T-Api/v3/analytics/list", json_body=payload)
        data = _unwrap_envelope(raw)
        if not isinstance(data, dict):
            raise TepiloraAPIError(message="Unexpected analytics.list response")

        if category is None:
            self._list_cache = data
        return data

    async def info(self, function: str, *, refresh: bool = False) -> Dict[str, Any]:
        if not refresh and function in self._info_cache:
            return self._info_cache[function]

        raw = await self._client._request("POST", "/T-Api/v3/analytics/info", json_body={"function": function})
        data = _unwrap_envelope(raw)
        if not isinstance(data, dict):
            raise TepiloraAPIError(message="Unexpected analytics.info response")
        self._info_cache[function] = data
        return data

    async def help(self, function: Optional[str] = None) -> str:
        if function is None:
            try:
                listing = await self.list()
                funcs = listing.get("functions", [])
                count = listing.get("count", len(funcs))
                cats = listing.get("categories", [])
                out = [f"Analytics functions: {count} (categories: {cats})"]
                if isinstance(funcs, list) and funcs:
                    preview = ", ".join(funcs[:20])
                    out.append(f"Examples: {preview}{', ...' if len(funcs) > 20 else ''}")
                out.append("Call: client.analytics.<function>(identifiers=..., Period=..., ...)")
                return "\n".join(out)
            except Exception:
                logger.debug("Failed to fetch analytics list for help()", exc_info=True)
                return "Call: client.analytics.<function>(...) or client.analytics.info('<function>')"

        info = await self.info(function)
        params = info.get("parameters", {}) if isinstance(info, dict) else {}
        common = params.get("common", []) if isinstance(params, dict) else []
        specific = params.get("specific", []) if isinstance(params, dict) else []

        lines: List[str] = []
        lines.append(f"analytics.{function}")
        if info.get("description"):
            lines.append(str(info.get("description")))
        if info.get("module"):
            lines.append(f"module: {info.get('module')}")
        lines.append("")
        lines.append("Parameters (common):")
        if isinstance(common, list) and common:
            lines.extend([f"- {_format_param(p)}" for p in common if isinstance(p, dict)])
        else:
            lines.append("- (none)")
        lines.append("")
        lines.append("Parameters (specific):")
        if isinstance(specific, list) and specific:
            lines.extend([f"- {_format_param(p)}" for p in specific if isinstance(p, dict)])
        else:
            lines.append("- (none)")

        doc = info.get("docstring")
        if doc:
            lines.append("")
            lines.append("Docstring:")
            lines.append(indent(str(doc).strip(), "  "))

        return "\n".join(lines)

    async def search(self, text: str, *, category: Optional[str] = None) -> List[str]:
        listing = await self.list(category=category)
        funcs = listing.get("functions", []) if isinstance(listing, dict) else []
        if not isinstance(funcs, list):
            return []
        q = text.strip().lower()
        if not q:
            return [f for f in funcs if isinstance(f, str)]
        return [f for f in funcs if isinstance(f, str) and q in f.lower()]

    async def schema(self, function: str) -> Dict[str, Any]:
        info = await self.info(function)
        params = info.get("parameters", {})
        if isinstance(params, dict):
            return params
        return {}

    def __getattr__(self, name: str) -> AsyncAnalyticsFunction:
        if name.startswith("_"):
            raise AttributeError(name)
        return AsyncAnalyticsFunction(self, name)


class _ModuleAnalyticsProxy:
    def __getattr__(self, name: str) -> Any:
        from ._default_client import get_default_client

        return getattr(get_default_client().analytics, name)

    def __dir__(self) -> List[str]:
        from ._default_client import get_default_client

        return dir(get_default_client().analytics)


# Allows: `import Tepilora as T; T.analytics.rolling_volatility(...)`
analytics = _ModuleAnalyticsProxy()
