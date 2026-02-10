from __future__ import annotations

import logging
import os
import warnings
from dataclasses import dataclass
from typing import Any, Dict, Mapping, Optional, Tuple, Union

import httpx

from .errors import TepiloraAPIError
from .capabilities import _client_capabilities

logger = logging.getLogger("Tepilora")
from .models import V3BinaryMeta, V3BinaryResponse, V3Request, V3Response
from .version import __version__


V3_PREFIX = "/T-Api/v3"


def _normalize_base_url(base_url: str) -> str:
    return base_url.rstrip("/")


def _is_json_response(response: httpx.Response) -> bool:
    content_type = response.headers.get("Content-Type", "")
    base = content_type.split(";", 1)[0].strip().lower()
    return base == "application/json" or base.endswith("+json")


def _content_type(response: httpx.Response) -> str:
    return response.headers.get("Content-Type", "").split(";", 1)[0].strip().lower()


def _format_to_accept(response_format: str) -> str:
    """
    Convert response format to Accept header value.

    Accepts:
    - Known keywords: json, arrow, parquet, csv -> mapped to MIME types
    - Explicit MIME types (containing '/'): passed through as-is
    - Unknown keywords: raises ValueError for early error detection
    """
    fmt = response_format.strip().lower()
    format_map = {
        "json": "application/json",
        "arrow": "application/vnd.apache.arrow.stream",
        "parquet": "application/vnd.apache.parquet",
        "csv": "text/csv",
    }
    if fmt in format_map:
        return format_map[fmt]
    # Allow explicit MIME types (e.g., "application/x-custom")
    if "/" in response_format:
        return response_format.strip()
    # Unknown format - raise for early error detection
    raise ValueError(
        f"Unsupported response format: {response_format!r}. "
        f"Valid formats: {', '.join(format_map.keys())} or explicit MIME type"
    )


def _parse_binary_meta(headers: Mapping[str, str]) -> V3BinaryMeta:
    def get_int(name: str) -> Optional[int]:
        raw = headers.get(name)
        if raw is None or raw == "":
            return None
        try:
            return int(raw)
        except ValueError:
            return None

    return V3BinaryMeta(
        request_id=headers.get("X-Tepilora-Request-Id"),
        execution_time_ms=get_int("X-Tepilora-Execution-Time-Ms"),
        total_count=get_int("X-Tepilora-Total-Count"),
        row_count=get_int("X-Tepilora-Row-Count"),
    )


# ---------------------------------------------------------------------------
# Option 2: Server header SDK version check
# ---------------------------------------------------------------------------
_upgrade_warned = False

_UPGRADE_HINT = "This may require a newer SDK version. Try: pip install --upgrade tepilora"


def _parse_semver(version_str: str) -> Tuple[int, ...]:
    """Parse a semver string like '0.3.1' into a comparable tuple (0, 3, 1)."""
    return tuple(int(p) for p in version_str.strip().split("."))


def _check_sdk_version(response_headers: Mapping[str, str]) -> None:
    """Check X-Tepilora-Min-SDK-Version header and warn once if SDK is outdated."""
    global _upgrade_warned
    if _upgrade_warned:
        return

    min_version = response_headers.get("X-Tepilora-Min-SDK-Version")
    if not min_version:
        return

    try:
        current = _parse_semver(__version__)
        required = _parse_semver(min_version)
    except (ValueError, AttributeError):
        return

    if current < required:
        _upgrade_warned = True
        warnings.warn(
            f"Tepilora SDK v{__version__} is outdated (server requires >= {min_version}). "
            f"Upgrade: pip install --upgrade tepilora",
            stacklevel=4,
        )


# ---------------------------------------------------------------------------
# Error handling with upgrade hint (Option 3)
# ---------------------------------------------------------------------------
_UNKNOWN_ACTION_KEYWORDS = ("unknown action", "action not found", "invalid action", "unsupported action")


def _raise_for_error_response(response: httpx.Response) -> None:
    status = response.status_code
    if 200 <= status < 300:
        return

    error_data: Optional[Dict[str, Any]] = None
    response_text: Optional[str] = None
    message = f"Request failed ({status})"

    try:
        if _is_json_response(response):
            error_data = response.json()
            if isinstance(error_data, dict):
                message = (
                    error_data.get("message")
                    or error_data.get("detail")
                    or error_data.get("error", {}).get("message")
                    or message
                )
        else:
            response_text = response.text
    except (ValueError, TypeError, KeyError):
        # JSON parsing failed, fall back to raw text
        response_text = response.text

    # Option 3: suggest upgrade for unknown action errors
    if status in (400, 404):
        msg_lower = message.lower()
        if any(kw in msg_lower for kw in _UNKNOWN_ACTION_KEYWORDS):
            message = f"{message}\nHint: {_UPGRADE_HINT}"

    raise TepiloraAPIError(message=message, status_code=status, error_data=error_data, response_text=response_text)


@dataclass
class _ClientConfig:
    api_key: Optional[str]
    base_url: str
    timeout: Union[float, httpx.Timeout, None]
    send_legacy_query_key: bool

    def auth_headers(self) -> Dict[str, str]:
        headers: Dict[str, str] = {
            "Accept": "application/json",
            "User-Agent": f"Tepilora-Python/{__version__}",
        }
        if self.api_key:
            headers["X-API-Key"] = self.api_key
        return headers

    def auth_query(self) -> Dict[str, str]:
        if self.api_key and self.send_legacy_query_key:
            return {"apikey": self.api_key}
        return {}


class TepiloraClient:
    def __init__(
        self,
        api_key: Optional[str] = None,
        *,
        base_url: str = "https://tepiloradata.com",
        timeout: Union[float, httpx.Timeout, None] = 30.0,
        send_legacy_query_key: bool = False,
        client: Optional[httpx.Client] = None,
        transport: Optional[httpx.BaseTransport] = None,
    ) -> None:
        env_base_url = os.getenv("TEPILORA_BASE_URL")
        resolved_base_url = _normalize_base_url(
            env_base_url if (env_base_url and base_url == "https://tepiloradata.com") else base_url
        )
        resolved_api_key = api_key if api_key is not None else os.getenv("TEPILORA_API_KEY")
        self._config = _ClientConfig(
            api_key=resolved_api_key,
            base_url=resolved_base_url,
            timeout=timeout,
            send_legacy_query_key=send_legacy_query_key,
        )

        if client is not None:
            self._client = client
            self._owns_client = False
        else:
            self._client = httpx.Client(
                base_url=self._config.base_url,
                timeout=self._config.timeout,
                headers=self._config.auth_headers(),
                transport=transport,
            )
            self._owns_client = True

        from .endpoints import (
            NewsAPI, PublicationsAPI, QueriesAPI, SearchAPI, SecuritiesAPI,
            PortfolioAPI, MacroAPI, AlertsAPI,
            StocksAPI, BondsAPI, OptionsAPI, EsgAPI, FactorsAPI, FhAPI, DataAPI,
            ClientsAPI, ProfilingAPI, BillingAPI, DocumentsAPI, AlternativesAPI,
            WorkflowsAPI,
        )
        from .analytics import AnalyticsAPI

        # Existing namespaces
        self.securities = SecuritiesAPI(self)
        self.news = NewsAPI(self)
        self.publications = PublicationsAPI(self)
        self.queries = QueriesAPI(self)
        self.search = SearchAPI(self)
        self.analytics = AnalyticsAPI(self)

        # High priority namespaces
        self.portfolio = PortfolioAPI(self)
        self.macro = MacroAPI(self)
        self.alerts = AlertsAPI(self)

        # Medium priority namespaces
        self.stocks = StocksAPI(self)
        self.bonds = BondsAPI(self)
        self.options = OptionsAPI(self)
        self.esg = EsgAPI(self)
        self.factors = FactorsAPI(self)
        self.fh = FhAPI(self)
        self.data = DataAPI(self)

        # Low priority namespaces (B2B/enterprise)
        self.clients = ClientsAPI(self)
        self.profiling = ProfilingAPI(self)
        self.billing = BillingAPI(self)
        self.documents = DocumentsAPI(self)
        self.alternatives = AlternativesAPI(self)

        # Cross-module
        self.workflows = WorkflowsAPI(self)

    def close(self) -> None:
        if self._owns_client:
            self._client.close()

    def __enter__(self) -> "TepiloraClient":
        return self

    def __exit__(self, exc_type: Any, exc: Any, tb: Any) -> Optional[bool]:
        self.close()
        return None

    def _request(
        self,
        method: str,
        path: str,
        *,
        params: Optional[Mapping[str, Any]] = None,
        json_body: Any = None,
        headers: Optional[Mapping[str, str]] = None,
    ) -> Any:
        query = dict(params or {})
        query.update(self._config.auth_query())
        logger.debug("Request: %s %s", method, path)
        response = self._client.request(method, path, params=query or None, json=json_body, headers=headers)
        logger.debug("Response: %d", response.status_code)
        _raise_for_error_response(response)
        if _is_json_response(response):
            return response.json()
        return response.text

    def health(self) -> Any:
        return self._request("GET", f"{V3_PREFIX}/health")

    def pricing(self) -> Any:
        return self._request("GET", f"{V3_PREFIX}/pricing")

    def logs_status(self) -> Any:
        return self._request("GET", f"{V3_PREFIX}/logs/status")

    def capabilities(
        self,
        namespace_or_action: Optional[str] = None,
        *,
        search: Optional[str] = None,
        format: str = "print",
    ) -> Any:
        """
        Discover SDK capabilities.

        Args:
            namespace_or_action: Namespace (e.g., "analytics") or action
                                (e.g., "analytics.rolling_volatility")
            search: Search query to find operations
            format: "print" (default), "text", or "dict"

        Examples:
            client.capabilities()                    # Print all
            client.capabilities("analytics")         # Print namespace
            client.capabilities(search="volatility") # Search
        """
        return _client_capabilities(self, namespace_or_action, search=search, format=format)

    def call(
        self,
        action: str,
        *,
        params: Optional[Dict[str, Any]] = None,
        options: Optional[Dict[str, Any]] = None,
        context: Optional[Dict[str, Any]] = None,
        response_format: Optional[str] = None,
    ) -> Union[V3Response, V3BinaryResponse]:
        request_options = dict(options or {})
        if response_format is not None and "format" not in request_options:
            request_options["format"] = response_format

        query_params: Dict[str, Any] = {}
        accept_headers: Dict[str, str] = {}
        effective_format = request_options.get("format")
        if isinstance(effective_format, str) and effective_format.strip():
            query_params["format"] = effective_format
            accept_headers["Accept"] = _format_to_accept(effective_format)

        req = V3Request(action=action, params=params or {}, options=(request_options or None), context=context)
        logger.debug("V3 call: %s", action)
        response = self._client.request(
            "POST",
            V3_PREFIX,
            params={**query_params, **self._config.auth_query()} or None,
            json=req.to_dict(),
            headers=accept_headers or None,
        )
        logger.debug("V3 response: %d", response.status_code)
        _check_sdk_version(response.headers)
        _raise_for_error_response(response)

        if _is_json_response(response):
            payload = response.json()
            if not isinstance(payload, dict):
                raise TepiloraAPIError(message="Unexpected non-object JSON response from v3 endpoint")
            return V3Response.from_dict(payload)

        content = response.content
        ctype = _content_type(response)
        fmt = str(effective_format or "binary")
        return V3BinaryResponse(
            action=action,
            format=fmt,
            content_type=ctype,
            content=content,
            meta=_parse_binary_meta(response.headers),
            headers=dict(response.headers),
        )

    # Option 3: suggest upgrade for unknown namespaces
    def __getattr__(self, name: str) -> Any:
        if not name.startswith("_"):
            raise AttributeError(
                f"'{type(self).__name__}' has no namespace '{name}'. "
                f"If this is a new API namespace, try: pip install --upgrade tepilora"
            )
        raise AttributeError(f"'{type(self).__name__}' has no attribute '{name}'")

    def call_data(
        self,
        action: str,
        *,
        params: Optional[Dict[str, Any]] = None,
        options: Optional[Dict[str, Any]] = None,
        context: Optional[Dict[str, Any]] = None,
        response_format: Optional[str] = None,
    ) -> Any:
        resp = self.call(action, params=params, options=options, context=context, response_format=response_format)
        if isinstance(resp, V3BinaryResponse):
            return resp.content
        if not resp.success:
            raise TepiloraAPIError(message="V3 action returned success=false", error_data={"response": resp})
        return resp.data

    def call_arrow_ipc_stream(
        self,
        action: str,
        *,
        params: Optional[Dict[str, Any]] = None,
        options: Optional[Dict[str, Any]] = None,
        context: Optional[Dict[str, Any]] = None,
    ) -> V3BinaryResponse:
        resp = self.call(action, params=params, options=options, context=context, response_format="arrow")
        if not isinstance(resp, V3BinaryResponse):
            raise TepiloraAPIError(message="Expected Arrow IPC stream response, got JSON")
        return resp


class AsyncTepiloraClient:
    def __init__(
        self,
        api_key: Optional[str] = None,
        *,
        base_url: str = "https://tepiloradata.com",
        timeout: Union[float, httpx.Timeout, None] = 30.0,
        send_legacy_query_key: bool = False,
        client: Optional[httpx.AsyncClient] = None,
        transport: Optional[httpx.AsyncBaseTransport] = None,
    ) -> None:
        env_base_url = os.getenv("TEPILORA_BASE_URL")
        resolved_base_url = _normalize_base_url(
            env_base_url if (env_base_url and base_url == "https://tepiloradata.com") else base_url
        )
        resolved_api_key = api_key if api_key is not None else os.getenv("TEPILORA_API_KEY")
        self._config = _ClientConfig(
            api_key=resolved_api_key,
            base_url=resolved_base_url,
            timeout=timeout,
            send_legacy_query_key=send_legacy_query_key,
        )

        if client is not None:
            self._client = client
            self._owns_client = False
        else:
            self._client = httpx.AsyncClient(
                base_url=self._config.base_url,
                timeout=self._config.timeout,
                headers=self._config.auth_headers(),
                transport=transport,
            )
            self._owns_client = True

        from .endpoints import (
            AsyncNewsAPI, AsyncPublicationsAPI, AsyncQueriesAPI, AsyncSearchAPI, AsyncSecuritiesAPI,
            AsyncPortfolioAPI, AsyncMacroAPI, AsyncAlertsAPI,
            AsyncStocksAPI, AsyncBondsAPI, AsyncOptionsAPI, AsyncEsgAPI, AsyncFactorsAPI, AsyncFhAPI, AsyncDataAPI,
            AsyncClientsAPI, AsyncProfilingAPI, AsyncBillingAPI, AsyncDocumentsAPI, AsyncAlternativesAPI,
            AsyncWorkflowsAPI,
        )
        from .analytics import AsyncAnalyticsAPI

        # Existing namespaces
        self.securities = AsyncSecuritiesAPI(self)
        self.news = AsyncNewsAPI(self)
        self.publications = AsyncPublicationsAPI(self)
        self.queries = AsyncQueriesAPI(self)
        self.search = AsyncSearchAPI(self)
        self.analytics = AsyncAnalyticsAPI(self)

        # High priority namespaces
        self.portfolio = AsyncPortfolioAPI(self)
        self.macro = AsyncMacroAPI(self)
        self.alerts = AsyncAlertsAPI(self)

        # Medium priority namespaces
        self.stocks = AsyncStocksAPI(self)
        self.bonds = AsyncBondsAPI(self)
        self.options = AsyncOptionsAPI(self)
        self.esg = AsyncEsgAPI(self)
        self.factors = AsyncFactorsAPI(self)
        self.fh = AsyncFhAPI(self)
        self.data = AsyncDataAPI(self)

        # Low priority namespaces (B2B/enterprise)
        self.clients = AsyncClientsAPI(self)
        self.profiling = AsyncProfilingAPI(self)
        self.billing = AsyncBillingAPI(self)
        self.documents = AsyncDocumentsAPI(self)
        self.alternatives = AsyncAlternativesAPI(self)

        # Cross-module
        self.workflows = AsyncWorkflowsAPI(self)

    async def aclose(self) -> None:
        if self._owns_client:
            await self._client.aclose()

    async def __aenter__(self) -> "AsyncTepiloraClient":
        return self

    async def __aexit__(self, exc_type: Any, exc: Any, tb: Any) -> Optional[bool]:
        await self.aclose()
        return None

    async def _request(
        self,
        method: str,
        path: str,
        *,
        params: Optional[Mapping[str, Any]] = None,
        json_body: Any = None,
        headers: Optional[Mapping[str, str]] = None,
    ) -> Any:
        query = dict(params or {})
        query.update(self._config.auth_query())
        logger.debug("Request: %s %s", method, path)
        response = await self._client.request(method, path, params=query or None, json=json_body, headers=headers)
        logger.debug("Response: %d", response.status_code)
        _raise_for_error_response(response)
        if _is_json_response(response):
            return response.json()
        return response.text

    async def health(self) -> Any:
        return await self._request("GET", f"{V3_PREFIX}/health")

    async def pricing(self) -> Any:
        return await self._request("GET", f"{V3_PREFIX}/pricing")

    async def logs_status(self) -> Any:
        return await self._request("GET", f"{V3_PREFIX}/logs/status")

    def capabilities(
        self,
        namespace_or_action: Optional[str] = None,
        *,
        search: Optional[str] = None,
        format: str = "print",
    ) -> Any:
        """
        Discover SDK capabilities (sync - reads local schema only).

        Args:
            namespace_or_action: Namespace (e.g., "analytics") or action
                                (e.g., "analytics.rolling_volatility")
            search: Search query to find operations
            format: "print" (default), "text", or "dict"
        """
        return _client_capabilities(self, namespace_or_action, search=search, format=format)

    async def call(
        self,
        action: str,
        *,
        params: Optional[Dict[str, Any]] = None,
        options: Optional[Dict[str, Any]] = None,
        context: Optional[Dict[str, Any]] = None,
        response_format: Optional[str] = None,
    ) -> Union[V3Response, V3BinaryResponse]:
        request_options = dict(options or {})
        if response_format is not None and "format" not in request_options:
            request_options["format"] = response_format

        query_params: Dict[str, Any] = {}
        accept_headers: Dict[str, str] = {}
        effective_format = request_options.get("format")
        if isinstance(effective_format, str) and effective_format.strip():
            query_params["format"] = effective_format
            accept_headers["Accept"] = _format_to_accept(effective_format)

        req = V3Request(action=action, params=params or {}, options=(request_options or None), context=context)
        logger.debug("V3 call: %s", action)
        response = await self._client.request(
            "POST",
            V3_PREFIX,
            params={**query_params, **self._config.auth_query()} or None,
            json=req.to_dict(),
            headers=accept_headers or None,
        )
        logger.debug("V3 response: %d", response.status_code)
        _check_sdk_version(response.headers)
        _raise_for_error_response(response)

        if _is_json_response(response):
            payload = response.json()
            if not isinstance(payload, dict):
                raise TepiloraAPIError(message="Unexpected non-object JSON response from v3 endpoint")
            return V3Response.from_dict(payload)

        content = response.content
        ctype = _content_type(response)
        fmt = str(effective_format or "binary")
        return V3BinaryResponse(
            action=action,
            format=fmt,
            content_type=ctype,
            content=content,
            meta=_parse_binary_meta(response.headers),
            headers=dict(response.headers),
        )

    # Option 3: suggest upgrade for unknown namespaces
    def __getattr__(self, name: str) -> Any:
        if not name.startswith("_"):
            raise AttributeError(
                f"'{type(self).__name__}' has no namespace '{name}'. "
                f"If this is a new API namespace, try: pip install --upgrade tepilora"
            )
        raise AttributeError(f"'{type(self).__name__}' has no attribute '{name}'")

    async def call_data(
        self,
        action: str,
        *,
        params: Optional[Dict[str, Any]] = None,
        options: Optional[Dict[str, Any]] = None,
        context: Optional[Dict[str, Any]] = None,
        response_format: Optional[str] = None,
    ) -> Any:
        resp = await self.call(action, params=params, options=options, context=context, response_format=response_format)
        if isinstance(resp, V3BinaryResponse):
            return resp.content
        if not resp.success:
            raise TepiloraAPIError(message="V3 action returned success=false", error_data={"response": resp})
        return resp.data

    async def call_arrow_ipc_stream(
        self,
        action: str,
        *,
        params: Optional[Dict[str, Any]] = None,
        options: Optional[Dict[str, Any]] = None,
        context: Optional[Dict[str, Any]] = None,
    ) -> V3BinaryResponse:
        resp = await self.call(action, params=params, options=options, context=context, response_format="arrow")
        if not isinstance(resp, V3BinaryResponse):
            raise TepiloraAPIError(message="Expected Arrow IPC stream response, got JSON")
        return resp
