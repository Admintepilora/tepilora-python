"""Base classes for API namespace endpoints using the unified V3 endpoint."""
from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Dict, Optional, Tuple

if TYPE_CHECKING:
    from ..client import AsyncTepiloraClient, TepiloraClient


@dataclass(frozen=True)
class BaseAPI:
    """Base class for synchronous API namespace endpoints."""

    _client: "TepiloraClient"

    def _call(
        self,
        action: str,
        *,
        params: Optional[Dict[str, Any]] = None,
        options: Optional[Dict[str, Any]] = None,
        context: Optional[Dict[str, Any]] = None,
        response_format: Optional[str] = None,
    ) -> Any:
        """Call the unified V3 endpoint with the given action."""
        return self._client.call_data(
            action,
            params=params,
            options=options,
            context=context,
            response_format=response_format,
        )

    def _call_multipart(
        self,
        action: str,
        *,
        params: Optional[Dict[str, Any]] = None,
        file_fields: Tuple[str, ...],
        options: Optional[Dict[str, Any]] = None,
        context: Optional[Dict[str, Any]] = None,
        response_format: Optional[str] = None,
    ) -> Any:
        """Call a typed V3 endpoint with multipart/form-data."""
        return self._client.call_multipart_data(
            action,
            params=params,
            file_fields=file_fields,
            options=options,
            context=context,
            response_format=response_format,
        )

    def _call_binary(
        self,
        action: str,
        *,
        params: Optional[Dict[str, Any]] = None,
        options: Optional[Dict[str, Any]] = None,
        context: Optional[Dict[str, Any]] = None,
    ) -> bytes:
        """Call a typed V3 endpoint that returns raw binary bytes."""
        return self._client.call_binary_data(
            action,
            params=params,
            options=options,
            context=context,
        )


@dataclass(frozen=True)
class AsyncBaseAPI:
    """Base class for asynchronous API namespace endpoints."""

    _client: "AsyncTepiloraClient"

    async def _call(
        self,
        action: str,
        *,
        params: Optional[Dict[str, Any]] = None,
        options: Optional[Dict[str, Any]] = None,
        context: Optional[Dict[str, Any]] = None,
        response_format: Optional[str] = None,
    ) -> Any:
        """Call the unified V3 endpoint with the given action."""
        return await self._client.call_data(
            action,
            params=params,
            options=options,
            context=context,
            response_format=response_format,
        )

    async def _call_multipart(
        self,
        action: str,
        *,
        params: Optional[Dict[str, Any]] = None,
        file_fields: Tuple[str, ...],
        options: Optional[Dict[str, Any]] = None,
        context: Optional[Dict[str, Any]] = None,
        response_format: Optional[str] = None,
    ) -> Any:
        """Call a typed V3 endpoint with multipart/form-data."""
        return await self._client.call_multipart_data(
            action,
            params=params,
            file_fields=file_fields,
            options=options,
            context=context,
            response_format=response_format,
        )

    async def _call_binary(
        self,
        action: str,
        *,
        params: Optional[Dict[str, Any]] = None,
        options: Optional[Dict[str, Any]] = None,
        context: Optional[Dict[str, Any]] = None,
    ) -> bytes:
        """Call a typed V3 endpoint that returns raw binary bytes."""
        return await self._client.call_binary_data(
            action,
            params=params,
            options=options,
            context=context,
        )
