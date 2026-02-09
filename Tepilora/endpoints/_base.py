"""Base classes for API namespace endpoints using the unified V3 endpoint."""
from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Dict, Optional

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
