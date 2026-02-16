from .client import AsyncTepiloraClient, TepiloraClient
from ._default_client import close_default_client, configure_default_client
from .errors import TepiloraAPIError, TepiloraError
from .models import V3BinaryMeta, V3BinaryResponse, V3Meta, V3Request, V3Response
from ._validators import coerce_date, validate_date, validate_date_range
from .version import __version__

from .analytics import AnalyticsAPI, AnalyticsFunction, analytics
from .capabilities import (
    capabilities,
    list_namespaces,
    list_operations,
    get_operation_info,
)


def configure(**kwargs) -> TepiloraClient:
    """
    Configure the module-level default client used by `Tepilora.analytics.*`.

    Args are passed to `TepiloraClient(...)` (e.g. api_key, base_url, timeout).
    """
    return configure_default_client(**kwargs)


__all__ = [
    "AsyncTepiloraClient",
    "AnalyticsAPI",
    "AnalyticsFunction",
    "TepiloraAPIError",
    "TepiloraClient",
    "TepiloraError",
    "V3BinaryMeta",
    "V3BinaryResponse",
    "V3Meta",
    "V3Request",
    "V3Response",
    "__version__",
    "analytics",
    "capabilities",
    "coerce_date",
    "configure",
    "close_default_client",
    "get_operation_info",
    "list_namespaces",
    "list_operations",
    "validate_date",
    "validate_date_range",
]
