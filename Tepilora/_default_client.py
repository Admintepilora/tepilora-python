from __future__ import annotations

import threading
from typing import Optional

from .client import TepiloraClient

_lock = threading.RLock()
_default_client: Optional[TepiloraClient] = None


def get_default_client() -> TepiloraClient:
    """
    Get or create the default global client (thread-safe).

    Uses double-check locking pattern with RLock for thread safety.
    """
    global _default_client
    if _default_client is None:
        with _lock:
            if _default_client is None:  # Double-check
                _default_client = TepiloraClient()
    return _default_client


def configure_default_client(**kwargs) -> TepiloraClient:
    """
    Configure the module-level default client used by `Tepilora.analytics.*`.

    Thread-safe: uses swap-close pattern to avoid closing while other threads use it.

    Example:
        import Tepilora as T
        T.configure(api_key="...", base_url="https://api.tepiloradata.com")
        T.analytics.rolling_volatility(...)
    """
    global _default_client
    new_client = TepiloraClient(**kwargs)
    with _lock:
        old = _default_client
        _default_client = new_client
    # Close old client OUTSIDE the lock to avoid holding lock during I/O
    if old is not None:
        old.close()
    return new_client


def close_default_client() -> None:
    """
    Close and clear the default client (thread-safe).

    Uses swap-close pattern: swaps to None under lock, then closes outside lock.
    """
    global _default_client
    with _lock:
        old = _default_client
        _default_client = None
    # Close OUTSIDE the lock
    if old is not None:
        old.close()
