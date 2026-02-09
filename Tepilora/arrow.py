from __future__ import annotations

import io
from typing import Any, Optional

from .errors import TepiloraError


class TepiloraArrowError(TepiloraError):
    pass


def read_ipc_stream(content: bytes) -> Any:
    """
    Decode Apache Arrow IPC Stream bytes.

    Note: uses `pyarrow.ipc.read_ipc_stream()` when available (not `read_ipc()` / IPC file).
    """
    try:
        import pyarrow as pa  # type: ignore
        import pyarrow.ipc as ipc  # type: ignore
    except Exception as e:  # pragma: no cover
        raise TepiloraArrowError("pyarrow is required to decode Arrow IPC streams") from e

    source = pa.py_buffer(content)
    if hasattr(ipc, "read_ipc_stream"):
        result = ipc.read_ipc_stream(source)
        if hasattr(result, "read_all"):
            return result.read_all()
        return result

    reader = ipc.open_stream(source)
    return reader.read_all()


def read_ipc_stream_polars(content: bytes) -> Any:
    try:
        import polars as pl  # type: ignore
    except Exception as e:  # pragma: no cover
        raise TepiloraArrowError("polars is required to decode Arrow IPC streams with polars") from e

    return pl.read_ipc_stream(io.BytesIO(content))
