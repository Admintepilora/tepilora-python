"""Parameter validation helpers."""
from __future__ import annotations

import re
from datetime import date, datetime
from typing import Optional

_DATE_RE = re.compile(r"^\d{4}-(?:0[1-9]|1[0-2])-(?:0[1-9]|[12]\d|3[01])$")


def validate_date(value: str, param_name: str = "date") -> str:
    """Validate date string format YYYY-MM-DD."""
    if not _DATE_RE.match(value):
        raise ValueError(
            f"Invalid date format for '{param_name}': {value!r}. "
            "Expected YYYY-MM-DD (e.g., '2024-01-15')"
        )
    return value


def validate_date_range(
    start_date: Optional[str],
    end_date: Optional[str],
) -> None:
    """Validate that start_date <= end_date when both are provided."""
    if start_date is not None and end_date is not None:
        if start_date > end_date:  # String comparison works for YYYY-MM-DD
            raise ValueError(
                f"start_date ({start_date}) must be <= end_date ({end_date})"
            )


def coerce_date(value) -> Optional[str]:
    """Coerce date-like objects to YYYY-MM-DD string."""
    if value is None:
        return None
    if isinstance(value, str):
        return value
    if isinstance(value, datetime):
        return value.strftime("%Y-%m-%d")
    if isinstance(value, date):
        return value.isoformat()
    raise TypeError(f"Cannot coerce {type(value).__name__} to date string")
