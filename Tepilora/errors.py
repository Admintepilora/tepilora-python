from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Optional


class TepiloraError(Exception):
    pass


@dataclass(frozen=True)
class TepiloraAPIError(TepiloraError):
    message: str
    status_code: Optional[int] = None
    error_data: Optional[Dict[str, Any]] = None
    response_text: Optional[str] = None

    def __str__(self) -> str:
        prefix = f"HTTP {self.status_code}: " if self.status_code is not None else ""
        return f"{prefix}{self.message}"
