"""Model-provider conversation events and errors shared by the HTTP adapter."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any


class SessionError(RuntimeError):
    """Base error for a provider-backed chat session."""


class InvalidSessionError(SessionError):
    """Raised when Open WebUI does not provide a usable persistent chat ID."""


@dataclass(frozen=True)
class TextDelta:
    text: str


@dataclass(frozen=True)
class OutputItemAdded:
    item: dict[str, Any]


RuntimeEvent = TextDelta | OutputItemAdded
