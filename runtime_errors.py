"""Shared runtime exceptions that do not import the model SDK."""


class SessionStateError(RuntimeError):
    """Raised when deterministic or conversation session state is unsafe."""


class ConversationPublicError(SessionStateError):
    """A fixed, safe conversation error suitable for API clients."""

    def __init__(self, code: str, message: str) -> None:
        super().__init__(message)
        self.code = code
