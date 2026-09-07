from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(slots=True)
class ControlPlaneError(Exception):
    code: str
    message: str
    status_code: int = 400
    details: dict[str, Any] | None = None

    def __str__(self) -> str:
        return self.message

    def as_dict(self) -> dict[str, Any]:
        """Return the stable domain-error envelope used by local transports."""

        value: dict[str, Any] = {"code": self.code, "message": self.message}
        if self.details:
            value["details"] = self.details
        return value


class NotFoundError(ControlPlaneError):
    def __init__(self, resource: str, identifier: str) -> None:
        super().__init__(
            code="not_found",
            message=f"{resource} not found: {identifier}",
            status_code=404,
            details={"resource": resource, "identifier": identifier},
        )


class ConflictError(ControlPlaneError):
    def __init__(self, message: str, **details: Any) -> None:
        super().__init__("conflict", message, 409, details or None)


class AuthorizationError(ControlPlaneError):
    def __init__(self, message: str = "not authorized", **details: Any) -> None:
        super().__init__("forbidden", message, 403, details or None)


class AuthenticationError(ControlPlaneError):
    def __init__(self, message: str = "invalid or missing credentials") -> None:
        super().__init__("unauthorized", message, 401)


class ValidationError(ControlPlaneError):
    def __init__(self, message: str, **details: Any) -> None:
        super().__init__("invalid_request", message, 422, details or None)


class AuthorityModeError(ControlPlaneError):
    def __init__(self, mode: str) -> None:
        super().__init__(
            "authority_not_canonical",
            "the control plane is not the active write and dispatch authority",
            503,
            {"authority_mode": mode},
        )


class StaleGoalError(ControlPlaneError):
    def __init__(self, expected: int, actual: int) -> None:
        super().__init__(
            "stale_goal_version",
            f"goal version is stale: expected {expected}, current {actual}",
            409,
            {"expected": expected, "current": actual},
        )


class StaleGenerationError(ControlPlaneError):
    def __init__(self, expected: int, actual: int) -> None:
        super().__init__(
            "stale_work_generation",
            f"work generation is stale: expected {expected}, current {actual}",
            409,
            {"expected": expected, "current": actual},
        )
