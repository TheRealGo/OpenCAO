from __future__ import annotations

import json
from collections.abc import AsyncIterator, Awaitable, Callable, Mapping
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, TypeVar

from fastapi import Depends, FastAPI, HTTPException, Request, Response
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse, StreamingResponse
from pydantic import ValidationError as PydanticValidationError
from starlette.middleware.trustedhost import TrustedHostMiddleware

from .a2a import (
    A2A_MEDIA_TYPE,
    A2A_VERSION,
    CONTENT_TYPE_NOT_SUPPORTED,
    PUSH_NOT_SUPPORTED,
    STREAMING_METHODS,
    TERMINAL_A2A_STATES,
    UNSUPPORTED_OPERATION,
    VERSION_NOT_SUPPORTED,
    A2AProtocolError,
    A2AServer,
)
from .attachment_issuer import AttachmentCapabilityIssuer, AttachmentPeerIdentityProvider
from .config import Settings
from .dashboard import DashboardCursor, DashboardReadModel, DashboardResyncRequired
from .dashboard_history import HISTORY_REFERENCE
from .database import Database
from .errors import (
    AuthenticationError,
    AuthorizationError,
    ConflictError,
    ControlPlaneError,
    NotFoundError,
    ValidationError,
)
from .mcp import (
    MCP_LATEST_VERSION,
    MCPServer,
    modern_http_status,
    modern_progress_token,
)
from .models import (
    AckInput,
    BoundaryDispositionInput,
    CAOSessionAttachment,
    DeliveryResolveInput,
    EffectCheckInput,
    EffectGrantInput,
    EffectResolveInput,
    GoalRevision,
    MemoryReadInput,
    MemorySearchInput,
    MemoryWriteInput,
    PrincipalCreate,
    QueryInput,
    ReasonerTurnAcquireInput,
    ReportInput,
    ReviewInput,
    RuntimeHeartbeat,
    RuntimeRegistration,
    WorkAssignment,
    WorkHistoryReadInput,
    WorkResumeInput,
    WorkState,
)
from .projection import build_projection
from .release_identity import current_release_identity
from .runtime import Dispatcher
from .security import (
    origin_allowed,
    redact_control_plane_secrets,
    safe_validation_details,
)
from .service import ControlPlane

T = TypeVar("T")


_DISPATCHER_READINESS_ERROR_CODES = frozenset(
    {
        "message_missing",
        "runtime_unavailable",
        "managed_mcp_launch_preparation_failed",
        "managed_mcp_launch_ticket_pending",
        "desktop_wake_pre_start_unavailable",
        "cao_provider_turn_evidence_unavailable",
        "assignment_dependency_unavailable",
        "mcp_startup_timeout",
        "mcp_startup_failed",
        "worker_inactive_timeout",
        "runtime_timeout",
        "runtime_protocol_invalid_json",
        "runtime_process_exited",
        "runtime_dispatch_failed",
        "runtime_provider_rate_limited",
        "runtime_turn_failed",
    }
)


def _dispatcher_readiness(dispatcher: Dispatcher, settings: Settings) -> dict[str, Any]:
    """Project bounded dispatcher health for the unauthenticated ready edge."""

    try:
        raw = dispatcher.status()
    except Exception:  # pragma: no cover - defensive edge around diagnostics
        return {
            "healthy": False,
            "running": False,
            "last_cycle_at": None,
            "last_error": "dispatcher_status_unavailable",
            "cycle_age_seconds": None,
            "stale_after_seconds": max(
                60.0,
                float(settings.dispatcher_recovery_scan_seconds) * 2.0 + 5.0,
            ),
            "issues": ["dispatcher_status_unavailable"],
            "queued_deliveries": 0,
            "leased_deliveries": 0,
            "unknown_delivery_outcomes": 0,
            "pending_push_deliveries": 0,
            "active_deliveries": 0,
        }
    running = bool(raw.get("running"))
    error_code = str(raw.get("last_error") or "")
    if error_code and error_code not in _DISPATCHER_READINESS_ERROR_CODES:
        error_code = "dispatcher_error"
    last_cycle_at = str(raw.get("last_cycle_at") or "")
    stale_after_seconds = max(60.0, float(settings.dispatcher_recovery_scan_seconds) * 2.0 + 5.0)
    cycle_age_seconds: float | None = None
    cycle_stale = True
    if last_cycle_at:
        try:
            observed = datetime.fromisoformat(last_cycle_at.replace("Z", "+00:00"))
            if observed.tzinfo is None:
                observed = observed.replace(tzinfo=UTC)
            raw_age = (datetime.now(UTC) - observed.astimezone(UTC)).total_seconds()
            cycle_age_seconds = max(0.0, raw_age)
            cycle_stale = raw_age < -5.0 or raw_age > stale_after_seconds
        except ValueError:
            cycle_stale = True
    issues: list[str] = []
    if not running:
        issues.append("dispatcher_not_running")
    if error_code:
        issues.append("dispatcher_cycle_error")
    if cycle_stale:
        issues.append("dispatcher_cycle_stale")
    return {
        "healthy": not issues,
        "running": running,
        "last_cycle_at": last_cycle_at or None,
        "last_error": error_code or None,
        "cycle_age_seconds": (
            round(cycle_age_seconds, 3) if cycle_age_seconds is not None else None
        ),
        "stale_after_seconds": stale_after_seconds,
        "issues": issues,
        "queued_deliveries": int(raw.get("queued_deliveries") or 0),
        "leased_deliveries": int(raw.get("leased_deliveries") or 0),
        "unknown_delivery_outcomes": int(raw.get("unknown_delivery_outcomes") or 0),
        "pending_push_deliveries": int(raw.get("pending_push_deliveries") or 0),
        "active_deliveries": int(raw.get("active_deliveries") or 0),
    }


@dataclass(slots=True)
class A2AHTTPError(Exception):
    status_code: int
    title: str
    detail: str
    error_type: str
    extras: dict[str, Any]


def _jsonable_error(error: ControlPlaneError) -> dict[str, Any]:
    return {
        "error": {
            "code": error.code,
            "message": redact_control_plane_secrets(error.message),
            "details": redact_control_plane_secrets(error.details or {}),
        }
    }


def _safe_request_validation_details(
    error: RequestValidationError | PydanticValidationError,
) -> list[dict[str, Any]]:
    """Return useful validation structure without reflecting submitted values.

    FastAPI's default request-validation response includes Pydantic's ``input``
    field.  Besides leaking a malformed scalar, that can recursively reflect a
    whole malformed object.  Only the stable schema-facing fields are safe to
    expose for ordinary endpoints.
    """

    return safe_validation_details(
        error.errors(),
        allowed_location_roots=frozenset({"body", "query", "path", "header", "cookie"}),
    )


def _bearer_token(request: Request) -> str:
    value = request.headers.get("authorization", "")
    scheme, _, token = value.partition(" ")
    if scheme.lower() != "bearer" or not token:
        raise AuthenticationError()
    return token.strip()


def _content_type(request: Request) -> str:
    return request.headers.get("content-type", "").split(";", 1)[0].strip().lower()


def _accepts(request: Request, media_type: str) -> bool:
    expected = media_type.lower()
    for raw_item in request.headers.get("accept", "*/*").split(","):
        parts = [part.strip().lower() for part in raw_item.split(";")]
        candidate = parts[0]
        quality = 1.0
        for parameter in parts[1:]:
            if parameter.startswith("q="):
                try:
                    quality = float(parameter[2:])
                except ValueError:
                    quality = 0.0
        if quality > 0 and candidate in {"*/*", expected}:
            return True
    return False


def _duplicated_mcp_routing_header(request: Request) -> str | None:
    """Detect ambiguity before Starlette folds repeated routing headers."""

    routing_headers = {b"mcp-protocol-version", b"mcp-method", b"mcp-name"}
    seen: set[bytes] = set()
    for raw_name, _ in request.scope.get("headers", []):
        if not isinstance(raw_name, bytes):
            continue
        name = raw_name.lower()
        if name not in routing_headers:
            continue
        if name in seen:
            return name.decode("ascii")
        seen.add(name)
    return None


def _sse_data(
    payload: Mapping[str, Any],
    *,
    event_id: str | None = None,
    event_type: str | None = None,
) -> bytes:
    lines: list[str] = []
    if event_id:
        lines.append(f"id: {event_id}")
    if event_type:
        lines.append(f"event: {event_type}")
    text = json.dumps(payload, ensure_ascii=False, separators=(",", ":"))
    lines.extend(f"data: {line}" for line in (text.splitlines() or [""]))
    return ("\n".join(lines) + "\n\n").encode("utf-8")


async def _mcp_sse_stream(
    messages: AsyncIterator[Mapping[str, Any] | None],
) -> AsyncIterator[bytes]:
    async for message in messages:
        if message is None:
            yield b": keepalive\n\n"
        else:
            yield _sse_data(message, event_type="message")


def _problem(
    status_code: int,
    title: str,
    detail: str,
    error_type: str,
    **extras: Any,
) -> JSONResponse:
    detail = str(redact_control_plane_secrets(detail))
    extras = redact_control_plane_secrets(extras)
    reason = error_type.replace("-", "_").upper()
    status_codes = {
        "TASK_NOT_FOUND": 5,
        "PUSH_NOTIFICATION_NOT_SUPPORTED": 12,
        "UNSUPPORTED_OPERATION": 9,
        "VERSION_NOT_SUPPORTED": 9,
        "CONTENT_TYPE_NOT_SUPPORTED": 3,
        "INVALID_REQUEST": 3,
        "NOT_ACCEPTABLE": 3,
    }
    metadata = {
        str(key): (value if isinstance(value, str) else json.dumps(value, separators=(",", ":")))
        for key, value in extras.items()
    }
    return JSONResponse(
        {
            "code": status_codes.get(reason, 13),
            "message": detail,
            "details": [
                {
                    "@type": "type.googleapis.com/google.rpc.ErrorInfo",
                    "reason": reason,
                    "domain": "a2a-protocol.org",
                    "metadata": metadata,
                }
            ],
        },
        status_code=status_code,
        media_type=A2A_MEDIA_TYPE,
        headers={"A2A-Version": A2A_VERSION},
    )


def create_app(
    settings: Settings | None = None,
    *,
    attachment_peer_identity_provider: AttachmentPeerIdentityProvider | None = None,
) -> FastAPI:
    resolved = settings or Settings.load()
    database = Database(resolved)
    service = ControlPlane(database, resolved)
    bootstrap = service.bootstrap()
    release_identity = current_release_identity()
    catalog_reconciliation = service.reconcile_conversation_tool_catalog(
        release_identity.mcp_catalog_digest,
        release_identity.release_id,
    )
    mcp = MCPServer(service)
    a2a = A2AServer(service)
    dispatcher = Dispatcher(service, resolved)
    attachment_issuer = AttachmentCapabilityIssuer(
        resolved.state_dir,
        service.issue_owner_local_attachment_bootstrap,
        peer_identity_provider=attachment_peer_identity_provider,
    )
    dashboard_read_model = DashboardReadModel(service)

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        del app
        await attachment_issuer.start()
        await dispatcher.start()
        try:
            yield
        finally:
            await dispatcher.stop()
            await attachment_issuer.close()

    app = FastAPI(
        title=resolved.server_name,
        version=resolved.server_version,
        description="Independent local durable MCP and A2A control plane for agent supervision.",
        lifespan=lifespan,
        docs_url="/docs" if resolved.enable_api_docs else None,
        redoc_url="/redoc" if resolved.enable_api_docs else None,
        openapi_url="/openapi.json" if resolved.enable_api_docs else None,
    )
    app.add_middleware(TrustedHostMiddleware, allowed_hosts=list(resolved.trusted_hosts))
    app.state.settings = resolved
    app.state.database = database
    app.state.service = service
    app.state.mcp = mcp
    app.state.a2a = a2a
    app.state.dispatcher = dispatcher
    app.state.attachment_issuer = attachment_issuer
    app.state.dashboard_read_model = dashboard_read_model
    app.state.bootstrap = bootstrap
    app.state.release_identity = release_identity
    app.state.catalog_reconciliation = catalog_reconciliation

    @app.middleware("http")
    async def hardening(
        request: Request, call_next: Callable[[Request], Awaitable[Response]]
    ) -> Response:
        if request.method in {"POST", "PUT", "PATCH", "DELETE"}:
            origin = request.headers.get("origin", "")
            if origin and not origin_allowed(origin, resolved.allowed_origins):
                return JSONResponse({"detail": "Origin is not allowed"}, status_code=403)
        raw_length = request.headers.get("content-length")
        if raw_length:
            try:
                if int(raw_length) > resolved.max_request_bytes:
                    return JSONResponse({"detail": "Request body is too large"}, status_code=413)
            except ValueError:
                return JSONResponse({"detail": "Invalid Content-Length"}, status_code=400)
        if request.method in {"POST", "PUT", "PATCH"}:
            body = await request.body()
            if len(body) > resolved.max_request_bytes:
                return JSONResponse({"detail": "Request body is too large"}, status_code=413)
            # Starlette's BaseHTTPMiddleware request caches ``body`` and
            # replays it to the downstream app. Replacing ``_receive`` here
            # duplicates the request frame and breaks long-lived SSE responses.
        response = await call_next(request)
        response.headers.setdefault("X-Content-Type-Options", "nosniff")
        response.headers.setdefault("Referrer-Policy", "no-referrer")
        response.headers.setdefault("X-Frame-Options", "DENY")
        response.headers.setdefault(
            "Permissions-Policy", "camera=(), microphone=(), geolocation=()"
        )
        if request.url.path != "/.well-known/agent-card.json":
            response.headers.setdefault("Cache-Control", "no-store")
        return response

    @app.exception_handler(ControlPlaneError)
    async def control_plane_error_handler(_: Request, error: ControlPlaneError) -> JSONResponse:
        headers = {"WWW-Authenticate": "Bearer"} if error.status_code == 401 else None
        return JSONResponse(_jsonable_error(error), status_code=error.status_code, headers=headers)

    @app.exception_handler(PydanticValidationError)
    async def validation_error_handler(_: Request, error: PydanticValidationError) -> JSONResponse:
        return JSONResponse(
            {
                "error": {
                    "code": "invalid_request",
                    "message": "request validation failed",
                    "details": _safe_request_validation_details(error),
                }
            },
            status_code=422,
        )

    @app.exception_handler(RequestValidationError)
    async def request_validation_error_handler(
        _request: Request, error: RequestValidationError
    ) -> JSONResponse:
        payload: dict[str, Any] = {
            "error": {
                "code": "invalid_request",
                "message": "request validation failed",
            }
        }
        payload["error"]["details"] = _safe_request_validation_details(error)
        return JSONResponse(payload, status_code=422)

    @app.exception_handler(A2AHTTPError)
    async def a2a_http_error_handler(_: Request, error: A2AHTTPError) -> JSONResponse:
        return _problem(
            error.status_code,
            error.title,
            error.detail,
            error.error_type,
            **error.extras,
        )

    async def actor(request: Request) -> dict[str, Any]:
        current = service.authenticate(_bearer_token(request))
        if current.get("_cao_attachment_bootstrap_credential_id") and not (
            request.method == "POST" and request.url.path == "/api/v1/cao-session-attachments"
        ):
            raise HTTPException(
                status_code=403,
                detail="attachment bootstrap capabilities are restricted to attachment create/renew",
            )
        if (
            current.get("_cao_conversation_credential_id")
            or current.get("_cao_runtime_credential_id")
        ) and request.url.path != "/mcp":
            raise HTTPException(
                status_code=403,
                detail="CAO conversation capabilities are restricted to MCP",
            )
        return current

    async def a2a_actor(request: Request) -> dict[str, Any]:
        _require_a2a_version(request)
        current = service.authenticate(_bearer_token(request))
        if (
            current.get("_cao_attachment_bootstrap_credential_id")
            or current.get("_cao_conversation_credential_id")
            or current.get("_cao_runtime_credential_id")
        ):
            raise HTTPException(
                status_code=403,
                detail="CAO conversation capabilities are restricted to their MCP surface",
            )
        return current

    def require_role(value: dict[str, Any], *roles: str) -> None:
        if value["role"] not in set(roles):
            raise HTTPException(status_code=403, detail="principal role is not permitted")

    def require_non_dashboard(value: dict[str, Any]) -> None:
        if value["role"] == "dashboard":
            raise HTTPException(
                status_code=403, detail="dashboard principal is REST read-model only"
            )

    def _require_a2a_version(request: Request) -> None:
        requested_value = (
            request.headers.get("a2a-version")
            or request.query_params.get("A2A-Version")
            or request.query_params.get("a2a-version")
        )
        requested = requested_value.strip() if requested_value else ""
        if not requested:
            requested = "0.3"
        if requested != A2A_VERSION:
            raise A2AHTTPError(
                400,
                "Protocol Version Not Supported",
                f"The requested A2A protocol version {requested} is not supported by this agent",
                "version-not-supported",
                {"requestedVersion": requested, "supportedVersions": [A2A_VERSION]},
            )

    def _require_media(request: Request, *accepted: str) -> None:
        media = _content_type(request)
        if media not in {value.lower() for value in accepted}:
            raise A2AHTTPError(
                415,
                "Unsupported Media Type",
                f"Content-Type must be one of: {', '.join(accepted)}",
                "content-type-not-supported",
                {},
            )

    def _a2a_call(call: Callable[[], T]) -> T:
        try:
            return call()
        except A2AProtocolError as error:
            error_type = (
                "push-notification-not-supported"
                if error.code == PUSH_NOT_SUPPORTED
                else error.reason.lower().replace("_", "-")
            )
            raise A2AHTTPError(
                400,
                "Operation Not Supported",
                error.message,
                error_type,
                error.metadata,
            ) from error
        except AuthorizationError:
            raise A2AHTTPError(
                404, "Task Not Found", "Task not found", "task-not-found", {}
            ) from None
        except NotFoundError as error:
            raise A2AHTTPError(
                404, "Task Not Found", error.message, "task-not-found", {}
            ) from error
        except ConflictError as error:
            raise A2AHTTPError(
                400,
                "Operation Not Supported",
                error.message,
                "unsupported-operation",
                error.details or {},
            ) from error
        except ValidationError as error:
            raise A2AHTTPError(
                400,
                "Invalid Request",
                error.message,
                "invalid-request",
                error.details or {},
            ) from error

    def _a2a_jsonrpc_pre_dispatch_error(request_id: Any, error: A2AHTTPError) -> JSONResponse:
        codes = {
            "version-not-supported": VERSION_NOT_SUPPORTED,
            "content-type-not-supported": CONTENT_TYPE_NOT_SUPPORTED,
            "push-notification-not-supported": PUSH_NOT_SUPPORTED,
            "unsupported-operation": UNSUPPORTED_OPERATION,
            "not-acceptable": UNSUPPORTED_OPERATION,
        }
        reason = error.error_type.replace("-", "_").upper()
        return JSONResponse(
            A2AServer._error(
                request_id,
                codes.get(error.error_type, -32600),
                error.detail,
                A2AServer._error_info(reason, error.extras),
            ),
            status_code=error.status_code,
            media_type=A2A_MEDIA_TYPE,
            headers={"A2A-Version": A2A_VERSION},
        )

    @app.get("/health", tags=["operations"])
    async def health() -> dict[str, Any]:
        return {
            "status": "ok",
            "service": "cao-a2a-control-plane",
            "version": resolved.server_version,
            "release_id": release_identity.release_id,
            "schema_version": release_identity.schema_version,
            "mcp_catalog_digest": release_identity.mcp_catalog_digest,
            "mcpProtocol": MCP_LATEST_VERSION,
            "a2aProtocol": A2A_VERSION if resolved.enable_a2a else None,
        }

    @app.get("/ready", tags=["operations"])
    def ready() -> JSONResponse:
        # SQLite integrity/projection work belongs in FastAPI's worker pool;
        # health probes must not hold up independent Dashboard reads.
        integrity = database.integrity_check()
        projection = build_projection(database)
        dispatcher_readiness = _dispatcher_readiness(dispatcher, resolved)
        body = {
            "ready": bool(
                integrity["ok"]
                and projection.healthy
                and database.is_canonical_authority()
                and dispatcher_readiness["healthy"]
            ),
            "release_id": release_identity.release_id,
            "schema_version": release_identity.schema_version,
            "mcp_catalog_digest": release_identity.mcp_catalog_digest,
            "database": integrity,
            "authority": dict(projection.snapshot["authority"]),
            "projection": {
                "healthy": projection.healthy,
                "watermark": dict(projection.watermark),
                "canonical_digest": projection.canonical_digest,
                "violations": [item.as_dict() for item in projection.violations],
            },
            "dispatcher": dispatcher_readiness,
        }
        return JSONResponse(body, status_code=200 if body["ready"] else 503)

    if resolved.enable_a2a:

        @app.get("/.well-known/agent-card.json", tags=["a2a"])
        async def public_agent_card() -> JSONResponse:
            return JSONResponse(
                a2a.agent_card(authenticated=False),
                media_type=A2A_MEDIA_TYPE,
                headers={"Cache-Control": "public, max-age=300"},
            )

    # ------------------------------------------------------------------
    # MCP: one current stateless protocol on one URL.
    # ------------------------------------------------------------------
    @app.post("/mcp", tags=["mcp"])
    async def mcp_post(request: Request, current: dict[str, Any] = Depends(actor)) -> Response:
        if _content_type(request) != "application/json":
            return JSONResponse(
                mcp._error(None, -32600, "Content-Type must be application/json", modern=True),
                status_code=415,
            )
        if not (_accepts(request, "application/json") and _accepts(request, "text/event-stream")):
            raise HTTPException(
                status_code=406,
                detail="MCP requires Accept: application/json, text/event-stream",
            )
        try:
            body = await request.json()
        except Exception as error:
            return JSONResponse(
                mcp._error(None, -32700, "Parse error", str(error), modern=True),
                status_code=400,
            )
        if isinstance(body, list):
            return JSONResponse(
                mcp._error(None, -32600, "JSON-RPC batches are not supported", modern=True),
                status_code=400,
            )
        if not isinstance(body, Mapping):
            return JSONResponse(
                mcp._error(None, -32600, "Invalid Request", modern=True),
                status_code=400,
            )

        duplicated_header = _duplicated_mcp_routing_header(request)
        if duplicated_header is not None:
            return JSONResponse(
                mcp._error(
                    body.get("id"),
                    -32020,
                    f"{duplicated_header} header appears more than once",
                    modern=True,
                ),
                status_code=400,
                headers={"MCP-Protocol-Version": MCP_LATEST_VERSION},
            )

        method = str(body.get("method", ""))
        validation = mcp.validate_modern_request(
            body,
            protocol_header=request.headers.get("mcp-protocol-version"),
            method_header=request.headers.get("mcp-method"),
            name_header=request.headers.get("mcp-name"),
        )
        headers = {
            "MCP-Protocol-Version": MCP_LATEST_VERSION,
            "Vary": "Authorization, MCP-Protocol-Version, MCP-Method, MCP-Name",
        }
        if validation is not None:
            return JSONResponse(validation, status_code=400, headers=headers)
        if method == "subscriptions/listen":
            subscription_validation = mcp.validate_subscription_request(current, body)
            if subscription_validation is not None:
                return JSONResponse(
                    subscription_validation,
                    status_code=modern_http_status(subscription_validation),
                    headers=headers,
                )
            return StreamingResponse(
                _mcp_sse_stream(mcp.subscription_messages(current, body)),
                media_type="text/event-stream",
                headers={
                    **headers,
                    "Cache-Control": "private, no-store, no-transform",
                    "X-Accel-Buffering": "no",
                },
            )
        if method == "tools/call" and "id" in body and modern_progress_token(body) is not None:
            progress_validation = mcp.validate_progress_tool_call(current, body)
            if progress_validation is not None:
                return JSONResponse(
                    progress_validation,
                    status_code=modern_http_status(progress_validation),
                    headers=headers,
                )
            progress_claim, progress_claim_error = mcp.claim_progress_token(current, body)
            if progress_claim_error is not None:
                return JSONResponse(
                    progress_claim_error,
                    status_code=modern_http_status(progress_claim_error),
                    headers=headers,
                )
            return StreamingResponse(
                _mcp_sse_stream(
                    mcp.progress_messages(current, body, progress_claim=progress_claim)
                ),
                media_type="text/event-stream",
                headers={
                    **headers,
                    "Cache-Control": "private, no-store, no-transform",
                    "X-Accel-Buffering": "no",
                },
            )
        result = mcp.handle_modern(current, body)
        if result is None:
            return Response(status_code=202, headers=headers)
        if method in {"server/discover", "tools/list", "resources/list"}:
            ttl = (
                resolved.mcp_discovery_cache_ttl_ms
                if method == "server/discover"
                else resolved.mcp_private_cache_ttl_ms
            )
            headers["Cache-Control"] = f"private, max-age={max(ttl // 1000, 0)}"
        else:
            headers["Cache-Control"] = "no-store"
        return JSONResponse(
            result,
            status_code=modern_http_status(result),
            headers=headers,
        )

    # ------------------------------------------------------------------
    # A2A 1.0 JSON-RPC and HTTP+JSON bindings.
    # ------------------------------------------------------------------
    if resolved.enable_a2a:

        @app.post("/a2a", tags=["a2a"])
        async def a2a_jsonrpc(
            request: Request,
            current: dict[str, Any] = Depends(actor),
        ) -> Response:
            try:
                body = await request.json()
            except Exception:
                return JSONResponse(
                    A2AServer._error(None, -32700, "Parse error"),
                    status_code=400,
                    media_type=A2A_MEDIA_TYPE,
                    headers={"A2A-Version": A2A_VERSION},
                )
            if not isinstance(body, Mapping):
                return JSONResponse(
                    A2AServer._error(None, -32600, "Request payload validation error"),
                    status_code=400,
                    media_type=A2A_MEDIA_TYPE,
                    headers={"A2A-Version": A2A_VERSION},
                )
            try:
                _require_a2a_version(request)
                _require_media(request, "application/json", A2A_MEDIA_TYPE)
            except A2AHTTPError as error:
                return _a2a_jsonrpc_pre_dispatch_error(body.get("id"), error)
            method = str(body.get("method", ""))
            params = body.get("params", {})
            if method in STREAMING_METHODS:
                if not _accepts(request, "text/event-stream"):
                    return _a2a_jsonrpc_pre_dispatch_error(
                        body.get("id"),
                        A2AHTTPError(
                            406,
                            "Not Acceptable",
                            "Streaming requires Accept: text/event-stream",
                            "not-acceptable",
                            {},
                        ),
                    )
                if not isinstance(params, Mapping):
                    return JSONResponse(
                        A2AServer._error(body.get("id"), -32602, "Invalid parameters"),
                        status_code=400,
                        media_type=A2A_MEDIA_TYPE,
                        headers={"A2A-Version": A2A_VERSION},
                    )
                if method == "SendStreamingMessage":
                    try:
                        task = a2a.send_message(current, dict(params))
                    except A2AProtocolError as error:
                        return JSONResponse(
                            A2AServer._error(
                                body.get("id"),
                                error.code,
                                error.message,
                                A2AServer._error_info(error.reason, error.metadata),
                            ),
                            status_code=400,
                            media_type=A2A_MEDIA_TYPE,
                            headers={"A2A-Version": A2A_VERSION},
                        )
                    except ControlPlaneError as error:
                        return JSONResponse(
                            A2AServer._error(body.get("id"), -32602, error.message),
                            status_code=400,
                            media_type=A2A_MEDIA_TYPE,
                            headers={"A2A-Version": A2A_VERSION},
                        )
                    reject_terminal = False
                else:
                    task_id = str(params.get("id") or "")
                    task = _a2a_call(lambda: a2a.get_task(task_id, actor=current))
                    if task["status"]["state"] in TERMINAL_A2A_STATES:
                        return JSONResponse(
                            A2AServer._error(
                                body.get("id"),
                                UNSUPPORTED_OPERATION,
                                "Terminal tasks cannot be subscribed",
                            ),
                            status_code=400,
                        )
                    reject_terminal = True
                request_id = body.get("id")

                async def stream() -> AsyncIterator[bytes]:
                    async for item in a2a.task_event_stream(
                        str(task["id"]), actor=current, reject_terminal=reject_terminal
                    ):
                        if "heartbeat" in item:
                            yield b": heartbeat\n\n"
                            continue
                        yield _sse_data({"jsonrpc": "2.0", "id": request_id, "result": item})

                return StreamingResponse(
                    stream(),
                    media_type="text/event-stream",
                    headers={
                        "A2A-Version": A2A_VERSION,
                        "Cache-Control": "no-store",
                        "X-Accel-Buffering": "no",
                    },
                )
            return JSONResponse(
                await a2a.handle_jsonrpc_async(current, body),
                media_type="application/json",
                headers={"A2A-Version": A2A_VERSION},
            )

        @app.get("/a2a/http/extendedAgentCard", tags=["a2a"])
        async def a2a_extended_card(
            current: dict[str, Any] = Depends(a2a_actor),
        ) -> JSONResponse:
            del current
            return JSONResponse(
                a2a.agent_card(authenticated=True),
                media_type=A2A_MEDIA_TYPE,
                headers={"A2A-Version": A2A_VERSION, "Cache-Control": "private, max-age=60"},
            )

        @app.post("/a2a/http/message:send", tags=["a2a"])
        async def a2a_send(
            request: Request,
            current: dict[str, Any] = Depends(a2a_actor),
        ) -> JSONResponse:
            _require_media(request, A2A_MEDIA_TYPE)
            body = await request.json()
            task = _a2a_call(lambda: a2a.send_message(current, body))
            if not a2a.return_immediately(body):
                task = await a2a.wait_for_settled(str(task["id"]), actor=current)
            return JSONResponse(
                {"task": task}, media_type=A2A_MEDIA_TYPE, headers={"A2A-Version": A2A_VERSION}
            )

        @app.post("/a2a/http/message:stream", tags=["a2a"])
        async def a2a_stream(
            request: Request,
            current: dict[str, Any] = Depends(a2a_actor),
        ) -> StreamingResponse:
            _require_media(request, A2A_MEDIA_TYPE)
            if not _accepts(request, "text/event-stream"):
                raise A2AHTTPError(
                    406,
                    "Not Acceptable",
                    "Streaming requires Accept: text/event-stream",
                    "not-acceptable",
                    {},
                )
            body = await request.json()
            task = _a2a_call(lambda: a2a.send_message(current, body))

            async def stream() -> AsyncIterator[bytes]:
                async for item in a2a.task_event_stream(str(task["id"]), actor=current):
                    if "heartbeat" in item:
                        yield b": heartbeat\n\n"
                    else:
                        yield _sse_data(item)

            return StreamingResponse(
                stream(),
                media_type="text/event-stream",
                headers={
                    "A2A-Version": A2A_VERSION,
                    "Cache-Control": "no-store",
                    "X-Accel-Buffering": "no",
                },
            )

        @app.get("/a2a/http/tasks", tags=["a2a"])
        async def a2a_tasks(
            contextId: str | None = None,
            status: str | None = None,
            pageSize: int = 50,
            pageToken: str | None = None,
            includeArtifacts: bool = False,
            current: dict[str, Any] = Depends(a2a_actor),
        ) -> JSONResponse:
            result = _a2a_call(
                lambda: a2a.list_tasks(
                    current,
                    {
                        "contextId": contextId,
                        "status": status,
                        "pageSize": pageSize,
                        "pageToken": pageToken,
                        "includeArtifacts": includeArtifacts,
                    },
                )
            )
            return JSONResponse(
                result, media_type=A2A_MEDIA_TYPE, headers={"A2A-Version": A2A_VERSION}
            )

        @app.get("/a2a/http/tasks/{task_id}", tags=["a2a"])
        async def a2a_task_get(
            task_id: str,
            historyLength: int | None = None,
            current: dict[str, Any] = Depends(a2a_actor),
        ) -> JSONResponse:
            result = _a2a_call(
                lambda: a2a.get_task(task_id, history_length=historyLength, actor=current)
            )
            return JSONResponse(
                result, media_type=A2A_MEDIA_TYPE, headers={"A2A-Version": A2A_VERSION}
            )

        @app.post("/a2a/http/tasks/{task_id}:cancel", tags=["a2a"])
        async def a2a_task_cancel(
            task_id: str,
            request: Request,
            current: dict[str, Any] = Depends(a2a_actor),
        ) -> JSONResponse:
            _require_media(request, A2A_MEDIA_TYPE)
            body = await request.json()
            result = _a2a_call(
                lambda: a2a.cancel_task(current, task_id, str(body.get("reason", "Canceled")))
            )
            return JSONResponse(
                result, media_type=A2A_MEDIA_TYPE, headers={"A2A-Version": A2A_VERSION}
            )

        @app.post("/a2a/http/tasks/{task_id}:subscribe", tags=["a2a"])
        async def a2a_task_subscribe(
            task_id: str,
            request: Request,
            current: dict[str, Any] = Depends(a2a_actor),
        ) -> StreamingResponse:
            if not _accepts(request, "text/event-stream"):
                raise A2AHTTPError(
                    406,
                    "Not Acceptable",
                    "Streaming requires Accept: text/event-stream",
                    "not-acceptable",
                    {},
                )
            task = _a2a_call(lambda: a2a.get_task(task_id, actor=current))
            if task["status"]["state"] in TERMINAL_A2A_STATES:
                raise A2AHTTPError(
                    400,
                    "Operation Not Supported",
                    "Terminal tasks cannot be subscribed",
                    "unsupported-operation",
                    {},
                )

            async def stream() -> AsyncIterator[bytes]:
                async for item in a2a.task_event_stream(
                    task_id, actor=current, reject_terminal=True
                ):
                    if "heartbeat" in item:
                        yield b": heartbeat\n\n"
                    else:
                        yield _sse_data(item)

            return StreamingResponse(
                stream(),
                media_type="text/event-stream",
                headers={
                    "A2A-Version": A2A_VERSION,
                    "Cache-Control": "no-store",
                    "X-Accel-Buffering": "no",
                },
            )

        @app.post("/a2a/http/tasks/{task_id}/pushNotificationConfigs", tags=["a2a"])
        async def a2a_push_set(
            task_id: str,
            request: Request,
            current: dict[str, Any] = Depends(a2a_actor),
        ) -> JSONResponse:
            _require_media(request, A2A_MEDIA_TYPE)
            body = await request.json()
            result = _a2a_call(lambda: a2a.set_push_config(current, {"taskId": task_id, **body}))
            return JSONResponse(
                result, media_type=A2A_MEDIA_TYPE, headers={"A2A-Version": A2A_VERSION}
            )

        @app.get("/a2a/http/tasks/{task_id}/pushNotificationConfigs", tags=["a2a"])
        async def a2a_push_list(
            task_id: str,
            current: dict[str, Any] = Depends(a2a_actor),
        ) -> JSONResponse:
            result = _a2a_call(lambda: a2a.list_push_configs(current, {"taskId": task_id}))
            return JSONResponse(
                result, media_type=A2A_MEDIA_TYPE, headers={"A2A-Version": A2A_VERSION}
            )

        @app.get("/a2a/http/tasks/{task_id}/pushNotificationConfigs/{config_id}", tags=["a2a"])
        async def a2a_push_get(
            task_id: str,
            config_id: str,
            current: dict[str, Any] = Depends(a2a_actor),
        ) -> JSONResponse:
            result = _a2a_call(
                lambda: a2a.get_push_config(current, {"taskId": task_id, "configId": config_id})
            )
            return JSONResponse(
                result, media_type=A2A_MEDIA_TYPE, headers={"A2A-Version": A2A_VERSION}
            )

        @app.delete("/a2a/http/tasks/{task_id}/pushNotificationConfigs/{config_id}", tags=["a2a"])
        async def a2a_push_delete(
            task_id: str,
            config_id: str,
            current: dict[str, Any] = Depends(a2a_actor),
        ) -> Response:
            _a2a_call(
                lambda: a2a.delete_push_config(current, {"taskId": task_id, "configId": config_id})
            )
            return Response(status_code=204, headers={"A2A-Version": A2A_VERSION})

    # ------------------------------------------------------------------
    # Administrative REST API
    # ------------------------------------------------------------------
    @app.get("/api/v1/me", tags=["admin"])
    async def me(current: dict[str, Any] = Depends(actor)) -> dict[str, Any]:
        # Runtime credentials add private authentication/enrollment fences to
        # the actor mapping.  They are useful only inside service methods and
        # must never become a public identity contract.
        return {key: value for key, value in current.items() if not key.startswith("_")}

    @app.get("/api/v1/principals", tags=["admin"])
    async def principals(current: dict[str, Any] = Depends(actor)) -> list[dict[str, Any]]:
        require_role(current, "cao")
        return service.list_principals()

    @app.post("/api/v1/principals", tags=["admin"])
    async def principal_create(
        body: PrincipalCreate,
        current: dict[str, Any] = Depends(actor),
    ) -> dict[str, Any]:
        return service.create_principal(current, body)

    @app.post("/api/v1/principals/{principal_id}/token:rotate", tags=["admin"])
    async def principal_rotate(
        principal_id: str,
        current: dict[str, Any] = Depends(actor),
    ) -> dict[str, Any]:
        return service.rotate_principal_token(current, principal_id)

    @app.post("/api/v1/principals/{principal_id}:enable", tags=["admin"])
    async def principal_enable(
        principal_id: str,
        current: dict[str, Any] = Depends(actor),
    ) -> dict[str, Any]:
        return service.set_principal_enabled(current, principal_id, True)

    @app.post("/api/v1/principals/{principal_id}:disable", tags=["admin"])
    async def principal_disable(
        principal_id: str,
        current: dict[str, Any] = Depends(actor),
    ) -> dict[str, Any]:
        return service.set_principal_enabled(current, principal_id, False)

    @app.get("/api/v1/runtimes", tags=["admin"])
    async def runtimes(
        principal_id: str | None = None,
        current: dict[str, Any] = Depends(actor),
    ) -> list[dict[str, Any]]:
        require_role(current, "cao")
        return service.list_runtimes(principal_id)

    @app.post("/api/v1/principals/{principal_id}/runtimes", tags=["admin"])
    async def runtime_register(
        principal_id: str,
        body: RuntimeRegistration,
        current: dict[str, Any] = Depends(actor),
    ) -> dict[str, Any]:
        return service.register_runtime(current, principal_id, body)

    @app.post("/api/v1/runtimes/{runtime_id}:heartbeat", tags=["admin"])
    async def runtime_heartbeat(
        runtime_id: str,
        body: RuntimeHeartbeat,
        current: dict[str, Any] = Depends(actor),
    ) -> dict[str, Any]:
        return service.heartbeat_runtime(current, runtime_id, body)

    @app.post("/api/v1/runtimes/{runtime_id}:stop", tags=["admin"])
    async def runtime_stop(
        runtime_id: str,
        current: dict[str, Any] = Depends(actor),
    ) -> dict[str, Any]:
        return service.stop_runtime(current, runtime_id)

    @app.post("/api/v1/work", tags=["admin"])
    async def work_assign(
        body: WorkAssignment,
        current: dict[str, Any] = Depends(actor),
    ) -> dict[str, Any]:
        return service.assign_work(current, body)

    @app.post("/api/v1/cao-session-attachments", tags=["runtime"])
    async def cao_session_attach(
        body: CAOSessionAttachment,
        current: dict[str, Any] = Depends(actor),
    ) -> dict[str, Any]:
        """Bind this MCP client's existing CAO thread without ingesting text."""

        if not current.get("_cao_attachment_bootstrap_credential_id"):
            raise HTTPException(
                status_code=403,
                detail="attachment create/renew requires a dedicated bootstrap capability",
            )
        return service.attach_cao_session(current, body)

    @app.get("/api/v1/work", tags=["admin"])
    async def work_query(
        worker_id: str | None = None,
        state_: WorkState | None = None,
        attention_owner: str | None = None,
        limit: int = 100,
        cursor: str = "",
        current: dict[str, Any] = Depends(actor),
    ) -> dict[str, Any]:
        require_role(current, "cao", "user")
        request_body: dict[str, Any] = {
            "worker_id": worker_id,
            "state": state_.value if state_ else None,
            "attention_owner": attention_owner,
            "limit": limit,
            "cursor": cursor,
        }
        return service.query_work(QueryInput.model_validate(request_body), current)

    @app.get("/api/v1/work/{work_id}", tags=["admin"])
    async def work_get(
        work_id: str,
        current: dict[str, Any] = Depends(actor),
    ) -> dict[str, Any]:
        require_role(current, "cao", "user")
        return service.get_work(work_id, current)

    @app.post("/api/v1/work/{work_id}:revise", tags=["admin"])
    async def work_revise(
        work_id: str,
        body: GoalRevision,
        current: dict[str, Any] = Depends(actor),
    ) -> dict[str, Any]:
        return service.revise_goal(current, work_id, body)

    @app.post("/api/v1/work/{work_id}:reply", tags=["admin"])
    async def work_reply(
        work_id: str,
        body: dict[str, Any],
        current: dict[str, Any] = Depends(actor),
    ) -> dict[str, Any]:
        return service.reply(
            current,
            work_id,
            str(body.get("message", "")),
            in_reply_to=body.get("in_reply_to"),
            idempotency_key=str(body.get("idempotency_key", "")),
        )

    @app.post("/api/v1/work/{work_id}:cancel", tags=["admin"])
    async def work_cancel(
        work_id: str,
        body: dict[str, Any],
        current: dict[str, Any] = Depends(actor),
    ) -> dict[str, Any]:
        return service.cancel_work(
            current,
            work_id,
            str(body.get("reason", "Canceled")),
            str(body.get("idempotency_key", "")),
        )

    @app.post("/api/v1/work/{work_id}:resume", tags=["supervision"])
    async def work_resume(
        work_id: str,
        body: WorkResumeInput,
        current: dict[str, Any] = Depends(actor),
    ) -> dict[str, Any]:
        require_role(current, "cao")
        return service.resume_work(current, work_id, body)

    @app.post("/api/v1/memories:search", tags=["supervision"])
    async def memories_search(
        body: MemorySearchInput,
        current: dict[str, Any] = Depends(actor),
    ) -> dict[str, Any]:
        require_role(current, "cao")
        return service.search_memories(current, body)

    @app.post("/api/v1/memories:read", tags=["supervision"])
    async def memory_read(
        body: MemoryReadInput,
        current: dict[str, Any] = Depends(actor),
    ) -> dict[str, Any]:
        require_role(current, "cao")
        return service.read_memory(current, body)

    @app.post("/api/v1/memories:remember", tags=["supervision"])
    async def memory_remember(
        body: MemoryWriteInput,
        current: dict[str, Any] = Depends(actor),
    ) -> dict[str, Any]:
        require_role(current, "cao")
        return service.remember_memory(current, body)

    @app.post("/api/v1/work:history", tags=["supervision"])
    async def work_history_read(
        body: WorkHistoryReadInput,
        current: dict[str, Any] = Depends(actor),
    ) -> dict[str, Any]:
        require_role(current, "cao")
        return service.read_work_history(current, body)

    @app.post("/api/v1/work/{work_id}/attempts", tags=["admin"])
    async def attempt_create(
        work_id: str,
        body: dict[str, Any],
        current: dict[str, Any] = Depends(actor),
    ) -> dict[str, Any]:
        return service.create_attempt(
            current,
            work_id,
            body.get("worker_id"),
            body.get("runtime_session_id"),
            str(body.get("reason", "retry")),
            str(body.get("idempotency_key", "")),
            managed_worker_thread_id=body.get("managed_worker_thread_id"),
            managed_worker_thread_generation=body.get("managed_worker_thread_generation"),
        )

    @app.post("/api/v1/work/{work_id}/reasoner-turns", tags=["supervision"])
    async def reasoner_turn_acquire(
        work_id: str,
        body: ReasonerTurnAcquireInput,
        current: dict[str, Any] = Depends(actor),
    ) -> dict[str, Any]:
        return service.acquire_reasoner_turn(
            current,
            work_id,
            boundary_id=body.boundary_id,
            expected_generation=body.expected_generation,
            lease_seconds=body.lease_seconds,
            idempotency_key=body.idempotency_key,
        )

    @app.post("/api/v1/boundaries/{boundary_id}:dispose", tags=["supervision"])
    async def boundary_dispose(
        boundary_id: str,
        body: BoundaryDispositionInput,
        current: dict[str, Any] = Depends(actor),
    ) -> dict[str, Any]:
        return service.dispose_boundary(current, boundary_id, body)

    @app.get("/api/v1/worker/context", tags=["worker"])
    async def worker_context(
        attempt_id: str | None = None,
        current: dict[str, Any] = Depends(actor),
    ) -> dict[str, Any]:
        return service.get_worker_context(current, attempt_id)

    @app.get("/api/v1/inbox", tags=["messaging"])
    async def inbox(
        after: int = 0,
        limit: int = 100,
        attempt_id: str | None = None,
        include_acknowledged: bool = False,
        current: dict[str, Any] = Depends(actor),
    ) -> dict[str, Any]:
        return service.get_inbox(
            current,
            after=after,
            limit=limit,
            attempt_id=attempt_id,
            include_acknowledged=include_acknowledged,
        )

    @app.post("/api/v1/inbox:ack", tags=["messaging"])
    async def inbox_ack(
        body: AckInput,
        current: dict[str, Any] = Depends(actor),
    ) -> dict[str, Any]:
        return service.acknowledge(current, body)

    @app.post("/api/v1/messages/{message_id}:handled", tags=["messaging"])
    async def message_handled(
        message_id: str,
        body: dict[str, Any],
        current: dict[str, Any] = Depends(actor),
    ) -> dict[str, Any]:
        return service.mark_message_handled(
            current,
            message_id,
            evidence=str(body.get("evidence", "")),
        )

    @app.post("/api/v1/messages/{message_id}/delivery:resolve", tags=["messaging"])
    async def delivery_resolve(
        message_id: str,
        body: DeliveryResolveInput,
        current: dict[str, Any] = Depends(actor),
    ) -> dict[str, Any]:
        return service.resolve_delivery(current, message_id, body)

    @app.post("/api/v1/attempts/{attempt_id}:report", tags=["worker"])
    async def attempt_report(
        attempt_id: str,
        body: ReportInput,
        current: dict[str, Any] = Depends(actor),
    ) -> dict[str, Any]:
        return service.report(current, attempt_id, body)

    @app.post("/api/v1/reviews", tags=["admin"])
    async def review(
        body: ReviewInput,
        current: dict[str, Any] = Depends(actor),
    ) -> dict[str, Any]:
        return service.review(current, body)

    @app.get("/api/v1/events", tags=["operations"])
    async def events(
        after: int = 0,
        limit: int = 100,
        aggregate_type: str | None = None,
        aggregate_id: str | None = None,
        current: dict[str, Any] = Depends(actor),
    ) -> dict[str, Any]:
        require_role(current, "cao")
        return service.list_events(
            after=after,
            limit=limit,
            aggregate_type=aggregate_type,
            aggregate_id=aggregate_id,
            actor=current,
        )

    @app.post("/api/v1/effects/grants", tags=["effects"])
    async def effect_grant(
        body: EffectGrantInput,
        current: dict[str, Any] = Depends(actor),
    ) -> dict[str, Any]:
        return service.grant_effect(current, body)

    @app.post("/api/v1/effects:check", tags=["effects"])
    async def effect_check(
        body: EffectCheckInput,
        current: dict[str, Any] = Depends(actor),
    ) -> dict[str, Any]:
        if current["id"] != body.principal_id:
            require_role(current, "cao")
        return service.check_effect(body)

    @app.post("/api/v1/effects:start", tags=["effects"])
    async def effect_start(
        body: EffectCheckInput,
        current: dict[str, Any] = Depends(actor),
    ) -> dict[str, Any]:
        return service.start_effect(current, body)

    @app.post("/api/v1/effects/{operation_id}:resolve", tags=["effects"])
    async def effect_resolve(
        operation_id: str,
        body: EffectResolveInput,
        current: dict[str, Any] = Depends(actor),
    ) -> dict[str, Any]:
        return service.resolve_effect(current, operation_id, body)

    @app.get("/api/v1/dispatcher", tags=["operations"])
    async def dispatcher_status(current: dict[str, Any] = Depends(actor)) -> dict[str, Any]:
        require_role(current, "cao")
        return dispatcher.status()

    @app.post("/api/v1/dispatcher:run-once", tags=["operations"])
    async def dispatcher_once(current: dict[str, Any] = Depends(actor)) -> dict[str, Any]:
        require_role(current, "cao")
        return {"processed": await dispatcher.run_once(), "status": dispatcher.status()}

    if resolved.enable_dashboard:

        def require_dashboard(current: dict[str, Any]) -> None:
            # A dashboard receives its own read-only principal.  A CAO token
            # must never be installed in a browser, mobile client, or text UI.
            require_role(current, "dashboard")

        @app.get("/api/v1/dashboard/v1/snapshot", tags=["dashboard"])
        def dashboard_snapshot(
            current: dict[str, Any] = Depends(actor),
        ) -> dict[str, Any]:
            require_dashboard(current)
            return dashboard_read_model.snapshot()

        @app.get("/api/v1/dashboard/v1/history", tags=["dashboard"])
        def dashboard_history(
            after: str = "",
            limit: int = 100,
            current: dict[str, Any] = Depends(actor),
        ) -> JSONResponse:
            require_dashboard(current)
            if after:
                try:
                    DashboardCursor.decode(after)
                except ValueError:
                    return JSONResponse(
                        {
                            "error": {
                                "code": "invalid_request",
                                "message": "invalid dashboard cursor",
                            }
                        },
                        status_code=422,
                    )
            result = dashboard_read_model.history(
                after=after,
                limit=min(max(limit, 1), 1000),
            )
            if isinstance(result, DashboardResyncRequired):
                return JSONResponse(result.as_dict(), status_code=409)
            return JSONResponse(result)

        @app.get("/api/v1/dashboard/v1/work-history", tags=["dashboard"])
        def dashboard_work_history(
            work: str = "",
            before: str = "",
            limit: int = 20,
            current: dict[str, Any] = Depends(actor),
        ) -> JSONResponse:
            require_dashboard(current)
            if not 1 <= limit <= 100 or any(
                value and not HISTORY_REFERENCE.fullmatch(value) for value in (work, before)
            ):
                return JSONResponse(
                    {
                        "error": {
                            "code": "invalid_request",
                            "message": "invalid dashboard history request",
                        }
                    },
                    status_code=422,
                )
            try:
                return JSONResponse(
                    dashboard_read_model.work_history(work=work, before=before, limit=limit)
                )
            except ValueError:
                return JSONResponse(
                    {"error": {"code": "not_found", "message": "dashboard history is unavailable"}},
                    status_code=404,
                )

        @app.get("/api/v1/dashboard/v1/stream", tags=["dashboard"])
        async def dashboard_stream(
            request: Request,
            after: str = "",
            limit: int = 0,
            current: dict[str, Any] = Depends(actor),
        ) -> Response:
            require_dashboard(current)
            last_event_id = request.headers.get("last-event-id", "").strip()
            if after and last_event_id and after != last_event_id:
                return JSONResponse(
                    {
                        "error": {
                            "code": "invalid_request",
                            "message": "conflicting dashboard cursors",
                        }
                    },
                    status_code=422,
                )
            cursor = last_event_id or after
            if cursor:
                try:
                    DashboardCursor.decode(cursor)
                except ValueError:
                    return JSONResponse(
                        {
                            "error": {
                                "code": "invalid_request",
                                "message": "invalid dashboard cursor",
                            }
                        },
                        status_code=422,
                    )
            if limit < 0 or limit > 1000:
                return JSONResponse(
                    {
                        "error": {
                            "code": "invalid_request",
                            "message": "invalid dashboard stream limit",
                        }
                    },
                    status_code=422,
                )

            async def stream() -> AsyncIterator[bytes]:
                async for item in dashboard_read_model.stream(
                    after=cursor,
                    max_events=limit or None,
                    heartbeat_seconds=resolved.sse_heartbeat_seconds,
                ):
                    if "heartbeat" in item:
                        yield b": heartbeat\n\n"
                    elif "resync-required" in item:
                        payload = item["resync-required"]
                        yield _sse_data(
                            payload,
                            event_id=str(payload["cursor"]),
                            event_type="resync-required",
                        )
                    elif "synced" in item:
                        payload = item["synced"]
                        yield _sse_data(
                            payload,
                            event_id=str(payload["cursor"]),
                            event_type="dashboard-synced",
                        )
                    else:
                        payload = item["event"]
                        yield _sse_data(
                            payload,
                            event_id=str(item["cursor"]),
                            event_type="dashboard-update",
                        )

            return StreamingResponse(
                stream(),
                media_type="text/event-stream",
                headers={
                    "Cache-Control": "no-store",
                    "X-Accel-Buffering": "no",
                },
            )

    return app
