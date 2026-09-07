from __future__ import annotations

import asyncio
import base64
import json
from collections.abc import AsyncIterator, Mapping, Sequence
from datetime import UTC, datetime
from typing import Any

from pydantic import ValidationError as PydanticValidationError

from .canonical import canonical_json_bytes
from .errors import (
    AuthorizationError,
    ConflictError,
    ControlPlaneError,
    NotFoundError,
    ValidationError,
)
from .models import AttentionOwner, GoalMaturity, PrincipalRole, WorkAssignment, WorkState
from .security import redact_control_plane_secrets, safe_validation_details
from .service import ControlPlane

A2A_VERSION = "1.0"
A2A_MEDIA_TYPE = "application/a2a+json"

TASK_STATE_UNSPECIFIED = "TASK_STATE_UNSPECIFIED"
TASK_STATE_SUBMITTED = "TASK_STATE_SUBMITTED"
TASK_STATE_WORKING = "TASK_STATE_WORKING"
TASK_STATE_COMPLETED = "TASK_STATE_COMPLETED"
TASK_STATE_FAILED = "TASK_STATE_FAILED"
TASK_STATE_CANCELED = "TASK_STATE_CANCELED"
TASK_STATE_REJECTED = "TASK_STATE_REJECTED"
TASK_STATE_INPUT_REQUIRED = "TASK_STATE_INPUT_REQUIRED"
TASK_STATE_AUTH_REQUIRED = "TASK_STATE_AUTH_REQUIRED"

ROLE_USER = "ROLE_USER"
ROLE_AGENT = "ROLE_AGENT"

TERMINAL_A2A_STATES = {
    TASK_STATE_COMPLETED,
    TASK_STATE_CANCELED,
    TASK_STATE_FAILED,
    TASK_STATE_REJECTED,
}

TASK_NOT_FOUND = -32001
TASK_NOT_CANCELABLE = -32002
PUSH_NOT_SUPPORTED = -32003
UNSUPPORTED_OPERATION = -32004
CONTENT_TYPE_NOT_SUPPORTED = -32005
INVALID_AGENT_RESPONSE = -32006
EXTENDED_CARD_NOT_CONFIGURED = -32007
VERSION_NOT_SUPPORTED = -32009

JSONRPC_METHODS = {
    "SendMessage",
    "SendStreamingMessage",
    "GetTask",
    "ListTasks",
    "CancelTask",
    "SubscribeToTask",
    "CreateTaskPushNotificationConfig",
    "GetTaskPushNotificationConfig",
    "ListTaskPushNotificationConfigs",
    "DeleteTaskPushNotificationConfig",
    "GetExtendedAgentCard",
}
STREAMING_METHODS = {"SendStreamingMessage", "SubscribeToTask"}


class A2AProtocolError(Exception):
    """A standard A2A error that can be rendered in either transport binding."""

    def __init__(self, code: int, reason: str, message: str, **metadata: Any) -> None:
        super().__init__(message)
        self.code = code
        self.reason = reason
        self.message = message
        self.metadata = metadata


def _iso_now() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _text_from_parts(parts: Sequence[Any]) -> str:
    """Extract a deterministic instruction string from A2A 1.0 Part objects.

    A Part is a ProtoJSON oneof and therefore contains exactly one of text,
    raw, url, or data. Binary content is represented by a compact descriptor;
    raw bytes are never copied into the control-plane instruction text.
    """

    chunks: list[str] = []
    for index, part in enumerate(parts):
        if not isinstance(part, Mapping):
            raise ValidationError("message.parts entries must be objects", index=index)
        content_fields = [field for field in ("text", "raw", "url", "data") if field in part]
        if len(content_fields) != 1:
            raise ValidationError(
                "each A2A Part must contain exactly one of text, raw, url, or data",
                index=index,
                fields=content_fields,
            )
        field = content_fields[0]
        media_type = part.get("mediaType")
        if media_type is not None and (not isinstance(media_type, str) or not media_type.strip()):
            raise ValidationError("Part.mediaType must be a non-empty string", index=index)
        filename = part.get("filename")
        if filename is not None and not isinstance(filename, str):
            raise ValidationError("Part.filename must be a string", index=index)
        if field == "text":
            value = part.get("text")
            if not isinstance(value, str):
                raise ValidationError("Part.text must be a string", index=index)
            chunks.append(value)
        elif field == "data":
            value = part.get("data")
            if not isinstance(value, Mapping):
                raise ValidationError("Part.data must be an object", index=index)
            chunks.append(json.dumps(value, ensure_ascii=False, sort_keys=True))
        elif field == "url":
            value = part.get("url")
            if not isinstance(value, str) or not value:
                raise ValidationError("Part.url must be a non-empty string", index=index)
            chunks.append(
                json.dumps(
                    {
                        "url": value,
                        "filename": part.get("filename", ""),
                        "mediaType": part.get("mediaType", ""),
                    },
                    ensure_ascii=False,
                    sort_keys=True,
                )
            )
        else:
            raw = part.get("raw")
            if not isinstance(raw, str) or not raw:
                raise ValidationError("Part.raw must be a non-empty base64 string", index=index)
            try:
                padding = "=" * (-len(raw) % 4)
                base64.b64decode((raw + padding).encode("ascii"), altchars=b"-_", validate=True)
            except (UnicodeEncodeError, ValueError) as error:
                raise ValidationError("Part.raw must be valid base64", index=index) from error
            chunks.append(
                json.dumps(
                    {
                        "embeddedBytes": len(raw),
                        "filename": part.get("filename", ""),
                        "mediaType": part.get("mediaType", ""),
                    },
                    ensure_ascii=False,
                    sort_keys=True,
                )
            )
    return "\n".join(value for value in chunks if value).strip()


def _encode_page(updated_at: str, task_id: str) -> str:
    payload = canonical_json_bytes({"updatedAt": updated_at, "taskId": task_id})
    return base64.urlsafe_b64encode(payload).decode("ascii").rstrip("=")


def _decode_page(value: str | None) -> tuple[str, str] | None:
    if not value:
        return None
    try:
        padded = value + "=" * (-len(value) % 4)
        parsed = json.loads(base64.urlsafe_b64decode(padded.encode("ascii")).decode("utf-8"))
        updated_at = parsed["updatedAt"]
        task_id = parsed["taskId"]
        if not isinstance(updated_at, str) or not isinstance(task_id, str):
            raise ValueError
        return updated_at, task_id
    except (KeyError, TypeError, ValueError, UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ValidationError("invalid page token") from error


class A2AServer:
    """A2A 1.0 facade over the durable CAO domain model.

    WorkItem is the stable A2A Task identity. Attempts remain internal retries
    beneath that Task, preserving task identity across corrections and worker
    restarts.
    """

    def __init__(self, service: ControlPlane) -> None:
        self.service = service
        self.settings = service.settings

    def agent_card(self, *, authenticated: bool = False) -> dict[str, Any]:
        base = self.settings.public_base_url.rstrip("/")
        card: dict[str, Any] = {
            "name": self.settings.server_name,
            "description": (
                "A durable local Agent-to-Agent supervision control plane. It keeps "
                "WorkItems, Attempts, ordered messages, evidence, reviews, runtime "
                "per-recipient deliveries, and effect authority separate and auditable."
            ),
            "supportedInterfaces": [
                {
                    "url": f"{base}/a2a",
                    "protocolBinding": "JSONRPC",
                    "protocolVersion": A2A_VERSION,
                },
                {
                    "url": f"{base}/a2a/http",
                    "protocolBinding": "HTTP+JSON",
                    "protocolVersion": A2A_VERSION,
                },
            ],
            "version": self.settings.server_version,
            "capabilities": {
                "streaming": True,
                "pushNotifications": bool(self.settings.enable_a2a_push),
                "stateTransitionHistory": True,
            },
            "securitySchemes": {
                "localBearer": {
                    "httpAuthSecurityScheme": {
                        "scheme": "bearer",
                        "bearerFormat": "CAO local principal token",
                        "description": "Local bearer token issued by the control plane.",
                    }
                }
            },
            "securityRequirements": [{"schemes": {"localBearer": {"list": []}}}],
            "defaultInputModes": ["text/plain", "application/json"],
            "defaultOutputModes": ["text/plain", "application/json"],
            "skills": [
                {
                    "id": "supervise-agent-work",
                    "name": "Supervise agent work",
                    "description": (
                        "Assigns a Worker, accepts structured questions and evidence, and "
                        "keeps Worker completion claims separate from CAO review and user acceptance."
                    ),
                    "tags": ["supervision", "orchestration", "mcp", "a2a"],
                    "examples": [
                        "Assign this objective with explicit acceptance conditions.",
                        "Continue the existing task with this additional instruction.",
                    ],
                    "inputModes": ["text/plain", "application/json"],
                    "outputModes": ["text/plain", "application/json"],
                },
                {
                    "id": "durable-agent-messaging",
                    "name": "Durable agent messaging",
                    "description": (
                        "Provides ordered inboxes, acknowledgements, idempotency, retries, "
                        "runtime deliveries, and recovery after process restart."
                    ),
                    "tags": ["messaging", "durability", "runtime"],
                    "inputModes": ["application/json", "text/plain"],
                    "outputModes": ["application/json", "text/plain"],
                },
            ],
        }
        if authenticated:
            if self.settings.enable_api_docs:
                card["documentationUrl"] = f"{base}/docs"
            card["metadata"] = {
                "mcpEndpoint": f"{base}/mcp",
                "healthEndpoint": f"{base}/health",
                "controlPlaneModel": "WorkItem/Attempt",
                "completionPolicy": (
                    "A Worker completion claim remains declared evidence until CAO review."
                ),
            }
        return card

    @staticmethod
    def return_immediately(params: Mapping[str, Any]) -> bool:
        configuration = params.get("configuration", {})
        return bool(
            isinstance(configuration, Mapping) and configuration.get("returnImmediately", False)
        )

    async def wait_for_settled(
        self,
        task_id: str,
        *,
        actor: dict[str, Any],
        timeout_seconds: float | None = None,
    ) -> dict[str, Any]:
        """Wait for a blocking SendMessage response boundary.

        A2A blocking calls return when work completes or requires external
        input.  The bounded timeout is an operational guard; on expiry the
        current Task projection is returned rather than losing its identity.
        """

        timeout = timeout_seconds or self.settings.a2a_blocking_timeout_seconds
        deadline = asyncio.get_running_loop().time() + timeout
        while True:
            task = self.get_task(task_id, actor=actor)
            state = str(task["status"]["state"])
            if state in TERMINAL_A2A_STATES or state in {
                TASK_STATE_INPUT_REQUIRED,
                TASK_STATE_AUTH_REQUIRED,
            }:
                return task
            if asyncio.get_running_loop().time() >= deadline:
                return task
            await asyncio.sleep(min(0.25, max(0.05, deadline - asyncio.get_running_loop().time())))

    async def handle_jsonrpc_async(
        self,
        actor: dict[str, Any],
        request: Mapping[str, Any],
    ) -> dict[str, Any]:
        response = self.handle_jsonrpc(actor, request)
        if (
            request.get("method") == "SendMessage"
            and "result" in response
            and not self.return_immediately(
                request.get("params", {}) if isinstance(request.get("params", {}), Mapping) else {}
            )
        ):
            result = response.get("result", {})
            task = result.get("task") if isinstance(result, Mapping) else None
            if isinstance(task, Mapping) and task.get("id"):
                settled = await self.wait_for_settled(str(task["id"]), actor=actor)
                response["result"] = {"task": settled}
        return response

    def handle_jsonrpc(self, actor: dict[str, Any], request: Mapping[str, Any]) -> dict[str, Any]:
        request_id = request.get("id")
        method = request.get("method")
        params = request.get("params", {})
        if request.get("jsonrpc") != "2.0" or not isinstance(method, str):
            return self._error(request_id, -32600, "Request payload validation error")
        if params is None:
            params = {}
        if not isinstance(params, Mapping):
            return self._error(request_id, -32602, "Invalid parameters")
        if method in STREAMING_METHODS:
            return self._error(
                request_id,
                UNSUPPORTED_OPERATION,
                "Streaming operations require an SSE response",
            )
        try:
            if method == "SendMessage":
                result = {"task": self.send_message(actor, dict(params))}
            elif method == "GetTask":
                result = self.get_task(
                    str(params.get("id") or ""),
                    history_length=self._optional_int(params.get("historyLength")),
                    actor=actor,
                )
            elif method == "ListTasks":
                result = self.list_tasks(actor, dict(params))
            elif method == "CancelTask":
                result = self.cancel_task(
                    actor,
                    str(params.get("id") or ""),
                    str(params.get("reason") or "Canceled by A2A client"),
                )
            elif method == "CreateTaskPushNotificationConfig":
                result = self.set_push_config(actor, dict(params))
            elif method == "GetTaskPushNotificationConfig":
                result = self.get_push_config(actor, dict(params))
            elif method == "ListTaskPushNotificationConfigs":
                result = self.list_push_configs(actor, dict(params))
            elif method == "DeleteTaskPushNotificationConfig":
                self.delete_push_config(actor, dict(params))
                result = {}
            elif method == "GetExtendedAgentCard":
                result = self.agent_card(authenticated=True)
            else:
                return self._error(request_id, -32601, "Method not found")
            return {"jsonrpc": "2.0", "id": request_id, "result": result}
        except PydanticValidationError as error:
            return self._error(
                request_id,
                -32602,
                "Invalid parameters",
                self._bad_request_details(
                    safe_validation_details(
                        error.errors(include_url=False),
                        default_location="request",
                    )
                ),
            )
        except NotFoundError as error:
            return self._error(
                request_id,
                TASK_NOT_FOUND,
                error.message,
                self._error_info("TASK_NOT_FOUND", error.details or {}),
            )
        except AuthorizationError:
            return self._error(
                request_id,
                TASK_NOT_FOUND,
                "Task not found",
                self._error_info("TASK_NOT_FOUND", {}),
            )
        except ConflictError as error:
            return self._error(
                request_id,
                TASK_NOT_CANCELABLE,
                error.message,
                self._error_info("TASK_NOT_CANCELABLE", error.details or {}),
            )
        except A2AProtocolError as error:
            return self._error(
                request_id,
                error.code,
                error.message,
                self._error_info(error.reason, error.metadata),
            )
        except ValidationError as error:
            return self._error(
                request_id,
                -32602,
                "Invalid parameters",
                self._bad_request_details(
                    [{"field": "request", "description": error.message, **(error.details or {})}]
                ),
            )
        except ControlPlaneError as error:
            return self._error(
                request_id,
                -32000,
                error.message,
                self._error_info(error.code.upper(), error.details or {}),
            )
        except (TypeError, ValueError) as error:
            return self._error(
                request_id,
                -32602,
                "Invalid parameters",
                self._bad_request_details(
                    [
                        {
                            "field": "request",
                            "description": "Input validation failed",
                            "type": type(error).__name__,
                        }
                    ]
                ),
            )
        except Exception as error:
            return self._error(
                request_id,
                -32603,
                "Internal error",
                self._error_info(
                    "INTERNAL_ERROR",
                    {"type": type(error).__name__},
                ),
            )

    def send_message(self, actor: dict[str, Any], params: dict[str, Any]) -> dict[str, Any]:
        if actor["role"] != PrincipalRole.CAO.value:
            raise AuthorizationError("A2A SendMessage requires a CAO principal")
        message = params.get("message")
        if not isinstance(message, Mapping):
            raise ValidationError("SendMessage requires a message object")
        message_id = message.get("messageId")
        if not isinstance(message_id, str) or not message_id.strip():
            raise ValidationError("A2A messages require a non-empty messageId")
        if message.get("role") != ROLE_USER:
            raise ValidationError("A2A client messages must use role ROLE_USER")
        parts = message.get("parts")
        if not isinstance(parts, list) or not parts:
            raise ValidationError("A2A message.parts must be a non-empty array")
        text = _text_from_parts(parts)
        if not text:
            raise ValidationError("A2A message must contain usable content")
        metadata = message.get("metadata", {})
        if not isinstance(metadata, Mapping):
            raise ValidationError("message.metadata must be an object")
        task_id = self._consistent_identifier(message, params, "taskId")
        context_id = self._consistent_identifier(message, params, "contextId")

        if task_id:
            mapping, work = self._authorized_task(actor, task_id)
            if context_id and context_id != mapping["context_id"]:
                raise ValidationError("message contextId does not match the task context")
            state = self._a2a_state(work, work.get("current_attempt") or {})
            if state in TERMINAL_A2A_STATES:
                raise ConflictError(
                    "terminal A2A tasks cannot accept additional messages", state=state
                )
            self.service.reply(
                actor,
                mapping["work_item_id"],
                text,
                in_reply_to=str(metadata.get("inReplyTo") or "") or None,
                idempotency_key=message_id,
            )
            return self.get_task(task_id, actor=actor)

        worker_id = str(metadata.get("workerId") or params.get("workerId") or "")
        if not worker_id:
            worker_id = self._default_worker_id()
        title = str(metadata.get("title") or text.splitlines()[0][:300] or "A2A task")
        objective = str(metadata.get("objective") or text)
        acceptance = metadata.get("acceptance", [])
        non_goals = metadata.get("nonGoals", [])
        if not isinstance(acceptance, list) or not isinstance(non_goals, list):
            raise ValidationError("acceptance and nonGoals must be arrays")
        raw_maturity = metadata.get("maturity")
        maturity = (
            GoalMaturity(str(raw_maturity))
            if raw_maturity is not None
            else (GoalMaturity.DEFINED if acceptance else GoalMaturity.EXPLORING)
        )
        assignment = self.service.assign_work(
            actor,
            WorkAssignment(
                worker_id=worker_id,
                title=title,
                objective=objective,
                maturity=maturity,
                acceptance=[str(value) for value in acceptance],
                non_goals=[str(value) for value in non_goals],
                priority=int(metadata.get("priority", 50)),
                runtime_session_id=metadata.get("runtimeSessionId"),
                managed_worker_thread_id=metadata.get("managedWorkerThreadId"),
                managed_worker_thread_generation=metadata.get("managedWorkerThreadGeneration"),
                requester_id=(
                    str(metadata.get("requesterId")) if metadata.get("requesterId") else None
                ),
                metadata={
                    "a2a": True,
                    "contextId": message.get("contextId") or params.get("contextId"),
                    "clientMessageId": message.get("messageId"),
                    **(
                        dict(metadata.get("workMetadata", {}))
                        if isinstance(metadata.get("workMetadata"), Mapping)
                        else {}
                    ),
                },
                idempotency_key=message_id,
            ),
        )
        mapping = self.service.task_for_work(assignment["id"])
        if context_id:
            with self.service.db.transaction() as connection:
                connection.execute(
                    "UPDATE a2a_task_map SET context_id = ? WHERE task_id = ?",
                    (context_id, mapping["task_id"]),
                )

        configuration = params.get("configuration", {})
        if isinstance(configuration, Mapping):
            push = configuration.get("taskPushNotificationConfig")
            if push is not None:
                if not self.settings.enable_a2a_push:
                    raise A2AProtocolError(
                        PUSH_NOT_SUPPORTED,
                        "PUSH_NOTIFICATION_NOT_SUPPORTED",
                        "A2A push notifications are disabled",
                    )
                self.set_push_config(
                    actor,
                    {"taskId": mapping["task_id"], "taskPushNotificationConfig": push},
                )
        return self.get_task(mapping["task_id"], actor=actor)

    @staticmethod
    def _consistent_identifier(
        message: Mapping[str, Any], params: Mapping[str, Any], field: str
    ) -> str:
        message_value = message.get(field)
        params_value = params.get(field)
        for value in (message_value, params_value):
            if value is not None and (not isinstance(value, str) or not value.strip()):
                raise ValidationError(f"{field} must be a non-empty string when present")
        if message_value is not None and params_value is not None and message_value != params_value:
            raise ValidationError(f"message.{field} must match params.{field}")
        value = message_value if message_value is not None else params_value
        return str(value or "")

    def get_task(
        self,
        task_id: str,
        *,
        history_length: int | None = None,
        actor: dict[str, Any] | None = None,
        include_artifacts: bool = True,
    ) -> dict[str, Any]:
        mapping, work = self._authorized_task(actor, task_id)
        return self._task_view(
            mapping,
            work,
            history_length=history_length,
            include_artifacts=include_artifacts,
        )

    def list_tasks(self, actor: dict[str, Any], params: dict[str, Any]) -> dict[str, Any]:
        self._require_client_actor(actor)
        page_size = min(max(int(params.get("pageSize", 50)), 1), 100)
        cursor = _decode_page(params.get("pageToken"))
        context_id = str(params.get("contextId") or "")
        status = str(params.get("status") or "")
        include_artifacts = bool(params.get("includeArtifacts", False))
        clauses = ["1 = 1"]
        values: list[Any] = []
        if actor.get("_cao_attachment_id"):
            clauses.append("w.supervisor_attachment_id = ?")
            values.append(str(actor["_cao_attachment_id"]))
        if context_id:
            clauses.append("m.context_id = ?")
            values.append(context_id)
        status_expr = """
            CASE
              WHEN w.state = 'completed' THEN 'TASK_STATE_COMPLETED'
              WHEN w.state = 'canceled' THEN 'TASK_STATE_CANCELED'
              WHEN w.state = 'failed' THEN 'TASK_STATE_FAILED'
              WHEN w.state = 'user_needed' OR a.state = 'input_required'
                   OR EXISTS (
                       SELECT 1
                       FROM boundaries AS b
                       LEFT JOIN boundary_dispositions AS d ON d.boundary_id = b.id
                       LEFT JOIN boundary_supersessions AS s ON s.boundary_id = b.id
                       WHERE b.work_item_id = w.id
                         AND d.id IS NULL
                         AND s.boundary_id IS NULL
                         AND b.kind IN (
                             'question', 'blocker', 'review_rejected', 'user_rejection'
                         )
                   )
                THEN 'TASK_STATE_INPUT_REQUIRED'
              WHEN w.state = 'open' OR a.state = 'assigned' THEN 'TASK_STATE_SUBMITTED'
              ELSE 'TASK_STATE_WORKING'
            END
        """
        if status:
            if status not in {
                TASK_STATE_SUBMITTED,
                TASK_STATE_WORKING,
                TASK_STATE_INPUT_REQUIRED,
                TASK_STATE_COMPLETED,
                TASK_STATE_CANCELED,
                TASK_STATE_FAILED,
            }:
                return {"tasks": [], "nextPageToken": "", "pageSize": page_size, "totalSize": 0}
            clauses.append(f"({status_expr}) = ?")
            values.append(status)
        if cursor is not None:
            updated_at, task_id = cursor
            clauses.append("(w.updated_at < ? OR (w.updated_at = ? AND m.task_id < ?))")
            values.extend([updated_at, updated_at, task_id])
        rows = self.service.db.fetchall(
            f"""
            SELECT m.task_id, m.work_item_id, m.context_id, w.updated_at
            FROM a2a_task_map AS m
            JOIN work_items AS w ON w.id = m.work_item_id
            LEFT JOIN attempts AS a
              ON a.work_item_id = w.id
             AND a.attempt_number = (
                SELECT MAX(a2.attempt_number) FROM attempts AS a2 WHERE a2.work_item_id = w.id
             )
            WHERE {" AND ".join(clauses)}
            ORDER BY w.updated_at DESC, m.task_id DESC
            LIMIT ?
            """,
            [*values, page_size + 1],
        )
        has_more = len(rows) > page_size
        rows = rows[:page_size]
        tasks = [
            self.get_task(
                str(row["task_id"]),
                history_length=0,
                actor=actor,
                include_artifacts=include_artifacts,
            )
            for row in rows
        ]
        count_clauses = [clause for clause in clauses if not clause.startswith("(w.updated_at <")]
        count_values = values[:-3] if cursor is not None else values
        total = self.service.db.fetchone(
            f"""
            SELECT COUNT(*) AS count
            FROM a2a_task_map AS m
            JOIN work_items AS w ON w.id = m.work_item_id
            LEFT JOIN attempts AS a
              ON a.work_item_id = w.id
             AND a.attempt_number = (
                SELECT MAX(a2.attempt_number) FROM attempts AS a2 WHERE a2.work_item_id = w.id
             )
            WHERE {" AND ".join(count_clauses)}
            """,
            count_values,
        )
        next_token = ""
        if has_more and rows:
            next_token = _encode_page(str(rows[-1]["updated_at"]), str(rows[-1]["task_id"]))
        return {
            "tasks": tasks,
            "nextPageToken": next_token,
            "pageSize": page_size,
            "totalSize": int(total["count"] if total else len(tasks)),
        }

    def cancel_task(self, actor: dict[str, Any], task_id: str, reason: str) -> dict[str, Any]:
        mapping, work = self._authorized_task(actor, task_id)
        if self._a2a_state(work, work.get("current_attempt") or {}) in TERMINAL_A2A_STATES:
            raise ConflictError("task is not cancelable in its current state")
        self.service.cancel_work(
            actor,
            mapping["work_item_id"],
            reason,
            idempotency_key=f"a2a-cancel:{task_id}",
        )
        return self.get_task(task_id, actor=actor)

    def set_push_config(self, actor: dict[str, Any], params: dict[str, Any]) -> dict[str, Any]:
        if not self.settings.enable_a2a_push:
            raise A2AProtocolError(
                PUSH_NOT_SUPPORTED,
                "PUSH_NOTIFICATION_NOT_SUPPORTED",
                "A2A push notifications are disabled",
            )
        task_id = str(params.get("taskId") or params.get("id") or "")
        self._authorized_task(actor, task_id)
        config = params.get("taskPushNotificationConfig", params.get("config", params))
        if not isinstance(config, Mapping):
            raise ValidationError("push notification config must be an object")
        authentication = config.get("authentication", {})
        if not isinstance(authentication, Mapping):
            raise ValidationError("push notification authentication must be an object")
        scheme = str(authentication.get("scheme") or config.get("authenticationScheme") or "Bearer")
        created = self.service.create_push_config(
            actor,
            task_id,
            str(config.get("url") or ""),
            token=str(authentication.get("credentials") or config.get("token") or ""),
            authentication_scheme=scheme,
            metadata=dict(config.get("metadata", {}))
            if isinstance(config.get("metadata"), Mapping)
            else {},
        )
        return self._a2a_push_view(created)

    def get_push_config(self, actor: dict[str, Any], params: dict[str, Any]) -> dict[str, Any]:
        task_id = str(params.get("taskId") or "")
        self._authorized_task(actor, task_id)
        config_id = str(
            params.get("configId")
            or params.get("pushNotificationConfigId")
            or params.get("id")
            or ""
        )
        return self._a2a_push_view(self.service.get_push_config(task_id, config_id))

    def list_push_configs(self, actor: dict[str, Any], params: dict[str, Any]) -> dict[str, Any]:
        task_id = str(params.get("taskId") or params.get("id") or "")
        self._authorized_task(actor, task_id)
        return {
            "configs": [
                self._a2a_push_view(value) for value in self.service.list_push_configs(task_id)
            ]
        }

    def delete_push_config(self, actor: dict[str, Any], params: dict[str, Any]) -> dict[str, Any]:
        task_id = str(params.get("taskId") or "")
        self._authorized_task(actor, task_id)
        config_id = str(
            params.get("configId")
            or params.get("pushNotificationConfigId")
            or params.get("id")
            or ""
        )
        try:
            return self._a2a_push_view(self.service.delete_push_config(actor, task_id, config_id))
        except NotFoundError:
            # Deletion is idempotent in the A2A HTTP binding.
            return {"id": config_id}

    async def task_event_stream(
        self,
        task_id: str,
        *,
        actor: dict[str, Any] | None = None,
        after: int = 0,
        heartbeat_seconds: float | None = None,
        reject_terminal: bool = False,
    ) -> AsyncIterator[dict[str, Any]]:
        mapping, work = self._authorized_task(actor, task_id)
        state = self._a2a_state(work, work.get("current_attempt") or {})
        if reject_terminal and state in TERMINAL_A2A_STATES:
            raise ConflictError("terminal tasks cannot be subscribed", state=state)
        work_id = mapping["work_item_id"]
        cursor = max(after, 0)
        heartbeat = heartbeat_seconds or self.settings.sse_heartbeat_seconds
        yield {"task": self.get_task(task_id, actor=actor)}
        if state == TASK_STATE_INPUT_REQUIRED:
            return
        last_state = state
        known_artifacts = {
            item["artifactId"] for item in self.get_task(task_id, actor=actor).get("artifacts", [])
        }
        last_emit = asyncio.get_running_loop().time()
        while True:
            events = self.service.list_events(after=cursor, limit=100)
            emitted = False
            for event in events["items"]:
                cursor = max(cursor, int(event["sequence"]))
                if not self._event_matches_work(event, work_id):
                    continue
                task = self.get_task(task_id, actor=actor)
                current_state = str(task["status"]["state"])
                if current_state != last_state or str(event.get("type", "")).startswith("attempt."):
                    emitted = True
                    last_state = current_state
                    yield {
                        "statusUpdate": {
                            "taskId": task_id,
                            "contextId": mapping["context_id"],
                            "status": task["status"],
                            "metadata": {
                                "eventSequence": event["sequence"],
                                "eventType": event["type"],
                            },
                        }
                    }
                for artifact in task.get("artifacts", []):
                    artifact_id = str(artifact["artifactId"])
                    if artifact_id in known_artifacts:
                        continue
                    known_artifacts.add(artifact_id)
                    emitted = True
                    yield {
                        "artifactUpdate": {
                            "taskId": task_id,
                            "contextId": mapping["context_id"],
                            "artifact": artifact,
                            "append": False,
                            "lastChunk": True,
                            "metadata": {"eventSequence": event["sequence"]},
                        }
                    }
                if (
                    current_state in TERMINAL_A2A_STATES
                    or current_state == TASK_STATE_INPUT_REQUIRED
                ):
                    return
            current = self.get_task(task_id, actor=actor)
            if (
                current["status"]["state"] in TERMINAL_A2A_STATES
                or current["status"]["state"] == TASK_STATE_INPUT_REQUIRED
            ):
                if current["status"]["state"] != last_state:
                    yield {
                        "statusUpdate": {
                            "taskId": task_id,
                            "contextId": mapping["context_id"],
                            "status": current["status"],
                        }
                    }
                return
            now = asyncio.get_running_loop().time()
            if not emitted and now - last_emit >= heartbeat:
                last_emit = now
                yield {"heartbeat": {"timestamp": _iso_now(), "cursor": cursor}}
            await asyncio.sleep(min(1.0, max(0.1, heartbeat / 2)))

    def push_payload(
        self,
        task_id: str,
        *,
        event_sequence: int | None = None,
        event_type: str = "",
    ) -> dict[str, Any]:
        """Build one A2A 1.0 StreamResponse for an asynchronous webhook."""
        mapping = self.service.task_mapping(task_id)
        task = self.get_task(task_id)
        data: dict[str, Any] = {
            "taskId": task_id,
            "contextId": mapping["context_id"],
            "status": task["status"],
        }
        metadata: dict[str, Any] = {}
        if event_sequence is not None:
            metadata["eventSequence"] = event_sequence
        if event_type:
            metadata["eventType"] = event_type
        if metadata:
            data["metadata"] = metadata
        return {"statusUpdate": data}

    def _task_view(
        self,
        mapping: Mapping[str, Any],
        work: Mapping[str, Any],
        *,
        history_length: int | None,
        include_artifacts: bool,
    ) -> dict[str, Any]:
        current_attempt = work.get("current_attempt") or {}
        task: dict[str, Any] = {
            "id": mapping["task_id"],
            "contextId": mapping["context_id"],
            "status": {
                "state": self._a2a_state(work, current_attempt),
                "message": self._status_message(work, current_attempt),
                "timestamp": work["updated_at"],
            },
            "metadata": {
                "workItemId": work["id"],
                "goalVersion": work["goal_version"],
                "goalMaturity": work["maturity"],
                "objective": work["objective"],
                "acceptance": work["acceptance"],
                "nonGoals": work["non_goals"],
                "attentionOwner": work["attention_owner"],
                "attemptId": current_attempt.get("id"),
                "attemptNumber": current_attempt.get("attempt_number"),
                "trajectory": current_attempt.get("trajectory"),
                "evidenceConfidence": current_attempt.get("evidence_confidence"),
                "reviews": work.get("reviews", []),
            },
        }
        if history_length is not None and history_length > 0:
            task["history"] = self._history_for_work(str(work["id"]), history_length)
        if include_artifacts:
            task["artifacts"] = [self._a2a_artifact(value) for value in work.get("artifacts", [])]
        return task

    def _history_for_work(self, work_id: str, history_length: int | None) -> list[dict[str, Any]]:
        rows = self.service.db.fetchall(
            "SELECT * FROM messages WHERE work_item_id = ? ORDER BY sequence ASC",
            (work_id,),
        )
        if history_length is not None:
            rows = rows[-max(history_length, 0) :] if history_length > 0 else []
        task_id = self.service.task_for_work(work_id)["task_id"]
        result: list[dict[str, Any]] = []
        for row in rows:
            message = self.service._message_view(row)
            deliveries = [
                {
                    "recipientId": delivery["recipient_id"],
                    "state": delivery["state"],
                }
                for delivery in self.service.db.fetchall(
                    """
                    SELECT recipient_id, state
                    FROM message_deliveries
                    WHERE message_id = ?
                    ORDER BY recipient_id
                    """,
                    (message["id"],),
                )
            ]
            sender = self.service.get_principal(message["sender_id"])
            role = ROLE_AGENT if sender["role"] in {"cao", "worker"} else ROLE_USER
            result.append(
                {
                    "messageId": message["id"],
                    "taskId": task_id,
                    "role": role,
                    "parts": [
                        {
                            "data": {
                                "kind": message["kind"],
                                "payload": message["payload"],
                                "goalVersion": message["goal_version"],
                            }
                        }
                    ],
                    "metadata": {
                        "sequence": message["sequence"],
                        "senderId": message["sender_id"],
                        "deliveries": deliveries,
                        "createdAt": message["created_at"],
                    },
                }
            )
        return result

    @staticmethod
    def _a2a_artifact(value: Mapping[str, Any]) -> dict[str, Any]:
        part: dict[str, Any] = {
            "url": value["uri"],
            "mediaType": value["media_type"],
        }
        if value.get("name"):
            part["filename"] = value["name"]
        return {
            "artifactId": value["id"],
            "name": value["name"],
            "description": value.get("metadata", {}).get("description", ""),
            "parts": [part],
            "metadata": {
                "digest": value.get("digest", ""),
                "producerId": value.get("producer_id", ""),
                **dict(value.get("metadata", {})),
            },
        }

    @staticmethod
    def _a2a_push_view(value: Mapping[str, Any]) -> dict[str, Any]:
        authentication: dict[str, Any] = {
            "scheme": value["authentication_scheme"],
        }
        token = str(value.get("token", ""))
        if token and token != "***":
            authentication["credentials"] = token
        return {
            "id": value["id"],
            "url": value["url"],
            "authentication": authentication,
            "metadata": value.get("metadata", {}),
        }

    @staticmethod
    def _a2a_state(work: Mapping[str, Any], attempt: Mapping[str, Any]) -> str:
        work_state = str(work.get("state", ""))
        attempt_state = str(attempt.get("state", ""))
        if work_state == WorkState.COMPLETED.value:
            return TASK_STATE_COMPLETED
        if work_state == WorkState.CANCELED.value:
            return TASK_STATE_CANCELED
        if work_state == WorkState.FAILED.value:
            return TASK_STATE_FAILED
        open_boundaries = work.get("open_boundaries") or []
        attention_owner = str(work.get("attention_owner", ""))
        client_actionable_boundary = any(
            isinstance(boundary, Mapping)
            and boundary.get("kind") in {"question", "blocker", "review_rejected", "user_rejection"}
            for boundary in open_boundaries
        )
        if (
            work_state == WorkState.USER_NEEDED.value
            or attempt_state == "input_required"
            or client_actionable_boundary
            or attention_owner != AttentionOwner.WORKER.value
        ):
            return TASK_STATE_INPUT_REQUIRED
        if attempt_state == "assigned":
            return TASK_STATE_SUBMITTED
        return TASK_STATE_WORKING

    @staticmethod
    def _status_message(work: Mapping[str, Any], attempt: Mapping[str, Any]) -> dict[str, Any]:
        summary = ""
        claim = attempt.get("completion_claim") or {}
        if isinstance(claim, Mapping):
            summary = str(claim.get("summary", ""))
        if not summary:
            summary = str(attempt.get("stage") or work.get("objective") or work.get("title") or "")
        return {
            "messageId": (
                f"status-{work['id']}-{work['goal_version']}-{attempt.get('attempt_number', 0)}"
            ),
            "role": ROLE_AGENT,
            "parts": [{"text": summary}],
            "metadata": {
                "attentionOwner": work.get("attention_owner"),
                "nextBoundary": attempt.get("next_boundary", ""),
            },
        }

    def _authorized_task(
        self,
        actor: dict[str, Any] | None,
        task_id: str,
    ) -> tuple[dict[str, Any], dict[str, Any]]:
        if not task_id:
            raise ValidationError("task ID is required")
        mapping = self.service.task_mapping(task_id)
        work = self.service.get_work(mapping["work_item_id"], actor)
        if actor is not None:
            self._require_client_actor(actor)
        return mapping, work

    @staticmethod
    def _require_client_actor(actor: Mapping[str, Any]) -> None:
        if actor.get("role") != PrincipalRole.CAO.value:
            raise AuthorizationError("only CAO principals may use the A2A client interface")

    def _default_worker_id(self) -> str:
        rows = [
            value
            for value in self.service.list_principals()
            if value["role"] == PrincipalRole.WORKER.value and value["enabled"]
        ]
        if len(rows) != 1:
            raise ValidationError(
                "new A2A tasks require message.metadata.workerId unless exactly one Worker exists",
                enabled_worker_count=len(rows),
            )
        return str(rows[0]["id"])

    def _event_matches_work(self, event: Mapping[str, Any], work_id: str) -> bool:
        aggregate_type = str(event.get("aggregate_type", ""))
        aggregate_id = str(event.get("aggregate_id", ""))
        if aggregate_type == "work_item":
            return aggregate_id == work_id
        if aggregate_type == "attempt":
            row = self.service.db.fetchone(
                "SELECT work_item_id FROM attempts WHERE id = ?", (aggregate_id,)
            )
            return bool(row and row["work_item_id"] == work_id)
        if aggregate_type == "message":
            row = self.service.db.fetchone(
                "SELECT work_item_id FROM messages WHERE id = ?", (aggregate_id,)
            )
            return bool(row and row["work_item_id"] == work_id)
        return False

    @staticmethod
    def _work_states_for_a2a(state: str) -> list[str]:
        return {
            TASK_STATE_SUBMITTED: [WorkState.OPEN.value, WorkState.ACTIVE.value],
            TASK_STATE_WORKING: [
                WorkState.ACTIVE.value,
                WorkState.SUSPENDED.value,
                WorkState.WAITING_SUPERVISOR.value,
                WorkState.WAITING_REVIEW.value,
                WorkState.WAITING_USER.value,
            ],
            TASK_STATE_INPUT_REQUIRED: [WorkState.USER_NEEDED.value],
            TASK_STATE_COMPLETED: [WorkState.COMPLETED.value],
            TASK_STATE_CANCELED: [WorkState.CANCELED.value],
            TASK_STATE_FAILED: [WorkState.FAILED.value],
            TASK_STATE_REJECTED: [],
            TASK_STATE_AUTH_REQUIRED: [],
        }.get(state, [])

    @staticmethod
    def _optional_int(value: Any) -> int | None:
        return None if value is None else max(int(value), 0)

    @staticmethod
    def _error_info(reason: str, metadata: Mapping[str, Any]) -> list[dict[str, Any]]:
        return [
            {
                "@type": "type.googleapis.com/google.rpc.ErrorInfo",
                "reason": reason,
                "domain": "a2a-protocol.org",
                "metadata": {str(key): str(value) for key, value in metadata.items()},
            }
        ]

    @staticmethod
    def _bad_request_details(violations: Sequence[Any]) -> list[dict[str, Any]]:
        normalized: list[dict[str, str]] = []
        for value in violations:
            if isinstance(value, Mapping):
                field = value.get("field") or value.get("loc") or "request"
                if isinstance(field, (list, tuple)):
                    field = ".".join(str(item) for item in field)
                description = value.get("description") or value.get("msg") or str(value)
            else:
                field = "request"
                description = str(value)
            normalized.append(
                {
                    "field": str(redact_control_plane_secrets(field)),
                    "description": str(redact_control_plane_secrets(description)),
                }
            )
        return [
            {
                "@type": "type.googleapis.com/google.rpc.BadRequest",
                "fieldViolations": normalized,
            }
        ]

    @staticmethod
    def _error(request_id: Any, code: int, message: str, data: Any = None) -> dict[str, Any]:
        error: dict[str, Any] = {
            "code": code,
            "message": str(redact_control_plane_secrets(message)),
        }
        if data is not None:
            error["data"] = redact_control_plane_secrets(data)
        return {"jsonrpc": "2.0", "id": request_id, "error": error}
