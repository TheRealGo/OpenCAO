from __future__ import annotations

from enum import StrEnum
from pathlib import Path
from typing import Annotated, Any, Literal

from pydantic import BaseModel, ConfigDict, Field, HttpUrl, field_validator, model_validator


class PrincipalRole(StrEnum):
    CAO = "cao"
    WORKER = "worker"
    USER = "user"
    EXTERNAL = "external"
    DASHBOARD = "dashboard"


class GoalMaturity(StrEnum):
    UNSET = "unset"
    EXPLORING = "exploring"
    DEFINED = "defined"


class CompletionContract(StrEnum):
    """Sealed expectation for the user-facing result of one WorkItem.

    ``legacy_unclassified`` keeps pre-contract callers compatible.  The
    managed target facade assigns an explicit contract for every new task so
    an artifact-producing task cannot reach requester acceptance without an
    inspectable delivery.
    """

    LEGACY_UNCLASSIFIED = "legacy_unclassified"
    COMPLETION_REQUIRED = "completion_required"
    NO_ARTIFACT_EXPECTED = "no_artifact_expected"


class WorkState(StrEnum):
    OPEN = "open"
    ACTIVE = "active"
    SUSPENDED = "suspended"
    WAITING_SUPERVISOR = "waiting_supervisor"
    WAITING_REVIEW = "waiting_review"
    WAITING_USER = "waiting_user"
    USER_NEEDED = "user_needed"
    COMPLETED = "completed"
    CANCELED = "canceled"
    FAILED = "failed"


class AttemptState(StrEnum):
    ASSIGNED = "assigned"
    ACCEPTED = "accepted"
    WORKING = "working"
    SUSPENDED = "suspended"
    WAITING_SUPERVISOR = "waiting_supervisor"
    INPUT_REQUIRED = "input_required"
    BLOCKED = "blocked"
    SUBMITTED = "submitted"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELED = "canceled"


class RuntimeState(StrEnum):
    STARTING = "starting"
    READY = "ready"
    BUSY = "busy"
    WAITING = "waiting"
    STOPPED = "stopped"
    FAILED = "failed"
    MISSING = "missing"


class EnrollmentState(StrEnum):
    """Durable lifecycle states for a managed Worker MCP enrollment."""

    AWAITING_HANDSHAKE = "awaiting_handshake"
    READY = "ready"
    STALE = "stale"
    REVOKED = "revoked"
    FAILED = "failed"


class WorkerThreadState(StrEnum):
    """CAO-logical lifecycle state, independent of provider-native storage."""

    ACTIVE = "active"
    ARCHIVED = "archived"
    LEGACY_STOPPED = "legacy_stopped"


class Trajectory(StrEnum):
    UNTRACKED = "untracked"
    ADVANCING = "advancing"
    AT_RISK = "at_risk"
    STALLED = "stalled"
    DRIFTING = "drifting"
    COMPLETE = "complete"


class EvidenceConfidence(StrEnum):
    UNKNOWN = "unknown"
    DECLARED = "declared"
    OBSERVED = "observed"
    VERIFIED = "verified"


class AttentionOwner(StrEnum):
    NONE = "none"
    WORKER = "worker"
    CAO = "cao"
    USER = "user"
    EXTERNAL = "external"


class MessageKind(StrEnum):
    ASSIGNMENT = "assignment"
    INSTRUCTION = "instruction"
    QUESTION = "question"
    BLOCKER = "blocker"
    PROGRESS = "progress"
    ARTIFACT = "artifact"
    COMPLETION_CLAIM = "completion_claim"
    STATUS_REQUEST = "status_request"
    REVIEW = "review"
    USER_INPUT = "user_input"
    CANCEL = "cancel"
    SYSTEM = "system"


class DeliveryState(StrEnum):
    QUEUED = "queued"
    LEASED = "leased"
    DISPATCHED = "dispatched"
    DELIVERED = "delivered"
    ACKNOWLEDGED = "acknowledged"
    HANDLED = "handled"
    DEAD = "dead"


class DeliveryReactivationPolicy(StrEnum):
    """Whether a dead Delivery may be re-armed by runtime availability alone."""

    TERMINAL = "terminal"
    RETRYABLE = "retryable"


class IntentKind(StrEnum):
    TASK = "task"
    DIRECTIVE = "directive"
    NOOP = "noop"


class IntentRelation(StrEnum):
    INDEPENDENT = "independent"
    CONTINUE = "continue"
    AUGMENT = "augment"
    INTERRUPT = "interrupt"
    REPLACE = "replace"
    CANCEL = "cancel"


class DirectiveState(StrEnum):
    PENDING = "pending"
    HANDLED = "handled"
    SUPERSEDED = "superseded"
    CANCELED = "canceled"


class BoundaryKind(StrEnum):
    DIRECTIVE = "directive"
    IDLE = "idle"
    QUESTION = "question"
    BLOCKER = "blocker"
    COMPLETION = "completion"
    WORKER_OUTPUT = "worker_output"
    REVIEW_REJECTED = "review_rejected"
    USER_REJECTION = "user_rejection"
    RETRY_REQUEST = "retry_request"
    EXIT = "exit"
    FAILURE = "failure"


class BoundaryDispositionKind(StrEnum):
    CONTINUE = "continue"
    CORRECT = "correct"
    RETRY = "retry"
    WAIT_USER = "wait_user"
    PAUSE = "pause"
    ACCEPT = "accept"
    CANCEL = "cancel"
    FAIL = "fail"


class ReasonerTurnState(StrEnum):
    LEASED = "leased"
    COMPLETED = "completed"
    ABANDONED = "abandoned"


class ReviewVerdict(StrEnum):
    PENDING = "pending"
    OK = "ok"
    NEEDS_WORK = "needs_work"
    ACCEPTED = "accepted"
    REJECTED = "rejected"


class EffectKind(StrEnum):
    LOCAL = "local"
    EXTERNAL = "external"
    DESTRUCTIVE = "destructive"


class EffectStatus(StrEnum):
    STARTED = "started"
    SUCCEEDED = "succeeded"
    NOT_APPLIED = "not_applied"
    FAILED = "failed"
    UNKNOWN = "unknown"


class ReportKind(StrEnum):
    PROGRESS = "progress"
    QUESTION = "question"
    BLOCKER = "blocker"
    ARTIFACT = "artifact"
    COMPLETION_CLAIM = "completion_claim"


class APIModel(BaseModel):
    model_config = ConfigDict(extra="forbid", populate_by_name=True)


def _clean_conditions(value: list[str]) -> list[str]:
    return [item.strip() for item in value if item.strip()]


def _clean_target(value: str) -> str:
    target = value.strip()
    if not target or "\x00" in target or "\n" in target or "\r" in target:
        raise ValueError("target must be a bounded name or alias")
    return target


def _absolute_working_directory(value: str) -> str:
    if "\x00" in value or "\n" in value or "\r" in value:
        raise ValueError("working_directory is invalid")
    expanded = Path(value).expanduser()
    if not expanded.is_absolute():
        raise ValueError("working_directory must be absolute")
    return str(expanded)


class PrincipalCreate(APIModel):
    name: str = Field(min_length=1, max_length=128)
    role: PrincipalRole
    operator_scope: Literal["production", "acceptance-test", "system", "unclassified"] = (
        "unclassified"
    )
    operator_label: str = Field(default="", max_length=128)
    metadata: dict[str, Any] = Field(default_factory=dict)

    @model_validator(mode="after")
    def validate_operator_identity(self) -> PrincipalCreate:
        if self.role != PrincipalRole.WORKER:
            if self.operator_scope != "unclassified" or self.operator_label:
                raise ValueError("operator identity is valid only for Worker principals")
            return self
        if self.operator_scope in {"production", "acceptance-test"}:
            label = self.operator_label.strip()
            if not label or "\x00" in label or "\n" in label or "\r" in label:
                raise ValueError("visible Worker scope requires a bounded operator label")
            self.operator_label = label
        elif self.operator_label:
            raise ValueError("unclassified or system Worker cannot publish an operator label")
        return self


class PrincipalTokenRotate(APIModel):
    disable_previous: bool = True


class RuntimeRegistration(APIModel):
    adapter: Literal["codex-app-server", "claude", "subprocess", "webhook"]
    endpoint: str = ""
    native_session_id: str = ""
    lease_seconds: int = Field(default=86400, ge=15, le=86400)
    # None preserves inference from the authenticated runtime enrollment.
    managed_mcp: bool | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)


class ProvisionManagedWorkerInput(APIModel):
    """Attachment-scoped request to create one managed Worker lifecycle.

    This is intentionally narrower than the administrator principal/runtime
    APIs: the caller selects a server-owned profile and opaque workspace
    reference, never a command, cwd, token, endpoint, or environment.
    """

    worker_profile_id: str = Field(min_length=1, max_length=64, pattern=r"^[a-z][a-z0-9_-]{0,63}$")
    adapter: Literal["codex-app-server", "claude"]
    workspace_ref: str = Field(
        min_length=1, max_length=128, pattern=r"^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$"
    )
    model: str = Field(min_length=1, max_length=128, pattern=r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$")
    reasoning_effort: Literal["low", "medium", "high", "xhigh", "max", "ultra"]
    operator_scope: Literal["production", "acceptance-test"]
    operator_label: str = Field(min_length=1, max_length=128)
    idempotency_key: str = Field(min_length=1, max_length=256)

    @field_validator("operator_label")
    @classmethod
    def validate_operator_label(cls, value: str) -> str:
        label = value.strip()
        if not label or "\x00" in label or "\n" in label or "\r" in label:
            raise ValueError("operator label must be a bounded display label")
        return label


class WorkerThreadLifecycleInput(APIModel):
    """Command for one logical Worker thread.

    The optional generation is an advanced compare-and-swap guard. Ordinary
    CAO control does not need to pre-read a generation before issuing a
    lifecycle command; SQLite still serializes the mutation atomically.
    """

    worker_thread_id: str = Field(pattern=r"^[A-Za-z0-9][A-Za-z0-9_-]{0,127}$")
    expected_generation: int | None = Field(default=None, ge=1)
    idempotency_key: str = Field(min_length=1, max_length=256)


class DeleteWorkerThreadInput(WorkerThreadLifecycleInput):
    """Delete one exact logical Worker; this call is the terminal authority."""


class ResumeWorkerThreadInput(WorkerThreadLifecycleInput):
    """Resume one archived Worker without creating Work.

    Task assignment is intentionally a separate ``InstructWorkerThreadInput``
    operation so Worker lifecycle and Work lifecycle cannot be coupled again.
    """


class InstructWorkerThreadInput(WorkerThreadLifecycleInput):
    """Record one sealed Work for an exact active managed Worker thread."""

    title: str | None = Field(default=None, min_length=1, max_length=300)
    objective: str = Field(min_length=1)
    maturity: GoalMaturity = GoalMaturity.UNSET
    acceptance: list[str] = Field(default_factory=list)
    non_goals: list[str] = Field(default_factory=list)
    priority: int = Field(default=50, ge=0, le=100)
    completion_contract: CompletionContract = CompletionContract.COMPLETION_REQUIRED
    dependencies: list[AssignmentDependency] = Field(default_factory=list)
    metadata: dict[str, Any] = Field(default_factory=dict)

    @field_validator("acceptance", "non_goals")
    @classmethod
    def clean_conditions(cls, value: list[str]) -> list[str]:
        return _clean_conditions(value)

    @field_validator("dependencies")
    @classmethod
    def unique_dependencies(cls, value: list[AssignmentDependency]) -> list[AssignmentDependency]:
        return list(dict.fromkeys(value))

    @model_validator(mode="after")
    def validate_task_shape(self) -> InstructWorkerThreadInput:
        if self.maturity == GoalMaturity.DEFINED and not self.acceptance:
            raise ValueError("defined work requires at least one acceptance condition")
        if self.completion_contract == CompletionContract.LEGACY_UNCLASSIFIED:
            raise ValueError(
                "Worker-thread instruction requires completion_required or no_artifact_expected"
            )
        return self


class CloseCAOConversationInput(APIModel):
    """Close the exact CAO conversation authenticated by its CSC."""

    idempotency_key: str = Field(min_length=1, max_length=256)


class RuntimeHeartbeat(APIModel):
    state: RuntimeState = RuntimeState.READY
    lease_seconds: int = Field(default=180, ge=15, le=86400)
    expected_enrollment_generation: int | None = Field(default=None, ge=0)
    sequence: int | None = Field(default=None, ge=0)
    metadata: dict[str, Any] = Field(default_factory=dict)


class CAOSessionAttachment(APIModel):
    """Bind one existing CAO conversation to a managed control-plane wake path.

    This is deliberately not a user-message ingress record.  It identifies the
    already-running CAO thread that may be resumed after a durable Worker
    boundary is created by the Control Plane.
    """

    native_thread_id: str = Field(min_length=1, max_length=256)
    project_digest: str = Field(pattern=r"^[0-9a-f]{64}$")
    # These values describe the bridge implementation already loaded in the
    # requesting process. The owner-local issuer binds the same values into its
    # one-use bootstrap capability; the HTTP edge may only repeat them.
    proxy_catalog_digest: str = Field(pattern=r"^[0-9a-f]{64}$")
    proxy_abi_version: int = Field(ge=1, le=2_147_483_647)
    # Resuming an existing Codex thread preserves that thread's own model and
    # sandbox.  These optional diagnostics must never be guessed or used as
    # overrides by the Control Plane.
    model: str = Field(default="", max_length=128)
    sandbox: str = Field(default="", max_length=128)
    # A CAO turn may supervise work for substantially longer than a transient
    # runtime exchange.  Clients may renew this idempotently before expiry;
    # the day-long default prevents a normal task from losing its exact-thread
    # binding merely because an out-of-band heartbeat is not installed yet.
    lease_seconds: int = Field(default=86400, ge=15, le=86400)
    metadata: dict[str, Any] = Field(default_factory=dict)


class AssignmentDependency(StrEnum):
    """Closed, side-effect-free host capabilities required before dispatch."""

    DOCKER_API_PING = "docker_api_ping"


class WorkAssignment(APIModel):
    worker_id: str
    title: str = Field(min_length=1, max_length=300)
    objective: str = Field(min_length=1)
    maturity: GoalMaturity = GoalMaturity.DEFINED
    acceptance: list[str] = Field(default_factory=list)
    non_goals: list[str] = Field(default_factory=list)
    priority: int = Field(default=50, ge=0, le=100)
    completion_contract: CompletionContract = CompletionContract.LEGACY_UNCLASSIFIED
    runtime_session_id: str | None = None
    # Managed assignment authority is the exact logical Worker-thread
    # lifecycle generation.  A runtime is only replaceable delivery state and
    # must never be used to infer this pair.
    managed_worker_thread_id: str | None = Field(default=None, min_length=1, max_length=128)
    managed_worker_thread_generation: int | None = Field(
        default=None,
        ge=1,
        strict=True,
    )
    # A WorkItem is owned by the existing CAO conversation that delegated it.
    # These are an explicit, immutable routing contract, never a hint for
    # selecting a recently active CAO runtime.
    supervisor_attachment_id: str | None = None
    supervisor_project_digest: str | None = Field(default=None, pattern=r"^[0-9a-f]{64}$")
    requester_id: str | None = None
    dependencies: list[AssignmentDependency] = Field(
        default_factory=list,
        exclude_if=lambda value: not value,
        description=(
            "Closed read-only prerequisites checked before Worker dispatch; "
            "request docker_api_ping only when the assigned work requires a ready "
            "local Docker API."
        ),
    )
    metadata: dict[str, Any] = Field(default_factory=dict)
    idempotency_key: str = ""

    @field_validator("acceptance", "non_goals")
    @classmethod
    def clean_conditions(cls, value: list[str]) -> list[str]:
        return _clean_conditions(value)

    @field_validator("dependencies")
    @classmethod
    def unique_dependencies(cls, value: list[AssignmentDependency]) -> list[AssignmentDependency]:
        return list(dict.fromkeys(value))

    @model_validator(mode="after")
    def defined_requires_acceptance(self) -> WorkAssignment:
        if self.maturity == GoalMaturity.DEFINED and not self.acceptance:
            raise ValueError("defined work requires at least one acceptance condition")
        return self

    @model_validator(mode="after")
    def attachment_binding_is_complete(self) -> WorkAssignment:
        if bool(self.supervisor_attachment_id) != bool(self.supervisor_project_digest):
            raise ValueError(
                "supervisor attachment id and project digest must be supplied together"
            )
        if (self.managed_worker_thread_id is None) != (
            self.managed_worker_thread_generation is None
        ):
            raise ValueError("managed Worker thread id and generation must be supplied together")
        return self


class NewWorkerThreadInput(APIModel):
    """Create one empty managed Worker thread in an existing Directory."""

    working_directory: str = Field(min_length=1, max_length=4096)
    runner: Literal["codex", "claude"] = "codex"
    model: str | None = Field(
        default=None,
        min_length=1,
        max_length=128,
        pattern=r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$",
    )
    reasoning_effort: Literal["low", "medium", "high", "xhigh", "max", "ultra"] | None = None
    name: str | None = Field(default=None, max_length=128)
    idempotency_key: str = Field(min_length=1, max_length=256)

    @field_validator("working_directory")
    @classmethod
    def validate_working_directory(cls, value: str) -> str:
        return _absolute_working_directory(value)

    @field_validator("name")
    @classmethod
    def validate_name(cls, value: str | None) -> str | None:
        if value is None:
            return None
        label = value.strip()
        if not label:
            return None
        if "\x00" in label or "\n" in label or "\r" in label or "/" in label or "\\" in label:
            raise ValueError("name must be a bounded display label")
        return label

    @model_validator(mode="after")
    def validate_optional_model(self) -> NewWorkerThreadInput:
        if "model" in self.model_fields_set and self.model is None:
            raise ValueError("model must be omitted or contain a model identifier")
        return self


class SubmittedIntent(APIModel):
    """A durable receipt created only from an authoritative submit event."""

    source_id: str = Field(min_length=1, max_length=500)
    payload: dict[str, Any]
    payload_digest: str = ""


class IntentDisposition(APIModel):
    kind: IntentKind
    relation: IntentRelation | None = None
    target_work_item_id: str | None = None
    assignment: WorkAssignment | None = None
    directive: str = ""
    reason: str = Field(min_length=1)

    @model_validator(mode="after")
    def validate_shape(self) -> IntentDisposition:
        if self.kind == IntentKind.TASK:
            if self.assignment is None:
                raise ValueError("task disposition requires an assignment")
            if self.relation not in {
                IntentRelation.INDEPENDENT,
                IntentRelation.CONTINUE,
                IntentRelation.INTERRUPT,
            }:
                raise ValueError("task disposition requires a task relation")
            if (
                self.relation
                in {
                    IntentRelation.CONTINUE,
                    IntentRelation.INTERRUPT,
                }
                and not self.target_work_item_id
            ):
                raise ValueError("continuation and interrupt require a target work item")
            if self.directive:
                raise ValueError("task disposition cannot contain a directive")
            return self
        if self.kind == IntentKind.DIRECTIVE:
            if self.relation not in {
                IntentRelation.AUGMENT,
                IntentRelation.REPLACE,
                IntentRelation.CANCEL,
            }:
                raise ValueError(
                    "directive disposition requires augment, replace, or cancel relation"
                )
            if not self.target_work_item_id or not self.directive.strip():
                raise ValueError("directive disposition requires a target and content")
            if self.relation == IntentRelation.AUGMENT and self.assignment is not None:
                raise ValueError("augment directive cannot replace the goal")
            if self.relation == IntentRelation.REPLACE and self.assignment is None:
                raise ValueError("replace directive requires the successor goal")
            if self.relation == IntentRelation.CANCEL and self.assignment is not None:
                raise ValueError("cancel directive cannot contain a successor goal")
            self.directive = self.directive.strip()
            return self
        if self.assignment is not None or self.directive or self.target_work_item_id:
            raise ValueError("noop disposition cannot create or target work")
        if self.relation is not None:
            raise ValueError("noop disposition cannot have a relation")
        return self


class GoalRevision(APIModel):
    expected_version: int = Field(ge=1)
    objective: str = Field(min_length=1)
    maturity: GoalMaturity
    acceptance: list[str] = Field(default_factory=list)
    non_goals: list[str] = Field(default_factory=list)
    reason: str = Field(min_length=1)
    idempotency_key: str = ""

    @field_validator("acceptance", "non_goals")
    @classmethod
    def clean_conditions(cls, value: list[str]) -> list[str]:
        return _clean_conditions(value)

    @model_validator(mode="after")
    def defined_requires_acceptance(self) -> GoalRevision:
        if self.maturity == GoalMaturity.DEFINED and not self.acceptance:
            raise ValueError("defined goals require at least one acceptance condition")
        return self


class BoundaryInput(APIModel):
    source_event_id: str = Field(min_length=1, max_length=500)
    work_item_id: str = Field(min_length=1)
    attempt_id: str = Field(min_length=1)
    expected_goal_version: int = Field(ge=1)
    expected_goal_packet_digest: str = Field(pattern=r"^[0-9a-f]{64}$")
    expected_task_packet_digest: str = Field(pattern=r"^[0-9a-f]{64}$")
    expected_generation: int = Field(ge=1)
    kind: BoundaryKind
    summary: str = Field(min_length=1)
    runtime_state: RuntimeState
    metadata: dict[str, Any] = Field(default_factory=dict)


class ReasonerTurnAcquireInput(APIModel):
    boundary_id: str = Field(min_length=1)
    expected_generation: int = Field(ge=1)
    lease_seconds: int = Field(default=180, ge=15, le=86400)
    idempotency_key: str = Field(min_length=1)


class BoundaryDispositionInput(APIModel):
    turn_id: str = Field(min_length=1)
    lease_token: str = Field(min_length=1)
    expected_generation: int = Field(ge=1)
    kind: BoundaryDispositionKind
    reason: str = Field(min_length=1)
    instruction: str = ""
    resume_condition: str = ""
    worker_id: str | None = None
    runtime_session_id: str | None = None

    @model_validator(mode="after")
    def retry_target_is_retry_only(self) -> BoundaryDispositionInput:
        if self.kind != BoundaryDispositionKind.RETRY and (
            self.worker_id is not None or self.runtime_session_id is not None
        ):
            raise ValueError("worker/runtime overrides are valid only for retry")
        if self.kind == BoundaryDispositionKind.WAIT_USER and not self.instruction.strip():
            raise ValueError("wait_user requires the exact requester decision")
        if self.kind == BoundaryDispositionKind.PAUSE:
            if not self.reason.strip():
                raise ValueError("pause requires a concrete reason")
            if self.instruction:
                raise ValueError("pause cannot send a Worker instruction")
        if self.kind in {BoundaryDispositionKind.WAIT_USER, BoundaryDispositionKind.PAUSE}:
            if not self.resume_condition.strip():
                raise ValueError("wait_user and pause require a concrete resume condition")
        elif self.resume_condition:
            raise ValueError("resume_condition is valid only for wait_user or pause")
        return self


class WorkResumeInput(APIModel):
    """Consume one exact CAO-owned pause after an explicit changed-condition decision."""

    expected_generation: int = Field(ge=1, le=2**63 - 1, strict=True)
    pause_boundary_id: str = Field(min_length=1, max_length=128, pattern=r"\S")
    reason: str = Field(min_length=1, max_length=4000, pattern=r"\S")
    instruction: str = Field(min_length=1, max_length=16000, pattern=r"\S")
    resume_evidence: str = Field(min_length=1, max_length=4000, pattern=r"\S")
    idempotency_key: str = Field(min_length=1, max_length=256, pattern=r"\S")


class MemorySearchInput(APIModel):
    """Recall relevant persistent memory through abstractions and cue anchors."""

    query: str = Field(min_length=1, max_length=2000, pattern=r"\S")
    limit: int = Field(default=10, ge=1, le=20, strict=True)
    offset: int = Field(default=0, ge=0, le=2**63 - 1, strict=True)
    related_to: str | None = Field(default=None, pattern=r"^[A-Za-z0-9][A-Za-z0-9_-]{0,127}$")


class MemoryReadInput(APIModel):
    """Read one bounded part of an exact rich-memory revision as untrusted evidence."""

    memory_id: str = Field(pattern=r"^[A-Za-z0-9][A-Za-z0-9_-]{0,127}$")
    expected_revision: int = Field(ge=1, le=2**63 - 1, strict=True)
    character_offset: int = Field(default=0, ge=0, le=2**63 - 1, strict=True)
    max_chars: int = Field(default=8000, ge=1, le=16000, strict=True)


class MemoryWriteInput(APIModel):
    """Record CAO-curated memory without treating it as execution authority."""

    work_item_id: str = Field(pattern=r"^[A-Za-z0-9][A-Za-z0-9_-]{0,127}$")
    primary_abstraction: str = Field(min_length=1, max_length=512, pattern=r"\S")
    cue_anchors: list[Annotated[str, Field(min_length=1, max_length=256, pattern=r"\S")]] = Field(
        max_length=32, strict=True
    )
    value: str = Field(min_length=1, max_length=64000, pattern=r"\S")
    scope: Literal["conversation", "project"] = "conversation"
    memory_id: str | None = Field(default=None, pattern=r"^[A-Za-z0-9][A-Za-z0-9_-]{0,127}$")
    expected_revision: int = Field(default=0, ge=0, le=2**63 - 1, strict=True)
    idempotency_key: str = Field(min_length=1, max_length=256, pattern=r"\S")


class WorkHistoryReadInput(APIModel):
    """Page exact Work observations and decisions across every Goal revision."""

    work_item_id: str = Field(pattern=r"^[A-Za-z0-9][A-Za-z0-9_-]{0,127}$")
    before_sequence: int | None = Field(default=None, ge=1, le=2**63 - 1, strict=True)
    limit: int = Field(default=20, ge=1, le=50, strict=True)


class AckInput(APIModel):
    message_ids: list[str] = Field(min_length=1)


class DeliveryResolveInput(APIModel):
    recipient_id: str = Field(min_length=1)
    outcome: Literal["delivered", "not_delivered", "dead"]
    evidence: str = Field(min_length=1)


class ArtifactInput(APIModel):
    name: str = Field(min_length=1, max_length=300)
    uri: str = Field(min_length=1, max_length=2_098_176)
    media_type: str = "application/octet-stream"
    digest: str = ""
    metadata: dict[str, Any] = Field(default_factory=dict)


class ArtifactContentReadInput(APIModel):
    """Read one bounded text chunk from an exact verified artifact manifest."""

    work_item_id: str = Field(pattern=r"^[A-Za-z0-9][A-Za-z0-9_-]{0,127}$")
    attempt_id: str = Field(pattern=r"^[A-Za-z0-9][A-Za-z0-9_-]{0,127}$")
    artifact_id: str = Field(pattern=r"^[A-Za-z0-9][A-Za-z0-9_-]{0,127}$")
    expected_digest: str = Field(pattern=r"^[0-9a-f]{64}$")
    expected_media_type: str = Field(min_length=1, max_length=200)
    byte_offset: int = Field(default=0, ge=0, le=1_048_576)
    max_bytes: int = Field(default=65_536, ge=4, le=65_536)
    idempotency_key: str = Field(min_length=1, max_length=256)

    @field_validator("expected_media_type")
    @classmethod
    def validate_expected_media_type(cls, value: str) -> str:
        if value != value.strip() or any(
            ord(character) < 0x20 or ord(character) > 0x7E for character in value
        ):
            raise ValueError("expected media type must be bounded printable ASCII")
        return value


class WorkerOutputReadInput(APIModel):
    """Read exact private provider output without granting it authority."""

    work_item_id: str = Field(pattern=r"^[A-Za-z0-9][A-Za-z0-9_-]{0,127}$")
    attempt_id: str = Field(pattern=r"^[A-Za-z0-9][A-Za-z0-9_-]{0,127}$")
    output_id: str = Field(pattern=r"^[A-Za-z0-9][A-Za-z0-9_-]{0,127}$")
    expected_digest: str = Field(pattern=r"^[0-9a-f]{64}$")
    byte_offset: int = Field(default=0, ge=0, le=1_048_576)
    max_bytes: int = Field(default=65_536, ge=4, le=65_536)
    idempotency_key: str = Field(min_length=1, max_length=256)


class ReportInput(APIModel):
    kind: ReportKind
    expected_goal_version: int = Field(ge=1)
    expected_goal_packet_digest: str = Field(pattern=r"^[0-9a-f]{64}$")
    expected_task_packet_digest: str = Field(pattern=r"^[0-9a-f]{64}$")
    expected_generation: int = Field(ge=1)
    summary: str = Field(min_length=1)
    trajectory: Trajectory | None = None
    stage: str = ""
    next_boundary: str = ""
    evidence: list[dict[str, Any]] = Field(default_factory=list)
    artifacts: list[ArtifactInput] = Field(default_factory=list)
    incorporated_message_ids: list[str] = Field(default_factory=list, max_length=1)
    idempotency_key: str = ""

    @field_validator("incorporated_message_ids")
    @classmethod
    def validate_incorporated_message_ids(cls, value: list[str]) -> list[str]:
        for message_id in value:
            if (
                message_id != message_id.strip()
                or not message_id.startswith("msg_")
                or len(message_id) > 128
                or not message_id[4:]
                or not message_id[4:].isalnum()
            ):
                raise ValueError("incorporated message id is invalid")
        return value


class ReplyInput(APIModel):
    message: str = Field(min_length=1)
    in_reply_to: str | None = None
    idempotency_key: str = ""


class StatusRequestInput(APIModel):
    expected_generation: int = Field(ge=1)
    summary: str = Field(
        default="Report current activity, latest completed boundary, and next observable result.",
        min_length=1,
        max_length=1000,
    )
    response_due_seconds: int = Field(default=300, ge=30, le=86400)
    idempotency_key: str = Field(min_length=1, max_length=256)


class ReviewInput(APIModel):
    attempt_id: str
    verdict: ReviewVerdict
    summary: str = Field(min_length=1)
    evidence: list[dict[str, Any]] = Field(default_factory=list)
    idempotency_key: str = ""


class RequesterDecisionInput(APIModel):
    """A requester decision observed and recorded by the attached CAO conversation."""

    review_id: str = Field(min_length=1)
    verdict: Literal["accepted", "rejected"]
    summary: str = Field(min_length=1)
    evidence: list[dict[str, Any]] = Field(default_factory=list)
    conversation_evidence_id: str = Field(
        min_length=1, max_length=128, pattern=r"^[a-zA-Z0-9][a-zA-Z0-9._:-]{0,127}$"
    )
    idempotency_key: str = Field(min_length=1, max_length=256)


class CloseArtifactInput(APIModel):
    artifact_id: str = Field(min_length=1, max_length=128)
    digest: str = Field(pattern=r"^[0-9a-f]{64}$")
    evidence_id: str = Field(
        min_length=1, max_length=128, pattern=r"^[a-zA-Z0-9][a-zA-Z0-9._:-]{0,127}$"
    )


class CloseCleanupInput(APIModel):
    target_kind: Literal[
        "runtime",
        "supervision-registration",
        "workspace",
        "temporary",
        "log",
        "branch",
    ]
    target_fingerprint: str = Field(pattern=r"^[0-9a-f]{64}$")
    action: Literal["stop", "detach", "archive", "trash", "delete"]
    outcome: Literal["succeeded", "not-applied", "unknown"]
    evidence_id: str = Field(
        min_length=1, max_length=128, pattern=r"^[a-zA-Z0-9][a-zA-Z0-9._:-]{0,127}$"
    )
    effect_operation_id: str = Field(default="", max_length=128)
    destructive_authority_evidence_id: str = Field(default="", max_length=128)


class WorkCloseInput(APIModel):
    work_item_id: str = Field(min_length=1)
    attempt_id: str = Field(min_length=1)
    review_id: str = Field(min_length=1)
    requester_decision_id: str = Field(min_length=1)
    expected_goal_version: int = Field(ge=1)
    expected_goal_packet_digest: str = Field(pattern=r"^[0-9a-f]{64}$")
    expected_task_packet_digest: str = Field(pattern=r"^[0-9a-f]{64}$")
    expected_generation: int = Field(ge=1)
    retention_policy_evidence_id: str = Field(
        min_length=1, max_length=128, pattern=r"^[a-zA-Z0-9][a-zA-Z0-9._:-]{0,127}$"
    )
    artifact_manifest_evidence_id: str = Field(
        min_length=1, max_length=128, pattern=r"^[a-zA-Z0-9][a-zA-Z0-9._:-]{0,127}$"
    )
    cleanup_inventory_evidence_id: str = Field(
        min_length=1, max_length=128, pattern=r"^[a-zA-Z0-9][a-zA-Z0-9._:-]{0,127}$"
    )
    close_preparation_id: str = Field(min_length=1, max_length=128)
    artifacts: list[CloseArtifactInput] = Field(default_factory=list)
    cleanup: list[CloseCleanupInput] = Field(default_factory=list)
    idempotency_key: str = Field(min_length=1, max_length=256)


class WorkClosePreparationInput(APIModel):
    """Bind caller-provided retention labels to a server-created close plan."""

    work_item_id: str = Field(min_length=1)
    retention_policy_evidence_id: str = Field(
        min_length=1, max_length=128, pattern=r"^[a-zA-Z0-9][a-zA-Z0-9._:-]{0,127}$"
    )
    artifact_manifest_evidence_id: str = Field(
        min_length=1, max_length=128, pattern=r"^[a-zA-Z0-9][a-zA-Z0-9._:-]{0,127}$"
    )
    idempotency_key: str = Field(min_length=1, max_length=256)


class ExecutePreparedCleanupInput(APIModel):
    """Run cleanup after the owner-private edge preserves canonical artifacts."""

    close_preparation_id: str = Field(min_length=1, max_length=128)
    idempotency_key: str = Field(min_length=1, max_length=256)


class QueryInput(APIModel):
    worker_id: str | None = None
    state: WorkState | None = None
    attention_owner: AttentionOwner | None = None
    limit: int = Field(default=100, ge=1, le=1000)
    cursor: str = ""


class PushNotificationConfigInput(APIModel):
    url: HttpUrl
    token: str = ""
    authentication_scheme: str = "Bearer"
    metadata: dict[str, Any] = Field(default_factory=dict)


class EffectGrantInput(APIModel):
    principal_id: str
    kind: EffectKind
    target_pattern: str = Field(min_length=1)
    action_pattern: str = Field(min_length=1)
    content_digest: str = ""
    argv_digest: str = ""
    workdir_digest: str = ""
    expires_at: str | None = None
    standing: bool = False


class EffectCheckInput(APIModel):
    principal_id: str
    kind: EffectKind
    target: str
    action: str
    content_digest: str = ""
    argv_digest: str = ""
    workdir_digest: str = ""
    # Empty for ordinary effects.  Close cleanup effects must carry all four
    # fields, which makes their authority/evidence non-reusable across work.
    cleanup_work_item_id: str = ""
    cleanup_generation: int | None = Field(default=None, ge=1)
    cleanup_preparation_id: str = ""
    cleanup_target_kind: str = ""
    cleanup_target_fingerprint: str = ""
    cleanup_execution_digest: str = Field(default="", pattern=r"^[0-9a-f]{64}$")

    @model_validator(mode="after")
    def _validate_cleanup_binding(self) -> EffectCheckInput:
        values = (
            self.cleanup_work_item_id,
            self.cleanup_generation,
            self.cleanup_preparation_id,
            self.cleanup_target_kind,
            self.cleanup_target_fingerprint,
            self.cleanup_execution_digest,
        )
        if any(value not in ("", None) for value in values) and not (
            self.cleanup_work_item_id
            and self.cleanup_generation is not None
            and self.cleanup_preparation_id
            and self.cleanup_target_kind
            and self.cleanup_target_fingerprint
            and self.cleanup_execution_digest
        ):
            raise ValueError("cleanup effect binding must be complete")
        return self


class EffectResolveInput(APIModel):
    status: EffectStatus
    evidence: str = Field(min_length=1)


class RuntimeDispatchResult(APIModel):
    success: bool
    native_session_id: str = ""
    state: RuntimeState = RuntimeState.READY
    output: str = ""
    error: str = ""
    metadata: dict[str, Any] = Field(default_factory=dict)
