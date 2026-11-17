"""Public status vocabulary shared by Dashboard producers and presentation edges.

Both boundaries still validate incoming values. A single vocabulary keeps the
read-only Web/text edge from silently dropping a supported supervisor state.
"""

CAO_SUPERVISION_STATES = frozenset({"scheduled", "active", "unscheduled", "connection_required"})
ATTENTION_REASONS = frozenset(
    {
        "user-action-required",
        "cao-action-required",
        "cao-processing",
        "cao-connection-required",
        "external-action-required",
        "progress-at-risk",
        "awaiting-explicit-close",
        "system-reconciliation",
        "runner-failed",
        "runner-missing",
        "runner-stopped",
    }
)

CURRENT_WORK_STATES = frozenset(
    {"open", "active", "suspended", "waiting_supervisor", "waiting_review",
     "waiting_user", "user_needed", "completed"}
)


def is_successful_completion(
    *,
    state: object,
    attempt_state: object,
    latest_cao_review: object,
    pending_supervisor_boundary: bool,
    recovery_action: object,
) -> bool:
    """Classify a settled result identically during selection and presentation."""

    return bool(
        state in {"waiting_user", "completed"}
        and attempt_state == "completed"
        and latest_cao_review == "ok"
        and not pending_supervisor_boundary
        and recovery_action is None
    )
