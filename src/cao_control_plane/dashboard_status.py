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
