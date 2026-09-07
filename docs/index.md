# CAO documentation map

This page is the entry point for agents and maintainers. Read only the row
needed for the current operation, then follow its canonical document. Avoid
loading the whole documentation tree into one model turn.

| Need | Canonical document | Implementation | Primary regression | Update when |
| --- | --- | --- | --- | --- |
| Install a private local macOS + Codex instance or export public source | [README quickstart](../README.md#install-on-macos-with-codex-desktop) | `local_setup.py`, `cli.py`, `tools/check_publication.py` | `test_local_setup.py`, `test_public_codex_install.py`, `test_publication.py` | installation, local serving, credential generation or distribution boundaries change |
| Classify feedback as a system gap, operator mistake, or task-local issue | [CAO operator runbook](cao-operator-runbook.md#classify-feedback-before-recording) | `AGENTS.md`; owner-private `ops-log/README.md` | `test_agent_knowledge_contract.py` | feedback is recorded in the wrong place or CAO-improvement work crosses into an individual task |
| Choose the correct CAO lifecycle or recover a failed operation | [CAO operator runbook](cao-operator-runbook.md) | `mcp.py`, `service.py`, `dashboard_lifecycle.py` | `test_agent_knowledge_contract.py`, `test_review_before_disposition_and_runtime_recovery.py` | an operator chooses the wrong lifecycle or a failure has no safe next action |
| Recall earlier experience and explicitly pause/resume one Work | [CAO operator runbook](cao-operator-runbook.md#recall-experience-before-deciding) | `supervision_memory.py`, `supervision_control.py`, `memory_import.py`, `service.py` | `test_supervision_memory.py`, `test_supervision_pause.py`, `test_memory_import.py` | memory is not available to CAO judgment or a pause loses its exact quiet/resumption fence |
| Attach a CAO conversation and use MCP/A2A | [Protocols](protocols.md) | `mcp.py`, `api.py`, `a2a.py` | `test_protocols.py`, `test_transport_semantics_matrix.py` | a public tool, catalog, transport, or role changes |
| Maintain owner-local attachment admission and replaceable connection fencing | [Attachment bootstrap capability](attachment-bootstrap-capability.md) | `attachment_issuer.py`, `mcp_stdio_proxy.py`, `service.py` | `test_cao_auto_attachment.py`, `test_transport_semantics_matrix.py` | CAB issuance, CSC exchange, peer identity, connection epochs, or reattachment changes |
| Create a Worker in an existing Directory | [Managed Worker provisioning](managed-worker-provisioning.md) | `private_policy.py`, `service.py`, `runtime.py` | `test_worker_thread_public_control_contract.py` | placement, runner profile, launch, or recovery changes |
| Resolve a Directory without exposing owner-private paths | [Owner-private placement policy](owner-private-policy.md) | `private_policy.py` | `test_private_policy.py` | Directory identity, runner placement, registry integrity, or path redaction changes |
| New, Close (Finish), Resume, or Delete one Worker thread | [Managed Worker thread lifecycle](managed-worker-thread-lifecycle.md) | `models.py`, `service.py`, `mcp.py`, `runtime.py` | `test_worker_thread_lifecycle_core.py`, `test_worker_thread_delete_authority.py` | archive, terminal authority, cross-conversation scope, epoch, or preservation semantics change |
| Interpret Dashboard state | [Dashboard read model](dashboard-read-model.md) | `projection.py`, `dashboard.py` | `test_dashboard_read_model.py`, `test_agent_knowledge_contract.py` | grouping, attention, recovery visibility, status, or delivery fields change |
| Check serving readiness or diagnose Dispatcher, effect, and Delivery state | [Operations](operations.md#health-readiness-and-diagnosis) | `api.py`, `cli.py`, `runtime.py` | `test_api.py`, `test_cli_doctor.py` | readiness fields, cycle health, or blocking/degraded diagnostic conditions change |
| Restart or update the shared CAO system | [Dashboard operator edge](dashboard-operator-edge.md) | `dashboard_lifecycle.py`, `dashboard_cli.py` | `test_dashboard_lifecycle.py` | deployment, backup, fencing, or readiness changes |
| Preserve artifacts and close Work | [Explicit close contract](explicit-close-contract.md) | `service.py`, `close_contract.py` | `test_explicit_close_contract.py` | review, preservation, cleanup, or requester decision changes |
| Close one CAO conversation without changing project-local Worker lifecycle | [Explicit close contract](explicit-close-contract.md#commands-and-authority) | `models.py`, `service.py`, `mcp.py` | `test_explicit_close_contract.py`, `test_worker_thread_delete_authority.py` | conversation-owned Work cancellation, attachment revocation, close blockers, or result counts change |
| Understand authority and trust boundaries | [Architecture](architecture.md) and [Security](security.md) | Control Plane modules | `test_projection.py`, security-focused tests | authority, credentials, private state, or effects change |

The owning role for these contracts is **Control Plane maintainers**. Private
incident records are evidence for improving these contracts, never a source to
copy identifiers, paths, prompts, or credentials from into tracked files.
Current behavior belongs in the operation-specific canonical document above;
chronological incident history belongs only in the ignored owner-private
`ops-log/` tree. There is intentionally no tracked generic improvement log.
