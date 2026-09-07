# Dashboard read model

The Control Plane exposes a single, versioned, read-only operator contract:

- `GET /api/v1/dashboard/v1/snapshot`
- `GET /api/v1/dashboard/v1/history?after=<cursor>&limit=<n>`
- `GET /api/v1/dashboard/v1/stream`
- `GET /api/v1/dashboard/v1/work-history?work=<reference>&before=<reference>&limit=<n>`

## Reading and information hierarchy

The Dashboard answers three everyday questions: what is the objective, what
did the Worker report, and who needs to act next. It uses the existing
server-owned Worker categories and completion criteria. Presentation does not
change the status-request clock, recovery capabilities, delivery authority,
Worker lifecycle or close contract.

| Information | Presentation and meaning |
| --- | --- |
| Objective and latest report | `objective_text` and `latest_report_text` preserve the complete redacted text and paragraph breaks. Short content is immediately visible; long content uses native, keyboard-operable disclosure. The 280-character summary fields remain compatibility previews, never the source of the expanded body. |
| Current progress and next step | Show available Worker-declared values and one current action label. Never repeat protocol state, trajectory and attention owner as equivalent user-facing status rows. |
| Verified completion | Lead with CAO review completion and its timestamp. Later requester acceptance or close bookkeeping is diagnostic history, not an active request for input. A completion claim remains distinct from review. |
| Recovery and automatic supervision | Show an exceptional notice only for an actually present, allowlisted recovery capability. Details contain the matching notification evidence; recovery is never a menu of suggested lifecycle operations. |
| Status requests and runtime clocks | Diagnostic detail only, when applicable to current Work. A status response deadline is a system-owned stall detection threshold, not a promise that an LLM measures elapsed time. Heartbeats, task activity and status responses remain different facts. |
| Connection and models | Collapsed details with effective model/effort once. Show requested values only when different. |
| Missing, zero and obsolete fields | Do not fill the main view with unavailable rows, zero unresolved counts, completed Work's old status requests, or recovery fields without a current recovery action. |

Expanded text keeps its source text as text nodes; it never executes HTML or
Markdown from a report. The existing locator, credential and internal-identity
redaction applies to the entire string before it reaches any client. Full text
does not grant access to artifact bodies or provider transcripts. Native/text
snapshot consumers receive the same full-text fields. The read edge accepts
bounded JSON responses up to 16 MiB so ordinary long reports do not hit the
former preview-sized response ceiling.

The work-history reader is one bounded, scrollable panel with a compact work
index and a selected work's objective, latest report and chronological
exchanges. It loads 20 work records and 30 exchanges at a time in the browser;
explicit older-page controls expose all retained pages. The index filter is
clearly scoped to loaded records. Selection, disclosures, focus and scroll
remain stable during unrelated SSE snapshot updates. A separate refresh action
updates the reader; slow, failed or out-of-order history requests cannot block
current Worker updates or replace the selected work with a different result.

### Work-history read contract

An empty `work` parameter lists work records in reverse creation order.
`work=<reference>` returns that exact Work and chronological exchanges; `before`
continues the same list toward older records. Limits are 1–100, default 20.
References are deterministic, domain-separated SHA-256 display handles. They
are not database IDs, credentials, Worker command targets, or authority.
Unknown/hidden Work and invalid positions return a bounded error. Each read
checks the Work's immutable production scope. Current principal inventory is
not historical scope: Finish/Delete may remove an active Worker label while
its production Work remains readable with a generic Worker label. This never
puts it back into any active Worker category or count.

Each request opens a fresh SQLite read transaction. Opaque references are
resolved from a minimal retained index; only the selected Work or requested
index page uses the canonical Work projection. Work details and exchanges
share that snapshot, so a concurrent report cannot produce a newer conversation
beside older status fields. History does not rebuild global health audits,
Worker/runtime inventories or event watermarks. There is no history cache.
The snapshot and history share the same Work and close-proof builder, including
cross-Work duplicate-receipt checks; faster reads do not relax completion or
recovery semantics. Regression checks enforce bounded projection work, fresh
reads and exact canonical DTO equivalence independently of machine timing.
Finite Dashboard and readiness reads use the HTTP framework's worker pool;
SSE replay also moves its synchronous database read off the event loop. An
in-progress full snapshot or integrity check must not hold up an independent
history selection. A concurrency regression holds each competing reader open
and requires the selected Work to return before that reader is released.

Responses contain only `format`, `items`, `work`, `entries`, `has_more`, and
`next_before`. Index items allow `history_reference`, `work_title`,
`worker_label`, `completed_at`, and `state`; selected `work` uses the same Work
DTO as the snapshot. Exchanges allow only `reference`, `kind`, `occurred_at`,
`text`, and a bounded `outcome`. Sources are Goal revisions, exact-Work/Attempt
Worker reports, CAO instructions, CAO reviews, requester decisions and Boundary
dispositions. Earlier Attempts and revisions remain visible. Event identity
and sequence break timestamp ties; no generic event payload becomes text.
Automatically captured output contributes only its typed report notification
with a matching capture receipt, never the underlying provider transcript.
The edge independently re-applies the allowlist and redaction. No command,
artifact read, filesystem locator, private note store or mutation is added.

Every dashboard surface, including Web, text, and native Codex clients,
consumes this exact JSON representation. Native Codex obtains the same DTO
only through the single MCP resource `cao://dashboard/v1/snapshot`; it has no
Dashboard tools, other resources, event feed, or mutation path. The contract
is an operator visibility edge; it does not provide a conversation or
task-submission UI.

## Authentication and data boundary

Only a `dashboard` principal may call these routes or read the dashboard MCP
resource. A dashboard client must
use a dedicated read-only credential, never a CAO credential. That credential
is rejected from generic REST, dispatcher, A2A, tools, all non-Dashboard MCP
resources, and mutation surfaces; the four versioned routes plus the one
MCP snapshot resource are its only authenticated contracts.
The read model contains a strict `operator` DTO with format
`cao-dashboard-operator/v1`, a locator-free integrity summary, and event
envelopes with only event type, aggregate type, timestamp, and opaque cursor.
It never includes event payloads, actor identifiers, correlations, raw task packets,
metadata, artifact locations, commands, credentials, endpoints, or internal
row identifiers.

The operator DTO is Worker-first. The Control Plane supplies six mutually
exclusive ordered Worker arrays: `cao_processing`, `user_confirmation`,
`stopped_or_failed`, `working`, `ready`, and `inactive_workers`. It also
supplies `recently_completed`, the latest 20 supervisor-verified successful
Work results. The deprecated `needs_attention` array is a compatibility-only
aggregate of the first three Worker arrays; no Dashboard surface renders it or
uses it to infer ownership. Existing `counts.needs_attention` likewise counts
that aggregate, while the other count keys retain their v1 meaning and
`counts.current_work_items` counts only current production Work. Every current
production WorkItem also appears in the compatibility alias `work_items`. The
alias is not an all-time Work ledger and a client must never use it to infer a
Worker's category.

Each Worker record contains only `worker_label`, `attention_reason`, `worker_state`,
`runner_adapter`, `runner_model`, `runner_reasoning_effort`,
`runner_requested_model`, `runner_effective_model`,
`runner_requested_reasoning_effort`, `runner_effective_reasoning_effort`,
`runner_availability`, `runner_connection_state`, and
`current_work_items`. A Worker has one server-owned category. CAO-owned Work
with a live scheduled wake or reasoner lease appears in `cao_processing`;
genuine requester-owned input appears in `user_confirmation`; and an
unscheduled CAO obligation, external blocker, at-risk trajectory, or failed
runner appears in `stopped_or_failed`. Healthy Worker-owned current Work
appears in `working`. Current Work always takes precedence over stale runner
inventory. A supervisor-settled result is not current Work when its latest
Attempt is `completed`, its latest CAO review is `ok`, and it has neither an
open supervisor Boundary nor a recovery action.
This rule applies whether the bookkeeping Work state is `waiting_user` or
`completed`: requester-decision, preservation, cleanup, and explicit-close
records remain durable, but their absence alone is not an active requester
input request. A real requester question remains current because it has an
`input_required` Attempt and/or an open Boundary. With no current Work, old
settled, closed, canceled, or failed Work, dead Delivery counts, reviews, and
status requests cannot promote a Worker into an action category. A settled
success remains visible in `recently_completed`; an active reusable Worker
appears in `ready`, even when its replaceable runtime must reconnect, while a
stopped or revoked Worker appears in `inactive_workers`. A later current Work
makes the Worker actionable again.

`attention_reason` is present only for `cao_processing`,
`user_confirmation`, and `stopped_or_failed`; all other categories carry
`null`. Its value is a bounded, server-owned reason such as `cao-processing`,
`user-action-required`, a failed runner, at-risk progress,
explicit-close work, or `system-reconciliation`. A scheduled or active CAO
supervision obligation takes precedence as `cao-processing`; an unresolved
system reconciliation with no live wake is explicitly abnormal rather than
being collapsed into generic CAO action. It contains no runtime diagnostic
text.

Each nested Work record and each `operator.work_items[]` compatibility record
contains only these allowlisted keys:
`display_label`, `history_reference`, `worker_label`, `work_title`,
`objective_summary`, `objective_text`, `latest_report_text`, `state`,
`attempt_state`, `progress_stage`, `trajectory`, `attention_owner`,
`next_boundary_summary`, `pending_supervisor_boundary`, `recovery_action`,
`recovery_waiting_since`, `recovery_notification_state`,
`cao_supervision_state`, `cao_supervision_updated_at`, `latest_report_kind`,
`latest_report_summary`, `latest_reported_at`,
`runtime_heartbeat_at`, `last_worker_activity_at`, `last_artifact_at`,
`status_request_state`, `status_requested_at`, `status_response_due_at`,
`status_responded_at`, `completion_contract`, `delivery_state`,
`runner_adapter`, `runner_model`,
`runner_reasoning_effort`, `runner_requested_model`,
`runner_effective_model`, `runner_requested_reasoning_effort`,
`runner_effective_reasoning_effort`, `runner_availability`, `runner_state`,
`runner_connection_state`,
`latest_cao_review_decision`, `requester_decision`, `closure_state`, `completed_at`,
`closure_summary`, `availability`, and the v1 compatibility aliases `stage`,
`next_observable_boundary`, and `latest_worker_report_summary`.
`display_label` is a stable ordinal label. `worker_label` is a bounded,
redacted operator label and falls back to a generic stable label when an
owner-private legacy identity cannot be proven; neither label is a database
identifier. `runtime_delivery` contains aggregate runtime and delivery counts
only and is a lower-level technical summary, not a source of Worker category
membership. Internal launch-failure classifications and scheduling timestamps
are not Dashboard fields. Ordinary target availability is reported separately
by the managed Worker catalog as `available` or `temporarily_unavailable`.

`attempt_state` is the protocol lifecycle state. `progress_stage` and
`next_boundary_summary` are the bounded, locator-redacted values from the
current Attempt's latest structured Worker report. They are not reconstructed
from a terminal or inferred from silence. `pending_supervisor_boundary` is a
separate boolean: it means an explicit Worker question, blocker, or completion
boundary awaits its admitted action, or an unresolved system-reconciliation
Boundary remains under operator attention. A Boundary-bearing notification
cannot be handled until that Boundary is disposed or superseded, so a handled
notification can no longer strand an open Boundary. The field must never
replace the Worker's declared next milestone. `latest_report_kind`,
`latest_report_summary`, and
`latest_reported_at` identify the most recent current-Attempt structured Worker
handoff and make an old report distinguishable from current activity. Raw
adapter output and terminal transcripts remain outside this contract.

Recovery and automatic-supervision visibility use five separate, allowlisted
Work fields:

- `recovery_action` is `dispose_continue_or_correct`,
  `reconcile_continue_same_thread`, `system_reconciliation`, or `null`
  when no supported open recovery Boundary is present;
- `recovery_waiting_since` is the bounded timestamp of the oldest unresolved
  Boundary represented by the current recovery state; and
- `recovery_notification_state` is the latest exact recovery notification's
  bounded Delivery state: `queued`, `leased`, `dispatched`, `delivered`,
  `acknowledged`, `handled`, `dead`, or `null` when unavailable;
- `cao_supervision_state` is `scheduled` when an exact live Delivery has been
  admitted to the same attached CAO conversation, `active` only while an
  unexpired reasoner turn owns the Boundary, `unscheduled` when the obligation
  has neither, or `null` when no CAO-owned Boundary exists; and
- `cao_supervision_updated_at` is the timestamp of that exact Delivery or
  reasoner-turn state.

The three `recovery_*` fields have matching `availability` entries. The two
`cao_supervision_*` fields are direct derived state and timestamp values; all
five pass the same enum, timestamp, and locator-redaction gates as the rest of
the v1 DTO. Notification state is delivery evidence, not Boundary lifecycle
authority. The Control Plane automatically creates a replacement wake for a
legacy handled/missing notification, for each actual expired or incomplete CAO
turn, and when a new authentic connection or wake runtime makes a definitively
dead internal wake actionable again. A provider-accepted wake is not replaced;
the Dispatcher matches its stable client-message identity and resumes its exact
`notLoaded` Desktop thread without submitting another message. If the provider
has consumed that item but the thread ends before MCP acquisition, the
Delivery remains `scheduled` through one durable observation interval and one
idempotent successor wake. Thus the Worker stays in `cao_processing` while
automatic recovery remains live. A conversation reconnect cannot reactivate a
semantically superseded predecessor beside that successor. It emits nothing
merely because a periodic scan saw unchanged state. Only a separately admitted
Boundary disposition or supersession clears the obligation.

`runtime_heartbeat_at`, `last_worker_activity_at`, and `last_artifact_at` are
independent durable clocks. `status_request_state`, `status_requested_at`,
`status_response_due_at`, and `status_responded_at` describe the latest explicit
CAO liveness request.
For a managed Worker, `runtime_heartbeat_at` is shown only when the heartbeat
belongs to the consumed launch-ticket generation explicitly bound to the
current Attempt. A heartbeat from an earlier headless process is unavailable,
even if its timestamp is recent.
The state is `pending`, `overdue`, or `responded`; a heartbeat does not count as
task activity, an artifact does not count as a status response, and no client
may collapse these fields into a single inferred “running” indicator.
If an explicit status deadline expires while the exact Assignment is still
provably `queued` and no Worker report, artifact, or current-generation
heartbeat exists, the Dispatcher converts that pre-dispatch stall into one
audited `runtime_dispatch_failed` recovery Boundary. A leased or dispatched
Assignment is never reinterpreted this way because its handoff outcome may be
in progress or unknown.

`completion_contract` and `delivery_state` are bounded Control Plane values,
not inferences from a completion label. `completion_required` Work remains
`delivery_missing` until a digest-bound artifact is registered; the Dashboard
therefore distinguishes a completion claim from an inspectable handoff.

Only `production` principals and WorkItems enter the six Worker arrays,
`recently_completed`, or their counts. `inactive_workers` represents stopped
or revoked production Worker inventory and may additionally contain explicitly
migrated history. It never contributes to the active Worker or current Work
counts. `acceptance-test`, `system`, and `unclassified` records are absent from
every operator array, count, compatibility alias, and history/event result.
Filtering and categorization happen in the Control Plane projection; Web,
text, and native clients render the supplied arrays without inspecting labels,
project names, state strings, timestamps, or metadata.

A managed Worker thread in either `archived` or `legacy_stopped` state is
excluded before Dashboard categorization. It appears in none of `working`,
`ready`, `cao_processing`, `user_confirmation`, `stopped_or_failed`, or
`inactive_workers`, contributes to no Worker or current-Work count, and cannot
be promoted by retained Work, Delivery, runtime, status, or close history.
Resume restores an archived logical thread to normal classification; Delete
keeps its retained audit history outside the operator Worker projection. A
`legacy_stopped` thread is non-resumable and remains outside the Dashboard
after conversation close or migration.

A production `managed_worker_thread` lifecycle event is nevertheless an
operator-visible refresh trigger. In particular, `managed_worker_thread.finished`
wakes an already-open Dashboard so it replaces a pre-Finish Worker card with a
fresh snapshot in which the archived thread is absent. The event envelope
contains no Worker identifier, and its production scope is resolved through
the retained thread, managed specification, and principal records. Missing or
non-production scope fails closed. This visibility is only a redraw signal; it
does not put an archived or legacy-stopped thread back into any Dashboard
category or remove an unrelated Worker that still needs attention.

`closure_state` has exactly three values: `open`, `awaiting-explicit-close`,
and `closed`. The Dashboard preserves a valid canonical `closure_state`; it
never derives `closed` from a terminal WorkItem state. For a legacy projection
row with no closure field, `completed` is rendered as
`awaiting-explicit-close`, while other states remain `open`. Closure evidence
does not decide active-card membership by itself: both a supervisor-settled
result awaiting later bookkeeping and a fully `closed` WorkItem are no longer
current. They do not appear in a Worker's `current_work_items` or the flat
compatibility alias. A successful supervisor-settled result appears in
`recently_completed` with its bounded `completed_at` timestamp even while later
requester-decision or close bookkeeping remains pending. Other durable
decision, close, and event history stays canonical but outside the recent
success list. A Worker with no later current Work is categorized from its own
current lifecycle (`ready` when reusable, `inactive_workers` when stopped or
revoked), never from old Work history.

`closure_summary` is the only per-WorkItem close-evidence field. Its
allowlisted, actionable values are requester decision
(`accepted`/`rejected`/`pending`), CAO review (`ok`/`needs-work`/`pending`), artifact preservation
(`preserved`/`pending`), cleanup (`verified`/`pending`/`unknown`), and optional
non-negative counts for unresolved deliveries/effects and active runtimes.
It contains no decision text, paths, cleanup targets, artifact locators,
payloads, evidence identifiers, tokens, or database identifiers.

The current canonical projection supplies `closure_state` and
`closure_summary` for every WorkItem. They are derived from durable requester
decision, review, close-receipt, artifact, cleanup, delivery, effect, and
scoped-runtime records. The `completed` → `awaiting-explicit-close` fallback
applies only when a legacy row has no closure field; it never infers a receipt.

The canonical projection supplies bounded, deterministic, redacted title and
objective text from the current CAO-authored Goal and the kind, timestamp,
summary, stage, and next boundary of the latest current-Attempt Worker MCP
report. Runner details are supplied only by
one matching `managed_worker_specs` row: its Worker principal and runtime must
match the current Attempt, and its CAO attachment and attachment generation
must match the owning WorkItem. The DTO exposes the adapter, requested and
effective model, requested and effective reasoning effort, plus an explicit
availability and spec state. `runner_model` and `runner_reasoning_effort` are
compatibility aliases for the effective values.

`runner_connection_state` is a separately bounded durable lifecycle label:
`enrolling`, `connected-idle`, `connected-busy`, `enrolled-reopenable`,
`stopped`, `failed`, `missing`, or `unavailable`. It is intentionally not a
PID, terminal, or process claim. In particular, `enrolled-reopenable` means a
previously verified Worker identity and native conversation can receive a
fresh, fenced App Server/MCP launch; it does not imply an idle OS process is
being retained.

Missing, duplicate, attachment-mismatched, malformed, unsupported, stopped,
or revoked specs fail closed: the runner detail fields are unavailable and
`runner_state` records the bounded reason (`unsupported`, `mismatched`,
`invalid`, `stopped`, `revoked`, or `unavailable`). A Worker with a
managed-capable runtime but no spec is therefore displayed as
`runner_availability: unavailable`, `runner_state: unavailable`; no field is
guessed from runtime metadata, native sessions, messages, event payloads, or
identifiers. The projection redacts credential-like values, paths, URIs, and
internal-ID labels before the versioned DTO is built.

## Snapshot and cursor contract

The snapshot contains the authority mode and generation, a durable cursor, and
a deterministic digest over the complete response payload.  A cursor is
versioned and bound to the authority generation and durable event sequence.
Its integrity value detects accidental corruption; it is not an authentication
credential and remains protected by the dashboard principal boundary.

Clients fetch a snapshot, retain its cursor, then request history or open an
SSE stream using `Last-Event-ID`.  Each emitted `dashboard-update` carries the
next cursor.  Reconnecting with that cursor replays only later durable events,
without duplicates.

## Resynchronization

If an authority generation changes, a cursor is older than retained history,
or it is ahead of the durable log, the history endpoint returns HTTP 409 with
`status: "resync-required"`.  The stream emits the same value as a
`resync-required` SSE event and immediately closes.  The client must discard
its cursor and fetch a new snapshot.

SSE heartbeat frames are comments only (`: heartbeat`); they do not advance a
cursor, represent a state transition, fetch a snapshot, or redraw the
Dashboard. Clients may request a bounded stream
with `limit` for controlled reconnects or verification; an omitted limit keeps
the stream open.

Within one Control Plane process, a successful database commit wakes waiting
dashboard streams immediately. The stream otherwise waits for the heartbeat
period, then performs one durable recovery scan before emitting a heartbeat.
This bounded recovery scan is intentionally retained for another process that
commits to the same SQLite database: process-local notifications do not make a
cross-process delivery guarantee. No worker runtime, terminal, or screen is
inspected by this mechanism.
