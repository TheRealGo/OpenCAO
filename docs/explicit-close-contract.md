# Internal Work close through an attached CAO conversation

## Purpose

`completed` is evidence that the Worker result passed CAO review and that the
requester made a decision. It is **not** permission to disappear from the
operator surface or to remove local state. `closed` is a later, explicit,
durable transition that proves preservation and cleanup for the exact accepted
work generation.

This Work/effect lifecycle is not the requester-facing Worker **Close
(Finish)** operation. Public Worker Close is one
`cao_finish_worker_thread` call for an exact generation; it may stop an active
Worker, terminalizes unsettled Work, preserves unknown evidence, and creates a
resumable archive without requiring requester acceptance or cleanup. The
primitives below remain internal Work completion and separately authorized
cleanup operations. They are never a prerequisite or manual recipe for Worker
Close.

The only interaction topology is:

```text
Requester <-> existing CAO conversation <-> Control Plane <-> Worker
```

The requester does not become an MCP client, and the Control Plane does not
receive a raw requester prompt. After reasoning in the existing CAO
conversation, its attachment-bound credential records the decision as an
audited fact. This is a record of the decision, not impersonation of the
requester.

## Commands and authority

These internal attachment-scoped Work/effect commands require an
attachment-bound CAO conversation credential (`csc_...`):

| Command | Purpose | Caller-supplied identity |
| --- | --- | --- |
| `cao_record_requester_decision` | Record an accepted or rejected requester decision against one reviewed attempt. | reviewed attempt only; the service derives the exact role=`user` requester from its Work, and the public schema never accepts a requester identity |
| `cao_stop_work_runtime` | Stop the completed WorkItem's exact Worker runtime, only when it is not shared by another active WorkItem. | WorkItem only; never an arbitrary runtime id |
| `cao_prepare_work_close` | Freeze the server-owned cleanup inventory for an accepted generation. | work and retention evidence only; never resource locators or outcomes |
| `cao_execute_prepared_cleanup` | Preserve canonical local artifacts and run the owner-private executor for the exact frozen worktree/tmp/log/owned-branch set. | preparation id only; never a path, branch name, command, artifact claim, or cleanup result |
| `cao_close_work` | Record a verified explicit close plan for one accepted work generation. | work/review/decision/packet/artifact-evidence references; never a cleanup result, thread, attachment, project, path, command, or credential |
| `cao_close_conversation` | End supervision owned by this exact CAO conversation, safely canceling its terminalizable nonterminal Work and revoking its attachment credential without changing Worker lifecycle. | idempotency key. The authenticated CSC supplies the exact attachment and generation. |

The credential's durable attachment is the authority boundary. The service
derives the attachment, native conversation, project digest, CAO principal and
current attachment generation from the authenticated actor; it rejects caller
attempts to supply or override any of them. A credential for conversation A
cannot read, decide, close, acknowledge, or resolve a delivery for work bound
to conversation B, even when both conversations use the same CAO principal.

No User-role `cao_user_acceptance` MCP tool, generic
`POST /api/v1/user-acceptances` route, requester MCP runtime, or parallel
requester UI remains on this path. A dashboard is read-only and cannot make
either command available.

`cao_close_work` and `cao_close_conversation` are deliberately different.
The first records preservation and cleanup for one accepted Work generation;
it does not detach CAO. The second ends one conversation's supervision scope;
it does not shut down the shared Control Plane daemon or Dashboard. The same
Codex conversation normally stays attached and can be reused for many Worker
tasks. After an explicit conversation close, the same thread may call
`cao_start` once to reattach with a new credential generation. Reattachment
is not required to restore Workers: project-local Workers never belonged to the
closed attachment and retain their active or archived lifecycle state. Another
or later same-project CAO attachment can list and operate the exact Worker.
Only explicit Worker Finish or Delete changes that state.

The authenticated attachment generation at transaction start is the close
generation. If the same conversation was reattached before close, that one
close retires its still-active Work from every attachment generation up to and
including the current generation. A later exact continuation binds cancel
evidence to the close generation, not to an older Work's generation. Historical
receipts that stopped attachment-owned Workers remain valid evidence of the old
contract, but new close operations always report zero stopped Workers. The CSC is
revalidated inside the close transaction before idempotent replay, so renewal
of the CSC at the same attachment generation fences the older bearer without
allowing any close mutation.

Conversation close is one fail-before-mutation transaction. A leased CAO turn,
ambiguous `dispatched` Delivery or unresolved external effect belonging to that
conversation, interrupt chain, or completed Work that still lacks its formal
Work close receipt blocks the entire operation. Work or runtime state belonging
only to another same-project conversation does not block it. Queued or otherwise
known non-dispatched Deliveries are terminalized, nonterminal Work is canceled,
open Boundaries receive an explicit cancel disposition, the conversation wake
runtime is stopped, and the CSC/ephemeral credentials are revoked. A terminal pre-MCP
runtime-recovery assignment may be closed only under the same strict proof used
by exact-target ensure; its old task packet is never replayed. Past consumed
wake tickets are historical evidence and do not make a conversation permanently
uncloseable.

## Durable requester-decision record

In the same transaction, `cao_record_requester_decision` must:

1. authenticate an active, non-expired `csc_...` credential and lock the
   attachment and reviewed WorkItem;
2. prove that the review was performed by the same attached CAO conversation,
   has verdict `ok`, and references the current Worker Attempt;
3. derive the WorkItem's exact requester and prove that it resolves
   unambiguously to a role=`user` principal;
4. copy the review's exact WorkItem, Attempt, Goal version, Goal packet digest,
   Task packet digest, and generation into the decision record;
5. store the requester decision, short CAO-authored summary, and opaque
   conversation-evidence identifier; and
6. transition accepted work to `completed` but **not** `closed`.

The internal record contains both `requester_id` (who made the decision) and
`recorded_by` (the CAO principal that recorded it). They normally differ;
neither identity appears in the conversation result or decision event.
Idempotency is scoped to the attached CAO credential's principal and command;
a replay with the same key and request digest returns the original decision,
while a different digest conflicts.

## Explicit close plan

`cao_prepare_work_close` freezes an exact inventory before any cleanup. An
owner-private `0600`, non-symlink resolver ledger holds raw local locators;
the control-plane database, events, MCP response, and Dashboard retain only
HMAC fingerprints, provider proofs, and execution digests.  Every workspace,
temporary, log, and branch category is represented exactly once or more as
enumerated resources, or by a provider-signed `not-applicable` record.
Absence is never interpreted as `not-applicable`. Managed launch/adoption
registers a linked worktree together with its owned branch and preserves any
already registered temporary/log resources. A persistent main workspace is
covered only by four explicit `not-applicable` records.

Canonical local artifact paths are staged at report time into an owner-private,
content-addressed archive and replaced in durable Control Plane state by an
opaque artifact reference. Before reserving any destructive effect,
`cao_execute_prepared_cleanup` seals and verifies the exact artifact receipt
set against the archived bytes. Caller-supplied artifact paths, preservation
claims, or evidence identifiers cannot authorize cleanup.

`cao_close_work` is an effect-free finalization record; it does not accept
cleanup outcomes. The scoped runtime is stopped first through
`cao_stop_work_runtime`. Local temporary/log/local-branch cleanup may be run
only by `cao_execute_prepared_cleanup`, which first reserves the complete exact
effect batch and then stores provider-signed postcondition receipts. A linked
worktree is removed before its exact owned local branch. Remote,
ownership-ambiguous, unregistered, shared, or unknown-result cleanup fails
closed; persistent main workspaces are explicit `not-applicable`. The close
transaction re-reads canonical state and requires all of the following before
emitting `work.closed`:

Stop, prepare, and execute each independently require the exact accepted
requester decision for the current completed Work generation, latest completed
Attempt, `ok` Review, Goal/Task packet pair, role=`user` requester, and attached
conversation lineage. Execute checks this before idempotent replay, artifact
preservation, provider verification, or effect reservation. It then requires
all Work runtimes to be `stopped`/`failed`/`missing`, all associated enrollments
to be `revoked`/`failed`, zero active runtime credentials, zero pending
enrollment tickets, and zero unrelated Work- or Worker-scoped effects in
`started`/`unknown`. An unresolved effect from this same exact cleanup
preparation remains on its existing exact-replay reconciliation path; it is
never executed a second time.

- the WorkItem and selected Attempt are completed;
- the exact selected review has verdict `ok`;
- the exact requester decision has verdict `accepted` and is bound to the same
  work, attempt, review, attachment, goal packet, task packet, and generation;
- `expected_goal_version`, Goal packet digest, Task packet digest, and
  generation all equal current canonical values;
- a new close preparation exactly covers the frozen final completion-claim
  artifact manifest, with preserved digest and evidence for each selected
  artifact; already-prepared legacy rows retain their immutable
  `work_history_v1` all-history scope;
- retention-policy, artifact-manifest, and complete cleanup-inventory evidence
  are present;
- every required cleanup target has a unique canonical opaque fingerprint and
  a known successful/not-applied server-derived outcome with provider or
  terminal-state evidence; caller self-attestation is rejected;
- destructive cleanup references a resolved effect operation whose principal,
  action, opaque target fingerprint, and authority grant match the record;
- zero open deliveries, active scoped runtimes, unresolved effects, and
  unknown cleanup outcomes remain.

The four low-level Work-close steps are internal service operations, not the
normal attached-conversation tool catalog and not the public Worker lifecycle.
Each service use case revalidates the current CSC, attachment generation, and
lease inside its transaction before reading an idempotency receipt or mutating
state. A stale, revoked, cross-attachment, or runtime-only credential cannot
replay a close result. Internal projections use step-specific allowlists: they
expose the opaque Work, preparation, artifact, packet, and evidence bindings
needed by the next step, never a raw requester, runtime, enrollment,
attachment, filesystem locator, effect grant, or provider identity.

The persisted close receipt contains the sealed plan digest, decision/review
references, opaque artifact/cleanup evidence references, close timestamp, and
the attachment id/generation. It must never contain raw workspace paths,
commands, URLs, credentials, native thread IDs, or event payload text. The
same close idempotency key plus identical plan returns the same receipt; any
change to the plan or generation conflicts.

## Dashboard semantics

The Dashboard consumes only the Control Plane projection and keeps close
evidence distinct without mislabeling settled completion bookkeeping as active
requester work:

- `completed` with no close receipt is `closure_state: "awaiting-explicit-close"`;
- a valid receipt is `closure_state: "closed"`;
- a supervisor-settled result leaves the actionable Worker arrays and flat
  current-Work alias once the current Attempt is `completed`, the latest CAO
  review is `ok`, and no open Boundary or recovery action remains, whether its
  bookkeeping Work state is `waiting_user` or `completed`;
- requester-decision, preservation, cleanup, and close records remain durable
  and available to their exact authority path; hiding their bookkeeping from
  the current-work lane does not create or infer any of those records;
- a real requester question remains current through its `input_required`
  Attempt and/or open Boundary; and
- a Worker with no later current Work remains only as inactive history.

Closing is never inferred from a terminal WorkItem state. Residual runner,
Delivery, review, or status-request data from closed Work cannot return its
Worker to `needs_attention`, while any genuinely current Work continues to
take precedence over terminal runner inventory.

The dashboard exposes only allowlisted status and evidence availability. It
does not show decision text, paths, cleanup targets, artifact locators,
attachments, or credentials.

## Required implementation points

This document is an executable contract; `tests/test_explicit_close_contract.py`
names the public behavior. The implementation points are:

1. **Models** - add `RequesterDecisionInput` and `WorkCloseInput` with the
   public fields above, and replace the User-role acceptance model on the
   MCP-only path.
2. **Database** - add requester-decision and close-receipt tables keyed by
   work/attempt/review, attachment id/generation, and command idempotency;
   preserve immutable packet/generation bindings and indexed closure state.
3. **Service** - add transactional `record_requester_decision()` and
   `close_work()` use cases. Reuse `require_close_ready()` only after the
   canonical read, enforce attachment scope on every read/write/delivery, and
   revoke close authority when the attachment is stale or failed.
4. **MCP/API** - expose only these attachment-scoped commands to a `csc_...`
   actor, remove the
   User acceptance mutation surface, and reject attachment/thread fields from
   command arguments. Do not add requester ingress or a separate UI.
5. **Projection/Dashboard** - project receipt state from canonical data; do
   not infer `closed` from `completed`.

The production implementation must add transaction-level cross-conversation
tests for decision, close, query, inbox, acknowledgement and delivery
resolution. A string check or tool filtering alone is insufficient evidence
of that isolation.
