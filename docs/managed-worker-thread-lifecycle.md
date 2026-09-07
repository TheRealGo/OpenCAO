# Managed Worker thread lifecycle

Each managed Worker has one opaque `worker_thread_id` and one monotonically
increasing lifecycle generation. The identifier represents CAO supervision of
a Worker lifecycle, not a Work, process, credential, runtime session, workspace, or
provider-native conversation identifier. Ordinary listing and lifecycle use
are project-scoped: any currently attached CAO conversation for the same CAO
principal and project can list and operate the exact Worker. The attachment
that created the Worker is retained as provenance, not ownership. Foreign-project
and unknown identifiers remain indistinguishable.

The conversation-facing lifecycle is:

```text
New -> active -> Close (Finish) -> archived -> Resume -> active
        |                              |
        +------------ Delete ----------+-> deleted
```

`cao_new_worker_thread` performs **New** without creating a Work or Goal.
Directory and idempotency key are sufficient; runner defaults to Codex, the
server profile supplies model and reasoning effort when omitted, and an absent
display name becomes the fixed safe `Codex Worker` or `Claude Worker` label.
No Directory basename is reused as public metadata. Its result and
`cao_list_managed_workers` expose the opaque thread identifier, public state,
generation, and bounded display/launch-profile labels. Runtime, enrollment,
provider-session, and workspace identities remain private.

`cao_instruct_worker_thread` is the separate task operation for one exact
active thread. It requires only the thread, bounded objective, and idempotency
key. Title is optional and maturity defaults
to the explicitly stored `unset`; `defined` alone requires an acceptance
condition. The instruction is durably queued even when its Worker connection
is unavailable or the Worker already has unsettled Work. Every call creates an
independent Work record; it never ends or replaces the Worker. The current CAO
attachment becomes that Work's report/review destination. New and instruction
remain separate commands so neither lifecycle nor task state is inferred.

`cao_finish_worker_thread`,
`cao_resume_worker_thread`, and `cao_delete_worker_thread` require that exact
identifier and an idempotency key. `expected_generation` is an optional
compare-and-swap guard for advanced callers, not a prerequisite for ordinary
control. “Close”
in the requester-facing four-operation lifecycle maps to the existing Finish
operation and its resumable archive. A stale generation cannot mutate a newer
lifecycle. A foreign or unknown identifier is reported as the same scoped
not-found condition.

## Archive meaning

The archive is a logical CAO supervision archive. It is deliberately distinct
from a provider-native Codex or Claude session store:

- **Finish** stops future authority for the exact Worker, terminalizes its
  unsettled Work, removes it from active CAO supervision and every Dashboard
  Worker category, and preserves dispatched/unknown evidence. It retains the
  opaque native resume handle and all durable audit history.
- **Resume** reactivates the same logical Worker thread and native conversation
  with a fresh runtime and enrollment epoch. It does not require or invent a
  next Goal. The next ordinary dispatch issues its one-time launch ticket and
  the handshake creates a fresh credential. It does not revive an old
  credential or redispatch an old Work message. A pure Resume therefore also
  succeeds when Finish retained a dispatched/unknown Delivery or unknown
  effect: that evidence stays byte-for-byte unchanged. The resumed Worker can
  accept a later instruction; a disconnected transport is replaced or the new
  Work remains queued.
- **Delete** removes the exact CAO thread ledger and its resume capability. The
  Delete call itself is the explicit lifecycle command; active state,
  cross-conversation provenance, or an omitted generation does not add a
  second acknowledgment ceremony. It does not
  claim to erase a provider's global conversation storage.

None of these actions deletes, moves, archives, checks out, resets, or cleans a
project Directory, workspace, branch, local file, artifact, artifact archive,
or close receipt. Existing Work, Attempt, Message, Delivery, Artifact, Review,
decision, effect, and close history remains durable after Delete.

Worker-thread Resume is not runtime-failure recovery. A recoverable launch
failure advances the active logical thread through a fresh internal execution
epoch; it does not archive the thread first and does not authorize CAO to
create an equivalent replacement Worker.

## Close authority boundary

Finish (the requester-facing Close operation) accepts the exact current Worker
generation even while it is active or working. In one bounded lifecycle saga,
the Control Plane fences future Worker report and dispatch authority, stops the
exact runtime, terminalizes unsettled Work and pending directives, and archives
the thread as resumable. Only provably unclaimed queued or leased Deliveries
may be terminalized; dispatched/claimed Deliveries, started/unknown effects,
packets, artifacts, and their generation fences remain immutable evidence.
Close is not Work acceptance, cleanup, Delete, or conversation close, and none
of those is a prerequisite. Process exit or silence does not prove Close; the
durable archived state does.

An archived thread cannot receive a Delivery, issue a launch ticket, appear in
Dashboard counts, or be recreated automatically by catalog ensure. Resume is
the only operation that can reactivate it. After a successful Delete, the old
identifier cannot be resumed. A later New request may create a distinct Worker
thread without reviving the old one.

An explicit non-archived Delete is terminal supervision authority, not
evidence about what an in-flight Worker, provider, Delivery, or external effect
did. In one transaction the Control Plane cancels every unsettled Work bound to
the exact Worker, abandons its decision leases, supersedes its open Boundaries
and pending directives, revokes Worker credentials and launch tickets, retires
the thread internally, and removes the logical ledger. Only provably unclaimed
queued or leased Deliveries are dead-lettered. Claimed or contradictory queued
rows, dispatched or otherwise claimed Deliveries, started/unknown effects,
messages, artifacts, provider-circuit
records, and all task history remain unchanged as reconciliation evidence. No
cancel packet, retry, provider deletion, or filesystem cleanup is issued.

Cross-conversation Delete requires the current exact CSC and the database-bound
same CAO owner and project digest. An optional generation may narrow concurrent
intent but is not required.
Missing, malformed, or historically inconsistent Goal, task-packet, event, or
directive metadata is retained as an anomaly; it is not authority to refuse
explicit Close or Delete of the exact thread. Only an actual ambiguity in
shared requester/supervision authority blocks before mutation. Sealed-packet
integrity remains mandatory for assignment, dispatch, reporting, and effect
authority, not for terminal lifecycle fencing. The source attachment may still
be live and may continue supervising other Workers: the transaction serializes
commands, fences only the exact Worker, and makes every later command for that
deleted Worker fail without killing the attachment.

Attachment generation `0` is the valid initial generation, not missing
metadata. Close/Delete cleanup audits distinguish `NULL` from every valid
non-negative generation and must not emit a retained anomaly merely because a
Worker and its Work were created before the attachment's first renewal.
Likewise, a Work awaiting requester acceptance normally has a completed latest
Attempt and no mutable Attempt to cancel. Cleanup validates the Work/Attempt
state pair instead of requiring one nonterminal Attempt for every unsettled
Work.

Schema migration creates an active thread for each existing enabled managed
specification, even when its historical runtime is missing or its enrollment
failed. Existing stopped or revoked specifications have no Finish proof and are
classified internally as non-resumable stopped history, never as archived
threads. That history remains outside Dashboard Worker groups and does not
block creation of a fresh catalog lifecycle; an exact acknowledged Delete may
remove its remaining CAO thread ledger without introducing another public
lifecycle operation.

Conversation close is also distinct from Finish. It cancels terminalizable
Work supervised by that conversation and revokes that attachment, but leaves
every project-local Worker lifecycle unchanged. A same-project CAO conversation
can continue using the exact Worker. Only explicit Finish creates the resumable
archived state that requires Resume or Delete; only explicit Delete removes the
thread ledger.

`system_reconciliation` does not add a fifth requester-facing lifecycle
operation. When the requester explicitly chooses Delete, acknowledged Delete
atomically cancels its Work, supersedes its Boundary without disposition,
preserves unknown Delivery/effect/provider evidence, and removes the exact
thread ledger. No Work-targeted abandonment command is exposed. Explicit
Finish or Delete is the Worker lifecycle authority; unresolved Work evidence
remains preserved.

Pure Resume reactivates the archived logical thread and appends its fresh
fenced runtime/enrollment epoch without creating Work. The resumed Worker can
accept a later instruction through `cao_instruct_worker_thread`. Resume does
not pre-issue a launch ticket or credential. Normal Assignment dispatch issues
the one-use ticket, and the Worker MCP handshake creates the fresh credential.
