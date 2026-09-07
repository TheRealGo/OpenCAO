# Supervisor Control Plane Contract

Operational routing is defined in the [CAO operator
runbook](cao-operator-runbook.md). This document remains the normative durable
state and authority contract.

## Status and decision

This document is the target architecture contract for the control plane. Its
purpose is to make the control plane—not an agent context, runtime session,
terminal, dashboard, transport session, or background scheduler—the sole
durable authority for supervision.

MCP, A2A, HTTP, stdio bridges, runtime adapters, and UI projections are
replaceable edges. CAO uses them to record its Tasks and Directives, read
projections, receive delivery attempts, and supervise Worker reports. A
requester speaks only in the existing CAO conversation. Those edges do not own
supervisory state or transition it outside this contract.

### Goals

- Preserve every CAO-authored Task and Directive as a durable, attributable
  decision after CAO has interpreted the requester conversation, including
  accumulated work alongside already-open work.
- Represent the current objective as an immutable revision history with an
  explicit current pointer; never overwrite prior objective, acceptance, or
  non-goal text.
- Serialize supervisory reasoning so that one generation of a reasoner turn is
  the only writer allowed to make its resulting decision.
- Make task progression, message delivery, CAO review, exact
  conversation-scoped requester decisions, and external-effect authority
  independently auditable and recoverable.
- Deliver messages at least once, in recipient order, with recipient-specific
  acknowledgement and handling state.
- Drive normal execution from committed events and durable Delivery records. Lease expiry and
  retry due-times are recovery mechanisms, not an alternate scheduler.
- Support one canonical stateless MCP route, its stdio transport adapter, and
  A2A 1.0 over the same domain commands and projections.
- Permit a staged migration from existing authorities without allowing two
  systems to make competing supervisory decisions.

### Non-goals

- This is not a second planner layered over an existing scheduler. Once cut
  over, there is exactly one durable authority for supervisor decisions.
- It does not replace CAO's substantive judgment. CAO records that judgment as
  a review and, separately, may record an exact requester decision observed in
  the bound CAO conversation.
- It does not make a runtime, model, terminal, or browser into a trusted
  source of task completion.
- It does not ingest requester prompts, raw conversation text, pre-model text,
  predictions, draft text, keystrokes, incomplete streamed input, or an
  alternate requester UI action.
- It does not treat a runtime becoming idle, a delivery being acknowledged, or
  a lease expiring as completion.
- It does not turn effect authorization into an operating-system security
  boundary. Adapters and hosts still need their own least-privilege controls.

## Normative language and authority boundary

The terms **MUST**, **MUST NOT**, **SHOULD**, and **MAY** are normative.

The control plane persists its state in one SQLite database. SQLite is the
only authoritative store for the entities and decisions in this document. An
append-only event stream, cache, search index, dashboard, protocol session,
runtime-native history, and logs are projections or evidence only. They MUST
be reconstructible from SQLite or safely discarded.

Every externally visible write is converted into a typed domain command. A
domain command is accepted only after authentication, authorization,
validation, idempotency resolution, and an atomic SQLite transaction. No
adapter or transport is permitted to write a domain table directly.

SQLite MUST run with foreign keys enabled, WAL journaling, `synchronous=FULL`,
a bounded busy timeout, and explicit short write transactions. The schema MUST
include an application identifier, migration ledger, integrity checks, and
backups. A process that cannot establish this configuration MUST fail closed
for writes.

## Canonical model

The following model is normative. Names are domain names; table names and
column layouts may vary only when all stated identities, relations,
immutability, and invariants remain true.

```text
CAO Task (WorkItem) 1 ── 1..n GoalRevision (current_goal_revision_id)
         1 ── 0..n Directive          (augment | interrupt | independent | replace)
         1 ── 0..n Attempt
         1 ── 0..n ReasonerTurn
         1 ── 0..n Boundary ── 1 Disposition
         1 ── 0..n Review
         1 ── 0..n RequesterDecision (conversation-scoped)

Attempt 1 ── 0..n immutable Message ── 1..n Delivery
Message  ── correlation / causation ── Message | Directive | Boundary

EffectGrant 1 ── 0..n EffectOperation
```

All entities use opaque stable identifiers. All timestamps are UTC. Every
state-changing record carries `created_at`, acting principal or system actor,
correlation ID, causation ID when applicable, and an idempotency key or an
explicit reason that the command cannot be retried. Payloads are immutable;
changes create a new record rather than updating the old payload.

### Conversation boundary and CAO-authored records

The requester gives task language only to the existing CAO conversation. The
Control Plane receives neither that raw prompt nor a transcript, pre-model
input, predicted text, draft, keystroke, partial stream, or alternate requester
UI action. CAO reasons first, then records either a CAO Task (a `WorkItem` with
its immutable GoalRevision) or a Directive that relates existing work.

A Task or Directive records its CAO author, bound conversation attachment,
correlation, reason, and immutable payload. An additional instruction is never
dropped because a target is busy, a runtime is idle, or another Task is open;
CAO records the appropriate `augment`, `interrupt`, `independent`, or `replace`
Directive. The control plane stores no substitute requester transport to make
that decision.

Historical `SourceReceipt`, `SubmittedIntent`, and `UserAcceptance` records may
remain as migration compatibility evidence. They are not a public ingress,
requester principal, or alternate requester UI contract.

### WorkItem and immutable GoalRevision

`WorkItem` is the stable, user-visible unit of requested outcome. It owns the
requester, supervising principal, current lifecycle state, current goal
revision pointer, current attention owner, and sequence counters. It does not
store mutable goal prose as its authoritative objective.

`GoalRevision` is immutable and belongs to exactly one WorkItem. It contains:

- a monotonically increasing revision number;
- title, objective, maturity, acceptance conditions, and non-goals;
- priority, requester, supervisor, and immutable metadata for that revision;
- revision reason and revision class;
- author, source intent or directive, correlation, and timestamp; and
- the prior revision identifier; and
- canonical packet JSON plus its cryptographic digest.

`WorkItem.current_goal_revision_id` MUST reference one of its own revisions.
Creating a WorkItem atomically creates revision 1. Updating the current
objective, acceptance conditions, or non-goals MUST atomically append the next
revision and advance the pointer with optimistic expected-version checking.
No report, runtime adapter, or delivery acknowledgement may advance the goal
pointer. A report against a non-current revision is retained as evidence but
MUST NOT change current work state without an explicit supervisor disposition.

Goal revisions are retained through terminal states. Retention or archival
policy MAY move payloads to protected durable storage, but the revision's
identity, ordering, digest, provenance, and acceptance/non-goal semantics
MUST remain verifiable.

Every Attempt stores both the exact Goal packet digest and a Task packet digest
that also binds its attempt number, Worker, and RuntimeSession. Assignment
messages, Worker acknowledgement/start evidence, Boundaries, ReasonerTurn
input/output evidence, completion claims, CAO Reviews, and RequesterDecisions must
carry and revalidate that same pair. A version match without both packet
digests is insufficient and fails closed.

### Directive and work relations

`Directive` is the immutable CAO decision artifact that links a CAO-interpreted
instruction to the work it affects. It contains issuer, target WorkItem,
optional source WorkItem, relation, rendered instruction, expected goal
revision, and reason. Its relation is exactly one of:

| Relation | Required effect |
| --- | --- |
| `augment` | Adds bounded work to the target without changing its sealed goal unless a separate GoalRevision is created. |
| `interrupt` | Suspends an active runnable path at a durable Boundary, then schedules the directive's work ahead of it. |
| `independent` | Creates or relates separately executable work without changing the source WorkItem's goal. |
| `replace` | Explicitly supersedes the target's current goal through a new GoalRevision and records the replaced revision. |

`replace` requires explicit source evidence and a new goal revision. It MUST
NOT be inferred from recency, a similar request, or a runtime report. An
`interrupt` does not erase the interrupted work; it remains resumable or is
separately cancelled with a recorded authority. `augment` is not a substitute
for `replace`, and `independent` never silently shares completion state.

The relation graph MUST reject self-cycles and cycles that would make an item
its own transitive predecessor. It MUST preserve a total per-target directive
order. A Directive is rendered into messages only after its transaction
commits; the rendered message is not the source of the relation.

### Attempt

`Attempt` is one bounded execution assignment for a WorkItem and a particular
goal revision. It records attempt number, assigned worker, runtime binding if
any, state, start/end times, observed trajectory, evidence confidence, and
the goal revision it was created to execute. Attempts are never reused across
retries or worker reassignment. A retry creates a new Attempt and preserves
all prior reports and messages.

An Attempt can make a completion claim, but a claim is only declared evidence.
It cannot complete its WorkItem. A terminal Attempt does not imply a terminal
WorkItem; the work may require review, a requester decision recorded from the
existing CAO conversation, a correction,
another attempt, or an explicit cancellation/failure disposition.

### ReasonerTurn and exclusive generation-bound lease

`ReasonerTurn` represents one authority-side reasoning invocation. It is not a
runtime turn and it does not inherit authority from a model context. A turn is
bound to one exact open Boundary, its WorkItem, goal revision, and WorkItem
generation. It also records the owning principal, lease token digest, lease
expiry, state, and immutable result reference. A turn for one Boundary cannot
dispose another Boundary even when both belong to the same WorkItem.

For a given serialized subject lane, at most one `ReasonerTurn` MAY hold an
active lease. The unique key is `(subject_type, subject_id, active_lease)` or
an equivalent partial unique index. Leasing a turn is an atomic compare-and-
swap from `queued` to `leased` for its generation. Every transition that emits
a decision MUST require the matching turn ID, generation, and unexpired lease
token in the same transaction. A stale token, different generation, or expired
lease fails closed.

On expiry, recovery marks the old turn `abandoned`, supersedes its incomplete
wake-up with explicit evidence, and creates one new durable wake-up for the
same still-open Boundary. The replacement gets a new turn ID and lease token;
the WorkItem generation remains unchanged unless a domain transition itself
requires fencing. The old owner can never finish. A completed turn stores a
digest/reference to its inputs and outputs, then becomes immutable. Its result
is advisory until materialized by a validated domain command; model text alone
never changes a WorkItem.

### Boundary, Disposition, and terminal supersession

`Boundary` records a durable decision point: for example intent routing, an
attempt report requiring attention, a requested interruption, a review result,
or a requested external effect. It names its subject, reason, required owner,
input snapshot, and opening event.

Every normally closed Boundary MUST have exactly one immutable `Disposition`. A
Disposition names the decision class, actor, evidence, and resulting domain
references. Examples are `route`, `delegate`, `resume`, `wait_user`,
`needs_work`, `accept`, `reject`, `cancel`, `fail`, `suspend`, and
`effect_authorize`. A partial unique constraint on `Disposition.boundary_id`,
plus a closed-boundary constraint, enforces one disposition. An actionable open
Boundary has neither a Disposition nor a supersession; a normally closed
Boundary has one and only one Disposition. A previous disposition is never
amended—corrections open a new boundary with explicit causation.

Explicit Work cancellation is the sole terminal exception. In the same
transaction, every still-open Boundary receives one immutable
`BoundarySupersession` bound to the exact later `work.canceled` event. This is
not a fabricated review or decision. A Boundary can have a Disposition or a
supersession, never both. Migration may backfill this relation only when the
Work is canceled, the Boundary is from an older Work generation, and the
canonical cancellation event is later than the exact `boundary.recorded`
event. Missing proof, nonterminal Work, and every current-generation Boundary
remain fail-closed.

This separation prevents prose such as "looks done" from becoming a state
transition. A state change is valid only when it is the declared result of the
single disposition for its boundary.

### Immutable Message and per-recipient Delivery

`Message` is immutable content plus routing metadata: sender, kind, payload
reference/digest, causation, correlation, sequence, WorkItem/Attempt/Directive
references, and the expected goal revision. A message can have one or more
recipients. It never has a mutable global delivery state.

`Delivery` is the recipient-specific lifecycle record. It has a unique
`(message_id, recipient_id)` key and recipient-local monotonic sequence. It
contains lease token/generation, retry count, next action time, acknowledgement
and handling evidence, and exactly one state:

| State | Meaning |
| --- | --- |
| `queued` | Durable and eligible for event-driven dispatch. |
| `leased` | A dispatcher holds a bounded attempt lease. |
| `dispatched` | Adapter invocation may have begun; the outcome is unknown until explicit evidence resolves it. |
| `delivered` | The adapter confirmed handoff; recipient acknowledgement is still required. |
| `acknowledged` | The recipient confirmed it incorporated the message. |
| `handled` | The message's required domain response was durably recorded, or no response was required. |
| `dead` | Delivery exhausted bounded retries and requires an explicit recovery disposition. |

Adapter handoff is not acknowledgement; acknowledgement is not handling; and
handling is not task completion. A recipient can acknowledge duplicate
deliveries idempotently. A stale recipient may read historical messages but
MUST NOT report a result against a newer expected goal revision.

### CAO review and conversation-scoped requester decision

`Review` records CAO's evidence-based verdict on an Attempt. The public
`cao_review` verdicts are exactly `ok` and `needs_work`. An `ok` review moves
the WorkItem to requester attention; it does not complete the WorkItem.

`RequesterDecision` is a separate immutable record linked to an `ok` Review.
Only the attached CAO conversation can record it, using the exact requester
identity, a conversation evidence identifier, and an `accepted` or `rejected`
verdict. It is not a requester-principal MCP or REST action. Only an exact
accepted RequesterDecision may enter the explicit close path; rejection creates
a Boundary for CAO disposition and does not modify the prior Review or erase
the Attempt.

Renewing the same conversation attachment advances its credential epoch but
does not invalidate an earlier immutable RequesterDecision or close receipt.
Projection accepts only a monotonic later generation of the same attachment;
a different attachment, principal, Work generation, Review, or task packet
still fails closed.

This accepted-Work completion path is distinct from the requester-facing
Worker Close (Finish) lifecycle. Public Worker Close is one
`cao_finish_worker_thread` operation on the exact Worker generation: active or
working state is accepted, unsettled Work is terminalized, unknown evidence is
preserved, and the Worker is archived as resumable. It does not require this
RequesterDecision or any artifact-cleanup operation, and the internal Work
close primitives are not published as a manual Worker Close recipe.

### EffectGrant and EffectOperation

`EffectGrant` is authority to perform a bounded external or destructive
effect. It identifies the authorized principal, effect kind, target/action
patterns, optional content/argument/working-directory digests, scope, expiry,
issuer, and whether it is one-time or standing. It is separate from a
Directive, message, review, and requester decision.

`EffectOperation` is the actual-effect ledger entry. Before an adapter crosses
an effect boundary, the control plane atomically finds a matching grant and
creates an operation in `started`, consuming a one-time grant in the same
transaction. The executor then performs the exact bounded action. The outcome
is one of `succeeded`, `not_applied`, `failed`, or `unknown`.

An operation is `unknown` whenever execution may have started but completion
cannot be established, including crashes, timeout after handoff, or lost
transport acknowledgement. For the same principal and normalized effect
identity, a `started` or `unknown` operation blocks retry. A new action needs
verification evidence and an explicit resolution Boundary; it MUST NOT be
blindly retried.

## State machines

The following transitions are the canonical domain state machines. A command
outside a listed transition is rejected. Terminal means no implicit forward
transition; a new Boundary and explicit authorized command are required for
any exceptional follow-up.

### WorkItem lifecycle

| From | To | Authorized condition |
| --- | --- | --- |
| `open` | `active` | A disposition delegates an Attempt for the current GoalRevision. |
| `active` | `waiting_supervisor` | A report, interruption, failure, or effect question opens a supervisor Boundary. |
| `active` | `waiting_user` | Only a disposition with an `ok` Review requests a requester decision in the existing CAO conversation. |
| `waiting_supervisor` | `active` | A disposition resumes, corrects, or creates a next Attempt. |
| `waiting_supervisor` | `waiting_user` | An `ok` Review requires a requester decision in the existing CAO conversation. |
| `waiting_user` | `completed` | CAO records an exact accepted RequesterDecision and completes the explicit close path. |
| `waiting_user` | `active` | CAO records a rejected RequesterDecision and a CAO disposition resumes or replans. |
| nonterminal | `cancelled` | An authorized cancellation disposition. |
| nonterminal | `failed` | An explicit terminal failure disposition with evidence. |

`idle` is not a WorkItem state and is not a steady-state supervision signal.
The control plane never derives a Boundary from a runtime, screen, process, or
silent interval. Only a Worker MCP report can create its question, blocker, or
completion Boundary.

### Attempt lifecycle

| From | To | Trigger |
| --- | --- | --- |
| `assigned` | `working` | Worker acknowledges assignment and begins. |
| `assigned` or `working` | `input_required` | Worker reports a bounded question or blocker. |
| `working` | `submitted` | Worker submits a completion claim for its expected goal revision. |
| `submitted` | `completed` | CAO Review records `ok` with evidence. |
| `submitted` | `working` | Supervisor Review requires more work. |
| active attempt state | `cancelled` | Authorized cancellation or replacement boundary. |
| active attempt state | `failed` | Explicit failure disposition. |

### Delivery lifecycle

| From | To | Transactional precondition |
| --- | --- | --- |
| `queued` | `leased` | Due, recipient order is unblocked, and lease compare-and-swap succeeds. |
| `leased` | `dispatched` | The transaction commits immediately before invoking the adapter. |
| `dispatched` | `delivered` | Adapter reports successful handoff for the same lease generation. |
| `dispatched` | `queued` | Explicit evidence proves handoff did not occur; generation increments before re-lease. |
| `leased` | `queued` | Pre-invocation failure or lease expiry; generation increments before re-lease. |
| `leased` | `dead` | Retry budget is exhausted or a non-retryable adapter result is recorded. |
| `dispatched` | `dead` | Explicit recovery evidence proves the uncertain delivery must not be retried. |
| `delivered` | `acknowledged` | Recipient authenticates and acknowledges the exact message. |
| `acknowledged` | `handled` | Required report/disposition is committed, or the message requires none. |
| `dead` | `queued` | A recovery Boundary explicitly re-arms it after evidence. |

Delivery ordering applies per recipient. A later recipient sequence MUST NOT
be leased while an earlier delivery is `queued`, `leased`, `dispatched`, `delivered`, or
`acknowledged`, unless an explicit priority/override policy is represented by
a Directive and recorded Boundary.

### Review and requester-decision lifecycle

| Record | Allowed transition | Precondition |
| --- | --- | --- |
| completion claim | `Review(ok)` | Attempt is `submitted`; CAO evidence is attached. |
| completion claim | `Review(needs_work)` | Attempt is `submitted`; corrective reason is attached. |
| `ok` review | `RequesterDecision(accepted)` | CAO records the exact decision from the bound conversation; review is current for the WorkItem. |
| `ok` review | `RequesterDecision(rejected)` | CAO records the exact decision from the bound conversation; rejection reason opens a new Boundary. |

## Transactional invariants

Every state-changing command MUST use a single SQLite write transaction. The
transaction commits the aggregate mutation, the immutable audit/event record,
all messages and Delivery records caused by it, and any transactional outbox rows.
It commits none of them if any invariant fails.

1. **One authority.** All authoritative tables are in the one SQLite database.
   No transport session, adapter, UI, queue, or cache is another writer.
2. **CAO reasoning before recording.** A requester statement remains in the
   existing CAO conversation; only CAO-authored Tasks, Directives, and exact
   conversation-scoped RequesterDecisions enter the Control Plane.
3. **Idempotent CAO commands.** A duplicate CAO command or idempotency key
   returns the original command result. A key with a different canonical digest
   is a conflict, not a retry.
4. **Directive conservation.** Every accepted additional instruction has one
   CAO-authored Directive with an explicit relation. A busy target never causes
   omission.
5. **Goal integrity.** Every WorkItem has exactly one current GoalRevision;
   revisions are append-only, ordered, and reference the same WorkItem.
6. **Directive integrity.** Every accepted accumulated, interrupt, independent,
   or replace instruction has one Directive with exactly one relation. `replace`
   also has a successor GoalRevision.
7. **Lease fencing.** A ReasonerTurn mutation requires the exact Boundary,
   turn ID, owner token, and WorkItem generation. A Delivery mutation requires
   its owner token and delivery generation. Expired or stale leases cannot write.
8. **Boundary closure.** An actionable Boundary has neither a Disposition nor
   a supersession. It closes with exactly one Disposition, or an explicit Work
   cancellation terminally supersedes it with the exact later cancellation
   event. No Boundary may have both, and no unrelated history can supersede it.
9. **Message immutability.** Message payload and routing metadata never change.
   Each recipient has one Delivery record and acknowledgement belongs to that
   recipient only.
10. **Completion split.** Attempt submission, CAO Review, and a
    conversation-scoped RequesterDecision are separate records and transitions.
    No runtime state, message delivery, or requester MCP/REST action can
    substitute for either review or decision recording.
11. **Effect fencing.** One-time grants are consumed atomically with creation
    of `EffectOperation(started)`. `started` and `unknown` block duplicate
    effects until explicit resolution evidence is committed.
12. **Event atomicity.** Each durable mutation has an append-only event with
    correlation and causation. Consumers advance their cursor only after their
    local projection or Delivery action is durable.

The database SHOULD enforce these rules with foreign keys, partial unique
indexes, check constraints, and guarded updates. Service-layer validation is
not a sufficient substitute for constraints that SQLite can enforce.

## Event-driven operation and recovery

### Steady state

Normal operation is event-driven:

1. A command commits domain state, an event, and durable per-recipient Deliveries.
2. The dispatcher is notified after commit, reads the event/outbox cursor, and
   leases due recipient deliveries.
3. The dispatcher commits `dispatched` immediately before adapter invocation.
   The adapter then attempts delivery and commits `delivered` only on a
   confirmed handoff. An uncertain result remains `dispatched` and blocks
   automatic retry until explicit evidence resolves it.
4. A recipient acknowledgement or report submits another command, creating the
   next event and, when needed, a supervisor ReasonerTurn or Boundary.

The dispatcher MUST be restart-safe. It reads committed state rather than
relying on in-memory callbacks. Dashboards, streaming endpoints, and push
callbacks consume the event stream as projections and can fall behind without
changing domain correctness.

### Polling is recovery only

Polling MUST NOT be the regular means of asking whether an agent is done.
Periodic scans are permitted only to recover durable deadlines that cannot
otherwise wake the process:

- an expired ReasonerTurn, Delivery, adapter, or runtime lease;
- a due retry/backoff time;
- a delayed delivery whose event notification was lost during process restart;
- startup reconciliation of unfinished outbox work.

Each recovery scan is bounded, reads due rows by indexed deadline, and makes
the same fenced compare-and-swap transitions as an event-driven worker. It
MUST NOT create completion from elapsed time, a silent runtime, or a missed
heartbeat. Repeated runtime output without a Worker MCP report does not create
a supervisory message.

## Runtime adapters

Runtime adapters are effectors, never supervisory authorities.
They receive a fully rendered immutable Message plus delivery lease token,
generation, and callback capability scoped only to acknowledgement/reporting.
They return a structured delivery result. They cannot create a WorkItem,
advance a GoalRevision, close a Boundary, verify their own work, accept for a
requester, or grant an effect.

The common adapter contract is:

```text
lease delivery -> start/resume runtime -> hand off exact message
              -> report handoff result -> await recipient protocol actions
```

Managed Codex App Server and Claude Code Workers use this handoff mechanism.
They receive the scoped MCP bridge for their exact launch and persist only
bounded diagnostics. A handoff that may have started remains `dispatched`; it
is not automatically retryable. Native session and runtime state are adapter
metadata, not domain state.

An adapter reports only its message-handoff result. It may update delivery
diagnostics, but it cannot open a Boundary, infer a Worker state, or answer
whether a WorkItem is complete.

## Transport/domain separation

All transports follow this pipeline:

```text
authenticate -> parse/version-check -> validate CAO command
             -> domain service transaction -> projection/transport response
```

The transport layer owns protocol parsing, response envelopes, and request limits. The domain
layer owns authorization, idempotency, entities, transitions, leases,
deliveries, CAO review, requester-decision recording, and effect authority. No protocol-specific
state machine may duplicate WorkItem, Attempt, Message, or supervisor
transitions.

### MCP

The current MCP surface is stateless. Each request is independently
authenticated and version-checked, then mapped to a domain query or command.
It MUST NOT create a durable supervisory session, private queue, or private
database. Tool names, resources, and notifications are views of the canonical
model; tool success is not a domain state transition unless its mapped command
commits.

### A2A 1.0

A2A maps one stable A2A Task to one WorkItem; internal retries remain Attempts
under the same task identity. JSON-RPC, HTTP+JSON, SSE subscriptions, and push
notifications are projections and command adapters. A2A task state is derived
from the WorkItem, latest Attempt, active Boundary, Review, and
RequesterDecision—it is never an independent authority.

An A2A `SendMessage` records a CAO-authored Task or Directive only when the
request is complete and authorized. It cannot carry requester conversation
content or create a requester decision. Streaming updates and subscriptions are
read-only event projections. A disconnected SSE client or failed push callback
does not change supervisory state. A2A callers MUST NOT obtain a direct path
to instruct a worker outside the supervisor authority path.

## Crash, replay, and idempotency behavior

The design assumes every process can crash between any two instructions.

- **Crash before transaction commit:** no state transition exists; a CAO command
  retry safely recreates or retrieves the same result by idempotency key.
- **Crash after commit before response:** the retry returns the recorded result;
  it does not create another Task, Directive, WorkItem, message, or effect operation.
- **Crash after leasing before handoff:** the lease expires and recovery queues
  the next generation. The former holder cannot commit a stale result.
- **Crash after handoff before delivery result:** delivery remains
  `dispatched`. Automatic retry is blocked until evidence resolves it as
  delivered, not delivered, or dead. A stable message ID still makes recipient
  acknowledgement idempotent.
- **Crash after worker action before report:** the WorkItem remains active or
  waiting at its last durable Boundary. Runtime idleness is only evidence; a
  supervisor disposition is required to move it.
- **Crash during an effect:** the effect operation remains `started` or becomes
  `unknown`; retry is blocked pending verification evidence.
- **Crash during projection delivery:** the event remains unread by that
  consumer; the projection replays independently without duplicating the
  domain command.

Idempotency is scoped to authenticated actor and operation. The idempotency
record stores the canonical request digest and durable result reference. The
same key and same digest return the original result; the same key and different
digest is rejected. Delivery retry idempotency is separate from command
idempotency and uses `(message_id, recipient_id, generation)`.

## Acceptance matrix

The following scenarios are minimum acceptance tests for an implementation of
this contract.

| Scenario | Setup/action | Required evidence |
| --- | --- | --- |
| Accumulated tasks | CAO interprets a second actionable instruction in the existing conversation while related work is active. | A new CAO-authored Directive with `augment` or `independent` relation exists; no instruction is dropped and the existing WorkItem remains traceable. |
| Goal retention | Revise a defined objective after work has started, then query history. | Prior GoalRevision is byte-for-byte/digest-stable; a new revision is current; old reports remain linked to their expected revision and cannot advance current state. |
| Idle is not completion | Make an assigned runtime report idle or stop without a completion claim. | WorkItem does not enter `completed`; a Boundary may be opened, but no Review or RequesterDecision is manufactured. |
| Conversation exclusion | Produce raw prompt, pre-model, draft, or predicted conversation text and then abandon it. | No WorkItem, Directive, RequesterDecision, or message is created. Only a subsequent CAO-authored Task or Directive may enter the Control Plane. |
| Interrupt | Issue an explicit interrupt against active work. | Directive relation is `interrupt`; an interruption Boundary has exactly one disposition; interrupted work remains preserved and resumable or explicitly cancelled. |
| Replace | Issue an explicit replacement of a defined goal. | Directive relation is `replace`; a successor GoalRevision points to the prior revision; recency alone cannot reproduce this path. |
| Exclusive reasoner lease | Lease a ReasonerTurn for one Boundary, expire it, then attempt to commit with the old token. | The old turn is abandoned and rejected; one recovery wake-up is durable; only a new turn bound to that same open Boundary can commit; at most one active lease exists. |
| Delivery replay | Crash after adapter invocation but before recording its result, then recover. | Delivery remains `dispatched`; no retry occurs until explicit evidence resolves the outcome. Evidence of no handoff creates a fenced next generation; evidence of handoff advances the same immutable Message without duplicate mutation. |
| Delivery lifecycle | Drive one recipient through handoff, acknowledgement, and required report. | Delivery transitions only through `queued → leased → dispatched → delivered → acknowledged → handled`; a second recipient has independent state. |
| Headless continuation | Let a managed runner exit cleanly after a completion report, record `needs_work`, and issue `correct`. | The logical runtime is `waiting`; a fresh credential/launch generation receives the correction without CAO lifecycle input or reuse of the exited process. |
| Cancellation supersession | Cancel Work while one Boundary is open, then rebuild projection and query the Work. | The exact later `work.canceled` event supersedes the older-generation Boundary; it remains audit history but is absent from actionable/open counts. A current-generation Boundary or missing/unrelated event remains unhealthy. |
| Attachment renewal | Record an accepted requester decision, renew the same conversation attachment, then rebuild readiness. | The historic decision and completion remain valid under the monotonic attachment generation; a different attachment or stale Work generation is rejected. |
| Review/decision split | Submit a completion claim, record `cao_review(ok)`, then record a rejected requester decision from the bound CAO conversation. | Review is immutable; RequesterDecision is separate; WorkItem does not complete and a new Boundary is open. |
| Effect unknown outcome | Start an authorized effect and simulate loss after handoff. | EffectOperation is `unknown` or unresolved `started`; matching retry is blocked until verification evidence resolves it. |
| Transport equivalence | Record equivalent CAO Tasks or Directives through stateless HTTP MCP, its stdio adapter, and A2A. | Each produces the same canonical Task/Directive semantics; protocol connection loss does not alter them. |

## Implementation checklist

An implementation is conformant only when it can demonstrate all of the
following from schema constraints, transaction tests, and crash/replay tests:

- one SQLite writer model with no side authority;
- CAO-authored Task/Directive records and requester-conversation exclusion;
- immutable directive, message, goal revision, review, requester decision,
  boundary/disposition, and effect records;
- generation-fenced reasoner and delivery leases;
- recipient-specific delivery state, ordering, acknowledgement, and handling;
- separate CAO review and conversation-scoped requester decision recording;
- event-driven dispatch with only bounded recovery scans;
- canonical MCP HTTP/stdio and A2A 1.0 as adapters over shared domain commands;
- effect unknown-outcome fencing.
