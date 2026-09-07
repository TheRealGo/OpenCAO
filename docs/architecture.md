# Architecture

The [documentation map](index.md) identifies the canonical contract for each
operation. The [CAO operator runbook](cao-operator-runbook.md) is the routing
layer; this document describes the underlying durable architecture.

## One durable supervisor kernel

The daemon is the sole durable state, resume, and dispatch authority. It owns
CAO-authored Tasks and Directives, immutable goals, Worker attempts,
Worker-reported boundaries, exact-Boundary-bound and generation-fenced CAO
reasoning turns, messages, per-recipient delivery, evidence review, exact
conversation-scoped requester decisions, and effect authority. A CAO model
supplies judgments through leased turns; it does not maintain a second task
ledger or scheduler outside this database. The full transition contract is
specified in [supervisor-control-plane.md](supervisor-control-plane.md).

## Domain model

```text
Principal
  ├── RuntimeSession(s)
  └── scoped bearer token

WorkItem
  ├── CAO-authored Task and Directive relations (independent/augment/interrupt/replace/cancel)
  ├── requester reference, supervising CAO, and immutable CAO session attachment
  ├── immutable GoalRevision history
  ├── Attempt 1..n
  ├── Boundary → one BoundaryDisposition, or an exact cancellation-bound supersession
  ├── generation-fenced ReasonerTurn
  ├── immutable Message → recipient Delivery 1..n
  ├── Artifact 0..n
  ├── CAO Review 0..n
  └── conversation-scoped RequesterDecision 0..n
```

A WorkItem is the stable user-facing objective. An Attempt is one execution by one Worker. Creating a retry never rewrites prior evidence.

State is deliberately multi-dimensional:

- goal maturity: `unset`, `exploring`, `defined`
- work state: `open`, `active`, `suspended`, `waiting_supervisor`, `waiting_review`, `waiting_user`, `user_needed`, terminal states
- attempt state: assigned, working, suspended, waiting-supervisor, input-required, submitted and terminal states
- trajectory: advancing, at-risk, stalled, drifting, complete
- attention owner: Worker, CAO, user, external or none
- evidence confidence: unknown, declared, observed or verified

## Delivery model

1. A domain transition writes the immutable Message and its per-recipient Delivery records in one transaction.
2. A Dispatcher leases queued work.
3. Before invoking a Runtime Adapter, the Dispatcher durably marks the Delivery `dispatched`.
4. Confirmed handoff marks it `delivered`. A definite pre-dispatch failure may retry; a post-dispatch failure remains outcome-unknown until target evidence resolves it as delivered, not delivered, or dead.
5. The recipient explicitly acknowledges only after incorporating the message. A Boundary disposition atomically marks its matching supervisor delivery handled, so a crash after the decision cannot block later work.

This is crash-safe at-least-once delivery without blind retries across an
uncertain runtime boundary. Idempotency keys prevent duplicate domain effects.

A CAO-bound WorkItem records the exact attachment ID plus its supervisor,
runtime, native Codex thread, project digest, model, sandbox, and adapter in
the canonical goal and task packets. Worker boundaries may wake only that
runtime; attachment recency and historical runtime metadata are never routing
inputs. Historical unbound WorkItems remain immutable audit evidence with no
runtime target. Canonical deployments require an exact CAO attachment for new
Work.

## Durable authority and replaceable connections

The CAO session attachment is the durable conversation identity. MCP stdio and
HTTP connections are replaceable transport sessions, not owners of the
attachment, Worker threads, Work, or wake route. One attachment may have
multiple independently authenticated connections. Each connection is bound to
its exact process generation and loaded catalog, receives its own short-lived
credential, and is fenced independently.

Connection death, expiry, or catalog replacement does not advance the
attachment generation and does not rewrite Worker or Work state. Conversation
Close is the operation that terminates the durable authority and therefore
revokes all connections. Reattachment creates a fresh fenced connection while
leaving the durable conversation authority and historical records intact.

## Process layout

One HTTP daemon is the normal deployment. Streamable HTTP clients connect directly. stdio-only clients run a small proxy to that daemon. This avoids per-client databases, divergent session state and duplicated dispatchers.

## Persistence

SQLite is the source of truth. Connections enable WAL, foreign keys, `synchronous=FULL`, busy timeout, trusted-schema protection and explicit write transactions. Schema versions are recorded in both SQLite `user_version` and the migration ledger.

## Degraded mode

Runtime delivery and A2A push remain optional projections. The Dashboard is a
read-only projection and never owns authority. Dashboard unavailability is a
bounded presentation degradation; it does not block connection, list, New,
instruction recording, or lifecycle control. Existing Worker reports, stop,
cleanup, and close also remain durable during a Dashboard incident so a
presentation failure cannot destroy or strand protocol truth. A queued
instruction is bound to the exact active Worker-thread generation and may bind
to that generation's fresh runtime when it becomes available. Once a Delivery
is dispatched, its runtime boundary is immutable. A Delivery bound through
either its own runtime or its Attempt to a retired Worker epoch remains
historical and is never re-armed onto a resumed thread generation.
Outcome-unknown `dispatched` deliveries and unresolved effects remain blocked
until evidence resolves them; they are never re-armed by heartbeat alone.

## Authority path

Only a CAO principal can create or revise Tasks and Directives, retry, dispose
Worker-reported boundaries, or send instructions to a Worker. A WorkItem
separately records its `requester_id` and `supervisor_id`, but a requester does
not submit, inspect, accept, or cancel through MCP or REST. The requester
speaks in the existing CAO conversation; CAO reasons first and may record the
exact accepted or rejected decision for that attachment. Worker questions,
blockers, and completion claims route to the supervising CAO and cannot advance
lifecycle state without one generation-fenced disposition. This prevents an
API, transport adapter, dashboard, or runtime output from bypassing the
supervisor kernel.

Every CAO ReasonerTurn is bound to the exact unresolved Boundary, goal revision,
and WorkItem generation it may dispose. Deadline recovery abandons an expired
turn, supersedes its incomplete wake-up, and issues exactly one durable
replacement wake-up for that same Boundary; it never asks a Worker runtime
whether time has passed.

The review sequence is enforced by durable state transitions:

```text
Worker completion boundary (declared)
        ↓
CAO boundary disposition (accept for review)
        ↓
CAO review (`ok` or `needs_work`)
        ↓
CAO records the requester decision from the bound conversation
```

Calls made out of order fail closed.
