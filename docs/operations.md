# Operations

CAO model operations start with the [CAO operator
runbook](cao-operator-runbook.md). This document is the deeper owner/operator
reference for process, database, backup, and deployment effects.

- Owner: Control Plane maintainers
- Lifecycle state: current
- Primary validation: `test_api.py`, `test_cli_doctor.py`, and lifecycle tests
- Review trigger: readiness fields, dispatcher cadence, diagnostic conditions,
  authority mode, backup, or deployment behavior changes

## Private state

Default directory: `~/.local/state/cao-a2a` (`0700`). Database, WAL, shared-memory, token export and callback key files are kept at `0600`.

## Health, readiness, and diagnosis

These surfaces answer different questions and are not substitutes:

| Surface | Authentication | Meaning |
| --- | --- | --- |
| `/health` | none | Process and protocol identity only. HTTP 200 does not prove that CAO can safely accept work. |
| `/ready` | none | Bounded serving-readiness projection. HTTP 200 requires database integrity, canonical authority, a healthy kernel projection, and a live Dispatcher with a recent error-free cycle. |
| `cao-a2a doctor` | owner-local CLI | Deeper integrity, permissions, lease, effect, Delivery, and push-delivery diagnosis. Its process exit status follows `ok`, while `degraded` preserves nonblocking conditions that still need operator attention. |

### `/ready` Dispatcher contract

The unauthenticated response contains a safe `dispatcher` projection, never an
owner token, credential, private path, or raw error. `ready=false` with HTTP
503 names one or more bounded issues:

- `dispatcher_not_running`: the background Dispatcher is stopped;
- `dispatcher_cycle_error`: its latest cycle has a sanitized error code;
- `dispatcher_cycle_stale`: no valid cycle timestamp exists, or its age exceeds
  `max(60 seconds, 2 * dispatcher_recovery_scan_seconds + 5 seconds)`; or
- `dispatcher_status_unavailable`: the bounded Dispatcher status projection
  could not be read, so readiness fails closed without returning raw details.

The projection includes `running`, `last_cycle_at`, sanitized `last_error`,
`cycle_age_seconds`, `stale_after_seconds`, and `issues`. It also exposes only
bounded counts for queued, leased, active, and unknown-outcome Deliveries plus
pending push deliveries. Those counts support unauthenticated readiness
diagnosis; they reveal no owner capability and do not, by themselves, make the
Dispatcher cycle unhealthy. Use `cao-a2a doctor` for the deeper outcome gate.

### `cao-a2a doctor` outcome contract

`ok=false` and exit status 1 mean at least one blocking condition, integrity
failure, or permission failure exists. `ok=true` returns exit status 0, but
`degraded=true` still requires the named conditions to be understood before an
operator describes the system as fully healthy.

| Durable observation | Condition | `ok` | `degraded` | Operator meaning |
| --- | --- | --- | --- | --- |
| Effect status `unknown` | `unknown_effect_outcome` | false | true | Preserve the unknown effect and reconcile authoritative evidence; never retry it blindly. |
| MessageDelivery status `dispatched` | `unknown_delivery_outcome` | false | true | The handoff may have occurred; resolve from target evidence before any retry. |
| A2A push delivery status `dead` | `dead_push_delivery` | false | true | A required notification exhausted delivery and needs explicit repair or reconciliation. |
| Effect status `started` | `effect_in_progress` | unchanged | true | Work is still in progress, not yet an unknown outcome; inspect before shutdown or close. |
| Ordinary MessageDelivery status `dead` | `dead_delivery_history` | unchanged | true | Terminal delivery history is visible for diagnosis but does not alone block `ok`. |

The result keeps separate counts for started and unknown effects, unknown
Delivery outcomes, dead ordinary Deliveries, and dead push deliveries.
`unresolved_effect_operations` remains their started-plus-unknown compatibility
total; operators must use the split fields and condition arrays for decisions.
Do not reinterpret a blocking condition as user-owned work merely to obtain a
green diagnostic.

## Backup and restore

`cao-a2a backup [destination]` branches before `Database` construction, so it
never creates or migrates the source schema. It validates the owner-only CAO
application/schema identity and integrity, uses SQLite online backup into a
private temporary file, verifies the backup identity, fsyncs it, atomically
replaces the destination and fsyncs the containing directory. The controlled
Dashboard lifecycle upgrade uses the same primitive with replacement disabled
for its plan-bound final backup. Upgrade preflight also requires the loaded
Control Plane arguments to match the reviewed plist and its single explicit
owner-only config to contain an absolute `state_dir` resolving to that same
plan-bound database; defaults, tilde-relative values, and environment
overrides cannot redirect the restart to an unprotected database.
Before that final backup, the lifecycle controller binds the complete Edge and
Control Plane process groups to macOS audit tokens, proves every exact root and
child execution has exited, and requires both groups to be empty. A stalled
graceful shutdown is escalated only through those tokens; unknown new members
fail closed before the database can be migrated or a replacement daemon can
start.

Restore only while the daemon is stopped:

1. preserve the existing state directory;
2. replace `control-plane.sqlite3` with a verified backup;
3. remove stale `-wal` and `-shm` files;
4. run `cao-a2a doctor`;
5. start the daemon and check `/ready`.

## Retention

`cao-a2a prune` removes handled old messages and old events only after excluding
durable authority that remains executable. The exclusions are deliberately
narrow: a canonical recovery notification for an open
`system_reconciliation` Boundary; the path- and content-free
`artifact.content_read` record that enforces exact chunk-read replay; an event
sequence still referenced by a provider circuit; and the exact conversation
close authority chain needed for a later ordinary or provider continuation.
Both current and legacy close receipts retain that chain. Copied or malformed
recovery messages, unrelated handled messages, ordinary cancellation/stop
history, and unrelated events remain eligible for normal retention pruning.

## Dispatcher

The Dispatcher leases and processes up to `dispatcher_concurrency` runtime
MessageDelivery and push-delivery records per cycle. An expired pre-dispatch
lease is safely requeued. A post-dispatch uncertainty remains `dispatched` and
must be resolved from target evidence before retry. This distinction prevents
duplicate prompts after a crash between runtime handoff and database
acknowledgement.

Delivery ordering serializes only an active handoff. A Worker's unknown
Delivery or effect remains a blocking unknown outcome. For an attached CAO
wake, however, a `dispatched` predecessor whose serialization lease is absent
or expired and no longer has active Dispatcher/runtime ownership remains
immutable unknown evidence but cannot permanently hide a later, independently
committed state-change wake on the same conversation. The exact unknown
Delivery is neither retried nor rewritten. It is also excluded from the
attachment inbox and cannot be acknowledged by ID; a later CAO turn must not
accidentally convert it to handled history while draining the lane. A handoff
still owned by the running Dispatcher or a busy CAO runtime blocks the lane
even if its short claim lease elapsed during a longer model turn. Therefore a
completion queued behind active ownership may be transient; one still hidden
after ownership ended and a fresh successful Dispatcher cycle indicates a
stale or unhealthy Dispatcher and must not be repaired by retrying the unknown
Delivery.

Before claiming CAO wakes, the Dispatcher also coalesces an unclaimed queued
`progress` wake when the same Work and Attempt already has a later question,
blocker, completion, or recovery Boundary on the same attachment. Only the
wake Delivery becomes terminal history with
`cao_progress_wake_superseded_by_boundary`; the original progress Message,
Event, Attempt, and all Worker evidence remain immutable. This prevents a
stale progress turn from delaying the actionable Boundary without turning
silence or elapsed time into lifecycle evidence.

`cao-a2a effect run` computes the exact argv and working-directory digests,
reserves authority, executes without a shell, and records only an exit code and
output digest. `cao-a2a effect recover` marks reservations left `started` by a
prior process as `unknown`; matching effects remain blocked until verified.

An expired CAO ReasonerTurn is recovered by a bounded deadline scan. The old
turn becomes immutable `abandoned` and its incomplete delivery is superseded
with evidence. At most one recovery Message per open Boundary may re-arm the
CAO; a later interruption exhausts that shared recovery budget and waits for
an actual state change. This scan never infers completion or sends routine
status probes.

## Canonical authority

The Control Plane database is the sole authority. Its only supported mode is
`canonical`; domain writes, effects, runtime dispatch, and supervisory
decisions all pass through the same transaction boundary. An alternate writer,
shadow scheduler, or operational fallback is a deployment error, not a mode
that the running system can enter.

## Token rotation

`cao-a2a agent rotate-token <principal-id>` invalidates the previous token immediately. Update client configuration atomically when uninterrupted operation is required.

## Logging and audit

Uvicorn logs process and HTTP failures. Domain history, message delivery, task transitions and effect authority are durable database records and should be inspected there rather than reconstructed from log text.
