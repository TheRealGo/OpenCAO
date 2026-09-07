# Managed Worker provisioning

The conversation-facing route creates one empty managed Worker in an exact
existing Directory:

```text
cao_start
  -> cao_list_managed_workers
  -> cao_new_worker_thread
  -> cao_instruct_worker_thread
```

New and instruction are deliberately separate. New creates lifecycle state;
instruction creates one independent Work record. The Control Plane never
infers reuse, replacement, parallelism, closure, or a first Goal from a
Directory, display name, similar objective, or historical Work.

## New

`cao_new_worker_thread` accepts an absolute existing Directory, a runner
(`codex` by default or `claude`), optional safe public labels, and a required
idempotency key. The owner-private placement edge resolves and seals the
Directory before any runtime authority is issued. The raw path is never stored
in Control Plane task state, returned by MCP, exposed by the Dashboard, or
placed in a Worker packet.

Each successful non-replayed New creates a distinct `worker_thread_id`, even
when another Worker already uses the same Directory and runner. This is the
same freedom as starting another terminal process in that Directory. Capacity
or owner policy may reject the concrete launch, but no saved Worker name,
Directory match, prior Work, runtime, or native session is used to choose or
veto the lifecycle operation.

The server-owned runner profile validates model and reasoning effort. Omitted
values use the current server default. Omitted display names use the bounded
`Codex Worker` or `Claude Worker` label and never leak the Directory basename.
The result exposes only the opaque Worker identifier, lifecycle state,
generation, and safe labels.

## Instruction

`cao_instruct_worker_thread` accepts the exact public `worker_thread_id`, a
bounded objective, and a required idempotency key. Title is optional and goal
maturity defaults to `unset`. Every committed instruction creates a distinct
Work bound to the current CAO attachment for report and review delivery.

An active Worker may accept another instruction while busy or disconnected.
The instruction is durable before runtime delivery and remains queued for that
exact Worker lifecycle generation. Existing unsettled Work does not cause the
Control Plane to create a replacement Worker or reject a separate requested
task. A dispatched or outcome-unknown Delivery remains fenced and is never
silently rebound or retried.

## Runtime enrollment

New creates the managed Worker specification and the first fenced runtime and
enrollment epoch. It does not launch a model turn by itself. Dispatch of the
first instruction issues a one-use launch ticket; the Worker exchanges it
through the owner-local broker, discovers the exact Worker MCP tool catalog,
and publishes an ordered heartbeat before reporting.

Credentials, launch commands, provider-native thread identifiers, enrollment
IDs, runtime locators, and Directory paths are private implementation details.
CAO selects only the exact public `worker_thread_id`.

## Failure and recovery

Failure before MCP or Assignment submission can advance a fresh fenced
connection epoch for the same logical Worker only when the durable boundary
proves that continuation safe. An unknown handoff is preserved as
`system_reconciliation`; it does not block an unrelated New, instruction,
Finish, Resume, or Delete selected by CAO.

Runtime recovery never uses Worker Resume. Resume is lifecycle-only for an
explicitly archived Worker. It never chooses another Worker from Directory,
name, objective, model, runtime, or history.

## Required evidence

Provisioning is complete only when durable state shows the new active Worker
and `cao_list_managed_workers` returns that exact public identifier to the
attached conversation. Worker execution additionally requires the exact Work,
Attempt, Delivery, enrollment handshake, and Worker report evidence. A process
exit, terminal prompt, Dashboard row, or launch response alone proves none of
those transitions.
