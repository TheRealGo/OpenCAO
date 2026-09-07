# Security

## Network boundary

The default server binds to `127.0.0.1`. Non-loopback binds require explicit configuration; insecure HTTP on such a bind requires a second explicit opt-in. Host headers and browser Origins are allowlisted.

A2A push targets must resolve entirely to loopback unless remote callbacks are
deliberately enabled. Redirect following is disabled, preventing validation
bypass after the initial URL check.

## Identity and authorization

CAO, requester, and administrative principals receive distinct bearer tokens.
Managed Workers use a separate launch credential lifecycle: CAO creates a
high-entropy one-use ticket, SQLite stores only its scrypt verifier and launch
generation, and the raw ticket remains only in CAO process memory. A one-shot
Unix-socket broker releases the resulting short-lived runtime bearer only
after the kernel-reported peer PID matches the exact launched runner generation
or one of its current descendants. The bridge retains that bearer only in
memory. There is no public HTTP ticket-exchange endpoint.
Runtime bearers are also stored only as scrypt verifiers. Callback credentials
are encrypted with a private local key. Role and launch-generation checks live
in the domain service, not only in HTTP routes.

- Workers read and report only their own Attempts.
- Only CAO principals assign, revise, retry, or instruct Workers.
- Users see only WorkItems where `requester_id` matches their identity.
- Users can accept or reject only after CAO verification and can cancel only their own request.
- The A2A client surface is CAO-only; it cannot be used as a user-to-Worker bypass.
- Dashboard principals are read-only.
- A Worker principal token cannot rotate itself or use context, inbox, ACK,
  handled, report, or heartbeat operations. Those calls require the current
  runtime credential and, except for discovery/first heartbeat, a fresh ready
  enrollment bound to the exact runtime.
- Worker heartbeats cannot persist caller-supplied metadata. They renew only
  the fenced runtime state and lease, preventing a Worker from storing a
  credential or rewriting a later launch command through heartbeat metadata.
- Worker reports are recursively checked for CAO principal, runtime, or
  enrollment credentials before any Attempt, Artifact, Boundary, Message,
  Event, or idempotency result is written.
- Durable instruction recording may bind the exact active Worker thread and
  lifecycle generation to its current connection epoch while that runtime is
  starting or awaiting its handshake. This admission grants no runtime
  authority: actual dispatch and Worker reporting fail closed until the exact
  enrollment has authenticated, discovered the complete MCP tool contract, and
  heartbeated ready. A failed or missing connection may advance to a fresh
  connection epoch, and its pending Delivery may rebind, only after proven
  pre-MCP failure. A dispatched or unknown Delivery is never rebound or retried,
  and a pinned Delivery never falls back to a sibling runtime.

Never place bearer tokens or enrollment tickets in prompts, task metadata,
artifacts, A2A history, runtime configuration, argv, environment, adapter
output, events, or logs. The non-secret capability-socket locator is a transient
launch input and is redacted before any adapter result becomes durable.
REST, MCP, and A2A validation responses omit invalid submitted values and
attacker-controlled nested locations; credential-shaped diagnostics are
redacted again at the final protocol error serializer.

## Goal and evidence integrity

Every Worker report includes the expected goal version. Stale reports fail closed. Completion claims remain `declared`; CAO review moves evidence to `verified`; user acceptance is separate.

## Runtime execution

Managed Worker launches execute argument vectors without shell interpolation.
Runtime output is drained continuously and retained only up to a configured
bound. Timed-out launch process groups are terminated. Codex approval requests
are denied by default and cannot derive authority from task prose.

## Effect authority

Remote and destructive operations use a separate ledger keyed by principal, kind, target, action and optional content/argv/workdir digests. One-time grants are consumed atomically. A started or unknown operation blocks blind retry until remote state is verified and the record is resolved.

This is an application boundary, not an operating-system sandbox. Run the daemon and Workers as appropriately restricted OS users.
