# Managed Worker transport

Normal operation is `user ↔ existing CAO conversation ↔ cao_control_plane
MCP/A2A ↔ Worker`. This page documents only the managed Worker transports on
that path. A runtime registration binds one principal to its scoped delivery
mechanism; it never grants authority outside that principal or its effect
authority.

## Common registration

```bash
cao-a2a runtime register <principal-id> \
  --adapter <adapter> \
  --endpoint '<adapter endpoint>' \
  --metadata '<JSON object>'
```

## Dispatcher wake-up model

The Dispatcher scans once at startup and after a successful in-process
database state change. MCP, A2A, and CAO service commits use the Database's
canonical commit generation, so idle operation does not repeatedly inspect
deliveries, terminals, panes, process trees, composers, or silence.

`dispatcher_recovery_scan_seconds` is a bounded recovery deadline for
cross-process SQLite writers and time-based lease/expiry recovery; it is not a
worker or terminal polling interval. Its default is 30 seconds and its
environment form is `CAO_A2A_DISPATCHER_RECOVERY_SCAN_SECONDS`.

SQLite exposes no portable cross-process commit subscription. A separate
process modifying the same database may therefore wait until the bounded
recovery deadline; same-process service commits wake the Dispatcher promptly.
Long-running Worker turns occupy only their own concurrency slots. The
background Dispatcher retains those active jobs and immediately refills free
slots after a database commit, so a milestone report or later assignment does
not wait for an unrelated model turn to exit. The administrative one-shot
cycle still waits for every job it claimed before returning.

A meaningful Worker progress, question, blocker, or completion report queues a
wake for the exact attached CAO conversation. Each queue submission and resumed
turn owns one exact durable Delivery ID and processes only that Delivery; it
never drains later inbox entries. After transport accepts the owned Delivery,
the next queued notification can receive its own submission without waiting
for the model to acknowledge or handle the earlier one. Artifact-only
reports remain non-waking. If a CAO
launch fails while its one-use runtime ticket is provably unconsumed, the exact
Delivery stays queued with bounded backoff instead of waiting for requester
activity. Once the ticket is consumed, a failed launch is an unknown outcome
and remains fenced against blind retry.

For an existing CAO conversation, the Dispatcher connects to the canonical
owner-local App Server over its local WebSocket; it never launches a second App
Server or tries to take the conversation's rollout writer. It first submits
the exact Delivery to Codex's durable per-thread queue with one stable opaque
client-message identity. This works whether the authentic writer is the
Desktop host, a TUI, or the owner daemon itself. A matching queue ACK is durable
delivery acceptance, not model-turn completion; the existing conversation
bridge and scoped credential remain authoritative while that writer drains the
queue. Recovery later reads the exact thread plus every page of that queue and
matches only the Delivery's stable client-message identity. A matching queued
item on `notLoaded` resumes that same thread; a missing item on `active` is the
running turn. A missing item on `idle` or `notLoaded` means Codex consumed the
item but no Control Plane reasoner turn incorporated it. The first such
observation is recorded durably, and only a second observation after the
bounded recovery interval supersedes that Delivery and creates one successor
wake. This separates an actual lost turn from the non-atomic queue/start edge,
without resubmitting raw input or producing periodic messages. An unsupported
queue response is a bounded delivery failure and never
falls back to resume or a direct turn. Failure before queue submission leaves
the attachment active and retries the exact Delivery with bounded backoff;
transport loss or an unclassified response after submission is outcome-unknown
and is never retried blindly. This is the event-driven path from a Worker
completion claim to CAO evidence review; it does not require a Dashboard
refresh or a requester message to start the review turn.

A CAO runtime has a lease and heartbeat. A managed Worker runtime additionally
has an enrollment generation. It starts in `starting`. Its first assignment
must name that exact fresh runtime and doubles as the launch envelope: the
one-use ticket is exchanged, the MCP client lists the required Worker tools,
and an exact generation/sequence heartbeat makes the enrollment `ready`
before any Worker report can commit. The control plane does not spend a
separate model turn merely to bootstrap MCP. Unpinned assignments still
require an already verified runtime. Expired runtimes become `missing`; their
credentials become unusable.

The provider adapter owns output capture independently of model tool use.
Codex completed `agentMessage` items are correlated to the exact queued input,
native thread and turn. Claude uses the documented streaming JSON envelopes,
client user-message UUID correlation and assistant item UUIDs. Ordinary
assistant output is retained as private, untrusted evidence and delivered
through the durable outbox; it is never automatically accepted as task
completion. Structured `cao_report` remains optional. The
[operator runbook](cao-operator-runbook.md#provider-owned-worker-output) defines
capture fencing, explicit failure/partial states, audited content reads and
CAO Review authority. Legacy adapters without capture still use the typed
completed-turn reconciliation boundary; no adapter persists raw output into
runtime diagnostics.
The authoritative origin of that completed provider turn is the runtime's
allowlisted `last_dispatch_message_id` plus its single matching
`runtime.message_delivered` event, never whichever Delivery happens to have the
greatest sequence. A later settled status request cannot hide that turn; a later
unsettled Worker command still blocks reconciliation until its outcome is known.

An existing CAO conversation is attached idempotently by its exact principal,
native thread, project digest, model, and sandbox. Repeating that exact request
renews its lease; a mismatch conflicts. The default CAO attachment lease is one
day so a normal long-running task does not depend on a 180-second attachment.
An expired or failed exact attachment may be explicitly reattached: pending
tickets and active credentials are revoked and the attachment generation is
advanced before it becomes dispatchable again. Clients should renew with the
same attach request before expiry; automatic client heartbeats remain an
integration responsibility, not a server-side inference or hook.

Ticket issue, ticket exchange, runtime authentication, and every action made
with a CAO runtime bearer require attachment and runtime leases to be strictly
fresh (the expiry second is rejected). A WorkItem binds its own attachment in
its canonical goal/task packet, so delivery never selects the most recently
updated CAO session.

Codex and Claude launches receive a secretless stdio bridge definition. The
bearer is never placed in runtime metadata, configuration, argv, environment,
logs, or adapter output. CAO keeps the one-use ticket only in memory. The
bridge receives only a non-secret Unix-socket locator. The broker validates the
kernel-reported peer PID against the exact launched runner generation and its
current descendants before exchanging the ticket internally and returning the
short-lived bearer. Merely sharing the CAO OS user or knowing the socket path
does not authorize a sibling Worker. After discovery, the bridge renews the
same generation with ordered protocol heartbeats until its stdio connection
closes. A rejected heartbeat terminates that bridge fail-closed. Managed
Worker heartbeats renew only the generation-bound lease and runtime state;
they cannot add or rewrite durable runtime metadata, launch commands, or
environment values.

MCP/app-server startup has its own short deadline. After a managed Worker has
authenticated, its turn has no global 120-second wall-clock limit by default.
Ordered heartbeat sequence changes and the exact Attempt's durable structured
report sequence renew an in-memory activity lease; unchanged activity beyond
`worker_inactivity_timeout_seconds` terminates that launch with the fixed
`worker_inactive_timeout` reason and creates one supervisor-visible recovery
boundary without retrying the outcome-unknown assignment. A terminal or
revoked enrollment fails immediately. Operators may configure the bounded
server-owned `managed_worker_hard_timeout_seconds` emergency ceiling, but task
or Worker metadata cannot set it.

One managed launch is one credential epoch. While its runtime is `busy`, later
messages remain in the durable ordered inbox for that already-connected Worker;
the dispatcher does not start a second process, issue another ticket, or rotate
the live credential. When the runner process exits cleanly, the runtime becomes
`waiting`, its bearer is revoked, and only then may the next delivery start a
new launch generation. `waiting` therefore means a previously verified but
currently quiescent runner, not a live connection. A new delivery is not exposed
to the runner until the replacement bridge has authenticated and heartbeated.

This is a durable logical Worker lifecycle, not a requirement to retain an
idle OS process. For Codex, a replacement launch resumes the recorded native
thread and injects the scoped MCP server only for that thread. The Worker
remains visible and reopenable through its principal/runtime/attempt records;
it never falls back to tmux, terminal inspection, or Hooks. A clean exit,
daemon restart, crash, explicit stop, or lease expiry cannot be mistaken for a
completion report. Stop/revocation fences the credential, and a new launch is
allowed only through a fresh ticket, discovery, and heartbeat.

## Live vendor acceptance

The ordinary suite uses HTTP, SQLite, stdio MCP, and process boundaries without
model inference. The public macOS + Codex release additionally uses this actual
opt-in test (Codex Desktop must be running and signed in):

```bash
env -u OPENAI_API_KEY -u ANTHROPIC_API_KEY \
  CAO_RUN_LIVE_VENDOR_E2E=1 \
  python -m pytest -s tests/test_public_codex_install.py
```

The test refuses API-key authentication and verifies ChatGPT subscription
login. It creates fresh private configuration, launches the local Control Plane
and Dashboard, attaches a disposable native Codex conversation via the real
stdio bridge, and instructs a real managed Codex Worker through New/Instruct.
The model receives only a bounded no-effect exact-marker objective; it must
enroll through managed MCP and produce provider-captured output.

A deterministic supervisor fixture reads the full captured result, compares the
marker, records review before completion disposition, and then records the
fixture's separately preauthorized requester decision. The authenticated
Dashboard must show the result. Worker deletion and native fixture archival run
in cleanup. This verifies the integration and acceptance transitions; it does
not claim to evaluate a CAO model's independent judgment. Claude is not required
for this public target. Without explicit opt-in this test is skipped, not passed.

The Desktop-bundled executable takes precedence over an unrelated `codex` on
PATH, keeping Worker transport aligned with the installed Desktop queue API.

## Codex App Server

Adapter name: `codex-app-server`

```json
{
  "command": ["codex", "app-server", "--stdio"],
  "cwd": "/path/to/project",
  "model": "<configured-model>",
  "approval_policy": "never",
  "sandbox": "workspace-write",
  "timeout_seconds": 120,
  "environment": {}
}
```

The adapter:

1. starts App Server without a shell;
2. sends `initialize` and `initialized`;
3. starts or resumes a thread;
4. starts a turn with the rendered durable message;
5. consumes notifications until `turn/completed`;
6. stores the native thread ID and bounded event output;
7. terminates the local App Server process cleanly.

Those steps describe a managed Worker launch. An attached Codex Desktop CAO
conversation is different: the adapter initializes one owner-local WebSocket
client and first adds the sealed wake to the exact thread's persistent queue.
The canonical queue host and the GUI or TUI retaining that thread's writer may
be different processes sharing the same queue. A local `thread/resume` failure
must not prevent durable queue admission. Separate activation observes accepted
input and may materialize a `notLoaded` thread with `excludeTurns=true`; it
does not submit input or downgrade an existing queue ACK. A busy actual owner
preserves FIFO until its current turn ends. Activation failures are isolated per
attached conversation, so one unavailable cold thread degrades health without starving another
attachment. Replacing the internal wake runtime does not rewrite or resubmit an
already-accepted Delivery; activation uses the attachment's current authentic
runtime while preserving the Delivery's original acceptance provenance. The
adapter observes the exact stable client-message identity across paginated
queue results. Queue absence plus local `idle`/`notLoaded` is not terminal
evidence: a different host may still own the active writer. After an item leaves
the queue, activation reads at most five persisted turn-summary pages of twenty
turns, binds the exact user-message client ID, and requires an authoritative
`completed`, `failed`, or `interrupted` turn. Missing, running, unsupported, or
ambiguous history never creates a successor. Only two identical digest-bound
terminal observations for the current attachment and Delivery generation,
separated by the recovery scan interval, admit one unresolved-Boundary successor.
Expected socket truncation and I/O failures are typed activation failures and
cannot prevent independent Delivery admission. Activation does not inject a
second Control Plane bridge, rotate the conversation credential,
or blindly submit the old input again. Reconnection may re-arm only a
provider-unaccepted failed Delivery. A Delivery with durable
`message.delivery_superseded` evidence remains terminal history forever, and
any queued, in-flight, delivered, or acknowledged successor blocks an older
candidate from reactivation.

The adapter passes the managed Control Plane MCP stdio bridge only in the exact
`thread/start` or `thread/resume` request. It uses a stable, runtime-scoped
`mcp_servers.cao_managed_<opaque digest>` dotted override, preserving all other
user-, project-, plugin-, and app-scoped MCP configuration. A nested
`mcp_servers` object would replace the whole effective table and is therefore
not used. A fixed same-name override is also unsafe because App Server
deep-merges inherited environment and enabled state. The runtime-scoped name
keeps the managed bridge isolated while leaving an existing conversation bridge
unchanged and outside the managed bridge's automatic-approval boundary. The
adapter does not call `config/read` to reconstruct the table, because effective
MCP configuration can contain credential values, and it does not edit global
or project Codex configuration. App Server therefore continues to enforce each
preserved server's `enabled_tools`, `disabled_tools`, OAuth state, and tool
approval policy. CAO auto-approves only its generation-bound managed alias; an
interactive approval from another server is still declined rather than silently
granting a remote effect. This also prevents another app-server scope from
consuming the one-shot enrollment capability before the assigned thread starts
its bridge.

Command, file, permission, and unknown approval requests are declined. The
adapter accepts only the MCP-tool approval emitted for the exact injected
`cao_control_plane` server; its Worker tools are annotated as local and
non-destructive. This lets the Worker report through its scoped runtime
credential without crossing the separate effect-authority boundary.

## Claude Code

Adapter name: `claude`

```json
{
  "command": ["claude"],
  "cwd": "/path/to/project",
  "model": "sonnet",
  "permission_mode": "default",
  "allowed_tools": ["Read", "Edit", "Bash"],
  "timeout_seconds": 120
}
```

The adapter runs print mode with JSON output and uses `--resume` when a native session ID exists. It does not enable permission bypass flags automatically.
It resolves the default Claude Code executable before dispatch, using the
service PATH first and the native user-local installation as a macOS fallback.
The resolved path is ephemeral; it is neither runtime metadata nor durable
diagnostic content. An explicit server-owned command remains the higher-priority
compatibility contract.
For each managed launch it supplies an inline `--mcp-config`; the config adds
the secretless control-plane bridge without hiding project or user MCP servers
that the Worker may need for its task. It contains only the one-shot capability
socket locator, never the ticket or bearer.

## Retired tmux transport and transcript visibility

tmux is not a runtime adapter and cannot be registered or dispatched by the
Control Plane. Normal Worker operation uses the scoped MCP/A2A path; the
Control Plane does not send keys, write tmux buffers, inspect terminals, parse
composers, observe process trees, or infer a boundary from silence.

Historical terminal records are outside the Control Plane. They are not an
operational fallback.

Terminal transcript visibility is replaced by structured Worker MCP reports
(progress, question, blocker, artifact, and completion claim), preserved
artifacts, and the sanitized Dashboard read model. The read model preserves the
latest report kind, bounded summary, reported time, progress stage, and next
boundary as distinct operator fields. Raw adapter output and terminal
transcripts are never restored to durable state or the Dashboard.

## Retry and unknown-outcome behavior

A failure proved to occur before adapter invocation records the error and may
schedule bounded backoff. Immediately before adapter invocation, the Delivery
is committed as `dispatched`. If the adapter or process then fails before the
handoff result is durably recorded, the Delivery stays `dispatched` and blocks
later recipient messages. Target evidence must resolve it as `delivered`,
`not_delivered` (which safely requeues with a new generation), or `dead`.
Runtime stdout and stderr are drained concurrently and truncated before
persistence. No runtime heartbeat turns an outcome-unknown Delivery into a
retry.

For managed Workers, each quiescent-to-active launch gets a new credential
generation. A launch that exits successfully without completing MCP discovery
and heartbeat is still failed. A post-start failure revokes the generation and
freezes the delivery as outcome-unknown. Stopping the runtime wins the launch
generation fence: a late adapter return cannot resurrect it. Stop and lease
expiry revoke credentials and pending tickets before safely cleaning transient
capability sockets; an expired enrollment is terminal so a fresh registration
can recover without reusing stale authority. Every dispatcher entry point,
including an administrative one-shot cycle, expires leases before claiming a
delivery; ticket issuance also checks both runtime and enrollment lease expiry
inside its transaction. Ticket exchange, tool discovery, and every heartbeat
independently enforce those same leases, so an MCP request cannot revive an
epoch while the dispatcher is stopped or delayed. The public HTTP API has no
ticket exchange route; exchange is confined to the process-bound local broker.

The broker uses process identity only to authenticate the one-time launch
handoff. It does not monitor the Worker, infer progress from processes, inspect
terminal output, or replace MCP reports as the supervision source of truth.
