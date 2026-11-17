# Managed Worker transport

## Owner-local publication constraints

`runtime.worker_publication_forbidden_terms` is an optional owner-local array
of private names. It is not forwarded into purposes or task packets. For managed
Claude launches, a session-local Git hook path checks actual staged additions,
commit messages and all newly pushed commits, including an intermediate commit
whose text was removed at the tip. Deleted lines do not block a correction.
Existing executable repository hooks still run; no repository or user-level
configuration is edited. Diagnostic reads and private evidence remain available.

The launch-local `gh` command checks actual public-text arguments, body files
and stdin, including calls from scripts. Body files are forwarded as the same
checked bytes, rather than reopened after validation. Claude's native
`PreToolUse` checks GitHub/Slack MCP text parameters and ordinary Git bypass
options. Its native Bash filter is best-effort, so the handler only parses
commands containing a possible explicit Git hook override, including quote-joined
spellings. Unrelated local shell preparation is not publication input; quoted
heredoc contents must not fail a general shell parse before the command runs.
Actual publication bytes remain checked by the Git hooks and `gh` command.
Missing or unreadable publication input is
rejected before submission with a generic explanation and no approval prompt.
Automatic title/body generation is not a checked payload: use explicit text.
The native hook uses `onFailure: block` and requires Claude Code 2.1.295 or newer.
These boundaries prevent accidental disclosure on those supported paths; they
are not a sandbox for arbitrary scripts/network clients or protection against
deliberate hook bypass. Empty configuration leaves launch behavior unchanged.
Post-update E2E must prove both rejection and clean continuation on the installed
provider; a saved setting or a clean assignment alone proves neither.

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

The owner-local native adapter consumes the official `thread/status/changed`
stream after a read-only attachment. Token streaming does not cause repeated
head reads. The adapter never resumes a writer or reads transcript bytes.
One real status transition renews the relevant recovery observation; the supported
native history API must still prove exact input identity and terminal chronology.
A conversation owned by another native host uses OS filesystem change events
on its provider-issued path; these are only hints and never transcript reads.
The supported history API verifies a changed terminal head with chronology before
renewing supervision. Native `kqueue` on macOS and `inotify` on Linux also report
cross-process database writes; SQLite `data_version` verifies an actual commit
before waking the Dispatcher. There is no filesystem polling fallback.
Connection loss triggers connection recovery with backoff; it does not create a
new requester grant or repeat an unknown effect.

There is no scheduled recovery scan. The Dispatcher waits for a commit, a native
head transition, a claimed job's completion, or the next actual persisted retry,
lease or observation deadline. `dispatcher_recovery_scan_seconds` remains accepted
in older configuration files for compatibility and no longer schedules scans.
A healthy idle subscription may wait indefinitely; old cycle timestamps alone do
not make that wait unhealthy. Subscription failure is diagnosed separately from
ordinary command and lifecycle authority.
Native subscription connection and replies use the configured
`runtime_mcp_startup_timeout_seconds`; initialization is covered with real
Settings in addition to isolated transition tests.

Instruction review reports real source/model progress through request-scoped MCP
progress notifications. HTTP relay reads have no independent model-computation
wall timeout; connection and write deadlines still apply. Managed native MCP
bridges use the existing Worker inactivity allowance for tool calls. A lost
client response does not cancel or replay an admitted canonical command. Once
its actual result exists, one deduplicated notice targets only the exact active
originating conversation attachment. It requests canonical readback and
continuation; it never claims that an error proves no effect occurred.

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
item but no Control Plane reasoner turn incorporated it only when an exact
persisted terminal turn also proves the input identity, terminal status and
completion chronology. One such authoritative observation records that proof and
atomically supersedes the consumed Delivery with one successor wake, after the
transaction rechecks current attachment/Delivery generations and incorporation.
Idle alone, missing timestamps, silence or a second unchanged read adds no proof.
An unsupported
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
Later structured reports may reference the same incorporated instruction after
its first report handles it. The exact task packet, Attempt, runtime lane and
acknowledgement receipt still match; the original handling timestamp and event
remain unchanged. Repeating that reference never reapplies the instruction.
The authoritative origin of that completed provider turn is the runtime's
allowlisted `last_dispatch_message_id` plus its single matching
`runtime.message_delivered` event, never whichever Delivery happens to have the
greatest sequence. A later settled status request cannot hide that turn; a later
unsettled Worker command still blocks reconciliation until its outcome is known.
An inbox input acknowledged during that exact provider turn is settled when
its ACK event precedes the provider's completion event in durable event order.
This also repairs retained receipts after an upgrade. It does not replay the
consumed input or claim Work completion. Later queued inputs keep their own
turn; missing or post-completion ACK evidence keeps its existing fence.

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

MCP/app-server startup has its own short deadline. The managed server receives
that same configured startup budget; an implicit provider default cannot impose
a shorter hidden timeout. Only `ready` for the exact thread/server establishes
readiness. An exact `failed` state stops before input submission with
`mcp_startup_failed`; transient cancellation may still progress to ready within
the deadline. Failures retain only the bounded last startup state and supported
authentication reason, never arbitrary provider errors or native locators.
When a retained thread has no matching ready transition, the adapter also makes
one bounded `mcpServerStatus/list` request for that exact thread and managed
server. Only a current `connected` runtime with a usable tool catalog and no
discovery error establishes readiness. A cached catalog with null runtime state,
another server, an empty catalog, or a Control Plane heartbeat is insufficient.
The read shares the existing startup deadline and does not poll or reset it.
Unavailable status reads retain the notification path. Failure diagnostics keep
only fixed probe status/catalog enums; successful retained dispatch records its readiness
source. Release E2E must cover later inputs on the same retained Worker and native
history across new process/broker launches, as well as initial activation.
After a managed Worker has
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

A late failed provider exit belongs to its exact input, Work and Attempt. If
that input was durably received and its Work or Attempt has already ended,
retire only the ended launch credential and return the verified route to
`waiting`. Preserve the failed result without failing independently queued
Work or rewriting its sealed packets. Its next launch performs normal MCP
enrollment. Current execution failures and typed provider capacity limits
retain their usual recovery paths; a stale enrollment generation cannot
overwrite a newer launch.
Output capture still rejects a retired Work or dispatch generation. If that
rejection also prevents recording the provider's failure terminal, continue
with the exact dispatch result settlement; do not leave the ended launch
`busy`, publish stale output as current evidence, or fail another queued Work.

This is a durable logical Worker lifecycle, not a requirement to retain an
idle OS process. For Codex, a replacement launch resumes the recorded native
thread and injects the scoped MCP server only for that thread. The Worker
remains visible and reopenable through its principal/runtime/attempt records;
it never falls back to tmux, terminal inspection, or Hooks. A clean exit,
daemon restart, crash, explicit stop, or lease expiry cannot be mistaken for a
completion report. Stop/revocation fences the credential, and a new launch is
allowed only through a fresh ticket, discovery, and heartbeat.

Before loading a retained Codex thread, the adapter reads its persistent queue.
Any prior input remains untouched and produces `runtime_thread_queue_blocked`
before the new Assignment is submitted. After scoped MCP authentication, the
exclusive headless Worker uses the official `turn/start` request with the exact
stable Delivery client identity, model and effort. The Control Plane already
owns its durable ordered inbox. Worker dispatch never submits through
`thread/queue/add` or `thread/queue/start`, and has no queue fallback. The shared
Desktop CAO notification queue has its separate transport contract above.

The successful response to that exact `turn/start` request names the created
Turn and is provider activation evidence. A matching streamed `userMessage`
may also bind the same Turn; contradictory identities are refused. Input
preparation, hooks or compaction may delay that item event after activation.
The bounded startup deadline applies until activation is established, rather
than interrupting an already started Turn while waiting for that later event.
A heartbeat or empty ACK cannot establish activation. Activation remains
separate from the Worker's evidence-bearing `execution_started` report and
business completion. The [official App Server contract](https://learn.chatgpt.com/docs/app-server)
describes the `turn/start` response and streamed lifecycle separately.

Lost or ambiguous submission responses preserve the unknown Delivery without
resubmission. After Dispatcher replacement, an overdue assigned dispatch with
no observed start/report/output receives one CAO notice. A retained live bridge
with an expired dispatch lease leaves Work/Attempt generations, runtime authority
and native input unchanged; a late authenticated report remains admissible.
An authoritatively terminal enrollment uses its separate reconciliation Boundary.
The Dispatcher maintains bounded read-only diagnosis after a notice is handled.
It correlates the exact Assignment's opaque client identity with the persistent
queue and paginated persisted user-message turn identities. Historical queue
receipts remain evidence, never a reason to replay their input through the new
Worker transport. `cao_get_work.current_attempt.activity.provider_start_diagnosis`
exposes the fixed diagnosis, input state, presence of other pending input and
whether bounded history completed. Absence or a partial scan never proves
non-execution. Terminal input observation requires actual completion chronology.
Diagnosis never activates, resumes, removes or resubmits input. Changed evidence
wakes CAO once; unchanged evidence remains quiet across restart. Finish, a newer
Attempt or an authenticated Worker report makes stale probe results inert.

An idle desktop view may retain a native thread's writer after the managed
process exits. A structured writer-conflict response admits one provider-handle
repair only after an empty native queue, one exact persisted terminal turn with
completion chronology, and the dispatch's current Work/Worker/generation/effect
fences are verified. The adapter forks history through that terminal turn with
goal continuation deferred, injects the fresh scoped MCP, and commits the new
private handle before submitting the current instruction. The logical Worker,
purpose, Work, packets, model, effort and verified Directory stay bound. The
source writer is never killed and its history stays intact. Running or unknown
turns, pending input, an accepted current input, a competing execution, or an
unknown effect forbid this repair. The failed unbound fork alone is disposable.
Ordinary same-thread reconciliation never grants permission to fork an unknown
execution or replay its effects.

A failure proven before input submission retains its native-handle observation
only as audit evidence, without binding an unpersisted `thread/start` handle as
the runtime's resumable thread. Submitted or unknown input still retains its
native context even when no Turn ID was observed. For retained records, a subsequent
authoritative `runtime_thread_not_found` before input submission may replace
that handle in the next fenced connection epoch only when normal managed
creation and exact non-submission receipts cover every historical Attempt.
Any accepted/unknown input, Worker report, provider Turn, substantive output,
effect, active credential or pending launch ticket refuses this repair. It
preserves the logical Worker, purpose, packets and old runtime/delivery records;
it never reconstructs used history, replays an old Assignment or bypasses the
current command's admission.

`wait_system` retains both admission supervision and bounded read-only provider
diagnosis, including non-empty `worker_command_outcome_unsettled` fences and
legacy empty-blocker records. An execution fence must never exclude the very
diagnosis needed to resolve it. Only actual admission changes re-arm an exact
Worker-command Boundary. Changed provider evidence can independently deliver
one non-blocking diagnostic notice to CAO while that command remains forbidden;
it does not change the held Boundary, Work generation or unknown outcomes.
CAO must choose an admitted concrete recovery operation or separately authorized
lifecycle/handoff, rather than wait for the same unknown input to resolve itself.

Read-only diagnosis distinguishes launch/transport failure, timeout, RPC
rejection, missing/archived/unloaded native threads and the exact failed probe
stage. An unloaded thread is a blocked observation, not proof that its input
never executed and not permission to resume it while diagnosing. A diagnosis
that remains unavailable updates bounded evidence quietly; it never re-arms a
Worker decision or changes the held Work/Boundary generation. Changed usable
provider observations still allow the existing generation-fenced CAO decision.

Only fixed codes and an opaque fingerprint of release, exact client identity,
provider, queue and terminal-turn evidence are durable. The fingerprint and
public diagnosis are scoped to the exact Work and Attempt, so one Work's earlier
diagnosis cannot suppress another Work's recovery. Unchanged evidence stays
quiet across restart and repeated holds. Candidate pagination prevents older
holds from starving later ones. Technical recovery needs no renewed requester
approval, and diagnosis never establishes execution or business completion.

Provider stderr is discarded at the adapter boundary instead of being left
in an unread pipe. JSON-RPC errors become fixed operational codes immediately;
provider bodies, prompts, native locators and credentials cannot enter runtime
diagnostics, the Dashboard or recovery notifications.

## Live vendor acceptance

The normal suite uses real HTTP, SQLite, the stdio MCP bridge, and real runner
process boundaries without spending model-plan capacity. Release acceptance for
the managed runner path additionally requires the opt-in live test:

```bash
env -u OPENAI_API_KEY -u ANTHROPIC_API_KEY \
  CAO_RUN_LIVE_VENDOR_E2E=1 \
  CAO_LIVE_CLAUDE_ROOT=/absolute/allowed/claude/root \
  CAO_LIVE_CLAUDE_CWD=/absolute/allowed/claude/root/empty-test-workspace \
  uv run --extra dev pytest -q tests/test_vendor_model_mcp_e2e.py
```

The test refuses API-key authentication. It requires the installed Codex CLI
to report ChatGPT subscription authentication and Claude Code to report a
`claude.ai` subscription, then gives each real model one tiny task. Each model
must discover the managed MCP server, read and acknowledge its assignment,
submit an exact completion claim, and reach CAO review plus requester
acceptance. The resulting runtime credential must remain absent from durable
state and captured output.

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
approval policy. For an owned CAO conversation bridge, its additive override
explicitly approves only `cao_delete_worker_thread` and `cao_close_conversation`.
These local lifecycle calls retain the Control Plane's authentication, scope and
exact-intent checks; their MCP client policy must not require an interactive
approval when execution uses `approval_policy=never`. An existing native CAO
conversation uses the equivalent owner-local project tool policies. This changes
no Worker bridge, shell/file permission, other server, or external-effect grant.
The [Codex MCP tool policy](https://learn.chatgpt.com/docs/extend/mcp#other-configuration-options)
supports these per-tool approval overrides. CAO auto-approves only its
generation-bound managed alias; an interactive approval from another server is
still declined rather than silently granting a remote effect. This also prevents
another app-server scope from consuming the one-shot enrollment capability before
the assigned thread starts its bridge.

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


### Recovery capability across diagnostic holds

A diagnostic `wait_system` and its automatic successor preserve the original
runtime-recovery class and failure reason. Re-arming a decision does not replace
a proven pre-submission continuation with generic reconciliation. Older generic
successors can recover that class only through the exact server-recorded causal
chain, with the same Work, Attempt and packet and an earlier wait-system decision.
All current runtime, enrollment, Delivery and effect fences are checked again;
restoring the recovery class never submits input or makes an unknown outcome safe.

Exact versioned non-submission evidence is distinct from an unmatched input.
The read-only provider diagnosis reports `runtime_assignment_not_submitted` only
when the canonical Message/runtime/Delivery-generation proof agrees with the
queue and complete structured history. A matching queued or historical input is
a conflict, not permission to replay. CAO owns any subsequent correction.
