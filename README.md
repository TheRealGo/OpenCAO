# OpenCAO

An open-source, local-first control plane for supervising Codex Workers on macOS.
The Control Plane is the single canonical authority for Work, Worker lifecycle,
delivery, review, and effects.

## Architecture decision

For multiple agents running on one trusted host, the preferred layout is one loopback-bound durable daemon:

```text
user ↔ existing CAO conversation
        │
        ├── MCP 2026-07-28 (current, stateless)
        └── A2A 1.0 JSON-RPC, HTTP+JSON and SSE
                         │
                         ▼
              CAO Control Plane daemon
              ├── SQLite source of truth
              ├── WorkItems and Attempts
              ├── ordered inbox/outbox + ACK
              ├── goal versions and immutable history
              ├── evidence, artifacts and reviews
              ├── runtime leases and durable deliveries
              ├── safe retry, unknown-outcome fencing and push delivery
              ├── append-only event log
              └── exact effect-authority ledger
                         │
                         ▼
               managed Workers
           Codex App Server / Claude Code
```

The normal operating path is `user ↔ existing CAO conversation ↔
cao_control_plane MCP/A2A ↔ Worker`. MCP/A2A is the sole steady-state
agent-control path, not the durable state store or wake-up mechanism. The
daemon owns persistence and delivery. Managed Workers report progress,
questions, blockers, artifacts, and completion through `cao_report`; the daemon
queues the resulting CAO message and Boundary for a fenced CAO disposition.
A2A is the optional peer-agent boundary. This separation prevents an MCP client
session, an LLM context, or a terminal screen from becoming the source of
truth.

## Invariants

- CAO remains the holder of user intent, delegation judgment and independent review.
- A Worker completion claim is declared evidence, not verified completion.
- CAO review and the requester decision recorded from the existing CAO conversation are separate transitions.
- `WorkItem` is stable; retries and corrections create new `Attempt` records.
- Messages use crash-safe durable delivery with a pre-handoff state, unknown-outcome fencing, sequence numbers, idempotency, explicit ACK, and handled evidence; uncertain handoffs are never blindly retried.
- Each CAO decision lease is bound to one exact unresolved Boundary; every actual expiry or incomplete turn creates one durable successor wake, while unchanged scans create none.
- The database has one authority mode, `canonical`; no alternate scheduler,
  migration writer, or fallback ledger may dispatch Work or effects.
- Immutable Goal and Task packet digests follow an assignment through Worker acknowledgement, Boundary/Reasoner handling, completion claim, CAO review, and the exact requester decision CAO records from its conversation.
- A stale Worker cannot report against a newer goal version.
- A Worker is not assignable until its exact launch has exchanged a one-use
  enrollment ticket, discovered the required MCP tools, and sent a
  generation/sequence-bound heartbeat. Principal bootstrap tokens cannot use
  Worker context, inbox, ACK, handled, or report operations.
- Remote and destructive effects are authorized only at the actual effect boundary.
- Failure of a runtime adapter does not discard durable messages or disable reads.

## Included capabilities

- SQLite WAL, `synchronous=FULL`, foreign keys, schema migrations, atomic backups, integrity checks and retention pruning
- scoped CAO, Worker, user, external and dashboard principals
- scrypt-hashed credentials, one-use Worker launch tickets, short-lived
  launch-bound Worker bearers, and encrypted callback credentials
- versioned WorkItems, immutable goal revisions and multiple Attempts
- ordered messages with correlation, causation, idempotency and receipts
- structured progress, question, blocker, artifact and completion-claim reports
- one current stateless MCP protocol on the single `/mcp` endpoint
- stdio-to-HTTP MCP bridge so all agents share one daemon
- A2A 1.0 Agent Card, JSON-RPC, HTTP+JSON, Task listing, streaming and push configuration
- bounded concurrent runtime dispatch with leases, safe pre-dispatch retries, outcome-unknown fencing, and dead-delivery visibility
- managed Codex App Server and Claude Code Workers with scoped MCP enrollment
- separate exact/standing effect grants with unresolved-outcome protection

## Install on macOS with Codex Desktop

This distribution targets **macOS and Codex Desktop**. Install Python 3.11 or
later and Codex Desktop, sign in to Codex with your own account, and keep the
app running. The managed runner uses the Desktop app's bundled Codex executable
before a separate CLI on `PATH`. The release check uses Codex app-server
0.153.4 and its experimental persistent thread queue; older hosts without
`thread/queue/add` are unsupported. The live check below verifies compatibility
with the host actually installed on your Mac.

Clone the repository and install it:

```bash
git clone https://github.com/TheRealGo/OpenCAO.git
cd OpenCAO
python3 -m venv .venv
source .venv/bin/activate
python -m pip install .
cao-a2a setup
cao-a2a run-local
```

`setup` creates a new, private installation. It refuses to overwrite an existing
configuration or state directory. Keep the terminal running; Ctrl+C stops the
Control Plane and Dashboard together. Restart them later with `cao-a2a run-local`.
No Cloudflare account, domain, tunnel, Docker, or remote server is required.

| Created locally | Purpose |
| --- | --- |
| `~/.config/cao-a2a/config.toml` | This installation's settings |
| `~/.local/state/cao-a2a/` | Private database, credentials, output and runtime state |
| `~/.local/state/cao-a2a/codex-mcp.toml` | Secret-free Codex MCP configuration with an absolute executable path |

The installer uses owner-only directories and files. Keep the entire state
and configuration directories private. Share the source checkout, not these
runtime directories. Keep the virtual environment at the installed location;
if you move it, regenerate the MCP command paths for that installation.

For another installation or occupied default ports, select fresh paths and
ports. Use the same `--config` with every later command:

```bash
cao-a2a --config "$HOME/.config/cao-test/config.toml" setup \
  --state-dir "$HOME/.local/state/cao-test" --port 8878 --dashboard-port 8879
cao-a2a --config "$HOME/.config/cao-test/config.toml" run-local
```

## Connect the existing Codex conversation

In another terminal, activate the same virtual environment and display the
configuration generated for your installation:

```bash
source .venv/bin/activate
cat "$HOME/.local/state/cao-a2a/codex-mcp.toml"
```

Add that MCP server section to your Codex configuration, preserving other
settings. If a `cao_control_plane` section already exists, update that one
section rather than adding a duplicate. Setup does not edit Codex configuration
for you. Reload the MCP connection in Codex after saving it.

The generated command contains the absolute Python executable and the exact
CAO configuration file. This also works when the desktop application's `PATH`
does not include the virtual environment. The ordinary bridge starts without
a bearer; it obtains a conversation-scoped credential through the owner-local
attachment socket and holds it only in memory.

In your existing Codex conversation, ask CAO to use this sequence:

1. Read the current `CODEX_THREAD_ID`, call `cao_start` with that exact ID, and
   confirm attachment with `cao_list_managed_workers`.
2. Use `cao_new_worker_thread` with the existing project directory you selected.
   The default runner is Codex. Private placement configuration is generated
   locally on first use; no author's workspace registry is needed.
3. Give that exact `worker_thread_id` a bounded objective using
   `cao_instruct_worker_thread`. Provide acceptance conditions and scope limits.
4. Read the full Worker result, perform CAO review, and record your acceptance
   separately. Finish or delete the exact disposable Worker when appropriate.

For a first check, use an empty directory and ask a Worker to reply with one
fixed marker without editing files or using external services. Verify that the
result appears in the conversation and Dashboard. A queued instruction or a
Worker completion claim alone is not evidence of accepted completion.

Codex model access belongs to your account. The default profile includes
`gpt-5.6-terra`; select a supported model from your configured profile when
needed. See [managed Worker provisioning](docs/managed-worker-provisioning.md)
for profile and lifecycle details. Administrative `agent create`, `runtime
register`, and `work assign` commands are advanced interfaces; ordinary Work
uses the attached conversation route above.

## Open the local Dashboard

In the activated terminal:

```bash
cao-a2a dashboard-link
```

Open the printed one-use link in your browser. The Dashboard is at
`http://127.0.0.1:8769/dashboard/`; the sign-in link establishes a read-only
session without exposing the upstream credential. Generate a new link when a
link has been consumed or expired. Do not share sign-in links or cookies.

The Dashboard shows objectives and structured reports in full, with folding,
current work and scrollable history. The supervising Codex conversation reads
full captured provider answers. Authentication is required for Dashboard data
endpoints even on localhost. The Control Plane listens on `127.0.0.1:8768`; `/health` and `/ready`
are available for diagnosis. API documentation remains disabled by default.

Optional remote access is configured with **your own** infrastructure and
credentials; see [Dashboard operator edge](docs/dashboard-operator-edge.md).
It is separate from this local quickstart. The repository includes no account,
domain, access policy, tunnel credential, or deployed Dashboard belonging to its
author.

## Verify and distribute the source

For development and the ordinary offline-model checks:

```bash
python -m pip install -e '.[dev]'
python tools/check_repository_hygiene.py
python tools/check_publication.py
ruff check .
mypy src/cao_control_plane
pytest
```

The opt-in [real Codex release check](docs/runtime-adapters.md#live-vendor-acceptance)
starts a fresh local installation and an actual managed Worker. It uses your
subscription capacity, has no file-changing objective, and verifies captured
output, independent fixture review, requester acceptance, Dashboard access and
Worker cleanup. A skipped live test is not a passed compatibility check.

[tools/check_publication.py](tools/check_publication.py) checks the distributable
file set for private file types, owner paths and credential patterns. It also
creates a source-only archive with a SHA-256 manifest:

```bash
python tools/check_publication.py --archive ../OpenCAO-source.zip
```

The archive contains no Git history, virtual environment, private state or
ignored operational files. The checker refuses an existing output archive and
refuses suspicious or unexpected source files. Review new fixtures and docs
before publication as pattern checks cannot prove the absence of every kind of
private information.

## A2A behavior

- A2A protocol version: `1.0`
- The A2A client surface requires a CAO principal; it is not a user-to-Worker bypass.
- `SendMessage` blocks until terminal state or input is required unless `configuration.returnImmediately` is `true`.
- `SendStreamingMessage` and `SubscribeToTask` use SSE.
- A2A Task identity remains stable across internal Attempt retries.
- Task listing is authorization-scoped, cursor-paginated and ordered by latest status timestamp.
- Push callbacks receive an A2A `StreamResponse` with `Content-Type: application/a2a+json`.

See [docs/protocols.md](docs/protocols.md).

## Operations

```bash
cao-a2a status
cao-a2a doctor
cao-a2a backup
cao-a2a prune
cao-a2a dispatcher status
cao-dashboard lifecycle restart --help
```

An explicit CAO system restart or update is an owner-local lifecycle effect,
not a conversation close. `cao-dashboard lifecycle restart` is the discoverable
alias of the controlled upgrade executor: it takes the reviewed offline
backup, fences the exact old Control Plane and Dashboard process groups,
starts both services, and verifies the target release, schema, MCP catalog,
and read surfaces. It still executes when the target release already matches;
identity equality alone is not restart evidence. Print the plan first and
execute the same arguments only with exact owner authorization. See
[Dashboard operator edge](docs/dashboard-operator-edge.md#shared-system-restart-is-not-conversation-close).

See:

- [CAO documentation map](docs/index.md)
- [CAO operator runbook](docs/cao-operator-runbook.md)
- [Architecture](docs/architecture.md)
- [Protocols](docs/protocols.md)
- [Runtime adapters](docs/runtime-adapters.md)
- [Dashboard operator edge](docs/dashboard-operator-edge.md)
- [Security](docs/security.md)
- [Operations](docs/operations.md)

## Validation

```bash
python -m compileall -q src
python tools/check_repository_hygiene.py
pytest
ruff check .
mypy src/cao_control_plane
python -m build
```

The hygiene check also rejects raw `sqlite3.connect(...)` transaction contexts
in runtime code, tools and tests: they commit or roll back but do not close the
connection. Use `closing(sqlite3.connect(...)) as connection, connection` when
both transaction handling and deterministic release are required. Keep resource
warnings enabled; Python 3.13 can report a leaked connection during a later test.

## License

Apache-2.0.
