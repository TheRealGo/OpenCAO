# Dashboard operator edge

The dashboard operator edge is a separate, read-only Web, text, and native
Codex surface for
the versioned Control Plane dashboard contract. It never reads the Control
Plane SQLite database or imports its service, MCP, runtime, or generic REST
interfaces.

## Boundary and deployment

Run the Control Plane on loopback only and give the edge a dedicated
`dashboard` principal bearer credential. The edge accepts that credential at
process startup and sends it only to these fixed upstream paths:

- `GET /api/v1/dashboard/v1/snapshot`
- `GET /api/v1/dashboard/v1/history`
- `GET /api/v1/dashboard/v1/stream`
- `GET /api/v1/dashboard/v1/work-history`

The upstream origin must be loopback, a private IP address, or an explicitly
configured private DNS host. Redirects are disabled. Browser requests cannot
choose an upstream URL or a path, and no CAO credential belongs in this edge.

For a Tailscale deployment, keep both the Control Plane and this edge bound to
loopback. Publish only the edge with Tailscale Serve HTTPS. This keeps the
Control Plane off the tailnet-facing listener while Safari receives normal
HTTPS and therefore a `Secure` cookie. Do not expose the Control Plane's
listener directly and do not enable a generic reverse proxy path.

For a Cloudflare deployment, use one remotely managed named Tunnel whose only
HTTP ingress maps the exact Dashboard hostname to the loopback edge. The
fallback ingress returns 404, and Tunnel-side Access validation is required
for the exact Access application audience. A self-hosted Access application
protects the hostname with one-time email-code authentication and an exact
owner-email allow rule. The Control Plane remains on loopback and never
appears in DNS, Tunnel ingress, or an Access application.

The Cloudflare Access assertion is not trusted merely because the request came
through Cloudflare. The edge fetches signing keys only from the configured
HTTPS team-domain certificate endpoint and verifies the RS256 signature,
issuer, application audience, expiry, issued-at time, and exact allowed email.
Only then may the read-only snapshot, history, and stream routes run without a
local bootstrap cookie. Missing, malformed, stale, wrong-application, or
wrong-email assertions fail closed. Signing-key rotation causes one bounded
key refresh rather than a fixed cache-duration outage.

`cao-dashboard` is the standalone read-edge command. Its `serve`, `snapshot`,
`follow`, and native `mcp-stdio` modes load a dedicated dashboard bearer only from an owner-owned,
regular `0600` credential JSON file; symlinks and broader permissions are
rejected. The bearer is never accepted as an argument or environment variable.
`mcp-stdio` derives only `<private-upstream-origin>/mcp` from that checked file
and exposes exactly `cao://dashboard/v1/snapshot`, so a thread-scoped Codex
MCP configuration names the command and credential-file path but never a
bearer. It does not expose Dashboard tools, generic Control Plane resources,
SSE/history cursors, or any mutation capability.
`issue` writes a `0600` hash-only, expiring, one-use record to an owner-only
`0700` bootstrap directory and prints the browser fragment URL. A running
`serve` process consumes records from that directory dynamically, so issuing a
new link does not require an edge restart.

For Tailscale Serve, this command still binds the edge to loopback and
Tailscale Serve terminates HTTPS in front of that local listener. This is an
operational deployment boundary, not an application authentication boundary:
the dashboard session and one-use bootstrap checks remain required, and the
Control Plane listener must remain loopback-only.

## Browser session and access boundary

For a non-Cloudflare local deployment, an operator may explicitly issue a
high-entropy, short-lived bootstrap link shaped like this (using the real edge
origin in place of the example):

```text
https://dashboard.example.invalid/dashboard/#dashboard_bootstrap=ONE_TIME_SECRET
```

The secret is in the URL fragment, so browsers do not send it in an HTTP
request or Referer header. The same-origin JavaScript consumes it with a
`POST /dashboard/session`, immediately removes the fragment with
`history.replaceState`, and the server creates a short-lived session cookie.
The raw bootstrap secret is stored as a hash, consumed once, and expires. The
session cookie is `HttpOnly`, `SameSite=Strict`, path-limited to `/dashboard`,
and `Secure` whenever the configured public edge origin uses HTTPS. Neither
the bearer nor a browser token is stored in JavaScript, localStorage, a URL, or
HTML.

The HTML uses same-origin external JavaScript and CSS only. The edge sets a
strict CSP that disallows inline scripts, third-party assets, framing, and
non-edge network connections. It contains no mutation, prompt, or requester
chat route.

Cloudflare owner deployments use the independently authenticated Access
boundary described above. Neither `cao_start` nor `cao_show_dashboard` mints a
browser bootstrap or transfers a bearer. The MCP client receives only the
clean public Dashboard URL.

## Supervision independence invariant

The durable Control Plane is the sole authority for conversation attachment,
Worker supervision, reports, Deliveries, Boundaries, and lifecycle actions.
Dashboard service, public access, browser authentication, viewer connection,
rendered cursor, and local GUI state are read-only projection concerns. None
of them may block, downgrade, retry, or repair a CAO attachment or Worker wake.
They also cannot trigger a shared-service restart, browser launch, tab focus,
or a Control Plane mutation.

`cao_start` verifies the exact conversation attachment, MCP catalog, and
conversation-scoped Worker listing. It returns `ready` on that evidence alone.
It may include the configured Dashboard resource link but does not probe or
present it. `cao_show_dashboard` is the explicit read-only access check. It probes
the authenticated Control Plane snapshot, loopback edge HTML, and public
Cloudflare Access boundary exactly once without redirects or retries. Success
returns an MCP `resource_link`. A tool-originated access failure returns
`isError=true`, a stable `reason_code`, and the observed component states
inside the Tool Result rather than turning the MCP connection into a protocol
error.

The Dashboard view is global across all production CAO conversations; MCP
inspection and mutation remain scoped to the currently attached conversation.
`cao_start` and `cao_show_dashboard` return that split as machine-readable
`view_scope`, `control_scope`, and routing evidence. A card absent from the
current conversation's scoped list/query results belongs to another CAO
conversation. CAO must not classify it as a stale rendering and must not act
on a similarly named Work or Worker in the current conversation. A current
rendered view never broadens the conversation's control authority.

Opening, focusing, refreshing, and closing a browser are client/user actions,
not service availability signals. A browser viewer disconnect therefore only
changes the viewer count. It never initiates GUI automation or recovery.

The browser session store persists only hashes in an owner-private directory,
so an edge restart does not invalidate an otherwise live Dashboard cookie.
The canonical Control Plane and edge LaunchAgents are continuously supervised
by their declared owner-local lifecycle, not by an MCP tool or browser event.
An authenticated internal observation distinguishes a dispatched URL from a
browser that has actually established its Dashboard event stream. The
browser acknowledges the exact cursor and snapshot digest only after rendering
that snapshot. The viewer uses those values for its own convergence
diagnostics. An active stream or rendered digest is never promoted into
Control Plane readiness and the server never reloads a client on its behalf.

## Operator behavior

The responsive page renders the server-supplied `cao_processing`,
`user_confirmation`, `stopped_or_failed`, `working`, and `ready` categories.
When all are empty it says `現在稼働中の本番Workerはありません`. Current
Work leads with objective, report and the next action; diagnostics are
conditional and collapsed. A single work-history reader provides selection,
full-text disclosure and older pages, including retained historical Work after
Worker Finish/Delete. This history does not contribute to active counts.
The [reading and information hierarchy](dashboard-read-model.md#reading-and-information-hierarchy)
is the canonical presentation and work-history contract.

No Dashboard client classifies or suppresses a Worker by inspecting its name,
state text, project, timestamp, or metadata. The Control Plane is the sole
owner of category and operator-scope decisions. Test, system-development, and
unclassified records therefore never enter the DTO, counts, compatibility
Work alias, or event history; the browser has no heuristic filter to drift
from that boundary.

Event visibility is sealed into each durable event when the aggregate still
exists. History and SSE therefore never re-resolve a past event through the
current mutable aggregate row. In particular, Worker Delete removes the
Worker and its Work from every active Dashboard section immediately while the
sanitized `managed_worker_thread.deleted` event remains available in history.
Schema upgrades backfill attributable legacy events and append one production
resynchronization event so an already-open surface fetches the migrated
snapshot.

The page uses semantic headings, lists, definition lists and native disclosure
controls. Full objective/report fields preserve paragraph breaks and are
rendered as text. Internal statuses and missing fields do not dominate the
reading surface. Closure, liveness and recovery keep their canonical meanings;
unknown values are never reconstructed. The edge independently allowlists all
four read-response shapes before a browser sees them. Snapshot updates retain
reading state and the separate history reader cannot delay SSE convergence.
Retained Work history projects only the selected Work or index page in one
fresh read transaction. The separate 30-second browser and upstream budget is
a failure ceiling, not a target latency. The ordinary live update deadlines
remain unchanged. History loading is visible, and a timed-out read leaves an
explicit retry action without blocking live snapshot rendering.

The latest report is a durable Worker handoff: either a structured `cao_report`
or a metadata-only notification backed by automatically captured provider output.
Automatic capture does not depend on the model calling a reporting tool. The
Dashboard shows the notification's bounded summary and timestamp, never raw
output, provider locators, or private content-store references. Captured text is
available only through the exact supervising CAO attachment's verified read
route; capture alone is not completion or acceptance. The timestamp makes stale
information visible. `next_boundary_summary` remains the Worker's declared next
milestone; `pending_supervisor_boundary` separately signals an explicit CAO
disposition boundary, so the UI never conflates the two.

The browser keeps one SSE connection open. It does not poll snapshots or
redraw on a healthy timer. The stream emits production `dashboard-update`
events followed by a durable `dashboard-synced` replay barrier carrying the
latest global cursor. The barrier is emitted after every catch-up pass even
when only non-operator events advanced the cursor. The browser batches replayed
updates and fetches one current snapshot when the barrier differs from its
rendered cursor. If that snapshot fetch or render
acknowledgement fails transiently, the browser retains the same pending update
and retries it with bounded backoff until it converges; it never waits for an
unrelated later event after EventSource has already consumed the durable event
ID. SSE comments, connection keepalives, and a transport-open signal never
claim convergence or trigger a fetch. After a transport error, the browser
closes that exact stale `EventSource`, serially fetches and render-acknowledges
one authoritative snapshot with the existing bounded retry path, and opens one
replacement stream from the resulting cursor. Every browser HTTP request has
an explicit deadline; a Safari request that remains pending across an Edge
restart is aborted and enters that same retry path instead of blocking its
serialized update queue forever. A second error emitted by the closed source
is ignored. This explicit recovery does not depend on browser-specific native
EventSource retry behavior, so an Edge or Control Plane restart cannot leave an
authenticated page silently behind or connected to two streams.
`resync-required` discards the cursor, fetches a new snapshot, and intentionally
opens a replacement stream from that snapshot cursor.
The text client offers `snapshot()` and `follow(after=...)` methods with the
same fixed upstream paths, server-owned categories, and actionable field set.
Native Codex reads the complete same snapshot DTO through the fixed MCP
resource; cursor and SSE semantics remain the REST/Web/text contract and are
not mirrored as additional native resources. The page has
explicit loading, empty, access-required, unavailable, and resynchronization
states; only its concise connection status uses `aria-live`, and it requires
no client-side storage.

The owner deployment is browser-neutral. No browser, exposure configuration,
or persistent service is started merely by importing, probing, or testing this
feature. Shared services restart only through the explicit reviewed
`cao-dashboard lifecycle restart` boundary.

## Owner-local service and exposure lifecycle

`cao-dashboard lifecycle` provides a typed, local-first lifecycle contract for
the two owner services and exactly one selected exposure provider:

- the Control Plane is bound to `127.0.0.1:<cp-port>`;
- the Dashboard edge is bound to `127.0.0.1:<edge-port>`; and
- either Tailscale Serve or a named Cloudflare Tunnel publishes only the edge.

The canonical plist files live beneath an owner-private (`0700`) Application
Support directory. They use launchd-compatible, owner-owned, non-writable
`0644` permissions and contain paths but no bearer value; credentials remain
in their separate `0600` file. The plists have explicit working-directory and
log paths and stable labels
`dev.cao.dashboard.control-plane` and `dev.cao.dashboard.edge`. The edge
plist names its credential and bootstrap-record files but never contains the
dashboard bearer, any other secret, an environment variable, or a log value
derived from those credentials. Existing plists are only reused when their
owner, type, mode, and exact canonical content match. A foreign, changed, or
unknown file is never overwritten.

Cloudflare mode adds `dev.cao.dashboard.cloudflare-tunnel`. Its plist contains
an absolute, owner-controlled `cloudflared` binary path and a path to an
owner-only `0600` token file, never the Tunnel token itself. Automatic
`cloudflared` updates are disabled so a shared-system restart cannot silently
change the reviewed executable. The connector exposes its readiness endpoint
only on a distinct loopback metrics port; it does not bind a LAN listener.
The Access team domain, application audience, and exact allowed email are
stored together in the owner-only Dashboard credential file. A partial Access
identity is rejected.

Start with the side-effect-free plan:

```sh
cao-dashboard lifecycle plan \
  --application-support-dir "$HOME/Library/Application Support/CAO/dashboard" \
  --working-directory "$PWD" \
  --control-plane-command-json '["cao-a2a", "--config", "/owner/private/control-plane.toml"]' \
  --credentials-file /owner/private/dashboard-credentials.json \
  --bootstrap-record-dir /owner/private/dashboard-bootstrap \
  --session-record-dir /owner/private/dashboard-sessions
```

The plan contains exact, version-observed commands for `tailscale version`,
`tailscale serve status --json`, and `tailscale status --json`, and proposes
only:

```text
tailscale serve --https=443 http://127.0.0.1:<edge-port>
```

For the named-Tunnel deployment, select Cloudflare explicitly and provide only
non-secret topology on the command line:

```sh
cao-dashboard lifecycle plan \
  --application-support-dir /owner/private/cao-state \
  --working-directory /owner/private/cao-checkout \
  --control-plane-command-json '["cao-a2a", "--config", "/owner/private/control-plane.toml"]' \
  --dashboard-command-json '["cao-dashboard"]' \
  --credentials-file /owner/private/dashboard-credentials.json \
  --bootstrap-record-dir /owner/private/dashboard-bootstrap \
  --session-record-dir /owner/private/dashboard-sessions \
  --exposure-provider cloudflare \
  --cloudflared-binary /owner/private/bin/cloudflared \
  --cloudflare-token-file /owner/private/cloudflare-tunnel-token \
  --cloudflare-hostname dashboard.example.test \
  --cloudflare-access-team-domain owner-team.cloudflareaccess.com \
  --cloudflare-metrics-port 8770 \
  --launchagent-dir "$HOME/Library/LaunchAgents" \
  --log-dir /owner/private/cao-state \
  --control-plane-log-name control-plane.log \
  --edge-log-name dashboard-edge.log \
  --cloudflare-log-name cloudflare-tunnel.log
```

The Cloudflare plan starts only
`cloudflared tunnel --no-autoupdate --metrics 127.0.0.1:<metrics-port> run
--token-file <owner-only-file>`. It never places the token in the process
arguments, plist, environment, plan, or logs. DNS, Tunnel ingress, Access
application, identity provider, and exact allow policy are separately verified
Cloudflare-side resources; the local lifecycle does not invent or broaden any
of them.

`status` checks the generated plist identities, LaunchAgent registration,
Tailscale version and Serve state, the MagicDNS/Tailnet URL, the authenticated
loopback Control Plane Dashboard API (`application/json`), and the loopback
browser page (`text/html`, not an attachment). The final check makes an iPhone
Safari content-type/download regression visible rather than silently treating
it as healthy.

In Cloudflare mode, `status` additionally requires the exact connector
LaunchAgent, a successful `cloudflared tunnel ... ready` result against the
fixed loopback metrics address, and an unauthenticated public response that
redirects only to the configured Access team domain. A loaded-but-disconnected
connector is therefore not reported as ready, and an Access redirect alone is
not mistaken for Tunnel health.

`apply` and `remove` are effect boundaries. They require either a matching,
owner-only local effect receipt for the exact plan digest and action, or both
`--execute` and `--owner-confirm`. They first re-read Tailscale state. A
conflicting Serve configuration, an attempt to expose the Control Plane, an
unparseable status, or any failed LaunchAgent/Serve command produces an
unknown outcome and stops; the command never retries automatically. `remove`
is the rollback plan: it removes the exact edge Serve mapping first, then only
the matching two LaunchAgents and canonical plist files.

No system service, Tailscale setting, or Cloudflare resource is changed by
writing a plan, running the test suite, or building the package. The operator
performs an explicit effect command only after reviewing the current plan and
status.

This deployment uses the Cloudflare Website Free plan and free Zero Trust
Tunnel/Access capabilities only. The lifecycle must not enable Workers,
storage, load balancing, paid health checks, Argo, Browser Isolation, a paid
Zero Trust seat plan, or any other paid or metered add-on. A future change that
could incur usage charges requires its exact price/cap and separate requester
approval; it is never an implicit repair or upgrade step.

### Shared-system restart is not conversation close

`cao_start` attaches one Codex conversation and presents its Dashboard; it
does not start, stop, restart, or update the shared services.
`cao_close_conversation` closes only that conversation's supervision scope; it
does not stop the Control Plane or Dashboard and must not be used as a
shared-system restart step. It cancels terminalizable Work supervised by that
conversation and revokes only its attachment; project-local Workers remain
available until explicit Finish or Delete. Conversation close cannot authorize
or substitute for either Worker lifecycle or a shared-system lifecycle action.

An explicit request to restart, freshly launch, redeploy, or update the CAO
system uses `cao-dashboard lifecycle restart`. `restart` is an explicit alias
of the controlled `upgrade` workflow below. It performs the complete
backup/fence/restart/readiness sequence even when the running and target
release identities already match. Matching release, schema, and MCP catalog
values prove version equality only; they do not prove that the requested
process restart happened. A failed or unavailable conversation attachment
does not redirect this owner-local operation to conversation close.

### Controlled code/schema upgrade and restart

`apply` remains an idempotent deployment/configuration action. It does not
restart an already loaded LaunchAgent merely because Python or static files
changed. Use the separate `upgrade` action for a reviewed code or schema
release. Use its `restart` alias when the requested effect is explicitly a
fresh shared-system process generation, including a same-release restart.

An upgrade plan binds the live database path, a new non-existing final backup
path, the current database schema identity, the exact target release/content
digest, target schema, conversation MCP catalog digest, and the exact existing
Control Plane and Edge plist paths and SHA-256 digests. Supplying an existing
plist path without its digest (or the reverse) is rejected. Omitting the
explicit paths uses the canonical lifecycle paths; if the running owner
deployment uses different plist locations, preflight fails instead of silently
adopting them. The reviewed Control Plane plist must contain exactly one
explicit `--config` argument. Preflight reads that owner-only config and proves
that its explicitly configured, absolute `state_dir` resolves to the
plan-bound database. Defaults, tilde-relative values, and state-directory
environment overrides are rejected so neither the running job nor its restart
can silently select a different database.

Print the side-effect-free plan first:

```sh
cao-dashboard lifecycle upgrade \
  --application-support-dir "$HOME/Library/Application Support/CAO/dashboard" \
  --working-directory "$PWD" \
  --control-plane-command-json '["cao-a2a", "--config", "/owner/private/control-plane.toml"]' \
  --credentials-file /owner/private/dashboard-credentials.json \
  --bootstrap-record-dir /owner/private/dashboard-bootstrap \
  --session-record-dir /owner/private/dashboard-sessions \
  --database-path /owner/private/control-plane.sqlite3 \
  --backup-destination /owner/private/backups/pre-release.sqlite3 \
  --control-plane-plist-path /owner/private/LaunchAgents/control-plane.plist \
  --control-plane-plist-sha256 CONTROL_PLIST_SHA256 \
  --edge-plist-path /owner/private/LaunchAgents/edge.plist \
  --edge-plist-sha256 EDGE_PLIST_SHA256 \
  --plan-only
```

For an explicit restart, replace `upgrade` with `restart`; every argument, the
plan format, authorization rule, and executor remain identical. The alias does
not expose a Control Plane self-stop or a separate restart implementation.

Execute the exact same arguments with either a matching owner-only `upgrade`
effect receipt or both `--execute --owner-confirm`, and without `--plan-only`.
The authorized workflow is fixed:

1. re-read the database, release, owner plist, loaded LaunchAgent arguments,
   and exact config-to-database binding;
2. take a disposable online backup, run the current `Database` migration on
   only that copy, verify the complete projection at the target schema, and
   delete the disposable copy;
3. require the currently running Control Plane and Edge to be healthy. A
   source Control Plane whose only readiness failure is its projection may be
   upgraded only when the source database is integral, authority is canonical,
   the dispatcher and Edge are healthy, and the disposable target migration
   has already rebuilt that exact database copy with a healthy projection;
   database, authority, dispatcher, target-projection, or Edge degradation is
   never bypassed;
4. capture every process in each isolated LaunchAgent process group with its
   macOS audit token, boot out Edge and then Control Plane, and confirm both
   jobs are unloaded; wait for every captured execution to exit, escalating
   only those exact audit tokens through bounded `TERM` and `KILL` phases when
   graceful shutdown stalls, then require each process group to be empty and
   both loopback HTTP surfaces to be unreachable;
5. take the plan-bound, migration-free final backup with replacement disabled;
6. bootstrap Control Plane and require `/health` and `/ready` to report the
   exact target release, schema, and MCP catalog digest;
7. bootstrap Edge and require both authenticated snapshot JSON and browser
   HTML readiness;
8. before stopping either CAO service, verify the plan-bound bundled Codex
   executable, exact owner LaunchAgent, and canonical socket's kernel peer.
   After CAO service readiness, reuse an attested current host or replace one
   proven-stale mapped-image generation through the exact targeted LaunchAgent
   path below; then verify the Codex initialize identity and submit
   `config/mcpServer/reload` with a matching empty JSON-RPC result. The
   lifecycle may report the CAO services `ready`, but current-task catalog
   verification remains explicitly `pending`.

The final backup is never taken while an old LaunchAgent root or child still
exists. PID reuse cannot redirect an escalation signal, and a new or
previously uncaptured process-group member stops the upgrade before backup
instead of being killed or ignored. No Tailscale command runs during
`upgrade`, and no stopped service or unknown effect is retried automatically.
If an effect command has an unknown outcome,
inspect the recorded plan and live state before any recovery action. The final
backup remains at the pre-migration schema for an explicitly reviewed restore;
restoring it is never an automatic response after new durable writes.

The final Codex host refresh is mandatory. The plan uses only Codex Desktop's
canonical owner-local `~/.codex/app-server-control/app-server-control.sock`;
the public lifecycle CLI accepts no alternate socket or executable. The socket
must have a real, owner-only `0700` parent and an owner-owned, non-symlink Unix
socket leaf with `0600` permissions. The bundled executable must be a real,
non-writable executable owned by root or the current owner and satisfy the
OpenAI signing requirement. Its file generation is part of the reviewed plan
digest and is checked again immediately before a host effect. The daemon
observation must report `running`, the exact canonical socket, and bounded
CLI/app-server versions.

A Desktop or owner-LaunchAgent backend may correctly omit `backend` and report
a null `managedCodexVersion`. The plan binds the unique canonical owner
LaunchAgent by label, owner plist path and content hash, exact ordered
arguments, a digest of its bounded string-only environment, and fixed safe
launch semantics. Optional hard/soft resource limits are accepted only for
launchd's known numeric resource keys, and a soft limit cannot exceed its hard
limit. Raw environment values are never returned. Preflight binds the loaded launchd
path, arguments, PID and ASID, the canonical socket's kernel-authenticated peer
UID/PID, the root audit-token generation, and the first bounded
`lsof -d txt` mapped vnode. If that vnode, process path, and argv are the
signed plan-bound executable, the healthy host is not restarted.

If the mapped vnode is provably a different or deleted image, preflight
captures the exact stale root plus the stable set of its CAO `mcp-stdio`
descendant audit-token generations. Immediately before the effect it rereads
the plist/job/socket/process/bridge fences and re-verifies the executable
signature and file generation. It invokes exactly one
`/bin/launchctl kickstart -kp gui/<uid>/<label>`; it never calls standalone
daemon bootstrap/restart, selects or signals a process itself, or uses a broad
kill. Completion requires the returned new PID, a replaced socket device/inode
owned by that PID, retirement of the exact old root and captured bridges, and
a new audit-token generation whose path, mapped vnode, argv, and loaded job
match the plan. Missing mapped-image evidence fails before the kickstart;
timeout, nonzero or malformed kickstart output, or an unproved postcondition
is an unknown effect and is never retried automatically.

A declared `pid` backend instead requires the exact standalone managed path,
a non-null managed version, and the official controlled lineage
`current -> releases/<managedCodexVersion>` with
`current/codex -> bin/codex`. The resolved managed executable must remain in
that owner-controlled release directory, satisfy the OpenAI signing
requirement, and return the same version from its bounded `--version` check.
This managed backend uses reload only; it is not routed through the Desktop
LaunchAgent kickstart.
Any malformed, conflicting, or unready observation blocks the upgrade before
either CAO service is stopped.

The reviewed digest covers every upgrade or refresh field, including the
target release, exact bundled executable file generation, owner LaunchAgent
binding, exact socket, reload scope, and application boundary. Each executor
recomputes that digest before
authorization or effects, so replacing one field while reusing an earlier
effect receipt is rejected.

CAO reuses Codex's owner-local JSON-RPC control protocol, requires the current
Codex initialize identity before sending a reload, sends the exact
initialize/initialized/reload envelopes, and accepts only the matching empty
reload result. The socket device and inode observed during daemon preflight
must remain identical through the immediate connect and reload; a replacement
at the canonical path fails before the reload effect. For a Desktop or
owner-LaunchAgent observation with no standalone backend, the explicit trust
boundary is the current owner's real `0700` socket directory, current owner's
non-symlink `0600` socket, plan-bound owner LaunchAgent, signed mapped
executable generation, and validated Codex initialize identity. An error or
EOF after the reload request was sent has an unknown outcome and must be
inspected before another attempt. After a confirmed targeted restart, a
pre-send failure is reported as
`restart=performed, reload=not_submitted`; a post-send ambiguity is
`restart=performed, reload=unknown`. Both stop with `retryable=false` and
never issue another kickstart in that operation. An acknowledged host refresh
records the factual `codex_app_server_restart=not_performed` or `performed`
and keeps the
compatibility reload field `submitted_for_next_active_turn`, with scope
`all_loaded_codex_threads` and application `next_active_turn`. It also records
`current_conversation_verification=pending`. Upgrade `status=ready` proves only
the CAO services' release/readiness checks; neither that status nor the reload
ACK means the affected task has the new catalog. Catalog refresh succeeds only
when that exact task's next active turn calls `cao_start`, receives a current
attachment, and then completes `cao_list_managed_workers` against the planned
catalog. Durable CAO attachment, Work, Attempt, native task, and generation
identities remain in the Control Plane.

### Refresh a stale or closed Codex MCP bridge without restarting CAO

When the services already run the intended release but a Codex task has a
stale MCP catalog or reports `Transport closed`, use the separate owner-local
action. First print its bounded plan:

```sh
cao-dashboard lifecycle refresh-mcp --plan-only
```

Then submit the same plan with a matching `refresh-mcp` effect receipt or with
explicit owner confirmation:

```sh
cao-dashboard lifecycle refresh-mcp --execute --owner-confirm
```

This action validates the target release, signed plan-bound executable,
current daemon observation, exact owner LaunchAgent, canonical owner-only
socket, kernel peer and mapped image. On a healthy owner Desktop backend it
submits the official `config/mcpServer/reload` request to the existing App
Server and requires its ACK; the
[official App Server contract](https://github.com/openai/codex/blob/main/codex-rs/app-server/README.md)
queues that refresh for every loaded thread's next active turn. It performs one
targeted LaunchAgent kickstart only when the mapped App Server executable image
is proven stale, then proves the
old generation retired and the socket was replaced before submitting reload.
A backend without the exact owner LaunchAgent needed for that stale-image
replacement fails before mutation. The action does not invoke standalone
daemon `bootstrap`, `restart`, or `stop`. It reports
`submitted_for_next_active_turn` only as the compatibility reload state and
reports `current_conversation_verification=pending`. It does not stop or
restart the Control Plane or Dashboard, signal stdio bridge processes, mutate
Worker state, or run a broad process kill. Bridge generations are enumerated
only when a proven-stale root needs exact retirement fencing. A task whose own
stdio transport is already closed cannot repair that transport by calling a
tool through it; run this owner-local lifecycle action through a still-live
host control path, continue the affected task, then require `cao_start` and
`cao_list_managed_workers` to prove the current attachment and catalog before
calling the refresh successful.
