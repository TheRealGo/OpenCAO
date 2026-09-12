# CAO conversation attachment bootstrap capability

The standard path is `User ↔ existing Codex CAO conversation ↔ MCP/A2A Control
Plane ↔ Worker`. It has no raw requester-text ingress and no hook or tmux
monitoring path.

`cao.<cab-id>.<secret>` is a dedicated one-use attachment bootstrap capability
(CAB). Its verifier is stored with scrypt in SQLite; plaintext exists only in
the short-lived owner-local Unix-socket response and is never exported through
files, environment variables, arguments, or the public catalog. The private
attachment endpoint exchanges the CAB for a connection-bound CSC and revokes
the CAB in the same transaction. It does not revoke credentials belonging to
other authentic connections on the durable attachment.

The CAB is not an administrator or generic CAO credential. It is rejected by
ordinary REST, MCP, and A2A operations, and an administrator bearer is rejected
by the private attachment endpoint. Direct stdio mode may attach only with this
owner-local capability or an existing CSC already bound to the exact current
conversation.

## Owner-local admission boundary

The daemon creates the issuer in a real owner-owned directory with exact
`0700` permissions and exposes an owner-owned, non-symlink Unix socket with
exact `0600` permissions. Both the issuer and bridge verify those filesystem
properties. The issuer then requires the kernel-reported peer UID to equal the
daemon effective UID. A different UID, unsafe directory, unsafe socket, or
unavailable kernel peer identity fails closed.

The issuer also derives the peer PID and process start identity from the live
socket. A caller cannot nominate a PID, parent, launch root, executable, or
process generation. PID/start are retained only as bounded audit and
connection bindings. They do not authorize the connection by executable path,
argv, ancestry, application bundle, code signature, or LaunchAgent state.
Consequently an application update or disappearance of the process's source
path cannot make an otherwise valid live bridge unable to connect.

The CAB and connection each persist one `peer_pid`/`peer_start_signature`
pair. The durable logical attachment persists no process identity. There is no
second host/root identity, compatibility alias, or process field from which a
caller can assemble a different authority chain.

This is an application boundary, not an OS sandbox. Processes already running
as the daemon's OS user are within the boundary described in
[Security](security.md). Preventing a malicious same-UID process requires OS
user separation and is not approximated with a host-signature gate.

Same UID alone is not the complete protocol contract. `cao_start` supplies the
exact native thread identifier from the active execution context; the bridge
derives the project digest locally and submits the MCP catalog digest and proxy
ABI loaded in that bridge process. The Control Plane matches that catalog/ABI
to its current conversation contract before admitting the connection. A
generic local tool that does not implement and present that configured bridge
contract cannot attach merely by reaching the socket.

## Project identity across host restarts

Persistent identity and a live filesystem race fence are separate contracts.
The bridge derives the project from the canonical directory, persistent volume
UUID on macOS, inode and available birth/generation marker. On macOS the kernel's
descriptor path normalizes case and aliases. A mount's `st_dev` is never part
of this durable identity: reboot or remount may renumber it. Device/inode pairs
still prove that path resolution and an open descriptor refer to the same object
during one operation. A different volume, renamed directory or replacement
object does not inherit the registered identity. The portable fallback binds
canonical location, inode and available generation; filesystems without persistent
volume/generation support retain a same-location object-reuse limitation.

Schema 45 preserves each attachment's original `project_digest` in every sealed
Goal, Task, evidence and memory record. `project_scope_digest` separately binds
current project membership. Existing version-1 registrations migrate only after
the owner edge reads the exact native conversation using the supported Codex
`thread/read` operation and derives its persisted workspace's current identity.
The requested project must match that observation before bootstrap issuance.
The migration transaction checks the observed attachment, principal, native
thread, original digest and generation, binds only that exact attachment once,
and records a path-free event. A moved native thread cannot migrate other
conversations that shared its historical digest. Concurrent equal proofs are idempotent; conflicting
proofs are rejected. It neither replays Work nor resets the attachment lifecycle.
An unavailable historical native thread remains unmigrated and cannot block
other conversations. Startup attempts independent legacy attachments in the background;
an affected conversation's own attachment verifies its migration synchronously.

Project-local Worker list, instruction and lifecycle authority, recovery,
projection validation and explicitly project-shared memory compare the persistent
scope. Packet validation and conversation-private memory continue to use the
original exact attachment and sealed digest. A new conversation in the same
project can therefore use its existing Workers after migration without granting
access to another project's Work or private memory.

The owner-private Directory registry uses the same persistent directory identity
for new Worker references. An integrity-verified legacy entry keeps its original
opaque reference: canonical location, inode and the saved object-generation
marker must still match before its persistent identity is added atomically.
Across a changed device number, a legacy entry without a real birth/generation
marker fails closed. The registry retains old mount numbers only as migration
evidence; launch still checks the live descriptor immediately before use. No
workspace marker, registry deletion, new Worker or database reset is required.

## Replaceable connections and bounded failure

Each admitted bridge creates or renews only its own replaceable connection
under the durable conversation attachment. Another admitted bridge for the
same immutable thread/project may connect concurrently; it never waits for,
kills, supersedes, or proves the death of a prior bridge. Connection expiry or
catalog mismatch revokes only that connection and CSC. It does not change the
attachment generation, wake credential, Worker, Work, Delivery, or evidence.
Conversation Close revokes all connections because it terminates the durable
authority.

CAB issuance also snapshots either the exact existing attachment ID and
generation or the explicit fact that no row exists. Attachment consumes that
observation with a compare-and-swap. A no-row race creates one attachment and
revokes sibling no-row CABs; an ordinary same-generation connection does not
advance the lifecycle generation. Conversation Close revokes CABs bound to the
closing generation while leaving that closed generation unchanged. Only a CAB
issued after Close can observe the revoked row at generation G and reopen that
same attachment row with one compare-and-swap to G+1. Reopen creates a fresh
CAO wake runtime and a fresh connection epoch; it never revives the old
runtime, Worker, Work, or
Delivery. If two post-Close CABs race, one compare-and-swap wins and the other
is revoked as stale.

Public pre-attachment outcomes are fixed and path-free:

- `attachment_peer_unavailable`: the kernel peer could not be captured, or its
  UID was outside the owner boundary;
- `attachment_context_invalid`: thread/project or proxy-contract input was
  malformed;
- `attachment_catalog_refresh_required`: the loaded catalog or proxy ABI was
  not current.

Only a proven pre-issuance peer race marked `retryable=true` opens one fresh
owner-local socket and retries once. A foreign UID is non-retryable. Invalid
context, stale catalog, an issued CAB, and the later HTTP attachment request are
never retried by this path, so one logical attempt issues at most one CAB and
makes at most one HTTP request. Private socket paths, PID/start values, bearer
values, and submitted invalid content never enter public results.

A later `cao_start` readiness probe is not new admission. When this exact
process has already verified the same connection and still-loaded proxy digest,
one transient non-authoritative catalog/list probe failure preserves the CSC
and returns bounded degraded-ready evidence marked
`attachment_verification=previously_verified` and `current_probe=failed` so
ordinary operations remain usable. It never substitutes for a successful fresh catalog proof. Credential
401, explicit catalog-stale evidence, or a digest mismatch clears that
eligibility and follows the typed refresh/reattach path; an unverified initial
attachment is never promoted to ready.
