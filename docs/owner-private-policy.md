# Owner-private placement policy

`cao_control_plane.private_policy` is a local edge contract for deciding which
runner may use a workspace.  It preserves a placement rule while ensuring that
the control plane never receives the policy body or workspace locator.

## Private edge

The deployment constructs `OwnerPrivatePolicyEdge` with an explicitly supplied
local policy file.  The module deliberately has no default state directory,
policy filename, workspace root, or endpoint.

Before every evaluation the edge requires:

- the immediate parent to be a canonical, owner-owned `0700` directory, not a
  symlink;
- the policy to be a canonical, owner-owned `0600` regular file with exactly
  one link, not a symlink; and
- every configured root and evaluated workspace to resolve to an existing
  directory.

The policy shape has `policy_id`, `policy_version`, `evidence_key`, and
`runners`. A static catalog deployment may additionally include a `workspaces`
object; omitting it is valid when only dynamically registered Directories are
used. No other keys are accepted. A provider with static catalog workspaces has
this shape:

```json
{
  "policy_id": "opaque-policy-id",
  "policy_version": "opaque-revision",
  "evidence_key": "base64url-secret-at-least-32-bytes",
  "workspaces": {
    "opaque-workspace-reference": "owner-local-absolute-directory"
  },
  "runners": {
    "claude": {
      "allow_within": ["owner-local-absolute-directory"],
      "deny_within": []
    },
    "codex": {
      "allow_within": [],
      "deny_within": ["owner-local-absolute-directory"]
    }
  }
}
```

The values in `workspaces`, `allow_within`, and `deny_within` belong exclusively
in that owner-only file.  They never belong in source control, CP database
rows, MCP payloads, errors, logs, adapter results, or documentation examples
beyond this abstract schema. `workspace_ref` is the only workspace-related
value stored in managed Worker runtime metadata; the provider resolves it to a
canonical directory object in local process memory.
The Claude allow list and Codex deny list must each be non-empty.  The runner
map is policy data: it expresses the existing split placement rule without
embedding a private location in tracked code.

## Dynamic Directory registry

`cao_new_worker_thread` supplies one absolute existing Directory and the
selected `codex` or `claude` runner to this local edge. The edge resolves the
concrete directory identity, applies the same runner allow/deny rules, and
stores the raw path, device/inode identity, the strongest stable filesystem
object-generation marker available, runner, and integrity seal only in a
separate owner-only `0600` registry. It returns a deterministic opaque
workspace reference for the Directory/runner pair. That opaque reference is
the only value admitted to Control Plane state.

Existing static workspace mappings retain precedence over the dynamic
namespace, including a historical static identifier that happens to match the
new opaque-reference syntax. Registry updates are size-bounded and atomic; a
rejected append leaves every previously registered Directory resolvable.
When a Directory is renamed, a fresh authorized registration receives a new
opaque reference and Worker. The old reference remains bound to the old
canonical path and fails closed. On filesystems exposing stable birth or
generation metadata, that marker also prevents delete/recreate inode reuse
from silently redirecting an older Worker. Where the OS/filesystem exposes no
stable generation marker, the edge deliberately falls back to canonical
path, device, and inode: ordinary project writes remain usable, while exact
delete/recreate inode reuse remains a documented residual risk. No private
registry surgery is required.

A default local installation needs no separate policy setup before using this
tool. If `owner_private_policy_file` is unset, CAO atomically creates a
dedicated `0600` dynamic-placement policy in its owner-only state directory.
It permits owner-accessible existing Directories while denying the Control
Plane state directory, its descendants, and every enclosing Directory. The
file remains local and untracked. A configured policy replaces this default
for dynamic placement and retains the final allow/deny decision.

The registry is not a Worker selector and grants no standing delegation or
cleanup authority. A missing, replaced, non-directory, policy-denied, or
integrity-mismatched entry fails before launch with a generic non-location
error. The CAO attachment project is not a placement constraint: it continues
to bind supervision authority while the registered Directory is independently
selected by the requester and owner policy.

Containment uses resolved directory identities (`st_dev`, `st_ino`) while
walking ancestors.  It therefore rejects textual-prefix siblings and symlink
or `..` escapes, and treats aliases to the same directory on a
case-insensitive macOS volume as the same object.  A non-resolvable workspace,
invalid policy, changed permissions, or any ambiguous filesystem condition is
an error and fails closed.

## Durable CP representation

`PlacementDecision.as_durable()` returns exactly these fields:

- `policy_id`, `policy_version`, `policy_digest`
- `decision`
- `runner_adapter`
- `workspace_identity_digest`
- `evidence_id`
- `expires_at`, `revoked_at`

`workspace_identity_digest` and `evidence_id` are HMAC-derived opaque values.
The evidence binds the principal, runtime, assignment, work item, adapter,
canonical workspace directory identity, policy ID/version/digest, decision,
and launch generation.  Those binding inputs are re-supplied to the local edge
at validation time; they are not serialized in the CP record.

The policy body and error diagnostics remain local.  `PrivatePolicyError`
contains only a stable, non-location code and a generic message.

## Launch integration contract

This module does not launch processes. A managed Worker launch resolves the
opaque `workspace_ref` only after the exact delivery, attempt, runtime, and
ticket generation are known; it then obtains a decision with `evaluate(...)`
and persists only `as_durable()` plus opaque binding IDs/digests in the
dedicated placement-decision table. Immediately before process creation, Codex
and Claude re-read policy and registry state, open the selected Directory with
no symlink following, and compare the open descriptor's identity to the sealed
evidence and current launch generation. The child performs `fchdir` through
that inherited verified descriptor before replacing the small trusted shim
with the runner. A rename or same-name symlink replacement after the final
check therefore cannot redirect the process. Policy changes, expiration,
assignment or delivery-retry substitution, worker/runtime substitution,
adapter changes, workspace replacement, or generation replay deny the launch
before the Worker process receives authority.

Set `CAO_A2A_OWNER_PRIVATE_POLICY_FILE` to the owner-only provider file and
`CAO_A2A_REQUIRE_OWNER_PRIVATE_POLICY=true`. That setting rejects
raw managed Worker `cwd` metadata and requires `workspace_ref`; missing,
unknown, unreadable, or malformed provider state fails closed. A policy denial
becomes a terminal,
sanitized delivery block (`owner_private_policy_*`) with no automatic retry.
The policy does not apply to CAO conversation attachments.

Existing worker migration can call the same edge to evaluate or drain a
runtime.  It transfers only the durable decision record, so migration state
does not reveal the local placement rule or its location.

A dynamically registered Directory is borrowed rather than adopted as a
managed workspace. At launch, close inventory records workspace, temporary,
log, and branch cleanup as not applicable. Stopping or closing the Worker may
revoke its runtime authority, but it must not remove, archive, trash, or mutate
the borrowed Directory merely as cleanup. Static catalog workspaces retain
their existing managed-work adoption and authorized cleanup contract.
