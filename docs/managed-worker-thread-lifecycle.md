# Managed Worker thread lifecycle

Each managed Worker has one opaque `worker_thread_id` and one monotonically
increasing lifecycle generation. The identifier represents CAO supervision of
a Worker lifecycle, not a Work, process, credential, runtime session, workspace, or
provider-native conversation identifier. Ordinary listing and lifecycle use
are project-scoped: any currently attached CAO conversation for the same CAO
principal and project can list and operate the exact Worker. The attachment
that created the Worker is retained as provenance, not ownership. Foreign-project
and unknown identifiers remain indistinguishable.

The conversation-facing lifecycle is:

```text
New -> active -> Close (Finish) -> archived -> Resume -> active
        |                              |
        +------------ Delete ----------+-> deleted
```

`cao_new_worker_thread` performs **New** without creating a Work or Goal.
Directory and idempotency key are sufficient; runner defaults to Codex, the
server profile supplies model and reasoning effort when omitted, and an absent
display name becomes the fixed safe `Codex Worker` or `Claude Worker` label.
No Directory basename is reused as public metadata. Its result and
`cao_list_managed_workers` expose the opaque thread identifier, public state,
generation, and bounded display/launch-profile labels. Runtime, enrollment,
provider-session, and workspace identities remain private.

`cao_instruct_worker_thread` is the separate task operation for one exact
active thread. It requires the thread, bounded `instruction`, current
`expected_purpose_version`, and idempotency key. Title is optional and maturity defaults
to the explicitly stored `unset`; `defined` alone requires an acceptance
condition. The instruction is durably queued even when its Worker connection
is unavailable or the Worker already has unsettled Work. Every call creates a
separate Work record; it never ends or replaces the Worker. Record independence
does not establish that the outcomes are independent or should share a Worker.
CAO first makes the [allocation judgment](cao-operator-runbook.md#choose-worker-allocation-before-new-or-instruction)
from actual dependencies and resources, including whether shared-asset conflicts
favor the same Worker or separate Workers with ordering or isolation. Queue
admission never selects that policy or imposes a task-count limit. The current CAO
attachment becomes that Work's report/review destination. New and instruction
remain separate commands so neither lifecycle nor task state is inferred.

`cao_finish_worker_thread`,
`cao_resume_worker_thread`, and `cao_delete_worker_thread` require that exact
identifier and an idempotency key. `expected_generation` is an optional
compare-and-swap guard for advanced callers, not a prerequisite for ordinary
control. “Close”
in the requester-facing four-operation lifecycle maps to the existing Finish
operation and its resumable archive. A stale generation cannot mutate a newer
lifecycle. A foreign or unknown identifier is reported as the same scoped
not-found condition.

## Business purpose and CAO judgment

CAO is the Chief Agent Officer: one CAO supervises multiple Workers and knows
why each Worker's business responsibility exists. Each logical Worker retains
its own business purpose independently of individual Work instructions. CAO
uses that purpose to define outcomes and evaluate submitted evidence against
task background and acceptance. The Worker owns ordinary method choices,
diagnosis, implementation and experiments within its sealed task. CAO returns
missing evidence or unmet outcomes without taking over execution or imposing
approval on each choice; scope, authority, resource and unresolved blocker
boundaries still require supervision. A task is often a checkpoint toward the
purpose. Accepting a satisfied task does not complete or revise the purpose;
delegate the next outcome without moving the current acceptance conditions.

### Writing a stable purpose

CAO writes the purpose as the state the requester needs to achieve, together with
the background that explains why that state matters. Keep it within the entrusted
responsibility and make achievement recognizable. A broader slogan loses the
decision boundary; a list of immediate tasks loses the reason for doing them.
An issue's title, WHAT or WHY can supply evidence, but does not determine the right
purpose by itself. Planning vocabularies and analogies are optional aids.

Use the existing `purpose` text for background, need and desired state. Use the
structured contract below for observable conditions and boundaries. The current
task explains which unmet condition it addresses and what evidence it should
produce. Methods, intermediate deliverables, approved message payloads and
unrelated operating rules belong in their task or authority context, not in the
business-direction statement. No extra field, hierarchy, purpose-quality AI or
approval step is required.

For example, a purpose can be that users can recover saved work after an
interruption without losing confirmed changes, because interruptions currently
prevent reliable use. Reproducing the loss, comparing recovery methods and
verifying a repair are possible tasks serving that same state. A failed method
changes the next investigation, not the desired state.

Before revising purpose, distinguish a real change in the entrusted need or
desired state from progress, a new method, a checkpoint or fulfillment of an
existing approval condition. Retain the purpose in the latter cases. A correction
to sealed scope or authority still uses the explicit versioned contract; preserve
unchanged background and outcome, and identify the exact correction. Do not
rewrite business direction to accommodate a rejected instruction. A genuinely
new requester responsibility may justify a new direction; related work is not
permission to enlarge the existing purpose.

When a task meets an unexpected obstacle, CAO and Worker should be able to explain
the same chain: the required state, the condition still unmet, the observed
evidence and uncertainty, the check or condition needed next, and why the proposed
authorized action can close that gap. CAO reviews that reasoning rather than
replacing purpose with the obstacle or insisting on a method that no longer works.

### Fixed purpose contract and task review

New purpose definitions and revisions require a structured `contract` alongside
business direction. CAO defines these fields from the requester direction before
delegation; incomplete or whitespace-only fields and duplicate criterion IDs
cannot commit a purpose revision.

| Field | Required meaning |
| --- | --- |
| `outcome` | The observable state the responsibility should achieve |
| `scope` | The subjects and boundaries to which the judgment applies |
| `criteria` | Stable IDs, fixed pass conditions, and the evidence for each condition |
| `completion` | The finite completion boundary, or the explicit checkpoint and continuation rule for ongoing work |
| `authority` | Permitted operations, exclusions, resource and cost boundaries |
| `unmet_action` | How to continue unmet outcomes and escalate a concrete unresolved blocker |

The contract is sealed as `cao-worker-purpose/v2` in the existing immutable
purpose packet. Existing v1 purpose and task packets retain their exact contents
and digests; the system never invents historical criteria. Existing task recovery
and terminal lifecycle authority remain available. New managed assignments first
require a defined contract. Dashboard purpose text includes the complete contract.
Structural validation cannot establish that prose is sufficiently precise: CAO
must choose observable evidence, a fixed reference and a decision boundary, rather
than repeat an adjective or an unbounded future guarantee.

`cao_get_work.review_criteria` lists IDs and conditions from that task's verified
immutable Goal packet. Explicit acceptance conditions become `task_1`, `task_2`,
and so on; an unset/exploring task is judged against its bounded `task_instruction`.
A Review of a structured assignment supplies `criterion_assessments` for every
listed ID, each with `met`, `unmet`, or `unverified` and supporting evidence.
Unknown/duplicate/missing IDs and a contradictory overall verdict cannot commit.
These assessments remain bound to the original task packet and assignment purpose
version in the Review evidence. Satisfying a checkpoint does not require every
outstanding business-purpose outcome to be met.

After a purpose revision, read `purpose_alignment` on each affected task. Its
current and assignment versions distinguish a saved purpose from a revised task.
A Review after a purpose change explicitly confirms
`expected_assignment_purpose_version`; it still judges the frozen task conditions.
If the task must change, use `cao_revise_task` at its admitted safe boundary.
A never-claimed queued Assignment may be replaced on its current exact connection
even while another Work is executing. That transaction retires only proven
unclaimed terminal inputs; it does not rotate the executing connection or replay
its Delivery. A claimed/dispatched Assignment, launch authority, captured output,
artifact or unresolved effect cannot use this queue-only route.

Before each instruction or business judgment, CAO reads the current
`worker_purpose` from `cao_list_managed_workers`, `cao_get_work`, or
`cao_acquire_reasoner_turn`. This context includes the complete purpose text,
version and digest, and instructions to reason from it. CAO confirms the read
version with `expected_purpose_version` on instruction, task revision, reply,
review, handling a report without a Boundary, Boundary disposition, task
resumption, retry and requester-decision
commands. The same transaction checks that the version is still current;
stale or omitted confirmation cannot commit a new decision. Confirmed business
judgments are recorded in the existing event ledger. A matching idempotent
replay returns the already committed result after authenticating the caller;
it does not make another judgment. A reasoner-lease replay refreshes current
purpose context without changing the lease identity.

`cao_set_worker_purpose` defines or explicitly revises business direction for
one exact same-project `worker_thread_id`. Supply `purpose`, the reason for the
business-direction decision, an idempotency key and `expected_purpose_version`
(`0` for an unset purpose). Revisions are immutable and independently numbered;
`expected_generation` remains an optional Worker lifecycle guard. A changed
local instruction, format constraint or method is never grounds to replace
business purpose. Define or revise it from the requester's business direction.

New managed assignments require a defined purpose. New Work seals that purpose
snapshot into its existing contract; `assignment_purpose` stays frozen while
`worker_purpose` shows the current business direction. If they differ,
`purpose_changed_since_assignment` is true so CAO can judge what the changed
purpose means for that task. The public `instruction` field and
`cao_revise_task` name describe the task contract. `objective` and
`cao_revise_goal` remain compatible names for that same task data; they do not
control Worker business purpose. No additional task/execution hierarchy,
automatic business-purpose planner is introduced.

### Continuing after an accepted checkpoint

For a registered Worker purpose, checkpoint acceptance requires `next_action`
in the same command. It commits one of: a distinct sealed `next_instruction`, an
exact existing successor Work, verified purpose achievement, a concrete timed or
requester-input wait, or a requester-directed pause. The accepted checkpoint,
new Work when requested, and purpose decision commit together. Failure in any
part rolls back all of them. Replaying the same command returns the same result.
There is no intermediate Next notification for a decision already made in that
transaction. Requester acceptance remains separate from both checkpoint review
and purpose achievement.

`cao_dispose_boundary(kind=accept)` carries this decision when a completion
Boundary exists. An OK `cao_review` carries it when that review directly accepts
the Work without a separate Boundary. A review recorded ahead of Boundary
acceptance does not prematurely assign a successor.

Pre-upgrade accepted checkpoints retain their durable purpose-continuation
obligations in the existing event ledger. `cao_get_work.purpose_continuations`
exposes their identity, generation, purpose version and next action. The existing
Dispatcher delivers those unresolved handoffs to the originating conversation;
it never replays their accepted Work. An accepted pre-upgrade checkpoint with
no continuation record is restored once when its purpose has neither active
Work nor a pending decision. A recorded achievement, explicit purpose pause or
closed originating attachment prevents that recovery. Missing fixed legacy
contract fields are completed from existing requester direction by CAO; they
never justify asking for routine recovery approval.

An older-purpose checkpoint is recovered for a revised purpose only in the
conversation that defined that revision. A checkpoint from another conversation
remains eligible when its frozen assignment already carries the current purpose
version. Recovery does not enlist a former conversation to observe a new direction.

Within the existing purpose, authority and budget, CAO delegates the next bounded
outcome without repeated requester approval or an invented iteration/time cap.
Passing `purpose_continuation_id` and `expected_continuation_generation` to
`cao_instruct_worker_thread` commits that distinct Work and the continuation
decision atomically. If a suitable successor already committed, link its exact
Work with `cao_record_purpose_next_action(kind=continue)` instead of duplicating it.
The Control Plane checks its current sealed purpose, attachment and unsettled
execution; an unrelated or already accepted Work cannot discharge the obligation.
An explicit handoff may link another Worker only when its current sealed purpose
and complete fixed contract preserve the source purpose. Ordinary allocation
remains CAO's judgment.

Assignment transfers supervision to the exact successor Work; it does not prove
purpose achievement. If that successor ends before an accepted checkpoint, the
same continuation advances to a new pending generation and wakes CAO. Its unknown
Delivery/effect evidence stays untouched, and a closed Worker is never revived.
An accepted successor already owns its own continuation and is not duplicated.

The same decision tool admits `achieved` only with evidence for every fixed
purpose criterion, `wait_until` with an explicit future observation time and
condition, or `wait_user` with the exact missing requester-owned input and its
kind. A scheduled observation re-arms the same obligation once when due. Technical
recovery remains CAO-owned. These records do not revise purpose, accept for the
requester, Finish a Worker or authorize external effects.
CAO must ground observation and requester-input waits in that concrete missing
condition. A CAO-only decision does not invoke independent alignment review.

`pause` and `resume` record observed requester direction for the business purpose.
CAO records the observed direction; ordinary continuation never requires a
fresh requester grant. A purpose pause applies to every pending continuation of
that exact Worker purpose version and fences new assignments until an explicit
resumption or requester-directed purpose revision. It does not itself cancel an
already executing Work: its separately authorized lifecycle remains explicit.
CAO judges `achieved` against the fixed purpose completion boundary; satisfying
the current task alone cannot end an ongoing responsibility.
Verified achievement applies to every pending checkpoint of that exact Worker
purpose version and fences further assignments until a requester-directed new
purpose is defined. Other Worker purposes and existing unknown outcomes remain
independent.

A wake cannot be marked handled while its exact continuation generation is still
pending. Missing transport is re-armed; accepted or unknown deliveries are never
blindly replayed. A proven incomplete native turn uses the exact persisted
terminal-turn recovery path. Recovery records the genuine requester-input,
review-policy and executable-release digests for that exact continuation generation.
A completed turn with the same condition cannot recursively create another wake.
The unresolved purpose remains visible with Control Plane technical recovery
ownership. A genuine native head change renews the authenticated source read outside
database transactions; a restored/changed source or corrected release admits the
next wake once. The latest verified source observation is shared by reconciliation
and recovery so an older review snapshot cannot flip the condition back. Delivery IDs, assistant status, timestamps and repeated
rejected proposals are not changed conditions. Unknown handoffs are never replayed. Worker Finish/Delete fences that Worker's future
authority while retaining an unmet purpose's next decision for CAO. CAO chooses a
purpose-preserving handoff or records the requester's explicit purpose pause;
Worker Resume alone never resumes a paused purpose. Conversation Close fences its
attachment's wakes without claiming purpose achievement. Purpose revision
supersedes old pending decisions and carries unresolved responsibility to the
new purpose version. Historical accepted tasks are not replayed
during upgrade.

Native requester observation retains every in-turn correction through full native
items and, for an oversized turn, complete item pagination rather than display
summaries. Automatic input is identified by its exact recorded Delivery identity
(or exact legacy frame), not by a text prefix a requester could quote. This
observation is separate from instruction alignment and is not a reading list for
its checker.

Model progress renews only the exact validated decision lease. Starting an active
alignment review extends that lease before the model call; completion restores
the original idle window. A superseded or expired lease cannot be revived.

### Independent review before instruction commit

The independent Codex reviewer checks one responsibility: whether the effective
Worker instruction aligns with CAO's registered purpose and structured contract.
It receives only that purpose and the task/message fields to be sent. It checks
outcome, scope, fixed criteria, completion boundary and authority against the
purpose as given. A bounded prerequisite need not complete the whole purpose.
Read the retained task and its corrective message together. A correction may
inspect existing execution records, clarify provenance or repair a report to
resolve one unmet acceptance condition without repeating completed execution.
This does not replace the purpose with documentation or waive the remaining
criteria. A correction label grants no exemption: instructions that cancel
required work, claim purpose completion from paperwork or exceed authority still
contradict the purpose.

CAO owns defining and updating precise purposes, judging the user's actual intent,
permissions and approved content, reviewing Worker evidence, and choosing next
actions. Purpose registration/revision and a CAO-only checkpoint/next-action
decision do not invoke another AI. An actual next Worker instruction still passes
alignment review. The checker does not audit original requester conversations,
approval records, previous results or artifacts, and does not judge purpose quality
or invent new criteria. It has no evidence MCP, search, shell, browser or other tools.

The normal command prepares its effective instruction and rolls back before the
model call; no SQLite writer lock is held while waiting. A private approval admits
only the same purpose and effective instruction on re-execution. Purpose version,
attachment, current execution, replay and unknown-effect fences remain kernel
responsibilities. An unchanged idempotent replay returns its committed result
without another model call or delivery. A review never grants external authority.

Use `tools/evaluate_instruction_review.py` with fixed synthetic purpose/instruction
cases to record verdicts, latency and token usage. These evaluations create no
Worker and grant no external effect authority.

The reviewer uses the configured Codex model and owner's selected effort in a
fresh isolated session. It returns only a verdict and concise reason. New records
have scope `purpose_alignment` and one alignment assessment. Historical five-check
reviews retain their original content and meaning; they are not reinterpreted as
new business judgments. Model, elapsed time, safe diagnostics and usage remain
available. Provider failure blocks only this proposed instruction, preserving
listing, New, Finish/Delete and unrelated lifecycle operations.

Native conversation observation is a separate capability. Its v4 coverage contract
preserves a final assistant proposal completed after a notification-only checkpoint
when a newer requester input refers to it. It forces one fresh source capture
instead of inheriting an older checkpoint that may already omit that proposal.
New alignment checks never invoke that observation or reread conversation archives.

Recording later Work while the first Assignment starts preserves the current
connection and enrollment; it never retires an in-flight bootstrap. Worker
context resolves the current consumed launch-ticket Attempt. An older unscoped
launch uses the exact credential runtime and Assignment sequence, never the
latest updated Work or a queued later task.

Each new Assignment message also seals the selected Worker's current purpose,
including after a correction or retry. An explicitly authorized administrative
handoff checks both the source purpose and the exact successor's
`expected_successor_purpose_version` before committing. The original Work
purpose remains historical context; neither Work transfer nor replay changes
either Worker's business responsibility.

Schema 47 stores purpose revisions and exact Work-revision references.
Purpose identity uses the shared immutable-packet canonical JSON encoder for
both writing and verification, including non-ASCII purpose and revision text.
Previously sealed Work retains its original packets and digests without an
inferred purpose. Historical task supervision and recovery remain available
when purpose is unset; a new managed assignment first requires CAO to define
the known business purpose. An unknown purpose stays visibly unset, never
copied from the latest task, label, constraint or Directory.

Reconnect, task completion and archive/resume preserve purpose. Delete retains
its revision history alongside Work evidence. Explicit Finish/Delete and
conversation cancellation are terminal authority, independent of business
purpose confirmation. Corrupt purpose metadata is reported as invalid on the
Worker list and cannot authorize a new instruction; it does not hide unrelated
Workers or block exact terminal lifecycle control.

## Archive meaning

The archive is a logical CAO supervision archive. It is deliberately distinct
from a provider-native Codex or Claude session store:

- **Finish** stops future authority for the exact Worker, terminalizes its
  unsettled Work, removes it from active CAO supervision and every Dashboard
  Worker category, and preserves dispatched/unknown evidence. It retains the
  opaque native resume handle and all durable audit history.
- **Resume** reactivates the same logical Worker thread and native conversation
  with a fresh runtime and enrollment epoch. It does not require or invent a
  next Goal. The next ordinary dispatch issues its one-time launch ticket and
  the handshake creates a fresh credential. It does not revive an old
  credential or redispatch an old Work message. A pure Resume therefore also
  succeeds when Finish retained a dispatched/unknown Delivery or unknown
  effect: that evidence stays byte-for-byte unchanged. The resumed Worker can
  accept a later instruction; a disconnected transport is replaced or the new
  Work remains queued.
- **Delete** removes the exact CAO thread ledger and its resume capability. The
  Delete call itself is the explicit lifecycle command; active state,
  cross-conversation provenance, or an omitted generation does not add a
  second acknowledgment ceremony. It does not
  claim to erase a provider's global conversation storage.

None of these actions deletes, moves, archives, checks out, resets, or cleans a
project Directory, workspace, branch, local file, artifact, artifact archive,
or close receipt. Existing Work, Attempt, Message, Delivery, Artifact, Review,
decision, effect, and close history remains durable after Delete.

Worker-thread Resume is not runtime-failure recovery. A recoverable launch
failure advances the active logical thread through a fresh internal execution
epoch; it does not archive the thread first and does not authorize CAO to
create an equivalent replacement Worker.

## Close authority boundary

Finish (the requester-facing Close operation) accepts the exact current Worker
generation even while it is active or working. In one bounded lifecycle saga,
the Control Plane fences future Worker report and dispatch authority, stops the
exact runtime, terminalizes unsettled Work and pending directives, and archives
the thread as resumable. Only provably unclaimed queued or leased Deliveries
may be terminalized; dispatched/claimed Deliveries, started/unknown effects,
packets, artifacts, and their generation fences remain immutable evidence.
Close is not Work acceptance, cleanup, Delete, or conversation close, and none
of those is a prerequisite. Process exit or silence does not prove Close; the
durable archived state does.

An archived thread cannot receive a Delivery, issue a launch ticket, appear in
Dashboard counts, or be recreated automatically by catalog ensure. Resume is
the only operation that can reactivate it. After a successful Delete, the old
identifier cannot be resumed. A later New request may create a distinct Worker
thread without reviving the old one.

An explicit non-archived Delete is terminal supervision authority, not
evidence about what an in-flight Worker, provider, Delivery, or external effect
did. In one transaction the Control Plane cancels every unsettled Work bound to
the exact Worker, abandons its decision leases, supersedes its open Boundaries
and pending directives, revokes Worker credentials and launch tickets, retires
the thread internally, and removes the logical ledger. Only provably unclaimed
queued or leased Deliveries are dead-lettered. Claimed or contradictory queued
rows, dispatched or otherwise claimed Deliveries, started/unknown effects,
messages, artifacts, provider-circuit
records, and all task history remain unchanged as reconciliation evidence. No
cancel packet, retry, provider deletion, or filesystem cleanup is issued.

Cross-conversation Delete requires the current exact CSC and the database-bound
same CAO owner and project digest. An optional generation may narrow concurrent
intent but is not required.
Missing, malformed, or historically inconsistent Goal, task-packet, event, or
directive metadata is retained as an anomaly; it is not authority to refuse
explicit Close or Delete of the exact thread. Only an actual ambiguity in
shared requester/supervision authority blocks before mutation. Sealed-packet
integrity remains mandatory for assignment, dispatch, reporting, and effect
authority, not for terminal lifecycle fencing. The source attachment may still
be live and may continue supervising other Workers: the transaction serializes
commands, fences only the exact Worker, and makes every later command for that
deleted Worker fail without killing the attachment.

Attachment generation `0` is the valid initial generation, not missing
metadata. Close/Delete cleanup audits distinguish `NULL` from every valid
non-negative generation and must not emit a retained anomaly merely because a
Worker and its Work were created before the attachment's first renewal.
Likewise, a Work awaiting requester acceptance normally has a completed latest
Attempt and no mutable Attempt to cancel. Cleanup validates the Work/Attempt
state pair instead of requiring one nonterminal Attempt for every unsettled
Work.
Each Attempt is audited against its own immutable Goal revision. A legitimate
older revision is retained as normal history, including after task replacement;
it is not an anomaly merely because the Work now uses a newer revision. A missing
revision or mismatched historical packet digest remains an anomaly.

Schema migration creates an active thread for each existing enabled managed
specification, even when its historical runtime is missing or its enrollment
failed. Existing stopped or revoked specifications have no Finish proof and are
classified internally as non-resumable stopped history, never as archived
threads. That history remains outside Dashboard Worker groups and does not
block creation of a fresh catalog lifecycle; an exact acknowledged Delete may
remove its remaining CAO thread ledger without introducing another public
lifecycle operation.

Conversation close is also distinct from Finish. It cancels terminalizable
Work supervised by that conversation and revokes that attachment, but leaves
every project-local Worker lifecycle unchanged. A same-project CAO conversation
can continue using the exact Worker. Only explicit Finish creates the resumable
archived state that requires Resume or Delete; only explicit Delete removes the
thread ledger.

`system_reconciliation` does not add a fifth requester-facing lifecycle
operation. When the requester explicitly chooses Delete, acknowledged Delete
atomically cancels its Work, supersedes its Boundary without disposition,
preserves unknown Delivery/effect/provider evidence, and removes the exact
thread ledger. No Work-targeted abandonment command is exposed. Explicit
Finish or Delete is the Worker lifecycle authority; unresolved Work evidence
remains preserved.

Pure Resume reactivates the archived logical thread and appends its fresh
fenced runtime/enrollment epoch without creating Work. The resumed Worker can
accept a later instruction through `cao_instruct_worker_thread`. Resume does
not pre-issue a launch ticket or credential. Normal Assignment dispatch issues
the one-use ticket, and the Worker MCP handshake creates the fresh credential.
