from __future__ import annotations

import argparse
import asyncio
import json
import os
import stat
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, cast

import uvicorn

from . import __version__
from .api import create_app
from .attachment_issuer import attachment_issuer_path
from .config import Settings
from .dashboard_access import DashboardAccessCoordinator
from .database import Database, backup_sqlite_database
from .effects import EffectExecutor, effect_argv_digest, effect_workdir_digest
from .mcp import serve_stdio
from .models import (
    AckInput,
    EffectCheckInput,
    EffectGrantInput,
    EffectResolveInput,
    GoalRevision,
    PrincipalCreate,
    QueryInput,
    ReportInput,
    ReviewInput,
    RuntimeHeartbeat,
    RuntimeRegistration,
    WorkAssignment,
)
from .runtime import Dispatcher
from .service import ControlPlane


def _print(value: Any) -> None:
    print(json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True, default=str))


def _json_value(raw: str, *, expected: type = dict) -> Any:
    value = json.loads(raw)
    if not isinstance(value, expected):
        raise argparse.ArgumentTypeError(f"expected JSON {expected.__name__}")
    return value


def _list_json(raw: str) -> list[Any]:
    return cast(list[Any], _json_value(raw, expected=list))


def _dict_json(raw: str) -> dict[str, Any]:
    return cast(dict[str, Any], _json_value(raw, expected=dict))


def _service(settings: Settings) -> ControlPlane:
    service = ControlPlane(Database(settings), settings)
    service.bootstrap()
    return service


def _token_from_bootstrap(settings: Settings, role: str = "cao") -> str:
    environment_name = (
        "CAO_ATTACHMENT_BOOTSTRAP_TOKEN" if role == "cao_attachment_bootstrap" else "CAO_A2A_TOKEN"
    )
    explicit = os.environ.get(environment_name, "")
    if explicit:
        return explicit
    if settings.token_export_path.exists():
        raw = json.loads(settings.token_export_path.read_text(encoding="utf-8"))
        value = raw.get(role, {})
        if isinstance(value, dict) and value.get("token"):
            return str(value["token"])
    raise SystemExit(
        f"No token available. Set {environment_name} or read the one-time bootstrap token file: "
        f"{settings.token_export_path}"
    )


def _actor(service: ControlPlane, settings: Settings, token: str | None) -> dict[str, Any]:
    return service.authenticate(token or _token_from_bootstrap(settings))


def _add_global_options(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--config", type=Path, help="TOML configuration file")
    parser.add_argument(
        "--token", help="Bearer token; defaults to CAO_A2A_TOKEN/bootstrap CAO token"
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="cao-a2a",
        description="Independent local durable MCP and A2A control plane for agent supervision.",
    )
    parser.add_argument("--version", action="version", version=f"%(prog)s {__version__}")
    _add_global_options(parser)
    commands = parser.add_subparsers(dest="command", required=True)

    setup = commands.add_parser(
        "setup", help="create private configuration for a new local install"
    )
    setup.add_argument("--state-dir", type=Path)
    setup.add_argument("--port", type=int, default=8768)
    setup.add_argument("--dashboard-port", type=int, default=8769)
    commands.add_parser("run-local", help="run the Control Plane and Dashboard until stopped")
    commands.add_parser("dashboard-link", help="print a one-use local Dashboard sign-in link")

    commands.add_parser("init", help="Initialize the database and bootstrap CAO/user principals")
    serve = commands.add_parser("serve", help="Run the HTTP MCP/A2A/API server")
    serve.add_argument("--host")
    serve.add_argument("--port", type=int)
    serve.add_argument("--reload", action="store_true")

    mcp_stdio = commands.add_parser(
        "mcp-stdio",
        help="Bridge MCP stdio to the shared local HTTP daemon",
    )
    mcp_stdio.add_argument("--url", help="MCP endpoint; defaults to public_base_url/mcp")
    mcp_stdio.add_argument(
        "--operator",
        action="store_true",
        help="Use an explicit operator bearer instead of the conversation bridge",
    )
    mcp_stdio.add_argument(
        "--enrollment-broker-socket",
        type=Path,
        help="Process-bound managed Worker enrollment broker (proxy mode only)",
    )
    mcp_stdio.add_argument(
        "--cao-runtime-broker-socket",
        type=Path,
        help="Process-bound attached CAO runtime broker (proxy mode only)",
    )
    commands.add_parser("status", help="Show a control-plane summary")
    commands.add_parser("doctor", help="Run integrity, permission, lease, and queue diagnostics")
    backup = commands.add_parser("backup", help="Create a consistent SQLite backup")
    backup.add_argument("destination", type=Path, nargs="?")
    prune = commands.add_parser("prune", help="Prune acknowledged messages and retained events")
    prune.add_argument("--event-days", type=int)
    prune.add_argument("--message-days", type=int)

    agent = commands.add_parser("agent", help="Manage principals")
    agent_cmd = agent.add_subparsers(dest="agent_command", required=True)
    agent_cmd.add_parser("list")
    agent_create = agent_cmd.add_parser("create")
    agent_create.add_argument("name")
    agent_create.add_argument(
        "--role", required=True, choices=["cao", "worker", "user", "external", "dashboard"]
    )
    agent_create.add_argument("--metadata", type=_dict_json, default={})
    agent_rotate = agent_cmd.add_parser("rotate-token")
    agent_rotate.add_argument("principal_id")
    for action in ("enable", "disable"):
        item = agent_cmd.add_parser(action)
        item.add_argument("principal_id")

    runtime = commands.add_parser("runtime", help="Manage runtime sessions")
    runtime_cmd = runtime.add_subparsers(dest="runtime_command", required=True)
    runtime_list = runtime_cmd.add_parser("list")
    runtime_list.add_argument("--principal-id")
    runtime_register = runtime_cmd.add_parser("register")
    runtime_register.add_argument("principal_id")
    runtime_register.add_argument(
        "--adapter",
        required=True,
        choices=["codex-app-server", "claude", "subprocess", "webhook"],
    )
    runtime_register.add_argument("--endpoint", default="")
    runtime_register.add_argument("--native-session-id", default="")
    runtime_register.add_argument("--lease-seconds", type=int, default=180)
    runtime_register.add_argument("--metadata", type=_dict_json, default={})
    runtime_heartbeat = runtime_cmd.add_parser("heartbeat")
    runtime_heartbeat.add_argument("runtime_id")
    runtime_heartbeat.add_argument(
        "--state",
        default="ready",
        choices=["starting", "ready", "busy", "waiting", "stopped", "failed", "missing"],
    )
    runtime_heartbeat.add_argument("--lease-seconds", type=int, default=180)
    runtime_heartbeat.add_argument("--metadata", type=_dict_json, default={})
    runtime_stop = runtime_cmd.add_parser("stop")
    runtime_stop.add_argument("runtime_id")

    work = commands.add_parser("work", help="Manage durable WorkItems")
    work_cmd = work.add_subparsers(dest="work_command", required=True)
    work_assign = work_cmd.add_parser("assign")
    work_assign.add_argument("worker_id")
    work_assign.add_argument("--title", required=True)
    work_assign.add_argument("--objective", required=True)
    work_assign.add_argument(
        "--maturity", default="defined", choices=["unset", "exploring", "defined"]
    )
    work_assign.add_argument("--acceptance", action="append", default=[])
    work_assign.add_argument("--non-goal", action="append", default=[])
    work_assign.add_argument("--priority", type=int, default=50)
    work_assign.add_argument("--runtime-session-id")
    work_assign.add_argument("--managed-worker-thread-id")
    work_assign.add_argument("--managed-worker-thread-generation", type=int)
    work_assign.add_argument("--requester-id")
    work_assign.add_argument("--metadata", type=_dict_json, default={})
    work_assign.add_argument("--idempotency-key", default="")
    work_list = work_cmd.add_parser("list")
    work_list.add_argument("--worker-id")
    work_list.add_argument(
        "--state",
        choices=[
            "open",
            "active",
            "suspended",
            "waiting_supervisor",
            "waiting_review",
            "waiting_user",
            "user_needed",
            "completed",
            "canceled",
            "failed",
        ],
    )
    work_list.add_argument(
        "--attention-owner", choices=["none", "worker", "cao", "user", "external"]
    )
    work_list.add_argument("--limit", type=int, default=100)
    work_list.add_argument("--cursor", default="")
    work_show = work_cmd.add_parser("show")
    work_show.add_argument("work_id")
    work_revise = work_cmd.add_parser("revise")
    work_revise.add_argument("work_id")
    work_revise.add_argument("--expected-version", type=int, required=True)
    work_revise.add_argument("--objective", required=True)
    work_revise.add_argument("--maturity", required=True, choices=["unset", "exploring", "defined"])
    work_revise.add_argument("--acceptance", action="append", default=[])
    work_revise.add_argument("--non-goal", action="append", default=[])
    work_revise.add_argument("--reason", required=True)
    work_revise.add_argument("--idempotency-key", default="")
    work_reply = work_cmd.add_parser("reply")
    work_reply.add_argument("work_id")
    work_reply.add_argument("message")
    work_reply.add_argument("--in-reply-to")
    work_reply.add_argument("--idempotency-key", default="")
    work_cancel = work_cmd.add_parser("cancel")
    work_cancel.add_argument("work_id")
    work_cancel.add_argument("--reason", required=True)
    work_cancel.add_argument("--idempotency-key", default="")

    attempt = commands.add_parser("attempt", help="Manage Worker attempts")
    attempt_cmd = attempt.add_subparsers(dest="attempt_command", required=True)
    attempt_retry = attempt_cmd.add_parser("create")
    attempt_retry.add_argument("work_id")
    attempt_retry.add_argument("--worker-id")
    attempt_retry.add_argument("--runtime-session-id")
    attempt_retry.add_argument("--managed-worker-thread-id")
    attempt_retry.add_argument("--managed-worker-thread-generation", type=int)
    attempt_retry.add_argument("--reason", default="retry")
    attempt_retry.add_argument("--idempotency-key", default="")
    attempt_context = attempt_cmd.add_parser("context")
    attempt_context.add_argument("--attempt-id")
    attempt_report = attempt_cmd.add_parser("report")
    attempt_report.add_argument("attempt_id")
    attempt_report.add_argument(
        "--kind",
        required=True,
        choices=["progress", "question", "blocker", "artifact", "completion_claim"],
    )
    attempt_report.add_argument("--expected-goal-version", type=int, required=True)
    attempt_report.add_argument("--expected-goal-packet-digest", required=True)
    attempt_report.add_argument("--expected-task-packet-digest", required=True)
    attempt_report.add_argument("--expected-generation", type=int, required=True)
    attempt_report.add_argument("--summary", required=True)
    attempt_report.add_argument(
        "--trajectory",
        choices=["untracked", "advancing", "at_risk", "stalled", "drifting", "complete"],
    )
    attempt_report.add_argument("--stage", default="")
    attempt_report.add_argument("--next-boundary", default="")
    attempt_report.add_argument("--evidence", type=_list_json, default=[])
    attempt_report.add_argument("--artifacts", type=_list_json, default=[])
    attempt_report.add_argument("--idempotency-key", default="")

    inbox = commands.add_parser("inbox", help="Read or acknowledge the current principal inbox")
    inbox_cmd = inbox.add_subparsers(dest="inbox_command", required=True)
    inbox_list = inbox_cmd.add_parser("list")
    inbox_list.add_argument("--after", type=int, default=0)
    inbox_list.add_argument("--limit", type=int, default=100)
    inbox_list.add_argument("--attempt-id")
    inbox_list.add_argument("--include-acknowledged", action="store_true")
    inbox_ack = inbox_cmd.add_parser("ack")
    inbox_ack.add_argument("message_ids", nargs="+")

    review = commands.add_parser(
        "review",
        help="Record a CAO review; requester decisions are recorded only from the attached CAO conversation",
        description="Record a CAO review; requester decisions are recorded only from the attached CAO conversation.",
    )
    review.add_argument("attempt_id")
    review.add_argument(
        "--verdict",
        required=True,
        choices=["ok", "needs_work"],
        help="CAO review verdict (`ok` or `needs_work`)",
    )
    review.add_argument("--summary", required=True)
    review.add_argument("--evidence", type=_list_json, default=[])
    review.add_argument("--idempotency-key", default="")

    effect = commands.add_parser(
        "effect", help="Manage exact external/destructive effect authority"
    )
    effect_cmd = effect.add_subparsers(dest="effect_command", required=True)
    effect_grant = effect_cmd.add_parser("grant")
    effect_grant.add_argument("principal_id")
    effect_grant.add_argument("--kind", required=True, choices=["local", "external", "destructive"])
    effect_grant.add_argument("--target-pattern", required=True)
    effect_grant.add_argument("--action-pattern", required=True)
    effect_grant.add_argument("--content-digest", default="")
    effect_grant.add_argument("--argv-digest", default="")
    effect_grant.add_argument("--workdir-digest", default="")
    effect_grant.add_argument("--expires-at")
    effect_grant.add_argument("--standing", action="store_true")
    for action in ("check", "start"):
        item = effect_cmd.add_parser(action)
        item.add_argument("principal_id")
        item.add_argument("--kind", required=True, choices=["local", "external", "destructive"])
        item.add_argument("--target", required=True)
        item.add_argument("--action", required=True)
        item.add_argument("--content-digest", default="")
        item.add_argument("--argv-digest", default="")
        item.add_argument("--workdir-digest", default="")
    effect_resolve = effect_cmd.add_parser("resolve")
    effect_resolve.add_argument("operation_id")
    effect_resolve.add_argument(
        "--status", required=True, choices=["succeeded", "not_applied", "failed", "unknown"]
    )
    effect_resolve.add_argument("--evidence", required=True)
    effect_revoke = effect_cmd.add_parser("revoke")
    effect_revoke.add_argument("grant_id")
    effect_run = effect_cmd.add_parser("run")
    effect_run.add_argument("principal_id")
    effect_run.add_argument("--kind", required=True, choices=["local", "external", "destructive"])
    effect_run.add_argument("--target", required=True)
    effect_run.add_argument("--action", required=True)
    effect_run.add_argument("--content-digest", default="")
    effect_run.add_argument("--workdir", type=Path, required=True)
    effect_run.add_argument("--timeout-seconds", type=float, default=300.0)
    effect_run.add_argument("argv", nargs=argparse.REMAINDER)
    effect_cmd.add_parser("recover")

    dispatcher = commands.add_parser(
        "dispatcher", help="Inspect or run durable delivery processing"
    )
    dispatcher_cmd = dispatcher.add_subparsers(dest="dispatcher_command", required=True)
    dispatcher_cmd.add_parser("status")
    dispatcher_cmd.add_parser("run-once")

    memory = commands.add_parser("memory", help="Migrate preserved owner-local memory")
    memory_cmd = memory.add_subparsers(dest="memory_command", required=True)
    memory_import = memory_cmd.add_parser("import-legacy")
    memory_import.add_argument("--source", type=Path, required=True)
    memory_import.add_argument("--project-digest", required=True)
    memory_import.add_argument("--attachment-id", required=True)
    memory_import.add_argument("--apply", action="store_true", help="Commit the scoped import")

    return parser


def _settings(args: argparse.Namespace) -> Settings:
    return Settings.load(args.config)


def _dashboard_access_coordinator(
    settings: Settings,
) -> DashboardAccessCoordinator | None:
    if not settings.enable_dashboard_access_probe:
        return None
    credentials_file = settings.dashboard_credentials_file
    if credentials_file is None:
        raise SystemExit("Dashboard access configuration is incomplete")
    return DashboardAccessCoordinator(
        credentials_file=credentials_file,
        edge_base_url=settings.dashboard_edge_base_url,
        timeout_seconds=settings.dashboard_access_timeout_seconds,
    )


def _doctor(service: ControlPlane, settings: Settings) -> dict[str, Any]:
    integrity = service.db.integrity_check()
    service.expire_runtime_leases()
    permissions: dict[str, Any] = {}
    for path in [settings.state_dir, settings.database_path, settings.token_export_path]:
        if path.exists():
            mode = stat.S_IMODE(path.stat().st_mode)
            expected = 0o700 if path.is_dir() else 0o600
            permissions[str(path)] = {
                "mode": oct(mode),
                "expected": oct(expected),
                "ok": mode == expected,
            }
    started_effects = service.db.fetchone(
        "SELECT COUNT(*) AS count FROM effect_operations WHERE status = 'started'"
    )
    unknown_effects = service.db.fetchone(
        "SELECT COUNT(*) AS count FROM effect_operations WHERE status = 'unknown'"
    )
    unknown_deliveries = service.db.fetchone(
        "SELECT COUNT(*) AS count FROM message_deliveries WHERE state = 'dispatched'"
    )
    dead_deliveries = service.db.fetchone(
        "SELECT COUNT(*) AS count FROM message_deliveries WHERE state = 'dead'"
    )
    dead_pushes = service.db.fetchone(
        "SELECT COUNT(*) AS count FROM a2a_push_deliveries WHERE state = 'dead'"
    )
    started_effect_count = int(started_effects["count"] if started_effects else 0)
    unknown_effect_count = int(unknown_effects["count"] if unknown_effects else 0)
    unknown_delivery_count = int(unknown_deliveries["count"] if unknown_deliveries else 0)
    dead_delivery_count = int(dead_deliveries["count"] if dead_deliveries else 0)
    dead_push_count = int(dead_pushes["count"] if dead_pushes else 0)
    blocking_conditions: list[str] = []
    if unknown_effect_count:
        blocking_conditions.append("unknown_effect_outcome")
    if unknown_delivery_count:
        blocking_conditions.append("unknown_delivery_outcome")
    if dead_push_count:
        blocking_conditions.append("dead_push_delivery")
    degraded_conditions: list[str] = []
    if started_effect_count:
        degraded_conditions.append("effect_in_progress")
    if dead_delivery_count:
        degraded_conditions.append("dead_delivery_history")
    permissions_ok = all(value["ok"] for value in permissions.values())
    result = {
        "ok": bool(integrity["ok"] and permissions_ok and not blocking_conditions),
        "degraded": bool(blocking_conditions or degraded_conditions),
        "blocking_conditions": blocking_conditions,
        "degraded_conditions": degraded_conditions,
        "database": integrity,
        "permissions": permissions,
        "principals": len(service.list_principals()),
        "runtimes": len(service.list_runtimes()),
        "started_effect_operations": started_effect_count,
        "unknown_effect_operations": unknown_effect_count,
        "unresolved_effect_operations": started_effect_count + unknown_effect_count,
        "unknown_delivery_outcomes": unknown_delivery_count,
        "dead_deliveries": dead_delivery_count,
        "dead_push_deliveries": dead_push_count,
    }
    return result


def run(args: argparse.Namespace) -> int:
    if args.command == "setup":
        from .local_setup import default_config_path, setup_local

        _print(
            setup_local(
                config_path=args.config or default_config_path(),
                state_dir=args.state_dir or Settings().state_dir,
                port=args.port,
                dashboard_port=args.dashboard_port,
            )
        )
        return 0
    settings = _settings(args)
    if args.command == "run-local":
        from .local_setup import default_config_path, run_local

        return run_local(settings, (args.config or default_config_path()).expanduser().absolute())
    if args.command == "dashboard-link":
        from .local_setup import local_dashboard_link

        print(local_dashboard_link(settings))
        return 0
    if args.command == "serve":
        app = create_app(settings)
        uvicorn.run(
            app,
            host=args.host or settings.host,
            port=args.port or settings.port,
            reload=bool(args.reload),
        )
        return 0
    if args.command == "backup":
        destination = args.destination or (
            settings.state_dir
            / "backups"
            / f"control-plane-{datetime.now(UTC).strftime('%Y%m%dT%H%M%SZ')}.sqlite3"
        )
        backup = backup_sqlite_database(settings.database_path, destination)
        _print(
            {
                "backup": str(backup.path),
                "sha256": backup.sha256,
                "schema_version": backup.backup_identity.schema_version,
            }
        )
        return 0

    # Proxy mode must not open or bootstrap a local control-plane database: a
    # managed Worker receives its bearer solely by exchanging its one-shot file
    # with the shared daemon.  Ordinary explicit token behavior remains intact.
    if args.command == "mcp-stdio":
        endpoint = args.url or f"{settings.public_base_url.rstrip('/')}/mcp"
        is_worker_or_runtime = (
            args.enrollment_broker_socket is not None or args.cao_runtime_broker_socket is not None
        )
        if args.token and not args.operator and not is_worker_or_runtime:
            raise SystemExit("mcp-stdio --token requires --operator")
        token = None
        pending_conversation_bridge = False
        if args.operator:
            token = args.token or _token_from_bootstrap(settings)
        elif not is_worker_or_runtime:
            pending_conversation_bridge = True
        dashboard_access = (
            _dashboard_access_coordinator(settings) if pending_conversation_bridge else None
        )
        return asyncio.run(
            serve_stdio(
                endpoint,
                token,
                enrollment_broker_socket=args.enrollment_broker_socket,
                cao_runtime_broker_socket=args.cao_runtime_broker_socket,
                cao_attachment_issuer_socket=(
                    attachment_issuer_path(settings.state_dir)
                    if pending_conversation_bridge
                    else None
                ),
                pending_conversation_bridge=pending_conversation_bridge,
                timeout_seconds=settings.runtime_timeout_seconds,
                dashboard_access=dashboard_access,
            )
        )

    service = ControlPlane(Database(settings), settings)
    if args.command == "init":
        result = service.bootstrap()
        result["state_dir"] = str(settings.state_dir)
        result["database"] = str(settings.database_path)
        _print(result)
        return 0
    service.bootstrap()

    if args.command == "status":
        _print(
            {
                "state_dir": str(settings.state_dir),
                "database": str(settings.database_path),
                "principals": service.list_principals(),
                "runtimes": service.list_runtimes(),
                "work": service.query_work(QueryInput(limit=1000)),
                "integrity": service.db.integrity_check(),
            }
        )
        return 0
    if args.command == "doctor":
        result = _doctor(service, settings)
        _print(result)
        return 0 if result["ok"] else 1
    if args.command == "prune":
        _print(
            service.db.prune(
                event_days=args.event_days or settings.event_retention_days,
                message_days=args.message_days or settings.message_retention_days,
            )
        )
        return 0

    actor = _actor(service, settings, args.token)

    if args.command == "memory":
        from .memory_import import import_legacy_memories

        _print(
            import_legacy_memories(
                service,
                actor,
                source=args.source,
                project_digest=args.project_digest,
                attachment_id=args.attachment_id,
                dry_run=not args.apply,
            )
        )
        return 0

    if args.command == "agent":
        if args.agent_command == "list":
            _print(service.list_principals())
        elif args.agent_command == "create":
            _print(
                service.create_principal(
                    actor,
                    PrincipalCreate(name=args.name, role=args.role, metadata=args.metadata),
                )
            )
        elif args.agent_command == "rotate-token":
            _print(service.rotate_principal_token(actor, args.principal_id))
        elif args.agent_command in {"enable", "disable"}:
            _print(
                service.set_principal_enabled(
                    actor, args.principal_id, args.agent_command == "enable"
                )
            )
        return 0

    if args.command == "runtime":
        if args.runtime_command == "list":
            _print(service.list_runtimes(args.principal_id))
        elif args.runtime_command == "register":
            _print(
                service.register_runtime(
                    actor,
                    args.principal_id,
                    RuntimeRegistration(
                        adapter=args.adapter,
                        endpoint=args.endpoint,
                        native_session_id=args.native_session_id,
                        lease_seconds=args.lease_seconds,
                        metadata=args.metadata,
                    ),
                )
            )
        elif args.runtime_command == "heartbeat":
            _print(
                service.heartbeat_runtime(
                    actor,
                    args.runtime_id,
                    RuntimeHeartbeat(
                        state=args.state,
                        lease_seconds=args.lease_seconds,
                        metadata=args.metadata,
                    ),
                )
            )
        elif args.runtime_command == "stop":
            _print(service.stop_runtime(actor, args.runtime_id))
        return 0

    if args.command == "work":
        if args.work_command == "assign":
            _print(
                service.assign_work(
                    actor,
                    WorkAssignment(
                        worker_id=args.worker_id,
                        title=args.title,
                        objective=args.objective,
                        maturity=args.maturity,
                        acceptance=args.acceptance,
                        non_goals=args.non_goal,
                        priority=args.priority,
                        runtime_session_id=args.runtime_session_id,
                        managed_worker_thread_id=args.managed_worker_thread_id,
                        managed_worker_thread_generation=(args.managed_worker_thread_generation),
                        requester_id=args.requester_id,
                        metadata=args.metadata,
                        idempotency_key=args.idempotency_key,
                    ),
                )
            )
        elif args.work_command == "list":
            _print(
                service.query_work(
                    QueryInput(
                        worker_id=args.worker_id,
                        state=args.state,
                        attention_owner=args.attention_owner,
                        limit=args.limit,
                        cursor=args.cursor,
                    ),
                    actor,
                )
            )
        elif args.work_command == "show":
            _print(service.get_work(args.work_id, actor))
        elif args.work_command == "revise":
            _print(
                service.revise_goal(
                    actor,
                    args.work_id,
                    GoalRevision(
                        expected_version=args.expected_version,
                        objective=args.objective,
                        maturity=args.maturity,
                        acceptance=args.acceptance,
                        non_goals=args.non_goal,
                        reason=args.reason,
                        idempotency_key=args.idempotency_key,
                    ),
                )
            )
        elif args.work_command == "reply":
            _print(
                service.reply(
                    actor,
                    args.work_id,
                    args.message,
                    in_reply_to=args.in_reply_to,
                    idempotency_key=args.idempotency_key,
                )
            )
        elif args.work_command == "cancel":
            _print(
                service.cancel_work(
                    actor,
                    args.work_id,
                    args.reason,
                    args.idempotency_key,
                )
            )
        return 0

    if args.command == "attempt":
        if args.attempt_command == "create":
            _print(
                service.create_attempt(
                    actor,
                    args.work_id,
                    args.worker_id,
                    args.runtime_session_id,
                    args.reason,
                    args.idempotency_key,
                    managed_worker_thread_id=args.managed_worker_thread_id,
                    managed_worker_thread_generation=(args.managed_worker_thread_generation),
                )
            )
        elif args.attempt_command == "context":
            _print(service.get_worker_context(actor, args.attempt_id))
        elif args.attempt_command == "report":
            _print(
                service.report(
                    actor,
                    args.attempt_id,
                    ReportInput(
                        kind=args.kind,
                        expected_goal_version=args.expected_goal_version,
                        expected_goal_packet_digest=args.expected_goal_packet_digest,
                        expected_task_packet_digest=args.expected_task_packet_digest,
                        expected_generation=args.expected_generation,
                        summary=args.summary,
                        trajectory=args.trajectory,
                        stage=args.stage,
                        next_boundary=args.next_boundary,
                        evidence=args.evidence,
                        artifacts=args.artifacts,
                        idempotency_key=args.idempotency_key,
                    ),
                )
            )
        return 0

    if args.command == "inbox":
        if args.inbox_command == "list":
            _print(
                service.get_inbox(
                    actor,
                    after=args.after,
                    limit=args.limit,
                    attempt_id=args.attempt_id,
                    include_acknowledged=args.include_acknowledged,
                )
            )
        else:
            _print(service.acknowledge(actor, AckInput(message_ids=args.message_ids)))
        return 0

    if args.command == "review":
        _print(
            service.review(
                actor,
                ReviewInput(
                    attempt_id=args.attempt_id,
                    verdict=args.verdict,
                    summary=args.summary,
                    evidence=args.evidence,
                    idempotency_key=args.idempotency_key,
                ),
            )
        )
        return 0

    if args.command == "effect":
        if args.effect_command == "grant":
            _print(
                service.grant_effect(
                    actor,
                    EffectGrantInput(
                        principal_id=args.principal_id,
                        kind=args.kind,
                        target_pattern=args.target_pattern,
                        action_pattern=args.action_pattern,
                        content_digest=args.content_digest,
                        argv_digest=args.argv_digest,
                        workdir_digest=args.workdir_digest,
                        expires_at=args.expires_at,
                        standing=args.standing,
                    ),
                )
            )
        elif args.effect_command in {"check", "start"}:
            request = EffectCheckInput(
                principal_id=args.principal_id,
                kind=args.kind,
                target=args.target,
                action=args.action,
                content_digest=args.content_digest,
                argv_digest=args.argv_digest,
                workdir_digest=args.workdir_digest,
            )
            _print(
                service.check_effect(request)
                if args.effect_command == "check"
                else service.start_effect(actor, request)
            )
        elif args.effect_command == "resolve":
            _print(
                service.resolve_effect(
                    actor,
                    args.operation_id,
                    EffectResolveInput(status=args.status, evidence=args.evidence),
                )
            )
        elif args.effect_command == "revoke":
            _print(service.revoke_effect_grant(actor, args.grant_id))
        elif args.effect_command == "run":
            argv = list(args.argv)
            if argv[:1] == ["--"]:
                argv = argv[1:]
            workdir = args.workdir.resolve(strict=True)
            request = EffectCheckInput(
                principal_id=args.principal_id,
                kind=args.kind,
                target=args.target,
                action=args.action,
                content_digest=args.content_digest,
                argv_digest=effect_argv_digest(argv),
                workdir_digest=effect_workdir_digest(workdir),
            )
            _print(
                EffectExecutor(service).run(
                    actor,
                    request,
                    argv,
                    workdir=workdir,
                    timeout_seconds=args.timeout_seconds,
                )
            )
        elif args.effect_command == "recover":
            _print({"marked_unknown": EffectExecutor(service).recover_incomplete(actor)})
        return 0

    if args.command == "dispatcher":
        dispatcher = Dispatcher(service, settings)
        if args.dispatcher_command == "status":
            _print(dispatcher.status())
        else:
            _print({"processed": asyncio.run(dispatcher.run_once()), "status": dispatcher.status()})
        return 0

    raise SystemExit(f"unhandled command: {args.command}")


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        return run(args)
    except KeyboardInterrupt:
        return 130
    except Exception as error:
        if os.environ.get("CAO_A2A_DEBUG"):
            raise
        print(f"cao-a2a: {type(error).__name__}: {error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
