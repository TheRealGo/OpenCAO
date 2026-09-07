"""Standalone command line entry point for the read-only dashboard edge."""

from __future__ import annotations

import argparse
import asyncio
import json
import sys
from collections.abc import Sequence
from pathlib import Path
from typing import TYPE_CHECKING

import uvicorn

from .dashboard_credentials import load_dashboard_credentials
from .dashboard_edge import (
    DashboardEdgeSettings,
    DashboardTextClient,
    create_dashboard_edge,
    dashboard_mcp_endpoint,
    issue_dashboard_bootstrap_url,
)
from .mcp import serve_stdio_proxy

if TYPE_CHECKING:
    from .dashboard_lifecycle import DashboardLifecycleSettings


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="cao-dashboard", description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)

    issue = commands.add_parser("issue", help="print a one-use browser bootstrap URL")
    issue.add_argument("--bootstrap-record-dir", type=Path, required=True)
    issue.add_argument("--public-origin", required=True)
    issue.add_argument("--ttl-seconds", type=int, default=300)

    for name, help_text in (
        ("snapshot", "print the current text dashboard"),
        ("follow", "follow dashboard changes"),
    ):
        command = commands.add_parser(name, help=help_text)
        command.add_argument("--credentials-file", type=Path, required=True)
        if name == "follow":
            command.add_argument("--after", default="")
            command.add_argument("--limit", type=int)

    native_mcp = commands.add_parser(
        "mcp-stdio",
        help="serve the one read-only dashboard MCP resource over stdio",
    )
    native_mcp.add_argument("--credentials-file", type=Path, required=True)

    serve = commands.add_parser("serve", help="serve the private browser dashboard edge")
    serve.add_argument("--credentials-file", type=Path, required=True)
    serve.add_argument("--bootstrap-record-dir", type=Path, required=True)
    serve.add_argument("--session-record-dir", type=Path, required=True)
    serve.add_argument("--host", default="127.0.0.1")
    serve.add_argument("--port", type=int, default=8769)

    lifecycle = commands.add_parser(
        "lifecycle",
        help="plan or execute the owner-local service and Codex MCP lifecycle",
    )
    lifecycle_commands = lifecycle.add_subparsers(dest="lifecycle_command", required=True)
    refresh_mcp = lifecycle_commands.add_parser(
        "refresh-mcp",
        help="reload canonical Codex MCP configuration without restarting CAO services",
        description=(
            "verify the exact owner-local Codex app-server generation, then submit one "
            "MCP reload without restarting healthy services or selecting bridge processes"
        ),
    )
    refresh_mcp.add_argument("--plan-only", action="store_true")
    refresh_mcp.add_argument("--effect-receipt-file", type=Path)
    refresh_mcp.add_argument("--execute", action="store_true")
    refresh_mcp.add_argument("--owner-confirm", action="store_true")
    for name, help_text in (
        ("plan", "print the owner-local deployment plan without changing services"),
        ("status", "inspect the exact owner-local deployment and readiness"),
        ("apply", "apply the reviewed LaunchAgent and Tailnet deployment plan"),
        ("remove", "remove only the exact reviewed owner-local deployment"),
        (
            "upgrade",
            "back up and restart the shared Control Plane and Dashboard on this release",
        ),
        (
            "restart",
            "explicit alias of upgrade for a full shared-system restart, even on the same release",
        ),
    ):
        command = lifecycle_commands.add_parser(
            name,
            help=help_text,
            description=help_text,
        )
        command.add_argument("--application-support-dir", type=Path, required=True)
        command.add_argument("--working-directory", type=Path, required=True)
        command.add_argument(
            "--control-plane-command-json",
            required=True,
            help='JSON argv prefix, e.g. ["cao-a2a", "--config", "/owner/private/config.toml"]',
        )
        command.add_argument(
            "--dashboard-command-json",
            default='["cao-dashboard"]',
            help='JSON argv prefix, e.g. ["uv", "run", "cao-dashboard"]',
        )
        command.add_argument("--credentials-file", type=Path, required=True)
        command.add_argument("--bootstrap-record-dir", type=Path, required=True)
        command.add_argument("--session-record-dir", type=Path, required=True)
        command.add_argument("--cp-port", type=int, default=8768)
        command.add_argument("--edge-port", type=int, default=8769)
        command.add_argument("--tailscale-binary", default="tailscale")
        command.add_argument(
            "--exposure-provider",
            choices=("tailscale", "cloudflare"),
            default="tailscale",
        )
        command.add_argument("--cloudflared-binary", default="cloudflared")
        command.add_argument("--cloudflare-token-file", type=Path)
        command.add_argument("--cloudflare-hostname")
        command.add_argument("--cloudflare-access-team-domain")
        command.add_argument("--cloudflare-metrics-port", type=int, default=8770)
        command.add_argument("--launchagent-dir", type=Path)
        command.add_argument("--log-dir", type=Path)
        command.add_argument("--control-plane-log-name", default="dev.cao.dashboard.control-plane.log")
        command.add_argument("--edge-log-name", default="dev.cao.dashboard.edge.log")
        command.add_argument(
            "--cloudflare-log-name",
            default="dev.cao.dashboard.cloudflare-tunnel.log",
        )
        if name in {"upgrade", "restart"}:
            command.add_argument("--database-path", type=Path, required=True)
            command.add_argument("--backup-destination", type=Path, required=True)
            command.add_argument("--control-plane-plist-path", type=Path)
            command.add_argument("--control-plane-plist-sha256")
            command.add_argument("--edge-plist-path", type=Path)
            command.add_argument("--edge-plist-sha256")
            command.add_argument("--readiness-attempts", type=int, default=30)
            command.add_argument("--readiness-interval-seconds", type=float, default=1.0)
            command.add_argument("--plan-only", action="store_true")
        if name in {"apply", "remove", "upgrade", "restart"}:
            command.add_argument("--effect-receipt-file", type=Path)
            command.add_argument("--execute", action="store_true")
            command.add_argument("--owner-confirm", action="store_true")
    return parser


def _lifecycle_settings(args: argparse.Namespace) -> DashboardLifecycleSettings:
    from .dashboard_lifecycle import DashboardLifecycleSettings

    try:
        command = json.loads(args.control_plane_command_json)
    except json.JSONDecodeError as error:
        raise ValueError("--control-plane-command-json must be a JSON string array") from error
    if (
        not isinstance(command, list)
        or not command
        or not all(isinstance(item, str) for item in command)
    ):
        raise ValueError("--control-plane-command-json must be a non-empty JSON string array")
    try:
        dashboard_command = json.loads(args.dashboard_command_json)
    except json.JSONDecodeError as error:
        raise ValueError("--dashboard-command-json must be a JSON string array") from error
    if (
        not isinstance(dashboard_command, list)
        or not dashboard_command
        or not all(isinstance(item, str) for item in dashboard_command)
    ):
        raise ValueError("--dashboard-command-json must be a non-empty JSON string array")
    return DashboardLifecycleSettings(
        application_support_dir=args.application_support_dir,
        working_directory=args.working_directory,
        control_plane_command=tuple(command),
        dashboard_command=tuple(dashboard_command),
        credentials_file=args.credentials_file,
        bootstrap_record_dir=args.bootstrap_record_dir,
        session_record_dir=args.session_record_dir,
        cp_port=args.cp_port,
        edge_port=args.edge_port,
        tailscale_binary=args.tailscale_binary,
        exposure_provider=args.exposure_provider,
        cloudflared_binary=args.cloudflared_binary,
        cloudflare_token_file=args.cloudflare_token_file,
        cloudflare_hostname=args.cloudflare_hostname,
        cloudflare_access_team_domain=args.cloudflare_access_team_domain,
        cloudflare_metrics_port=args.cloudflare_metrics_port,
        launchagent_dir_override=args.launchagent_dir,
        log_dir_override=args.log_dir,
        control_plane_log_name=args.control_plane_log_name,
        edge_log_name=args.edge_log_name,
        cloudflare_log_name=args.cloudflare_log_name,
    )


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    if args.command == "issue":
        print(
            issue_dashboard_bootstrap_url(
                args.bootstrap_record_dir,
                args.public_origin,
                ttl_seconds=args.ttl_seconds,
            )
        )
        return 0

    if args.command == "lifecycle":
        from dataclasses import asdict

        from .dashboard_lifecycle import (
            CodexMCPRefreshPhaseError,
            CodexMCPRefreshPhaseUnknown,
            HttpxDashboardUpgradeProbe,
            HttpxProbe,
            SubprocessRunner,
            apply_dashboard_lifecycle,
            build_codex_mcp_refresh_plan,
            build_dashboard_lifecycle_plan,
            build_dashboard_upgrade_plan,
            dashboard_lifecycle_status,
            discover_codex_desktop_launchagent_binding,
            load_effect_authorization,
            refresh_codex_mcp,
            remove_dashboard_lifecycle,
            upgrade_dashboard_lifecycle,
        )

        if args.lifecycle_command == "refresh-mcp":
            refresh_plan = build_codex_mcp_refresh_plan(
                codex_desktop_launchagent=discover_codex_desktop_launchagent_binding(),
            )
            refresh_receipt = (
                load_effect_authorization(args.effect_receipt_file)
                if args.effect_receipt_file is not None
                else None
            )
            if args.plan_only:
                if refresh_receipt is not None or args.execute or args.owner_confirm:
                    raise ValueError("--plan-only cannot be combined with effect authorization")
                print(
                    json.dumps(
                        refresh_plan.to_dict(),
                        ensure_ascii=False,
                        indent=2,
                        sort_keys=True,
                    )
                )
                return 0
            try:
                refresh_result = refresh_codex_mcp(
                    refresh_plan,
                    receipt=refresh_receipt,
                    execute=args.execute,
                    owner_confirmed=args.owner_confirm,
                )
            except (
                CodexMCPRefreshPhaseError,
                CodexMCPRefreshPhaseUnknown,
            ) as error:
                print(
                    json.dumps(
                        error.to_dict(),
                        ensure_ascii=False,
                        indent=2,
                        sort_keys=True,
                    )
                )
                return 2
            print(
                json.dumps(
                    refresh_result.to_dict(),
                    ensure_ascii=False,
                    indent=2,
                    sort_keys=True,
                )
            )
            return 0

        lifecycle_settings = _lifecycle_settings(args)
        plan = build_dashboard_lifecycle_plan(lifecycle_settings)
        if args.lifecycle_command == "plan":
            print(json.dumps(plan.to_dict(), ensure_ascii=False, indent=2, sort_keys=True))
            return 0
        runner = SubprocessRunner()
        if args.lifecycle_command == "status":
            status = dashboard_lifecycle_status(
                plan, runner, probe=HttpxProbe(), credentials_file=args.credentials_file
            )
            print(json.dumps(asdict(status), ensure_ascii=False, indent=2, sort_keys=True))
            return 0 if status.ready else 2
        receipt = (
            load_effect_authorization(args.effect_receipt_file)
            if args.effect_receipt_file is not None
            else None
        )
        if args.lifecycle_command in {"upgrade", "restart"}:
            from .database import inspect_sqlite_database

            upgrade_plan = build_dashboard_upgrade_plan(
                plan,
                database_path=args.database_path,
                backup_destination=args.backup_destination,
                credentials_file=args.credentials_file,
                source_identity=inspect_sqlite_database(args.database_path),
                control_plane_plist_path=args.control_plane_plist_path,
                control_plane_plist_sha256=args.control_plane_plist_sha256,
                edge_plist_path=args.edge_plist_path,
                edge_plist_sha256=args.edge_plist_sha256,
                codex_desktop_launchagent=discover_codex_desktop_launchagent_binding(),
                readiness_attempts=args.readiness_attempts,
                readiness_interval_seconds=args.readiness_interval_seconds,
            )
            if args.plan_only:
                if receipt is not None or args.execute or args.owner_confirm:
                    raise ValueError("--plan-only cannot be combined with effect authorization")
                print(
                    json.dumps(
                        upgrade_plan.to_dict(),
                        ensure_ascii=False,
                        indent=2,
                        sort_keys=True,
                    )
                )
                return 0
            try:
                result = upgrade_dashboard_lifecycle(
                    upgrade_plan,
                    runner,
                    probe=HttpxDashboardUpgradeProbe(HttpxProbe()),
                    receipt=receipt,
                    execute=args.execute,
                    owner_confirmed=args.owner_confirm,
                )
            except (
                CodexMCPRefreshPhaseError,
                CodexMCPRefreshPhaseUnknown,
            ) as error:
                print(
                    json.dumps(
                        error.to_dict(),
                        ensure_ascii=False,
                        indent=2,
                        sort_keys=True,
                    )
                )
                return 2
            print(json.dumps(result.to_dict(), ensure_ascii=False, indent=2, sort_keys=True))
            return 0
        if args.lifecycle_command == "apply":
            apply_dashboard_lifecycle(
                plan,
                runner,
                receipt=receipt,
                execute=args.execute,
                owner_confirmed=args.owner_confirm,
            )
        else:
            remove_dashboard_lifecycle(
                plan,
                runner,
                receipt=receipt,
                execute=args.execute,
                owner_confirmed=args.owner_confirm,
            )
        print(json.dumps({"status": "effect-submitted", "plan_digest": plan.plan_digest}))
        return 0

    credentials = load_dashboard_credentials(args.credentials_file)
    if args.command == "mcp-stdio":
        # The bearer is loaded only from the checked owner-only file and held
        # in process memory while proxying.  It is never accepted from argv,
        # environment, or a Codex MCP configuration value.
        return asyncio.run(
            serve_stdio_proxy(
                dashboard_mcp_endpoint(
                    credentials.upstream_base_url,
                    allowed_private_upstream_hosts=credentials.allowed_private_upstream_hosts,
                ),
                credentials.dashboard_bearer,
            )
        )
    if args.command == "snapshot":
        print(
            DashboardTextClient(
                credentials.upstream_base_url,
                credentials.dashboard_bearer,
                allowed_private_upstream_hosts=credentials.allowed_private_upstream_hosts,
            ).snapshot()
        )
        return 0
    if args.command == "follow":
        for line in DashboardTextClient(
            credentials.upstream_base_url,
            credentials.dashboard_bearer,
            allowed_private_upstream_hosts=credentials.allowed_private_upstream_hosts,
        ).follow(after=args.after, limit=args.limit):
            print(line)
        return 0

    settings = DashboardEdgeSettings(
        upstream_base_url=credentials.upstream_base_url,
        dashboard_bearer=credentials.dashboard_bearer,
        bootstrap_record_dir=args.bootstrap_record_dir,
        session_record_dir=args.session_record_dir,
        public_origin=credentials.public_origin,
        allowed_private_upstream_hosts=credentials.allowed_private_upstream_hosts,
        cloudflare_access=credentials.cloudflare_access,
    )
    uvicorn.run(create_dashboard_edge(settings), host=args.host, port=args.port)
    return 0


if __name__ == "__main__":  # pragma: no cover - package script entry point
    sys.exit(main())
