"""Observe official native status and database commit hints without polling.

Status notifications renew observation, not authority. Canonical native APIs
and the existing ledger still prove every recovery and exact Work transition.
"""
from __future__ import annotations

import asyncio
import json
import sqlite3
from contextlib import suppress
from pathlib import Path
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from .service import ControlPlane


class NativeChangeFeed:
    def __init__(self, service: ControlPlane) -> None:
        self.service = service
        self.changed = asyncio.Event()
        self._sync = asyncio.Event()
        self._task: asyncio.Task[None] | None = None
        self.thread_revisions: dict[str, int] = {}
        self.connection_revision = 0
        self.generation = 0
        self.error = ""
        self._known: set[str] = set()
        self._statuses: dict[str, str] = {}
        self._capabilities: dict[tuple[str, str], str] = {}
        self._pending: dict[int, asyncio.Future[dict[str, Any]]] = {}
        self._next_id = 1
        self._rpc: Any = None
        self._database: sqlite3.Connection | None = None
        self._data_version = 0
        self._filesystem: Any = None
        self._native_paths: dict[Path, str] = {}
        self._foreign_pending: set[str] = set()
        self._foreign_heads: dict[str, tuple[Any, ...]] = {}

    async def start(self) -> None:
        self._task = asyncio.create_task(self._run(), name="cao-native-change-subscription")

    async def stop(self) -> None:
        if self._task is not None:
            self._task.cancel()
            await asyncio.gather(self._task, return_exceptions=True)
            self._task = None

    def sync(self) -> None:
        self._sync.set()

    def revision(self, thread_id: str) -> tuple[int, int]:
        return self.connection_revision, self.thread_revisions.get(thread_id, 0)

    async def _request(self, method: str, params: dict[str, Any]) -> dict[str, Any]:
        identity = self._next_id
        self._next_id += 1
        future: asyncio.Future[dict[str, Any]] = asyncio.get_running_loop().create_future()
        self._pending[identity] = future
        try:
            await self._rpc.send({"id": identity, "method": method, "params": params})
            async with asyncio.timeout(self.service.settings.runtime_mcp_startup_timeout_seconds):
                return await future
        finally:
            self._pending.pop(identity, None)

    async def _read(self) -> None:
        while True:
            message = await self._rpc.read_line(None)
            identity = message.get("id")
            if identity in self._pending:
                pending = self._pending[identity]
                if not pending.done():
                    if "error" in message:
                        pending.set_exception(ValueError("native_change_subscription_request_failed"))
                    else:
                        pending.set_result(message.get("result", {}))
                continue
            method, params = message.get("method"), message.get("params", {})
            if method == "thread/status/changed":
                self.observe_status(params)
            elif method == "mcpServer/startupStatus/updated":
                self.observe_capability(params)

    def _renew(self, thread: str) -> None:
        self.thread_revisions[thread] = self.thread_revisions.get(thread, 0) + 1
        self.generation += 1
        self.changed.set()

    def observe_status(self, params: dict[str, Any]) -> None:
        thread = str(params.get("threadId") or "")
        status = params.get("status")
        if thread not in self._known or not isinstance(status, dict):
            return
        encoded = json.dumps(status, sort_keys=True)
        if self._statuses.get(thread) != encoded:
            self._statuses[thread] = encoded
            self._renew(thread)

    def observe_paths(self, paths: list[str]) -> None:
        database = self.service.settings.database_path.resolve()
        targets = {database, Path(str(database) + "-wal"), database.parent}
        resolved = {Path(raw).resolve() for raw in paths}
        for path in resolved & self._native_paths.keys():
            thread = self._native_paths[path]
            status = json.loads(self._statuses.get(thread, '{"type":"notLoaded"}'))
            if status.get("type") == "notLoaded":
                self._foreign_pending.add(thread)
                if not path.exists():
                    self._known.discard(thread)
                self._sync.set()
        if not resolved & targets:
            return
        if self._database is not None:
            # A directory hint may describe an unrelated log write. SQLite's
            # event-driven data_version check proves a committed database change.
            version = int(self._database.execute("PRAGMA data_version").fetchone()[0])
            if version == self._data_version:
                return
            self._data_version = version
            self.service.db.notify_external_commit()
        self.changed.set()

    def observe_capability(self, params: dict[str, Any]) -> None:
        thread, name, status = (str(params.get(key) or "") for key in ("threadId", "name", "status"))
        if thread not in self._known or not name or status not in {"ready", "failed"}:
            return
        key = thread, name
        if self._capabilities.get(key) == status:
            return
        self._capabilities[key] = status
        from .canonical import canonical_sha256
        digest = canonical_sha256(sorted((server, state) for (native, server), state in self._capabilities.items() if native == thread))
        with self.service.db.transaction() as connection:
            for attachment in connection.execute(
                "SELECT id FROM cao_session_attachments WHERE native_thread_id=? AND state='active'", (thread,),
            ).fetchall():
                prior = connection.execute(
                    "SELECT data_json FROM events WHERE event_type='cao.native_capability_observed' "
                    "AND aggregate_id=? ORDER BY sequence DESC LIMIT 1", (attachment["id"],),
                ).fetchone()
                if prior is None or json.loads(prior["data_json"]).get("digest") != digest:
                    self.service._event(connection, "cao.native_capability_observed", "cao_attachment", attachment["id"], "", {"digest": digest})
        self._renew(thread)

    async def _synchronize(self) -> None:
        rows = self.service.db.fetchall(
            "SELECT native_thread_id AS thread_id FROM cao_session_attachments WHERE state='active' "
            "UNION SELECT runtime.native_session_id FROM runtime_sessions AS runtime "
            "JOIN attempts AS attempt ON attempt.runtime_session_id=runtime.id "
            "JOIN work_items AS work ON work.id=attempt.work_item_id "
            "WHERE runtime.adapter='codex-app-server' AND runtime.native_session_id IS NOT NULL "
            "AND work.state IN ('active','waiting_supervisor','user_needed','open')"
        )
        current = {str(row["thread_id"]) for row in rows if row["thread_id"]}
        self._known.intersection_update(current)
        self._native_paths = {path: thread for path, thread in self._native_paths.items() if thread in current}
        self._foreign_heads = {thread: head for thread, head in self._foreign_heads.items() if thread in current}
        self._statuses = {thread: status for thread, status in self._statuses.items() if thread in current}
        self._capabilities = {key: value for key, value in self._capabilities.items() if key[0] in current}
        for thread in sorted(current - self._known):
            self._known.add(thread)
            try:
                # Seed the supported read-only status stream once; no writer is
                # resumed, no transcript path is read and no turn is submitted.
                result = await self._request("thread/read", {"threadId": thread, "includeTurns": False})
                path = result.get("thread", {}).get("path")
                if isinstance(path, str) and path:
                    self._native_paths = {target: owner for target, owner in self._native_paths.items() if owner != thread}
                    self._native_paths[Path(path).resolve()] = thread
                status = result.get("thread", {}).get("status")
                if isinstance(status, dict):
                    self._statuses.setdefault(thread, json.dumps(status, sort_keys=True))
            except ValueError:
                # A missing exact native thread is not a reason to reread it on
                # every unrelated commit. Changed binding/reconnect retries it.
                pass
        if self._filesystem is not None:
            database = self.service.settings.database_path.resolve()
            self._filesystem.set_paths([database, Path(str(database) + "-wal"), *self._native_paths])
        pending = self._foreign_pending & current
        self._foreign_pending.difference_update(pending)
        for thread in sorted(pending):
            # This read has an actual foreign-writer storage event, not a
            # clock tick. Coalesced events share one bounded head transfer.
            try:
                result = await self._request("thread/turns/list", {
                    "threadId": thread, "itemsView": "summary", "limit": 1, "sortDirection": "desc",
                })
            except ValueError:
                # Exact-thread absence degrades this observation, not another
                # conversation's change stream or ordinary lifecycle control.
                continue
            turns = result.get("data", [])
            if not isinstance(turns, list) or not turns or not isinstance(turns[0], dict):
                continue
            turn = turns[0]
            head = tuple(turn.get(key) for key in ("id", "status", "startedAt", "completedAt"))
            previous = self._foreign_heads.get(thread)
            self._foreign_heads[thread] = head
            if (head != previous and turn.get("status") in {"completed", "failed", "interrupted"}
                    and type(turn.get("startedAt")) is int and type(turn.get("completedAt")) is int
                    and turn["completedAt"] >= turn["startedAt"]):
                self._renew(thread)

    async def _run(self) -> None:
        from .runtime import _CODEX_DESKTOP_CONTROL_SOCKET, _JsonRpcDesktopSocket
        retry_delay = 1.0
        while True:
            reader: asyncio.Task[None] | None = None
            sync: asyncio.Task[bool] | None = None
            try:
                self._rpc = await _JsonRpcDesktopSocket.connect(
                    _CODEX_DESKTOP_CONTROL_SOCKET, timeout=self.service.settings.runtime_mcp_startup_timeout_seconds,
                )
                await self._rpc.request("initialize", {
                    "clientInfo": {"name": "cao-native-change-feed", "version": "1"},
                    "capabilities": {"experimentalApi": True},
                })
                await self._rpc.send({"method": "initialized", "params": {}})
                self._known.clear()
                self._statuses.clear()
                self._capabilities.clear()
                from .filesystem_events import FilesystemEvents
                self._filesystem = FilesystemEvents(self.observe_paths)
                reader = asyncio.create_task(self._read())
                self._database = sqlite3.connect(self.service.settings.database_path.resolve().as_uri() + "?mode=ro", uri=True)
                self._data_version = int(self._database.execute("PRAGMA data_version").fetchone()[0])
                self._sync.clear()
                await self._synchronize()
                self.connection_revision += 1
                self.generation += 1
                self.error = ""
                self.changed.set()
                retry_delay = 1.0
                while True:
                    sync = asyncio.create_task(self._sync.wait())
                    await asyncio.wait({sync, reader, self._filesystem.failure}, return_when=asyncio.FIRST_COMPLETED)
                    if self._filesystem.failure.done():
                        await self._filesystem.failure
                    if reader.done():
                        await reader
                        raise ConnectionError("native_change_subscription_closed")
                    self._sync.clear()
                    await self._synchronize()
            except asyncio.CancelledError:
                raise
            except Exception:
                self.error = "native_change_subscription_unavailable"
                self.changed.set()
            finally:
                for task in (reader, sync):
                    if task is not None:
                        task.cancel()
                await asyncio.gather(*(task for task in (reader, sync) if task is not None), return_exceptions=True)
                for pending in self._pending.values():
                    if not pending.done():
                        pending.cancel()
                if self._filesystem is not None:
                    self._filesystem.close()
                    self._filesystem = None
                if self._database is not None:
                    self._database.close()
                    self._database = None
                if self._rpc is not None:
                    with suppress(Exception):
                        await self._rpc.close()
                    self._rpc = None
            # Recovery attempts after a failed connection; never idle state reads.
            await asyncio.sleep(retry_delay)
            retry_delay = min(retry_delay * 2, 30.0)
