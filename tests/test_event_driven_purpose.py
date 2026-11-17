from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta
from unittest.mock import patch

import pytest
from test_purpose_continuation import _checkpoint

from cao_control_plane.api import _dispatcher_readiness
from cao_control_plane.errors import ConflictError
from cao_control_plane.instruction_review import _alignment_input
from cao_control_plane.models import CheckpointNextInput, InstructWorkerThreadInput
from cao_control_plane.native_changes import NativeChangeFeed
from cao_control_plane.runtime import Dispatcher, _requester_source_items


def test_checkpoint_cannot_commit_without_a_next_state(system, tmp_path):
    service, actor, _, work, boundary, request = _checkpoint(system, tmp_path, accept=False)
    with pytest.raises(ConflictError) as error:
        service.dispose_boundary(actor, boundary['id'], request)
    assert error.value.details['reason_code'] == 'checkpoint_next_action_required'
    current = service.get_work(work['id'])
    assert current['state'] == 'waiting_supervisor'
    assert current['purpose_continuations'] == []
    assert not service.db.fetchone('SELECT 1 FROM boundary_dispositions WHERE boundary_id=?', (boundary['id'],))


def test_checkpoint_accepts_and_assigns_next_work_once_without_an_intermediate_notification(system, tmp_path):
    service, actor, thread, work, boundary, request = _checkpoint(system, tmp_path, accept=False)
    action = CheckpointNextInput(kind='continue', reason='One unmet outcome remains.',
        evidence='The first verified checkpoint supplies the next outcome.',
        next_instruction=InstructWorkerThreadInput(worker_thread_id=thread, expected_purpose_version=1,
            instruction='Implement the next distinct unmet outcome using the verified checkpoint.',
            completion_contract='no_artifact_expected', idempotency_key='atomic-next'))
    request = request.model_copy(update={'next_action': action})
    accepted = service.dispose_boundary(actor, boundary['id'], request)
    assert service.dispose_boundary(actor, boundary['id'], request) == accepted
    current = service.get_work(work['id'])
    continuation = current['purpose_continuations'][0]
    assert continuation['state'] == 'assigned'
    assert continuation['next_work_item_id'] != work['id']
    assert service.db.fetchone('SELECT count(*) FROM work_items WHERE managed_worker_thread_id=?', (thread,))[0] == 2
    assert service.db.fetchone("SELECT count(*) FROM messages WHERE json_extract(payload_json,'$.purpose_continuation_id')=?", (continuation['id'],))[0] == 0


def test_failed_successor_rolls_back_checkpoint_and_next_obligation(system, tmp_path):
    service, actor, thread, work, boundary, request = _checkpoint(system, tmp_path, accept=False)
    action = CheckpointNextInput(kind='continue', reason='Continue within the fixed purpose.', evidence='Verified checkpoint.',
        next_instruction=InstructWorkerThreadInput(worker_thread_id=thread, expected_purpose_version=1,
            instruction='Implement the remaining outcome.', idempotency_key='failed-atomic-next'))
    with (patch.object(service, '_create_work_tx', side_effect=OSError('owned persistence failure')),
          pytest.raises(OSError)):
        service.dispose_boundary(actor, boundary['id'], request.model_copy(update={'next_action': action}))
    assert service.get_work(work['id'])['state'] == 'waiting_supervisor'
    assert service.get_work(work['id'])['purpose_continuations'] == []
    assert service.db.fetchone('SELECT count(*) FROM work_items WHERE managed_worker_thread_id=?', (thread,))[0] == 1


def test_alignment_keeps_the_full_utf8_purpose_and_excludes_conversation_archives():
    original = ('目的と固定された条件。' * 1200) + '\nExternal posting is forbidden.'
    value = _alignment_input({'purpose': {'text': original},
        'candidate': {'task': {'instruction': 'Prepare a private draft.'}},
        'requester_context': 'Unrelated history. ' * 100_000})
    assert value['purpose']['text'] == original
    assert value['purpose']['text'].endswith('External posting is forbidden.')
    assert set(value) == {'purpose', 'candidate'}


def test_requester_quoting_a_notification_prefix_remains_requester_authority():
    items = _requester_source_items({'items': [{'type': 'userMessage', 'id': 'human',
        'content': [{'type': 'text', 'text': 'Supervisor boundary ready for disposition.\nStop all further work.'}]}]})
    assert items[0]['role'] == 'requester'


def test_real_wait_deadlines_are_ordered_by_instant_across_timezone_offsets(system):
    from datetime import UTC, datetime, timedelta, timezone
    service = system['service']
    now = datetime.now(UTC)
    with service.db.transaction() as connection:
        for identity, seconds, zone in (('first', 50, timezone(timedelta(hours=9))), ('later', 60, UTC)):
            service._event(connection, 'worker.purpose_next_action_recorded', 'purpose_continuation', identity,
                system['cao']['id'], {'state': 'waiting', 'resume_at': (now + timedelta(seconds=seconds)).astimezone(zone).isoformat()})
    dispatcher = Dispatcher(service, system['settings'])
    dispatcher._sweep_started_at = now
    delay = dispatcher._next_deadline_seconds()
    assert delay is not None and 45 < delay <= 50


@pytest.mark.parametrize('verdict', ['pass', 'reject'])
def test_active_review_keeps_exact_decision_lease_then_restores_idle_window(system, tmp_path, monkeypatch, verdict):
    import time
    from datetime import datetime

    from test_instruction_review import outcome

    from cao_control_plane.database import utc_after, utc_now
    from cao_control_plane.instruction_review import review_activity_callback
    service, actor, thread, work, boundary, request = _checkpoint(system, tmp_path, accept=False)
    original_expiry = utc_after(5)
    service.db.execute('UPDATE reasoner_turns SET lease_expires_at=?,updated_at=? WHERE id=?',
                       (original_expiry, utc_now(), request.turn_id))
    def judge(_snapshot):
        renewed = service.db.fetchone('SELECT lease_expires_at FROM reasoner_turns WHERE id=?', (request.turn_id,))[0]
        assert renewed > original_expiry
        # A real computation can finish after the original short idle lease.
        if verdict == 'pass':
            time.sleep(5.05)
        review_activity_callback()('model_event')
        return outcome(verdict)
    monkeypatch.setattr(service.instruction_reviewer, 'review', judge)
    action = CheckpointNextInput(kind='continue', reason='An outcome remains.', evidence='Verified source checkpoint.',
        next_instruction=InstructWorkerThreadInput(worker_thread_id=thread, expected_purpose_version=1,
            instruction='Implement the next distinct unmet outcome.', completion_contract='no_artifact_expected',
            idempotency_key='review-lease-successor'))
    if verdict == 'pass':
        service.dispose_boundary(actor, boundary['id'], request.model_copy(update={'next_action': action}))
        assert service.get_work(work['id'])['purpose_continuations'][0]['state'] == 'assigned'
    else:
        with pytest.raises(ConflictError):
            service.dispose_boundary(actor, boundary['id'], request.model_copy(update={'next_action': action}))
        row = service.db.fetchone('SELECT * FROM reasoner_turns WHERE id=?', (request.turn_id,))
        remaining = (datetime.fromisoformat(row['lease_expires_at']) - datetime.fromisoformat(utc_now())).total_seconds()
        assert row['state'] == 'leased' and 0 < remaining <= 5.5


def test_disconnected_command_result_wakes_only_exact_active_attachment_once(system):
    from test_managed_worker_command_admission import _attached
    service = system['service']
    actor = _attached(service, suffix='lost-command-owner')
    other = _attached(service, suffix='other-command-owner')
    before = service.db.fetchone('SELECT count(*) FROM work_items')[0]
    for nonce in ('first-connection', 'replacement-connection'):
        service.record_disconnected_command_result(actor, operation='cao_review',
            arguments={'attempt_id': 'owned-attempt', 'lease_token': nonce}, succeeded=True)
    rows = service.db.fetchall("SELECT m.payload_json,d.recipient_attachment_id,d.runtime_session_id FROM messages m "
        "JOIN message_deliveries d ON d.message_id=m.id WHERE json_extract(m.payload_json,'$.action')='cao_command_result_available'")
    assert len(rows) == 1
    assert rows[0]['recipient_attachment_id'] == actor['_cao_attachment_id']
    assert rows[0]['recipient_attachment_id'] != other['_cao_attachment_id']
    assert rows[0]['runtime_session_id'] is not None
    assert 'lease_token' not in rows[0]['payload_json'] and 'owned-attempt' not in rows[0]['payload_json']
    service.db.execute("UPDATE cao_session_attachments SET state='revoked' WHERE id=?", (actor['_cao_attachment_id'],))
    service.record_disconnected_command_result(actor, operation='cao_review', arguments={'attempt_id': 'different'}, succeeded=False)
    assert service.db.fetchone("SELECT count(*) FROM messages WHERE json_extract(payload_json,'$.action')='cao_command_result_available'")[0] == 1
    assert service.db.fetchone('SELECT count(*) FROM work_items')[0] == before


@pytest.mark.asyncio
async def test_disconnected_progress_stream_retains_one_command_and_reports_its_actual_result(system, monkeypatch):
    import threading

    from test_managed_worker_command_admission import _attached

    from cao_control_plane.instruction_review import review_activity_callback
    from cao_control_plane.mcp import MCPServer
    service = system['service']
    actor = _attached(service, suffix='lost-progress-owner')
    server = MCPServer(service)
    request = {'jsonrpc': '2.0', 'id': 1, 'method': 'tools/call', 'params': {
        'name': 'cao_review', 'arguments': {'attempt_id': 'owned'}, '_meta': {'progressToken': 'owned-progress'}}}
    release = threading.Event()
    started = asyncio.Event()
    loop = asyncio.get_running_loop()
    executed = []
    def handle(_actor, _request):
        executed.append(1)
        review_activity_callback()('review_computing')
        loop.call_soon_threadsafe(started.set)
        assert release.wait(2)
        return {'jsonrpc': '2.0', 'id': 1, 'result': {'isError': False}}
    monkeypatch.setattr(server, 'handle_modern', handle)
    stream = server.progress_messages(actor, request)
    assert (await anext(stream))['params']['message'] == 'started'
    await asyncio.wait_for(started.wait(), 1)
    assert (await anext(stream))['params']['message'] == 'review_computing'
    await stream.aclose()
    release.set()
    # Observe the canonical commit signal, never repeatedly query for completion.
    generation = service.db.commit_generation()
    await asyncio.to_thread(service.db.wait_for_commit, generation, 2)
    assert executed == [1]
    assert service.db.fetchone("SELECT count(*) FROM messages WHERE json_extract(payload_json,'$.action')='cao_command_result_available'")[0] == 1


@pytest.mark.asyncio
async def test_subscription_keepalive_does_not_read_state_again_without_a_commit(system, monkeypatch):
    from cao_control_plane.mcp import MCPServer
    server = MCPServer(system['service'])
    reads = []
    monkeypatch.setattr(server, '_resource_revision', lambda actor, uri: reads.append(uri) or 'unchanged')
    request = {'jsonrpc': '2.0', 'id': 2, 'method': 'subscriptions/listen', 'params': {
        'notifications': {'resourceSubscriptions': ['cao://events']}}}
    stream = server.subscription_messages(system['cao'], request, heartbeat_seconds=0.01, poll_seconds=0.001)
    await anext(stream)
    assert await anext(stream) is None
    assert await anext(stream) is None
    assert reads == ['cao://events']
    await stream.aclose()


@pytest.mark.asyncio
async def test_idle_dispatcher_has_no_periodic_scan_and_native_change_wakes_once(system):
    service = system['service']
    dispatcher = Dispatcher(service, system['settings'])
    cycles = []
    observed = asyncio.Event()
    def scan():
        cycles.append(1)
        observed.set()
        return 0
    async def nothing():
        pass
    dispatcher._start_background_jobs = scan
    dispatcher.native_changes.start = nothing
    dispatcher.native_changes.stop = nothing
    await dispatcher.start()
    try:
        await asyncio.wait_for(observed.wait(), 1)
        observed.clear()
        await asyncio.sleep(0.06)  # six former fixture scan intervals
        assert len(cycles) == 1 and dispatcher.waiting_for_event
        dispatcher.native_changes.changed.set()
        await asyncio.wait_for(observed.wait(), 1)
        await asyncio.sleep(0.03)
        assert len(cycles) == 2
    finally:
        await dispatcher.stop()


@pytest.mark.asyncio
async def test_dispatcher_wake_keeps_readiness_until_the_stale_budget_expires(system, monkeypatch):
    dispatcher = Dispatcher(system['service'], system['settings'])
    waiting, cleaning_up, release = asyncio.Event(), asyncio.Event(), asyncio.Event()
    native_wait = dispatcher.native_changes.changed.wait

    async def delayed_native_wait():
        waiting.set()
        try:
            await native_wait()
        finally:
            cleaning_up.set()
            await release.wait()

    async def nothing():
        pass

    monkeypatch.setattr(dispatcher, '_start_background_jobs', lambda: 0)
    monkeypatch.setattr(dispatcher.native_changes, 'start', nothing)
    monkeypatch.setattr(dispatcher.native_changes, 'stop', nothing)
    monkeypatch.setattr(dispatcher.native_changes.changed, 'wait', delayed_native_wait)
    await dispatcher.start()
    try:
        await asyncio.wait_for(waiting.wait(), 1)
        dispatcher.last_cycle_at = '2000-01-01T00:00:00Z'
        assert _dispatcher_readiness(dispatcher, system['settings'])['healthy']

        system['service'].db.notify_external_commit()
        await asyncio.wait_for(cleaning_up.wait(), 1)
        assert dispatcher.running and not dispatcher.waiting_for_event
        resumed = _dispatcher_readiness(dispatcher, system['settings'])
        assert resumed['healthy']

        stalled_at = datetime.now(UTC) + timedelta(seconds=resumed['stale_after_seconds'] + 1)

        class AfterStaleBudget(datetime):
            @classmethod
            def now(cls, tz=None):
                return stalled_at if tz else stalled_at.replace(tzinfo=None)

        monkeypatch.setattr('cao_control_plane.api.datetime', AfterStaleBudget)
        stalled = _dispatcher_readiness(dispatcher, system['settings'])
        assert stalled['issues'] == ['dispatcher_cycle_stale']
    finally:
        release.set()
        await dispatcher.stop()


def test_only_actual_native_status_changes_renew_probes(system):
    feed = NativeChangeFeed(system['service'])
    feed._known.add('owned-thread')
    feed.observe_status({'threadId': 'owned-thread', 'status': {'type': 'active', 'activeFlags': []}})
    assert feed.revision('owned-thread') == (0, 1)
    feed.changed.clear()
    feed.observe_status({'threadId': 'owned-thread', 'status': {'activeFlags': [], 'type': 'active'}})
    assert not feed.changed.is_set()
    feed.observe_status({'threadId': 'unrelated-thread', 'status': {'type': 'idle'}})
    assert not feed.changed.is_set()
    feed.observe_status({'threadId': 'owned-thread', 'status': {'type': 'idle'}})
    assert feed.revision('owned-thread') == (0, 2) and feed.changed.is_set()


@pytest.mark.asyncio
async def test_native_subscription_initialization_and_requests_use_real_settings(system, monkeypatch):
    feed = NativeChangeFeed(system['service'])
    closed = []
    class RPC:
        async def request(self, method, params):
            assert method == 'initialize'
            return {}, {}
        async def send(self, message):
            if 'id' in message:
                feed._pending[message['id']].set_result({'subscribed': True})
        async def close(self):
            closed.append(True)
    async def connect(socket, *, timeout):
        assert timeout == system['settings'].runtime_mcp_startup_timeout_seconds
        return RPC()
    async def reader():
        await asyncio.Event().wait()
    async def synchronize():
        assert await feed._request('owned-read', {}) == {'subscribed': True}
    monkeypatch.setattr('cao_control_plane.runtime._JsonRpcDesktopSocket.connect', connect)
    monkeypatch.setattr(feed, '_read', reader)
    monkeypatch.setattr(feed, '_synchronize', synchronize)
    task = asyncio.create_task(feed._run())
    try:
        await asyncio.wait_for(feed.changed.wait(), 1)
        assert feed.error == '' and feed.connection_revision == 1
    finally:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
    assert closed == [True]


@pytest.mark.asyncio
async def test_native_filesystem_subscription_rearms_atomic_replacement_and_stays_idle(tmp_path):
    from cao_control_plane.filesystem_events import FilesystemEvents
    path = tmp_path / 'owned-evidence.txt'
    path.write_text('first')
    delivered = asyncio.Event()
    observed = []
    watcher = FilesystemEvents(lambda paths: (observed.extend(paths), delivered.set()))
    try:
        watcher.set_paths([path])
        await asyncio.sleep(0.03)
        assert not observed
        replacement = tmp_path / 'replacement.txt'
        replacement.write_text('second')
        replacement.replace(path)
        await asyncio.wait_for(delivered.wait(), 2)
        assert str(path.resolve()) in observed
        observed.clear()
        delivered.clear()
        path.write_text('third value')
        await asyncio.wait_for(delivered.wait(), 2)
        assert observed == [str(path.resolve())]
        delivered.clear()
        await asyncio.sleep(0.03)
        assert not delivered.is_set()
    finally:
        watcher.close()


def test_database_directory_hints_require_a_real_cross_connection_commit(system):
    import sqlite3
    feed = NativeChangeFeed(system['service'])
    connection = sqlite3.connect(feed.service.settings.database_path.as_uri() + '?mode=ro', uri=True)
    feed._database = connection
    feed._data_version = connection.execute('PRAGMA data_version').fetchone()[0]
    try:
        feed.observe_paths([str(feed.service.settings.database_path.parent)])
        assert not feed.changed.is_set()
        system['service'].db.execute("INSERT INTO metadata(key,value) VALUES('native-event-test','committed')")
        feed.observe_paths([str(feed.service.settings.database_path.parent)])
        assert feed.changed.is_set()
        feed.changed.clear()
        feed.observe_paths([str(feed.service.settings.database_path.parent)])
        assert not feed.changed.is_set()
    finally:
        connection.close()
        feed._database = None


@pytest.mark.asyncio
async def test_foreign_writer_storage_event_requires_exact_terminal_chronology(system, tmp_path):
    feed = NativeChangeFeed(system['service'])
    thread = 'owned-foreign-thread'
    path = (tmp_path / 'provider-source.jsonl').resolve()
    path.write_text('opaque source never read by the adapter')
    feed._native_paths[path] = thread
    feed._known.add(thread)
    feed._statuses[thread] = '{"type":"notLoaded"}'
    status, completed = 'interrupted', None
    calls = []
    async def request(method, params):
        calls.append(method)
        assert method == 'thread/turns/list' and params['limit'] == 1
        return {'data': [{'id': 'exact-turn', 'status': status, 'startedAt': 10, 'completedAt': completed}]}
    feed._request = request
    with patch.object(system['service'].db, 'fetchall', return_value=[{'thread_id': thread}]):
        feed.observe_paths([str(path)])
        await feed._synchronize()
        assert not feed.changed.is_set()
        await feed._synchronize()
        assert calls == ['thread/turns/list']
        status, completed = 'completed', 20
        feed.observe_paths([str(path)])
        await feed._synchronize()
        assert feed.changed.is_set() and feed.revision(thread) == (0, 1)
        feed.changed.clear()
        feed.observe_paths([str(path)])
        await feed._synchronize()
        assert not feed.changed.is_set()


@pytest.mark.asyncio
async def test_missing_foreign_thread_does_not_restart_other_observations(system):
    feed = NativeChangeFeed(system['service'])
    feed._known.update({'absent-owned-thread', 'present-owned-thread'})
    feed._foreign_pending.update(feed._known)
    calls = []
    async def request(method, params):
        calls.append(params['threadId'])
        if params['threadId'] == 'absent-owned-thread':
            raise ValueError('native_change_subscription_request_failed')
        return {'data': [{'id': 'terminal', 'status': 'completed', 'startedAt': 10, 'completedAt': 20}]}
    feed._request = request
    with patch.object(system['service'].db, 'fetchall', return_value=[{'thread_id': value} for value in feed._known]):
        await feed._synchronize()
        assert feed.revision('present-owned-thread') == (0, 1)
        assert feed.revision('absent-owned-thread') == (0, 0)
        await feed._synchronize()
        assert calls == ['absent-owned-thread', 'present-owned-thread']


def test_pre_upgrade_accepted_checkpoint_is_recovered_once_without_replaying_work(system, tmp_path):
    service, actor, _, work, boundary, request = _checkpoint(system, tmp_path, accept=False)
    # Seed the accepted state written before the continuation feature existed.
    with patch.object(service, '_accept_checkpoint_next_tx', return_value=None):
        service.dispose_boundary(actor, boundary['id'], request)
    assert service.get_work(work['id'])['purpose_continuations'] == []
    before = service.db.fetchone('SELECT count(*) FROM work_items')[0]
    assert service.reconcile_cao_supervision_obligations() == 1
    current = service.get_work(work['id'])
    assert current['state'] == 'waiting_user'
    assert current['purpose_continuations'][0]['state'] == 'pending'
    assert service.db.fetchone('SELECT count(*) FROM work_items')[0] == before
    assert service.reconcile_cao_supervision_obligations() == 0
    assert len(service.get_work(work['id'])['purpose_continuations']) == 1


def test_existing_successor_keeps_legacy_checkpoint_from_generating_a_duplicate_decision(system, tmp_path):
    service, actor, thread, _, boundary, request = _checkpoint(system, tmp_path, accept=False)
    with patch.object(service, '_accept_checkpoint_next_tx', return_value=None):
        service.dispose_boundary(actor, boundary['id'], request)
    service.instruct_worker_thread(actor, InstructWorkerThreadInput(
        worker_thread_id=thread, expected_purpose_version=1, instruction='Investigate the next unmet condition.',
        completion_contract='no_artifact_expected', idempotency_key='existing-pre-upgrade-successor'))
    before = service.db.fetchone('SELECT count(*) FROM messages')[0]
    assert service.reconcile_cao_supervision_obligations() == 0
    assert service.db.fetchone('SELECT count(*) FROM messages')[0] == before


def test_owner_review_effort_preserves_configured_default_and_profile_override(tmp_path):
    from cao_control_plane.instruction_review import _owner_review_effort
    preferences = tmp_path / 'preferences.toml'
    preferences.write_text('model_reasoning_effort = "max"\n')
    assert _owner_review_effort(preferences) == 'max'
    preferences.write_text('model_reasoning_effort = "max"\nprofile = "selected"\n[profiles.selected]\nmodel_reasoning_effort = "high"\n')
    assert _owner_review_effort(preferences) == 'high'
