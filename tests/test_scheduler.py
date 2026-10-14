"""Scheduler execution integration without network calls or message delivery."""

import asyncio
from dataclasses import dataclass

import pytest

from inkbox_codex.config import BridgeConfig
from inkbox_codex import scheduler as scheduler_module
from inkbox_codex.scheduler import Scheduler
from inkbox_codex.schedules import ScheduleStore


@dataclass
class Clock:
    value: float = 1767268800.0

    def __call__(self):
        return self.value


class FakeClient:
    def __init__(self, factory, cfg, options):
        self.factory = factory
        self.cfg = cfg
        self.options = options
        self.resumed = None
        self.prompts = []
        self.disconnected = False
        self.process_group_id = None

    async def connect(self, resume_thread_id=None):
        self.resumed = resume_thread_id
        if self.process_group_id is not None:
            self.options['process_started'](self.process_group_id)
        if self.factory.connect_error:
            raise self.factory.connect_error
        return resume_thread_id or f"dedicated-job-thread-{len(self.factory.clients)}"

    async def run(self, text):
        self.prompts.append(text)
        self.factory.started.set()
        if self.factory.gate:
            await self.factory.gate.wait()
        if self.factory.run_error:
            raise self.factory.run_error
        return "The scheduled report is ready."

    async def disconnect(self):
        if self.factory.disconnect_error:
            raise self.factory.disconnect_error
        self.disconnected = True


class ClientFactory:
    def __init__(self):
        self.clients = []
        self.connect_error = None
        self.run_error = None
        self.disconnect_error = None
        self.gate = None
        self.started = asyncio.Event()

    def __call__(self, cfg, **options):
        client = FakeClient(self, cfg, options)
        self.clients.append(client)
        return client


@pytest.fixture
def environment(tmp_path, monkeypatch):
    monkeypatch.setenv("INKBOX_CODEX_HOME", str(tmp_path / "state"))
    cfg = BridgeConfig(identity="test-agent", project_dir=str(tmp_path), codex_turn_timeout_s=5)
    with ScheduleStore(tmp_path / "jobs.sqlite3") as store:
        yield cfg, store, Clock()


def add_job(store, clock, **changes):
    return store.create(
        name="Status report",
        prompt="Read the project notes and write a brief status report.",
        schedule={"kind": "interval", "seconds": 60},
        now=clock(),
        **changes,
    )


async def settle(scheduler):
    await scheduler.tick()
    if scheduler.active is not None:
        await asyncio.wait_for(asyncio.shield(scheduler.active), timeout=3)


def test_worker_saves_result_and_resumes_job_session_next_time(environment):
    async def scenario():
        cfg, store, clock = environment
        job = add_job(store, clock)
        clients = ClientFactory()
        scheduler = Scheduler(cfg, store=store, client_factory=clients, clock=clock)
        try:
            clock.value += 60
            await settle(scheduler)
            first = store.runs(job["id"])[0]
            assert first["status"] == "succeeded"
            assert first["result"] == "The scheduled report is ready."
            assert clients.clients[0].resumed is None
            assert clients.clients[0].disconnected
            assert job["prompt"] in clients.clients[0].prompts[0]
            thread = store.get(job["id"])["thread_id"]
            assert thread
            clock.value += 60
            await settle(scheduler)
            assert clients.clients[1].resumed == thread
            assert clients.clients[1].disconnected
            assert len(store.runs(job["id"])) == 2
        finally:
            await scheduler.close()

    asyncio.run(scenario())


def test_connection_failure_is_failed_without_submitting_prompt(environment):
    async def scenario():
        cfg, store, clock = environment
        job = add_job(store, clock)
        clients = ClientFactory()
        clients.connect_error = RuntimeError("Local agent is unavailable")
        scheduler = Scheduler(cfg, store=store, client_factory=clients, clock=clock)
        try:
            clock.value += 60
            await settle(scheduler)
            run = store.runs(job["id"])[0]
            assert run["status"] == "failed"
            assert clients.clients[0].prompts == []
            assert clients.clients[0].disconnected
            assert store.get(job["id"])["thread_id"] is None
        finally:
            await scheduler.close()

    asyncio.run(scenario())


def test_failure_after_submission_is_uncertain_and_never_auto_retried(environment):
    async def scenario():
        cfg, store, clock = environment
        job = add_job(store, clock, catch_up=True)
        clients = ClientFactory()
        clients.run_error = RuntimeError("Connection interrupted after submission")
        scheduler = Scheduler(cfg, store=store, client_factory=clients, clock=clock)
        try:
            clock.value += 60
            await settle(scheduler)
            assert store.runs(job["id"])[0]["status"] == "uncertain"
            clock.value += 600
            await settle(scheduler)
            assert len(clients.clients) == 1
            assert clients.clients[0].disconnected
        finally:
            await scheduler.close()

    asyncio.run(scenario())


def test_turn_timeout_is_uncertain_and_disconnects_client(environment):
    async def scenario():
        cfg, store, clock = environment
        cfg.codex_turn_timeout_s = 0.01
        job = add_job(store, clock)
        clients = ClientFactory()
        clients.gate = asyncio.Event()
        scheduler = Scheduler(cfg, store=store, client_factory=clients, clock=clock)
        try:
            clock.value += 60
            await settle(scheduler)
            assert store.runs(job["id"])[0]["status"] == "uncertain"
            assert clients.clients[0].disconnected
        finally:
            await scheduler.close()

    asyncio.run(scenario())


def test_scheduled_process_has_no_inherited_conversation_or_delivery_target(environment, monkeypatch):
    async def scenario():
        cfg, store, clock = environment
        job = add_job(store, clock, owner="source-conversation")
        monkeypatch.setenv("INKBOX_CODEX_CHAT_ID", "unrelated-conversation")
        monkeypatch.setenv("INKBOX_CODEX_CHAT_MODE", "sms")
        monkeypatch.setenv("INKBOX_CODEX_TURN_CONTEXT", "unrelated-turn")
        monkeypatch.setenv("INKBOX_SCHEDULE_JOB_ID", "stale-job-id")
        clients = ClientFactory()
        scheduler = Scheduler(cfg, store=store, client_factory=clients, clock=clock)
        try:
            clock.value += 60
            await settle(scheduler)
            options = clients.clients[0].options
            process_env = options["process_env"]
            assert not any(key.startswith(("INKBOX_CODEX_CHAT_", "INKBOX_CODEX_TURN_")) for key in process_env)
            assert process_env["INKBOX_SCHEDULE_JOB_ID"] == job["id"]
            assert process_env["INKBOX_SCHEDULE_RUN_ID"] == store.runs(job["id"])[0]["id"]
            assert options["mcp_server_config"]["env"]["INKBOX_CODEX_CHAT_ID"] == ""
            assert options["mcp_server_config"]["env"]["INKBOX_SCHEDULE_JOB_ID"] == job["id"]
            assert options["mcp_server_config"]["env"]["INKBOX_SCHEDULE_RUN_ID"] == store.runs(job["id"])[0]["id"]
            assert options["isolate_process_group"] is True
            assert clients.clients[0].cfg.codex_sandbox == cfg.codex_sandbox
            assert clients.clients[0].cfg.codex_approval_policy == cfg.codex_approval_policy
        finally:
            await scheduler.close()

    asyncio.run(scenario())


def test_unconfirmed_process_cleanup_prevents_next_execution(environment, monkeypatch):
    async def scenario():
        cfg, store, clock = environment
        job = add_job(store, clock, catch_up=True)
        clients = ClientFactory()
        clients.disconnect_error = RuntimeError("Process cleanup is unavailable")
        monkeypatch.setattr(scheduler_module, "process_stamp", lambda pid: "same-process" if pid else None)
        monkeypatch.setattr(scheduler_module, "fence_host", lambda run: False)

        def factory(config, **options):
            client = clients(config, **options)
            client.process_group_id = 123456
            return client

        scheduler = Scheduler(cfg, store=store, client_factory=factory, clock=clock)
        try:
            clock.value += 60
            await settle(scheduler)
            assert store.runs(job["id"])[0]["host_pid"] == 123456
            clock.value += 60
            await settle(scheduler)
            assert len(clients.clients) == 1
        finally:
            await scheduler.close()

    asyncio.run(scenario())


def test_failed_handshake_retains_host_until_cleanup_is_confirmed(environment, monkeypatch):
    async def scenario():
        cfg, store, clock = environment
        job = add_job(store, clock)
        clients = ClientFactory()
        clients.connect_error = RuntimeError('Initialization failed')
        clients.disconnect_error = RuntimeError('Cleanup is unavailable')
        monkeypatch.setattr(scheduler_module, 'process_stamp', lambda pid: 'same-process')
        monkeypatch.setattr(scheduler_module, 'fence_host', lambda run: False)

        def factory(config, **options):
            client = clients(config, **options)
            client.process_group_id = 123456
            return client

        scheduler = Scheduler(cfg, store=store, client_factory=factory, clock=clock)
        try:
            clock.value += 60
            await settle(scheduler)
            run = store.runs(job['id'])[0]
            assert run['status'] == 'failed'
            assert run['thread_id'] is None
            assert run['host_pid'] == 123456
            assert run['host_start'] == 'same-process'
            assert not clients.clients[0].prompts
            with pytest.raises(ValueError, match='stop the previous execution'):
                store.recover(run['id'], action='retry', acknowledge_duplicate_risk=True, now=clock())
            clock.value += 60
            await settle(scheduler)
            assert len(clients.clients) == 1
        finally:
            await scheduler.close()

    asyncio.run(scenario())


@pytest.mark.parametrize('persistent_failure', [False, True])
def test_failed_host_receipt_write_is_retried_without_starting_more_work(
    environment, monkeypatch, persistent_failure,
):
    async def scenario():
        cfg, store, clock = environment
        job = add_job(store, clock)
        clients = ClientFactory()
        clients.disconnect_error = RuntimeError('Cleanup is unavailable')
        monkeypatch.setattr(scheduler_module, 'process_stamp', lambda pid: 'same-process')
        monkeypatch.setattr(scheduler_module, 'fence_host', lambda run: False)
        record_host = store.record_host
        writes = []

        def busy_receipt(*args):
            writes.append(args)
            if persistent_failure or len(writes) == 1:
                import sqlite3
                raise sqlite3.OperationalError('database is locked')
            return record_host(*args)

        monkeypatch.setattr(store, 'record_host', busy_receipt)

        def factory(config, **options):
            client = clients(config, **options)
            client.process_group_id = 123456
            return client

        scheduler = Scheduler(cfg, store=store, client_factory=factory, clock=clock)
        try:
            clock.value += 60
            await settle(scheduler)
            assert len(writes) == 2
            run = store.runs(job['id'])[0]
            assert not clients.clients[0].prompts
            if persistent_failure:
                assert run['status'] == 'starting'
                with pytest.raises(Exception, match='database is locked'):
                    await scheduler.tick()
                assert len(clients.clients) == 1
                monkeypatch.setattr(store, 'record_host', record_host)
                await scheduler.tick()
                run = store.runs(job['id'])[0]
            assert run['status'] == 'failed'
            assert run['host_pid'] == 123456
            assert not scheduler._unrecorded_hosts
            clock.value += 60
            await settle(scheduler)
            assert len(clients.clients) == 1
        finally:
            await scheduler.close()

    asyncio.run(scenario())


def test_process_fence_never_signals_a_reused_pid(monkeypatch):
    signalled = []
    monkeypatch.setattr(scheduler_module, "process_stamp", lambda pid: "different-process")
    monkeypatch.setattr(scheduler_module.os, "killpg", lambda *args: signalled.append(args))
    assert scheduler_module.fence_host({"host_pid": 123456, "host_start": "original-process"})
    assert signalled == []


def test_close_retries_unrecorded_host_ownership_before_releasing_worker(environment, monkeypatch):
    async def scenario():
        cfg, store, clock = environment
        job = add_job(store, clock)
        clients = ClientFactory()
        clients.disconnect_error = RuntimeError('Cleanup is unavailable')
        record_host = store.record_host
        monkeypatch.setattr(scheduler_module, 'process_stamp', lambda pid: 'same-process')
        monkeypatch.setattr(scheduler_module, 'fence_host', lambda run: False)

        def busy_receipt(*args):
            import sqlite3
            raise sqlite3.OperationalError('database is locked')

        monkeypatch.setattr(store, 'record_host', busy_receipt)

        def factory(config, **options):
            client = clients(config, **options)
            client.process_group_id = 123456
            return client

        scheduler = Scheduler(cfg, store=store, client_factory=factory, clock=clock)
        clock.value += 60
        await settle(scheduler)
        assert scheduler._unrecorded_hosts
        monkeypatch.setattr(store, 'record_host', record_host)
        await scheduler.close()
        assert not scheduler._unrecorded_hosts
        run = store.runs(job['id'])[0]
        assert run['host_pid'] == 123456
        assert run['status'] == 'failed'

    asyncio.run(scenario())


@pytest.mark.parametrize("original_boot,cleared", [("old-boot", True), ("current-boot", True)])
def test_process_fence_distinguishes_reboot_from_same_boot_pid_reuse(monkeypatch, original_boot, cleared):
    signalled = []
    read_text = scheduler_module.Path.read_text

    def read_boot(path, *args, **kwargs):
        if str(path) == "/proc/sys/kernel/random/boot_id":
            return "current-boot\n"
        return read_text(path, *args, **kwargs)

    monkeypatch.setattr(scheduler_module.Path, "read_text", read_boot)
    monkeypatch.setattr(scheduler_module, "process_stamp", lambda pid: "current-boot:999")
    monkeypatch.setattr(scheduler_module.os, "killpg", lambda *args: signalled.append(args))
    assert scheduler_module.fence_host({"host_pid": 123456, "host_start": f"{original_boot}:111"}) is cleared
    assert signalled == []


@pytest.mark.parametrize('recorded_boot', ['old-boot', 'current-boot'])
def test_restart_clears_replaced_host_but_requires_explicit_retry(environment, monkeypatch, recorded_boot):
    async def scenario():
        cfg, store, clock = environment
        job = add_job(store, clock, catch_up=True)
        clock.value += 60
        run = store.claim(now=clock())
        store.mark_running(run["id"], "before-reboot-thread", host_pid=123456, host_start=f"{recorded_boot}:111")
        signalled = []
        read_text = scheduler_module.Path.read_text

        def read_boot(path, *args, **kwargs):
            if str(path) == "/proc/sys/kernel/random/boot_id":
                return "current-boot\n"
            return read_text(path, *args, **kwargs)

        monkeypatch.setattr(scheduler_module.Path, "read_text", read_boot)
        monkeypatch.setattr(scheduler_module, "process_stamp", lambda pid: "current-boot:999")
        monkeypatch.setattr(scheduler_module.os, "killpg", lambda *args: signalled.append(args))
        clients = ClientFactory()
        scheduler = Scheduler(cfg, store=store, client_factory=clients, clock=clock)
        try:
            clock.value += 300
            await scheduler.start()
            await scheduler.tick()
            previous = next(item for item in store.runs(job["id"]) if item["id"] == run["id"])
            assert previous["status"] == "uncertain"
            assert previous["host_pid"] is None
            assert clients.clients == []
            assert signalled == []
            retry = store.recover(run["id"], action="retry", acknowledge_duplicate_risk=True, now=clock())
            assert retry["status"] == "pending"
        finally:
            await scheduler.close()

    asyncio.run(scenario())


def test_disable_prevents_new_submissions_until_enabled(environment):
    async def scenario():
        cfg, store, clock = environment
        add_job(store, clock, catch_up=True)
        clients = ClientFactory()
        scheduler = Scheduler(cfg, store=store, client_factory=clients, clock=clock)
        try:
            store.set_enabled(False)
            clock.value += 300
            await settle(scheduler)
            assert clients.clients == []
            store.set_enabled(True)
            await settle(scheduler)
            assert len(clients.clients) == 1
            assert store.runs()[0]["status"] == "succeeded"
            assert store.runs()[0]["count"] == 5
        finally:
            await scheduler.close()

    asyncio.run(scenario())


def test_slow_run_does_not_overlap_and_gateway_close_marks_uncertain(environment):
    async def scenario():
        cfg, store, clock = environment
        job = add_job(store, clock, catch_up=True)
        clients = ClientFactory()
        clients.gate = asyncio.Event()
        scheduler = Scheduler(cfg, store=store, client_factory=clients, clock=clock)
        clock.value += 60
        await scheduler.tick()
        await asyncio.wait_for(clients.started.wait(), timeout=3)
        clock.value += 300
        await scheduler.tick()
        assert len(clients.clients) == 1
        await scheduler.close()
        assert clients.clients[0].disconnected
        previous = [run for run in store.runs(job["id"]) if run["status"] == "uncertain"]
        assert len(previous) == 1
        assert store.claim(now=clock()) is None

    asyncio.run(scenario())


def test_two_jobs_receive_different_sessions(environment):
    async def scenario():
        cfg, store, clock = environment
        one = add_job(store, clock)
        two = add_job(store, clock)
        clients = ClientFactory()
        scheduler = Scheduler(cfg, store=store, client_factory=clients, clock=clock)
        try:
            clock.value += 60
            await settle(scheduler)
            await settle(scheduler)
            assert len(clients.clients) == 2
            assert store.get(one["id"])["thread_id"] != store.get(two["id"])["thread_id"]
            assert all(client.resumed is None for client in clients.clients)
        finally:
            await scheduler.close()

    asyncio.run(scenario())


def test_second_worker_cannot_recover_or_run_first_workers_inflight_work(environment):
    async def scenario():
        cfg, store, clock = environment
        job = add_job(store, clock)
        clients = ClientFactory()
        clients.gate = asyncio.Event()
        first = Scheduler(cfg, store=store, client_factory=clients, clock=clock)
        second = Scheduler(cfg, store=store, client_factory=ClientFactory(), clock=clock)
        try:
            await first.start()
            clock.value += 60
            await first.tick()
            await asyncio.wait_for(clients.started.wait(), timeout=3)
            with pytest.raises((RuntimeError, OSError)):
                await second.start()
            assert store.runs(job["id"])[0]["status"] == "running"
        finally:
            await second.close()
            await first.close()

    asyncio.run(scenario())


def test_busy_online_worker_keeps_other_jobs_queued_beyond_grace(environment):
    async def scenario():
        cfg, store, clock = environment
        first_job = add_job(store, clock)
        second_job = add_job(store, clock)
        clients = ClientFactory()
        clients.gate = asyncio.Event()
        scheduler = Scheduler(cfg, store=store, client_factory=clients, clock=clock)
        try:
            clock.value += 60
            await scheduler.tick()
            await asyncio.wait_for(clients.started.wait(), timeout=2)
            # Regular heartbeats prove the worker is busy, not down.
            for _ in range(4):
                clock.value += 10
                await scheduler.tick()
            assert len(clients.clients) == 1
            assert sorted(run['status'] for run in store.runs()) == ['pending', 'running']
            clients.gate.set()
            await asyncio.wait_for(scheduler.active, timeout=2)
            await settle(scheduler)
            assert len(clients.clients) == 2
            assert store.runs(first_job['id'])[0]['status'] == 'succeeded'
            assert store.runs(second_job['id'])[0]['status'] == 'succeeded'
            assert store.status()['missed_pending'] == 0
        finally:
            clients.gate.set()
            await scheduler.close()

    asyncio.run(scenario())


def test_pending_scheduled_run_expires_after_worker_outage(environment):
    _, store, clock = environment
    job = add_job(store, clock)
    clock.value += 60
    store.materialize(clock())
    store.heartbeat(now=clock())
    clock.value += 40
    assert store.claim(clock()) is None
    assert store.runs(job['id'])[0]['status'] == 'missed'


def test_global_disable_retains_queued_schedule_as_missed_despite_fresh_heartbeat(environment):
    _, store, clock = environment
    job = add_job(store, clock)
    clock.value += 60
    store.materialize(clock())
    store.set_enabled(False)
    clock.value += 40
    store.heartbeat(now=clock())
    store.set_enabled(True)
    assert store.claim(clock()) is None
    assert store.status()['missed_pending'] == 1
    store.update(job['id'], catch_up=True, now=clock())
    run = store.claim(clock())
    assert run['source'] == 'auto_catch_up' and run['count'] == 1


def test_unknown_process_stamp_never_signals_or_clears_a_live_pid(monkeypatch):
    signalled = []
    monkeypatch.setattr(scheduler_module, 'process_stamp', lambda pid: 'current-process')
    monkeypatch.setattr(scheduler_module.os, 'killpg', lambda *args: signalled.append(args))
    assert not scheduler_module.fence_host({'host_pid': 123456, 'host_start': None})
    assert signalled == []


def test_invalidated_schedule_is_paused_without_stalling_other_jobs(environment, monkeypatch):
    from inkbox_codex import schedules
    _, store, clock = environment
    broken = store.create('Cron task', 'Write report',
                          {'kind': 'cron', 'expression': '* * * * *', 'timezone': 'UTC'}, now=clock())
    healthy = add_job(store, clock)
    real_next = schedules.next_cron

    def next_cron(schedule, after):
        if schedule['kind'] == 'cron':
            raise schedules.ZoneInfoNotFoundError('Timezone no longer available')
        return real_next(schedule, after)

    monkeypatch.setattr(schedules, 'next_cron', next_cron)
    clock.value += 60
    run = store.claim(clock())
    assert run['job_id'] == healthy['id']
    assert store.get(broken['id'])['enabled'] is False
    failed = store.runs(broken['id'])
    assert len(failed) == 1 and failed[0]['status'] == 'failed'
    assert failed[0]['source'] == 'schedule_error'
    assert 'edit the schedule' in failed[0]['error']
    store.materialize(clock())
    assert len(store.runs(broken['id'])) == 1


@pytest.mark.parametrize('error', [None, RuntimeError('Run interrupted')])
def test_decision_required_worker_pauses_without_successful_decision(environment, error):
    async def scenario():
        cfg, store, clock = environment
        job = add_job(store, clock, decision_required=True)
        clients = ClientFactory()
        clients.run_error = error
        worker = Scheduler(cfg, store=store, client_factory=clients, clock=clock)
        try:
            clock.value += 60
            await settle(worker)
            assert store.get(job['id'])['stop_reason'] == ('decision_incomplete' if error else 'decision_missing')
            assert 'inkbox_schedule_decide' in clients.clients[0].options['developer_instructions']
            clock.value += 60
            await settle(worker)
            assert len(clients.clients) == 1
        finally:
            await worker.close()
    asyncio.run(scenario())
