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


def test_process_fence_never_signals_a_reused_pid(monkeypatch):
    signalled = []
    monkeypatch.setattr(scheduler_module, "process_stamp", lambda pid: "different-process")
    monkeypatch.setattr(scheduler_module.os, "killpg", lambda *args: signalled.append(args))
    assert not scheduler_module.fence_host({"host_pid": 123456, "host_start": "original-process"})
    assert signalled == []


@pytest.mark.parametrize("original_boot,cleared", [("old-boot", True), ("current-boot", False)])
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


def test_restart_after_reboot_clears_old_host_but_requires_explicit_retry(environment, monkeypatch):
    async def scenario():
        cfg, store, clock = environment
        job = add_job(store, clock, catch_up=True)
        clock.value += 60
        run = store.claim(now=clock())
        store.mark_running(run["id"], "before-reboot-thread", host_pid=123456, host_start="old-boot:111")
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
