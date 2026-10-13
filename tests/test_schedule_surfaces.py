"""Scheduled-job CLI/MCP contracts using real persistent state, without network calls."""

import asyncio
import json
import sqlite3

import pytest

from inkbox_codex import cli, schedule_tools, tools
from inkbox_codex.config import BridgeConfig, read_config
from inkbox_codex.mcp_stdio import InkboxMcpServer
from inkbox_codex.prompts import build_channel_prompt
from inkbox_codex.schedules import ScheduleStore, schedule_path


@pytest.fixture(autouse=True)
def local_state(monkeypatch, tmp_path):
    monkeypatch.setenv("INKBOX_CODEX_HOME", str(tmp_path / "state"))
    monkeypatch.setenv("CODEX_PROJECT_DIR", str(tmp_path / "project"))
    monkeypatch.setenv("INKBOX_IDENTITY", "schedule-agent")
    monkeypatch.setenv("INKBOX_BASE_URL", "https://api.example.com")
    monkeypatch.delenv("INKBOX_API_KEY", raising=False)
    monkeypatch.delenv("INKBOX_CODEX_CHAT_ID", raising=False)
    monkeypatch.delenv("INKBOX_SCHEDULE_JOB_ID", raising=False)
    monkeypatch.setattr(cli.daemon, "_maybe_load_env_file", lambda: None)


def call(tool_name, **args):
    response = asyncio.run(tools.call_inkbox_tool(None, "schedule-agent", "inkbox_schedule_" + tool_name, args))
    return json.loads(response["content"][0]["text"]), response.get("isError", False)


def add(**kwargs):
    result, error = call("add", name="Daily review", prompt="Inspect project status and save a summary.",
                         schedule={"kind": "interval", "seconds": 3600}, **kwargs)
    assert not error, result
    return result


def run_cli(capsys, *args):
    status = cli.main(["schedule", *args])
    return status, json.loads(capsys.readouterr().out)


def test_cli_persisted_crud_prompt_file_and_global_switch(tmp_path, capsys):
    prompt = tmp_path / "task.txt"
    prompt.write_text("Review project status.\nSave the result.", encoding="utf-8")
    code, job = run_cli(capsys, "add", "--name", "Review", "--every", "60", "--prompt-file", str(prompt))
    assert code == 0
    assert job["prompt"] == prompt.read_text()
    assert job["catch_up"] is False
    code, jobs = run_cli(capsys, "list")
    assert code == 0 and [item["id"] for item in jobs] == [job["id"]]
    code, edited = run_cli(capsys, "update", job["id"], "--catch-up", "--prompt", "Review tests.")
    assert code == 0 and edited["catch_up"] is True and edited["prompt"] == "Review tests."
    assert run_cli(capsys, "pause", job["id"])[1]["enabled"] is False
    assert run_cli(capsys, "resume", job["id"])[1]["enabled"] is True
    assert run_cli(capsys, "disable")[1]["enabled"] is False
    assert run_cli(capsys, "status")[1]["enabled"] is False
    assert run_cli(capsys, "enable")[1]["enabled"] is True
    assert run_cli(capsys, "show", job["id"])[1]["id"] == job["id"]
    assert run_cli(capsys, "delete", job["id"])[1]["deleted"] is True
    assert run_cli(capsys, "list")[1] == []


def test_cli_cron_and_timestamp_validation(capsys):
    code, job = run_cli(capsys, "add", "--name", "Review", "--cron", "0 9 * * 1-5", "--timezone", "America/New_York", "--prompt", "Review.")
    assert code == 0 and job["schedule"]["timezone"] == "America/New_York"
    code, error = run_cli(capsys, "add", "--name", "Review", "--once", "2035-01-01T09:00:00", "--prompt", "Review.")
    assert code == 1 and "offset" in error["error"]
    code, error = run_cli(capsys, "update", job["id"], "--timezone", "UTC")
    assert code == 1 and "--cron" in error["error"]
    code, error = run_cli(capsys, "update", job["id"])
    assert code == 1 and "field" in error["error"]


def test_cli_manual_run_and_history(capsys):
    job = add()
    code, run = run_cli(capsys, "run-now", job["id"])
    assert code == 0 and run["status"] == "pending"
    assert run_cli(capsys, "runs", job["id"], "--limit", "1")[1][0]["id"] == run["id"]
    code, error = run_cli(capsys, "run-now", job["id"])
    assert code == 1 and "active" in error["error"]


def test_tools_scope_job_prompts_and_runs_to_conversation(monkeypatch):
    monkeypatch.setenv("INKBOX_CODEX_CHAT_ID", "contact:one")
    job = add()
    run, error = call("run_now", job_id=job["id"])
    assert not error and run["status"] == "pending"
    monkeypatch.setenv("INKBOX_CODEX_CHAT_ID", "contact:two")
    assert call("list") == ([], False)
    assert call("history") == ([], False)
    for name, args in (("update", {"enabled": False}), ("delete", {}), ("run_now", {}), ("catch_up", {}), ("history", {})):
        data, error = call(name, job_id=job["id"], **args)
        assert error and data["error"] == "Schedule not found"
    monkeypatch.delenv("INKBOX_CODEX_CHAT_ID")
    assert call("list")[0][0]["id"] == job["id"]
    assert call("history")[0][0]["id"] == run["id"]


def test_mcp_local_operations_do_not_construct_sdk(monkeypatch):
    server = InkboxMcpServer()
    def unexpected():
        pytest.fail("Local schedule CRUD must not construct a network SDK client")
    monkeypatch.setattr(server, "_inkbox", unexpected)
    result = asyncio.run(server.handle({"jsonrpc": "2.0", "id": 5, "method": "tools/call", "params": {"name": "inkbox_schedule_add", "arguments": {"name": "Review", "prompt": "Inspect status.", "schedule": {"kind": "interval", "seconds": 60}}}}))
    assert result["id"] == 5
    assert not result["result"].get("isError")
    assert json.loads(result["result"]["content"][0]["text"])["name"] == "Review"


@pytest.mark.parametrize("tool,args", [("add", {"name": "No", "prompt": "No", "schedule": {"kind": "interval", "seconds": 60}}), ("update", {"job_id": "id", "enabled": False}), ("delete", {"job_id": "id"}), ("run_now", {"job_id": "id"}), ("catch_up", {"job_id": "id"})])
def test_scheduled_jobs_cannot_mutate_schedules(monkeypatch, tool, args):
    monkeypatch.setenv("INKBOX_SCHEDULE_JOB_ID", "scheduled-run")
    result, error = call(tool, **args)
    assert error and "cannot create or change schedules" in result["error"]


def test_cli_does_not_bypass_scheduled_job_restriction(monkeypatch, capsys):
    monkeypatch.setenv("INKBOX_SCHEDULE_JOB_ID", "scheduled-run")
    assert run_cli(capsys, "enable")[0] == 1
    assert run_cli(capsys, "add", "--name", "No", "--prompt", "No", "--every", "60")[0] == 1


def test_chat_cannot_use_admin_commands(monkeypatch, capsys):
    monkeypatch.setenv("INKBOX_CODEX_CHAT_ID", "contact:one")
    for args in (("enable",), ("disable",), ("status",), ("recover", "run-id", "--action", "dismiss")):
        code, result = run_cli(capsys, *args)
        assert code == 1 and "local administrator" in result["error"]


def test_mcp_validates_arguments_and_hides_storage_errors(monkeypatch):
    for args in ({"owner": "contact:other"}, {"job_id": {"invalid": "value"}}, {"limit": 201}, {"limit": True}):
        result, error = call("history", **args)
        assert error and "error" in result
    class BrokenStore:
        def __init__(self, path):
            raise sqlite3.OperationalError("private-path and database details")
    monkeypatch.setattr(schedule_tools, "ScheduleStore", BrokenStore)
    result, error = call("list")
    assert error and result == {"error": "Could not access scheduled-job data."}


def test_cron_tools_and_project_scope_are_propagated(tmp_path):
    project = tmp_path / "project"
    cfg = BridgeConfig(project_dir=str(project), identity="schedule-agent")
    server, names = tools.build_inkbox_mcp_server_config(cfg)
    assert server["env"]["CODEX_PROJECT_DIR"] == str(project.resolve())
    expected = {"mcp__inkbox__" + tool["name"] for tool in schedule_tools.SCHEDULE_TOOLS}
    assert expected <= set(names)
    assert {item["name"] for item in schedule_tools.SCHEDULE_TOOLS} <= {item["name"] for item in tools.mcp_tool_list()}


def test_catch_up_and_recovery_through_cli(capsys, monkeypatch):
    monkeypatch.setattr("inkbox_codex.schedules.time.time", lambda: 221.0)
    with ScheduleStore(schedule_path(read_config())) as store:
        job = store.create("Review", "Review status.", {"kind": "interval", "seconds": 60}, now=0)
        store.materialize(now=180)
        # Finish the current occurrence, leaving old missed occurrences available.
        running = store.claim(now=180)
        store.mark_running(running["id"], "dedicated-thread")
        store.finish(running["id"], "succeeded", now=181)
    # Request a manual catch-up between current occurrences; only one pending run.
    code, run = run_cli(capsys, "catch-up", job["id"])
    assert code == 0 and run["status"] == "pending" and run["count"] >= 2
    with ScheduleStore(schedule_path(read_config())) as store:
        claimed = store.claim()
        store.mark_running(claimed["id"], "dedicated-thread")
        store.recover_interrupted()
    code, error = run_cli(capsys, "recover", claimed["id"], "--action", "retry")
    assert code == 1 and "duplicate" in error["error"]
    code, retried = run_cli(capsys, "recover", claimed["id"], "--action", "retry", "--acknowledge-duplicate-risk")
    assert code == 0 and retried["status"] == "pending"
    assert retried["id"] != claimed["id"]


def test_channel_prompt_explains_saved_jobs_and_delivery():
    prompt = build_channel_prompt("/workspace")
    assert "inkbox_schedule_add" in prompt
    assert "Only say a job is\nscheduled after the tool succeeds" in prompt
    assert "IANA timezone" in prompt
    assert "plain job output is saved in history rather than sent" in prompt


def test_scheduled_readers_only_see_their_job(monkeypatch, capsys):
    own = add()
    other = add()
    monkeypatch.setenv("INKBOX_SCHEDULE_JOB_ID", own["id"])
    jobs, error = call("list")
    assert not error and [job["id"] for job in jobs] == [own["id"]]
    assert call("history") == ([], False)
    assert call("history", job_id=other["id"])[1] is True
    assert run_cli(capsys, "list")[1][0]["id"] == own["id"]
    assert run_cli(capsys, "show", other["id"])[0] == 1
    assert run_cli(capsys, "runs", other["id"])[0] == 1
    assert run_cli(capsys, "status")[0] == 1
