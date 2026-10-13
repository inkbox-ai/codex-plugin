import json
import os
from pathlib import Path
import subprocess
import sys

import pytest


INSTALLER = Path(__file__).parent / "ci" / "install_codex.sh"
VERSION = "0.1.0-alpha.2"


@pytest.fixture
def install_codex(tmp_path):
    commands = {
        "node": "#!/bin/sh\nprintf linux-x64\n",
        "sleep": '#!/bin/sh\nprintf "%s\\n" "$1" >> "$SLEEP_LOG"\n',
        "codex": f'#!/bin/sh\necho "${{CLI_VERSION:-codex-cli {VERSION}}}"\n',
        "npm": f"""#!{sys.executable}
import json
import os
from pathlib import Path
import sys
import time

args = sys.argv[1:]
log = Path(os.environ["INSTALL_LOG"])
with log.open("a") as f:
    f.write(json.dumps(args) + "\\n")
calls = [json.loads(line) for line in log.read_text().splitlines()]
mode = os.environ["INSTALL_MODE"]
if mode == "hang":
    time.sleep(30)
if args[0] == "view":
    if args[1] == "@openai/codex@alpha":
        count = sum(call[1] == args[1] for call in calls)
        if mode == "metadata_failure" and count == 1:
            sys.exit(1)
        if mode == "stable_tag":
            print("0.1.0")
        elif mode == "moving_tag" and count == 1:
            print("0.1.0-alpha.1")
        else:
            print("{VERSION}")
    else:
        count = sum(call[0] == "view" and call[1] != "@openai/codex@alpha" for call in calls)
        if mode == "missing_native" or (mode == "late_native" and count <= 5):
            sys.exit(1)
        if mode == "moving_tag" and count == 1:
            sys.exit(1)
        if mode == "wrong_native":
            print("0.1.0-alpha.1-linux-x64")
        elif mode != "empty_native":
            print(args[1].split("@")[-1])
elif args[0] == "install":
    if mode == "install_failure" and sum(call[0] == "install" for call in calls) == 1:
        sys.exit(1)
else:
    sys.exit(1)
""",
    }
    for name, content in commands.items():
        path = tmp_path / name
        path.write_text(content)
        path.chmod(0o755)

    def run(mode="success", **overrides):
        log = tmp_path / "install.log"
        sleep_log = tmp_path / "sleep.log"
        env = {key: value for key, value in os.environ.items()
               if not key.startswith("CODEX_INSTALL_")}
        result = subprocess.run(
            ["bash", str(INSTALLER)],
            env={**env, "PATH": str(tmp_path) + ":" + os.environ["PATH"],
                 "INSTALL_LOG": str(log), "SLEEP_LOG": str(sleep_log),
                 "INSTALL_MODE": mode, **overrides},
            capture_output=True, text=True, timeout=5,
        )
        calls = [json.loads(line) for line in log.read_text().splitlines()] if log.exists() else []
        delays = [int(line) for line in sleep_log.read_text().splitlines()] if sleep_log.exists() else []
        return result, calls, delays

    return run


def _assert_matching_install(calls):
    installs = [call for call in calls if call[0] == "install"]
    assert installs
    for call in installs:
        assert f"@openai/codex@{VERSION}" in call
        assert f"@openai/codex-linux-x64@npm:@openai/codex@{VERSION}-linux-x64" in call


def test_waits_for_native_publication_beyond_four_attempts(install_codex):
    result, calls, delays = install_codex("late_native")
    assert result.returncode == 0, result.stderr
    assert delays == [15, 30, 45, 60, 60]
    assert len([call for call in calls if call[0] == "install"]) == 1
    _assert_matching_install(calls)
    assert f"codex-cli {VERSION}" in result.stdout


@pytest.mark.parametrize("mode", ["metadata_failure", "install_failure", "moving_tag"])
def test_retries_transient_failures_and_reresolves_current_alpha(install_codex, mode):
    result, calls, delays = install_codex(mode)
    assert result.returncode == 0, result.stderr
    assert delays == [15]
    _assert_matching_install(calls)


@pytest.mark.parametrize("mode", ["missing_native", "empty_native", "wrong_native", "stable_tag"])
def test_missing_or_invalid_artifacts_never_install_or_fall_back(install_codex, mode):
    result, calls, delays = install_codex(mode, CODEX_INSTALL_ATTEMPTS="3")
    assert result.returncode == 1
    assert "failed after 3 attempts" in result.stderr
    assert delays == [15, 30]
    assert not any(call[0] == "install" for call in calls)


def test_stale_executable_is_not_a_successful_install(install_codex):
    result, calls, delays = install_codex(
        CLI_VERSION="codex-cli 0.1.0-alpha.1", CODEX_INSTALL_ATTEMPTS="2",
    )
    assert result.returncode == 1
    assert "executable version does not match" in result.stderr
    assert len([call for call in calls if call[0] == "install"]) == 2
    assert delays == [15]


def test_total_budget_stops_a_hung_registry_request(install_codex):
    result, calls, delays = install_codex("hang", CODEX_INSTALL_TIMEOUT_SECONDS="2")
    assert result.returncode == 1
    assert len(calls) == 1
    assert not delays
    assert "budget 2s" in result.stderr


@pytest.mark.parametrize("overrides", [
    {"CODEX_INSTALL_ATTEMPTS": "0"},
    {"CODEX_INSTALL_ATTEMPTS": "not-a-number"},
    {"CODEX_INSTALL_TIMEOUT_SECONDS": "0"},
])
def test_invalid_limits_fail_before_registry_requests(install_codex, overrides):
    result, calls, _ = install_codex(**overrides)
    assert result.returncode == 2
    assert not calls
