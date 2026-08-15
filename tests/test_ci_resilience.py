from pathlib import Path

from tests.ci.hosted_marker import WORDS, marker_for
from tests.ci.summarize_live_failure import summarize

ROOT = Path(__file__).resolve().parents[1]


def test_hosted_marker_is_deterministic_distinct_and_uses_full_run_token():
    marker = marker_for("12345678901234567890", "2")
    assert marker_for("12345678901234567890", "2") == marker
    assert len(marker.split()) == 3
    assert len(set(marker.split())) == 3
    assert set(marker.split()) <= set(WORDS)
    assert marker_for("12345678901234567890", "2") != marker_for(
        "12345678901234567890", "3"
    )
    assert marker_for("112345678901234567890", "2") != marker


def test_hosted_marker_has_large_observed_space():
    markers = {marker_for(str(run_id), "1") for run_id in range(1_000)}
    assert len(markers) >= 900


def test_every_host_workflow_uses_bounded_codex_installer():
    for name in (
        "canary.yml",
        "tests.yml",
        "live-a2a.yml",
        "live-channels.yml",
        "live-external-events.yml",
        "live-voice.yml",
    ):
        workflow = ROOT.joinpath(".github", "workflows", name).read_text()
        assert 'bash "$GITHUB_WORKSPACE/tests/ci/install_codex.sh"' in workflow
        assert "npm install -g @openai/codex@alpha" not in workflow

    installer = ROOT.joinpath("tests", "ci", "install_codex.sh").read_text()
    assert "CODEX_INSTALL_ATTEMPTS:-4" in installer
    assert "attempt * 15" in installer
    assert "npm install -g @openai/codex@alpha && codex --version" in installer


def test_live_runs_never_cancel_an_existing_shared_cycle():
    workflow = ROOT.joinpath(".github", "workflows", "live-stack.yml").read_text()
    assert "cancel-in-progress: false" in workflow


def test_public_workflows_do_not_publish_raw_live_logs():
    for name in (
        "live-a2a.yml",
        "live-channels.yml",
        "live-external-events.yml",
        "live-voice.yml",
    ):
        workflow = ROOT.joinpath(".github", "workflows", name).read_text()
        assert "summarize_live_failure.py" in workflow
        assert "actions/upload-artifact" not in workflow
        assert 'cat "$GATEWAY_LOG"' not in workflow
        assert 'cat "$DRIVER_LOG"' not in workflow
        assert 'cat "$DRIVER_STATE"' not in workflow
        assert 'tail -n 300 "$RUNNER_TEMP/gateway.log"' not in workflow


def test_failure_summary_reports_only_counts_and_state(tmp_path):
    sensitive = "DO_NOT_PRINT_SENTINEL"
    path = tmp_path / "gateway.log"
    path.write_text(f"tunnel ready\nERROR {sensitive}\n")

    result = summarize(path, "gateway")

    assert result["tunnel_ready"] is True
    assert result["error_lines"] == 1
    assert sensitive not in repr(result)
