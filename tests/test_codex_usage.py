from datetime import datetime, timezone

from inkbox_codex import codex_usage


def test_format_usage_matches_codex_shape():
    now = datetime(2026, 6, 16, 12, 0, tzinfo=timezone.utc)
    data = {
        "rate_limits": {
            "rateLimits": {
                "limitName": "Codex",
                "primary": {
                    "usedPercent": 42,
                    "resetsAt": int(datetime(2026, 6, 16, 14, 30, tzinfo=timezone.utc).timestamp() * 1000),
                    "windowDurationMins": 300,
                },
                "secondary": {
                    "usedPercent": 10,
                    "resetsAt": int(datetime(2026, 6, 19, 12, 0, tzinfo=timezone.utc).timestamp() * 1000),
                    "windowDurationMins": 7 * 24 * 60,
                },
            }
        },
        "usage": {"summary": {"lifetimeTokens": 1234567, "currentStreakDays": 4}},
    }
    out = codex_usage.format_usage(data, now=now)
    assert "Codex (5h): 42% used, resets in 2h 30m" in out
    assert "Secondary (7d): 10% used, resets in 3d 0h" in out
    assert "Lifetime tokens: 1,234,567" in out
    assert "Current streak: 4d" in out


def test_format_usage_skips_missing_windows():
    out = codex_usage.format_usage({
        "rate_limits": {"rateLimits": {"primary": {"usedPercent": 50}}},
    })
    assert out == "Codex: 50% used"  # no reset suffix when unknown


def test_format_usage_empty_payload():
    assert codex_usage.format_usage({}) == "No Codex usage windows reported."


def test_usage_report_handles_fetch_error(monkeypatch):
    monkeypatch.setattr(codex_usage, "fetch_usage", lambda: (_ for _ in ()).throw(RuntimeError("no auth")))
    msg = codex_usage.usage_report()
    assert "Couldn't fetch Codex usage" in msg
