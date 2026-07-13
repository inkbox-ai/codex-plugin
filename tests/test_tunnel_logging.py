import logging

from inkbox_codex import gateway


def _record(message, *args, level=logging.WARNING):
    return logging.LogRecord(
        "inkbox.tunnels",
        level,
        __file__,
        1,
        message,
        args,
        None,
    )


def test_expected_intake_idle_cap_warning_is_filtered():
    record = _record(
        "/_system/intake slot=%d -> status=%s reason=%r body=%r",
        10,
        "408",
        "intake-idle-cap",
        b"",
    )

    assert gateway._ExpectedTunnelIdleFilter().filter(record) is False


def test_other_tunnel_warnings_remain_visible():
    filter_ = gateway._ExpectedTunnelIdleFilter()

    # Auth failures on the same intake path must keep surfacing.
    assert filter_.filter(_record(
        "/_system/intake slot=%d -> status=%s reason=%r body=%r",
        10,
        "401",
        "owner-token-invalid",
        b"",
    )) is True
    # Same status but a different server reason: not the expected idle cap.
    assert filter_.filter(_record(
        "/_system/intake slot=%d -> status=%s reason=%r body=%r",
        10,
        "408",
        "handshake-timeout",
        b"",
    )) is True
    assert filter_.filter(_record("tunnel runtime disconnected")) is True


def test_installing_filter_is_idempotent():
    logger = logging.getLogger("inkbox.tunnels")
    original_filters = list(logger.filters)
    try:
        logger.filters = [
            item for item in logger.filters
            if not isinstance(item, gateway._ExpectedTunnelIdleFilter)
        ]

        gateway._install_tunnel_log_filter()
        gateway._install_tunnel_log_filter()

        installed = [
            item for item in logger.filters
            if isinstance(item, gateway._ExpectedTunnelIdleFilter)
        ]
        assert len(installed) == 1
    finally:
        logger.filters = original_filters
