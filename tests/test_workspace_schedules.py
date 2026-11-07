from inkbox_codex.schedules import ScheduleStore


def test_revisioned_creation_edit_delete_and_registration_metadata(tmp_path):
    with ScheduleStore(tmp_path / "jobs.sqlite") as store:
        store.use_external_timer()
        command = {
            "action": "schedule_mutate",
            "command_id": "create",
            "operation": "create",
            "job_id": None,
            "revision": 0,
            "fields": {
                "name": "Daily summary",
                "task": {"kind": "prompt", "prompt": "Summarize messages"},
                "timing": {
                    "kind": "interval",
                    "seconds": 600,
                    "timezone": "America/New_York",
                },
            },
        }
        first = store.mutate_external(command)
        assert store.mutate_external(command) == first
        assert len(store.list_jobs()) == 1
        registration = store.registrations(pending_only=True)[0]
        assert registration["display"]["name"] == "Daily summary"
        assert registration["schedule"]["timezone"] == "America/New_York"
        updated = store.mutate_external(
            {
                "action": "schedule_mutate",
                "command_id": "edit",
                "operation": "update",
                "job_id": first["job_id"],
                "revision": first["revision"],
                "fields": {"name": "New name", "enabled": False},
            }
        )
        assert updated["revision"] > first["revision"]
        removed = store.mutate_external(
            {
                "action": "schedule_mutate",
                "command_id": "delete",
                "operation": "delete",
                "job_id": updated["job_id"],
                "revision": updated["revision"],
                "fields": {},
            }
        )
        assert removed["deleted"]
        assert store.registrations(pending_only=True)[0]["deleted"]
        store.acknowledge_registration(removed["job_id"], removed["revision"])
        assert store.registrations(pending_only=True) == []
