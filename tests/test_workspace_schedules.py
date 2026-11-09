from inkbox_codex.schedules import ScheduleStore


def test_managed_create_rejection_is_durable_and_rolls_back_partial_writes(tmp_path, monkeypatch):
    path = tmp_path / 'jobs.sqlite'
    command = {'action': 'schedule_mutate', 'command_id': 'rejected-create', 'operation': 'create',
               'job_id': None, 'revision': 0, 'fields': {'name': 'Example',
               'task': {'kind': 'prompt', 'prompt': 'Summarize notes'},
               'timing': {'kind': 'interval', 'seconds': 600}}}
    with ScheduleStore(path) as store:
        store.use_external_timer()
        original = store.create

        def rejected(**kwargs):
            original(**kwargs)
            raise ValueError('Validation failed')

        monkeypatch.setattr(store, 'create', rejected)
        receipt = store.mutate_managed(command)
        assert receipt == {'command_id': 'rejected-create', 'state': 'rejected', 'creation_rejected': True}
        assert store.list_jobs() == []
    with ScheduleStore(path) as store:
        assert store.mutate_managed(command) == receipt
        assert store.list_jobs() == []
        accepted = {**command, 'command_id': 'accepted-create'}
        success = store.mutate_managed(accepted)
        assert store.mutate_managed(accepted) == success
        assert len(store.list_jobs()) == 1


def test_managed_expired_once_rejects_without_creating_job(tmp_path):
    command = {'action': 'schedule_mutate', 'command_id': 'expired-create', 'operation': 'create',
               'job_id': None, 'revision': 0, 'fields': {'name': 'Example',
               'task': {'kind': 'prompt', 'prompt': 'Summarize notes'},
               'timing': {'kind': 'once', 'at': '2020-01-01T00:00:00Z'}}}
    with ScheduleStore(tmp_path / 'jobs.sqlite') as store:
        store.use_external_timer()
        assert store.mutate_managed(command)['creation_rejected'] is True
        assert store.mutate_managed(command)['creation_rejected'] is True
        assert store.list_jobs() == []


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
