import json
import io
import shutil
import threading
import unittest
import uuid
from contextlib import contextmanager
from pathlib import Path
from unittest.mock import patch

from sqlmodel import Session, SQLModel, create_engine, select

import app.main as main_module
from app import runtime_logging
from app import runtime_monitor
from app.runtime_monitor import get_runtime_heartbeat_status, upsert_runtime_heartbeat
from app.models import RuntimeHeartbeat, TikTokAuth
from app.models import utcnow


class RuntimeSplitStatusTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = Path.cwd() / "tests" / ".tmp_runtime_split_status" / str(uuid.uuid4())
        self.temp_dir.mkdir(parents=True, exist_ok=True)
        db_path = self.temp_dir / "runtime_split_status.db"
        self.engine = create_engine(
            f"sqlite:///{db_path.as_posix()}",
            connect_args={"check_same_thread": False},
        )
        SQLModel.metadata.create_all(self.engine)

    def tearDown(self) -> None:
        self.engine.dispose()
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def test_status_snapshot_uses_distinct_web_and_worker_heartbeats(self) -> None:
        with Session(self.engine) as session:
            session.add(
                RuntimeHeartbeat(
                    runtime_name="local_web_app",
                    host_name="web-host",
                    status="running",
                    details_json=json.dumps(
                        {
                            "service_mode": "web-app",
                            "discord_ingest_enabled": False,
                            "parser_worker_enabled": False,
                        }
                    ),
                )
            )
            session.add(
                RuntimeHeartbeat(
                    runtime_name="local_worker",
                    host_name="worker-host",
                    status="running",
                    details_json=json.dumps(
                        {
                            "service_mode": "worker-host",
                            "discord_status": "ready",
                            "discord_ingest_enabled": True,
                            "parser_worker_enabled": True,
                        }
                    ),
                )
            )
            session.commit()

            with patch.object(main_module, "APP_HEARTBEAT_RUNTIME_NAME", "local_web_app"), patch.object(
                main_module, "WORKER_RUNTIME_NAME", "local_worker"
            ):
                snapshot = main_module.build_status_snapshot(session)

        self.assertEqual(snapshot["app_runtime"]["host_name"], "web-host")
        self.assertEqual(snapshot["worker_runtime"]["host_name"], "worker-host")
        self.assertEqual(snapshot["app_runtime"]["label"], "Running")
        self.assertEqual(snapshot["worker_runtime"]["label"], "Running")
        self.assertEqual(snapshot["worker_runtime"]["details"].get("discord_status"), "ready")

    def test_status_snapshot_marks_recent_sqlite_contention_as_attention_needed(self) -> None:
        with Session(self.engine) as session:
            with patch.object(main_module, "recent_db_failure", return_value=True):
                snapshot = main_module.build_status_snapshot(session)

        self.assertFalse(snapshot["db_ok"])
        self.assertEqual(snapshot["db_health"]["label"], "Busy")
        self.assertTrue(
            any("SQLite recently reported write contention" in message for message in snapshot["alert_messages"])
        )

    def test_runtime_heartbeat_marks_degraded_status_as_attention_needed(self) -> None:
        with Session(self.engine) as session:
            session.add(
                RuntimeHeartbeat(
                    runtime_name="local_worker",
                    host_name="worker-host",
                    status="degraded",
                    details_json=json.dumps({"discord_status": "degraded"}),
                )
            )
            session.commit()

            heartbeat = get_runtime_heartbeat_status(
                session,
                "local_worker",
                runtime_label="Ingest Worker",
                updated_at_formatter=lambda value: value.isoformat() if value else "never",
            )

        self.assertEqual(heartbeat["label"], "Degraded")
        self.assertTrue(heartbeat["is_running"])
        self.assertTrue(heartbeat["needs_attention"])
        self.assertIn("degraded state", heartbeat["alert_message"])

    def test_runtime_heartbeat_serializes_datetime_details(self) -> None:
        with Session(self.engine) as session:
            recorded_at = utcnow()
            upsert_runtime_heartbeat(
                session,
                runtime_name="local_worker",
                host_name="worker-host",
                status="running",
                details={
                    "discord_status": "ready",
                    "last_pull_at": recorded_at,
                    "nested": {"last_callback_at": recorded_at},
                },
            )

            heartbeat = session.exec(
                select(RuntimeHeartbeat).where(RuntimeHeartbeat.runtime_name == "local_worker")
            ).first()

        self.assertIsNotNone(heartbeat)
        payload = json.loads(heartbeat.details_json)
        self.assertEqual(payload["last_pull_at"], recorded_at.isoformat())
        self.assertEqual(payload["nested"]["last_callback_at"], recorded_at.isoformat())

    def test_health_endpoint_surfaces_db_and_runtime_status(self) -> None:
        @contextmanager
        def fake_managed_session():
            with Session(self.engine) as session:
                yield session

        with patch.object(main_module, "managed_session", fake_managed_session), patch.object(
            main_module,
            "get_database_health",
            return_value={
                "ok": False,
                "needs_attention": True,
                "label": "Busy",
                "checked_at_label": "just now",
            },
        ), patch.object(
            main_module,
            "get_runtime_heartbeat_status",
            return_value={
                "status": "degraded",
                "label": "Degraded",
                "needs_attention": True,
                "updated_at": None,
                "updated_at_label": "1 minute ago",
            },
        ):
            health = main_module.health()

        self.assertFalse(health.ok)
        self.assertFalse(health.db_ok)
        self.assertEqual(health.local_runtime_status, "degraded")
        self.assertEqual(health.local_runtime_label, "Degraded")
        self.assertTrue(health.local_runtime_needs_attention)

    def test_status_snapshot_surfaces_tiktok_sync_freshness(self) -> None:
        auth_updated_at = utcnow()
        callback_received_at = utcnow()
        webhook_received_at = utcnow()
        pull_started_at = utcnow()
        pull_finished_at = utcnow()
        with Session(self.engine) as session:
            session.add(
                TikTokAuth(
                    tiktok_shop_id="shop-1",
                    shop_cipher="cipher-1",
                    access_token="access-token",
                    refresh_token="refresh-token",
                )
            )
            session.commit()

            with patch.object(
                main_module,
                "read_tiktok_integration_state",
                return_value={
                    "last_authorization_at": auth_updated_at,
                    "last_callback": {
                        "received_at": callback_received_at.isoformat(),
                        "query": {"app_key": "app-key", "shop_region": "US"},
                    },
                    "last_webhook_at": webhook_received_at,
                    "last_webhook": {
                        "received_at": webhook_received_at.isoformat(),
                        "topic": "order.status.change",
                        "body_sha256": "abc123",
                        "payload": {"kind": "object", "keys": ["order_id"]},
                    },
                    "is_pull_running": False,
                    "last_pull_started_at": pull_started_at,
                    "last_pull_finished_at": pull_finished_at,
                    "last_pull": {
                        "status": "success",
                        "trigger": "automatic",
                        "fetched": 2,
                        "inserted": 1,
                        "updated": 1,
                        "failed": 0,
                    },
                    "last_error": None,
                },
            ):
                snapshot = main_module.build_status_snapshot(session)

        tiktok_snapshot = snapshot["tiktok_sync"]
        self.assertEqual(tiktok_snapshot["status_label"], "Connected")
        self.assertEqual(tiktok_snapshot["sync_label"], "Sync healthy")
        self.assertNotEqual(tiktok_snapshot["last_authorization_label"], "none yet")
        self.assertNotEqual(tiktok_snapshot["last_callback_label"], "none yet")
        self.assertNotEqual(tiktok_snapshot["last_webhook_label"], "none yet")
        self.assertNotEqual(tiktok_snapshot["last_pull_started_label"], "none yet")
        self.assertNotEqual(tiktok_snapshot["last_pull_finished_label"], "none yet")
        self.assertEqual(tiktok_snapshot["last_pull"]["status"], "success")
        json.dumps(tiktok_snapshot)

    def test_runtime_heartbeat_loop_survives_details_provider_failure(self) -> None:
        class StopAfterOneWait:
            def __init__(self) -> None:
                self._is_set = False
                self.wait_calls: list[float | None] = []

            def is_set(self) -> bool:
                return self._is_set

            def wait(self, timeout: float | None = None) -> bool:
                self.wait_calls.append(timeout)
                self._is_set = True
                return True

        stop_event = StopAfterOneWait()
        details_provider_calls = []
        captured_exceptions = []

        def details_provider() -> dict:
            details_provider_calls.append("called")
            raise RuntimeError("details provider exploded")

        def capture_excepthook(args: threading.ExceptHookArgs) -> None:
            captured_exceptions.append(args)

        original_excepthook = threading.excepthook
        threading.excepthook = capture_excepthook
        try:
            runtime_monitor.runtime_heartbeat_loop(
                stop_event,  # type: ignore[arg-type]
                runtime_name="local_worker",
                host_name="worker-host",
                details_provider=details_provider,
            )
        finally:
            threading.excepthook = original_excepthook

        self.assertEqual(details_provider_calls, ["called"])
        self.assertEqual(stop_event.wait_calls, [runtime_monitor.RUNTIME_HEARTBEAT_INTERVAL_SECONDS])
        self.assertEqual(captured_exceptions, [])

    def test_tee_stream_flush_ignores_closed_mirror(self) -> None:
        class PrimaryStream:
            def __init__(self) -> None:
                self.flushed = False

            def write(self, data: str) -> int:
                return len(data)

            def flush(self) -> None:
                self.flushed = True

            def isatty(self) -> bool:
                return False

            def fileno(self) -> int:
                return 1

        primary = PrimaryStream()
        mirror = io.StringIO()
        mirror.close()
        tee = runtime_logging.TeeStream(primary, mirror)

        tee.flush()

        self.assertTrue(primary.flushed)

    def test_runtime_log_cleanup_ignores_already_closed_handle(self) -> None:
        handle = io.StringIO()
        handle.close()

        runtime_logging._close_runtime_log_handle(handle)


if __name__ == "__main__":
    unittest.main()
