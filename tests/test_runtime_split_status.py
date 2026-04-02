import json
import shutil
import unittest
import uuid
from pathlib import Path
from unittest.mock import patch

from sqlmodel import Session, SQLModel, create_engine

import app.main as main_module
from app.models import RuntimeHeartbeat


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


if __name__ == "__main__":
    unittest.main()
