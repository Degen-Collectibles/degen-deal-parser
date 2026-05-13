import unittest
from datetime import datetime, timedelta, timezone

from sqlmodel import Session, create_engine

from app.models import SQLModel, OperationsLog
from app.discord.ops_log import (
    count_recent_errors,
    list_operations_logs,
    write_operations_log,
)


def _utcnow():
    return datetime.now(timezone.utc)


class OpsLogFilterTests(unittest.TestCase):
    def setUp(self):
        self.engine = create_engine("sqlite:///:memory:")
        SQLModel.metadata.create_all(self.engine)
        self.session = Session(self.engine)

    def tearDown(self):
        self.session.close()

    def _add(self, event_type, level="info", source="worker", minutes_ago=0):
        row = OperationsLog(
            event_type=event_type,
            level=level,
            source=source,
            message=event_type,
            details_json="{}",
            created_at=_utcnow() - timedelta(minutes=minutes_ago),
        )
        self.session.add(row)
        self.session.commit()
        return row

    def test_no_filters_returns_all(self):
        self._add("queue.started")
        self._add("ingest.message")
        rows = list_operations_logs(self.session)
        self.assertEqual(len(rows), 2)

    def test_filter_by_level_error(self):
        self._add("queue.started", level="info")
        self._add("queue.failed", level="error")
        rows = list_operations_logs(self.session, level="error")
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0].level, "error")

    def test_filter_by_level_info(self):
        self._add("queue.started", level="info")
        self._add("queue.failed", level="error")
        rows = list_operations_logs(self.session, level="info")
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0].level, "info")

    def test_filter_by_event_type_prefix_queue(self):
        self._add("queue.started")
        self._add("queue.failed")
        self._add("ingest.message")
        rows = list_operations_logs(self.session, event_type_prefix="queue.")
        self.assertEqual(len(rows), 2)
        for row in rows:
            self.assertTrue(row.event_type.startswith("queue."))

    def test_filter_by_event_type_prefix_ingest(self):
        self._add("queue.started")
        self._add("ingest.message")
        self._add("ingest.deleted")
        rows = list_operations_logs(self.session, event_type_prefix="ingest.")
        self.assertEqual(len(rows), 2)

    def test_filter_by_date_since_excludes_old(self):
        self._add("queue.old", minutes_ago=120)
        self._add("queue.recent", minutes_ago=10)
        cutoff = _utcnow() - timedelta(minutes=30)
        rows = list_operations_logs(self.session, since=cutoff)
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0].event_type, "queue.recent")

    def test_filter_by_date_until_excludes_future(self):
        self._add("queue.old", minutes_ago=120)
        self._add("queue.recent", minutes_ago=10)
        cutoff = _utcnow() - timedelta(minutes=30)
        rows = list_operations_logs(self.session, until=cutoff)
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0].event_type, "queue.old")

    def test_combined_filters(self):
        self._add("queue.started", level="info", minutes_ago=10)
        self._add("queue.failed", level="error", minutes_ago=10)
        self._add("ingest.message", level="error", minutes_ago=10)
        rows = list_operations_logs(self.session, event_type_prefix="queue.", level="error")
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0].event_type, "queue.failed")

    def test_backward_compatible_no_args(self):
        for i in range(5):
            self._add(f"queue.event_{i}")
        rows = list_operations_logs(self.session)
        self.assertEqual(len(rows), 5)


class CountRecentErrorsTests(unittest.TestCase):
    def setUp(self):
        self.engine = create_engine("sqlite:///:memory:")
        SQLModel.metadata.create_all(self.engine)
        self.session = Session(self.engine)

    def tearDown(self):
        self.session.close()

    def _add_error(self, minutes_ago=0):
        row = OperationsLog(
            event_type="queue.failed",
            level="error",
            source="worker",
            message="failed",
            details_json="{}",
            created_at=_utcnow() - timedelta(minutes=minutes_ago),
        )
        self.session.add(row)
        self.session.commit()

    def test_counts_errors_within_window(self):
        self._add_error(minutes_ago=10)
        self._add_error(minutes_ago=30)
        self._add_error(minutes_ago=90)  # outside 60-min window
        count = count_recent_errors(self.session, since_minutes=60)
        self.assertEqual(count, 2)

    def test_zero_when_no_errors(self):
        row = OperationsLog(
            event_type="queue.started",
            level="info",
            source="worker",
            message="ok",
            details_json="{}",
            created_at=_utcnow() - timedelta(minutes=5),
        )
        self.session.add(row)
        self.session.commit()
        count = count_recent_errors(self.session)
        self.assertEqual(count, 0)

    def test_counts_only_errors_not_warnings(self):
        row = OperationsLog(
            event_type="queue.warn",
            level="warning",
            source="worker",
            message="warn",
            details_json="{}",
            created_at=_utcnow() - timedelta(minutes=5),
        )
        self.session.add(row)
        self.session.commit()
        count = count_recent_errors(self.session)
        self.assertEqual(count, 0)

    def test_respects_custom_window(self):
        self._add_error(minutes_ago=5)
        self._add_error(minutes_ago=25)
        self.assertEqual(count_recent_errors(self.session, since_minutes=10), 1)
        self.assertEqual(count_recent_errors(self.session, since_minutes=30), 2)


if __name__ == "__main__":
    unittest.main()
