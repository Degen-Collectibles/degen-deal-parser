import shutil
import unittest
import uuid
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch
from urllib.parse import parse_qs, urlsplit
from urllib.parse import unquote as parse_unquote

from fastapi import FastAPI
from fastapi.testclient import TestClient
from sqlmodel import Session, SQLModel, create_engine
from starlette.requests import Request

from app.db import get_session
from app.models import DiscordMessage, PARSE_PARSED, WatchedChannel, utcnow
from app.routers.deals import deal_detail_page, router as deals_router


def make_request(message_id: int, *, role: str = "admin") -> Request:
    request = Request(
        {
            "type": "http",
            "method": "GET",
            "path": f"/deals/{message_id}",
            "headers": [],
            "scheme": "http",
            "server": ("testserver", 80),
            "client": ("testclient", 50000),
            "root_path": "",
        }
    )
    request.state.current_user = SimpleNamespace(
        username="tester",
        display_name="Test Operator",
        role=role,
    )
    return request


class DealReturnPathSecurityTests(unittest.TestCase):
    maxDiff = None

    def setUp(self) -> None:
        self.temp_dir = Path.cwd() / "tests" / ".tmp_deal_return_path_security" / str(uuid.uuid4())
        self.temp_dir.mkdir(parents=True, exist_ok=True)
        db_path = self.temp_dir / "return-path.db"
        self.engine = create_engine(
            f"sqlite:///{db_path.as_posix()}",
            connect_args={"check_same_thread": False},
        )
        SQLModel.metadata.create_all(self.engine)

        with Session(self.engine) as session:
            session.add(
                WatchedChannel(
                    channel_id="chan-public",
                    channel_name="public deals",
                    is_enabled=True,
                )
            )
            row = DiscordMessage(
                discord_message_id=f"disc-{uuid.uuid4()}",
                channel_id="chan-public",
                channel_name="public deals",
                author_name="tester",
                content="Sell card 25 cash",
                created_at=utcnow(),
                parse_status=PARSE_PARSED,
            )
            session.add(row)
            session.commit()
            session.refresh(row)
            self.message_id = row.id

    def tearDown(self) -> None:
        self.engine.dispose()
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def _detail(self, session: Session, *, return_path: str):
        return deal_detail_page(
            message_id=self.message_id,
            request=make_request(self.message_id),
            return_path=return_path,
            status="parsed",
            channel_id="chan-public",
            entry_kind=None,
            expense_category=None,
            after=None,
            before=None,
            sort_by=None,
            sort_dir=None,
            page=1,
            limit=25,
            success=None,
            error=None,
            session=session,
        )

    def _http_client(self) -> TestClient:
        app = FastAPI()

        @app.middleware("http")
        async def add_test_user(request, call_next):
            request.state.current_user = SimpleNamespace(
                username="tester",
                display_name="Test Operator",
                role="admin",
            )
            return await call_next(request)

        app.include_router(deals_router)

        def override_get_session():
            with Session(self.engine) as session:
                yield session

        app.dependency_overrides[get_session] = override_get_session
        return TestClient(app)

    def assert_return_context(self, response, *, expected_path: str) -> None:
        self.assertEqual(response.context["return_path"], expected_path)
        parsed_back_url = urlsplit(response.context["back_url"])
        self.assertEqual(parsed_back_url.path, expected_path)
        self.assertEqual(
            parse_qs(parsed_back_url.query),
            {
                "status": ["parsed"],
                "channel_id": ["chan-public"],
                "limit": ["25"],
            },
        )
        self.assertEqual(parsed_back_url.fragment, "")

    def test_hostile_return_paths_fail_closed_everywhere(self) -> None:
        hostile_return_paths = (
            "javascript:alert(document.domain)",
            "data:text/html,<script>alert(1)</script>",
            "//evil.example/path",
            r"\evil.example\path",
            r"/\evil.example/path",
            r"/safe\path",
            "/%5cevil.example/path",
            "/%0D%0Aevil",
            "/%00evil",
            "/%250D%250Aevil",
            "/%252F%252Fevil.example/path",
            "/table?status=parsed",
            "/table#fragment",
            "//[invalid",
        )

        with Session(self.engine) as session:
            for return_path in hostile_return_paths:
                with self.subTest(return_path=return_path), patch(
                    "app.routers.deals.require_role_response",
                    return_value=None,
                ) as require_role:
                    response = self._detail(session, return_path=return_path)

                    self.assert_return_context(response, expected_path="/deals")
                    self.assertEqual(require_role.call_args.args[1], "viewer")

    def test_deeply_nested_percent_encoding_has_bounded_decode_work(self) -> None:
        deeply_nested_control = "/%" + ("25" * 8) + "0D"

        with Session(self.engine) as session, patch(
            "app.routers.deals.unquote",
            wraps=parse_unquote,
        ) as decode, patch(
            "app.routers.deals.require_role_response",
            return_value=None,
        ) as require_role:
            response = self._detail(session, return_path=deeply_nested_control)

        self.assert_return_context(response, expected_path="/deals")
        self.assertEqual(require_role.call_args.args[1], "viewer")
        self.assertLessEqual(decode.call_count, 4)

    def test_overlong_return_path_fails_closed_before_decoding(self) -> None:
        overlong_path = "/" + ("a" * 2048)

        with Session(self.engine) as session, patch(
            "app.routers.deals.unquote",
            wraps=parse_unquote,
        ) as decode, patch(
            "app.routers.deals.require_role_response",
            return_value=None,
        ) as require_role:
            response = self._detail(session, return_path=overlong_path)

        self.assert_return_context(response, expected_path="/deals")
        self.assertEqual(require_role.call_args.args[1], "viewer")
        decode.assert_not_called()

    def test_http_query_decoding_cannot_reintroduce_hostile_return_paths(self) -> None:
        hostile_wire_queries = (
            "return_path=javascript:alert(1)",
            "return_path=%256Aavascript%253Aalert%25281%2529",
            "return_path=/%0D%0Aevil",
            "return_path=%252F%25250D%25250Aevil",
        )

        with self._http_client() as client, patch(
            "app.routers.deals.require_role_response",
            return_value=None,
        ) as require_role:
            for wire_query in hostile_wire_queries:
                with self.subTest(wire_query=wire_query):
                    response = client.get(f"/deals/{self.message_id}?{wire_query}")

                    self.assertEqual(response.status_code, 200)
                    self.assertIn('name="return_path" value="/deals"', response.text)
                    self.assertNotIn("javascript:alert(1)", response.text)
                    self.assertNotIn("/%0D%0Aevil", response.text)
                    self.assertEqual(require_role.call_args.args[1], "viewer")

    def test_safe_local_return_paths_preserve_path_and_role_semantics(self) -> None:
        cases = (
            ("/deals", "viewer"),
            ("/table", "admin"),
            ("/review-table", "admin"),
            ("/ledger", "reviewer"),
            ("/inventory/42", "viewer"),
        )

        with Session(self.engine) as session:
            for return_path, expected_role in cases:
                with self.subTest(return_path=return_path), patch(
                    "app.routers.deals.require_role_response",
                    return_value=None,
                ) as require_role:
                    response = self._detail(session, return_path=return_path)

                    self.assert_return_context(response, expected_path=return_path)
                    self.assertEqual(require_role.call_args.args[1], expected_role)


if __name__ == "__main__":
    unittest.main()
