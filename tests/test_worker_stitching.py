import unittest
from datetime import timedelta

from app.models import DiscordMessage, utcnow
from app.discord.worker import (
    build_stitch_group,
    is_bare_amount_fragment_text,
    is_short_fragment,
    should_stitch_rows,
)


class _RowsResult:
    def __init__(self, rows: list[DiscordMessage]) -> None:
        self.rows = rows

    def all(self) -> list[DiscordMessage]:
        return self.rows


class _RowsSession:
    def __init__(self, rows: list[DiscordMessage]) -> None:
        self.rows = rows

    def exec(self, _statement: object) -> _RowsResult:
        return _RowsResult(self.rows)


class WorkerStitchingTests(unittest.TestCase):
    def make_row(self, content: str, *, seconds: int = 0, has_image: bool = False) -> DiscordMessage:
        return DiscordMessage(
            discord_message_id=f"msg-{content}-{seconds}",
            channel_id="chan-1",
            channel_name="store-sales-and-trades",
            author_id="author-1",
            author_name="tester",
            content=content,
            attachment_urls_json='["https://example.test/card.jpg"]' if has_image else "[]",
            created_at=utcnow() + timedelta(seconds=seconds),
        )

    def make_numbered_row(
        self,
        row_id: int,
        content: str,
        *,
        seconds: int,
        has_image: bool = False,
    ) -> DiscordMessage:
        row = self.make_row(content, seconds=seconds, has_image=has_image)
        row.id = row_id
        return row

    def test_short_explicit_image_deal_is_not_fragment(self) -> None:
        row = self.make_row("Buy for $374", has_image=True)

        self.assertFalse(is_short_fragment(row))

    def test_back_to_back_image_deals_do_not_steal_trade_fragment(self) -> None:
        buy_row = self.make_row("Buy for $374", seconds=0, has_image=True)
        trade_row = self.make_row("Top Put and Bottom In", seconds=15, has_image=True)

        self.assertFalse(should_stitch_rows(buy_row, [buy_row, trade_row]))

    def test_image_then_explicit_text_still_force_stitches(self) -> None:
        image_row = self.make_row("", seconds=0, has_image=True)
        text_row = self.make_row("Buy 450 cash", seconds=25, has_image=False)

        self.assertTrue(should_stitch_rows(image_row, [image_row, text_row]))

    def test_alternating_image_text_deals_pair_consistently_from_every_row(self) -> None:
        rows = [
            self.make_numbered_row(1, "", seconds=0, has_image=True),
            self.make_numbered_row(2, "Sold 4 packs 80", seconds=5),
            self.make_numbered_row(3, "", seconds=10, has_image=True),
            self.make_numbered_row(4, "Sold 15 packs 200 cash 25 register", seconds=27),
        ]
        session = _RowsSession(rows)

        groups = {
            row.id: [member.id for member in build_stitch_group(session, row, 45, 3)]
            for row in rows
        }

        self.assertEqual(groups, {1: [1, 2], 2: [1, 2], 3: [3, 4], 4: [3, 4]})

    def test_image_description_and_bare_amount_form_one_group_from_every_row(self) -> None:
        rows = [
            self.make_numbered_row(1, "", seconds=0, has_image=True),
            self.make_numbered_row(2, "Bought slabs and singles", seconds=3),
            self.make_numbered_row(3, "2162", seconds=5),
        ]
        session = _RowsSession(rows)

        groups = {
            row.id: [member.id for member in build_stitch_group(session, row, 30, 3)]
            for row in rows
        }

        self.assertEqual(groups, {1: [1, 2, 3], 2: [1, 2, 3], 3: [1, 2, 3]})

    def test_text_then_image_remains_a_supported_fallback(self) -> None:
        rows = [
            self.make_numbered_row(1, "Buy 450 cash", seconds=0),
            self.make_numbered_row(2, "", seconds=5, has_image=True),
        ]
        session = _RowsSession(rows)

        groups = {
            row.id: [member.id for member in build_stitch_group(session, row, 30, 3)]
            for row in rows
        }

        self.assertEqual(groups, {1: [1, 2], 2: [1, 2]})

    def test_multiplier_is_not_treated_as_a_bare_amount_fragment(self) -> None:
        self.assertTrue(is_bare_amount_fragment_text("2162"))
        self.assertTrue(is_bare_amount_fragment_text("$5"))
        self.assertFalse(is_bare_amount_fragment_text("5"))
        self.assertFalse(is_bare_amount_fragment_text("2x"))


if __name__ == "__main__":
    unittest.main()
