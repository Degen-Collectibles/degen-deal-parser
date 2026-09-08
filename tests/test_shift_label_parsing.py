"""Pin the shared shift-label parser.

Schedule labor cost, timecard variance, the pay-rates hourly summary, and the
employee dashboard sort all read the same free-text labels. They used to parse
them three different ways: "12-8" scored 20 hours instead of 8, and "3-7" was
3 AM on the timecard page while the dashboard sorted it as 3 PM (so an on-time
employee got a "Late arrival" pill). These tests hold all four surfaces to one
interpretation.
"""

import unittest

from app.routers.team import _parse_shift_start_minutes
from app.routers.team_admin_employees_timecards import (
    _parse_shift_ranges,
    _shift_total_hours,
)
from app.routers.team_admin_schedule import _parse_shift_hours


def _hhmm(minutes):
    return None if minutes is None else f"{(minutes // 60) % 24:02d}:{minutes % 60:02d}"


class NoonStartLabelTests(unittest.TestCase):
    """A bare noon start must not wrap overnight."""

    def test_twelve_to_eight_is_an_eight_hour_day(self):
        self.assertEqual(_parse_shift_hours("12-8"), 8.0)
        self.assertEqual(_parse_shift_ranges("12-8"), [(12 * 60, 20 * 60)])

    def test_twelve_thirty_start(self):
        self.assertEqual(_parse_shift_hours("12:30-8"), 7.5)

    def test_twelve_to_four(self):
        self.assertEqual(_parse_shift_hours("12-4"), 4.0)

    def test_explicit_pm_matches_bare(self):
        self.assertEqual(_parse_shift_hours("12-8"), _parse_shift_hours("12PM-8PM"))


class BareAfternoonLabelTests(unittest.TestCase):
    """Bare 1-5 starts mean afternoon on every surface, not just the dashboard."""

    def test_three_to_seven_is_afternoon(self):
        self.assertEqual(_parse_shift_ranges("3-7"), [(15 * 60, 19 * 60)])
        self.assertEqual(_parse_shift_hours("3-7"), 4.0)

    def test_one_to_nine_is_afternoon(self):
        self.assertEqual(_parse_shift_ranges("1-9"), [(13 * 60, 21 * 60)])

    def test_two_to_ten_is_afternoon(self):
        self.assertEqual(_parse_shift_ranges("2-10"), [(14 * 60, 22 * 60)])

    def test_explicit_am_end_keeps_morning_reading(self):
        self.assertEqual(_parse_shift_ranges("3-7am"), [(3 * 60, 7 * 60)])


class MorningLabelTests(unittest.TestCase):
    def test_nine_to_five(self):
        self.assertEqual(_parse_shift_ranges("9-5"), [(9 * 60, 17 * 60)])

    def test_ten_to_six(self):
        self.assertEqual(_parse_shift_ranges("10-6"), [(10 * 60, 18 * 60)])

    def test_ten_to_two(self):
        self.assertEqual(_parse_shift_ranges("10-2"), [(10 * 60, 14 * 60)])

    def test_nine_to_twelve_stays_a_morning_block(self):
        self.assertEqual(_parse_shift_ranges("9-12"), [(9 * 60, 12 * 60)])

    def test_eleven_to_seven(self):
        self.assertEqual(_parse_shift_ranges("11-7"), [(11 * 60, 19 * 60)])


class ExplicitAmPmTests(unittest.TestCase):
    def test_full_label(self):
        self.assertEqual(_parse_shift_hours("10:30 AM - 6:30 PM"), 8.0)

    def test_compact_label(self):
        self.assertEqual(_parse_shift_hours("10am-2pm"), 4.0)

    def test_quarter_hours(self):
        self.assertEqual(_parse_shift_hours("4 PM - 8:15 PM"), 4.25)

    def test_genuine_overnight_needs_explicit_ampm(self):
        self.assertEqual(_parse_shift_hours("10 PM - 2 AM"), 4.0)

    def test_mixed_bare_start_explicit_end(self):
        self.assertEqual(_parse_shift_ranges("10-6pm"), [(10 * 60, 18 * 60)])


class SplitShiftTests(unittest.TestCase):
    def test_ranges_sum(self):
        self.assertEqual(_parse_shift_hours("9 AM - 12 PM / 2 PM - 6 PM"), 7.0)

    def test_bare_split_shift(self):
        self.assertEqual(_parse_shift_hours("10-2 / 3-7"), 8.0)
        self.assertEqual(
            _parse_shift_ranges("10-2 / 3-7"),
            [(10 * 60, 14 * 60), (15 * 60, 19 * 60)],
        )


class NonShiftAndGarbageTests(unittest.TestCase):
    def test_non_shift_tokens_are_zero(self):
        for label in ("", "   ", "OFF", "SHOW", "REQUEST", "IF NEEDED", "STREAM"):
            self.assertEqual(_parse_shift_hours(label), 0.0, label)
            self.assertEqual(_parse_shift_ranges(label), [], label)

    def test_unparseable_is_zero_not_exception(self):
        self.assertEqual(_parse_shift_hours("whatever"), 0.0)
        self.assertEqual(_parse_shift_hours("10:30 AM -"), 0.0)
        self.assertEqual(_parse_shift_hours("garbage"), 0.0)

    def test_out_of_range_values_rejected(self):
        self.assertEqual(_parse_shift_ranges("25-30"), [])
        self.assertEqual(_parse_shift_ranges("10:75-6"), [])


class SurfaceAgreementTests(unittest.TestCase):
    """The dashboard sort key must match the window timecards score against."""

    LABELS = (
        "12-8",
        "12:30-8",
        "3-7",
        "1-9",
        "2-10",
        "9-5",
        "10-6",
        "11-7",
        "10-2",
        "10:30 AM - 6:30 PM",
        "10am-2pm",
        "4 PM - 8:15 PM",
        "10 PM - 2 AM",
    )

    def test_dashboard_sort_matches_timecard_start(self):
        for label in self.LABELS:
            ranges = _parse_shift_ranges(label)
            self.assertTrue(ranges, label)
            self.assertEqual(
                _hhmm(_parse_shift_start_minutes(label)),
                _hhmm(ranges[0][0]),
                label,
            )

    def test_schedule_hours_match_timecard_hours(self):
        for label in self.LABELS:
            self.assertEqual(
                _parse_shift_hours(label),
                _shift_total_hours(_parse_shift_ranges(label)),
                label,
            )

    def test_no_label_scores_more_than_a_double_shift(self):
        """Guards the class of bug where a bare label wrapped into ~20 hours."""
        for label in self.LABELS:
            self.assertLessEqual(_parse_shift_hours(label), 12.0, label)


if __name__ == "__main__":
    unittest.main()
