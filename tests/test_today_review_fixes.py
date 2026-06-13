"""Tests for the 7 review fixes applied on 2026-04-16.

Covers the gaps not exercised by the existing test suite:
  1. _parse_activity: zero-valued distance/calories/elevation not coerced to None
  2. _resolve_fetch_range: bridge warning logged when fetch range is extended
  3. _parse_tz_offset: half-hour and quarter-hour offsets parsed correctly
  4. _today(): respects COROS_TIMEZONE instead of system clock
  5. cmd_sync argparse: unknown flags rejected; --help exits cleanly
  6. load_dotenv not called at import time
"""

import contextlib
import importlib
import os
import sys
import unittest
from datetime import UTC, timedelta, timezone
from unittest.mock import patch

import pytest

# ---------------------------------------------------------------------------
# 1. _parse_activity — zero values must not fall through to fallback fields
# ---------------------------------------------------------------------------

class TestParseActivityZeroValues:
    """Issue #1: `or` coerced 0 to falsy; fixed with `is not None` checks."""

    def _make_item(self, **kwargs):
        """Minimal activity dict with only the provided fields."""
        return {"labelId": "42", "sportType": 402, **kwargs}

    def _parse(self, item):
        from coros_mcp.coros_api import _parse_activity
        return _parse_activity(item)

    # distance

    def test_distance_zero_uses_primary_field(self):
        """distance=0 must not fall through to totalDistance."""
        a = self._parse(self._make_item(distance=0, totalDistance=5000))
        assert a.distance_meters == 0

    def test_distance_none_falls_back_to_totalDistance(self):
        """When distance is absent, totalDistance is used."""
        a = self._parse(self._make_item(totalDistance=5000))
        assert a.distance_meters == 5000

    def test_distance_absent_and_totalDistance_absent_is_none(self):
        a = self._parse(self._make_item())
        assert a.distance_meters is None

    # calories

    def test_calories_passed_through_as_cal(self):
        """API field 'calorie' is in cal and stored as-is."""
        a = self._parse(self._make_item(calorie=932148))
        assert a.calories == 932148

    def test_calories_absent_is_none(self):
        a = self._parse(self._make_item())
        assert a.calories is None

    # elevation

    def test_elevation_zero_ascent_uses_primary_field(self):
        """ascent=0 must not fall through to totalAscent or elevationGain."""
        a = self._parse(self._make_item(ascent=0, totalAscent=100, elevationGain=200))
        assert a.elevation_gain == 0

    def test_elevation_none_ascent_falls_back_to_totalAscent(self):
        a = self._parse(self._make_item(totalAscent=100, elevationGain=200))
        assert a.elevation_gain == 100

    def test_elevation_zero_totalAscent_uses_second_field(self):
        """totalAscent=0 must not fall through to elevationGain."""
        a = self._parse(self._make_item(totalAscent=0, elevationGain=200))
        assert a.elevation_gain == 0

    def test_elevation_falls_back_to_elevationGain(self):
        a = self._parse(self._make_item(elevationGain=200))
        assert a.elevation_gain == 200

    def test_elevation_all_absent_is_none(self):
        a = self._parse(self._make_item())
        assert a.elevation_gain is None


class TestApplyWorkoutCalculation:
    """Schedule update flow: calculate() fields are mapped back to program fields."""

    def test_calculation_updates_program_copy(self):
        from coros_mcp.coros_api import apply_workout_calculation

        program = {
            "duration": 100,
            "estimatedTime": 100,
            "estimatedValue": 10,
            "trainingLoad": 10,
            "distance": "1000.00",
            "estimatedDistance": 1000,
            "elevGain": 1,
            "sets": 1,
            "totalSets": 1,
            "exerciseBarChart": [],
        }
        calculation = {
            "planDuration": 200,
            "planTrainingLoad": 30,
            "planDistance": "2500.00",
            "planElevGain": 5,
            "planSets": 2,
            "planHybridTotalSets": 3,
            "exerciseBarChart": [{"exerciseId": "1"}],
        }

        updated = apply_workout_calculation(program, calculation)

        assert updated is not program
        assert updated["duration"] == 200
        assert updated["estimatedTime"] == 200
        assert updated["trainingLoad"] == 30
        assert updated["estimatedValue"] == 30
        assert updated["distance"] == "2500.00"
        assert updated["estimatedDistance"] == 2500
        assert updated["elevGain"] == 5
        assert updated["sets"] == 2
        assert updated["totalSets"] == 3
        assert updated["exerciseBarChart"] == [{"exerciseId": "1"}]
        assert program["duration"] == 100

    def test_missing_calculation_fields_leave_program_untouched(self):
        from coros_mcp.coros_api import apply_workout_calculation

        program = {"duration": 100, "sets": 1}
        updated = apply_workout_calculation(program, {})
        assert updated == program
        assert updated is not program

    def test_sets_only_applied_when_present_in_program(self):
        from coros_mcp.coros_api import apply_workout_calculation

        # planSets/planHybridTotalSets are ignored when the program has no such keys.
        updated = apply_workout_calculation(
            {"duration": 100},
            {"planSets": 2, "planHybridTotalSets": 3},
        )
        assert "sets" not in updated
        assert "totalSets" not in updated

    def test_invalid_distance_skips_estimated_distance(self):
        from coros_mcp.coros_api import apply_workout_calculation

        updated = apply_workout_calculation(
            {"estimatedDistance": 1000},
            {"planDistance": "not-a-number"},
        )
        # distance string is still applied; estimatedDistance is left untouched.
        assert updated["distance"] == "not-a-number"
        assert updated["estimatedDistance"] == 1000


# ---------------------------------------------------------------------------
# 2. Bridge warning — logged when fetch range is extended for contiguity
# ---------------------------------------------------------------------------

class TestBridgeWarning(unittest.TestCase):

    def _resolve(self, min_cached, max_cached, start_day, end_day, cutoff="20260412"):
        from coros_mcp.cache.sync import _resolve_fetch_range
        return _resolve_fetch_range(min_cached, max_cached, start_day, end_day, cutoff)

    def test_warning_emitted_when_bridge_extends_range(self):
        """A historical gap that requires bridging past end_day must emit a warning."""
        with self.assertLogs("coros_mcp.cache.sync", level="WARNING") as cm:
            self._resolve("20260301", "20260414", "20240101", "20240630")
        assert any("bridge" in msg.lower() for msg in cm.output)

    def test_no_warning_when_end_day_already_reaches_min_cached(self):
        """If end_day already overlaps min_cached, no bridge extension → no warning."""
        with patch("coros_mcp.cache.sync.logger") as mock_logger:
            self._resolve("20260301", "20260414", "20250101", "20260315")
        mock_logger.warning.assert_not_called()

    def test_no_warning_on_tail_gap(self):
        """Tail-only gaps never trigger the bridge warning."""
        with patch("coros_mcp.cache.sync.logger") as mock_logger:
            self._resolve("20260301", "20260410", "20260305", "20260420")
        mock_logger.warning.assert_not_called()


# ---------------------------------------------------------------------------
# 3. _parse_tz_offset — half-hour and quarter-hour offsets
# ---------------------------------------------------------------------------

class TestParseTzOffset:

    def _parse(self, value):
        from coros_mcp.cache.utils import _parse_tz_offset
        return _parse_tz_offset(value)

    def test_integer_positive(self):
        assert self._parse("8") == timezone(timedelta(hours=8))

    def test_integer_negative(self):
        assert self._parse("-5") == timezone(timedelta(hours=-5))

    def test_float_half_hour(self):
        assert self._parse("5.5") == timezone(timedelta(hours=5, minutes=30))

    def test_float_negative(self):
        assert self._parse("-9.5") == timezone(timedelta(hours=-9, minutes=-30))

    def test_iso_positive(self):
        assert self._parse("+05:30") == timezone(timedelta(hours=5, minutes=30))

    def test_iso_negative(self):
        assert self._parse("-05:30") == timezone(timedelta(hours=-5, minutes=-30))

    def test_iso_nepal(self):
        """Nepal is UTC+5:45 — a real-world quarter-hour offset."""
        assert self._parse("+05:45") == timezone(timedelta(hours=5, minutes=45))

    def test_iso_no_sign(self):
        assert self._parse("5:30") == timezone(timedelta(hours=5, minutes=30))

    def test_invalid_raises(self):
        with pytest.raises((ValueError, TypeError)):
            self._parse("not-a-tz")


# ---------------------------------------------------------------------------
# 4. _today() — honours COROS_TIMEZONE
# ---------------------------------------------------------------------------

class TestTodayHonoursCOROSTIMEZONE:

    def _reload_utils_and_sync(self, tz_value=None):
        """Reload cache.utils (and cache.sync which imports LOCAL_TZ) with the given env."""
        env_patch = {}
        if tz_value is not None:
            env_patch["COROS_TIMEZONE"] = tz_value
        else:
            env_patch.pop("COROS_TIMEZONE", None)

        with patch.dict(os.environ, env_patch, clear=False):
            # Remove COROS_TIMEZONE if not set
            if tz_value is None:
                os.environ.pop("COROS_TIMEZONE", None)
            import coros_mcp.cache.sync as sync_mod
            import coros_mcp.cache.utils as utils_mod
            importlib.reload(utils_mod)
            importlib.reload(sync_mod)
            return sync_mod._today

    def test_today_utc_plus8_differs_from_utc_at_midnight(self):
        """At 23:30 UTC, UTC+8 is already the next calendar day."""
        from datetime import datetime
        # Freeze time to 2026-04-16 23:30 UTC
        fake_utc = datetime(2026, 4, 16, 23, 30, 0, tzinfo=UTC)

        _today = self._reload_utils_and_sync("8")

        with patch("coros_mcp.cache.sync.datetime") as mock_dt:
            mock_dt.now.side_effect = lambda tz=None: (
                fake_utc.astimezone(tz) if tz else fake_utc.replace(tzinfo=None)
            )
            result = _today()

        assert result == "20260417", (
            f"UTC+8 at 23:30 UTC should be next day, got {result}"
        )

    def test_today_no_timezone_uses_system_clock(self):
        """Without COROS_TIMEZONE, _today() calls datetime.now() without tz."""
        _today = self._reload_utils_and_sync(None)

        with patch("coros_mcp.cache.sync.datetime") as mock_dt:
            from datetime import datetime
            mock_dt.now.return_value = datetime(2026, 4, 16, 10, 0, 0)
            result = _today()

        assert result == "20260416"
        # Called without a tz argument
        mock_dt.now.assert_called_once_with()


# ---------------------------------------------------------------------------
# 5. cmd_sync argparse — unknown flags rejected; --help exits cleanly
# ---------------------------------------------------------------------------

class TestCmdSyncArgparse:

    def test_unknown_flag_raises_systemexit(self):
        with (
            patch.object(sys, "argv", ["coros-mcp", "sync", "--unknown-flag"]),
            pytest.raises(SystemExit) as exc_info,
        ):
            from coros_mcp import cli
            # Bypass auth by patching; we only care about arg parsing
            with patch("coros_mcp.cli.get_stored_auth", return_value=None), \
                 patch("coros_mcp.cli.try_auto_login", return_value=None):
                cli.cmd_sync()
        assert exc_info.value.code != 0

    def test_help_exits_zero(self):
        with patch.object(sys, "argv", ["coros-mcp", "sync", "--help"]), pytest.raises(SystemExit) as exc_info:
            from coros_mcp import cli
            cli.cmd_sync()
        assert exc_info.value.code == 0

    def test_valid_flags_parsed(self):
        """--from and --to must be accepted without error."""
        with patch.object(sys, "argv", ["coros-mcp", "sync", "--from", "20250101", "--to", "20250630"]), \
             patch("coros_mcp.cli.get_stored_auth", return_value=None), \
             patch("coros_mcp.cli.try_auto_login", return_value=None):
            from coros_mcp import cli
            result = cli.cmd_sync()
        # Returns 1 because auth fails, not because argparse rejected the flags
        assert result == 1


# ---------------------------------------------------------------------------
# 6. load_dotenv not called at import time
# ---------------------------------------------------------------------------

class TestLoadDotenvNotAtImport:

    def test_import_cli_does_not_call_load_dotenv(self):
        """Importing cli must not trigger load_dotenv — it belongs in main()."""
        # Remove cli from sys.modules so it re-imports cleanly
        sys.modules.pop("coros_mcp.cli", None)

        with patch("dotenv.load_dotenv") as mock_load:
            from coros_mcp import cli  # noqa: F401
            mock_load.assert_not_called()

    def test_main_calls_load_dotenv(self):
        """main() must call load_dotenv before dispatching."""
        from coros_mcp import cli

        with patch("dotenv.load_dotenv") as mock_load, \
             patch.object(sys, "argv", ["coros-mcp", "help"]), \
             patch("coros_mcp.cli.cmd_help", return_value=0), contextlib.suppress(SystemExit):
            cli.main()
        mock_load.assert_called_once()
