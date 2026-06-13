"""Tests for coros_mcp/cache/utils.py display/timezone helpers."""

from datetime import timedelta, timezone

from coros_mcp.cache import utils

TZ_PLUS8 = timezone(timedelta(hours=8))


class TestParseTzOffset:
    def test_iso_style_with_minutes(self):
        assert utils._parse_tz_offset("+05:30").utcoffset(None) == timedelta(hours=5, minutes=30)

    def test_negative_iso_style(self):
        assert utils._parse_tz_offset("-05:30").utcoffset(None) == timedelta(hours=-5, minutes=-30)

    def test_integer_hours(self):
        assert utils._parse_tz_offset("8").utcoffset(None) == timedelta(hours=8)

    def test_float_hours(self):
        assert utils._parse_tz_offset("5.5").utcoffset(None) == timedelta(hours=5, minutes=30)

    def test_negative_float_hours(self):
        assert utils._parse_tz_offset("-5.75").utcoffset(None) == timedelta(hours=-5, minutes=-45)


class TestFmtLocalTime:
    def test_formats_with_fixed_tz(self, monkeypatch):
        monkeypatch.setattr(utils, "LOCAL_TZ", TZ_PLUS8)
        # 2025-03-16 07:02:03 in +8 (the docstring example).
        assert utils.fmt_local_time("1742079723") == "2025-03-16 07:02:03"

    def test_falls_back_to_system_tz(self, monkeypatch):
        from datetime import datetime
        monkeypatch.setattr(utils, "LOCAL_TZ", None)
        expected = datetime.fromtimestamp(1742079723).strftime("%Y-%m-%d %H:%M:%S")
        assert utils.fmt_local_time("1742079723") == expected

    def test_non_numeric_returned_unchanged(self):
        assert utils.fmt_local_time("not-a-number") == "not-a-number"

    def test_none_returns_none(self):
        assert utils.fmt_local_time(None) is None
