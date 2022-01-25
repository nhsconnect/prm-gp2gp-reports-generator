from datetime import datetime

import pytest
from dateutil.tz import UTC

from prmreportsgenerator.domain.reporting_window import ReportingWindow
from tests.builders.common import a_datetime


def test_throws_value_error_given_start_datetime_not_at_midnight():
    start_datetime = a_datetime(year=2020, hour=5, minute=0, second=0)

    with pytest.raises(ValueError) as e:
        ReportingWindow(
            start_datetime=start_datetime,
            end_datetime=a_datetime(year=2021, hour=0, minute=0, second=0),
        )
    assert str(e.value) == "Datetime must be at midnight"


def test_throws_value_error_given_end_datetime_not_at_midnight():
    end_datetime = a_datetime(year=2022, hour=5, minute=0, second=0)

    with pytest.raises(ValueError) as e:
        ReportingWindow(
            start_datetime=a_datetime(year=2021, hour=0, minute=0, second=0),
            end_datetime=end_datetime,
        )
    assert str(e.value) == "Datetime must be at midnight"


def test_throws_value_error_given_end_datetime_but_no_start_datetime():
    end_datetime = datetime(year=2019, month=12, day=31, hour=0, minute=0, second=0, tzinfo=UTC)

    with pytest.raises(ValueError) as e:
        ReportingWindow(start_datetime=None, end_datetime=end_datetime)
    assert str(e.value) == "Start datetime must be provided if end datetime is provided"


def test_throws_value_error_given_start_datetime_but_no_end_datetime():
    start_datetime = datetime(year=2019, month=12, day=31, hour=0, minute=0, second=0, tzinfo=UTC)

    with pytest.raises(ValueError) as e:
        ReportingWindow(
            start_datetime=start_datetime,
            end_datetime=None,
        )
    assert str(e.value) == "End datetime must be provided if start datetime is provided"


def test_throws_value_error_given_start_datetime_is_after_end_datetime():
    start_datetime = datetime(year=2019, month=12, day=2, hour=0, minute=0, second=0, tzinfo=UTC)
    end_datetime = datetime(year=2019, month=12, day=1, hour=0, minute=0, second=0, tzinfo=UTC)

    with pytest.raises(ValueError) as e:
        ReportingWindow(
            start_datetime=start_datetime,
            end_datetime=end_datetime,
        )
    assert str(e.value) == "Start datetime must be before end datetime"


def test_returns_start_datetime_given_start_datetime():
    start_datetime = datetime(year=2019, month=12, day=1, hour=0, minute=0, second=0, tzinfo=UTC)

    reporting_window = ReportingWindow(
        start_datetime=start_datetime,
        end_datetime=a_datetime(year=2020, hour=0, minute=0, second=0),
    )
    actual = reporting_window.start_datetime

    assert actual == start_datetime


def test_dates_property_returns_list_of_datetimes_within_start_and_end_datetime():
    start_datetime = datetime(year=2021, month=12, day=30, tzinfo=UTC)
    end_datetime = datetime(year=2022, month=1, day=3, tzinfo=UTC)

    reporting_window = ReportingWindow(start_datetime, end_datetime)

    expected = [
        datetime(year=2021, month=12, day=30, tzinfo=UTC),
        datetime(year=2021, month=12, day=31, tzinfo=UTC),
        datetime(year=2022, month=1, day=1, tzinfo=UTC),
        datetime(year=2022, month=1, day=2, tzinfo=UTC),
    ]

    actual = reporting_window.dates

    assert actual == expected
