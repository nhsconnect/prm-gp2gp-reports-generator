from datetime import datetime

import pytest
from dateutil.tz import UTC

from prmreportsgenerator.domain.reporting_windows.custom_reporting_window import (
    CustomReportingWindow,
)
from prmreportsgenerator.domain.reporting_windows.reporting_window_calculator import (
    ReportingWindowCalculator,
)
from tests.builders.common import a_datetime


def test_generates_daily_reporting_window_given_start_datetime_and_end_datetime():
    start_datetime = datetime(year=2021, month=12, day=30, tzinfo=UTC)
    end_datetime = datetime(year=2022, month=1, day=3, tzinfo=UTC)

    reporting_window = ReportingWindowCalculator.generate(start_datetime, end_datetime)

    actual = reporting_window

    assert isinstance(actual, CustomReportingWindow)
    assert actual.start_datetime == start_datetime
    assert actual.end_datetime == end_datetime
    assert len(actual.get_dates()) == 4


@pytest.mark.parametrize(
    "start_datetime, end_datetime",
    [
        (None, None),
        (None, a_datetime()),
        (a_datetime(), None),
    ],
)
def test_throws_if_missing_valid_inputs(start_datetime, end_datetime):
    with pytest.raises(ValueError) as e:
        ReportingWindowCalculator.generate(start_datetime, end_datetime)
    assert str(e.value) == "Missing required config to generate reports"
