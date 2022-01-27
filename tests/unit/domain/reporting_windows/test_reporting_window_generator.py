from datetime import datetime

import pytest
from dateutil.tz import UTC

from prmreportsgenerator.domain.reporting_windows.custom_reporting_window import (
    CustomReportingWindow,
)
from prmreportsgenerator.domain.reporting_windows.daily_reporting_window import DailyReportingWindow
from prmreportsgenerator.domain.reporting_windows.reporting_window_calculator import (
    ReportingWindowCalculator,
)
from tests.builders.common import a_datetime


def test_generates_custom_reporting_window_given_start_datetime_and_end_datetime():
    a_start_datetime = datetime(year=2021, month=12, day=30, tzinfo=UTC)
    a_end_datetime = datetime(year=2022, month=1, day=3, tzinfo=UTC)

    reporting_window = ReportingWindowCalculator.generate(
        start_datetime=a_start_datetime, end_datetime=a_end_datetime
    )

    assert isinstance(reporting_window, CustomReportingWindow)
    assert len(reporting_window.get_dates()) == 4


def test_generates_daily_reporting_window_given_number_of_days_and_cutoff_days():
    reporting_window = ReportingWindowCalculator.generate(number_of_days=2, cutoff_days=1)

    assert isinstance(reporting_window, DailyReportingWindow)
    assert len(reporting_window.get_dates()) == 2


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
