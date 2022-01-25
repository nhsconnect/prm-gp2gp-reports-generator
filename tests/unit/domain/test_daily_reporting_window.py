from datetime import datetime

import pytest
from dateutil.tz import UTC
from freezegun import freeze_time

from prmreportsgenerator.domain.daily_reporting_window import DailyReportingWindow


@pytest.mark.parametrize(
    "number_of_days, expected_start_datetime",
    [
        (1, datetime(year=2020, month=12, day=31, hour=0, minute=0, second=0, tzinfo=UTC)),
        (3, datetime(year=2020, month=12, day=29, hour=0, minute=0, second=0, tzinfo=UTC)),
        (10, datetime(year=2020, month=12, day=22, hour=0, minute=0, second=0, tzinfo=UTC)),
    ],
)
@freeze_time(datetime(year=2021, month=1, day=1, hour=3, minute=0, second=0, tzinfo=UTC))
def test_returns_today_midnight_minus_days_given_number_of_days_and_0_cutoff(
    number_of_days, expected_start_datetime
):
    reporting_window = DailyReportingWindow(number_of_days=number_of_days, cutoff_days=0)
    actual = reporting_window.start_datetime

    assert actual == expected_start_datetime


@pytest.mark.parametrize(
    "cutoff_days, expected_start_datetime",
    [
        (1, datetime(year=2020, month=12, day=30, hour=0, minute=0, second=0, tzinfo=UTC)),
        (3, datetime(year=2020, month=12, day=28, hour=0, minute=0, second=0, tzinfo=UTC)),
        (10, datetime(year=2020, month=12, day=21, hour=0, minute=0, second=0, tzinfo=UTC)),
    ],
)
@freeze_time(datetime(year=2021, month=1, day=1, hour=3, minute=0, second=0, tzinfo=UTC))
def test_returns_today_midnight_minus_days_given_1_number_of_days_and_various_cutoffs(
    cutoff_days, expected_start_datetime
):
    reporting_window = DailyReportingWindow(number_of_days=1, cutoff_days=cutoff_days)
    actual = reporting_window.start_datetime

    assert actual == expected_start_datetime
