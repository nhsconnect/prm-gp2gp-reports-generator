from datetime import datetime

from dateutil.tz import UTC
from freezegun import freeze_time

from prmreportsgenerator.domain.reporting_windows.monthly_reporting_window import (
    MonthlyReportingWindow,
)
from tests.builders.common import a_datetime


@freeze_time(a_datetime(year=2022, month=1, day=1))
def test_property_given_start_datetime():
    reporting_window = MonthlyReportingWindow(cutoff_days=1, number_of_months=1)

    actual_start_datetime = reporting_window.start_datetime

    expected_start_datetime = datetime(
        year=2021, month=12, day=1, hour=0, minute=0, second=0, tzinfo=UTC
    )

    assert actual_start_datetime == expected_start_datetime
