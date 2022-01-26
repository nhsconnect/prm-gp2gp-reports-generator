from prmreportsgenerator.domain.reporting_windows.deprecated_monthly_reporting_window import (
    MonthlyReportingWindowDeprecated,
)
from tests.builders.common import a_datetime


def test_prior_to_correctly_determines_metric_month():
    moment = a_datetime(year=2021, month=3, day=4)

    reporting_window = MonthlyReportingWindowDeprecated.prior_to(moment)

    expected = 2021, 2

    actual = reporting_window.metric_month

    assert actual == expected


def test_prior_to_correctly_determines_metric_month_over_new_year():
    moment = a_datetime(year=2021, month=1, day=4)

    reporting_window = MonthlyReportingWindowDeprecated.prior_to(moment)

    expected = 2020, 12

    actual = reporting_window.metric_month

    assert actual == expected


def test_prior_to_correctly_determines_date_anchor_month():
    moment = a_datetime(year=2021, month=3, day=4)

    reporting_window = MonthlyReportingWindowDeprecated.prior_to(moment)

    expected = 2021, 3

    actual = reporting_window.date_anchor_month

    assert actual == expected
