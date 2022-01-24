import pytest

from prmreportsgenerator.domain.reporting_window import ReportingWindow


def test_throws_value_error_given_both_number_of_days_and_months():
    with pytest.raises(ValueError) as e:
        ReportingWindow(number_of_months=1, number_of_days=1)
    assert str(e.value) == "Cannot have both number of months and number of days"


def test_throws_value_error_given_no_number_of_days_and_months():
    with pytest.raises(ValueError) as e:
        ReportingWindow(number_of_months=None, number_of_days=None)
    assert str(e.value) == "Number of months or number of days must be specified"
