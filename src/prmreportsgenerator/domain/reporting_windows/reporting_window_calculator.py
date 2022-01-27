import logging
from datetime import datetime
from typing import Optional

from prmreportsgenerator.domain.reporting_windows.custom_reporting_window import (
    CustomReportingWindow,
)
from prmreportsgenerator.domain.reporting_windows.daily_reporting_window import DailyReportingWindow
from prmreportsgenerator.domain.reporting_windows.monthly_reporting_window import (
    MonthlyReportingWindow,
)
from prmreportsgenerator.domain.reporting_windows.reporting_window import ReportingWindow

logger = logging.getLogger(__name__)


class ReportingWindowCalculator:
    @classmethod
    def generate(
        cls,
        start_datetime: Optional[datetime] = None,
        end_datetime: Optional[datetime] = None,
        number_of_days: Optional[int] = None,
        cutoff_days: Optional[int] = None,
        number_of_months: Optional[int] = None,
    ) -> ReportingWindow:
        if start_datetime and end_datetime is None:
            raise ValueError("End datetime must be provided if start datetime is provided")
        if start_datetime and end_datetime:
            return CustomReportingWindow(start_datetime, end_datetime)
        if number_of_days and cutoff_days:
            return DailyReportingWindow(number_of_days, cutoff_days)
        if number_of_months:
            return MonthlyReportingWindow(number_of_months)
        raise ValueError("Missing required config to generate reports. Please see README.")
