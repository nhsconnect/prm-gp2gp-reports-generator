import logging
from datetime import datetime
from typing import Optional

from prmreportsgenerator.domain.reporting_windows.custom_reporting_window import (
    CustomReportingWindow,
)
from prmreportsgenerator.domain.reporting_windows.daily_reporting_window import DailyReportingWindow
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
    ) -> ReportingWindow:
        if start_datetime and end_datetime:
            return CustomReportingWindow(start_datetime, end_datetime)
        if number_of_days and cutoff_days:
            return DailyReportingWindow(number_of_days, cutoff_days)
        raise ValueError("Missing required config to generate reports")
