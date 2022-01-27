import logging
from datetime import datetime
from typing import Optional

from prmreportsgenerator.domain.reporting_windows.custom_reporting_window import (
    CustomReportingWindow,
)
from prmreportsgenerator.domain.reporting_windows.reporting_window import ReportingWindow

logger = logging.getLogger(__name__)


class ReportingWindowCalculator:
    @classmethod
    def generate(
        cls, start_datetime: Optional[datetime], end_datetime: Optional[datetime]
    ) -> ReportingWindow:
        if start_datetime and end_datetime:
            return CustomReportingWindow(start_datetime, end_datetime)

        raise ValueError("Missing required config to generate reports")
