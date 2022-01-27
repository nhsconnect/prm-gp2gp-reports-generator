from datetime import datetime

from dateutil.relativedelta import relativedelta
from dateutil.tz import UTC

from prmreportsgenerator.domain.reporting_windows.reporting_window import ReportingWindow
from prmreportsgenerator.utils.date_helpers import calculate_today_midnight_datetime


class MonthlyReportingWindow(ReportingWindow):
    def __init__(self, cutoff_days, number_of_months):
        self._cutoff_days = cutoff_days
        self._number_of_months = number_of_months

        super().__init__(self.start_datetime, None)

    @property
    def start_datetime(self) -> datetime:
        today_datetime = calculate_today_midnight_datetime()
        current_month_start_datetime = datetime(
            today_datetime.year, today_datetime.month, 1, tzinfo=UTC
        )
        return current_month_start_datetime - relativedelta(months=1)
