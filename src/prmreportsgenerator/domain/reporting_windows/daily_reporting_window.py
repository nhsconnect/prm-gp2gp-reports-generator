from datetime import datetime, time, timedelta
from dateutil.tz import UTC

from prmreportsgenerator.domain.reporting_windows.reporting_window import ReportingWindow


class DailyReportingWindow(ReportingWindow):
    def __init__(self, number_of_days: int, cutoff_days: int):
        self._number_of_days = number_of_days
        self._cutoff_days = cutoff_days
        super().__init__(self.start_datetime, self.end_datetime)

    @staticmethod
    def _calculate_today_midnight_datetime() -> datetime:
        today = datetime.now(UTC).date()
        today_midnight_utc = datetime.combine(today, time.min, tzinfo=UTC)
        return today_midnight_utc

    @property
    def start_datetime(self) -> datetime:
        today_midnight_datetime = self._calculate_today_midnight_datetime()
        return today_midnight_datetime - timedelta(days=self._number_of_days + self._cutoff_days)

    @property
    def end_datetime(self) -> datetime:
        return self.start_datetime + timedelta(days=self._number_of_days)
