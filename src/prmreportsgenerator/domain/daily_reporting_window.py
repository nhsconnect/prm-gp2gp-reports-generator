from datetime import datetime, time, timedelta
from typing import List

from dateutil.tz import UTC

from prmreportsgenerator.utils.date_converter import convert_date_range_to_dates


class DailyReportingWindow:
    def __init__(self, number_of_days: int, cutoff_days: int):
        self._number_of_days = number_of_days
        self._cutoff_days = cutoff_days

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
    def dates(self) -> List[datetime]:
        end_datetime = self.start_datetime + timedelta(days=self._number_of_days)
        return convert_date_range_to_dates(self.start_datetime, end_datetime)
