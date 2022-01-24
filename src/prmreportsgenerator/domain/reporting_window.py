from datetime import datetime, time, timedelta
from typing import Optional

from dateutil.relativedelta import relativedelta
from dateutil.tz import UTC


class ReportingWindow:
    def __init__(
        self,
        start_datetime: Optional[datetime],
        number_of_months: Optional[int],
        number_of_days: Optional[int],
    ):
        self._validate_datetime_is_at_midnight(start_datetime)

        if number_of_months and number_of_days:
            raise ValueError("Cannot have both number of months and number of days")
        if number_of_months is None and number_of_days is None:
            raise ValueError("Number of months or number of days must be specified")

        self._number_of_days: int = number_of_days or 0
        self._number_of_months: int = number_of_months or 0
        self._start_datetime = self._calculate_start_datetime(start_datetime)

    @staticmethod
    def _validate_datetime_is_at_midnight(a_datetime: Optional[datetime]):
        midnight = time(hour=0, minute=0, second=0)
        if a_datetime and a_datetime.time() != midnight:
            raise ValueError("Datetime must be at midnight")

    @staticmethod
    def _calculate_today_midnight_datetime() -> datetime:
        today = datetime.now(UTC).date()
        today_midnight_utc = datetime.combine(today, time.min, tzinfo=UTC)
        return today_midnight_utc

    def _calculate_start_datetime(self, start_datetime: Optional[datetime]):
        if start_datetime:
            return start_datetime
        today_midnight_datetime = self._calculate_today_midnight_datetime()
        if self._number_of_months > 0:
            return today_midnight_datetime - relativedelta(months=self._number_of_months)
        else:
            return today_midnight_datetime - timedelta(days=self._number_of_days)

    def get_start_datetime(self) -> datetime:
        return self._start_datetime
