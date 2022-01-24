from datetime import datetime, time
from typing import Optional


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

    @staticmethod
    def _validate_datetime_is_at_midnight(a_datetime: Optional[datetime]):
        midnight = time(hour=0, minute=0, second=0)
        if a_datetime and a_datetime.time() != midnight:
            raise ValueError("Datetime must be at midnight")
