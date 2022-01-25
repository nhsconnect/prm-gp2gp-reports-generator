from datetime import datetime, time
from typing import List, Optional

from prmreportsgenerator.utils.date_converter import convert_date_range_to_dates


class ReportingWindow:
    def __init__(
        self,
        start_datetime: Optional[datetime],
        end_datetime: Optional[datetime],
    ):
        self._validate_datetimes(start_datetime, end_datetime)
        self._start_datetime = start_datetime
        if start_datetime and end_datetime:
            self._dates = convert_date_range_to_dates(start_datetime, end_datetime)

    def _validate_datetimes(
        self, start_datetime: Optional[datetime], end_datetime: Optional[datetime]
    ):
        if start_datetime is None and end_datetime:
            raise ValueError("Start datetime must be provided if end datetime is provided")
        if end_datetime is None and start_datetime:
            raise ValueError("End datetime must be provided if start datetime is provided")

        self._validate_datetime_is_at_midnight(start_datetime)
        self._validate_datetime_is_at_midnight(end_datetime)

    @staticmethod
    def _validate_datetime_is_at_midnight(a_datetime: Optional[datetime]):
        midnight = time(hour=0, minute=0, second=0)
        if a_datetime and a_datetime.time() != midnight:
            raise ValueError("Datetime must be at midnight")

    @property
    def start_datetime(self) -> Optional[datetime]:
        return self._start_datetime

    @property
    def dates(self) -> List[datetime]:
        return self._dates
