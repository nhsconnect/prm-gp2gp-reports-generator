from datetime import datetime
from typing import List

from prmreportsgenerator.utils.date_helpers import convert_date_range_to_dates


class ReportingWindow:
    def __init__(self, start_datetime: datetime, end_datetime: datetime):
        self._start_datetime = start_datetime
        self._end_datetime = end_datetime

    @property
    def start_datetime(self) -> datetime:
        return self._start_datetime

    def get_dates(self) -> List[datetime]:
        return convert_date_range_to_dates(self._start_datetime, self._end_datetime)
