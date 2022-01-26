from datetime import datetime
from typing import Tuple

from dateutil.relativedelta import relativedelta
from dateutil.tz import tzutc

YearNumber = int
MonthNumber = int
YearMonth = Tuple[YearNumber, MonthNumber]


class MonthlyReportingWindowDeprecated:
    def __init__(
        self,
        date_anchor_month_start: datetime,
        metric_month_start: datetime,
    ):
        self._date_anchor_month_start = date_anchor_month_start
        self._metric_month_start = metric_month_start

    @classmethod
    def prior_to(cls, date_anchor: datetime):
        date_anchor_month_start = datetime(date_anchor.year, date_anchor.month, 1, tzinfo=tzutc())
        metric_month_start = date_anchor_month_start - relativedelta(months=1)
        return cls(date_anchor_month_start, metric_month_start)

    @property
    def metric_month(self) -> YearMonth:
        month = self._metric_month_start
        return month.year, month.month

    @property
    def date_anchor_month(self) -> YearMonth:
        month = self._date_anchor_month_start
        return month.year, month.month
