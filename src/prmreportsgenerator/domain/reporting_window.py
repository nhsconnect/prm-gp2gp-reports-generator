from typing import Optional


class ReportingWindow:
    def __init__(self, number_of_months: Optional[int], number_of_days: Optional[int]):

        if number_of_months and number_of_days:
            raise ValueError("Cannot have both number of months and number of days")
        if number_of_months is None and number_of_days is None:
            raise ValueError("Number of months or number of days must be specified")
