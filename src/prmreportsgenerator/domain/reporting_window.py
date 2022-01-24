class ReportingWindow:
    def __init__(self, number_of_months: int, number_of_days: int):

        if number_of_months and number_of_days:
            raise ValueError("Cannot have both number of months and number of days")
