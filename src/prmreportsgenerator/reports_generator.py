from prmreportsgenerator.config import PipelineConfig
from prmreportsgenerator.domain.reporting_windows.custom_reporting_window import (
    CustomReportingWindow,
)
from prmreportsgenerator.domain.reporting_windows.daily_reporting_window import DailyReportingWindow
from prmreportsgenerator.domain.reporting_windows.monthly_reporting_window import (
    MonthlyReportingWindow,
)
from prmreportsgenerator.domain.reporting_windows.reporting_window import ReportingWindow


class ReportsGenerator:
    @staticmethod
    def create_reporting_window(config: PipelineConfig) -> ReportingWindow:
        if config.start_datetime and config.end_datetime is None:
            raise ValueError("End datetime must be provided if start datetime is provided")
        if config.start_datetime and config.end_datetime:
            return CustomReportingWindow(config.start_datetime, config.end_datetime)
        if config.number_of_days and config.cutoff_days:
            return DailyReportingWindow(config.number_of_days, config.cutoff_days)
        if config.number_of_months:
            return MonthlyReportingWindow(config.number_of_months)
        raise ValueError("Missing required config to generate reports. Please see README.")
