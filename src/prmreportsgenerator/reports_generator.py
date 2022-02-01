import logging

import boto3
import polars as pl
import pyarrow as pa

from prmreportsgenerator.config import PipelineConfig
from prmreportsgenerator.domain.count_outcomes_per_supplier_pathway import (
    count_outcomes_per_supplier_pathway,
)
from prmreportsgenerator.domain.reporting_windows.custom_reporting_window import (
    CustomReportingWindow,
)
from prmreportsgenerator.domain.reporting_windows.daily_reporting_window import DailyReportingWindow
from prmreportsgenerator.domain.reporting_windows.monthly_reporting_window import (
    MonthlyReportingWindow,
)
from prmreportsgenerator.domain.reporting_windows.reporting_window import ReportingWindow
from prmreportsgenerator.io.reports_io import ReportsIO, ReportsS3UriResolver
from prmreportsgenerator.io.s3 import S3DataManager
from prmreportsgenerator.utils.date_helpers import convert_to_datetime_string

logger = logging.getLogger(__name__)


class ReportsGenerator:
    def __init__(self, config: PipelineConfig):
        s3 = boto3.resource("s3", endpoint_url=config.s3_endpoint_url)
        s3_manager = S3DataManager(s3)

        self._reporting_window = self.create_reporting_window(config)
        self._cutoff_days = config.cutoff_days

        self._uri_resolver = ReportsS3UriResolver(
            transfer_data_bucket=config.input_transfer_data_bucket,
            reports_bucket=config.output_reports_bucket,
        )

        self._date_range_info_json = self._construct_date_range_info_json(config)
        output_metadata = {
            "reports-generator-version": config.build_tag,
            **self._date_range_info_json,
        }

        self._io = ReportsIO(s3_data_manager=s3_manager, output_metadata=output_metadata)

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

    def _read_transfer_table(self) -> pa.Table:
        transfer_data_s3_uris = self._uri_resolver.input_transfer_data_uris(
            reporting_window=self._reporting_window, cutoff_days=self._cutoff_days
        )

        logger.info(
            "Attempting to read from the following transfer data S3 Uris",
            extra={
                "event": "ATTEMPTING_TO_READ_FROM_TRANSFER_DATA_S3_URIS",
                "transfer_data_s3_uris": transfer_data_s3_uris,
            },
        )

        return self._io.read_transfers_as_table(transfer_data_s3_uris)

    def _write_table(self, table: pa.Table):
        date = self._reporting_window.start_datetime
        output_table_uri = self._uri_resolver.output_table_uri(
            date=date, supplement_s3_key=self._reporting_window.config_string
        )
        self._io.write_table(
            table=table,
            s3_uri=output_table_uri,
        )

    @staticmethod
    def _count_outcomes_per_supplier_pathway(transfer_table: pa.Table) -> pa.Table:
        transfers_frame = pl.from_arrow(transfer_table)
        processed_transfers = count_outcomes_per_supplier_pathway(transfers_frame)
        return processed_transfers.to_arrow()

    def _construct_date_range_info_json(self, config: PipelineConfig) -> dict:
        return {
            "config-cutoff-days": str(config.cutoff_days),
            "config-number-of-months": str(config.number_of_months),
            "config-number-of-days": str(config.number_of_days),
            "config-start-datetime": convert_to_datetime_string(config.start_datetime),
            "config-end-datetime": convert_to_datetime_string(config.end_datetime),
            "reporting-window-start-datetime": convert_to_datetime_string(
                self._reporting_window.start_datetime
            ),
            "reporting-window-end-datetime": convert_to_datetime_string(
                self._reporting_window.end_datetime
            ),
        }

    def run(self):
        transfer_table = self._read_transfer_table()
        logger.info(
            "Attempting to produce supplier pathway outcome counts for transfers in date range",
            extra={
                "event": "ATTEMPTING_TO_PRODUCE_SUPPLIER_PATHWAY_OUTCOME_COUNTS",
                **self._date_range_info_json,
            },
        )
        supplier_pathway_outcome_counts = self._count_outcomes_per_supplier_pathway(transfer_table)

        logger.info(
            "Successfully produced supplier pathway outcome counts for transfers in date range",
            extra={
                "event": "PRODUCED_SUPPLIER_PATHWAY_OUTCOME_COUNTS",
                **self._date_range_info_json,
            },
        )
        self._write_table(supplier_pathway_outcome_counts)
