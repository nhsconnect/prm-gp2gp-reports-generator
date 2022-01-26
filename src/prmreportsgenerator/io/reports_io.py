import logging
from datetime import datetime, timedelta
from typing import Dict, List

import polars as pl
import pyarrow as pa

from prmreportsgenerator.domain.reporting_windows.deprecated_monthly_reporting_window import (
    YearMonth,
)
from prmreportsgenerator.io.s3 import S3DataManager
from prmreportsgenerator.utils.add_leading_zero import add_leading_zero

logger = logging.getLogger(__name__)


class ReportsS3UriResolver:
    _SUPPLIER_PATHWAY_OUTCOME_COUNTS_FILE_NAME = "supplier_pathway_outcome_counts.csv"
    _TRANSFER_DATA_FILE_NAME = "transfers.parquet"
    _TRANSFER_DATA_VERSION_DEPRECATED = "v6"
    _TRANSFER_DATA_VERSION = "v7"
    _DEFAULT_REPORTS_VERSION = "v1"

    def __init__(
        self,
        transfer_data_bucket: str,
        reports_bucket: str,
    ):
        self._transfer_data_bucket = transfer_data_bucket
        self._reports_bucket = reports_bucket
        self._reports_version = self._DEFAULT_REPORTS_VERSION

    @staticmethod
    def _s3_path(*fragments):
        return "s3://" + "/".join(fragments)

    def supplier_pathway_outcome_counts(self, year_month: YearMonth) -> str:
        year, month = year_month
        return self._s3_path(
            self._reports_bucket,
            self._reports_version,
            f"{year}/{month}",
            f"{year}-{month}-{self._SUPPLIER_PATHWAY_OUTCOME_COUNTS_FILE_NAME}",
        )

    def transfer_data_uri(self, year_month: YearMonth) -> str:
        year, month = year_month
        return self._s3_path(
            self._transfer_data_bucket,
            self._TRANSFER_DATA_VERSION_DEPRECATED,
            f"{year}/{month}",
            f"{year}-{month}-{self._TRANSFER_DATA_FILE_NAME}",
        )

    @staticmethod
    def _filepath(date: datetime, filename: str) -> str:
        year = add_leading_zero(date.year)
        month = add_leading_zero(date.month)
        day = add_leading_zero(date.day)
        return f"{year}-{month}-{day}-{filename}"

    def transfer_data_uris(
        self, start_datetime: datetime, end_datetime: datetime, cutoff_days: int
    ) -> List[str]:
        if start_datetime > end_datetime:
            raise ValueError("Start datetime must be before end datetime")

        delta = end_datetime - start_datetime
        dates = [start_datetime + timedelta(days=days) for days in range(delta.days)]

        return [
            self._s3_path(
                self._transfer_data_bucket,
                self._TRANSFER_DATA_VERSION,
                f"cutoff-{cutoff_days}",
                f"{add_leading_zero(date.year)}",
                f"{add_leading_zero(date.month)}",
                f"{add_leading_zero(date.day)}",
                self._filepath(date, self._TRANSFER_DATA_FILE_NAME),
            )
            for date in dates
        ]


class ReportsIO:
    def __init__(
        self,
        s3_data_manager: S3DataManager,
        output_metadata: Dict[str, str],
    ):
        self._s3_manager = s3_data_manager
        self._output_metadata = output_metadata

    def read_transfers_as_table(self, s3_uri: str) -> pa.Table:
        return self._s3_manager.read_parquet(s3_uri)

    def write_outcome_counts(self, dataframe: pl.DataFrame, s3_uri: str):
        self._s3_manager.write_dataframe_to_csv(
            object_uri=s3_uri, dataframe=dataframe, metadata=self._output_metadata
        )
