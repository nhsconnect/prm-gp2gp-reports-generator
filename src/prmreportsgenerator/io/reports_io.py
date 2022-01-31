import logging
from datetime import datetime
from typing import Dict, List

import polars as pl
import pyarrow as pa

from prmreportsgenerator.domain.reporting_windows.reporting_window import ReportingWindow
from prmreportsgenerator.io.s3 import S3DataManager
from prmreportsgenerator.utils.add_leading_zero import add_leading_zero

logger = logging.getLogger(__name__)


class ReportsS3UriResolver:
    _TRANSFER_DATA_FILE_NAME = "transfers.parquet"
    _TRANSFER_DATA_VERSION = "v7"
    _SUPPLIER_PATHWAY_OUTCOME_COUNTS_FILE_NAME = "supplier_pathway_outcome_counts.csv"
    _REPORTS_VERSION = "v2"

    def __init__(
        self,
        transfer_data_bucket: str,
        reports_bucket: str,
    ):
        self._transfer_data_bucket = transfer_data_bucket
        self._reports_bucket = reports_bucket

    @staticmethod
    def _s3_path(*fragments):
        return "s3://" + "/".join(fragments)

    @staticmethod
    def _filepath(date: datetime, filename: str) -> str:
        year = add_leading_zero(date.year)
        month = add_leading_zero(date.month)
        day = add_leading_zero(date.day)
        return f"{year}-{month}-{day}-{filename}"

    def input_transfer_data_uris(
        self, reporting_window: ReportingWindow, cutoff_days: int
    ) -> List[str]:
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
            for date in reporting_window.get_dates()
        ]

    def output_supplier_pathway_outcome_counts_uri(
        self, date: datetime, supplement_s3_key: str
    ) -> str:
        return self._s3_path(
            self._reports_bucket,
            self._REPORTS_VERSION,
            supplement_s3_key,
            f"{add_leading_zero(date.year)}",
            f"{add_leading_zero(date.month)}",
            f"{add_leading_zero(date.day)}",
            self._filepath(date, self._SUPPLIER_PATHWAY_OUTCOME_COUNTS_FILE_NAME),
        )


class ReportsIO:
    def __init__(
        self,
        s3_data_manager: S3DataManager,
        output_metadata: Dict[str, str],
    ):
        self._s3_manager = s3_data_manager
        self._output_metadata = output_metadata

    def read_transfers_as_table(self, s3_uris: List[str]) -> pa.Table:
        return pa.concat_tables(
            [self._s3_manager.read_parquet(s3_path) for s3_path in s3_uris],
        )

    def write_outcome_counts(self, dataframe: pl.DataFrame, s3_uri: str):
        self._s3_manager.write_dataframe_to_csv(
            object_uri=s3_uri, dataframe=dataframe, metadata=self._output_metadata
        )
