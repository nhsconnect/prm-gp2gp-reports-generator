import logging
from typing import Dict

import polars as pl
import pyarrow as pa

from prmreportsgenerator.domain.reporting_window import YearMonth
from prmreportsgenerator.io.s3 import S3DataManager

logger = logging.getLogger(__name__)


class ReportsS3UriResolver:
    _SUPPLIER_PATHWAY_OUTCOME_COUNTS_FILE_NAME = "supplier_pathway_outcome_counts.csv"
    _TRANSFER_DATA_FILE_NAME = "transfers.parquet"
    _TRANSFER_DATA_VERSION = "v6"
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
            self._TRANSFER_DATA_VERSION,
            f"{year}/{month}",
            f"{year}-{month}-{self._TRANSFER_DATA_FILE_NAME}",
        )


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
