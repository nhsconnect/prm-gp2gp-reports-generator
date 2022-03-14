import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional

import pyarrow as pa

from prmreportsgenerator.domain.reporting_windows.reporting_window import \
    ReportingWindow
from prmreportsgenerator.io.s3 import S3DataManager
from prmreportsgenerator.report_name import ReportName
from prmreportsgenerator.utils.add_leading_zero import add_leading_zero

logger = logging.getLogger(__name__)


class ReportsS3UriResolver:
    _TRANSFER_DATA_FILE_NAME = "transfers.parquet"
    _TRANSFER_DATA_VERSION = "v8"
    _EXTENSION = ".csv"
    _REPORTS_VERSION = "v3"

    def __init__(self, transfer_data_bucket: str, reports_bucket: str, report_name: ReportName):
        self._transfer_data_bucket = transfer_data_bucket
        self._reports_bucket = reports_bucket
        self._report_name = report_name.value

    @staticmethod
    def _s3_path(*fragments):
        return "s3://" + "/".join(fragments)

    @staticmethod
    def _filepath(start_date: datetime, filename: str, end_date: Optional[datetime] = None) -> str:
        start_year = add_leading_zero(start_date.year)
        start_month = add_leading_zero(start_date.month)
        start_day = add_leading_zero(start_date.day)

        if end_date:
            end_year = add_leading_zero(end_date.year)
            end_month = add_leading_zero(end_date.month)
            end_day = add_leading_zero(end_date.day)
            return (
                f"{start_year}-{start_month}-{start_day}-to-"
                f"{end_year}-{end_month}-{end_day}-{filename}"
            )

        return f"{start_year}-{start_month}-{start_day}-{filename}"

    def input_transfer_data_uris(
        self, reporting_window: ReportingWindow, cutoff_days: int
    ) -> List[str]:
        return [
            self._s3_path(
                self._transfer_data_bucket,
                self._TRANSFER_DATA_VERSION,
                f"cutoff-{cutoff_days}",
                f"{add_leading_zero(start_date.year)}",
                f"{add_leading_zero(start_date.month)}",
                f"{add_leading_zero(start_date.day)}",
                self._filepath(start_date=start_date, filename=self._TRANSFER_DATA_FILE_NAME),
            )
            for start_date in reporting_window.get_dates()
        ]

    def output_table_uri(
        self, start_date: datetime, end_date: datetime, supplement_s3_key: str
    ) -> str:
        filename = self._report_name.lower() + self._EXTENSION
        actual_end_date = end_date - timedelta(
            days=1
        )  # data is at until midnight, so the actual data is forp[;- the previous day

        return self._s3_path(
            self._reports_bucket,
            self._REPORTS_VERSION,
            supplement_s3_key,
            f"{add_leading_zero(start_date.year)}",
            f"{add_leading_zero(start_date.month)}",
            f"{add_leading_zero(start_date.day)}",
            self._filepath(start_date=start_date, end_date=actual_end_date, filename=filename),
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

    def write_table(self, table: pa.Table, s3_uri: str):
        self._s3_manager.write_table_to_csv(
            object_uri=s3_uri, table=table, metadata=self._output_metadata
        )
