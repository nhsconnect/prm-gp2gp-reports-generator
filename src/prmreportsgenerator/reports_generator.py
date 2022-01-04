import boto3
import polars as pl
import pyarrow as pa

from prmreportsgenerator.config import PipelineConfig
from prmreportsgenerator.domain.count_outcomes_per_supplier_pathway import (
    count_outcomes_per_supplier_pathway,
)
from prmreportsgenerator.domain.reporting_window import MonthlyReportingWindow
from prmreportsgenerator.io.reports_io import ReportsIO, ReportsS3UriResolver
from prmreportsgenerator.io.s3 import S3DataManager


class ReportsGenerator:
    def __init__(self, config: PipelineConfig):
        s3 = boto3.resource("s3", endpoint_url=config.s3_endpoint_url)
        s3_manager = S3DataManager(s3)

        self._reporting_window = MonthlyReportingWindow.prior_to(config.date_anchor)

        output_metadata = {
            "reports-generator-version": config.build_tag,
            "date-anchor": config.date_anchor.isoformat(),
        }

        self._uris = ReportsS3UriResolver(
            transfer_data_bucket=config.input_transfer_data_bucket,
            reports_bucket=config.output_reports_bucket,
        )

        self._io = ReportsIO(
            s3_data_manager=s3_manager,
            output_metadata=output_metadata,
        )

    def _read_transfer_table(self, year_month) -> pa.Table:
        transfer_table_s3_uri = self._uris.transfer_data_uri(year_month)
        return self._io.read_transfers_as_table(transfer_table_s3_uri)

    @staticmethod
    def _count_outcomes_per_supplier_pathway(transfer_table: pa.Table):
        transfers_frame = pl.from_arrow(transfer_table)
        return count_outcomes_per_supplier_pathway(transfers_frame)

    def _write_supplier_pathway_outcome_counts(
        self, supplier_pathway_outcome_counts: pl.DataFrame, month
    ):
        self._io.write_outcome_counts(
            dataframe=supplier_pathway_outcome_counts,
            s3_uri=self._uris.supplier_pathway_outcome_counts(month),
        )

    def run(self):
        last_month = self._reporting_window.metric_month
        transfer_table = self._read_transfer_table(last_month)
        supplier_pathway_outcome_counts = self._count_outcomes_per_supplier_pathway(transfer_table)
        self._write_supplier_pathway_outcome_counts(supplier_pathway_outcome_counts, last_month)
