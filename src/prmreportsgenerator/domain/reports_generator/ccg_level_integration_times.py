import polars as pl
import pyarrow as pa
from polars import DataFrame, col, count

from prmreportsgenerator.domain.reports_generator.reports_generator import ReportsGenerator


class CCGLevelIntegrationTimesReportsGenerator(ReportsGenerator):
    def __init__(self, transfers: pa.Table):
        super().__init__()
        self._transfers = transfers

    def _rename_columns(self, transfer_dataframe: DataFrame) -> DataFrame:
        return (
            transfer_dataframe.with_columns(
                [
                    col("requesting_practice_ccg_name").alias("CCG name"),
                    col("requesting_practice_ccg_ods_code").alias("CCG ODS"),
                    col("requesting_practice_name").alias("Requesting practice name"),
                    col("requesting_practice_ods_code").alias("Requesting practice ODS"),
                ]
            )
            .groupby(["Requesting practice name", "CCG name", "CCG ODS", "Requesting practice ODS"])
            .agg([count("conversation_id").alias("GP2GP Transfers received")])
        )

    def generate(self) -> pa.Table:
        transfers_frame = pl.from_arrow(self._transfers)
        processed_transfers = self._process(transfers_frame, self._rename_columns).to_dict()
        return pa.table(processed_transfers)
