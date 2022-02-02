import polars as pl
import pyarrow as pa
from polars import col

from prmreportsgenerator.domain.reports_generator.reports_generator import ReportsGenerator


class TransferLevelTechnicalFailuresReportsGenerator(ReportsGenerator):
    def __init__(self, transfers: pa.Table):
        super().__init__()
        self._transfers = transfers

    def generate(self) -> pa.Table:
        transfers_frame = pl.from_arrow(self._transfers)
        processed_transfers = transfers_frame.select(  # type: ignore
            [
                col("requesting_supplier").alias("requesting supplier"),
                col("sending_supplier").alias("sending supplier"),
                col("status"),
                col("failure_reason").alias("failure reason"),
                col("final_error_codes").apply(self._unique_errors).alias("unique final errors"),
                col("sender_error_codes").apply(self._unique_errors).alias("unique sender errors"),
                col("intermediate_error_codes")
                .apply(self._unique_errors)
                .alias("unique intermediate errors"),
            ]
        ).to_dict()

        return pa.table(processed_transfers)
