import polars as pl
import pyarrow as pa
from polars import DataFrame, col, count

from src.prmreportsgenerator.domain.reports_generator.reports_generator import ReportsGenerator


class TransferDetailsPerHourReportsGenerator(ReportsGenerator):
    def __init__(self, transfers: pa.Table):
        super().__init__()
        self._transfers = transfers

    def _create_hour_column(self, transfer_dataframe: DataFrame) -> DataFrame:
        date_requested_by_hour = col("date_requested").dt.strftime("%y/%m/%d %H:00")

        return transfer_dataframe.with_column(date_requested_by_hour.alias("Date/Time"))

    def _group_by_date_requested_hourly(self, transfer_dataframe: DataFrame) -> DataFrame:
        return (
            transfer_dataframe.groupby(["Date/Time"])
            .agg([count("conversation_id").alias("Total number of transfers")])
            .sort("Date/Time")
        )

    def generate(self) -> pa.Table:
        transfers_frame = pl.from_arrow(self._transfers)
        processed_transfers = self._process(
            transfers_frame,
            self._create_hour_column,
            self._group_by_date_requested_hourly,
        ).to_dict()
        return pa.table(processed_transfers)
