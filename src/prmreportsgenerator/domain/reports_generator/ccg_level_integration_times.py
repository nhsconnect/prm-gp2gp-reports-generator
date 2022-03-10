import polars as pl
import pyarrow as pa
from polars import DataFrame, col, count

from prmreportsgenerator.domain.reports_generator.reports_generator import ReportsGenerator
from prmreportsgenerator.domain.transfer import TransferStatus

THREE_DAYS_IN_SECONDS = 259200
EIGHT_DAYS_IN_SECONDS = 691200


def assign_to_sla_band(sla_duration: int) -> str:
    sla_duration_in_seconds = sla_duration
    if sla_duration_in_seconds <= THREE_DAYS_IN_SECONDS:
        return "WITHIN_3_DAYS"
    elif sla_duration_in_seconds <= EIGHT_DAYS_IN_SECONDS:
        return "WITHIN_8_DAYS"
    else:
        return "BEYOND_8_DAYS"


class CCGLevelIntegrationTimesReportsGenerator(ReportsGenerator):
    def __init__(self, transfers: pa.Table):
        super().__init__()
        self._transfers = transfers

    def _assign_sla_band(self, transfer_dataframe: DataFrame) -> DataFrame:
        sla_band_column = col("sla_duration").apply(assign_to_sla_band).alias("sla_band")
        return transfer_dataframe.with_column(sla_band_column)

    def _calculate_integrated_within_3_days(self, transfer_dataframe: DataFrame) -> DataFrame:
        integrated_within_3_days = (col("sla_band") == "WITHIN_3_DAYS") & (
            col("status") == TransferStatus.INTEGRATED_ON_TIME.value
        )
        sum_of_integrated_within_3_days = integrated_within_3_days.sum()
        return transfer_dataframe.with_column(
            sum_of_integrated_within_3_days.alias("Integrated within 3 days")
        )

    def _rename_columns(self, transfer_dataframe: DataFrame) -> DataFrame:
        return (
            transfer_dataframe.with_columns(
                [
                    col("requesting_practice_ccg_name").alias("CCG name"),
                    col("requesting_practice_ccg_ods_code").alias("CCG ODS"),
                    col("requesting_practice_name").alias("Requesting practice name"),
                    col("requesting_practice_ods_code").alias("Requesting practice ODS"),
                    col("Integrated within 3 days"),
                ]
            )
            .groupby(
                [
                    "Requesting practice name",
                    "CCG name",
                    "CCG ODS",
                    "Requesting practice ODS",
                    "Integrated within 3 days",
                ]
            )
            .agg([count("conversation_id").alias("GP2GP Transfers received")])
        )

    def generate(self) -> pa.Table:
        transfers_frame = pl.from_arrow(self._transfers)
        processed_transfers = self._process(
            transfers_frame,
            self._assign_sla_band,
            self._calculate_integrated_within_3_days,
            self._rename_columns,
        ).to_dict()
        return pa.table(processed_transfers)
