from enum import Enum

import polars as pl
import pyarrow as pa
from polars import DataFrame, Expr, Series, apply, col, count

from prmreportsgenerator.domain.reports_generator.reports_generator import ReportsGenerator
from prmreportsgenerator.domain.transfer import TransferStatus

THREE_DAYS_IN_SECONDS = 259200
EIGHT_DAYS_IN_SECONDS = 691200


class SlaDuration(Enum):
    WITHIN_3_DAYS = ("WITHIN_3_DAYS",)
    WITHIN_8_DAYS = ("WITHIN_8_DAYS",)
    BEYOND_8_DAYS = ("BEYOND_8_DAYS",)


def assign_to_sla_band(sla_duration: Series) -> SlaDuration:
    sla_duration_in_seconds = sla_duration
    if sla_duration_in_seconds <= THREE_DAYS_IN_SECONDS:
        return SlaDuration.WITHIN_3_DAYS
    elif sla_duration_in_seconds <= EIGHT_DAYS_IN_SECONDS:
        return SlaDuration.WITHIN_8_DAYS
    else:
        return SlaDuration.BEYOND_8_DAYS


class CCGLevelIntegrationTimesReportsGenerator(ReportsGenerator):
    def __init__(self, transfers: pa.Table):
        super().__init__()
        self._transfers = transfers

    def _calculate_integrated_within_3_days(
        self, sla_duration: Series, status: TransferStatus
    ) -> Expr:
        sla_band_column = assign_to_sla_band(sla_duration)
        integrated_within_3_days = (sla_band_column == SlaDuration.WITHIN_3_DAYS) & (
            status == TransferStatus.INTEGRATED_ON_TIME.value
        )
        return integrated_within_3_days.sum()

    def _calculate_integrated_within_3_days_percent(
        self, transfer_dataframe: DataFrame
    ) -> DataFrame:
        integrated_within_3_days_percent = (
            col("Integrated within 3 days") / col("Total transfers") * 100
        )
        return transfer_dataframe.with_column(
            integrated_within_3_days_percent.alias("Integrated within 3 days - %")
        )

    def _calculate_transfers_received(self, transfer_dataframe: DataFrame) -> DataFrame:
        transfers_total = col("conversation_id").count()
        return transfer_dataframe.with_column(transfers_total.alias("Total transfers"))

    def _generate_ccg_level_integration_times(self, transfer_dataframe: DataFrame) -> DataFrame:
        return transfer_dataframe.groupby(["requesting_practice_name"]).agg(
            [
                col("requesting_practice_ccg_name").first().keep_name(),
                col("requesting_practice_ccg_ods_code").first().keep_name(),
                col("requesting_practice_ods_code").first().keep_name(),
                count("conversation_id").alias("GP2GP Transfers received"),
                apply(
                    [col("sla_duration"), col("status")],
                    lambda tuple: self._calculate_integrated_within_3_days(tuple[0], tuple[1]),
                ).alias("Integrated within 3 days"),
            ]
        )

    def _generate_output(self, transfer_dataframe: DataFrame) -> DataFrame:
        return transfer_dataframe.select(
            [
                col("requesting_practice_name").alias("Requesting practice name"),
                col("requesting_practice_ccg_name").alias("CCG name"),
                col("requesting_practice_ccg_ods_code").alias("CCG ODS"),
                col("requesting_practice_ods_code").alias("Requesting practice ODS"),
                col("GP2GP Transfers received"),
                col("Integrated within 3 days"),
            ]
        ).sort(["CCG name", "Requesting practice name"])

    def generate(self) -> pa.Table:
        transfers_frame = pl.from_arrow(self._transfers)
        processed_transfers = self._process(
            transfers_frame,
            self._generate_ccg_level_integration_times,
            self._generate_output,
        ).to_dict()
        return pa.table(processed_transfers)
