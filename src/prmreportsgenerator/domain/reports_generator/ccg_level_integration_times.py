from enum import Enum

import polars as pl
import pyarrow as pa
from polars import DataFrame, col, count

from prmreportsgenerator.domain.reports_generator.reports_generator import ReportsGenerator
from prmreportsgenerator.domain.transfer import TransferStatus

THREE_DAYS_IN_SECONDS = 259200
EIGHT_DAYS_IN_SECONDS = 691200


class SlaDuration(Enum):
    WITHIN_3_DAYS = "WITHIN_3_DAYS"
    WITHIN_8_DAYS = "WITHIN_8_DAYS"
    BEYOND_8_DAYS = "BEYOND_8_DAYS"


def assign_to_sla_band(sla_duration) -> str:
    sla_duration_in_seconds = sla_duration
    if sla_duration_in_seconds <= THREE_DAYS_IN_SECONDS:
        return SlaDuration.WITHIN_3_DAYS.value
    elif sla_duration_in_seconds <= EIGHT_DAYS_IN_SECONDS:
        return SlaDuration.WITHIN_8_DAYS.value
    else:
        return SlaDuration.BEYOND_8_DAYS.value


class CCGLevelIntegrationTimesReportsGenerator(ReportsGenerator):
    def __init__(self, transfers: pa.Table):
        super().__init__()
        self._transfers = transfers

    def _calculate_sla_band(self, transfer_dataframe: DataFrame) -> DataFrame:
        return transfer_dataframe.with_column(
            col("sla_duration").apply(assign_to_sla_band).alias("sla_band")
        )

    def _calculate_integrated_within_3_days(self, transfer_dataframe: DataFrame) -> DataFrame:
        within_3_days_sla_band_bool = col("sla_band") == SlaDuration.WITHIN_3_DAYS.value
        integrated_on_time_bool = col("status") == TransferStatus.INTEGRATED_ON_TIME.value
        integrated_within_3_days_bool = within_3_days_sla_band_bool & integrated_on_time_bool
        return transfer_dataframe.with_column(
            integrated_within_3_days_bool.alias("Integrated within 3 days")
        )

    def _calculate_integrated_within_8_days(self, transfer_dataframe: DataFrame) -> DataFrame:
        within_8_days_sla_band_bool = col("sla_band") == SlaDuration.WITHIN_8_DAYS.value
        integrated_on_time_bool = col("status") == TransferStatus.INTEGRATED_ON_TIME.value
        integrated_within_8_days_bool = within_8_days_sla_band_bool & integrated_on_time_bool
        return transfer_dataframe.with_column(
            integrated_within_8_days_bool.alias("Integrated within 8 days")
        )

    def _generate_ccg_level_integration_times_totals(
        self, transfer_dataframe: DataFrame
    ) -> DataFrame:
        return transfer_dataframe.groupby(["requesting_practice_name"]).agg(
            [
                col("requesting_practice_ccg_name").first().keep_name(),
                col("requesting_practice_ccg_ods_code").first().keep_name(),
                col("requesting_practice_ods_code").first().keep_name(),
                col("Integrated within 3 days").sum().keep_name(),
                col("Integrated within 8 days").sum().keep_name(),
                count("conversation_id").alias("GP2GP Transfers received"),
            ]
        )

    def _generate_ccg_level_integration_times_percentages(
        self, transfer_dataframe: DataFrame
    ) -> DataFrame:
        return transfer_dataframe.with_columns(
            [
                (col("Integrated within 3 days") / col("GP2GP Transfers received") * 100).alias(
                    "Integrated within 3 days - %"
                ),
                (col("Integrated within 8 days") / col("GP2GP Transfers received") * 100).alias(
                    "Integrated within 8 days - %"
                ),
            ]
        )

    def _generate_output(self, transfer_dataframe: DataFrame) -> DataFrame:
        return transfer_dataframe.select(
            [
                col("requesting_practice_ccg_name").alias("CCG name"),
                col("requesting_practice_ccg_ods_code").alias("CCG ODS"),
                col("requesting_practice_name").alias("Requesting practice name"),
                col("requesting_practice_ods_code").alias("Requesting practice ODS"),
                col("GP2GP Transfers received"),
                col("Integrated within 3 days"),
                col("Integrated within 3 days - %"),
                col("Integrated within 8 days"),
                col("Integrated within 8 days - %"),
            ]
        ).sort(["CCG name", "Requesting practice name"])

    def generate(self) -> pa.Table:
        transfers_frame = pl.from_arrow(self._transfers)
        processed_transfers = self._process(
            transfers_frame,
            self._calculate_sla_band,
            self._calculate_integrated_within_3_days,
            self._calculate_integrated_within_8_days,
            self._generate_ccg_level_integration_times_totals,
            self._generate_ccg_level_integration_times_percentages,
            self._generate_output,
        ).to_dict()
        return pa.table(processed_transfers)
