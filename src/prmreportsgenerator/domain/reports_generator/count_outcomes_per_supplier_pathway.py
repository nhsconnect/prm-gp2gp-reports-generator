from typing import List, Union

from polars import DataFrame, Expr, col, count, lit, when

from prmreportsgenerator.domain.reports_generator.reports_generator import ReportsGenerator
from prmreportsgenerator.domain.transfer import TransferStatus


class TransferOutcomePerSupplierPathwayReportGenerator(ReportsGenerator):
    def __init__(self, transfers: DataFrame):
        super().__init__()
        self._transfers = transfers

    def _counted_by_supplier_pathway_and_outcome(self, transfer_dataframe: DataFrame) -> DataFrame:
        return (
            transfer_dataframe.with_columns(
                [
                    col("requesting_supplier").alias("requesting supplier"),
                    col("sending_supplier").alias("sending supplier"),
                    col("failure_reason").alias("failure reason"),
                    col("final_error_codes")
                    .apply(self._unique_errors)
                    .alias("unique final errors"),
                    col("sender_error_codes")
                    .apply(self._unique_errors)
                    .alias("unique sender errors"),
                    col("intermediate_error_codes")
                    .apply(self._unique_errors)
                    .alias("unique intermediate errors"),
                ]
            )
            .groupby(
                [
                    "requesting supplier",
                    "sending supplier",
                    "status",
                    "failure reason",
                    "unique final errors",
                    "unique sender errors",
                    "unique intermediate errors",
                ]
            )
            .agg([count("conversation_id").alias("number of transfers")])
        )

    def _with_percentage_of_all_transfers(self, transfer_dataframe: DataFrame) -> DataFrame:
        total_transfers = col("number of transfers").sum()
        percentage_of_total_transfers = (col("number of transfers") / total_transfers) * 100
        return transfer_dataframe.with_column(percentage_of_total_transfers.alias("% of transfers"))

    def _with_percentage_of_supplier_pathway(self, transfer_dataframe: DataFrame) -> DataFrame:
        supplier_pathway: List[Union[Expr, str]] = [
            col("requesting supplier"),
            col("sending supplier"),
        ]
        count_per_pathway = col("number of transfers").sum().over(supplier_pathway)
        percentage_of_pathway = (col("number of transfers") / count_per_pathway) * 100
        return transfer_dataframe.with_column(percentage_of_pathway.alias("% of supplier pathway"))

    def _with_percentage_of_technical_failures(self, transfer_dataframe: DataFrame) -> DataFrame:
        is_technical_failure = col("status") == TransferStatus.TECHNICAL_FAILURE.value
        total_technical_failures = col("number of transfers").filter(is_technical_failure).sum()
        percentage_of_tech_failures = (col("number of transfers") / total_technical_failures) * 100
        return transfer_dataframe.with_column(
            when(is_technical_failure)
            .then(percentage_of_tech_failures)
            .otherwise(lit(None).cast(float))
            .alias("% of technical failures"),
        )

    def _sorted_by_pathway_and_status(self, transfer_dataframe: DataFrame) -> DataFrame:
        return transfer_dataframe.sort(
            [
                col("number of transfers"),
                col("requesting supplier"),
                col("sending supplier"),
                col("status"),
            ],
            reverse=[True, False, False, False],
        )

    def count_outcomes_per_supplier_pathway(self) -> DataFrame:
        return self._process(
            self._transfers,
            self._counted_by_supplier_pathway_and_outcome,
            self._with_percentage_of_all_transfers,
            self._with_percentage_of_technical_failures,
            self._with_percentage_of_supplier_pathway,
            self._sorted_by_pathway_and_status,
        )
