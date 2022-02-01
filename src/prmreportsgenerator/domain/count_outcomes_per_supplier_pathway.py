from functools import reduce
from typing import List, Optional, Union

from polars import DataFrame, col, count, Expr  # type: ignore
from polars import lit, when  # type: ignore

from prmreportsgenerator.domain.transfer import TransferStatus

error_code_descriptions = {
    6: "Not at surgery",
    7: "GP2GP disabled",
    9: "Unexpected EHR",
    10: "Failed to generate",
    11: "Failed to integrate",
    12: "Duplicate EHR",
    13: "Config issue",
    14: "Req not LM compliant",
    15: "ABA suppressed",
    17: "ABA wrong patient",
    18: "Req malformed",
    19: "Unauthorised req",
    20: "Spine error",
    21: "Extract malformed",
    23: "Sender not LM compliant",
    24: "SDS lookup",
    25: "Timeout",
    26: "Filed as attachment",
    28: "Wrong patient",
    29: "LM reassembly",
    30: "LM general failure",
    31: "Missing LM",
    99: "Unexpected",
}


def _error_description(error_code: int) -> str:
    try:
        return error_code_descriptions[error_code]
    except KeyError:
        return "Unknown error code"


def _unique_errors(errors: List[Optional[int]]):
    unique_error_codes = {error_code for error_code in errors if error_code is not None}
    return ", ".join([f"{e} - {_error_description(e)}" for e in sorted(unique_error_codes)])


def _counted_by_supplier_pathway_and_outcome(transfers: DataFrame):
    return (
        transfers.with_columns(
            [
                col("requesting_supplier").alias("requesting supplier"),
                col("sending_supplier").alias("sending supplier"),
                col("failure_reason").alias("failure reason"),
                col("final_error_codes").apply(_unique_errors).alias("unique final errors"),
                col("sender_error_codes").apply(_unique_errors).alias("unique sender errors"),
                col("intermediate_error_codes")
                .apply(_unique_errors)
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


def _with_percentage_of_all_transfers(transfer_counts: DataFrame):
    total_transfers = col("number of transfers").sum()
    percentage_of_total_transfers = (col("number of transfers") / total_transfers) * 100
    return transfer_counts.with_column(percentage_of_total_transfers.alias("% of transfers"))


def _with_percentage_of_supplier_pathway(transfer_counts: DataFrame):
    supplier_pathway: List[Union[Expr, str]] = [
        col("requesting supplier"),
        col("sending supplier"),
    ]
    count_per_pathway = col("number of transfers").sum().over(supplier_pathway)
    percentage_of_pathway = (col("number of transfers") / count_per_pathway) * 100
    return transfer_counts.with_column(percentage_of_pathway.alias("% of supplier pathway"))


def _with_percentage_of_technical_failures(transfer_counts: DataFrame):
    is_technical_failure = col("status") == TransferStatus.TECHNICAL_FAILURE.value
    total_technical_failures = col("number of transfers").filter(is_technical_failure).sum()
    percentage_of_tech_failures = (col("number of transfers") / total_technical_failures) * 100
    return transfer_counts.with_column(
        when(is_technical_failure)
        .then(percentage_of_tech_failures)
        .otherwise(lit(None).cast(float))
        .alias("% of technical failures"),
    )


def _sorted_by_pathway_and_status(transfer_counts: DataFrame):
    return transfer_counts.sort(
        [
            col("number of transfers"),
            col("requesting supplier"),
            col("sending supplier"),
            col("status"),
        ],
        reverse=[True, False, False, False],
    )


def _process(data, *function_chain):
    return reduce(lambda d, func: func(d), list(function_chain), data)


def count_outcomes_per_supplier_pathway(transfers):
    return _process(
        transfers,
        _counted_by_supplier_pathway_and_outcome,
        _with_percentage_of_all_transfers,
        _with_percentage_of_technical_failures,
        _with_percentage_of_supplier_pathway,
        _sorted_by_pathway_and_status,
    )
