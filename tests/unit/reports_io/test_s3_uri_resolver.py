from prmreportsgenerator.reports_io import ReportsS3UriResolver
from tests.builders.common import a_datetime, a_string


def test_resolver_returns_correct_supplier_pathway_outcome_counts_uri():
    reports_bucket = a_string()
    date_anchor = a_datetime()
    year = date_anchor.year
    month = date_anchor.month

    uri_resolver = ReportsS3UriResolver(
        reports_bucket=reports_bucket,
        transfer_data_bucket=a_string(),
    )

    actual = uri_resolver.supplier_pathway_outcome_counts((year, month))

    s3_key = f"v1/{year}/{month}/{year}-{month}-supplier_pathway_outcome_counts.csv"
    expected = f"s3://{reports_bucket}/{s3_key}"

    assert actual == expected


def test_resolver_returns_correct_transfer_data_uris():
    transfer_data_bucket = a_string()

    transfer_data_month = (2021, 11)

    uri_resolver = ReportsS3UriResolver(
        reports_bucket=a_string(),
        transfer_data_bucket=transfer_data_bucket,
    )

    actual = uri_resolver.transfer_data_uri(transfer_data_month)

    expected = f"s3://{transfer_data_bucket}/v6/2021/11/2021-11-transfers.parquet"

    assert actual == expected
