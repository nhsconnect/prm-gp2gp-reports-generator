from datetime import datetime
from unittest.mock import patch

from dateutil.tz import UTC, tzutc

from prmreportsgenerator.domain.reporting_windows.reporting_window import ReportingWindow
from prmreportsgenerator.io.reports_io import ReportsS3UriResolver
from prmreportsgenerator.report_name import ReportName
from tests.builders.common import a_string, an_integer


@patch.multiple(ReportingWindow, __abstractmethods__=set())
def test_returns_correct_transfer_data_uris_given_start_and_end_datetime_and_cutoff():
    transfer_data_bucket = a_string()
    start_datetime = datetime(year=2021, month=1, day=1, tzinfo=tzutc())
    end_datetime = datetime(year=2021, month=1, day=3, tzinfo=tzutc())
    cutoff_days = an_integer(b=30)

    reporting_window = ReportingWindow(start_datetime=start_datetime, end_datetime=end_datetime)

    uri_resolver = ReportsS3UriResolver(
        reports_bucket=a_string(),
        transfer_data_bucket=transfer_data_bucket,
        report_name=ReportName.TRANSFER_OUTCOMES_PER_SUPPLIER_PATHWAY,
    )

    actual = uri_resolver.input_transfer_data_uris(reporting_window, cutoff_days)

    cutoff_key = f"cutoff-{cutoff_days}"
    expected = [  # doesn't append end date to file name
        f"s3://{transfer_data_bucket}/v7/{cutoff_key}/2021/01/01/2021-01-01-transfers.parquet",
        f"s3://{transfer_data_bucket}/v7/{cutoff_key}/2021/01/02/2021-01-02-transfers.parquet",
    ]

    assert actual == expected


def test_returns_uri_given_start_date_and_end_date():
    reports_bucket = a_string()
    report_name = ReportName.TRANSFER_OUTCOMES_PER_SUPPLIER_PATHWAY
    start_date = datetime(year=2022, month=3, day=5, tzinfo=UTC)
    end_date = datetime(year=2022, month=3, day=7, tzinfo=UTC)
    uri_resolver = ReportsS3UriResolver(
        reports_bucket=reports_bucket, transfer_data_bucket=a_string(), report_name=report_name
    )
    supplement_s3_key = "4-days"

    actual = uri_resolver.output_table_uri(
        start_date=start_date, end_date=end_date, supplement_s3_key=supplement_s3_key
    )

    expected_s3_key = f"{reports_bucket}/v2/{supplement_s3_key}/2022/03/05"
    expected_report_name = report_name.value.lower()
    expected = f"s3://{expected_s3_key}/2022-03-05-to-2022-03-06-{expected_report_name}.csv"

    assert actual == expected
