from os import environ

import pytest

from prmreportsgenerator.main import main
from prmreportsgenerator.report_name import ReportName
from tests.e2e.e2e_setup import (BUILD_TAG, DEFAULT_CONVERSATION_CUTOFF_DAYS,
                                 S3_INPUT_TRANSFER_DATA_BUCKET,
                                 S3_OUTPUT_REPORTS_BUCKET,
                                 _build_fake_s3_bucket,
                                 _override_transfer_data, _read_csv,
                                 _read_s3_csv, _read_s3_metadata, _setup)


@pytest.mark.filterwarnings("ignore:Conversion of")
def test_e2e_with_custom_reporting_window_given_start_and_end_date(
    shared_datadir,
):
    fake_s3, s3_client = _setup()
    fake_s3.start()

    output_reports_bucket = _build_fake_s3_bucket(S3_OUTPUT_REPORTS_BUCKET, s3_client)
    input_transfer_bucket = _build_fake_s3_bucket(S3_INPUT_TRANSFER_DATA_BUCKET, s3_client)

    expected_supplier_outcome_counts_output_key = (
        "/2019-12-19-to-2019-12-20-ccg_level_integration_times.csv"
    )
    expected_supplier_outcome_counts = _read_csv(
        shared_datadir
        / "expected_outputs"
        / "ccg_level_integration_times_report"
        / "custom_ccg_level_integration_times.csv"
    )

    s3_reports_output_path = "v3/custom/2019/12/19"

    try:
        environ["START_DATETIME"] = "2019-12-19T00:00:00Z"
        environ["END_DATETIME"] = "2019-12-21T00:00:00Z"
        environ["CONVERSATION_CUTOFF_DAYS"] = DEFAULT_CONVERSATION_CUTOFF_DAYS
        environ["REPORT_NAME"] = ReportName.CCG_LEVEL_INTEGRATION_TIMES.value

        for day in [19, 20]:
            _override_transfer_data(
                shared_datadir,
                S3_INPUT_TRANSFER_DATA_BUCKET,
                year=2019,
                data_month=12,
                data_day=day,
            )

        main()

        supplier_outcome_counts_s3_path = (
            f"{s3_reports_output_path}{expected_supplier_outcome_counts_output_key}"
        )
        actual_supplier_outcome_counts = _read_s3_csv(
            output_reports_bucket, supplier_outcome_counts_s3_path
        )
        assert actual_supplier_outcome_counts == expected_supplier_outcome_counts

        expected_metadata = {
            "reports-generator-version": BUILD_TAG,
            "config-start-datetime": "2019-12-19T00:00:00+00:00",
            "config-end-datetime": "2019-12-21T00:00:00+00:00",
            "config-number-of-months": "None",
            "config-number-of-days": "None",
            "config-cutoff-days": DEFAULT_CONVERSATION_CUTOFF_DAYS,
            "reporting-window-start-datetime": "2019-12-19T00:00:00+00:00",
            "reporting-window-end-datetime": "2019-12-21T00:00:00+00:00",
            "report-name": ReportName.CCG_LEVEL_INTEGRATION_TIMES.value,
        }

        actual_supplier_outcome_counts_s3_metadata = _read_s3_metadata(
            output_reports_bucket, supplier_outcome_counts_s3_path
        )

        assert actual_supplier_outcome_counts_s3_metadata == expected_metadata

    finally:
        output_reports_bucket.objects.all().delete()
        output_reports_bucket.delete()
        input_transfer_bucket.objects.all().delete()
        input_transfer_bucket.delete()
        fake_s3.stop()
        environ.clear()
