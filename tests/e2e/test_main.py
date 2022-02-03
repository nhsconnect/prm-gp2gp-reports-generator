import csv
import json
import logging
from datetime import datetime
from io import BytesIO, StringIO
from os import environ
from threading import Thread

import boto3
import pyarrow as pa
import pytest
from botocore.config import Config
from freezegun import freeze_time
from moto.server import DomainDispatcherApplication, create_backend_app
from pyarrow._s3fs import S3FileSystem
from pyarrow.parquet import write_table
from werkzeug.serving import make_server

from prmreportsgenerator.main import main
from prmreportsgenerator.report_name import ReportName
from prmreportsgenerator.utils.add_leading_zero import add_leading_zero
from tests.builders.common import a_datetime, a_string
from tests.builders.pa_table import PaTableBuilder

logger = logging.getLogger(__name__)


class ThreadedServer:
    def __init__(self, server):
        self._server = server
        self._thread = Thread(target=server.serve_forever)

    def start(self):
        self._thread.start()

    def stop(self):
        self._server.shutdown()
        self._thread.join()


FAKE_AWS_HOST = "127.0.0.1"
FAKE_AWS_PORT = 8887
FAKE_AWS_URL = f"http://{FAKE_AWS_HOST}:{FAKE_AWS_PORT}"
FAKE_S3_ACCESS_KEY = "testing"
FAKE_S3_SECRET_KEY = "testing"
FAKE_S3_REGION = "us-west-1"

S3_INPUT_TRANSFER_DATA_BUCKET = "input-transfer-data-bucket"
S3_OUTPUT_REPORTS_BUCKET = "output-reports-data-bucket"
DEFAULT_CONVERSATION_CUTOFF_DAYS = "14"

BUILD_TAG = a_string(7)


def _setup():
    s3_client = boto3.resource(
        "s3",
        endpoint_url=FAKE_AWS_URL,
        aws_access_key_id=FAKE_S3_ACCESS_KEY,
        aws_secret_access_key=FAKE_S3_SECRET_KEY,
        config=Config(signature_version="s3v4"),
        region_name=FAKE_S3_REGION,
    )

    environ["AWS_ACCESS_KEY_ID"] = FAKE_S3_ACCESS_KEY
    environ["AWS_SECRET_ACCESS_KEY"] = FAKE_S3_SECRET_KEY
    environ["AWS_DEFAULT_REGION"] = FAKE_S3_REGION

    environ["INPUT_TRANSFER_DATA_BUCKET"] = S3_INPUT_TRANSFER_DATA_BUCKET
    environ["OUTPUT_REPORTS_BUCKET"] = S3_OUTPUT_REPORTS_BUCKET

    environ["S3_ENDPOINT_URL"] = FAKE_AWS_URL
    environ["BUILD_TAG"] = BUILD_TAG

    fake_s3 = _build_fake_s3(FAKE_AWS_HOST, FAKE_AWS_PORT)
    return fake_s3, s3_client


def _read_json(path):
    return json.loads(path.read_text())


def _read_csv(path):
    with open(path, "r") as csvfile:
        reader = csv.reader(csvfile, skipinitialspace=True)
        return list(reader)


def _parse_dates(items):
    return [None if item is None else datetime.fromisoformat(item) for item in items]


def _read_parquet_columns_json(path):
    datetime_columns = ["date_requested", "last_sender_message_timestamp"]
    return {
        column_name: _parse_dates(values) if column_name in datetime_columns else values
        for column_name, values in _read_json(path).items()
    }


def _get_s3_path(bucket_name, year, month, day, cutoff_days):
    s3_filename = f"{year}-{month}-{day}-transfers.parquet"
    return f"{bucket_name}/v7/cutoff-{cutoff_days}/{year}/{month}/{day}/{s3_filename}"


def _read_s3_csv(bucket, key):
    f = BytesIO()
    bucket.download_fileobj(key, f)
    f.seek(0)
    data = f.read().decode("utf-8")
    reader = csv.reader(StringIO(data))
    return list(reader)


def _read_s3_metadata(bucket, key):
    return bucket.Object(key).get()["Metadata"]


def _write_transfer_parquet(input_transfer_parquet_columns_json, s3_path: str):
    transfers_dictionary = _read_parquet_columns_json(input_transfer_parquet_columns_json)
    transfers_table = pa.table(data=transfers_dictionary, schema=PaTableBuilder.get_schema())
    write_table(
        table=transfers_table,
        where=s3_path,
        filesystem=S3FileSystem(endpoint_override=FAKE_AWS_URL),
    )


def _build_fake_s3(host, port):
    app = DomainDispatcherApplication(create_backend_app, "s3")
    server = make_server(host, port, app)
    return ThreadedServer(server)


def _build_fake_s3_bucket(bucket_name: str, s3):
    s3_fake_bucket = s3.Bucket(bucket_name)
    s3_fake_bucket.create()
    return s3_fake_bucket


def _upload_template_transfer_data(
    datadir,
    input_transfer_bucket: str,
    year: int,
    data_month: int,
    time_range: range,
    cutoff_days: str = DEFAULT_CONVERSATION_CUTOFF_DAYS,
):
    for data_day in time_range:
        day = add_leading_zero(data_day)
        month = add_leading_zero(data_month)

        _write_transfer_parquet(
            datadir / "inputs" / "template-transfers.json",
            _get_s3_path(input_transfer_bucket, year, month, day, cutoff_days),
        )


def _override_transfer_data(
    datadir,
    input_transfer_bucket,
    year: int,
    data_month: int,
    data_day: int,
    cutoff_days: str = DEFAULT_CONVERSATION_CUTOFF_DAYS,
):
    day = add_leading_zero(data_day)
    month = add_leading_zero(data_month)

    _write_transfer_parquet(
        datadir / "inputs" / f"{year}-{month}-{day}-transfers.json",
        _get_s3_path(input_transfer_bucket, year, month, day, cutoff_days=cutoff_days),
    )


@pytest.mark.filterwarnings("ignore:Conversion of")
def test_e2e_outcomes_per_supplier_pathway_with_custom_reporting_window_given_start_and_end_date(
    datadir,
):
    fake_s3, s3_client = _setup()
    fake_s3.start()

    output_reports_bucket = _build_fake_s3_bucket(S3_OUTPUT_REPORTS_BUCKET, s3_client)
    input_transfer_bucket = _build_fake_s3_bucket(S3_INPUT_TRANSFER_DATA_BUCKET, s3_client)

    expected_supplier_pathway_outcome_counts_output_key = (
        "/2019-12-19-supplier_pathway_outcome_counts.csv"
    )
    expected_supplier_pathway_outcome_counts = _read_csv(
        datadir
        / "expected_outputs"
        / "transfer_outcomes_per_supplier_pathway_report"
        / "custom_supplier_pathway_outcome_counts.csv"
    )

    s3_reports_output_path = "v2/custom/2019/12/19"

    try:
        environ["START_DATETIME"] = "2019-12-19T00:00:00Z"
        environ["END_DATETIME"] = "2019-12-21T00:00:00Z"
        environ["CONVERSATION_CUTOFF_DAYS"] = DEFAULT_CONVERSATION_CUTOFF_DAYS
        environ["REPORT_NAME"] = ReportName.TRANSFER_OUTCOMES_PER_SUPPLIER_PATHWAY.value

        for day in [19, 20]:
            _override_transfer_data(
                datadir, S3_INPUT_TRANSFER_DATA_BUCKET, year=2019, data_month=12, data_day=day
            )

        main()

        supplier_pathway_outcome_counts_s3_path = (
            f"{s3_reports_output_path}{expected_supplier_pathway_outcome_counts_output_key}"
        )
        actual_supplier_pathway_outcome_counts = _read_s3_csv(
            output_reports_bucket, supplier_pathway_outcome_counts_s3_path
        )
        assert actual_supplier_pathway_outcome_counts == expected_supplier_pathway_outcome_counts

        expected_metadata = {
            "reports-generator-version": BUILD_TAG,
            "config-start-datetime": "2019-12-19T00:00:00+00:00",
            "config-end-datetime": "2019-12-21T00:00:00+00:00",
            "config-number-of-months": "None",
            "config-number-of-days": "None",
            "config-cutoff-days": DEFAULT_CONVERSATION_CUTOFF_DAYS,
            "reporting-window-start-datetime": "2019-12-19T00:00:00+00:00",
            "reporting-window-end-datetime": "2019-12-21T00:00:00+00:00",
            "report-name": ReportName.TRANSFER_OUTCOMES_PER_SUPPLIER_PATHWAY.value,
        }

        actual_supplier_pathway_outcome_counts_s3_metadata = _read_s3_metadata(
            output_reports_bucket, supplier_pathway_outcome_counts_s3_path
        )

        assert actual_supplier_pathway_outcome_counts_s3_metadata == expected_metadata

    finally:
        output_reports_bucket.objects.all().delete()
        output_reports_bucket.delete()
        input_transfer_bucket.objects.all().delete()
        input_transfer_bucket.delete()
        fake_s3.stop()
        environ.clear()


@freeze_time(a_datetime(year=2020, month=1, day=2))
@pytest.mark.filterwarnings("ignore:Conversion of")
def test_e2e_outcomes_per_supplier_pathway_with_monthly_reporting_window_given_number_of_months(
    datadir,
):
    fake_s3, s3_client = _setup()
    fake_s3.start()

    output_reports_bucket = _build_fake_s3_bucket(S3_OUTPUT_REPORTS_BUCKET, s3_client)
    input_transfer_bucket = _build_fake_s3_bucket(S3_INPUT_TRANSFER_DATA_BUCKET, s3_client)

    expected_supplier_pathway_outcome_counts_output_key = (
        "/2019-12-01-supplier_pathway_outcome_counts.csv"
    )
    expected_supplier_pathway_outcome_counts = _read_csv(
        datadir
        / "expected_outputs"
        / "transfer_outcomes_per_supplier_pathway_report"
        / "monthly_supplier_pathway_outcome_counts.csv"
    )

    s3_reports_output_path = "v2/1-months/2019/12/01"

    try:
        environ["NUMBER_OF_MONTHS"] = "1"
        environ["CONVERSATION_CUTOFF_DAYS"] = DEFAULT_CONVERSATION_CUTOFF_DAYS
        environ["REPORT_NAME"] = ReportName.TRANSFER_OUTCOMES_PER_SUPPLIER_PATHWAY.value

        _upload_template_transfer_data(
            datadir,
            S3_INPUT_TRANSFER_DATA_BUCKET,
            year=2019,
            data_month=12,
            time_range=range(1, 32),
        )

        for day in [1, 3, 5, 19, 20, 23, 24, 25, 29, 30, 31]:
            _override_transfer_data(
                datadir, S3_INPUT_TRANSFER_DATA_BUCKET, year=2019, data_month=12, data_day=day
            )

        main()

        supplier_pathway_outcome_counts_s3_path = (
            f"{s3_reports_output_path}{expected_supplier_pathway_outcome_counts_output_key}"
        )
        actual_supplier_pathway_outcome_counts = _read_s3_csv(
            output_reports_bucket, supplier_pathway_outcome_counts_s3_path
        )
        assert actual_supplier_pathway_outcome_counts == expected_supplier_pathway_outcome_counts

        expected_metadata = {
            "reports-generator-version": BUILD_TAG,
            "config-start-datetime": "None",
            "config-end-datetime": "None",
            "config-number-of-months": "1",
            "config-number-of-days": "None",
            "config-cutoff-days": DEFAULT_CONVERSATION_CUTOFF_DAYS,
            "reporting-window-start-datetime": "2019-12-01T00:00:00+00:00",
            "reporting-window-end-datetime": "2020-01-01T00:00:00+00:00",
            "report-name": ReportName.TRANSFER_OUTCOMES_PER_SUPPLIER_PATHWAY.value,
        }

        actual_supplier_pathway_outcome_counts_s3_metadata = _read_s3_metadata(
            output_reports_bucket, supplier_pathway_outcome_counts_s3_path
        )

        assert actual_supplier_pathway_outcome_counts_s3_metadata == expected_metadata
    finally:
        output_reports_bucket.objects.all().delete()
        output_reports_bucket.delete()
        input_transfer_bucket.objects.all().delete()
        input_transfer_bucket.delete()
        fake_s3.stop()
        environ.clear()


@freeze_time(a_datetime(year=2019, month=12, day=28))
@pytest.mark.filterwarnings("ignore:Conversion of")
def test_e2e_outcomes_per_supplier_pathway_with_daily_reporting_window_given_number_of_days(
    datadir,
):
    fake_s3, s3_client = _setup()
    fake_s3.start()

    output_reports_bucket = _build_fake_s3_bucket(S3_OUTPUT_REPORTS_BUCKET, s3_client)
    input_transfer_bucket = _build_fake_s3_bucket(S3_INPUT_TRANSFER_DATA_BUCKET, s3_client)

    expected_supplier_pathway_outcome_counts_output_key = (
        "/2019-12-23-supplier_pathway_outcome_counts.csv"
    )
    expected_supplier_pathway_outcome_counts = _read_csv(
        datadir
        / "expected_outputs"
        / "transfer_outcomes_per_supplier_pathway_report"
        / "daily_supplier_pathway_outcome_counts.csv"
    )

    s3_reports_output_path = "v2/2-days/2019/12/23"

    try:
        number_of_days = "2"
        environ["NUMBER_OF_DAYS"] = number_of_days
        cutoff_days = "3"
        environ["CONVERSATION_CUTOFF_DAYS"] = cutoff_days
        environ["REPORT_NAME"] = ReportName.TRANSFER_OUTCOMES_PER_SUPPLIER_PATHWAY.value

        for day in [23, 24]:
            _override_transfer_data(
                datadir,
                S3_INPUT_TRANSFER_DATA_BUCKET,
                year=2019,
                data_month=12,
                data_day=day,
                cutoff_days=cutoff_days,
            )

        main()

        supplier_pathway_outcome_counts_s3_path = (
            f"{s3_reports_output_path}{expected_supplier_pathway_outcome_counts_output_key}"
        )
        actual_supplier_pathway_outcome_counts = _read_s3_csv(
            output_reports_bucket, supplier_pathway_outcome_counts_s3_path
        )
        assert actual_supplier_pathway_outcome_counts == expected_supplier_pathway_outcome_counts

        expected_metadata = {
            "reports-generator-version": BUILD_TAG,
            "config-start-datetime": "None",
            "config-end-datetime": "None",
            "config-number-of-months": "None",
            "config-number-of-days": number_of_days,
            "config-cutoff-days": cutoff_days,
            "reporting-window-start-datetime": "2019-12-23T00:00:00+00:00",
            "reporting-window-end-datetime": "2019-12-25T00:00:00+00:00",
            "report-name": ReportName.TRANSFER_OUTCOMES_PER_SUPPLIER_PATHWAY.value,
        }

        actual_supplier_pathway_outcome_counts_s3_metadata = _read_s3_metadata(
            output_reports_bucket, supplier_pathway_outcome_counts_s3_path
        )

        assert actual_supplier_pathway_outcome_counts_s3_metadata == expected_metadata
    finally:
        output_reports_bucket.objects.all().delete()
        output_reports_bucket.delete()
        input_transfer_bucket.objects.all().delete()
        input_transfer_bucket.delete()
        fake_s3.stop()
        environ.clear()


@pytest.mark.filterwarnings("ignore:Conversion of")
def test_e2e_transfer_level_technical_failures_custom_reporting_window_given_start_and_end_date(
    datadir,
):
    fake_s3, s3_client = _setup()
    fake_s3.start()

    output_reports_bucket = _build_fake_s3_bucket(S3_OUTPUT_REPORTS_BUCKET, s3_client)
    input_transfer_bucket = _build_fake_s3_bucket(S3_INPUT_TRANSFER_DATA_BUCKET, s3_client)

    expected_transfer_level_technical_failures_output_key = (
        "/2019-12-19-supplier_pathway_outcome_counts.csv"
    )
    expected_transfer_level_technical_failures = _read_csv(
        datadir
        / "expected_outputs"
        / "transfer_level_technical_failures_report"
        / "custom_transfer_level_technical_failures_report.csv"
    )

    s3_reports_output_path = "v2/custom/2019/12/19"

    try:
        environ["START_DATETIME"] = "2019-12-19T00:00:00Z"
        environ["END_DATETIME"] = "2019-12-21T00:00:00Z"
        environ["CONVERSATION_CUTOFF_DAYS"] = DEFAULT_CONVERSATION_CUTOFF_DAYS
        environ["REPORT_NAME"] = ReportName.TRANSFER_LEVEL_TECHNICAL_FAILURES.value

        for day in [19, 20]:
            _override_transfer_data(
                datadir, S3_INPUT_TRANSFER_DATA_BUCKET, year=2019, data_month=12, data_day=day
            )

        main()

        transfer_level_technical_failures_report_s3_path = (
            f"{s3_reports_output_path}{expected_transfer_level_technical_failures_output_key}"
        )
        actual_transfer_level_technical_failures_report = _read_s3_csv(
            output_reports_bucket, transfer_level_technical_failures_report_s3_path
        )
        assert (
            actual_transfer_level_technical_failures_report
            == expected_transfer_level_technical_failures
        )

        expected_metadata = {
            "reports-generator-version": BUILD_TAG,
            "config-start-datetime": "2019-12-19T00:00:00+00:00",
            "config-end-datetime": "2019-12-21T00:00:00+00:00",
            "config-number-of-months": "None",
            "config-number-of-days": "None",
            "config-cutoff-days": DEFAULT_CONVERSATION_CUTOFF_DAYS,
            "reporting-window-start-datetime": "2019-12-19T00:00:00+00:00",
            "reporting-window-end-datetime": "2019-12-21T00:00:00+00:00",
            "report-name": ReportName.TRANSFER_LEVEL_TECHNICAL_FAILURES.value,
        }

        actual_transfer_level_technical_failures_report_s3_metadata = _read_s3_metadata(
            output_reports_bucket, transfer_level_technical_failures_report_s3_path
        )

        assert actual_transfer_level_technical_failures_report_s3_metadata == expected_metadata

    finally:
        output_reports_bucket.objects.all().delete()
        output_reports_bucket.delete()
        input_transfer_bucket.objects.all().delete()
        input_transfer_bucket.delete()
        fake_s3.stop()
        environ.clear()


@freeze_time(a_datetime(year=2020, month=1, day=2))
@pytest.mark.filterwarnings("ignore:Conversion of")
def test_e2e_transfer_level_technical_failures_with_monthly_reporting_window_given_number_of_months(
    datadir,
):
    fake_s3, s3_client = _setup()
    fake_s3.start()

    output_reports_bucket = _build_fake_s3_bucket(S3_OUTPUT_REPORTS_BUCKET, s3_client)
    input_transfer_bucket = _build_fake_s3_bucket(S3_INPUT_TRANSFER_DATA_BUCKET, s3_client)

    expected_transfer_level_technical_failures_output_key = (
        "/2019-12-01-supplier_pathway_outcome_counts.csv"
    )
    expected_transfer_level_technical_failures = _read_csv(
        datadir
        / "expected_outputs"
        / "transfer_level_technical_failures_report"
        / "monthly_transfer_level_technical_failures_report.csv"
    )

    s3_reports_output_path = "v2/1-months/2019/12/01"

    try:
        environ["NUMBER_OF_MONTHS"] = "1"
        environ["CONVERSATION_CUTOFF_DAYS"] = DEFAULT_CONVERSATION_CUTOFF_DAYS
        environ["REPORT_NAME"] = ReportName.TRANSFER_LEVEL_TECHNICAL_FAILURES.value

        _upload_template_transfer_data(
            datadir,
            S3_INPUT_TRANSFER_DATA_BUCKET,
            year=2019,
            data_month=12,
            time_range=range(1, 32),
        )

        for day in [1, 3, 5, 19, 20, 23, 24, 25, 29, 30, 31]:
            _override_transfer_data(
                datadir, S3_INPUT_TRANSFER_DATA_BUCKET, year=2019, data_month=12, data_day=day
            )

        main()

        transfer_level_technical_failures_report_s3_path = (
            f"{s3_reports_output_path}{expected_transfer_level_technical_failures_output_key}"
        )
        actual_transfer_level_technical_failures_report = _read_s3_csv(
            output_reports_bucket, transfer_level_technical_failures_report_s3_path
        )
        assert (
            actual_transfer_level_technical_failures_report
            == expected_transfer_level_technical_failures
        )

        expected_metadata = {
            "reports-generator-version": BUILD_TAG,
            "config-start-datetime": "None",
            "config-end-datetime": "None",
            "config-number-of-months": "1",
            "config-number-of-days": "None",
            "config-cutoff-days": DEFAULT_CONVERSATION_CUTOFF_DAYS,
            "reporting-window-start-datetime": "2019-12-01T00:00:00+00:00",
            "reporting-window-end-datetime": "2020-01-01T00:00:00+00:00",
            "report-name": ReportName.TRANSFER_LEVEL_TECHNICAL_FAILURES.value,
        }

        actual_transfer_level_technical_failures_report_s3_metadata = _read_s3_metadata(
            output_reports_bucket, transfer_level_technical_failures_report_s3_path
        )

        assert actual_transfer_level_technical_failures_report_s3_metadata == expected_metadata
    finally:
        output_reports_bucket.objects.all().delete()
        output_reports_bucket.delete()
        input_transfer_bucket.objects.all().delete()
        input_transfer_bucket.delete()
        fake_s3.stop()
        environ.clear()


@freeze_time(a_datetime(year=2020, month=1, day=2))
@pytest.mark.filterwarnings("ignore:Conversion of")
def test_e2e_transfer_level_technical_failures_with_daily_reporting_window_given_number_of_days(
    datadir,
):
    fake_s3, s3_client = _setup()
    fake_s3.start()

    output_reports_bucket = _build_fake_s3_bucket(S3_OUTPUT_REPORTS_BUCKET, s3_client)
    input_transfer_bucket = _build_fake_s3_bucket(S3_INPUT_TRANSFER_DATA_BUCKET, s3_client)

    expected_transfer_level_technical_failures_output_key = (
        "/2019-12-31-supplier_pathway_outcome_counts.csv"
    )
    expected_transfer_level_technical_failures = _read_csv(
        datadir
        / "expected_outputs"
        / "transfer_level_technical_failures_report"
        / "daily_transfer_level_technical_failures_report.csv"
    )

    s3_reports_output_path = "v2/1-days/2019/12/31"

    try:
        number_of_days = "1"
        environ["NUMBER_OF_DAYS"] = number_of_days
        cutoff_days = "1"
        environ["CONVERSATION_CUTOFF_DAYS"] = cutoff_days
        environ["REPORT_NAME"] = ReportName.TRANSFER_LEVEL_TECHNICAL_FAILURES.value

        for day in [31]:
            _override_transfer_data(
                datadir,
                S3_INPUT_TRANSFER_DATA_BUCKET,
                year=2019,
                data_month=12,
                data_day=day,
                cutoff_days=cutoff_days,
            )

        main()

        transfer_level_technical_failures_report_s3_path = (
            f"{s3_reports_output_path}{expected_transfer_level_technical_failures_output_key}"
        )
        actual_transfer_level_technical_failures_report = _read_s3_csv(
            output_reports_bucket, transfer_level_technical_failures_report_s3_path
        )
        assert (
            actual_transfer_level_technical_failures_report
            == expected_transfer_level_technical_failures
        )

        expected_metadata = {
            "reports-generator-version": BUILD_TAG,
            "config-start-datetime": "None",
            "config-end-datetime": "None",
            "config-number-of-months": "None",
            "config-number-of-days": number_of_days,
            "config-cutoff-days": cutoff_days,
            "reporting-window-start-datetime": "2019-12-31T00:00:00+00:00",
            "reporting-window-end-datetime": "2020-01-01T00:00:00+00:00",
            "report-name": ReportName.TRANSFER_LEVEL_TECHNICAL_FAILURES.value,
        }

        actual_transfer_level_technical_failures_report_s3_metadata = _read_s3_metadata(
            output_reports_bucket, transfer_level_technical_failures_report_s3_path
        )

        assert actual_transfer_level_technical_failures_report_s3_metadata == expected_metadata
    finally:
        output_reports_bucket.objects.all().delete()
        output_reports_bucket.delete()
        input_transfer_bucket.objects.all().delete()
        input_transfer_bucket.delete()
        fake_s3.stop()
        environ.clear()
