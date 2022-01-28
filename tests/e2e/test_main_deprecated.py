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
from moto.server import DomainDispatcherApplication, create_backend_app
from pyarrow._s3fs import S3FileSystem
from pyarrow.parquet import write_table
from werkzeug.serving import make_server

from prmreportsgenerator.main import main
from tests.builders.common import a_string

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
    transfers_table = pa.table(transfers_dictionary)
    write_table(
        table=transfers_table,
        where=s3_path,
        filesystem=S3FileSystem(endpoint_override=fake_s3_url),
    )


def _build_fake_s3(host, port):
    app = DomainDispatcherApplication(create_backend_app, "s3")
    server = make_server(host, port, app)
    return ThreadedServer(server)


def _build_fake_s3_bucket(bucket_name: str, s3):
    s3_fake_bucket = s3.Bucket(bucket_name)
    s3_fake_bucket.create()
    return s3_fake_bucket


fake_s3_host = "127.0.0.1"
fake_s3_port = 8888
fake_s3_url = f"http://{fake_s3_host}:{fake_s3_port}"


@pytest.mark.filterwarnings("ignore:Conversion of")
def test_end_to_end_with_fake_s3_deprecated(datadir):
    fake_s3_access_key = "testing"
    fake_s3_secret_key = "testing"
    fake_s3_region = "eu-west-2"
    s3_output_reports_bucket = "output-reports-bucket"
    s3_input_transfer_data_bucket_name = "input-transfer-data-bucket"
    build_tag = a_string(7)

    fake_s3 = _build_fake_s3(fake_s3_host, fake_s3_port)
    fake_s3.start()

    date_anchor = "2020-01-30T18:44:49Z"

    environ["AWS_ACCESS_KEY_ID"] = fake_s3_access_key
    environ["AWS_SECRET_ACCESS_KEY"] = fake_s3_secret_key
    environ["AWS_DEFAULT_REGION"] = fake_s3_region

    environ["INPUT_TRANSFER_DATA_BUCKET"] = s3_input_transfer_data_bucket_name
    environ["OUTPUT_REPORTS_BUCKET"] = s3_output_reports_bucket
    environ["DATE_ANCHOR"] = date_anchor
    environ["S3_ENDPOINT_URL"] = fake_s3_url
    environ["BUILD_TAG"] = build_tag

    s3 = boto3.resource(
        "s3",
        endpoint_url=fake_s3_url,
        aws_access_key_id=fake_s3_access_key,
        aws_secret_access_key=fake_s3_secret_key,
        config=Config(signature_version="s3v4"),
        region_name=fake_s3_region,
    )

    output_reports_bucket = _build_fake_s3_bucket(s3_output_reports_bucket, s3)
    input_transfer_bucket = _build_fake_s3_bucket(s3_input_transfer_data_bucket_name, s3)

    _write_transfer_parquet(
        datadir / "inputs" / "decTransfersParquetColumns.json",
        f"{s3_input_transfer_data_bucket_name}/v6/2019/12/2019-12-transfers.parquet",
    )

    expected_supplier_pathway_outcome_counts_output_key = (
        "2019-12-supplier_pathway_outcome_counts.csv"
    )
    expected_supplier_pathway_outcome_counts = _read_csv(
        datadir / "expected_outputs" / "supplier_pathway_outcome_counts.csv"
    )

    expected_metadata = {
        "reports-generator-version": build_tag,
        "date-anchor": "2020-01-30T18:44:49+00:00",
    }

    s3_reports_output_path = "v1/2019/12/"

    try:
        main()
        supplier_pathway_outcome_counts_s3_path = (
            f"{s3_reports_output_path}{expected_supplier_pathway_outcome_counts_output_key}"
        )
        actual_supplier_pathway_outcome_counts = _read_s3_csv(
            output_reports_bucket, supplier_pathway_outcome_counts_s3_path
        )
        actual_supplier_pathway_outcome_counts_s3_metadata = _read_s3_metadata(
            output_reports_bucket, supplier_pathway_outcome_counts_s3_path
        )
        assert actual_supplier_pathway_outcome_counts == expected_supplier_pathway_outcome_counts
        assert actual_supplier_pathway_outcome_counts_s3_metadata == expected_metadata
    finally:
        output_reports_bucket.objects.all().delete()
        output_reports_bucket.delete()
        input_transfer_bucket.objects.all().delete()
        input_transfer_bucket.delete()
        fake_s3.stop()
