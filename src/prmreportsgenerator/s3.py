import logging
from datetime import datetime
from io import BytesIO
from typing import Dict
from urllib.parse import urlparse

import polars as pl
import pyarrow.csv as csv
import pyarrow.parquet as pq
from pyarrow.lib import Table

logger = logging.getLogger(__name__)


def _serialize_datetime(obj):
    if isinstance(obj, datetime):
        return obj.isoformat()
    raise TypeError(f"Type {type(obj)} is not JSON serializable")


class S3DataManager:
    def __init__(self, client):
        self._client = client

    def _object_from_uri(self, uri: str):
        object_url = urlparse(uri)
        s3_bucket = object_url.netloc
        s3_key = object_url.path.lstrip("/")
        return self._client.Object(s3_bucket, s3_key)

    def read_parquet(self, object_uri: str) -> Table:
        logger.info(
            "Reading file from: " + object_uri,
            extra={"event": "READING_FILE_FROM_S3", "object_uri": object_uri},
        )
        s3_object = self._object_from_uri(object_uri)
        response = s3_object.get()
        body = BytesIO(response["Body"].read())
        return pq.read_table(body)

    def write_dataframe_to_csv(
        self, object_uri: str, dataframe: pl.DataFrame, metadata: Dict[str, str]
    ):
        logger.info(
            "Attempting to upload: " + object_uri,
            extra={"event": "ATTEMPTING_UPLOAD_CSV_TO_S3", "object_uri": object_uri},
        )
        s3_object = self._object_from_uri(object_uri)
        csv_buffer = BytesIO()
        csv.write_csv(dataframe.to_arrow(), csv_buffer)
        csv_buffer.seek(0)
        s3_object.put(Body=csv_buffer.getvalue(), ContentType="text/csv", Metadata=metadata)
        logger.info(
            "Successfully uploaded to: " + object_uri,
            extra={"event": "UPLOADED_CSV_TO_S3", "object_uri": object_uri},
        )
