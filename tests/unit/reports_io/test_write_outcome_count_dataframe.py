from unittest.mock import Mock

import polars as pl

from prmreportsgenerator.reports_io import ReportsIO
from tests.builders.common import a_string

_DATE_ANCHOR_MONTH = 1
_DATE_ANCHOR_YEAR = 2021


def test_given_dataframe_will_write_csv():
    s3_manager = Mock()
    reports_bucket = a_string()
    s3_key = f"v1/{_DATE_ANCHOR_YEAR}/{_DATE_ANCHOR_MONTH}/supplier_pathway_outcome_counts.csv"
    s3_uri = f"s3://{reports_bucket}/{s3_key}"

    output_metadata = {"metadata-field": "metadata_value"}

    metrics_io = ReportsIO(
        s3_data_manager=s3_manager,
        output_metadata=output_metadata,
    )
    data = {"Fruit": ["Banana"]}
    df = pl.DataFrame(data)

    metrics_io.write_outcome_counts(dataframe=df, s3_uri=s3_uri)

    expected_dataframe = df

    s3_manager.write_dataframe_to_csv.assert_called_once_with(
        object_uri=s3_uri, dataframe=expected_dataframe, metadata=output_metadata
    )
