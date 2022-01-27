from datetime import datetime

from dateutil.tz import tzutc

from prmreportsgenerator.config import PipelineConfig


def create_pipeline_config(**kwargs) -> PipelineConfig:
    return PipelineConfig(
        input_transfer_data_bucket="input-transfer-data-bucket",
        output_reports_bucket="output-reports-bucket",
        date_anchor=datetime(
            year=2020, month=1, day=30, hour=18, minute=44, second=49, tzinfo=tzutc()
        ),
        s3_endpoint_url=None,
        build_tag="123",
        start_datetime=kwargs.get("start_datetime", None),
        end_datetime=kwargs.get("end_datetime", None),
        number_of_months=kwargs.get("number_of_months", None),
        number_of_days=kwargs.get("number_of_days", None),
        cutoff_days=kwargs.get("cutoff_days", None),
    )
