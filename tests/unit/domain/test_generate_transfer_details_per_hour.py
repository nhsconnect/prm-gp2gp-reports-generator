import pyarrow as pa
from dateutil.tz import UTC

from src.prmreportsgenerator.domain.reports_generator.transfer_details_per_hour import (
    TransferDetailsPerHourReportsGenerator,
)
from tests.builders.common import a_datetime
from tests.builders.pa_table import PaTableBuilder


def test_total_transfers_shown_per_hour():
    # Given
    input_data = (
        PaTableBuilder()
        .with_row(
            date_requested=a_datetime(year=2021, month=1, day=1, hour=12, minute=10).astimezone(UTC)
        )
        .with_row(
            date_requested=a_datetime(year=2021, month=1, day=1, hour=12, minute=20).astimezone(UTC)
        )
        .with_row(
            date_requested=a_datetime(year=2021, month=1, day=2, hour=15, minute=10).astimezone(UTC)
        )
        .with_row(
            date_requested=a_datetime(year=2021, month=1, day=1, hour=14, minute=10).astimezone(UTC)
        )
        .build()
    )

    expected_output = pa.table(
        {
            "Date/Time": ["21/01/01 12:00", "21/01/01 14:00", "21/01/02 15:00"],
            "Total number of transfers": [2, 1, 1],
        }
    )

    # When
    report_generator = TransferDetailsPerHourReportsGenerator(input_data)
    result = report_generator.generate()

    # Then
    assert result == expected_output
