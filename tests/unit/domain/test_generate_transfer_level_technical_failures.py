import pyarrow as pa
import pytest

from prmreportsgenerator.domain.reports_generator.transfer_level_technical_failures import (
    TransferLevelTechnicalFailuresReportsGenerator,
)
from prmreportsgenerator.domain.transfer import TransferFailureReason, TransferStatus
from tests.builders.common import a_string
from tests.builders.pa_table import PaTableBuilder


@pytest.mark.filterwarnings("ignore:Conversion of")
def test_returns_table_with_transfer_level_technical_failure_columns():
    requesting_supplier = a_string(6)
    sending_supplier = a_string(6)
    status = TransferStatus.TECHNICAL_FAILURE.value
    failure_reason = TransferFailureReason.FINAL_ERROR.value
    table = (
        PaTableBuilder()
        .with_row(
            requesting_supplier=requesting_supplier,
            sending_supplier=sending_supplier,
            status=status,
            failure_reason=failure_reason,
        )
        .build()
    )

    report_generator = TransferLevelTechnicalFailuresReportsGenerator(table)
    actual_table = report_generator.generate()
    actual = actual_table.select(
        ["requesting supplier", "sending supplier", "status", "failure reason"]
    )

    expected = pa.table(
        {
            "requesting supplier": [requesting_supplier],
            "sending supplier": [sending_supplier],
            "status": [status],
            "failure reason": [failure_reason],
        }
    )

    assert actual == expected


# 1. certain/other columns exists
# 2. filtered statuses
# 3. errors have description and de-duplicated
# make sure all columns have been checked
