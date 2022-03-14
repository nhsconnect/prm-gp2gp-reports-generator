from datetime import timedelta

import pyarrow as pa
import pytest

from prmreportsgenerator.domain.reports_generator.ccg_level_integration_times import (
    CCGLevelIntegrationTimesReportsGenerator,
)
from prmreportsgenerator.domain.transfer import TransferFailureReason, TransferStatus
from tests.builders.common import a_string
from tests.builders.pa_table import PaTableBuilder


@pytest.mark.filterwarnings("ignore:Conversion of")
def test_returns_table_with_ccg_level_integration_times_columns():
    requesting_practice_name = "Practice A"
    requesting_practice_name2 = "Practice B"
    requesting_practice_name3 = "Practice C"
    requesting_practice_ods_code = a_string(6)
    requesting_practice_ods_code2 = a_string(6)
    requesting_practice_ods_code3 = a_string(6)
    requesting_practice_ccg_ods_code = a_string(3)
    requesting_practice_ccg_name = a_string(6)
    status_integrated_on_time = TransferStatus.INTEGRATED_ON_TIME.value
    failure_reason = TransferFailureReason.FINAL_ERROR.value
    table = (
        PaTableBuilder()
        .with_row(
            requesting_practice_name=requesting_practice_name,
            requesting_practice_ods_code=requesting_practice_ods_code,
            requesting_practice_ccg_ods_code=requesting_practice_ccg_ods_code,
            requesting_practice_ccg_name=requesting_practice_ccg_name,
            conversation_id=a_string(16),
            status=status_integrated_on_time,
            failure_reason=None,
            sla_duration=timedelta(days=1).total_seconds(),
        )
        .with_row(
            requesting_practice_name=requesting_practice_name,
            requesting_practice_ods_code=requesting_practice_ods_code,
            requesting_practice_ccg_ods_code=requesting_practice_ccg_ods_code,
            requesting_practice_ccg_name=requesting_practice_ccg_name,
            conversation_id=a_string(16),
            status=TransferStatus.TECHNICAL_FAILURE.value,
            failure_reason=failure_reason,
            sla_duration=timedelta(days=5).total_seconds(),
        )
        .with_row(
            requesting_practice_name=requesting_practice_name2,
            requesting_practice_ods_code=requesting_practice_ods_code2,
            requesting_practice_ccg_ods_code=requesting_practice_ccg_ods_code,
            requesting_practice_ccg_name=requesting_practice_ccg_name,
            conversation_id=a_string(16),
            status=TransferStatus.TECHNICAL_FAILURE.value,
            failure_reason=failure_reason,
            sla_duration=timedelta(days=1).total_seconds(),
        )
        .with_row(
            requesting_practice_name=requesting_practice_name2,
            requesting_practice_ods_code=requesting_practice_ods_code2,
            requesting_practice_ccg_ods_code=requesting_practice_ccg_ods_code,
            requesting_practice_ccg_name=requesting_practice_ccg_name,
            conversation_id=a_string(16),
            status=status_integrated_on_time,
            failure_reason=None,
            sla_duration=timedelta(days=6).total_seconds(),
        )
        .with_row(
            requesting_practice_name=requesting_practice_name3,
            requesting_practice_ods_code=requesting_practice_ods_code3,
            requesting_practice_ccg_ods_code=requesting_practice_ccg_ods_code,
            requesting_practice_ccg_name=requesting_practice_ccg_name,
            conversation_id=a_string(16),
            status=status_integrated_on_time,
            failure_reason=TransferFailureReason.INTEGRATED_LATE.value,
            sla_duration=timedelta(days=10).total_seconds(),
        )
        .build()
    )

    report_generator = CCGLevelIntegrationTimesReportsGenerator(table)
    actual_table = report_generator.generate()
    actual = actual_table.select(
        [
            "CCG name",
            "CCG ODS",
            "Requesting practice name",
            "Requesting practice ODS",
            "GP2GP Transfers received",
            "Integrated within 3 days",
            "Integrated within 3 days - %",
            "Integrated within 8 days",
            "Integrated within 8 days - %",
            # "Not integrated within 8 days (integrated late + not integrated)",
            # "Not integrated within 8 days (integrated late + not integrated) - %",
            "Integrated late",
            "Integrated late - %",
            # "Not integrated within 14 days",
            # "Not integrated within 14 days - %"
        ]
    )

    expected = pa.table(
        {
            "CCG name": [
                requesting_practice_ccg_name,
                requesting_practice_ccg_name,
                requesting_practice_ccg_name,
            ],
            "CCG ODS": [
                requesting_practice_ccg_ods_code,
                requesting_practice_ccg_ods_code,
                requesting_practice_ccg_ods_code,
            ],
            "Requesting practice name": [
                requesting_practice_name,
                requesting_practice_name2,
                requesting_practice_name3,
            ],
            "Requesting practice ODS": [
                requesting_practice_ods_code,
                requesting_practice_ods_code2,
                requesting_practice_ods_code3,
            ],
            "GP2GP Transfers received": [2, 2, 1],
            "Integrated within 3 days": [1, 0, 0],
            "Integrated within 3 days - %": [50.00, 0.00, 0.00],
            "Integrated within 8 days": [0, 1, 0],
            "Integrated within 8 days - %": [0.00, 50.00, 0.00],
            # "Not integrated within 8 days (integrated late + not integrated)": [None],
            # "Not integrated within 8 days (integrated late + not integrated) - %": [None],
            "Integrated late": [None, None, 1],
            "Integrated late - %": [None, None, 100.00],
            # "Not integrated within 14 days": [None],
            # "Not integrated within 14 days - %": [None]
        }
    )

    assert actual == expected
