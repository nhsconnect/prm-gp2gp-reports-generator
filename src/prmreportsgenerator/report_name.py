from enum import Enum


class ReportName(Enum):
    TRANSFER_OUTCOMES_PER_SUPPLIER_PATHWAY = "TRANSFER_OUTCOMES_PER_SUPPLIER_PATHWAY"
    TRANSFER_LEVEL_TECHNICAL_FAILURES = "TRANSFER_LEVEL_TECHNICAL_FAILURES"
    CCG_LEVEL_INTEGRATION_TIMES = "CCG_LEVEL_INTEGRATION_TIMES"
    TRANSFER_DETAILS_BY_HOUR = "TRANSFER_DETAILS_BY_HOUR"
