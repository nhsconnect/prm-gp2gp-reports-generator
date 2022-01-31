import logging
from dataclasses import dataclass
from datetime import datetime
from typing import Optional

from dateutil.parser import isoparse

logger = logging.getLogger(__name__)


class MissingEnvironmentVariable(Exception):
    pass


class InvalidEnvironmentVariableValue(Exception):
    pass


class EnvConfig:
    def __init__(self, env_vars):
        self._env_vars = env_vars

    def _read_env(self, name: str, optional: bool, converter=None, default=None):  # noqa: C901
        try:
            env_var = self._env_vars[name]
            if converter:
                return converter(env_var)
            else:
                return env_var
        except KeyError:
            if optional:
                return default
            else:
                raise MissingEnvironmentVariable(
                    f"Expected environment variable {name} was not set, exiting..."
                )
        except ValueError:
            raise InvalidEnvironmentVariableValue(
                f"Expected environment variable {name} value is invalid, exiting..."
            )

    def read_str(self, name: str) -> str:
        return self._read_env(name, optional=False)

    def read_optional_str(self, name: str) -> Optional[str]:
        return self._read_env(name, optional=True)

    def read_optional_int(self, name: str) -> Optional[int]:
        return self._read_env(name, optional=True, converter=int)

    def read_optional_datetime(self, name: str) -> datetime:
        return self._read_env(name, optional=True, converter=isoparse)


@dataclass
class PipelineConfig:
    build_tag: str
    input_transfer_data_bucket: str
    output_reports_bucket: str
    start_datetime: Optional[datetime]
    end_datetime: Optional[datetime]
    number_of_months: Optional[int]
    number_of_days: Optional[int]
    cutoff_days: Optional[int]
    s3_endpoint_url: Optional[str]

    @classmethod
    def from_environment_variables(cls, env_vars):
        env = EnvConfig(env_vars)
        return cls(
            build_tag=env.read_str("BUILD_TAG"),
            input_transfer_data_bucket=env.read_str("INPUT_TRANSFER_DATA_BUCKET"),
            output_reports_bucket=env.read_str("OUTPUT_REPORTS_BUCKET"),
            start_datetime=env.read_optional_datetime("START_DATETIME"),
            end_datetime=env.read_optional_datetime("END_DATETIME"),
            number_of_months=env.read_optional_int("NUMBER_OF_MONTHS"),
            number_of_days=env.read_optional_int("NUMBER_OF_DAYS"),
            cutoff_days=env.read_optional_int("CONVERSATION_CUTOFF_DAYS"),
            s3_endpoint_url=env.read_optional_str("S3_ENDPOINT_URL"),
        )
