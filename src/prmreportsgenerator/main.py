import logging
from os import environ

from prmreportsgenerator.config import PipelineConfig
from prmreportsgenerator.io.json_formatter import JsonFormatter
from prmreportsgenerator.reports_generator import ReportsGenerator

logger = logging.getLogger("prmreportsgenerator")


def _setup_logger():
    logger.setLevel(logging.INFO)
    formatter = JsonFormatter()
    handler = logging.StreamHandler()
    handler.setFormatter(formatter)
    logger.addHandler(handler)


def main():
    _setup_logger()
    config = PipelineConfig.from_environment_variables(environ)
    reports_generator = ReportsGenerator(config)
    reports_generator.run()


if __name__ == "__main__":
    main()
