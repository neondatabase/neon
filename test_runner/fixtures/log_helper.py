import logging
import logging.config
import re

"""
This file configures logging to use in python tests.
Logs are automatically captured and shown in their
own section after all tests are executed.

To see logs for all (even successful) tests, run
pytest with the following command:
- `poetry run pytest -n8 -rA`

Other log config can be set in pytest.ini file.
You can add `log_cli = true` to it to watch
logs in real time.

To get more info about logging with pytest, see
https://docs.pytest.org/en/6.2.x/logging.html
"""

# this config is only used for default log levels,
# log format is specified in pytest.ini file
LOGGING = {
    "version": 1,
    "loggers": {
        "root": {"level": "INFO"},
        "root.safekeeper_async": {"level": "INFO"},  # a lot of logs on DEBUG level
    },
}


class PasswordFilter(logging.Filter):
    """Filter out password from logs."""

    # Good enough to filter our passwords produced by PgProtocol.connstr
    FILTER = re.compile(r"(\s*)password=[^\s]+(\s*)")

    def filter(self, record: logging.LogRecord) -> bool:
        record.msg = self.FILTER.sub(r"\1password=<hidden>\2", record.msg)
        return True


def getLogger(name="root") -> logging.Logger:
    """Method to get logger for tests.

    Should be used to get correctly initialized logger."""
    return logging.getLogger(name)


# default logger for tests
log = getLogger()
log.addFilter(PasswordFilter())

logging.config.dictConfig(LOGGING)
