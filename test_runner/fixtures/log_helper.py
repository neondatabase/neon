import logging
import logging.config

"""
This file configures logging to use in python tests.
Logs are automatically captured and shown in their
own section after all tests are executed. 

To see logs for all (even successful) tests, run
pytest with the following command:
- `pipenv run pytest -n8 -rA`

More log config can be set in pytest.ini file.
You can add `log_cli = true` to it to watch
logs in real time.

To get more info about logging with pytest, see
https://docs.pytest.org/en/6.2.x/logging.html
"""

LOGGING = {
    "version": 1,
    "loggers": {
        "root": {
            "level": "DEBUG"
        },
        "root.wal_acceptor_async": { 
            "level": "INFO" # lot of logs on DEBUG level
        }
    }
}

def getLogger(name='root') -> logging.Logger:
    """Method to get logger for tests.
    
    Should be used to get correctly initialized logger. """
    return logging.getLogger(name)

# default logger for tests
log = getLogger()

logging.config.dictConfig(LOGGING)
