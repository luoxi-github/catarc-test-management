"""This is loggging module which is up to standart."""

import inspect
import logging
import logging.config

def logger_config(log_file, log_level):
    """
    It configures the logging module to log to a file, with a log level of your choice, and with a log
    format of your choice
    
    :param log_file: The path to the log file
    :param log_level: The level of logging. The default is INFO
    """

    config = {
        "version": 1,
        "formatters": {
            "default": {
                "format": f"%(asctime)s %(levelname)s {inspect.stack()[-1][1].split('/')[-1]}[%(process)d:%(thread)d] %(filename)s[%(funcName)s:%(lineno)d] %(message)s"
            }
        },
        "handlers": {
            "handler": {
                "formatter": "default",
                "class": "logging.handlers.TimedRotatingFileHandler",
                "filename": log_file,
                "when": "D",
                "interval": 1,
                "backupCount": 1
            }
        },
        "loggers": {"catarc": {"level": log_level, "handlers": ["handler"]}}
    }

    logging.raiseExceptions = False
    logging.config.dictConfig(config)

def get_logger():
    """
    It returns a logger object
    :return: The logger object.
    """

    return logging.getLogger("catarc")