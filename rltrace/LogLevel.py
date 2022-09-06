import logging
from enum import IntEnum, unique


@unique
class LogLevel(IntEnum):
    """
    Enum to capture the current trace logging level
    """
    debug = 0
    info = 1
    warn = 2
    error = 3
    critical = 4

    def set(self,
            logger: logging.Logger):
        """
        Set the logging level according to the current value of the Enum.
        :param logger: The logger to set the level of
        :return:
        """
        if self.value == self.debug:
            logger.setLevel(logging.DEBUG)
        elif self.value == self.info:
            logger.setLevel(logging.INFO)
        elif self.value == self.warn:
            logger.setLevel(logging.WARN)
        elif self.value == self.error:
            logger.setLevel(logging.ERROR)
        elif self.value == self.critical:
            logger.setLevel(logging.CRITICAL)
        else:
            logger.setLevel(logging.INFO)
        return

    @staticmethod
    def new(level: str = None) -> 'LogLevel':
        """
        Convert debug argument as string to DebugLevel Enum
        :param level: The debug level as string debug, info etc
        :return: The Debug level as Enum with info as default if given level is not a valid value of DebugLevel
        """
        try:
            target_log_level = LogLevel[level]
        except Exception as _:
            target_log_level = LogLevel['info']
        return target_log_level
