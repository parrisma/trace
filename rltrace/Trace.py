import logging
import sys
from UniqueRef import UniqueRef
from LogLevel import LogLevel


class Trace:
    # Annotation
    _CONSOLE_FORMATTER: logging.Formatter
    _logger: logging.Logger
    _console_handler: logging.Handler
    _session_uuid: str

    _CONSOLE_FORMATTER = logging.Formatter("%(asctime)s — %(name)s - %(levelname)s — %(message)s",
                                           datefmt='%Y-%m-%dT%H:%M:%S%.yaml')

    class StreamToLogger(object):
        """
        File-like stream object that redirects writes to a logger instance.
        """

        def __init__(self, logger, level):
            self.logger = logger
            self.level = level
            self.linebuf = ''

        def write(self, buf):
            for line in buf.rstrip().splitlines():
                self.logger.log(self.level, line.rstrip())

        def flush(self):
            pass

        def getvalue(self):
            return b''

    def __init__(self,
                 session_uuid: str = None,
                 log_level: LogLevel = LogLevel.new()):
        """
        Establish trace logging
        :param session_uuid: The session UUID to report trace messages as originating from
        :param log_level: The initial logging level
        """
        if session_uuid is None or len(session_uuid) == 0:
            session_uuid = UniqueRef().ref
        self._session_uuid = session_uuid
        self._elastic_handler = None
        self._console_handler = None
        self._logger = None
        self._bootstrap(session_uuid=session_uuid,
                        log_level=log_level)
        return

    def _bootstrap(self,
                   session_uuid: str,
                   log_level: LogLevel) -> None:
        """
        Create a logger and enable the default console logger
        :param session_uuid: The session uuid to use as the name of the logger
        """
        if self._logger is None:
            self._logger = logging.getLogger(session_uuid)
            log_level.set(self._logger)
            self._logger.propagate = False  # Ensure only added handlers log i.e. disable parent logging handler
            self.enable_console_handler()
            sys.stdout = self.StreamToLogger(self._logger, logging.INFO)
            sys.stderr = self.StreamToLogger(self._logger, logging.ERROR)
        return

    def enable_console_handler(self) -> None:
        """
        Create the console handler and add it as a handler to the current logger
        """
        if self._console_handler is None:
            self._console_handler = logging.StreamHandler(sys.stdout)
            self._console_handler.name = "{}-ConsoleHandler".format(self._logger.name)
            self._console_handler.setLevel(level=self._logger.level)
            self._console_handler.setFormatter(Trace._CONSOLE_FORMATTER)
            self._logger.addHandler(self._console_handler)
        return

    # def enable_elastic_handler(self,
    #                          elastic_handler: ElasticHandler) -> None:
    # """
    # Create the elastic handler and add it as a handler to the current logger
    # Note: elastic_handler contains the open connection to Elastic DB
    # """
    # if self._elastic_handler is None:
    #    self._elastic_handler = elastic_handler
    #    self._elastic_handler.name = "{}-ElasticHandler".format(self._logger.name)
    #    self._elastic_handler.setLevel(level=self._logger.level)
    #    self._elastic_handler.setFormatter(Trace._ELASTIC_FORMATTER)
    #    self._logger.addHandler(self._elastic_handler)
    #  return

    def enable_tf_capture(self,
                          tf_logger: logging.Logger) -> None:
        """
        Disable TF logging to console direct and re-direct to experiment trace console & elastic
        :param tf_logger: The tensorflow logger
        """

        loggers = [tf_logger]
        for logger in loggers:
            logger.handlers = []
            logger.propagate = False
            logger.addHandler(self._console_handler)
        return

    def log(self) -> logging.Logger:
        """
        Current logger
        :return: Session Logger
        """
        return self._logger

    def __call__(self, *args, **kwargs) -> logging.Logger:
        return self._logger
