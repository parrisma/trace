# ----------------------------------------------------------------------------
# MIT License
#
# Copyright (c) 2022 parris3142
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.
# ----------------------------------------------------------------------------

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

    # %f - milliseconds not supported on Windows for 'time' module
    _CONSOLE_FORMATTER = logging.Formatter("%(asctime)s — %(name)s - %(levelname)s — %(message)s",
                                           datefmt='%Y-%m-%dT%H:%M:%S.%z')

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

    def enable_handler(self,
                       handler: logging.Handler) -> None:
        """
        Attach the handler as an additional sink.
        :param handler: The log handler to attach
        """
        if handler is None:
            raise ValueError("Given handler to enable is None")
        if not isinstance(handler, logging.Handler):
            raise ValueError(f'Expected handler but given {handler.__class__.name}')

        handler.setLevel(level=self._logger.level)
        self._logger.addHandler(handler)

        return

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
