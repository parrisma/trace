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

import os
import logging
import sys
import os.path
from UniqueRef import UniqueRef
from LogLevel import LogLevel


class Trace:
    _TRACE_UNIQUE_NAME = 'Trace-73702c6afbb74892a5393278bd088bb4'
    # %f - milliseconds not supported on Windows for 'time' module
    _CONSOLE_FORMATTER = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s",
                                           datefmt='%Y-%m-%dT%H:%M:%S.%z')
    _pid = os.getpid()  # Used to detect multi-process mode.

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
                 log_level: LogLevel = LogLevel.new(),
                 log_dir_name: str = None,
                 log_file_name: str = 'rltrace.log',
                 session_uuid: str = None):
        """
        Establish trace logging
        :param log_level: The initial logging level
        :param log_dir_name: The directory where the log file is to be created, by default this is None and no log
                           : file is created. If the directory does not exist a Value error is thrown
        :param log_file_name: The name of the logfile, if default is rltrace.log
        """
        self._session_uuid = UniqueRef().ref if session_uuid is None else session_uuid
        self._elastic_handler = None
        self._console_handler = None
        self._file_handler = None
        self._log_level = log_level
        self._logger: logging.Logger = None
        self._log_dir_name = None
        self._log_file_name = None
        if log_dir_name is not None:
            if not os.path.isdir(log_dir_name):
                raise ValueError(f'Invalid directory name given for log file {log_dir_name}')
            else:
                self._log_dir_name = log_dir_name
                self._log_file_name = os.path.join(log_dir_name, log_file_name)
        self._bootstrap(log_level=log_level)
        return

    @classmethod
    def trace_unique_name(cls) -> str:
        return Trace._TRACE_UNIQUE_NAME

    @property
    def trace_console_handler_unique_name(self) -> str:
        return f'{self.trace_unique_name()}-ConsoleHandler'

    @property
    def trace_file_handler_unique_name(self) -> str:
        return f'{self.trace_unique_name()}-FileHandler'

    @property
    def session_uuid(self) -> str:
        return self._session_uuid

    @property
    def log_file_dir(self) -> str:
        return self._log_dir_name

    @property
    def current_log_level(self):
        return self._log_level

    def set_log_level(self,
                      level: int) -> None:
        self._log_level = level
        return

    def get_handler_by_name(self,
                            handler_name: str) -> logging.Handler:
        """
        Get the handler that matches the given name
        :param handler_name: The name of the handler to find and return
        :return: The matching handler object or None if not found
        """
        required_handler: logging.Handler = None
        for required_handler in self._logger.handlers:
            if required_handler.name == handler_name:
                break
        return required_handler

    def _bootstrap(self,
                   log_level: LogLevel) -> None:
        """
        Create a logger and enable the default console logger
        """

        # Is Trace already enabled via another call in this session
        self._logger = logging.getLogger(self.trace_unique_name())
        log_level.set(self._logger)
        self._logger.propagate = False  # Ensure only added handlers log i.e. disable parent logging handler
        self.enable_console_handler()
        self.enable_file_handler()
        if not isinstance(sys.stdout, Trace.StreamToLogger):
            sys.stdout = self.StreamToLogger(self._logger, logging.INFO)
        if not isinstance(sys.stderr, Trace.StreamToLogger):
            sys.stderr = self.StreamToLogger(self._logger, logging.ERROR)
        return

    def new_session(self) -> None:
        """
        Change the session id to a different, randomly generated GUID. This allows a specific subset of trace
        traffic to be selected from the overall handler capture.
        """
        self._session_uuid = UniqueRef().ref
        return

    def __list_loggers(self):
        """
        A debug function that can be used to dump out entire set of loggers and handlers.
        """
        print(self._logger)
        for h in self._logger.handlers:
            print('     %s' % h)

        for nm, lgr in logging.Logger.manager.loggerDict.items():
            print('+ [%-20s] %s ' % (nm, lgr))
            if not isinstance(lgr, logging.PlaceHolder):
                for h in lgr.handlers:
                    print('     %s' % h)

    def enable_console_handler(self) -> None:
        """
        Create the console handler and add it as a handler to the current logger
        """
        handler: logging.Handler = None
        for handler in self._logger.handlers:
            if handler.name == self.trace_console_handler_unique_name:
                self._console_handler = handler
                break

        if self._console_handler is None:
            self._console_handler = logging.StreamHandler(sys.stdout)
            self._console_handler.name = self.trace_console_handler_unique_name
            self._console_handler.setLevel(level=self._logger.level)
            self._console_handler.setFormatter(Trace._CONSOLE_FORMATTER)
            self._logger.addHandler(self._console_handler)
        return

    def enable_file_handler(self):
        """
        Attach a log to file handler
        """
        handler: logging.Handler = None
        for handler in self._logger.handlers:
            if handler.name == self.trace_file_handler_unique_name:
                self._file_handler = handler
                break

        if self._log_dir_name is not None:
            if self._file_handler is None:
                self._file_handler = logging.FileHandler(self._log_file_name)
                self._file_handler.name = self.trace_file_handler_unique_name
                self._file_handler.setLevel(level=self._logger.level)
                self._file_handler.setFormatter(Trace._CONSOLE_FORMATTER)
                self._logger.addHandler(self._file_handler)
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

        # Don't add handler if a handler of same name is already added
        if not self.contains_handler(handler.name):
            handler.setLevel(level=self._logger.level)
            self._logger.addHandler(handler)

        return

    def contains_handler(self,
                         handler_name: str) -> bool:
        """
        Check if a handler of the given name is already added to the Trace logger
        :param handler_name: The name of the handler to check for
        :return: True if teh given handler is mapped to the logger associated with the Trace object
        """
        return any([handler_name == h.name for h in self._logger.handlers])

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

    def log(self, msg: object, level: int = None) -> None:
        """
        Log the given message
        :param level: The log level
        :param msg: The message to log
        """
        log_level = self._log_level if level is None else level
        self._logger.log(log_level,
                         f'%s - {str(msg)}',
                         self._session_uuid,
                         extra={'session_uuid': self._session_uuid})
        return
