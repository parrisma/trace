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
from enum import IntEnum, unique


@unique
class LogLevel(IntEnum):
    """
    Enum to capture the current trace logging level
    """
    debug = logging.DEBUG
    info = logging.INFO
    warn = logging.WARN
    error = logging.ERROR
    critical = logging.CRITICAL
    not_set = logging.NOTSET

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
