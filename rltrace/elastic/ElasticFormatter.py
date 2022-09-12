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

from logging import Formatter, LogRecord
from typing import Dict
from elastic.ESUtil import ESUtil
import json


class ElasticFormatter(Formatter):
    json_log_fields = ["session_uuid", "level", "timestamp", "message"]

    # Allow cross-platform consistency of logging levels
    level_map = {50: "CRITICAL",
                 40: "ERROR",
                 30: "WARNING",
                 20: "INFO",
                 10: "DEBUG",
                 0: "NOTSET"
                 }

    def __init__(self):
        """
        Boostrap Elastic Log Formatter
        """
        super(ElasticFormatter, self).__init__()

        self._json_fields = ElasticFormatter.json_log_fields
        self._fmt = '{{{{"{}":"{{}}","{}":"{{}}","{}":"{{}}","{}":"{{}}"}}}}'.format(self._json_fields[0],
                                                                                     self._json_fields[1],
                                                                                     self._json_fields[2],
                                                                                     self._json_fields[3])
        self._date_formatter = ESUtil.DefaultElasticDateFormatter()
        self._level_map = ElasticFormatter.level_map
        return

    @staticmethod
    def default_level_map() -> Dict:
        return ElasticFormatter.level_map

    def _translate_level_no(self,
                            level_no: int) -> str:
        """
        Translate a logging level number into a uuid event type
        :param level_no: The level no to translate
        :return: UUID equivalent of the level no.
        """
        if self._level_map is not None:
            res = self._level_map.get(level_no, str(level_no))
        else:
            res = str(level_no)
        return res

    def format(self,
               record: LogRecord) -> str:
        """
        Extract Record (name, level, timestamp and message) and format as json ready to be written to elastic DB
        :param record: The logging record to parse
        :return: The log entry as JSON string
        """
        sess_n = record.name
        type_n = self._translate_level_no(record.levelno)
        trace_date = self._date_formatter.format(record.created)
        message = json.dumps(record.msg)[1:-1]  # ensure special characters are escaped eg ' & "
        json_msg = self._fmt.format(sess_n, type_n, trace_date, message)
        return json_msg
