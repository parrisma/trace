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

from logging import Handler, LogRecord
from elasticsearch import Elasticsearch
from elastic.ESUtil import ESUtil
from elastic.ElasticFormatter import ElasticFormatter
from rltrace.interface.MultiProcessHandler import MultiProcessHandler
from rltrace.interface.ElasticConnectionFactory import ElasticConnectionFactory


class ElasticHandler(MultiProcessHandler):
    _ELASTIC_HANDLER_UNIQUE_NAME: str = 'Trace-73702c6afbb74892a5393278bd088bb4-ElasticDBHandler'
    _es: Elasticsearch

    def __init__(self,
                 elastic_connection_factory: ElasticConnectionFactory,
                 trace_log_index_name: str):
        """
        Connect to given Elastic instance
        :param elastic_connection_factory: A connection factory that can create new elastic connections.
        :param trace_log_index_name: The name of the elastic index to write logs to
        """
        Handler.__init__(self)
        self._elastic_connection_factory = elastic_connection_factory
        self._es = self._elastic_connection_factory.new_connection()
        self._es_index = trace_log_index_name
        self.set_name(self.elastic_handler_unique_name())
        self.setFormatter(ElasticFormatter())
        return

    @classmethod
    def elastic_handler_unique_name(cls) -> str:
        return ElasticHandler._ELASTIC_HANDLER_UNIQUE_NAME

    @property
    def index_name(self) -> str:
        return self._es_index

    def emit(self,
             record: LogRecord) -> None:
        """
        Apply the associated formatter to the given LogRecord and persist it to Elastic
        :param record: The LogRecord to format and persist in elastic
        """
        msg = self.formatter.format(record=record)
        try:
            ESUtil.write_doc_to_index(es=self._es,
                                      idx_name=self._es_index,
                                      document_as_json_map=msg)
        except Exception as e:
            raise RuntimeError("Failed to write log to Elastic with exception [{}]".format(str(e)))
        return

    def reset_for_new_process(self) -> None:
        """
        Make any required changes to account for the fact that the Handler may have been forked into a new process
        """
        self._es = self._elastic_connection_factory.new_connection()
        return
