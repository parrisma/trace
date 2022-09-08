from logging import Handler, LogRecord
from elasticsearch import Elasticsearch
from elastic.ESUtil import ESUtil
from elastic.ElasticFormatter import ElasticFormatter


class ElasticHandler(Handler):
    _es: Elasticsearch

    def __init__(self,
                 es: Elasticsearch,
                 trace_log_index_name: str):
        """
        Connect to given Elastic instance.
        :param es: An elastic search connection object
        :param trace_log_index_name: The name of the elastic index to write logs to
        """
        Handler.__init__(self)
        self._es = es
        self._es_index = trace_log_index_name
        self.setFormatter(ElasticFormatter())
        return

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
