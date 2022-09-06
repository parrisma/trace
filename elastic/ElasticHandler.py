from logging import Handler, LogRecord
from elasticsearch import Elasticsearch


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
        return

    def emit(self,
             record: LogRecord) -> None:
        """
        Apply the associated formatter to the given LogRecord and persist it to Elastic
        :param record: The LogRecord to format and persist in elastic
        """
        msg = self.formatter.format(record=record)
        try:
            res = self._es.index(index=self._es_index,
                                 body=msg)
            if res.get('result', None) != 'created':
                raise RuntimeError("Bad Elastic return status [{}]".format(str(res)))
        except Exception as e:
            raise RuntimeError("Failed to write log to Elastic with exception [{}]".format(str(e)))
        return
