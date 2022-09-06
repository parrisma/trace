from typing import Dict
from src.interface.envbuilder import EnvBuilder
from Trace import Trace
from TraceReport import TraceReport
from ElasticHandler import ElasticHandler
from elastic.ESUtil import ESUtil
from elasticsearch import Elasticsearch


class TraceElasticEnvBuilder(EnvBuilder):
    _es: Elasticsearch
    _settings: Settings
    _context: Dict
    _run_spec: RunSpec
    _trace: Trace

    def __init__(self,
                 context: Dict):
        self._context = context
        self._trace = self._context[EnvBuilder.TraceContext]
        self._run_spec = self._context[EnvBuilder.RunSpecificationContext]
        self._trace.log().info("Invoked : {}".format(str(self)))
        self._es = self._context.get(EnvBuilder.ElasticDbConnectionContext, None)
        if self._es is None:
            raise ValueError("Elastic Connection not available in supplied context")
        self._settings = Settings(settings_yaml_stream=WebStream(self._run_spec.trace_settings_yaml()),
                                  bespoke_transforms=self._run_spec.setting_transformers())
        return

    def execute(self,
                purge: bool) -> None:
        """
        Execute actions to build the element of the environment owned by this builder
        :return: None: Implementation should throw and exception to indicate failure
        """
        self._establish_trace_log(purge)
        self._establish_notification_log(purge)
        self._add_trace_reporter_to_context()
        return

    def _establish_notification_log(self,
                                    purge: bool) -> None:
        """
        Establish the notification_log index. The notification log is where all traffic between SrcSink's is logged.
        This is to support analysis on SrcSink relationships and build notification flow graphs/networks.
        """
        index_name, index_json = self._settings.notification_log()
        self._trace.log().info("Establishing Index [{}]".format(index_name))

        # Delete notification_log index if purge is enabled
        if purge:
            ESUtil.delete_index(es=self._es,
                                idx_name=index_name)

        # Create notification_log index if it does not exist.
        ESUtil.create_index_from_json(es=self._es,
                                      idx_name=index_name,
                                      mappings_as_json=WebStream(index_json))

        return

    def _establish_trace_log(self,
                             purge: bool) -> None:
        """
        Establish the trace_log index. The trace_log index is where all general logging goes.
        :param purge: If True delete trace_log index and all data before creating index from Json
        """
        index_name, index_json = self._settings.trace_log()
        self._trace.log().info("Establishing Index [{}]".format(index_name))

        # Delete trace_log index if purge is enabled.
        if purge:
            ESUtil.delete_index(es=self._es,
                                idx_name=index_name)

        # Create trace_log index from Json
        ESUtil.create_index_from_json(es=self._es,
                                      idx_name=index_name,
                                      mappings_as_json=WebStream(index_json))

        self._trace.log().info("[{}] index established".format(index_name))
        # Enable the logging handler that will replicate logs to the trace_log index
        self._trace.enable_elastic_handler(ElasticHandler(es=self._es, trace_log_index_name=index_name))
        self._trace.log().info("[{}] Elastic logging handler installed".format(index_name))
        return

    def _add_trace_reporter_to_context(self) -> None:
        """
        Create a TraceReport object and add it to the context.
        """
        trace_log_index_name, _ = self._settings.trace_log()
        notification_log_index_name, _ = self._settings.notification_log()
        self._context[EnvBuilder.TraceReport] = TraceReport(es=self._es,
                                                            trace_log_index_name=trace_log_index_name,
                                                            notification_log_index_name=notification_log_index_name)
        self._trace.log().info("Added TraceReporter to environment context")
        return

    def uuid(self) -> str:
        """
        The immutable UUID of this build phase. This should be fixed at the time of coding as it is
        used in the environment factory settings to sequence build stages
        :return: immutable UUID
        """
        return "672c73af91e942c1a5685545411d919b"

    def __str__(self) -> str:
        return "Trace Elastic Logging Builder - Id: {}".format(self.uuid())

    def __repr__(self):
        return self.__str__()
