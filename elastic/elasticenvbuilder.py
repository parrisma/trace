from elasticsearch import Elasticsearch
from envbuilder import EnvBuilder
from elastic.ESUtil import ESUtil
from Trace import Trace


class ElasticEnvBuilder(EnvBuilder):
    """
    Container to bootstrap and manage an elastic-db session
    """
    _es: Elasticsearch
    _trace: Trace

    def __init__(self,
                 trace: Trace,
                 elastic_server_port_id: str,
                 elastic_server_hostname: str):
        """
        Constructor.
        :param trace: The trace logger to log events to
        :param elastic_server_hostname: The hostname were the elastic search server is running
        :param elastic_server_port_id: The port id on the for the elastic search server
        """
        self._trace = trace
        self._hostname = elastic_server_hostname
        self._port_id = elastic_server_port_id
        self._es = None
        self._trace.log().info("Invoked : {}".format(str(self)))
        pass

    def execute(self,
                purge: bool) -> None:
        """
        Execute actions to establish the elastic environment.
        Get the environment specific settings for elastic host and port, open a connection and save into the bootstrap
        context
        :param purge: If true eliminate any existing data and context
        :return: None: Implementation should throw and exception to indicate failure
        """
        self._trace.log().info("Initiating connection to Elastic DB")
        self._es = ESUtil.get_connection(hostname=self._hostname, port_id=self._port_id)
        self._trace.log().info("Connected to {}".format(str(self._es)))
        return

    def uuid(self) -> str:
        """
        The immutable UUID of this build phase. This should be fixed at the time of coding as it is
        used in the environment factory settings to sequence build stages
        :return: immutable UUID
        """
        return "55cd885be0004c6d84857c9cd260e417"

    def __str__(self) -> str:
        """
        A string representation of the environment
        :return: A string representation
        """
        return "Elastic Environment Builder - Id: {}".format(self.uuid())

    def __repr__(self):
        """
        A string representation of the environment for inspection & debug
        :return: A string representation
        """
        return self.__str__()
