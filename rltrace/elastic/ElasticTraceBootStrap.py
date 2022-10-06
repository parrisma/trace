import json
import os
from rltrace.Trace import Trace, LogLevel
from elastic.ESUtil import ESUtil
from elastic.ElasticHandler import ElasticHandler
from elastic.ElasticResources import ElasticResources
from elastic.TraceElasticConnectionFactory import TraceElasticConnectionFactory


class ElasticTraceBootStrap:

    def __init__(self,
                 trace: Trace = None,
                 hostname: str = 'localhost',
                 port_id: int = None,
                 elastic_user: str = 'elastic',
                 elastic_password: str = 'changeme',
                 index_name: str = 'trace_index',
                 index_definition: str = None,
                 kubernetes_namespace: str = 'elastic',
                 initial_log_level: LogLevel = LogLevel.debug,
                 log_dir_name: str = None,
                 log_file_name: str = 'rltrace.log',
                 *args,
                 **kwargs) -> None:
        """
        Create an elastic handler connected to the given elastic DB and associated it with the given trace object.

        If an index of the given name does not exist, then create it from the given index definition

        :param trace: The trace object to have the elastic handler attached
        :param hostname: The hostname where the elastic server is running
        :param port_id: The Port Id of the elastic server on the host, if None try to resolve via Kubernetes Service
        :param elastic_user: The elastic user to connect as, default = 'elastic'
        :param elastic_password: The elastic user password, default = 'changeme'
        :param index_name: The name of the index, if it does not exist it is created
        :param index_definition: The file name containing the JSON definition of the Index, if None use standard def
        :param kubernetes_namespace: The Kubernetes namespace where the elastic objects we defined, default = elastic
        :param initial_log_level: The log level to set (Debug, Info etc.) default is debug
        :param log_dir_name: The directory where the log file is to be created, no log file if dir is (default) None
        :param log_file_name: The name of the logfile, default is rltrace.log
        """
        self._trace: Trace = Trace(log_level=initial_log_level,
                                   log_dir_name=log_dir_name,
                                   log_file_name=log_file_name) if trace is None else trace
        self._hostname: str = hostname
        self._port_id: int = port_id
        self._elastic_user: str = elastic_user
        self._elastic_password: str = elastic_password
        self._elastic_connection_factory = None
        self._index_name: str = index_name
        self._index_definition: str = index_definition  # Lazy evaluate
        self._kubernetes_namespace: str = kubernetes_namespace
        self._initial_log_level = initial_log_level

        self._es_connection = None

        if not self._trace.contains_handler(ElasticHandler.elastic_handler_unique_name()):
            self._validate_port_id()
            self._connect_to_elastic()
            self._validate_or_create_index()
            self._create_and_attach_elastic_handler()
        else:
            current_elastic_handler: ElasticHandler = self._trace.get_handler_by_name(  # NOQA
                ElasticHandler.elastic_handler_unique_name())  # NOQA
            if current_elastic_handler.index_name != self._index_name:
                raise ValueError(
                    f'Using Elastic index [{current_elastic_handler.index_name}] cannot switch to [{self._index_name}]')

        return

    def _get_index_definition_as_json(self,
                                      dir_or_full_path_and_filename: str):
        """
        Given either a full path or directory, construct the full path & name of the json file that defines the
        trace index, retrieve the JSON
        If given is None then use default definition
        If given is a directory, assume the file is called elastic-log-index.json
        Else use the full path and file name

        :param dir_or_full_path_and_filename: The full path and filename or the directory where file exists

        """
        if dir_or_full_path_and_filename is None:
            index_mappings = json.loads(ElasticResources.trace_index_definition_as_json())
        else:
            f = open(self._get_index_definition(self._index_definition))
            index_mappings = json.load(f)
            f.close()
        return index_mappings

    @staticmethod
    def _get_index_definition(dir_or_full_path_and_filename: str):
        """
        Given either a full path or directory, construct the full path & name of the json file that defines the
        trace index.
        If given is None then look in current working directory.
        If given is a directory, assume the file is called elastic-log-index.json
        :param dir_or_full_path_and_filename: The full path and filename or the directory where file exists
        """
        index_definition: str = None
        if dir_or_full_path_and_filename is None:
            dir_or_full_path_and_filename = '.\\'  # assume its in cwd

        if os.path.isdir(dir_or_full_path_and_filename):
            index_definition = ElasticResources.trace_index_definition_file(resource_root=dir_or_full_path_and_filename)
        elif os.path.isfile(dir_or_full_path_and_filename):
            index_definition = dir_or_full_path_and_filename
        else:
            raise ValueError(f'Index definition cannot be found {index_definition}')
        return index_definition

    @property
    def trace(self) -> Trace:
        return self._trace

    @property
    def index_name(self) -> str:
        return self._index_name

    @property
    def elastic_connection(self):
        # connection may be None as we only open it if an elastic handler is not already added to logger
        if self._es_connection is None:
            self._validate_port_id()
            self._connect_to_elastic()
        return self._es_connection

    def _validate_port_id(self) -> None:
        """
        If the port_id is none, try to resolve by looking for a node_port in the given K8s namespace. At this
        point no other resolution types are offered. Or if integer just return 'as is'
        """
        if self._port_id is None:
            self._port_id = ESUtil.get_elastic_node_port_number(namespace=self._kubernetes_namespace)
        else:
            raise ValueError(
                f'Elastic port must be supplied or discoverable as node_port in K8s namespace {self._kubernetes_namespace}')
        return

    def _connect_to_elastic(self) -> None:
        """
        Try to connect to elastic with the given details and credentials.
        """
        try:
            if self._elastic_connection_factory is None:
                self._elastic_connection_factory = TraceElasticConnectionFactory(hostname=self._hostname,
                                                                                 port_id=str(self._port_id),
                                                                                 elastic_user=self._elastic_user,
                                                                                 elastic_password=self._elastic_password)
            # Open connection to elastic
            if self._es_connection is None:
                self._es_connection = self._elastic_connection_factory.new_connection()
        except Exception as e:
            raise RuntimeError(f'Failed to connect to Elastic DB with errorL {str(e)}')

    def _validate_or_create_index(self) -> None:
        """
        If the index exists do nothing else create the index with the given definition
        """
        try:
            # Test create index
            if not ESUtil.index_exists(es=self._es_connection,
                                       idx_name=self._index_name):
                index_mappings = self._get_index_definition_as_json(self._index_definition)
                res = ESUtil.create_index_from_json(es=self._es_connection, idx_name=self._index_name,
                                                    mappings_as_json=index_mappings)
                if not res:
                    raise ValueError(f'Failed to create Trace index with name {self._index_name}')
        except Exception as e:
            raise RuntimeError(f'Failed to create elastic index {self._index_name}')

        return

    def _create_and_attach_elastic_handler(self) -> None:
        """
        Create an elastic handler and bind to the trace object
        """
        try:
            elastic_handler = ElasticHandler(elastic_connection_factory=self._elastic_connection_factory,
                                             trace_log_index_name=self._index_name)
            # Create trace logger and attach elastic handler.
            self._trace.enable_handler(elastic_handler)
        except Exception as e:
            raise RuntimeError(f'Failed to create elastic trace log handler {self._index_name}')
        return
