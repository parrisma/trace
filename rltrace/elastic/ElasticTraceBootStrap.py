import json
from rltrace.Trace import Trace, LogLevel
from elastic.ESUtil import ESUtil
from elastic.ElasticHandler import ElasticHandler


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
                 *args,
                 **kwargs) -> None:
        """
        Create an elastic handler connected to the given elastic DB and associated it with the given trace object.

        If an index of teh given name does not exist, then create it from the given index definition.

        :param trace: The trace object to have the elastic handler attached
        :param hostname: The hostname where the elastic server is running
        :param port_id: The Port Id of the elastic server on the host, if None try to resolve via Kubernetes Service
        :param elastic_user: The elastic user to connect as, default = 'elastic'
        :param elastic_password: The elastic user password, default = 'changeme'
        :param index_name: The name of the index, if it does not exist it is created
        :param index_definition: The file name containing the JSON definition of the Index, if None use standard def
        :param kubernetes_namespace: The Kubernetes namespace where the elastic objects we defined, default = elastic
        """
        self._trace: Trace = Trace(log_level=initial_log_level) if trace is None else trace
        self._hostname: str = hostname
        self._port_id: int = port_id
        self._elastic_user: str = elastic_user
        self._elastic_password: str = elastic_password
        self._index_name: str = index_name
        self._index_definition: str = index_definition
        if self._index_definition is None:
            self._index_definition = '..\\elastic\\k8s-elastic\\elastic-log-index.json'
        self._kubernetes_namespace: str = kubernetes_namespace
        self._initial_log_level = initial_log_level

        self._es_connection = None

        self._validate_port_id()
        self._connect_to_elastic()
        self._validate_or_create_index()
        self._create_and_attach_elastic_handler()

        return

    @property
    def port_id(self) -> int:
        return self._port_id

    @property
    def index_name(self) -> str:
        return self._index_name

    @property
    def host_name(self) -> str:
        return self._hostname

    @property
    def elastic_connection(self):
        return self._es_connection

    @property
    def trace(self):
        return self._trace

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
            # Open connection to elastic
            if self._es_connection is None:
                self._es_connection = ESUtil.get_connection(hostname=self._hostname,
                                                            port_id=str(self._port_id),
                                                            elastic_user=self._elastic_user,
                                                            elastic_password=self._elastic_password)
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
                f = open(self._index_definition)
                index_mappings = json.load(f)
                f.close()
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
            elastic_handler = ElasticHandler(es=self._es_connection,
                                             trace_log_index_name=self._index_name)
            # Create trace logger and attach elastic handler.
            self._trace.enable_handler(elastic_handler)
        except Exception as e:
            raise RuntimeError(f'Failed to create elastic trace log handler {self._index_name}')
        return
