from elasticsearch import Elasticsearch
from rltrace.interface.ElasticConnectionFactory import ElasticConnectionFactory
from elastic.ESUtil import ESUtil


class TraceElasticConnectionFactory(ElasticConnectionFactory):

    def __init__(self,
                 hostname: str,
                 port_id: str,
                 elastic_user: str,
                 elastic_password) -> None:
        self._hostname = hostname
        self._port_id = port_id
        self._elastic_user = elastic_user
        self._elastic_password = elastic_password
        return

    def new_connection(self,
                       ) -> Elasticsearch:
        """
        Create a new connection to Elasticsearch
        """
        return ESUtil.get_connection(hostname=self._hostname,
                                     port_id=self._port_id,
                                     elastic_user=self._elastic_user,
                                     elastic_password=self._elastic_password)
