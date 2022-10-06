from elasticsearch import Elasticsearch
from abc import ABC, abstractmethod


class ElasticConnectionFactory(ABC):

    @abstractmethod
    def new_connection(self) -> Elasticsearch:
        """
        Create a new connection to Elasticsearch
        """
        pass
