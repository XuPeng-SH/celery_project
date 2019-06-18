import random
from pprint import pprint
from milvus_app import settings
from milvus import Milvus, Prepare, IndexType, Status
from milvus_app.exceptions import (SDKClientConnectionException,
        SDKClientSearchVectorException)

class SDKClient(object):
    def __init__(self, host=None, port=None):
        self.host = settings.MILVUS_SERVER_HOST if host is None else host
        self.port = settings.MILVUS_SERVER_PORT if port is None else port


    def init_client(self):
        self.client = Milvus()
        try:
            status = self.client.connect(host=self.host, port=self.port)
        except Exception as exc:
            raise SDKClientConnectionException(str(exc))

        if status != Status.SUCCESS:
            raise SDKClientConnectionException(str(status))

    def __enter__(self):
        self.init_client()

    def __exit__(self, type, value, traceback):
        self.client.disconnect()
        self.client = None

    def search_vectors(self, table_id, query_records, topK):
        dim = 256
        query_records = Prepare.records([[random.random()for _ in range(dim)] for _ in range(2)])
        table_id = 'test01'
        param = {
            'table_name': table_id,
            'query_records': query_records,
            'top_k': topK,
        }
        try:
            status, results = self.client.search_vectors(**param)
        except Exception as exc:
            raise SDKClientSearchVectorException(str(exc))

        if status != Status.SUCCESS:
            raise SDKClientSearchVectorException(str(status))
        pprint(results)

        return results
