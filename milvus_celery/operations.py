import random
from pprint import pprint
from milvus_celery import settings
from milvus import Milvus, Prepare, IndexType, Status
from milvus_celery.exceptions import (SDKClientConnectionException,
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

        return results

    def search_vectors_in_files(self, table_id, file_ids, query_records, topK, query_ranges=None):
        try:
            status, results = self.client.search_vectors_in_files(
                    table_id, file_ids, query_records, topK, query_ranges)
        except Exception as exc:
            raise SDKClientSearchVectorException(str(exc))

        if status != Status.SUCCESS:
            raise SDKClientSearchVectorException(str(status))

        return results
