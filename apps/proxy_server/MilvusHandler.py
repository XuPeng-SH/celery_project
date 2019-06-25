import logging
from milvus import Milvus, Prepare, IndexType, Status
from milvus.client.Exceptions import NotConnectError
from milvus.thrift.ttypes import (TopKQueryResult,
                                  QueryResult,
                                  Exception as ThriftExeception)

# import workflow
from milvus_celery.app_helper import create_app
from configurations import config
import sys
celery_app = create_app(config=config)

LOGGER = logging.getLogger('proxy_server')

class MilvusHandler:

    def __init__(self, uri):
        self.log = {}
        self.uri = uri
        self.retry_times = 0
        self.normal_times = 0
        self.thrift_client = None

    @property
    def client(self):
        if self.thrift_client:
            return self.thrift_client

        self.thrift_client = Milvus()
        self.thrift_client.connect(uri=self.uri)
        return self.thrift_client

    @property
    def can_retry(self):
        if self.normal_times >= 10:
            self.retry_times -= 1
            self.normal_times -= 10
        return self.retry_times < 3

    def Ping(self, args):
        LOGGER.info('Ping {}'.format(args))
        try:
            ans = self.client.server_status(args)
            if args == 'version':
                ans = self.client.server_version()
            self.normal_times += 1
            return ans
        except NotConnectError as e:
            if not self.can_retry:
                sys.exit(1)

            self.thrift_client = None
            self.retry_times += 1
            LOGGER.info('Reconnecting ... {}'.format(self.retry_times))


    def CreateTable(self, param):
        LOGGER.info('CreateTable: {}'.format(param))
        try:
            status = self.client.create_table(param)
            if not status.OK():
                raise ThriftExeception(code=status.code, reason=status.message)
            return status
        except NotConnectError as e:
            if not self.can_retry:
                sys.exit(1)

            self.thrift_client = None
            self.retry_times += 1
            LOGGER.info('Reconnecting ... {}'.format(self.retry_times))

    def DeleteTable(self, table_name):
        LOGGER.info('DeleteTalbe: {}'.format(table_name))
        try:
            status = self.client.delete_table(table_name)
            if not status.OK():
                raise ThriftExeception(code=status.code, reason=status.message)
            return table_name
        except NotConnectError as e:
            if not self.can_retry:
                sys.exit(1)

            self.thrift_client = None
            self.retry_times += 1
            LOGGER.info('Reconnecting ... {}'.format(self.retry_times))

    def AddVector(self, table_name, record_array):
        LOGGER.info('AddVectors to: {}'.format(table_name))
        try:
            status, ids = self.client.add_vectors(table_name, record_array)
            if not status.OK():
                raise ThriftExeception(code=status.code, reason=status.message)
            return ids
        except NotConnectError as e:
            if not self.can_retry:
                sys.exit(1)

            self.thrift_client = None
            self.retry_times += 1
            LOGGER.info('Reconnecting ... {}'.format(self.retry_times))

    def SearchVector(self, table_name, query_record_array, query_range_array, topk):
        try:
            async_result = workflow.query_vectors_1_n_1_workflow(table_name,
                                                                 query_record_array,
                                                                 topk,
                                                                 query_range_array)

            result = async_result.get(propagate=True, follow_parents=True)

            out = []
            for each_request_topk in result:
                inner = [QueryResult(id=each_result.id, score=each_result.score)
                        for each_result in each_request_topk]
                out.append(TopKQueryResult(inner))

            return out
        except Exception as exc:
            # TODO
            return []

    def SearchVectorInFiles(self, table_name, file_id_array, query_record_array, query_range_array, topk):
        LOGGER.info('Searching Vectors in files...')
        res = search_vector_in_files.delay(table_name=table_name,
                                           file_id_array=file_id_array,
                                           query_record_array=query_record_array,
                                           query_range_array=query_range_array,
                                           topk=topk)
        status, result = res.get(timeout=1)

        if not status.OK():
            raise ThriftExeception(code=status.code, reason=status.message)
        res = TopKQueryResult()
        for top_k_query_results in result:
            res.query_result_arrays.append([QueryResult(id=qr.id, score=qr.score)
                                            for qr in top_k_query_results])
        return res

    def DescribeTable(self, table_name):
        LOGGER.info('Describing table: {}'.format(table_name))
        try:
            status, table = self.client.describe_table(table_name)
            if not status.OK():
                raise ThriftExeception(code=status.code, reason=status.message)
            return table
        except NotConnectError as e:
            if not self.can_retry:
                sys.exit(1)

            self.thrift_client = None
            self.retry_times += 1
            LOGGER.info('Reconnecting ... {}'.format(self.retry_times))

    def GetTableRowCount(self, table_name):
        LOGGER.info('GetTableRowCount: {}'.format(table_name))
        try:
            status, count = self.client.get_table_row_count(table_name)
            if not status.OK():
                raise ThriftExeception(code=status.code, reason=status.message)
            return count
        except NotConnectError as e:
            if not self.can_retry:
                sys.exit(1)

            self.thrift_client = None
            self.retry_times += 1
            LOGGER.info('Reconnecting ... {}'.format(self.retry_times))

    def ShowTables(self):
        LOGGER.info('ShowTables ...')
        try:
            status, tables = self.client.show_tables()
            LOGGER.debug(status)
            if not status.OK():
                raise ThriftExeception(code=status.code, reason=status.message)
            return tables
        except NotConnectError as e:
            if not self.can_retry:
                sys.exit(1)

            self.thrift_client = None
            self.retry_times += 1
            LOGGER.info('Reconnecting ... {}'.format(self.retry_times))
