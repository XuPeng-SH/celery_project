import logging
from milvus import Milvus, Prepare, IndexType, Status
from milvus.thrift.ttypes import (TopKQueryResult,
                                  QueryResult,
                                  Exception as ThriftExeception)
import settings
import workflow

from milvus_celery.app_helper import create_app

from configurations import config
celery_app = create_app(config=config)

LOGGER = logging.getLogger(__name__)

uri = settings.THRIFTCLIENT_TRANSPORT
_milvus = Milvus()
_milvus.connect(uri=uri)


class MilvusHandler:

    def __init__(self):
        self.log = {}

    def Ping(self, args):
        LOGGER.info('Ping {}'.format(args))
        ans = _milvus.server_status(args)
        if args == 'version':
            ans = _milvus.server_version()

        return ans

    def CreateTable(self, param):
        LOGGER.info('CreateTable: {}'.format(param))
        status = _milvus.create_table(param)
        if not status.OK():
            raise ThriftExeception(code=status.code, reason=status.message)
        return status

    def DeleteTable(self, table_name):
        LOGGER.info('DeleteTalbe: {}'.format(table_name))
        status = _milvus.delete_table(table_name)
        if not status.OK():
            raise ThriftExeception(code=status.code, reason=status.message)
        return table_name

    def AddVector(self, table_name, record_array):
        LOGGER.info('AddVectors to: {}'.format(table_name))
        status, ids = _milvus.add_vectors(table_name, record_array)
        if not status.OK():
            raise ThriftExeception(code=status.code, reason=status.message)
        return ids

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
        status, table = _milvus.describe_table(table_name)
        if not status.OK():
            raise ThriftExeception(code=status.code, reason=status.message)
        return table

    def GetTableRowCount(self, table_name):
        LOGGER.info('GetTableRowCount: {}'.format(table_name))
        status, count = _milvus.get_table_row_count(table_name)
        if not status.OK():
            raise ThriftExeception(code=status.code, reason=status.message)
        return count

    def ShowTables(self):
        LOGGER.info('ShowTables ...')
        status, tables = _milvus.show_tables()
        if not status.OK():
            raise ThriftExeception(code=status.code, reason=status.message)
        return tables
