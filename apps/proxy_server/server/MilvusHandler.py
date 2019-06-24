from milvus.thrift.ttypes import (TopKQueryResult,
                                  QueryResult,
                                  Exception)
from milvus import Milvus, Prepare, IndexType, Status
from settings import DefaultConfig
import logging
LOGGER = logging.getLogger(__name__)
from contextlib import contextmanager
from functools import wraps


# from query_tasks_worker.tasks import (merge_query_results,
#         get_queryable_files, schedule_query, tranpose_n_topk_results,
#         reduce_one_request_files_results, reduce_n_request_files_results)
#
# from server_sidecar_worker.tasks import query_files
#
# def propagate_chain_get(terminal_node, timeout=None):
#     node = terminal_node.parent
#     while node:
#         node.get(propagate=True, timeout=timeout)
#         node = node.parent
#
# ###########################################################################
# ##                   1. QueryFile
# ## QueryFileList ->  2. QueryFile  -> Merge
# ##                      ...
# ###########################################################################
# def query_vectors_1_n_1_workflow(table_id, vectors, topK):
#     queue = 'for_query'
#     reducer = merge_query_results.s(topK=topK).set(queue=queue)
#     reducer_result = reducer.freeze()
#
#     r = (
#             get_queryable_files.s(table_id).set(queue=queue)
#             | schedule_query.s(query_files.s(vectors, topK).set(queue=queue), reducer).set(queue=queue)
#         )()
#
#     propagate_chain_get(r)
#
#     return reducer_result

uri = DefaultConfig.THRIFTCLIENT_TRANSPORT
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
            raise Exception(code=status.code, reason=status.message)
        return status

    def DeleteTable(self, table_name):
        LOGGER.info('DeleteTalbe: {}'.format(table_name))
        status = _milvus.delete_table(table_name)
        if not status.OK():
            raise Exception(code=status.code, reason=status.message)
        return table_name

    def AddVector(self, table_name, record_array):
        LOGGER.info('AddVectors to: {}'.format(table_name))
        status, ids = _milvus.add_vectors(table_name, record_array)
        if not status.OK():
            raise Exception(code=status.code, reason=status.message)
        return ids

    def SearchVector(self, table_name, query_record_array, topk, query_range_array=None):

        LOGGER.info('Searching Vectors...')
        res = search_vector.delay(table_name=table_name, query_record_array=query_record_array,
                                  topk=topk, query_range_array=query_range_array)
        status, result = res.get(timeout=1)

        if not status.OK():
            raise Exception(code=status.code, reason=status.message)
        res = TopKQueryResult()
        for top_k_query_results in result:
            res.query_result_arrays.append([QueryResult(id=qr.id, score=qr.score)
                                            for qr in top_k_query_results])
        return res

    def SearchVectorInFiles(self, table_name, file_id_array, query_record_array, topk, query_range_array=None):
        LOGGER.info('Searching Vectors in files...')
        res = search_vector_in_files.delay(table_name=table_name,
                                           file_id_array=file_id_array,
                                           query_record_array=query_record_array,
                                           query_range_array=query_range_array,
                                           topk=topk)
        status, result = res.get(timeout=1)

        if not status.OK():
            raise Exception(code=status.code, reason=status.message)
        res = TopKQueryResult()
        for top_k_query_results in result:
            res.query_result_arrays.append([QueryResult(id=qr.id, score=qr.score)
                                            for qr in top_k_query_results])
        return res

    def DescribeTable(self, table_name):
        LOGGER.info('Describing table: {}'.format(table_name))
        status, table = _milvus.describe_table(table_name)
        if not status.OK():
            raise Exception(code=status.code, reason=status.message)
        return table

    def GetTableRowCount(self, table_name):
        LOGGER.info('GetTableRowCount: {}'.format(table_name))
        status, count = _milvus.get_table_row_count(table_name)
        if not status.OK():
            raise Exception(code=status.code, reason=status.message)
        return count

    def ShowTables(self):
        LOGGER.info('ShowTables ...')
        status, tables = _milvus.show_tables()
        if not status.OK():
            raise Exception(code=status.code, reason=status.message)
        return tables

