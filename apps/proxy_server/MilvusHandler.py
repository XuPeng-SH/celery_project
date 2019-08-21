import struct
import logging
import time

from milvus import Milvus, IndexType, Status
from milvus.thrift.ttypes import (
        TopKQueryResult,
        TopKQueryBinResult,
        QueryResult,
        Exception as ThriftException)

from celery.exceptions import ChordError
from query_tasks_worker.exceptions import TableNotFoundException
import workflow

from proxy_server import api

LOGGER = logging.getLogger('proxy_server')


class MilvusHandler:

    def __init__(self):
        self.log = {}
        self.thrift_client = None

    @property
    def client(self):
        self.thrift_client = api.client
        return self.thrift_client

    @api.connect
    def Ping(self, args):
        LOGGER.info('Ping {}'.format(args))
        status, ans = self.client.server_status(args)
        if args == 'version':
            status, ans = self.client.server_version()
        if status.OK():
            return ans

    @api.connect
    def CreateTable(self, param):
        LOGGER.info('CreateTable: {}'.format(param.table_name))
        status = self.client.create_table(param)
        if not status.OK():
            raise ThriftException(code=status.code, reason=status.message)
        return status

    @api.connect
    def HasTable(self, table_name):
        LOGGER.info('If {} exsits...'.format(table_name))
        has_table = self.client.has_table(table_name)

        return has_table

    @api.connect
    def BuildIndex(self, table_name):
        LOGGER.info('BuildIndex: {}'.format(table_name))
        status = self.client.build_index(table_name)
        if not status.OK():
            raise ThriftException(code=status.code, reason=status.message)
        return table_name

    @api.connect
    def DeleteTable(self, table_name):
        LOGGER.info('DeleteTalbe: {}'.format(table_name))
        status = self.client.delete_table(table_name)
        if not status.OK():
            raise ThriftException(code=status.code, reason=status.message)
        return table_name

    @api.connect
    def AddVector(self, table_name, record_array):
        LOGGER.info('AddVectors to: {}'.format(table_name))
        status, ids = self.client.add_vectors(table_name, record_array)
        if not status.OK():
            raise ThriftException(code=status.code, reason=status.message)
        return ids

    @api.error_collector
    def SearchVector(self, table_name, query_record_array, query_range_array, topk):
        try:
            async_result = workflow.query_vectors_1_n_1_workflow(table_name,
                                                                 query_record_array,
                                                                 topk,
                                                                 query_range_array)
        except TableNotFoundException as exc:
            raise ThriftException(code=exc.code, reason=exc.message)

        try:
            result = async_result.get(propagate=True, follow_parents=True)
        except ChordError as exc:
            message = str(exc)
            start = message.index('Status')
            end = message[start:].index(')')
            message = message[start:start+end]
            LOGGER.error(message)
            submsg = message[7:-1]
            infos = [s.strip() for s in submsg.split(',')]
            infos = [info.split('=')[1] for info in infos]
            raise ThriftException(code=int(infos[0]), reason=infos[1])
        except TableNotFoundException as exc:
            raise ThriftException(code=exc.code, reason=exc.message)
        except Exception as exc:
            LOGGER.error(exc)

        out = []
        for each_request_topk in result:
            inner = [QueryResult(id=each_result.id, distance=each_result.distance)
                    for each_result in each_request_topk]
            out.append(TopKQueryResult(inner))

        return out

    @api.error_collector
    def SearchVector2(self, table_name, query_record_array, query_range_array, topk):
        LOGGER.info('SearchVector2: {}'.format(table_name))
        start = time.time()
        try:
            async_result = workflow.query_vectors_1_n_1_workflow(table_name,
                                                                 query_record_array,
                                                                 topk,
                                                                 query_range_array)
        except TableNotFoundException as exc:
            raise ThriftException(code=exc.code, reason=exc.message)

        try:
            result = async_result.get(propagate=True, follow_parents=True)
        except ChordError as exc:
            message = str(exc)
            start = message.index('Status')
            end = message[start:].index(')')
            message = message[start:start+end]
            LOGGER.error(message)
            submsg = message[7:-1]
            infos = [s.strip() for s in submsg.split(',')]
            infos = [info.split('=')[1] for info in infos]
            raise ThriftException(code=int(infos[0]), reason=infos[1])
        except TableNotFoundException as exc:
            raise ThriftException(code=exc.code, reason=exc.message)
        except Exception as exc:
            LOGGER.error(exc)

        # LOGGER.debug(result)
        LOGGER.info('SearchVector takes: {}'.format(time.time()-start))
        start = time.time()
        out = []
        for each_request_topk in result:
            id_array, distance_array = [], []
            for each_result in each_request_topk:
                id_array.append(each_result.id)
                distance_array.append(each_result.distance)
            bin_result = TopKQueryBinResult(struct.pack(str(len(id_array))+'l', *id_array),
                    struct.pack(str(len(distance_array))+'d', *distance_array))

            out.append(bin_result)
        LOGGER.info('Prepare SearchResults takes: {}'.format(time.time()-start))

        return out

    @api.error_collector
    def SearchVectorInFiles(self, table_name, file_id_array, query_record_array, query_range_array, topk):
        LOGGER.info('Searching Vectors in files...')
        res = search_vector_in_files.delay(table_name=table_name,
                                           file_id_array=file_id_array,
                                           query_record_array=query_record_array,
                                           query_range_array=query_range_array,
                                           topk=topk)
        status, result = res.get(timeout=1)

        if not status.OK():
            raise ThriftException(code=status.code, reason=status.message)
        res = TopKQueryResult()
        for top_k_query_results in result:
            res.query_result_arrays.append([QueryResult(id=qr.id, distance=qr.distance)
                                            for qr in top_k_query_results])
        return res

    @api.connect
    def DescribeTable(self, table_name):
        LOGGER.info('Describing table: {}'.format(table_name))
        status, table = self.client.describe_table(table_name)
        if not status.OK():
            raise ThriftException(code=status.code, reason=status.message)
        return table

    @api.connect
    def GetTableRowCount(self, table_name):
        LOGGER.info('GetTableRowCount: {}'.format(table_name))
        status, count = self.client.get_table_row_count(table_name)
        if not status.OK():
            raise ThriftException(code=status.code, reason=status.message)
        return count

    @api.connect
    def ShowTables(self):
        LOGGER.info('ShowTables ...')
        status, tables = self.client.show_tables()
        if not status.OK():
            raise ThriftException(code=status.code, reason=status.message)
        return tables
