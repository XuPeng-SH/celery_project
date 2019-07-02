import sys
import inspect
import logging
import socket
from functools import wraps
from celery.exceptions import ChordError
from milvus import Milvus, Prepare, IndexType, Status
from milvus.client.Exceptions import NotConnectError
from milvus.thrift.ttypes import (TopKQueryResult,
                                  QueryResult,
                                  Exception as ThriftException)
from query_tasks_worker.exceptions import TableNotFoundException

import workflow
import settings

LOGGER = logging.getLogger('proxy_server')
RECONNECT_URI = settings.THRIFTCLIENT_TRANSPORT


class ConnectionHandler:
    def __init__(self, uri):
        self.uri = uri
        self._retry_times = 0
        self._normal_times = 0
        self.thrift_client = None
        self.err_handlers = {}
        self.default_error_handler = None

    @property
    def client(self):
        self.thrift_client = Milvus()
        self.thrift_client.connect(uri=self.uri)
        return self.thrift_client

    def reconnect(self, uri=None):
        self.uri = uri if uri else self.uri
        self.thrift_client = None


    @property
    def can_retry(self):
        if self._normal_times >= settings.THRIFTCLIENT_NORMAL_TIME:
            self._retry_times = self._retry_times - 1 if self._retry_times > 0 else 0
            self._normal_times -= settings.THRIFTCLIENT_NORMAL_TIME
        return self._retry_times <= settings.THRIFTCLIENT_RETRY_TIME

    def retry(self, f):
        @wraps(f)
        def wrappers(*args, **kwargs):
            while self.can_retry:
                try:
                    return f(*args, **kwargs)

                except Exception as e:
                    handler = self.err_handlers.get(e.__class__, None)
                    if handler:
                        handler(e)
                    else:
                        raise e
                    LOGGER.error(e)
        return wrappers

    def err_handler(self, exception):
        if inspect.isclass(exception) and issubclass(exception, Exception):
            def wrappers(func):
                self.err_handlers[exception] = func
                return func
            return wrappers
        else:
            self.default_error_handler = exception
            return exception
    


connect = ConnectionHandler(uri=settings.THRIFTCLIENT_TRANSPORT)

class err:
    def __init__(self):
        self.err_handlers = {}
        self.default_error_handler = None


    def err_handler(self, exception):
        if inspect.isclass(exception) and issubclass(exception, Exception):
            def wrappers(func):
                self.err_handlers[exception] = func
                return func
            return wrappers
        else:
            self.default_error_handler = exception
            return exception

@connect.err_handler(ThriftException)
def ThriftExceptionHandler(e):
    LOGGER.error(e)
    raise e

@connect.err_handler(NotConnectError)
def NotConnectErrorHandler(e):
    LOGGER.error(e)
    connect._retry_times += 1
    if connect.can_retry:
        LOGGER.warning('Reconnecting .. {}'.format(connect._retry_times))
        connect.reconnect(RECONNECT_URI)
    else:
        sys.exit(1)


@connect.err_handler(socket.timeout)
def TimeOutHandler(e):
    LOGGER.error(e)
    connect._retry_times += 1
    if connect.can_retry:
        LOGGER.warning('Reconnecting .. {}'.format(connect._retry_times))
        connect.reconnect(RECONNECT_URI)
    else:
        sys.exit(1)


class MilvusHandler:

    def __init__(self):
        self.log = {}
        self.thrift_client = None

    @property
    def client(self):
        global connect

        self.thrift_client = connect.client
        return self.thrift_client

    @connect.retry
    def Ping(self, args):
        LOGGER.info('Ping {}'.format(args))
        status, ans = self.client.server_status(args)
        if args == 'version':
            status, ans = self.client.server_version()
        if status.OK():
            return ans

    @connect.retry
    def CreateTable(self, param):
        LOGGER.info('CreateTable: {}'.format(param))
        status = self.client.create_table(param)
        if not status.OK():
            raise ThriftException(code=status.code, reason=status.message)
        return status

    @connect.retry
    def HasTable(self, table_name):
        LOGGER.info('If {} exsits...'.format(table_name))
        has_table = self.client.has_table(table_name)
        return has_table

    @connect.retry
    def DeleteTable(self, table_name):
        LOGGER.info('DeleteTalbe: {}'.format(table_name))
        status = self.client.delete_table(table_name)
        if not status.OK():
            raise ThriftException(code=status.code, reason=status.message)
        return table_name

    @connect.retry
    def AddVector(self, table_name, record_array):
        LOGGER.info('AddVectors to: {}'.format(table_name))
        status, ids = self.client.add_vectors(table_name, record_array)
        if not status.OK():
            raise ThriftException(code=status.code, reason=status.message)
        return ids

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
            raise ThriftExeception(code=int(infos[0]), reason=infos[1])
        except TableNotFoundException as exc:
            raise ThriftException(code=exc.code, reason=exc.message)
        except Exception as exc:
            LOGGER.error(exc)

        out = []
        for each_request_topk in result:
            inner = [QueryResult(id=each_result.id, score=each_result.score)
                    for each_result in each_request_topk]
            out.append(TopKQueryResult(inner))

        return out

    @connect.retry
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
            res.query_result_arrays.append([QueryResult(id=qr.id, score=qr.score)
                                            for qr in top_k_query_results])
        return res

    @connect.retry
    def DescribeTable(self, table_name):
        LOGGER.info('Describing table: {}'.format(table_name))
        status, table = self.client.describe_table(table_name)
        if not status.OK():
            raise ThriftException(code=status.code, reason=status.message)
        return table

    @connect.retry
    def GetTableRowCount(self, table_name):
        LOGGER.info('GetTableRowCount: {}'.format(table_name))
        status, count = self.client.get_table_row_count(table_name)
        if not status.OK():
            raise ThriftException(code=status.code, reason=status.message)
        return count

    @connect.retry
    def ShowTables(self):
        LOGGER.info('ShowTables ...')
        status, tables = self.client.show_tables()
        if not status.OK():
            raise ThriftException(code=status.code, reason=status.message)
        return tables
