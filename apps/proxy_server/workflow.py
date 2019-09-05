import logging
import datetime
import time
import asyncio
from concurrent.futures import ThreadPoolExecutor
from milvus_celery.operations import SDKClient
from milvus.thrift.ttypes import Exception as ThriftExeception, ErrorCode
from milvus.thrift.ttypes import (Exception as ThriftException,)
from query_tasks_worker.tasks import (merge_query_results,
        get_queryable_files, schedule_query, tranpose_n_topk_results,
        reduce_one_request_files_results, reduce_n_request_files_results)

from server_sidecar_worker.tasks import query_files
import settings
from proxy_server import redis_client

logger = logging.getLogger(__name__)

def propagate_chain_get(terminal_node, timeout=None):
    node = terminal_node.parent
    while node:
        node.get(propagate=True, timeout=timeout)
        node = node.parent

def range_to_date(range_obj):
    try:
        start = datetime.datetime.strptime(range_obj.start_value, '%Y-%m-%d')
        end = datetime.datetime.strptime(range_obj.end_value, '%Y-%m-%d')
    except ValueError:
        raise ThriftExeception(code=ErrorCode.ILLEGAL_RANGE, reason='Invalid time range: {} {}'.format(
            range_obj.start_value, range_obj.end_value
            ))

    if start >= end:
        raise ThriftExeception(code=ErrorCode.ILLEGAL_RANGE, reason='Invalid time range: {} to {}'.format(
            range_obj.start_value, range_obj.end_value
            ))

    return ((start.year-1900)*10000 + (start.month-1)*100 + start.day
            , (end.year-1900)*10000 + (end.month-1)*100 + end.day)

###########################################################################
##                   1. QueryFile
## QueryFileList ->  2. QueryFile  -> Merge
##                      ...
###########################################################################


def query_vectors_1_n_1_workflow(table_id, vectors, topK, range_array=None):
    queue = settings.QUEUES
    reducer = merge_query_results.s(topK=topK).set(queue=queue)
    reducer_result = reducer.freeze()
    range_array = [range_to_date(r) for r in range_array] if range_array else None

    r = (
            get_queryable_files.s(table_id, range_array).set(queue=queue)
            | schedule_query.s(query_files.s(vectors, topK).set(queue=queue), reducer).set(queue=queue)
        )()

    propagate_chain_get(r)

    return reducer_result


def query_vectors(table_id, vectors, topK, range_array=None):
    queue = settings.QUEUES
    range_array = [range_to_date(r) for r in range_array] if range_array else None
    async_result = get_queryable_files.s(table_id, range_array).set(queue=queue).apply_async()
    routing = async_result.get(propagate=True)
    logger.debug(routing)

    rs = []
    all_topk_results = []

    thread_pool_size = settings.THREAD_POOL_SIZE

    def func(remote_id, query_params, vectors, topK):
        logger.info('Start Query @{}'.format(time.time()))
        logger.debug('Querying {} params {} nq={} topK={}'.format(remote_id, query_params, len(vectors), topK))
        host = redis_client.client.get(remote_id)
        if not host:
            raise ThriftException(code=1001, reason='Internal Server Error')

        host = str(host, encoding='utf-8')
        logger.debug('redis host {}'.format(host))

        client = SDKClient(host=host)
        with client:
            start = time.time()
            ret = client.search_vectors_in_files(table_id=query_params['table_id'], file_ids=query_params['file_ids'],
                    query_records=vectors, topK=topK)
            end = time.time()
            logger.info('search_vectors_in_files takes: {}'.format(end - start))

        all_topk_results.append(ret)

    with ThreadPoolExecutor(max_workers=thread_pool_size) as pool:
        for remote_id, params in routing.items():
            r = pool.submit(func, remote_id, params, vectors, topK)
            rs.append(r)

        for r in rs:
            r.result()

    results = merge_query_results(all_topk_results, topK)
    return results.to_dict()['data'] if results else results
