from random import randint
from functools import reduce
import numpy as np
from collections import defaultdict
from celery import maybe_signature, chord, group
from celery.utils.log import get_task_logger
from milvus import Milvus
from milvus.client.Abstract import TopKQueryResult, QueryResult
from common.factories import TopKQueryResultFactory

from query_tasks_worker.app import celery_app, db, redis_client, settings
from query_tasks_worker.models import Table
from milvus_celery.operations import SDKClient

from query_tasks_worker.exceptions import TableNotFoundException
from query_tasks_worker.factories import TableFactory, TableFileFactory

from milvus_celery.hash_ring import HashRing

logger = get_task_logger(__name__)


@celery_app.task
def get_queryable_files(table_id, date_range=None):
    try:
        table = db.Session.query(Table).filter_by(table_id=table_id).first()
    except Exception as exc:
        raise TableNotFoundException(exc)

    if not table:
        raise TableNotFoundException(table_id)

    files = table.files_to_search()
    result = [f.id for f in files]

    routing = {}
    servers = redis_client.smembers(settings.SERVERS_MONITOR_KEY)
    ring = HashRing(servers)
    logger.error(servers)
    for f in files:
        queue = ring.get_node(str(f.id))
        sub = routing.get(queue, None)
        if not sub:
            routing[queue] = {
                'table_id': table_id,
                'file_ids': []
            }
        routing[queue]['file_ids'].append(f.id)

    return routing

@celery_app.task
def query_files(routing, vectors, topK):
    logger.error('Querying routing {}'.format(routing))

    if not settings.MILVUS_CLIENT:
        results = [TopKQueryResultFactory() for _ in range(len(vectors))]
        return results

    client = SDKClient()
    with client:
        t = client.search_vectors(table_id='test01',
            query_records=vectors,
            topK=topK)
        results = [TopKQueryResult(r) for r in t]

    return results

@celery_app.task
def tranpose_n_topk_results(files_n_topk_results):
    results_array = np.asarray(files_n_topk_results).transpose()
    results = results_array.tolist()
    return results

@celery_app.task
def reduce_one_request_files_results(files_topk_results, topK):
    topk_results = []
    for file_topk_results in files_topk_results:
        topk_results.extend(file_topk_results)
        topk_results = sorted(topk_results, key=lambda x:x.score)[:topK]
    return TopKQueryResult(topk_results)

@celery_app.task
def reduce_n_request_files_results(topk_results):
    return topk_results

@celery_app.task
def merge_query_results(files_n_topk_results, topK):
    if not files_n_topk_results or topK <= 0:
        return []

    logger.debug(files_n_topk_results)

    request_results = defaultdict(list)

    for files_collection in files_n_topk_results:
        for request_pos, each_request_results in enumerate(files_collection):
            request_results[request_pos].extend(each_request_results)
            request_results[request_pos] = sorted(request_results[request_pos], key=lambda x: x.score,
                    reverse=True)[:topK]

    results = sorted(request_results.items())
    results = [value[1] for value in results]
    logger.debug(results)

    return results


@celery_app.task(bind=True)
def schedule_query(self, source_data, mapper, reducer=None):
    mapper = maybe_signature(mapper, self.app)
    reducer = maybe_signature(reducer, self.app) if reducer else None
    if isinstance(source_data, dict):
        logger.error(source_data)
        sub_tasks = [mapper.clone(args=((q,ids),)).set(queue=q) for q, ids in source_data.items()]
    else:
        sub_tasks = [mapper.clone(args=(data,)) for data in source_data]
    scheduled = chord(sub_tasks)(reducer) if reducer else group(sub_tasks)()

    return scheduled
