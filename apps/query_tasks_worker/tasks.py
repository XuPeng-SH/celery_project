from random import randint
from functools import reduce
import numpy as np
from collections import defaultdict
from celery import maybe_signature, chord, group
from celery.utils.log import get_task_logger
from milvus import Milvus
from milvus.client.Abstract import TopKQueryResult, QueryResult

from apps.query_tasks_worker.app import celery_app, db, redis_client, settings
from apps.query_tasks_worker.models import Table
from milvus_app.operations import SDKClient

from apps.query_tasks_worker.exceptions import TableNotFoundException
from apps.query_tasks_worker.factories import TopKQueryResultFactory

from milvus_app.hash_ring import HashRing

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

    routing = defaultdict(list)
    servers = redis_client.smembers(settings.SERVERS_MONITOR_KEY)
    ring = HashRing(servers)
    logger.error(servers)
    for f in files:
        target_server = ring.get_node(str(f.id))
        routing[target_server].append(f.id)

    return routing

@celery_app.task
def query_files(routing, vectors, topK):
    logger.error('Querying routing {}'.format(routing))

    # <<<TODO: ---Mock Now---------
    results = []
    # results = [TopKQueryResultFactory() for _ in range(len(vectors))]
    # logger.error('{} target server is {}'.format(file_id, target))
    # >>>TODO: ---Mock Now---------
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
        topk_results.extend(file_topk_results.query_results)
        topk_results = sorted(topk_results, key=lambda x:x.score)[:topK]
    return TopKQueryResult(topk_results)

@celery_app.task
def reduce_n_request_files_results(topk_results):
    return topk_results

@celery_app.task
def merge_query_results(files_n_topk_results, topK):
    if not files_n_topk_results or topK <= 0:
        return []

    results_array = np.asarray(files_n_topk_results).transpose()
    topk_results = []
    for files_topk_results in results_array:
        each_topk = []
        for file_topk_results in files_topk_results:
            each_topk.extend(file_topk_results.query_results)
            each_topk = sorted(each_topk, key=lambda x:x.score)[:topK]
        topk_results.append(TopKQueryResult(each_topk))

    logger.debug(topk_results)

    return topk_results

@celery_app.task(bind=True)
def schedule_query(self, source_data, mapper, reducer=None):
    mapper = maybe_signature(mapper, self.app)
    reducer = maybe_signature(reducer, self.app) if reducer else None
    if isinstance(source_data, dict):
        sub_tasks = [mapper.clone(args=(data,)) for data in source_data.items()]
    else:
        sub_tasks = [mapper.clone(args=(data,)) for data in source_data]
    scheduled = chord(sub_tasks)(reducer) if reducer else group(sub_tasks)()

    return scheduled
