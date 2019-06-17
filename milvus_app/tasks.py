from random import randint
from functools import reduce
import numpy as np
from celery import maybe_signature, chord
from celery.utils.log import get_task_logger
from milvus.client.Abstract import TopKQueryResult, QueryResult

from milvus_app import celery_app, db
from milvus_app.models import Table

from milvus_app.exceptions import TableNotFoundException
from milvus_app.factories import TopKQueryResultFactory

logger = get_task_logger(__name__)


@celery_app.task
def get_queryable_files(table_id, date_range=None):
    table = db.Session.query(Table).filter_by(table_id=table_id).first()

    if not table:
        raise TableNotFoundException(table_id)

    files = table.files_to_search()
    result = [f.id for f in files]
    # logger.error('Result={}'.format(result))
    return result

@celery_app.task
def query_file(file_id, vectors, topK):
    logger.error('Querying file {}'.format(file_id))

    # <<<TODO: ---Mock Now---------
    results = [TopKQueryResultFactory() for _ in range(len(vectors))]
    for r in results:
        logger.error(r)
    # >>>TODO: ---Mock Now---------

    return results

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
def schedule_query(self, file_ids, mapper, reducer):
    mapper = maybe_signature(mapper, self.app)
    reducer = maybe_signature(reducer, self.app)

    sub_tasks = [mapper.clone(args=(file_id,)) for file_id in file_ids]
    scheduled = chord(sub_tasks)(reducer)

    return scheduled
