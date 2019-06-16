from random import randint
from functools import reduce
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

    return TopKQueryResultFactory()

@celery_app.task
def merge_query_results(to_be_merged, topK):
    if not to_be_merged or topK <= 0:
        return None

    # to_be_merged = [TopKQueryResultFactory() for _ in range(10)]
    # topK = 200

    results = []
    for result in to_be_merged:
        results.extend(result.query_results)
        results = sorted(results, key=lambda x: x.score)[:topK]

    return TopKQueryResult(query_results=results)

@celery_app.task(bind=True)
def schedule_query(self, file_ids, mapper, reducer):
    mapper = maybe_signature(mapper, self.app)
    reducer = maybe_signature(reducer, self.app)

    sub_tasks = [mapper.clone(args=(file_id,)) for file_id in file_ids]
    scheduled = chord(sub_tasks)(reducer)

    return scheduled
