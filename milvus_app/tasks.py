from functools import reduce
from celery import maybe_signature, chord
from celery.utils.log import get_task_logger
from milvus_app import celery_app, db
from milvus_app.models import Table

from milvus_app.exceptions import TableNotFoundException

logger = get_task_logger(__name__)


@celery_app.task
def get_queryable_files(table_id, date_range=None):
    table = db.Session.query(Table).filter_by(table_id=table_id).first()

    if not table:
        raise TableNotFoundException(table_id)

    files = table.files_to_search()
    result = [f.id for f in files]

    logger.error('Result={}'.format(result))

    return result

@celery_app.task
def query_file(file_id, vectors, topK):
    logger.error('Querying file {}'.format(file_id))
    return '{}.result'.format(file_id)

@celery_app.task
def merge_query_results(results, topK):
    if not results:
        return None
    logger.error('Merging results: {}'.format(results))
    ret = reduce(lambda x, y: '{}{}'.format(x, y),
            map(lambda x: '--------{}-------\n'.format(x), results))
    logger.error(ret)
    return ret

@celery_app.task(bind=True)
def schedule_query(self, file_ids, mapper, reducer):
    mapper = maybe_signature(mapper, self.app)
    reducer = maybe_signature(reducer, self.app)

    sub_tasks = [mapper.clone(args=(file_id,)) for file_id in file_ids]
    scheduled = chord(sub_tasks)(reducer)

    return scheduled
