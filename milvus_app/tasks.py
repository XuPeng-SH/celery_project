from functools import reduce
from celery.exceptions import Reject
from celery import group, maybe_signature, chord, Task
from celery.utils.log import get_task_logger
from milvus_app import celery_app, db
from milvus_app.models import Table, TableFile
from milvus_app.errors import TableNotFoundError
from milvus_app.datatypes import QueryResponse

logger = get_task_logger(__name__)

from celery.worker.request import Request


@celery_app.task
def get_queryable_files(table_id, date_range=None):
    table = db.Session.query(Table).filter_by(table_id=table_id).first()

    if not table:
        response = QueryResponse.BuildErrorResponse(TableNotFoundError(
            table_id))
        response.result = []
        logger.error(response)
        return response

    files = table.files_to_search()
    result = [f.id for f in files]

    logger.error('Result={}'.format(result))

    return QueryResponse(result=result)

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
def schedule_query(self, files_response, mapper, reducer):
    mapper = maybe_signature(mapper, self.app)
    reducer = maybe_signature(reducer, self.app)

    file_ids = files_response.result

    sub_tasks = [mapper.clone(args=(file_id,)) for file_id in file_ids]
    scheduled = chord(sub_tasks)(reducer)
    logger.error(scheduled)

    return scheduled

@celery_app.task
def get_data(*args, **kwargs):
    return [3,4,5,6]

@celery_app.task
def do_map(n, index):
    logger.error('doing map {} {}'.format(n, index))
    result = n * index
    logger.error('result={}'.format(result))
    return result

@celery_app.task
def do_reduce(results):
    logger.error('doing reduce {}'.format(results))
    return sum(results)

@celery_app.task(bind=True)
def allocate(self, items, mapper, reducer):
    mapper = maybe_signature(mapper, self.app)
    reducer = maybe_signature(reducer, self.app)
    st = []
    for item in items:
        st.append(mapper.clone(args=(item,)))
    mapreduce = chord(st)(reducer)
    logger.error(mapreduce)
    return chord(st)(reducer)
