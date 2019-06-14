from celery import group, maybe_signature, chord
from celery.utils.log import get_task_logger
from milvus_app import celery_app, db
from milvus_app.models import Table, TableFile
from milvus_app.errors import TableNotFoundError
from milvus_app.datatypes import QueryResponse

logger = get_task_logger(__name__)

@celery_app.task
def subquery(query_vectors, topk, file_id):
    logger.error('file_id={}'.format(file_id))
    logger.error('query_vectors={}'.format(query_vectors))
    logger.error('topk={}'.format(topk))
    return file_id

@celery_app.task
def query(table_id, query_vectors, topk, date_range):
    table = db.Session.query(Table).filter_by(table_id=table_id).first()
    if not table:
        response = QueryResponse.BuildErrorResponse(TableNotFoundError(
            table_id))
        logger.error(response)
        return response

    files = table.files_to_search()
    # for f in files:
    #     r = subquery.apply_async(args=[f.id, query_vectors, topk])
    #     results[f.id] = r
    # logger.error(results)
    return QueryResponse(result=[f.id for f in files])

@celery_app.task
def get_data(*args, **kwargs):
    return [3,4,5,6]

@celery_app.task
def do_map(n, index):
    # logger.error('doing map {} {}'.format(n, index))
    result = n * index
    # logger.error('result={}'.format(result))
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
    logger.error('xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx')
    logger.error(st)
    logger.error(reducer)
    mapreduce = chord(st)(reducer)
    logger.error(mapreduce)
    # return group(st|reducer)
    return chord(st)(reducer)
