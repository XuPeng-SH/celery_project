from celery.utils.log import get_task_logger
from milvus import Milvus
from milvus.client.Abstract import TopKQueryResult, QueryResult

from milvus_celery.operations import SDKClient

from milvus_celery import settings
from .app import celery_app
from common.factories import TopKQueryResultFactory

logger = get_task_logger(__name__)

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
