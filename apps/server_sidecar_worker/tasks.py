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
    logger.debug('Querying routing {} nq={} topK={}'.format(routing, len(vectors), topK))
    client = SDKClient(host=settings.MILVUS_SERVER_HOST, port=settings.MILVUS_SERVER_PORT)
    with client:
        # results = client.search_vectors(table_id=routing[1]['table_id'],
        #         query_records=vectors, topK=topK)
        results = client.search_vectors_in_files(table_id=routing[1]['table_id'], file_ids=routing[1]['file_ids'],
                query_records=vectors, topK=topK)
        for pos, result in enumerate(results):
            logger.debug('result-{}: {}'.format(pos, result))
        logger.debug(len(results))

    return results
