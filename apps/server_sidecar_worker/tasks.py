import logging
import time
from celery.utils.log import get_task_logger
from milvus import Milvus
from milvus.client.Abstract import TopKQueryResult, QueryResult

from milvus_celery.operations import SDKClient

from milvus_celery import settings
from milvus_celery.datatypes import SearchBatchResults
from .app import celery_app
from common.factories import TopKQueryResultFactory

# logger = get_task_logger(__name__)
logger = logging.getLogger(__name__)

@celery_app.task(autoretry_for=(Exception,),
          retry_kwargs={'max_retries': 2})
def query_files(routing, vectors, topK):
    logger.info('Start Query @{}'.format(time.time()))
    logger.debug('Querying routing {} nq={} topK={}'.format(routing, len(vectors), topK))
    client = SDKClient(host=settings.MILVUS_SERVER_HOST, port=settings.MILVUS_SERVER_PORT)
    with client:
        start = time.time()
        results = client.search_vectors_in_files(table_id=routing[1]['table_id'], file_ids=routing[1]['file_ids'],
                query_records=vectors, topK=topK)
        end = time.time()
        logger.info('search_vectors_in_files takes: {}'.format(end - start))

    ret = SearchBatchResults(results, True)

    return ret
