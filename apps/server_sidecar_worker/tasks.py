from celery.utils.log import get_task_logger
from milvus import Milvus
from milvus.client.Abstract import TopKQueryResult, QueryResult

from milvus_celery.operations import SDKClient

from .app import celery_app

logger = get_task_logger(__name__)

@celery_app.task
def handle_request(file_ids, query_vectors, topK):
    logger.info(file_ids)
