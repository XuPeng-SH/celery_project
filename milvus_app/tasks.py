from celery.utils.log import get_task_logger
from milvus_app import celery_app, db
from milvus_app.models import Table, TableFile
from milvus_app.errors import TableNotFoundError
from milvus_app.datatypes import QueryResponse

logger = get_task_logger(__name__)


@celery_app.task
def query(table_id, query_vectors, topk, date_range):
    table = db.Session.query(Table).filter_by(table_id=table_id).first()
    if not table:
        response = QueryResponse.BuildErrorResponse(TableNotFoundError(
            table_id))
        logger.error(response)
        return response
