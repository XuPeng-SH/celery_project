from milvus_celery.exceptions import BaseException

class TableNotFoundException(BaseException):
    code = 1000
    message = 'TableNotFoundException'
