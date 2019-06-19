from milvus_app.exceptions import BaseException

class TableNotFoundException(BaseException):
    code = 1000
    message = 'TableNotFoundException'
