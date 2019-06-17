from celery import Celery
from milvus_app import settings
from milvus_app.db_base import DB
from milvus_app.app_helper import create_app

db = DB(uri=settings.DB_URI)

celery_app = create_app()
from milvus_app import tasks
from milvus_app import signal_handlers
