from celery import Celery
from milvus_app import settings
from milvus_app.db_base import DB
from milvus_app.app_helper import create_app

db = DB()

celery_app = create_app(db=db)
from milvus_app import tasks
from milvus_app import signal_handlers
