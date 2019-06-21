import os
ENV_PATH = os.environ.get('ENV_PATH', None)
os.environ['ENV_PATH'] = os.path.dirname(__file__) if ENV_PATH is None else ENV_PATH

import sys
sys.path.append('..')
from milvus import Milvus
from milvus_celery import settings
from milvus_celery.db_base import DB
from milvus_celery.app_helper import create_app
from milvus_celery.redis_handler import RedisHandler


db = DB()
redis_client = RedisHandler()
client = None
if settings.MILVUS_CLIENT:
    client = Milvus()

from . import config
# from configurations import config
celery_app = create_app(db=db, redis_client=redis_client, client=client, config=config)

from query_tasks_worker import tasks
