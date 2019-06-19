import sys
sys.path.append('..')
from milvus import Milvus
from milvus_app import settings
from milvus_app.db_base import DB
from milvus_app.app_helper import create_app
from milvus_app.redis_handler import RedisHandler

db = DB()
redis_client = RedisHandler()
client = None
if settings.MILVUS_CLIENT:
    client = Milvus()

from apps.configurations import config
celery_app = create_app(db=db, redis_client=redis_client, client=client, config=config)

from apps.query_tasks_worker import tasks
# from milvus_app import signal_handlers
