import sys
sys.path.append('..')


from milvus_celery.redis_handler import RedisHandler
from milvus_celery.app_helper import create_app
redis_client = RedisHandler()
celery_app = create_app(redis_client=redis_client)

from . import signal_handlers
