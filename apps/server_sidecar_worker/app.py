import os
ENV_PATH = os.environ.get('ENV_PATH', None)
os.environ['ENV_PATH'] = os.path.dirname(__file__) if ENV_PATH is None else ENV_PATH

import sys
sys.path.append('..')

from milvus_celery.redis_handler import RedisHandler
from milvus_celery.app_helper import create_app
redis_client = RedisHandler()

from . import config
celery_app = create_app(redis_client=redis_client, config=config)

import logging
logger = logging.getLogger(__name__)
logger.debug(config)

from . import tasks
from . import signal_handlers
