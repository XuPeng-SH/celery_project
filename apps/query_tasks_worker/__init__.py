import os
ENV_PATH = os.environ.get('ENV_PATH', None)
os.environ['ENV_PATH'] = os.path.dirname(__file__) if ENV_PATH is None else ENV_PATH

import sys
sys.path.append('..')
from milvus import Milvus
from milvus_celery.app_helper import create_app as create_celery
from milvus_celery.redis_handler import RedisHandler
from flask_sqlalchemy import SQLAlchemy
from . import settings

db = SQLAlchemy()
redis_client = RedisHandler()

from . import config
celery_app = create_celery(redis_client=redis_client, config=config)

from flask import Flask

def config_celery(c, app):
    TaskBase = c.Task

    class ContextTask(TaskBase):
        abstract = True

        def __call__(self, *args, **kwargs):
            with app.app_context():
                return TaskBase.__call__(self, *args, **kwargs)

    c.Task = ContextTask
    c.conf.update(app.config)

def create_app():
    app = Flask(__name__)
    app.config.from_object(settings.DefaultConfig)

    config_celery(celery_app, app)
    db.init_app(app)

    from . import tasks

    return app
