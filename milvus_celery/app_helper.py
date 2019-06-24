import logging
from celery import Celery
from milvus_celery import serializer
from milvus_celery import settings
from milvus_celery.settings import TestingConfig, DefaultConfig

logger = logging.getLogger(__name__)

def create_app(db=None, redis_client=None, testing=False, client=None, config=None):
    active_settings = TestingConfig if settings.TESTING else DefaultConfig
    app = Celery(__name__,
            broker=active_settings.CELERY_BROKER_URL,
            backend=active_settings.CELERY_BACKEND_URL)
    app.config_from_object(config)

    db and db.init_db(uri=active_settings.DB_URI)
    if redis_client:
        redis_client.init(host=active_settings.REDIS_HOST,
                port=active_settings.REDIS_PORT,
                password=active_settings.REDIS_PASSWORD,
                db=active_settings.REDIS_DB)

    if client:
       status = client.connect(host=active_settings.MILVUS_SERVER_HOST,
                port=active_settings.MILVUS_SERVER_PORT)
       assert status and status.code == status.SUCCESS

    return app
