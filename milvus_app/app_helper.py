from celery import Celery
from milvus_app import serializer
from milvus_app import settings
from milvus_app.settings import TestingConfig

def create_app(db=None, redis_client=None, testing=False, client=None):
    app = Celery(__name__,
            broker=settings.CELERY_BROKER_URL,
            backend=settings.CELERY_BACKEND_URL)
    app.config_from_object(TestingConfig if settings.TESTING else settings.CELERY_APP_CONFIG_FILE)
    active_settings = TestingConfig if settings.TESTING else settings

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
