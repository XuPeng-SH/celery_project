from celery import Celery
from milvus_app import serializer
from milvus_app import settings

def create_app(db=None, redis_client=None, testing=False):
    app = Celery(__name__,
            broker=settings.CELERY_BROKER_URL,
            backend=settings.CELERY_BACKEND_URL)
    if settings.TESTING:
        from milvus_app.settings import TestingConfig
        app.config_from_object(TestingConfig)
        db and db.init_db(uri=TestingConfig.DB_URI)
        if redis_client:
            redis_client.init(host=TestingConfig.REDIS_HOST,
                    port=TestingConfig.REDIS_PORT,
                    password=TestingConfig.REDIS_PASSWORD,
                    db=TestingConfig.REDIS_DB)
    else:
        app.config_from_object(settings.CELERY_APP_CONFIG_FILE)
        db and db.init_db(uri=settings.DB_URI)
        if redis_client:
            redis_client.init(host=settings.REDIS_HOST,
                    port=settings.REDIS_PORT,
                    password=settings.REDIS_PASSWORD,
                    db=settings.REDIS_DB)


    return app
