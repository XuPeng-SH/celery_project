from celery import Celery
from milvus_app import serializer
from milvus_app import settings

def create_app(testing_config=None):
    app = Celery(__name__,
            broker=settings.CELERY_BROKER_URL,
            backend=settings.CELERY_BACKEND_URL)
    if testing_config:
        app.config_from_object(testing_config)
    else:
        app.config_from_object(settings.CELERY_APP_CONFIG_FILE)

    return app
