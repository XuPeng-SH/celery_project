from celery import Celery
from milvus_app import settings
from milvus_app.db_base import DB

db = DB(uri=settings.DB_URI)

def create_app(testing_config=None):
    from milvus_app import serializer
    app = Celery(__name__,
            broker=settings.CELERY_BROKER_URL,
            backend=settings.CELERY_BACKEND_URL)
    if testing_config:
        app.config_from_object(testing_config)
    else:
        app.config_from_object(settings.CELERY_APP_CONFIG_FILE)

    return app

celery_app = create_app()
from milvus_app import tasks
from milvus_app import signal_handlers
