import logging
from milvus_app import settings
from milvus_app.db_base import DB
from milvus_app.app_helper import create_app

logger = logging.getLogger(__name__)

db = DB()
celery_app = create_app(db=db)

def monitor_loop(application):
    with application.connection() as connection:
        recv = application.events.Receiver(connection,
                handlers={
                    'server-online': handlers.on_server_online,
                    'server-offline': handlers.on_server_offline
                })

        recv.capture(limit=None, timeout=None, wakeup=True)

def main():
    monitor_loop(application)

if __name__ == '__main__':
    main()
