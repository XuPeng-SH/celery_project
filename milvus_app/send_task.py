import sys
sys.path.append('..')
import logging
from __init__ import celery_app

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

celery_app.config_from_object('milvus_app.config')

# celery_app.conf.update(
#     CELERY_TASK_SERIALIZER='json',
#     CELERY_RESULT_SERIALIZER='json',
#     CELERY_ENABLE_UTC=True,
#     CELERY_ROUTES={
#         'milvus_app.tasks.taskA' : {'queue': 'for_taskAA'},
#         'milvus_app.tasks.taskB' : {'queue': 'for_taskBB'},
#         'milvus_app.tasks.taskC' : {'queue': 'for_taskC'},
#     },
#     CELERY_QUEUES={
#         'for_taskAA': {
#             'exchange': 'for_taskA'
#         },
#         'for_taskBB': {
#             'exchange': 'for_taskB'
#         },
#         'for_taskC': {
#             'exchange': 'for_taskC'
#         },
#     }
# )

def main():
    for i in range(2):
        logger.debug('Send task query')
        resp = celery_app.send_task('milvus_app.tasks.query', ('1',2,3,4))
        logger.error(resp.get())
        # celery_app.send_task('milvus_app.tasks.query', ('test_group',2,3,4))

if __name__ == '__main__':
    main()
