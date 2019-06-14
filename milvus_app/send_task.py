import sys
sys.path.append('..')
import logging
from __init__ import celery_app
from celery import group, chain, signature

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

celery_app.config_from_object('milvus_app.config')
from milvus_app import tasks

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

def calculate():
    rs = []
    for i in range(100):
        final = tasks.do_reduce.s()
        final_result = final.freeze()

        chain(tasks.get_data.s(), tasks.allocate.s(tasks.do_map.s(-2), final))()
        rs.append(final_result)
    for f in rs:
        r = f.get()
        logger.error(r)

def main():
    calculate()
    return
    for i in range(2):
        logger.debug('Send task query')
        # celery.send_task(
        #     signature('milvus_app.tasks.query')
        # )
        resp = tasks.query.delay('test_group', 2, 3, 4).get()
        if not resp.result:
            logger.error('No result')
            continue
        group_task = group(tasks.subquery.s(i) for i in resp.result)
        r = group_task(3,4).get()
        logger.error(r)

if __name__ == '__main__':
    main()
