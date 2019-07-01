import os

CELERYD_HIJACK_ROOT_LOGGER = False
CELERY_ACCEPT_CONTENT = ['customjson']
CELERY_TASK_SERIALIZER = 'customjson'
CELERY_RESULT_SERIALIZER = 'customjson'
CELERY_CREATE_MISSING_QUEUES = True


def make_queues():
    q_str = os.environ.get('QUEUES', '')
    q_list = q_str.split(',')
    routes, queues = {}, {}
    for q in q_list:
        if not q:
            continue
        routes['tasks.query_files'] = q
        queues[q] = {
            'exchange': q
        }
    return routes, queues


CELERY_ROUTES, CELERY_QUEUES = make_queues()

CELERY_QUEUES.update({
    'celery': {
        'exchange': 'default'
    },
})
