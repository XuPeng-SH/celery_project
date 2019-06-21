import os

CELERY_ACCEPT_CONTENT = ['customjson']
CELERY_TASK_SERIALIZER = 'customjson'
CELERY_RESULT_SERIALIZER = 'customjson'
CELERY_CREATE_MISSING_QUEUES = False


def make_queues():
    q_str = os.environ.get('QUEUES', '')
    q_list = q_str.split(',')
    routes, queues = {}, {}
    for q in q_list:
        if not q:
            continue
        routes['tasks.handle_request'] = q
        queues[q] = {
            'exchange': q
        }
    assert len(routes) > 0

    return routes, queues


CELERY_ROUTES, CELERY_QUEUES = make_queues()

CELERY_QUEUES.update({
    'celery': {
        'exchange': 'default'
    },
})
print(CELERY_ROUTES)
print(CELERY_QUEUES)
