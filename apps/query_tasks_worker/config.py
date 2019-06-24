import os

CELERY_ACCEPT_CONTENT = ['customjson']
CELERY_TASK_SERIALIZER = 'customjson'
CELERY_RESULT_SERIALIZER = 'customjson'
CELERY_CREATE_MISSING_QUEUES = True
CELERY_ALWAYS_EAGER = True


def make_queues():
    q_str = os.environ.get('QUEUES', '')
    q_list = q_str.split(',')
    queues = {}
    for q in q_list:
        queues[q] = {
            'exchange' : q
        }

    return q_str, queues

QUEUES_STR, QUEUES = make_queues()

CELERY_ROUTES={
    'tasks.query_files' : {'queue': QUEUES_STR},
    'tasks.schedule_query' : {'queue': QUEUES_STR},
    'tasks.get_queryable_files' : {'queue': QUEUES_STR},
    'tasks.merge_query_results' : {'queue': QUEUES_STR},
    'tasks.tranpose_n_topk_results' : {'queue': QUEUES_STR},
    'tasks.reduce_one_request_files_results' : {'queue': QUEUES_STR},
    'tasks.reduce_n_request_files_results' : {'queue': QUEUES_STR},
}

CELERY_QUEUES={
    'celery': {
        'exchange': 'default'
    },
}

CELERY_QUEUES.update(QUEUES)
