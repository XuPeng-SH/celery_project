
CELERY_ACCEPT_CONTENT = ['custommsgpack']
CELERY_TASK_SERIALIZER = 'custommsgpack'
CELERY_RESULT_SERIALIZER = 'custommsgpack'
CELERY_CREATE_MISSING_QUEUES = False
CELERYD_MAX_TASKS_PER_CHILD = 1000
CELERY_TASK_RESULT_EXPIRES = 7200

CELERY_ROUTES={
}

CELERY_QUEUES={
    'celery': {
        'exchange': 'default'
    },
    'notask': {
        'exchange': 'notask'
    }
}
