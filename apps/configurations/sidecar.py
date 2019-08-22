
CELERY_ACCEPT_CONTENT = ['custommsgpack']
CELERY_TASK_SERIALIZER = 'custommsgpack'
CELERY_RESULT_SERIALIZER = 'custommsgpack'
CELERY_CREATE_MISSING_QUEUES = False

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
