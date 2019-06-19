CELERY_ACCEPT_CONTENT = ['customjson']
CELERY_TASK_SERIALIZER = 'customjson'
CELERY_RESULT_SERIALIZER = 'customjson'
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
