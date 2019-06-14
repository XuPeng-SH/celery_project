CELERY_ACCEPT_CONTENT = ['customjson']
CELERY_TASK_SERIALIZER = 'customjson'
CELERY_RESULT_SERIALIZER = 'customjson'
CELERY_IMPORTS = ['milvus_app']

CELERY_ROUTES={
    'milvus_app.tasks.query' : {'queue': 'for_query'},
}

CELERY_QUEUES={
    'for_query': {
        'exchange': 'for_query'
    }
}
