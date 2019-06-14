CELERY_ACCEPT_CONTENT = ['customjson']
CELERY_TASK_SERIALIZER = 'customjson'
CELERY_RESULT_SERIALIZER = 'customjson'
CELERY_IMPORTS = ['milvus_app']
CELERY_CREATE_MISSING_QUEUES = False

CELERY_ROUTES={
    'milvus_app.tasks.query' : {'queue': 'for_query'},
    'milvus_app.tasks.subquery' : {'queue': 'for_subquery'},
    'milvus_app.tasks.get_data' : {'queue': 'for_test'},
    'milvus_app.tasks.do_map' : {'queue': 'for_test'},
    'milvus_app.tasks.do_reduce' : {'queue': 'for_test'},
    'milvus_app.tasks.allocate' : {'queue': 'for_test'},
}

CELERY_QUEUES={
    'celery': {
        'exchange': 'default'
    },
    'for_query': {
        'exchange': 'for_query'
    },
    'for_subquery': {
        'exchange': 'for_subquery'
    },
    'for_test': {
        'exchange': 'for_test'
    },
}
