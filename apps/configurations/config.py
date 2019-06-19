CELERY_ACCEPT_CONTENT = ['customjson']
CELERY_TASK_SERIALIZER = 'customjson'
CELERY_RESULT_SERIALIZER = 'customjson'
CELERY_CREATE_MISSING_QUEUES = False

CELERY_ROUTES={
    'tasks.query_files' : {'queue': 'for_query'},
    'tasks.schedule_query' : {'queue': 'for_query'},
    'tasks.get_queryable_files' : {'queue': 'for_query'},
    'tasks.merge_query_results' : {'queue': 'for_query'},
    'tasks.tranpose_n_topk_results' : {'queue': 'for_query'},
    'tasks.reduce_one_request_files_results' : {'queue': 'for_query'},
    'tasks.reduce_n_request_files_results' : {'queue': 'for_query'},
}

CELERY_QUEUES={
    'celery': {
        'exchange': 'default'
    },
    'for_query': {
        'exchange': 'for_query'
    },
}
