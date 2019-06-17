CELERY_ACCEPT_CONTENT = ['customjson']
CELERY_TASK_SERIALIZER = 'customjson'
CELERY_RESULT_SERIALIZER = 'customjson'
CELERY_IMPORTS = ['milvus_app']
CELERY_CREATE_MISSING_QUEUES = False

CELERY_ROUTES={
    'milvus_app.tasks.query_file' : {'queue': 'for_query'},
    'milvus_app.tasks.schedule_query' : {'queue': 'for_query'},
    'milvus_app.tasks.get_queryable_files' : {'queue': 'for_query'},
    'milvus_app.tasks.merge_query_results' : {'queue': 'for_query'},
    'milvus_app.tasks.prepare_data_for_final_reduce' : {'queue': 'for_query'},
    'milvus_app.tasks.merge_files_query_results' : {'queue': 'for_query'},
    'milvus_app.tasks.final_reduce' : {'queue': 'for_query'},
}

CELERY_QUEUES={
    'celery': {
        'exchange': 'default'
    },
    'for_query': {
        'exchange': 'for_query'
    },
}
