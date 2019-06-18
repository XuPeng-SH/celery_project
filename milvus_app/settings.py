import os
import logging
import logging.config

from environs import Env
env = Env()
env.read_env()

TESTING = env.bool('TESTING', False)

DB_URI = env.str('DB_URI')

CELERY_APP_CONFIG_FILE = env.str('CELERY_APP_CONFIG_FILE')
CELERY_BROKER_URL = env.str('CELERY_BROKER_URL')
CELERY_BACKEND_URL = env.str('CELERY_BACKEND_URL')

REDIS_DB = env.int('REDIS_DB', 0)
REDIS_HOST = env.str('REDIS_HOST', 'localhost')
REDIS_PORT = env.int('REDIS_PORT', 6379)
REDIS_PASSWORD = env.str('REDIS_PASSWORD', None)

SERVERS_MONITOR_KEY = env.str('SERVERS_MONITOR_KEY', '')

DEBUG = env.bool('DEBUG', False)

LOG_LEVEL = env.str('LOG_LEVEL', 'DEBUG' if DEBUG else 'INFO')

from milvus_app import config_logger
config_logger.config(LOG_LEVEL)



# LOGGING = {
#     'version': 1,
#     'disable_existing_loggers': False,
#     'handlers': {
#         'console': {
#             'class': 'logging.StreamHandler',
#         },
#     },
#     'loggers': {
#         'default': {
#             'handlers': ['console'],
#             'level': LOG_LEVEL,
#         },
#     },
# }

# if DEBUG:
#     LOGGING['formatters'] = {
#         'colorful_console': {
#             'format': '[%(asctime)s-%(levelname)s-%(name)s]: %(message)s (%(filename)s:%(lineno)s)',
#             '()': ColorfulFormatter,
#         },
#     }
#     LOGGING['handlers']['default_console'] = {
#         'class': 'logging.StreamHandler',
#         'formatter': 'colorful_console',
#     }
#     LOGGING['loggers']['default'] = {
#         'handlers': ['default_console'],
#         'level': LOG_LEVEL,
#     }

# logging.config.dictConfig(LOGGING)

class TestingConfig:
    DB_URI = env.str('DB_TEST_URI')
    CELERY_APP_CONFIG_FILE = env.str('CELERY_APP_TEST_CONFIG_FILE', 'config_testing.py')
    CELERY_BROKER_URL = env.str('CELERY_BROKER_TEST_URL')
    CELERY_BACKEND_URL = env.str('CELERY_BACKEND_TEST_URL')

    TEST_DATA_SET_DIR = env.str('TEST_DATA_SET_DIR', './static_data/testing')
    TOPK_DATA_SET_FILE = 'topk_data_set_2k.json'

    REDIS_DB = env.int('REDIS_TEST_DB', 0)
    REDIS_HOST = env.str('REDIS_TEST_HOST', 'localhost')
    REDIS_PORT = env.int('REDIS_TEST_PORT', 6379)
    REDIS_PASSWORD = env.str('REDIS_TEST_PASSWORD', None)
