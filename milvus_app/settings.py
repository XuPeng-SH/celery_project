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

class TestingConfig:
    DB_URI = env.str('DB_TEST_URI')
    CELERY_APP_CONFIG_FILE = env.str('CELERY_APP_TEST_CONFIG_FILE', 'config_testing.py')
    CELERY_BROKER_URL = env.str('CELERY_BROKER_TEST_URL')
    CELERY_BACKEND_URL = env.str('CELERY_BACKEND_TEST_URL')

    TEST_DATA_SET_DIR = env.str('TEST_DATA_SET_DIR', './static_data/testing')
    TOPK_DATA_SET_FILE = 'topk_data_set_2k.json'
