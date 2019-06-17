import os
import logging
import logging.config

from environs import Env
env = Env()
env.read_env()

DB_URI = env.str('DB_URI')

CELERY_APP_CONFIG_FILE = env.str('CELERY_APP_CONFIG_FILE')
CELERY_BROKER_URL = env.str('CELERY_BROKER_URL')
CELERY_BACKEND_URL = env.str('CELERY_BACKEND_URL')

# class TestingConfig:
#     CELERY_APP_CONFIG_FILE = env.str('CELERY_APP_TEST_CONFIG_FILE', 'config_testing.py')
#     TEST_DATA_SET_DIR = env.str(TEST_DATA_SET_DIR, './static_data/testing')
#     TOPK_DATA_SET_FILE = 'topk_data_set_2k.json'
