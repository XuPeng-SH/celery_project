import sys
import os
import logging, logging.config
from environs import Env

env = Env()
env.read_env()

os.environ['ENV_PATH'] = os.path.dirname(os.path.abspath(__file__))
DEBUG = env.bool('DEBUG', default=False)

THRIFTCLIENT_TRANSPORT = env.str('THRIFTCLIENT_TRANSPORT')
THRIFTCLIENT_PROTOCOL = env.str('THRIFTCLIENT_PROTOCOL')
THRIFTCLIENT_BUFFERED = env.bool('THRIFTCLIENT_BUFFERED')
THRIFTCLIENT_ZLIB = env.bool('THRIFTCLIENT_ZLIB')
THRIFTCLIENT_FRAMED = env.bool('THRIFTCLIENT_FRAMED')
THRIFTCLIENT_RETRY_TIME = env.int('THRIFTCLIENT_RETRY_TIME')
THRIFTCLIENT_NORMAL_TIME = env.int('THRIFTCLIENT_NORMAL_TIME')

THRIFTSERVER_TRANSPORT = env.str('THRIFTSERVER_TRANSPORT')
THRIFTSERVER_PROTOCOL = env.str('THRIFTSERVER_PROTOCOL')
THRIFTSERVER_BUFFERED = env.bool('THRIFTSERVER_BUFFERED')
THRIFTSERVER_ZLIB = env.bool('THRIFTSERVER_ZLIB')
THRIFTSERVER_FRAMED = env.bool('THRIFTSERVER_FRAMED')

QUEUES = env.str('QUEUES')

# logging
COLORS = {
    'HEADER': '\033[95m',
    'INFO': '\033[92m',
    'DEBUG': '\033[94m',
    'WARNING': '\033[93m',
    'ERROR': '\033[95m',
    'CRITICAL': '\033[91m',
    'ENDC': '\033[0m',
}


class ColorFulFormatColMixin:
    def format_col(self, message_str, level_name):
        if level_name in COLORS.keys():
            message_str = COLORS.get(level_name) + message_str + COLORS.get(
                'ENDC')
        return message_str


class ColorfulFormatter(logging.Formatter, ColorFulFormatColMixin):
    def format(self, record):
        message_str = super(ColorfulFormatter, self).format(record)

        return self.format_col(message_str, level_name=record.levelname)


LOG_LEVEL = 'DEBUG'

LOGGING = {
    'version': 1,
    'disable_existing_loggers': False,
    'handlers': {
        'console': {
            'class': 'logging.StreamHandler',
        },
    },
    'loggers': {
        'proxy_server': {
            'handlers': ['console'],
            'level': LOG_LEVEL,
        },
    },
}

if LOG_LEVEL == 'DEBUG':
    LOGGING['formatters'] = {
        'colorful_console': {
            'format': '[%(asctime)s-%(levelname)s-%(name)s]: %(message)s (%(filename)s:%(lineno)s)',
            '()': ColorfulFormatter,
        },
    }
    LOGGING['handlers']['proxy_server_console'] = {
        'class': 'logging.StreamHandler',
        'formatter': 'colorful_console',
    }
    LOGGING['loggers']['proxy_server'] = {
        'handlers': ['proxy_server_console'],
        'level': LOG_LEVEL,
    }

logging.config.dictConfig(LOGGING,)
