import logging.config
import os

DEBUG = os.environ.get('DEBUG', False)
DEBUG = bool(DEBUG)

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

def config(log_level):
    LOGGING = {
        'version': 1,
        'disable_existing_loggers': False,
        'handlers': {
            'console': {
                'class': 'logging.StreamHandler',
            },
        },
        'loggers': {
            'milvus_celery': {
                'handlers': ['console'],
                'level': log_level,
                'propagate': False
            },
        },
        'propagate': False,
    }

    if DEBUG:
        LOGGING['formatters'] = {
            'colorful_console': {
                'format': '[%(asctime)s-%(levelname)s-%(name)s]: %(message)s (%(filename)s:%(lineno)s)',
                '()': ColorfulFormatter,
            },
        }
        LOGGING['handlers']['milvus_celery_console'] = {
            'class': 'logging.StreamHandler',
            'formatter': 'colorful_console',
        }
        LOGGING['loggers']['milvus_celery'] = {
            'handlers': ['milvus_celery_console'],
            'level': log_level,
        }

    logging.config.dictConfig(LOGGING)
