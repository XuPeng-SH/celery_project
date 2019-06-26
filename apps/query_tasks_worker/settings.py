import os
from environs import Env
env_path = os.environ.get('ENV_PATH', None)
env = Env()
env.read_env(env_path)


class DefaultConfig:
    DEBUG = env.bool('DEBUG', default=False)
    SECRET_KEY = env.str('FLASK_SECRET_KEY', 'guess')

    SQLALCHEMY_DATABASE_URI = env.str('SQLALCHEMY_DATABASE_URI')
    SQLALCHEMY_TRACK_MODIFICATIONS = env.bool('SQLALCHEMY_TRACK_MODIFICATIONS')
    SQLALCHEMY_POOL_SIZE = env.int('SQLALCHEMY_POOL_SIZE', 50)
    SQLALCHEMY_POOL_RECYCLE = env.int('SQLALCHEMY_POOL_RECYCLE')
