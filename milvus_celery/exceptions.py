class BaseException(Exception):
    code = -1
    def __init__(self, message=''):
        self.message = self.__class__.__name__
        self.message = '{}: {}'.format(self.message, message) if message else self.message

class SDKClientConnectionException(BaseException):
    code = 1001
    message = 'SDLClientConnectionException'

class SDKClientSearchVectorException(BaseException):
    code = 1002
    message = 'SDKClientSearchVectorException'
