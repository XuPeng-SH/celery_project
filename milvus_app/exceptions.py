class BaseException(Exception):
    code = -1
    def __init__(self, message=''):
        self.message = self.__class__.__name__
        self.message = '{}: {}'.format(self.message, message) if message else self.message

    def __str__(self):
        return self.message

class TableNotFoundException(BaseException):
    code = 1000
    message = 'TableNotFoundException'
