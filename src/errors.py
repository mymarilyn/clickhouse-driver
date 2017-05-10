
from six import PY3


class ErrorCodes(object):
    NO_SUCH_COLUMN_IN_TABLE = 16
    LOGICAL_ERROR = 49
    TYPE_MISMATCH = 53
    NETWORK_ERROR = 210
    SOCKET_TIMEOUT = 209
    SERVER_REVISION_IS_TOO_OLD = 197
    UNEXPECTED_PACKET_FROM_SERVER = 102
    UNKNOWN_PACKET_FROM_SERVER = 100

if PY3:
    class Error(Exception):
        code = None

        def __str__(self):
            return 'Code: {}.'.format(self.code)

    class ServerException(Error):
        def __init__(self, message, code, nested=None):
            self.message = message
            self.code = code
            self.nested = nested

        def __str__(self):
            nested = '\nNested: {}'.format(self.nested) if self.nested else ''
            return 'Code: {}.{}\n{}'.format(self.code, nested, self.message)

else:
    class Error(Exception):
        code = None

        def __unicode__(self):
            return 'Code: {}.'.format(self.code)

        def __str__(self):
            return unicode(self).encode('utf-8')

    class ServerException(Error):
        def __init__(self, message, code, nested=None):
            self.message = message
            self.code = code
            self.nested = nested

        def __unicode__(self):
            nested = '\nNested: {}'.format(self.nested) if self.nested else ''
            return 'Code: {}.{}\n{}'.format(self.code, nested, self.message)

        def __str__(self):
            return unicode(self).encode('utf-8')


class LogicalError(Error):
    code = ErrorCodes.LOGICAL_ERROR


class TypeMismatchError(Error):
    code = ErrorCodes.TYPE_MISMATCH


class NetworkError(Error):
    code = ErrorCodes.NETWORK_ERROR


class SocketTimeoutError(Error):
    code = ErrorCodes.SOCKET_TIMEOUT


class UnexpectedPacketFromServerError(Error):
    code = ErrorCodes.UNEXPECTED_PACKET_FROM_SERVER


class UnknownPacketFromServerError(Error):
    code = ErrorCodes.UNKNOWN_PACKET_FROM_SERVER
