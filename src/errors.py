
import six


class ErrorCodes(object):
    NO_SUCH_COLUMN_IN_TABLE = 16
    CHECKSUM_DOESNT_MATCH = 40
    LOGICAL_ERROR = 49
    UNKNOWN_TYPE = 50
    TYPE_MISMATCH = 53
    UNKNOWN_COMPRESSION_METHOD = 89
    TOO_LARGE_STRING_SIZE = 131
    NETWORK_ERROR = 210
    SOCKET_TIMEOUT = 209
    SERVER_REVISION_IS_TOO_OLD = 197
    UNEXPECTED_PACKET_FROM_SERVER = 102
    UNKNOWN_PACKET_FROM_SERVER = 100


if six.PY3:
    class Error(Exception):
        code = None

        def __init__(self, message=None):
            self.message = message

        def __str__(self):
            message = ' ' + self.message if self.message is not None else ''
            return 'Code: {}.{}'.format(self.code, message)

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

        def __init__(self, message=None):
            self.message = message

        def __unicode__(self):
            message = ' ' + self.message if self.message is not None else ''
            return 'Code: {}.{}'.format(self.code, message)

        def __str__(self):
            return six.text_type(self).encode('utf-8')

    class ServerException(Error):
        def __init__(self, message, code, nested=None):
            self.message = message
            self.code = code
            self.nested = nested

        def __unicode__(self):
            nested = '\nNested: {}'.format(self.nested) if self.nested else ''
            return 'Code: {}.{}\n{}'.format(self.code, nested, self.message)

        def __str__(self):
            return six.text_type(self).encode('utf-8')


class LogicalError(Error):
    code = ErrorCodes.LOGICAL_ERROR


class UnknownTypeError(Error):
    code = ErrorCodes.UNKNOWN_TYPE


class ChecksumDoesntMatchError(Error):
    code = ErrorCodes.CHECKSUM_DOESNT_MATCH


class TypeMismatchError(Error):
    code = ErrorCodes.TYPE_MISMATCH


class UnknownCompressionMethod(Error):
    code = ErrorCodes.UNKNOWN_COMPRESSION_METHOD


class TooLargeStringSize(Error):
    code = ErrorCodes.TOO_LARGE_STRING_SIZE


class NetworkError(Error):
    code = ErrorCodes.NETWORK_ERROR


class SocketTimeoutError(Error):
    code = ErrorCodes.SOCKET_TIMEOUT


class UnexpectedPacketFromServerError(Error):
    code = ErrorCodes.UNEXPECTED_PACKET_FROM_SERVER


class UnknownPacketFromServerError(Error):
    code = ErrorCodes.UNKNOWN_PACKET_FROM_SERVER
