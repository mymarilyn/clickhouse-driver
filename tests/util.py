from functools import wraps
import logging
from io import StringIO


def require_server_version(*version_required):
    def check(f):
        @wraps(f)
        def wrapper(*args, **kwargs):
            self = args[0]
            self.client.connection.connect()

            i = self.client.connection.server_info
            current = (i.version_major, i.version_minor, i.version_patch)

            if version_required <= current:
                return f(*args, **kwargs)
            else:
                self.skipTest(
                    'Mininum revision required: {}'.format(
                        '.'.join(str(x) for x in version_required)
                    )
                )

        return wrapper
    return check


class LoggingCapturer(object):
    def __init__(self, logger_name, level):
        self.old_stdout_handlers = []
        self.logger = logging.getLogger(logger_name)
        self.level = level
        super(LoggingCapturer, self).__init__()

    def __enter__(self):
        buffer = StringIO()

        self.new_handler = logging.StreamHandler(buffer)
        self.logger.addHandler(self.new_handler)
        self.old_logger_level = self.logger.level
        self.logger.setLevel(self.level)

        return buffer

    def __exit__(self, *exc_info):
        self.logger.setLevel(self.old_logger_level)
        self.logger.removeHandler(self.new_handler)


capture_logging = LoggingCapturer
