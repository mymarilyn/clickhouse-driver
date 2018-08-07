from functools import wraps


def require_server_version(*version_required):
    def check(f):
        @wraps(f)
        def wrapper(*args, **kwargs):
            self = args[0]
            self.client.connection.connect()

            info = self.client.connection.server_info
            current = (info.version_major, info.version_minor, info.revision)

            if version_required <= current:
                return f(*args, **kwargs)
            else:
                self.skipTest(
                    'Mininum revision required: {}'.format(version_required)
                )

        return wrapper
    return check
