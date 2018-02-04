from functools import wraps


def require_server_version(version_major, version_minor, min_revision):
    def check(f):
        @wraps(f)
        def wrapper(*args, **kwargs):
            self = args[0]
            self.client.connection.connect()

            info = self.client.connection.server_info

            if (
                info.version_major == version_major and
                info.version_minor == version_minor and
                min_revision <= info.revision
            ):
                return f(*args, **kwargs)
            else:
                self.skipTest(
                    'Mininum revision required: {}.{}.{}'
                    .format(version_major, version_minor, min_revision)
                )

        return wrapper
    return check
