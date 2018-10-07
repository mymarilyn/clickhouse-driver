from functools import wraps


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
