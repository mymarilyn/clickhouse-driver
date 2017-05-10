from datetime import timedelta, tzinfo


class tzutc(tzinfo):
    """
    Simple class for obtaining naive datetime without timezone.
    It prevents using whole pytz/dateutil in requirements.
    """

    ZERO = timedelta(0)

    def utcoffset(self, dt):
        return tzutc.ZERO

    def dst(self, dt):
        return tzutc.ZERO

    def tzname(self, dt):
        return 'UTC'
