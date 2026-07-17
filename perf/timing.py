import atexit
import resource
import sys
import time

_start = time.monotonic()


@atexit.register
def _report():
    elapsed = time.monotonic() - _start
    rss = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    if sys.platform != "darwin":
        rss *= 1024  # Linux reports kilobytes, macOS reports bytes.
    print("{:.2f} s / {} bytes max RSS".format(elapsed, rss), file=sys.stderr)
