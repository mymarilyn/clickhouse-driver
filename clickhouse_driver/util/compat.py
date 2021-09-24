
# Drop this when minimum supported version will be 3.7.
try:
    import threading
except ImportError:
    import dummy_threading as threading  # noqa: F401
