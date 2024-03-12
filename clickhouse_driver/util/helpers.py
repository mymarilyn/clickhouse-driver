from itertools import islice, tee

try:
    import numpy as np

    CHECK_NUMPY_TYPES = True
except ImportError:
    CHECK_NUMPY_TYPES = False


def _check_sequence_to_be_an_expected_iterable(seq):
    expected = [list, tuple]
    if CHECK_NUMPY_TYPES:
        expected.append(np.ndarray)
    return isinstance(seq, tuple(expected))


def chunks(seq, n):
    # islice is MUCH slower than slice for lists and tuples.
    if _check_sequence_to_be_an_expected_iterable(seq):
        i = 0
        item = seq[i : i + n]  # noqa: E203
        # DeprecationWarning: The truth value of an empty array is ambiguous.
        while len(item):
            yield list(item)
            i += n
            item = seq[i : i + n]  # noqa: E203
    else:
        it = iter(seq)
        item = list(islice(it, n))
        while item:
            yield item
            item = list(islice(it, n))


def pairwise(iterable):
    a, b = tee(iterable)
    next(b, None)
    return zip(a, b)


def column_chunks(columns, n):
    for column in columns:
        if not _check_sequence_to_be_an_expected_iterable(column):
            raise TypeError(
                f"Unsupported column type: {type(column)}. "
                "Expected list, tuple or numpy.ndarray"
            )

    # create chunk generator for every column
    g = [chunks(column, n) for column in columns]
    while True:
        # get next chunk for every column
        item = [next(column, []) for column in g]
        if not any(item):
            break
        yield item


# from paste.deploy.converters
def asbool(obj):
    if isinstance(obj, str):
        obj = obj.strip().lower()
        if obj in ["true", "yes", "on", "y", "t", "1"]:
            return True
        elif obj in ["false", "no", "off", "n", "f", "0"]:
            return False
        else:
            raise ValueError(f"String is not true/false: {obj!r}")
    return bool(obj)
