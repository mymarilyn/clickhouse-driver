from collections import Iterable
from itertools import islice, tee


def chunks(seq, n):
    # islice is MUCH slower than slice for lists and tuples.
    if isinstance(seq, (list, tuple)):
        i = 0
        item = seq[i:i+n]
        while item:
            yield list(item)
            i += n
            item = seq[i:i+n]

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
        if not isinstance(column, Iterable):
            raise TypeError(
                'Unsupported column type: {}. Iterable is expected.'
                .format(type(column))
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
        if obj in ['true', 'yes', 'on', 'y', 't', '1']:
            return True
        elif obj in ['false', 'no', 'off', 'n', 'f', '0']:
            return False
        else:
            raise ValueError('String is not true/false: %r' % obj)
    return bool(obj)
