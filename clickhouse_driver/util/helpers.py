from itertools import islice


def chunks(seq, n):
    it = iter(seq)
    item = list(islice(it, n))
    while item:
        yield item
        item = list(islice(it, n))


def column_chunks(columns, n):
    for column in columns:
        if not isinstance(column, (list, tuple)):
            raise TypeError(
                'Unsupported column type: {}. list or tuple is expected.'
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
