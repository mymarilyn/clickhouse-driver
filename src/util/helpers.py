from itertools import islice


def chunks(seq, n):
    it = iter(seq)
    item = list(islice(it, n))
    while item:
        yield item
        item = list(islice(it, n))
