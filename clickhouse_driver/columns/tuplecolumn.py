
from .base import Column
from .util import get_inner_spec, get_inner_columns


class TupleColumn(Column):
    py_types = (list, tuple)

    def __init__(self, nested_columns, **kwargs):
        self.nested_columns = nested_columns
        super(TupleColumn, self).__init__(**kwargs)

    def write_data(self, items, buf):
        items = list(zip(*items))

        for i, x in enumerate(self.nested_columns):
            x.write_data(list(items[i]), buf)

    def write_items(self, items, buf):
        return self.write_data(items, buf)

    def read_data(self, n_items, buf):
        rv = [x.read_data(n_items, buf) for x in self.nested_columns]
        return list(zip(*rv))

    def read_items(self, n_items, buf):
        return self.read_data(n_items, buf)


def create_tuple_column(spec, column_by_spec_getter):
    inner_spec = get_inner_spec('Tuple', spec)
    columns = get_inner_columns(inner_spec)

    return TupleColumn([column_by_spec_getter(x) for x in columns])
