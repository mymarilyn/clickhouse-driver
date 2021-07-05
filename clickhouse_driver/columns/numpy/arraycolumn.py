import numpy as np
from struct import Struct

from ..arraycolumn import ArrayColumn
from ...util.helpers import pairwise

class NumpyArrayColumn(ArrayColumn):
    def _read(self, size, buf):
        slices_series = [[0, size]]
        nested_column = self.nested_column

        cur_level_slice_size = size
        cur_level_slice = None
        while (isinstance(nested_column, ArrayColumn)):
            if cur_level_slice is None:
                cur_level_slice = [0]
            ns = Struct('<{}Q'.format(cur_level_slice_size))
            nested_sizes = ns.unpack(buf.read(ns.size))
            cur_level_slice.extend(nested_sizes)
            slices_series.append(cur_level_slice)
            cur_level_slice = None
            cur_level_slice_size = nested_sizes[-1] if len(nested_sizes) > 0 \
                else 0
            nested_column = nested_column.nested_column

        n_items = cur_level_slice_size if size > 0 else 0
        nulls_map = None
        if nested_column.nullable:
            nulls_map = self._read_nulls_map(n_items, buf)

        data = []
        if n_items:
            # use numpy array instead of list to improve memory usage
            # (especially for arrays of scalar types)
            data = np.array(nested_column._read_data(
                n_items, buf, nulls_map=nulls_map
            ))

        # Build nested structure.
        for slices in reversed(slices_series):
            data = [data[begin:end] for begin, end in pairwise(slices)]

        return tuple(data)

def create_numpy_array_column(spec, column_by_spec_getter):
    inner = spec[6:-1]
    return NumpyArrayColumn(column_by_spec_getter(inner))
